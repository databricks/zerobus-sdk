"""Scenario builder DSL, load-time validator and step executor.

Authoring (CONTRACTS.md "Python builder DSL surface"): a scenario function is run once, combo-blind,
against a ``Recorder``. Action calls append steps and return refs; nothing talks to a server while
recording. ``Executor`` later walks the recorded steps for one concrete (sdk, transport, format).
"""

from __future__ import annotations

import copy
import importlib.util
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from zerobus_test_runner.capability import CAPABILITIES, admits, valid_tuples
from zerobus_test_runner.data.golden import GOLDEN_FORMATS, expected_bytes
from zerobus_test_runner.data.schemas import load_schema, supported_formats
from zerobus_test_runner.errors import InfraError

KNOWN_FORMATS = frozenset({"proto", "json", "avro", "arrow"})


class ScenarioError(Exception):
    """Author error: bad gate, bad ref, recording failure, failed validation."""


# --------------------------------------------------------------------------- gates


@dataclass(frozen=True)
class Gate:
    """Applicability selector. ``None`` in a dimension is a wildcard."""

    formats: Optional[frozenset] = None
    transports: Optional[frozenset] = None
    sdks: Optional[frozenset] = None

    def matches(self, sdk: str, transport: str, fmt: str) -> bool:
        return (
            (self.sdks is None or sdk in self.sdks)
            and (self.transports is None or transport in self.transports)
            and (self.formats is None or fmt in self.formats)
        )


def formats(*names: str) -> Gate:
    if not names:
        raise ScenarioError("formats() needs at least one format name")
    bad = [n for n in names if n not in KNOWN_FORMATS]
    if bad:
        raise ScenarioError(f"unknown format(s) {bad}; known: {sorted(KNOWN_FORMATS)}")
    return Gate(formats=frozenset(names))


# --------------------------------------------------------------------------- refs


@dataclass(eq=False)  # identity equality on purpose: no __eq__/__bool__ traps
class _Ref:
    owner: "Recorder"
    index: int  # position of the step that produced this ref


@dataclass(eq=False)
class StreamRef(_Ref):
    pass


@dataclass(eq=False)
class OffsetRef(_Ref):
    """Symbolic: the offset the harness returned for a record. Resolved at execution."""

    record: "RecordRef"


@dataclass(eq=False)
class RecordRef(_Ref):
    stream: StreamRef
    offset: OffsetRef = field(init=False)

    def __post_init__(self) -> None:
        self.offset = OffsetRef(self.owner, self.index, self)


@dataclass(frozen=True, eq=False)
class GoldenRef:
    """Expected bytes, produced by the independent encoder in ``data.golden``."""

    schema: str
    record: dict
    fmt: str


@dataclass(frozen=True, eq=False)
class Target:
    """A MockAdmin observation. ``query`` is the MockAdminClient method name."""

    query: str
    args: dict
    label: str


def golden(schema: str, record: dict, fmt: str) -> GoldenRef:
    if not isinstance(record, dict):
        raise ScenarioError("golden(): record must be a dict")
    if fmt not in GOLDEN_FORMATS:
        raise ScenarioError(f"golden(): no golden encoder for '{fmt}' (have {sorted(GOLDEN_FORMATS)})")
    return GoldenRef(schema, copy.deepcopy(record), fmt)


# --------------------------------------------------------------------------- steps


@dataclass(eq=False)
class CreateStream:
    schema: str
    table: str
    out: StreamRef


@dataclass(eq=False)
class Ingest:
    stream: StreamRef
    record: dict
    out: RecordRef


@dataclass(eq=False)
class Flush:
    stream: StreamRef


@dataclass(eq=False)
class Close:
    stream: StreamRef


@dataclass(eq=False)
class AwaitWrite:
    rec: RecordRef
    timeout_ms: int


@dataclass(eq=False)
class MockInject:
    """Action: arm a one-shot stream abort on the mock (fault injection)."""

    after_offset: int
    retryable: bool


@dataclass(eq=False)
class Expect:
    target: Target
    equals: Any = None
    at_least: Any = None


# --------------------------------------------------------------------------- recorder


class Recorder:
    """The ``s`` object handed to a scenario function."""

    def __init__(self) -> None:
        self.steps: list = []
        self.sdk = _SdkRecorder(self)
        self.mock = _MockRecorder(self)

    def expect(self, target: Target, equals: Any = None, at_least: Any = None) -> None:
        if not isinstance(target, Target):
            raise ScenarioError("expect(): first argument must be an s.mock.* observation")
        if (equals is None) == (at_least is None):
            raise ScenarioError("expect(): pass exactly one of equals= or at_least=")
        self.steps.append(Expect(target, equals, at_least))

    def _check_stream(self, stream: Any, what: str) -> StreamRef:
        if not isinstance(stream, StreamRef) or stream.owner is not self:
            raise ScenarioError(f"{what}: expected a StreamRef returned by this scenario's s.sdk.create_stream")
        return stream


class _SdkRecorder:
    def __init__(self, rec: Recorder) -> None:
        self._r = rec

    def create_stream(self, schema: str, table: str = "cat.schema.table") -> StreamRef:
        if not isinstance(schema, str):
            raise ScenarioError("create_stream(): schema must be a schema name")
        ref = StreamRef(self._r, len(self._r.steps))
        self._r.steps.append(CreateStream(schema, table, ref))
        return ref

    def ingest(self, stream: StreamRef, record: dict) -> RecordRef:
        self._r._check_stream(stream, "ingest()")
        if not isinstance(record, dict):
            raise ScenarioError("ingest(): record must be a JSON-object dict")
        try:
            json.dumps(record)
        except (TypeError, ValueError) as e:
            raise ScenarioError(f"ingest(): record is not JSON-serializable: {e}") from e
        ref = RecordRef(self._r, len(self._r.steps), stream)
        self._r.steps.append(Ingest(stream, copy.deepcopy(record), ref))
        return ref

    def flush(self, stream: StreamRef) -> None:
        self._r._check_stream(stream, "flush()")
        self._r.steps.append(Flush(stream))

    def close(self, stream: StreamRef) -> None:
        self._r._check_stream(stream, "close()")
        self._r.steps.append(Close(stream))

    def await_write(self, rec: RecordRef, timeout_ms: int = 10000) -> None:
        if not isinstance(rec, RecordRef) or rec.owner is not self._r:
            raise ScenarioError("await_write(): expected a RecordRef returned by this scenario's s.sdk.ingest")
        if not isinstance(timeout_ms, int) or timeout_ms <= 0:
            raise ScenarioError("await_write(): timeout_ms must be a positive int")
        self._r.steps.append(AwaitWrite(rec, timeout_ms))


class _MockRecorder:
    def __init__(self, rec: Recorder) -> None:
        self._r = rec

    def inject_stream_error(self, after_offset: int = -1, retryable: bool = True) -> None:
        """Action (not an assertion): arm a one-shot stream abort. Call before create_stream."""
        if not isinstance(after_offset, int) or isinstance(after_offset, bool):
            raise ScenarioError("inject_stream_error(): after_offset must be an int")
        if not isinstance(retryable, bool):
            raise ScenarioError("inject_stream_error(): retryable must be a bool")
        self._r.steps.append(MockInject(after_offset, retryable))

    def _stream_target(self, query: str, stream: Optional[StreamRef], label: str, required: bool = False) -> Target:
        if stream is None and required:
            raise ScenarioError(f"{label}() requires a StreamRef")
        if stream is not None:
            self._r._check_stream(stream, label)
        return Target(query, {"stream_id": stream}, label)

    def call_count(self) -> Target:
        return Target("call_count", {}, "mock.call_count()")

    def captured_message_count(self) -> Target:
        return Target("captured_message_count", {}, "mock.captured_message_count()")

    def on_completed_count(self, stream: Optional[StreamRef] = None) -> Target:
        return self._stream_target("on_completed_count", stream, "mock.on_completed_count")

    def cancel_count(self, stream: Optional[StreamRef] = None) -> Target:
        return self._stream_target("cancel_count", stream, "mock.cancel_count")

    def captured_record_count(self, stream: StreamRef) -> Target:
        return self._stream_target("captured_record_count", stream, "mock.captured_record_count", required=True)

    def ack_offset(self, stream: StreamRef) -> Target:
        return self._stream_target("ack_offset", stream, "mock.ack_offset", required=True)

    def captured_record_bytes(self, stream: StreamRef, offset: Any) -> Target:
        self._r._check_stream(stream, "captured_record_bytes()")
        if isinstance(offset, RecordRef):
            offset = offset.offset
        if isinstance(offset, OffsetRef):
            if offset.owner is not self._r:
                raise ScenarioError("captured_record_bytes(): offset ref belongs to another scenario")
        elif not isinstance(offset, int):
            raise ScenarioError("captured_record_bytes(): offset must be r.offset or an int")
        return Target("captured_record_bytes", {"stream_id": stream, "offset": offset}, "mock.captured_record_bytes")


# --------------------------------------------------------------------------- scenario + registry


@dataclass
class Scenario:
    name: str
    fn: Callable[[Recorder], None]
    applies: Optional[Gate]
    _recorder: Optional[Recorder] = None

    def recording(self) -> Recorder:
        """Run the scenario body once (combo-blind) and cache the recorded steps."""
        if self._recorder is None:
            rec = Recorder()
            try:
                self.fn(rec)
            except ScenarioError:
                raise
            except Exception as e:
                raise ScenarioError(f"scenario '{self.name}' failed while recording: {e!r}") from e
            self._recorder = rec
        return self._recorder

    @property
    def steps(self) -> list:
        return self.recording().steps

    def schema_names(self) -> list[str]:
        return sorted({s.schema for s in self.steps if isinstance(s, CreateStream)})


REGISTRY: dict[str, Scenario] = {}


def scenario(applies: Optional[Gate] = None):
    """Register a scenario function. Does not execute the body."""
    if applies is not None and not isinstance(applies, Gate):
        raise ScenarioError("scenario(applies=...) must be a Gate (e.g. formats('proto')) or None")

    def decorate(fn: Callable[[Recorder], None]) -> Callable[[Recorder], None]:
        existing = REGISTRY.get(fn.__name__)
        if existing is not None and (existing.fn.__module__, existing.fn.__qualname__) != (fn.__module__, fn.__qualname__):
            raise ScenarioError(f"duplicate scenario name '{fn.__name__}'")
        REGISTRY[fn.__name__] = Scenario(fn.__name__, fn, applies)
        return fn

    return decorate


# --------------------------------------------------------------------------- validator


def schema_formats_for(sc: Scenario) -> frozenset:
    """Formats every schema used by the scenario ships artifacts for (format implied by schema)."""
    names = sc.schema_names()
    if not names:
        return KNOWN_FORMATS
    out = supported_formats(names[0])
    for name in names[1:]:
        out = out & supported_formats(name)
    return out


def static_admitted_tuples(sc: Scenario) -> list[tuple]:
    """(sdk, transport, format) tuples that pass gates 1-3 across the full static capability table."""
    schema_formats = schema_formats_for(sc)
    out = []
    for sdk in CAPABILITIES:
        for transport, fmt in valid_tuples():
            if admits(sdk, transport, fmt, sc.applies, schema_formats) is None:
                out.append((sdk, transport, fmt))
    return out


def validate(sc: Scenario) -> list[str]:
    """Load-time checks. Returns problems; empty list means the scenario is well-formed."""
    problems: list[str] = []
    try:
        rec = sc.recording()
    except ScenarioError as e:
        return [str(e)]
    owner = rec

    produced: set[int] = set()
    closed: set[int] = set()
    n_create = 0
    n_expect = 0

    def bad_offset(ref: Any, idx: int, what: str) -> None:
        if not isinstance(ref, OffsetRef):
            problems.append(f"step {idx}: {what} is not an OffsetRef")
        elif ref.owner is not owner:
            problems.append(f"step {idx}: {what} belongs to another scenario")
        elif id(ref.record) not in produced:
            problems.append(f"step {idx}: {what} refers to a record that is not ingested yet")

    def bad_ref(ref: Any, kind: type, idx: int, what: str) -> bool:
        if not isinstance(ref, kind):
            problems.append(f"step {idx}: {what} is not a {kind.__name__}")
            return True
        if ref.owner is not owner:
            problems.append(f"step {idx}: {what} belongs to another scenario")
            return True
        if id(ref) not in produced:
            problems.append(f"step {idx}: {what} used before it is produced")
            return True
        return False

    for idx, step in enumerate(rec.steps):
        if isinstance(step, CreateStream):
            n_create += 1
            try:
                supported_formats(step.schema)
            except InfraError as e:
                problems.append(f"step {idx}: {e}")
            produced.add(id(step.out))
        elif isinstance(step, Ingest):
            if not bad_ref(step.stream, StreamRef, idx, "ingest stream"):
                if id(step.stream) in closed:
                    problems.append(f"step {idx}: ingest after close")
            produced.add(id(step.out))
        elif isinstance(step, Flush):
            bad_ref(step.stream, StreamRef, idx, "flush stream")
        elif isinstance(step, Close):
            if not bad_ref(step.stream, StreamRef, idx, "close stream"):
                closed.add(id(step.stream))
        elif isinstance(step, AwaitWrite):
            bad_ref(step.rec, RecordRef, idx, "await_write record")
        elif isinstance(step, Expect):
            n_expect += 1
            t = step.target
            v = t.args.get("stream_id")
            if v is not None:
                bad_ref(v, StreamRef, idx, f"{t.label} stream")
            if isinstance(t.args.get("offset"), OffsetRef):
                bad_offset(t.args["offset"], idx, f"{t.label} offset")
            for eq in (step.equals, step.at_least):
                if isinstance(eq, OffsetRef):
                    bad_offset(eq, idx, "expected offset")
                elif isinstance(eq, StreamRef):
                    bad_ref(eq, StreamRef, idx, "expected stream")
                elif isinstance(eq, GoldenRef):
                    try:
                        expected_bytes(load_schema(eq.schema), eq.record, eq.fmt)
                    except Exception as e:  # report every bad golden, don't crash the load
                        problems.append(f"step {idx}: golden({eq.schema!r}, {eq.fmt!r}) cannot be encoded: {e}")

    if n_create == 0:
        problems.append("scenario never calls s.sdk.create_stream")
    if n_expect == 0:
        problems.append("scenario has no s.expect(...) assertion")
    if not problems and not static_admitted_tuples(sc):
        problems.append("gate admits no (sdk, transport, format) tuple in the capability table (dead or mistyped gate)")
    return problems


def load_scenarios(directory: Path) -> dict[str, Scenario]:
    """Import every ``*.py`` scenario file (registration only; bodies are not run) and return the registry."""
    if not directory.is_dir():
        raise ScenarioError(f"scenarios directory not found: {directory}")
    for path in sorted(directory.glob("*.py")):
        if path.name.startswith("_"):
            continue
        spec = importlib.util.spec_from_file_location(f"conformance_scenarios.{path.stem}", path)
        if spec is None or spec.loader is None:
            raise ScenarioError(f"cannot import scenario file {path}")
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
        except ScenarioError:
            raise
        except Exception as e:
            raise ScenarioError(f"error importing {path}: {e!r}") from e
    return dict(REGISTRY)


def validate_or_raise(sc: Scenario) -> None:
    problems = validate(sc)
    if problems:
        raise ScenarioError(f"scenario '{sc.name}' is invalid:\n  - " + "\n  - ".join(problems))


# --------------------------------------------------------------------------- executor


@dataclass(frozen=True)
class Check:
    description: str
    passed: bool
    expected: Any = None
    actual: Any = None


class Executor:
    """Walks a recorded step list for one concrete (sdk, transport, format).

    ``harness`` and ``mock`` are duck-typed to ``HarnessClient`` / ``MockAdminClient``; tests pass
    fakes with the same method names.
    """

    def __init__(self, *, harness, mock, transport: str, fmt: str, server_endpoint: str):
        self._h = harness
        self._m = mock
        self._transport = transport
        self._fmt = fmt
        self._endpoint = server_endpoint
        self._streams: dict[int, str] = {}
        self._offsets: dict[int, int] = {}

    async def run(self, steps: Iterable) -> list[Check]:
        """Execute all steps in order. Assertion misses are collected; infra errors raise InfraError."""
        checks: list[Check] = []
        for step in steps:
            if isinstance(step, CreateStream):
                await self._create(step)
            elif isinstance(step, Ingest):
                # Queue-only: never wait for an ack here (DESIGN §5.2 performance rule).
                sid = self._stream(step.stream)
                off = await self._h.ingest_record(sid, json.dumps(step.record, sort_keys=True))
                self._offsets[step.out.index] = off
            elif isinstance(step, Flush):
                await self._h.flush(self._stream(step.stream))
            elif isinstance(step, Close):
                await self._h.close_stream(self._stream(step.stream))
            elif isinstance(step, AwaitWrite):
                sid = self._stream(step.rec.stream)
                err = await self._h.await_write_completed(sid, self._offset(step.rec), step.timeout_ms)
                checks.append(Check(
                    f"await_write(record@{step.rec.index}) acknowledged",
                    passed=(err == ""),
                    actual=err or "ok",
                ))
            elif isinstance(step, MockInject):
                await self._m.inject_stream_error(step.after_offset, step.retryable)
            elif isinstance(step, Expect):
                actual = await self._query(step.target)
                if step.at_least is not None:
                    expected = self._resolve_expected(step.at_least)
                    checks.append(Check(
                        f"{step.target.label} >= {show_value(expected)}",
                        passed=(actual >= expected),
                        expected=expected,
                        actual=actual,
                    ))
                else:
                    expected = self._resolve_expected(step.equals)
                    checks.append(Check(
                        f"{step.target.label} == {show_value(expected)}",
                        passed=(actual == expected),
                        expected=expected,
                        actual=actual,
                    ))
            else:
                raise ScenarioError(f"unknown step type {type(step).__name__}")
        return checks

    async def _create(self, step: CreateStream) -> None:
        art = load_schema(step.schema)
        sid = await self._h.create_stream(
            protocol=self._transport,
            fmt=self._fmt,
            server_endpoint=self._endpoint,
            table_path=step.table,
            descriptor_proto_bytes=art.descriptor_proto_bytes if self._fmt == "proto" else b"",
            avro_schema_json=art.avro_schema_json if self._fmt == "avro" else "",
            arrow_schema_ipc=art.arrow_schema_ipc if self._fmt == "arrow" else b"",
        )
        self._streams[step.out.index] = sid

    async def _query(self, target: Target) -> Any:
        args = {}
        for k, v in target.args.items():
            args[k] = self._resolve(v)
        if "stream_id" in args and args["stream_id"] is None:
            args["stream_id"] = ""  # empty = whole mock
        return await getattr(self._m, target.query)(**args)

    def _resolve(self, v: Any) -> Any:
        if isinstance(v, StreamRef):
            return self._stream(v)
        if isinstance(v, OffsetRef):
            return self._offset(v.record)
        return v

    def _resolve_expected(self, v: Any) -> Any:
        if isinstance(v, GoldenRef):
            return expected_bytes(load_schema(v.schema), v.record, v.fmt)
        return self._resolve(v)

    def _stream(self, ref: StreamRef) -> str:
        try:
            return self._streams[ref.index]
        except KeyError:
            raise ScenarioError(f"stream step {ref.index} was not executed before use") from None

    def _offset(self, rec: RecordRef) -> int:
        try:
            return self._offsets[rec.index]
        except KeyError:
            raise ScenarioError(f"record step {rec.index} was not ingested before use") from None


def show_value(v: Any) -> str:
    if isinstance(v, (bytes, bytearray)):
        head = bytes(v[:32]).hex()
        return f"<{len(v)} bytes {head}{'...' if len(v) > 32 else ''}>"
    return repr(v)
