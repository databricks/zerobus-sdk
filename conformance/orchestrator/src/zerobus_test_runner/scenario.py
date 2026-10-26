"""Scenario builder DSL and load-time validator.

A scenario function runs once, combo-blind, against a Recorder: action calls append steps and
return refs, and nothing talks to a server while recording. The runtime executes the recorded
step list later for one concrete (sdk, transport, format).
"""

from __future__ import annotations

import copy
import importlib.util
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from zerobus_test_runner.capability import CAPABILITIES, admits, valid_tuples

KNOWN_FORMATS = frozenset({"proto", "json", "avro", "arrow"})


class ScenarioError(Exception):
    """Author error: bad gate, bad ref, recording failure, failed validation."""


# --------------------------------------------------------------------------- gates


@dataclass(frozen=True)
class Gate:
    """Applicability selector. None in a dimension is a wildcard."""

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


def gate(*, formats=None, sdks=None, transports=None) -> Gate:
    """Applicability gate over any dimension(s); unspecified dimensions are wildcards.

    e.g. gate(formats=["arrow"], sdks=["rust", "python", "java"]).
    """
    if formats is not None:
        bad = [n for n in formats if n not in KNOWN_FORMATS]
        if bad:
            raise ScenarioError(f"unknown format(s) {bad}; known: {sorted(KNOWN_FORMATS)}")
    if sdks is not None:
        unknown = [s for s in sdks if s not in CAPABILITIES]
        if unknown:
            raise ScenarioError(f"unknown sdk(s) {unknown}; known: {sorted(CAPABILITIES)}")
    return Gate(
        formats=frozenset(formats) if formats is not None else None,
        sdks=frozenset(sdks) if sdks is not None else None,
        transports=frozenset(transports) if transports is not None else None,
    )


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
class Target:
    """A MockAdmin observation. query is the MockAdminClient method name."""

    query: str
    args: dict
    label: str


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
    """The s object handed to a scenario function."""

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
        if existing is not None and (existing.fn.__module__, existing.fn.__qualname__) != (
            fn.__module__,
            fn.__qualname__,
        ):
            raise ScenarioError(f"duplicate scenario name '{fn.__name__}'")
        REGISTRY[fn.__name__] = Scenario(fn.__name__, fn, applies)
        return fn

    return decorate


# --------------------------------------------------------------------------- validator


def static_admitted_tuples(sc: Scenario) -> list[tuple]:
    """(sdk, transport, format) tuples that pass gates 1-3 across the full static capability table."""
    out = []
    for sdk in CAPABILITIES:
        for transport, fmt in valid_tuples():
            if admits(sdk, transport, fmt, sc.applies, KNOWN_FORMATS) is None:
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

    if n_create == 0:
        problems.append("scenario never calls s.sdk.create_stream")
    if n_expect == 0:
        problems.append("scenario has no s.expect(...) assertion")
    if not problems and not static_admitted_tuples(sc):
        problems.append("gate admits no (sdk, transport, format) tuple in the capability table (dead or mistyped gate)")
    return problems


def load_scenarios(directory: Path) -> dict[str, Scenario]:
    """Import every scenario .py (registration only, bodies not run) and return the registry."""
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
