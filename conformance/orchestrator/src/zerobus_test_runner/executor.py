"""Step executor: walks a recorded scenario for one concrete (sdk, transport, format).

harness and mock are duck-typed to HarnessClient / MockAdminClient; tests pass fakes with
the same method names.

Error handling splits by origin. A harness or mock RPC failure raises (HarnessError /
MockError) and the runner reports ERROR. An error the SDK under test reported comes back
in-band as an SdkFault: an unexpected one fails a check, an expected terminal one satisfies
expect_terminal_failure. Either way the stream is then dead and later actions on it skip.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable, Optional

from zerobus_test_runner.clients import DEFAULT_RPC_TIMEOUT_S, SdkFault
from zerobus_test_runner.data.schemas import load_schema
from zerobus_test_runner.scenario import (
    AwaitWrite,
    Close,
    CreateStream,
    Expect,
    ExpectTerminalFailure,
    Flush,
    Ingest,
    IngestMany,
    MockInject,
    OffsetRef,
    RecordRef,
    ScenarioError,
    StreamRef,
    Target,
)


@dataclass(frozen=True)
class Check:
    description: str
    passed: bool
    expected: Any = None
    actual: Any = None


class Executor:
    """Walks a recorded step list for one concrete (sdk, transport, format)."""

    def __init__(self, *, harness, mock, transport: str, fmt: str, server_endpoint: str):
        self._h = harness
        self._m = mock
        self._transport = transport
        self._fmt = fmt
        self._endpoint = server_endpoint
        self._streams: dict[int, str] = {}  # StreamRef.index -> harness stream id
        self._offsets: dict[int, int] = {}  # id(RecordRef) -> offset
        self._closed: set[int] = set()  # StreamRef.index values the scenario closed
        self._terminal_expected: set[int] = set()
        self._faults: dict[int, SdkFault] = {}  # StreamRef.index -> the SDK fault seen
        self._create_failed: set[int] = set()  # streams whose create_stream itself faulted
        self._checks: list[Check] = []

    @property
    def checks(self) -> list[Check]:
        """Checks recorded so far. Readable after run() raises, so the runner keeps partial results."""
        return self._checks

    async def run(self, steps: Iterable) -> list[Check]:
        """Execute all steps in order. Assertion misses become checks; infra faults raise."""
        steps = list(steps)
        self._terminal_expected = {s.stream.index for s in steps if isinstance(s, ExpectTerminalFailure)}
        try:
            for step in steps:
                await self._exec(step)
        finally:
            await self._close_open_streams()
        return self._checks

    async def _exec(self, step) -> None:
        match step:
            case CreateStream():
                art = load_schema(step.schema)
                sid, fault = await self._h.create_stream(
                    protocol=self._transport,
                    fmt=self._fmt,
                    server_endpoint=self._endpoint,
                    table_path=step.table,
                    descriptor_proto_bytes=art.descriptor_proto_bytes if self._fmt == "proto" else b"",
                    avro_schema_json=art.avro_schema_json if self._fmt == "avro" else "",
                    arrow_schema_ipc=art.arrow_schema_ipc if self._fmt == "arrow" else b"",
                )
                if fault is not None:
                    # A create failure is ALWAYS unexpected: the injected abort fires on records,
                    # not on create, so it must never satisfy expect_terminal_failure.
                    self._faults[step.out.index] = fault
                    self._create_failed.add(step.out.index)
                    self._checks.append(Check(f"stream@{step.out.index} created", passed=False, actual=fault.message))
                    return
                self._streams[step.out.index] = sid
            case Ingest():
                if self._dead(step.stream.index):
                    return
                off, fault = await self._h.ingest_record(self._stream(step.stream), self._json(step.record))
                if self._record_fault(step.stream.index, fault):
                    return
                self._offsets[id(step.out)] = off
            case IngestMany():
                if self._dead(step.stream.index):
                    return
                payloads = [self._json(r) for r in step.records]
                offset, fault = await self._h.ingest_records(self._stream(step.stream), payloads)
                if self._record_fault(step.stream.index, fault):
                    return
                self._offsets[id(step.out)] = offset
            case Flush():
                if self._dead(step.stream.index):
                    return
                self._record_fault(step.stream.index, await self._h.flush(self._stream(step.stream)))
            case Close():
                if self._dead(step.stream.index):
                    return
                self._record_fault(step.stream.index, await self._h.close_stream(self._stream(step.stream)))
                self._closed.add(step.stream.index)
            case AwaitWrite():
                if self._dead(step.rec.stream.index):
                    return
                fault = await self._h.await_write_completed(self._stream(step.rec.stream), self._offset(step.rec))
                self._record_fault(step.rec.stream.index, fault)
            case MockInject():
                await self._m.inject_stream_error(after_offset=step.after_offset, retryable=step.retryable)
            case ExpectTerminalFailure():
                idx = step.stream.index
                fault = self._faults.get(idx)
                # A create-time fault does not count: the terminal failure must come from the
                # injected abort on an ingest/flush/close, not from stream creation.
                ok = fault is not None and not fault.retryable and idx not in self._create_failed
                self._checks.append(
                    Check(
                        f"stream@{idx} fails terminally (non-retryable)",
                        passed=ok,
                        actual=fault.message if fault else "no terminal error",
                    )
                )
            case Expect():
                await self._expect(step)
            case _:
                raise ScenarioError(f"unknown step type {type(step).__name__}")

    def _dead(self, idx: int) -> bool:
        return idx in self._faults

    def _record_fault(self, idx: int, fault: Optional[SdkFault]) -> bool:
        """Record an SDK fault on a stream. Returns True if one was present (the stream is
        now dead). An unexpected fault also fails a check; an expected one is left for the
        ExpectTerminalFailure step."""
        if fault is None:
            return False
        self._faults[idx] = fault
        if idx not in self._terminal_expected:
            self._checks.append(Check(f"stream@{idx} has no unexpected SDK error", passed=False, actual=fault.message))
        return True

    async def _close_open_streams(self) -> None:
        """Close every stream this run opened and the scenario did not already close, so none
        outlives the run in the reused harness. Faulted streams are closed too: the harness only
        drops a stream from its map on Close, and closing a dead stream returns at once. Best-effort
        with a short deadline (a hung harness must not add 600 s per stream); errors are ignored."""
        for idx, sid in self._streams.items():
            if idx in self._closed:
                continue
            try:
                await self._h.close_stream(sid, timeout=DEFAULT_RPC_TIMEOUT_S)
            except Exception:
                pass

    async def _expect(self, step: Expect) -> None:
        dead_msg = self._dead_dependency(step)
        if dead_msg is not None:
            # An assertion on a record whose stream already failed cannot be evaluated: fail the
            # check with the SDK's message rather than raising (which would become ERROR and drop it).
            self._checks.append(Check(f"{step.target.label}: stream faulted", passed=False, actual=dead_msg))
            return
        actual = await self._query(step.target)
        if step.at_least is not None:
            expected = self._resolve(step.at_least)
            self._checks.append(
                Check(f"{step.target.label} >= {show_value(expected)}", actual >= expected, expected, actual)
            )
        else:
            expected = self._resolve(step.equals)
            self._checks.append(
                Check(f"{step.target.label} == {show_value(expected)}", actual == expected, expected, actual)
            )

    @staticmethod
    def _json(record: dict) -> str:
        return json.dumps(record, sort_keys=True)

    async def _query(self, target: Target) -> Any:
        args = {}
        for k, v in target.args.items():
            # The mock counts globally today; send global regardless of the stream ref until
            # the mock filters by stream (see issue #1068). The per-stream DSL is the seam.
            args[k] = "" if k == "stream_id" else self._resolve(v)
        return await getattr(self._m, target.query)(**args)

    def _dead_dependency(self, step: Expect) -> Optional[str]:
        """The SDK fault message if the expect depends on an offset from a stream that already
        failed (so the offset was never resolved), else None."""
        for v in (*step.target.args.values(), step.equals, step.at_least):
            if isinstance(v, OffsetRef) and id(v.record) not in self._offsets and self._dead(v.record.stream.index):
                return self._faults[v.record.stream.index].message
        return None

    def _resolve(self, v: Any) -> Any:
        if isinstance(v, OffsetRef):
            return self._offset(v.record)
        return v

    def _stream(self, ref: StreamRef) -> str:
        try:
            return self._streams[ref.index]
        except KeyError:
            raise ScenarioError(f"stream step {ref.index} was not executed before use") from None

    def _offset(self, rec: RecordRef) -> int:
        try:
            return self._offsets[id(rec)]
        except KeyError:
            raise ScenarioError(f"record step {rec.index} was not ingested before use") from None


def show_value(v: Any) -> str:
    if isinstance(v, (bytes, bytearray)):
        head = bytes(v[:32]).hex()
        return f"<{len(v)} bytes {head}{'...' if len(v) > 32 else ''}>"
    return repr(v)
