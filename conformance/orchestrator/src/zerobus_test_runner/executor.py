"""Step executor: walks a recorded scenario for one concrete (sdk, transport, format).

harness and mock are duck-typed to HarnessClient / MockAdminClient; tests pass fakes with the
same method names.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable

from zerobus_test_runner.data.schemas import load_schema
from zerobus_test_runner.errors import InfraError
from zerobus_test_runner.scenario import (
    AwaitWrite,
    Close,
    CreateStream,
    Expect,
    ExpectTerminalFailure,
    Flush,
    Ingest,
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
        self._streams: dict[int, str] = {}
        self._offsets: dict[int, int] = {}
        self._terminal_expected: set[int] = set()
        self._terminal_errors: dict[int, str] = {}

    async def run(self, steps: Iterable) -> list[Check]:
        """Execute all steps in order. Assertion misses are collected; infra errors raise InfraError."""
        steps = list(steps)
        # Streams a scenario expects to fail terminally: an action failure on them is the
        # expected outcome, not an infra error.
        self._terminal_expected = {s.stream.index for s in steps if isinstance(s, ExpectTerminalFailure)}
        checks: list[Check] = []
        for step in steps:
            if isinstance(step, CreateStream):
                await self._create(step)
            elif isinstance(step, Ingest):
                # Queue-only: never wait for an ack here (the ingest-then-flush rule).
                off = await self._action(
                    step.stream,
                    lambda step=step: self._h.ingest_record(
                        self._stream(step.stream), json.dumps(step.record, sort_keys=True)
                    ),
                )
                if off is not None:
                    self._offsets[step.out.index] = off
            elif isinstance(step, Flush):
                await self._action(step.stream, lambda step=step: self._h.flush(self._stream(step.stream)))
            elif isinstance(step, Close):
                await self._action(step.stream, lambda step=step: self._h.close_stream(self._stream(step.stream)))
            elif isinstance(step, AwaitWrite):
                if step.rec.stream.index in self._terminal_errors:
                    continue  # stream already failed terminally; nothing to await
                err = await self._action(
                    step.rec.stream,
                    lambda step=step: self._h.await_write_completed(
                        self._stream(step.rec.stream), self._offset(step.rec), step.timeout_ms
                    ),
                )
                if err is not None:
                    checks.append(
                        Check(
                            f"await_write(record@{step.rec.index}) acknowledged",
                            passed=(err == ""),
                            actual=err or "ok",
                        )
                    )
            elif isinstance(step, MockInject):
                await self._m.inject_stream_error(after_offset=step.after_offset, retryable=step.retryable)
            elif isinstance(step, ExpectTerminalFailure):
                err = self._terminal_errors.get(step.stream.index)
                checks.append(
                    Check(
                        f"stream@{step.stream.index} fails terminally",
                        passed=err is not None,
                        actual=err or "no terminal error",
                    )
                )
            elif isinstance(step, Expect):
                actual = await self._query(step.target)
                if step.at_least is not None:
                    expected = self._resolve(step.at_least)
                    checks.append(
                        Check(
                            f"{step.target.label} >= {show_value(expected)}",
                            passed=(actual >= expected),
                            expected=expected,
                            actual=actual,
                        )
                    )
                else:
                    expected = self._resolve(step.equals)
                    checks.append(
                        Check(
                            f"{step.target.label} == {show_value(expected)}",
                            passed=(actual == expected),
                            expected=expected,
                            actual=actual,
                        )
                    )
            else:
                raise ScenarioError(f"unknown step type {type(step).__name__}")
        return checks

    async def _action(self, stream_ref: StreamRef, make_coro):
        """Run one harness action. If the stream is expected to fail terminally, capture the
        InfraError as the expected outcome instead of raising, and skip once it has failed.
        (A non-expected failure still raises, so the runner reports ERROR.)"""
        idx = stream_ref.index
        if idx in self._terminal_errors:
            return None
        try:
            return await make_coro()
        except InfraError as e:
            if idx in self._terminal_expected:
                self._terminal_errors[idx] = str(e)
                return None
            raise

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
