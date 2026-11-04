"""Async gRPC clients for the two control planes (TestHarness and MockAdmin).

Method names mirror the step executor's vocabulary. A transport failure is re-raised as a
typed error that names the failing side (HarnessError / MockError) so the runner reports
ERROR. An error the SDK under test reported is returned in-band as an SdkFault, not raised.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import grpc

from zerobus_test_runner._generated import mock_admin_pb2 as admin_pb
from zerobus_test_runner._generated import mock_admin_pb2_grpc as admin_grpc
from zerobus_test_runner._generated import test_harness_pb2 as harness_pb
from zerobus_test_runner._generated import test_harness_pb2_grpc as harness_grpc
from zerobus_test_runner.errors import HarnessError, MockError

# Quick calls (create, ingest, mock queries) use the short deadline. Calls that block on the
# SDK's own acknowledgment (await, flush, close) use the long one, so the orchestrator never
# cuts the SDK off before its own ack/flush timeout fires.
DEFAULT_RPC_TIMEOUT_S = 60.0
WAIT_RPC_TIMEOUT_S = 600.0

# The mock accepts records up to 10 MiB; gRPC's 4 MiB receive default would truncate a large
# captured record. Raise both directions on every channel.
_CHANNEL_OPTIONS = [
    ("grpc.max_receive_message_length", 16 << 20),
    ("grpc.max_send_message_length", 16 << 20),
]


@dataclass(frozen=True)
class SdkFault:
    """An error the SDK under test reported in-band. Distinct from a harness or mock failure."""

    message: str
    retryable: bool


def _fault(resp) -> Optional[SdkFault]:
    """The SdkError the harness set on a rejected action, or None on success.

    Presence (HasField), not message text, marks a failure: an SDK error with an empty message is
    still a failure (some SDKs raise with a null/empty message).
    """
    if not resp.HasField("error"):
        return None
    return SdkFault(resp.error.message or "<empty SDK error>", resp.error.retryable)


async def _call(coro, what: str, err_cls):
    try:
        return await coro
    except grpc.aio.AioRpcError as e:
        raise err_cls(f"{what} failed: {e.code().name}: {e.details()}") from e


class HarnessClient:
    """Client for ``zerobus.conformance.harness.TestHarness``.

    Each action returns its payload paired with an optional SdkFault; a gRPC status instead
    raises HarnessError.
    """

    def __init__(self, endpoint: str, rpc_timeout: float = DEFAULT_RPC_TIMEOUT_S):
        self._channel = grpc.aio.insecure_channel(endpoint, options=_CHANNEL_OPTIONS)
        self._stub = harness_grpc.TestHarnessStub(self._channel)
        self._t = rpc_timeout
        self._wait_t = max(WAIT_RPC_TIMEOUT_S, rpc_timeout)

    async def _call(self, coro, what: str):
        return await _call(coro, what, HarnessError)

    async def create_stream(
        self,
        *,
        protocol: str,
        fmt: str,
        server_endpoint: str,
        table_path: str,
        descriptor_proto_bytes: bytes = b"",
        avro_schema_json: str = "",
        arrow_schema_ipc: bytes = b"",
    ) -> tuple[str, Optional[SdkFault]]:
        req = harness_pb.CreateStreamRequest(
            # Enum values are prefixed (PROTOCOL_GRPC, RECORD_FORMAT_PROTO); Value() rejects an unknown name.
            protocol=harness_pb.Protocol.Value(f"PROTOCOL_{protocol.upper()}"),
            format=harness_pb.RecordFormat.Value(f"RECORD_FORMAT_{fmt.upper()}"),
            server_endpoint=server_endpoint,
            table_path=table_path,
            descriptor_proto_bytes=descriptor_proto_bytes,
            avro_schema_json=avro_schema_json,
            arrow_schema_ipc=arrow_schema_ipc,
        )
        resp = await self._call(self._stub.CreateStream(req, timeout=self._t), "harness.CreateStream")
        return resp.stream_id, _fault(resp)

    async def ingest_record(self, stream_id: str, record_json: str) -> tuple[int, Optional[SdkFault]]:
        req = harness_pb.IngestRecordRequest(stream_id=stream_id, record_json=record_json)
        resp = await self._call(self._stub.IngestRecord(req, timeout=self._t), "harness.IngestRecord")
        return resp.offset, _fault(resp)

    async def ingest_records(self, stream_id: str, records_json: list[str]) -> tuple[int, Optional[SdkFault]]:
        req = harness_pb.IngestRecordsRequest(stream_id=stream_id, record_json=records_json)
        resp = await self._call(self._stub.IngestRecords(req, timeout=self._t), "harness.IngestRecords")
        return resp.offset, _fault(resp)

    async def await_write_completed(self, stream_id: str, offset: int) -> Optional[SdkFault]:
        req = harness_pb.AwaitRequest(stream_id=stream_id, offset=offset)
        resp = await self._call(
            self._stub.AwaitWriteCompleted(req, timeout=self._wait_t), "harness.AwaitWriteCompleted"
        )
        return _fault(resp)

    async def flush(self, stream_id: str) -> Optional[SdkFault]:
        resp = await self._call(
            self._stub.Flush(harness_pb.FlushRequest(stream_id=stream_id), timeout=self._wait_t), "harness.Flush"
        )
        return _fault(resp)

    async def close_stream(self, stream_id: str, timeout: Optional[float] = None) -> Optional[SdkFault]:
        resp = await self._call(
            self._stub.Close(harness_pb.CloseRequest(stream_id=stream_id), timeout=timeout or self._wait_t),
            "harness.Close",
        )
        return _fault(resp)

    async def shutdown(self) -> None:
        await self._call(self._stub.Shutdown(harness_pb.Empty(), timeout=self._t), "harness.Shutdown")

    async def aclose(self) -> None:
        await self._channel.close()


class MockAdminClient:
    """Client for ``zerobus.conformance.mock.MockAdmin``.

    Counters are global; an empty stream_id means "the whole mock".
    """

    def __init__(self, endpoint: str, rpc_timeout: float = DEFAULT_RPC_TIMEOUT_S):
        self._channel = grpc.aio.insecure_channel(endpoint, options=_CHANNEL_OPTIONS)
        self._stub = admin_grpc.MockAdminStub(self._channel)
        self._t = rpc_timeout

    async def _call(self, coro, what: str):
        return await _call(coro, what, MockError)

    async def _count(self, rpc: str, stream_id: str = "") -> int:
        resp = await self._call(
            getattr(self._stub, rpc)(admin_pb.StreamQuery(stream_id=stream_id), timeout=self._t), f"mock.{rpc}"
        )
        return resp.count

    async def call_count(self, stream_id: str = "") -> int:
        return await self._count("GetCallCount", stream_id)

    async def captured_message_count(self, stream_id: str = "") -> int:
        return await self._count("GetCapturedMessageCount", stream_id)

    async def on_completed_count(self, stream_id: str = "") -> int:
        return await self._count("GetOnCompletedCount", stream_id)

    async def cancel_count(self, stream_id: str = "") -> int:
        return await self._count("GetCancelCount", stream_id)

    async def captured_record_count(self, stream_id: str = "") -> int:
        return await self._count("GetCapturedRecordCount", stream_id)

    async def abort_count(self, stream_id: str = "") -> int:
        return await self._count("GetAbortCount", stream_id)

    async def ack_offset(self, stream_id: str = "") -> int:
        resp = await self._call(
            self._stub.GetAckOffset(admin_pb.StreamQuery(stream_id=stream_id), timeout=self._t), "mock.GetAckOffset"
        )
        return resp.offset

    async def captured_record_bytes(self, stream_id: str, offset: int) -> Optional[bytes]:
        """The bytes captured at an offset, or None if the mock captured nothing there. A missing
        record is the SDK failing to deliver (a FAIL for the scenario), not a mock/infra fault, so
        NOT_FOUND returns None rather than raising MockError."""
        req = admin_pb.RecordQuery(stream_id=stream_id, offset=offset)
        try:
            resp = await self._stub.GetCapturedRecordBytes(req, timeout=self._t)
        except grpc.aio.AioRpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                return None
            raise MockError(f"mock.GetCapturedRecordBytes failed: {e.code().name}: {e.details()}") from e
        return resp.record_bytes

    async def inject_stream_error(self, after_offset: int = -1, retryable: bool = True) -> None:
        req = admin_pb.InjectStreamErrorRequest(after_offset=after_offset, retryable=retryable)
        await self._call(self._stub.InjectStreamError(req, timeout=self._t), "mock.InjectStreamError")

    async def reset(self) -> None:
        await self._call(self._stub.Reset(admin_pb.Empty(), timeout=self._t), "mock.Reset")

    async def shutdown(self) -> None:
        await self._call(self._stub.Shutdown(admin_pb.Empty(), timeout=self._t), "mock.Shutdown")

    async def aclose(self) -> None:
        await self._channel.close()
