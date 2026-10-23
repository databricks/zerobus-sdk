"""Async gRPC clients for the two control planes (TestHarness and MockAdmin).

Method names mirror the step executor's vocabulary. Every RPC carries a deadline and any
transport failure is re-raised as ``InfraError`` so the runner reports ERROR, not FAIL.
"""

from __future__ import annotations



import grpc

from zerobus_test_runner._generated import mock_admin_pb2 as admin_pb
from zerobus_test_runner._generated import mock_admin_pb2_grpc as admin_grpc
from zerobus_test_runner._generated import test_harness_pb2 as harness_pb
from zerobus_test_runner._generated import test_harness_pb2_grpc as harness_grpc
from zerobus_test_runner.errors import InfraError

DEFAULT_RPC_TIMEOUT_S = 60.0

_PROTOCOL = {"grpc": "GRPC", "arrow_flight": "ARROW_FLIGHT"}
_FORMAT = {"proto": "PROTO", "json": "JSON", "avro": "AVRO", "arrow": "ARROW"}



async def _call(coro, what: str):
    try:
        return await coro
    except grpc.aio.AioRpcError as e:
        raise InfraError(f"{what} failed: {e.code().name}: {e.details()}") from e


class HarnessClient:
    """Client for ``zerobus.conformance.harness.TestHarness``."""

    def __init__(self, endpoint: str, rpc_timeout: float = DEFAULT_RPC_TIMEOUT_S):
        self._channel = grpc.aio.insecure_channel(endpoint)
        self._stub = harness_grpc.TestHarnessStub(self._channel)
        self._t = rpc_timeout

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
    ) -> str:
        req = harness_pb.CreateStreamRequest(
            protocol=harness_pb.Protocol.Value(_PROTOCOL[protocol]),
            format=harness_pb.RecordFormat.Value(_FORMAT[fmt]),
            server_endpoint=server_endpoint,
            table_path=table_path,
            descriptor_proto_bytes=descriptor_proto_bytes,
            avro_schema_json=avro_schema_json,
            arrow_schema_ipc=arrow_schema_ipc,
        )
        resp = await _call(self._stub.CreateStream(req, timeout=self._t), "harness.CreateStream")
        return resp.stream_id

    async def ingest_record(self, stream_id: str, record_json: str) -> int:
        req = harness_pb.IngestRecordRequest(stream_id=stream_id, record_json=record_json)
        resp = await _call(self._stub.IngestRecord(req, timeout=self._t), "harness.IngestRecord")
        return resp.offset

    async def await_write_completed(self, stream_id: str, offset: int, timeout_ms: int) -> str:
        """Returns the harness error message; empty string means the write was acknowledged."""
        req = harness_pb.AwaitRequest(stream_id=stream_id, offset=offset, timeout_ms=timeout_ms)
        resp = await _call(
            self._stub.AwaitWriteCompleted(req, timeout=timeout_ms / 1000 + self._t),
            "harness.AwaitWriteCompleted",
        )
        return resp.error_message

    async def flush(self, stream_id: str) -> None:
        req = harness_pb.FlushRequest(stream_id=stream_id)
        await _call(self._stub.Flush(req, timeout=self._t), "harness.Flush")

    async def close_stream(self, stream_id: str) -> None:
        req = harness_pb.CloseRequest(stream_id=stream_id)
        await _call(self._stub.Close(req, timeout=self._t), "harness.Close")

    async def shutdown(self) -> None:
        await _call(self._stub.Shutdown(harness_pb.Empty(), timeout=self._t), "harness.Shutdown")

    async def aclose(self) -> None:
        await self._channel.close()


class MockAdminClient:
    """Client for ``zerobus.conformance.mock.MockAdmin``.

    ``stream_id=""`` means "the whole mock" (first-slice simplification).
    """

    def __init__(self, endpoint: str, rpc_timeout: float = DEFAULT_RPC_TIMEOUT_S):
        self._channel = grpc.aio.insecure_channel(endpoint)
        self._stub = admin_grpc.MockAdminStub(self._channel)
        self._t = rpc_timeout

    async def _count(self, rpc: str, stream_id: str = "") -> int:
        method = getattr(self._stub, rpc)
        resp = await _call(method(admin_pb.StreamQuery(stream_id=stream_id or ""), timeout=self._t), f"mock.{rpc}")
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

    async def ack_offset(self, stream_id: str = "") -> int:
        resp = await _call(
            self._stub.GetAckOffset(admin_pb.StreamQuery(stream_id=stream_id), timeout=self._t),
            "mock.GetAckOffset",
        )
        return resp.offset

    async def captured_record_bytes(self, stream_id: str, offset: int) -> bytes:
        req = admin_pb.RecordQuery(stream_id=stream_id, offset=offset)
        resp = await _call(self._stub.GetCapturedRecordBytes(req, timeout=self._t), "mock.GetCapturedRecordBytes")
        return resp.record_bytes

    async def inject_stream_error(self, after_offset: int = -1, retryable: bool = True) -> None:
        req = admin_pb.InjectStreamErrorRequest(after_offset=after_offset, retryable=retryable)
        await _call(self._stub.InjectStreamError(req, timeout=self._t), "mock.InjectStreamError")

    async def reset(self) -> None:
        await _call(self._stub.Reset(admin_pb.Empty(), timeout=self._t), "mock.Reset")

    async def shutdown(self) -> None:
        await _call(self._stub.Shutdown(admin_pb.Empty(), timeout=self._t), "mock.Shutdown")

    async def aclose(self) -> None:
        await self._channel.close()
