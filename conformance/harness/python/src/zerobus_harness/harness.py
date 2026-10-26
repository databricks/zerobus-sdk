"""``TestHarness`` service: maps each contract RPC onto the Python Zerobus SDK.

Wired: ``protocol=GRPC`` with ``format=PROTO`` or ``format=AVRO``, and
``protocol=ARROW_FLIGHT`` with ``format=ARROW``. Every other combination returns
``UNIMPLEMENTED`` with a TODO (see :func:`check_slice_supported`).
"""

import functools
import itertools
import logging
import threading

import grpc

from zerobus.sdk.shared import HeadersProvider
from zerobus.sdk.sync import TableProperties, ZerobusSdk

from . import records
from ._generated import test_harness_pb2 as pb
from ._generated import test_harness_pb2_grpc as pb_grpc

log = logging.getLogger("zerobus_harness")


class _NoAuthHeadersProvider(HeadersProvider):
    """Sends no authentication headers. The Python binding has no no-auth Arrow
    constructor, so this stands in for it (same as the Rust core's NoAuthHeadersProvider)."""

    def get_headers(self):
        return []


class _StreamEntry:
    """One open SDK stream plus the converter and ingest call for its records."""

    def __init__(self, sdk, stream, to_payload, ingest):
        # Keep the SDK alive for as long as the stream it created.
        self.sdk = sdk
        self.stream = stream
        # record_json (str) -> what the SDK ingests: a protobuf message (PROTO), a dict
        # (AVRO), or a pyarrow.RecordBatch (ARROW). The SDK does the encoding, never the harness.
        self.to_payload = to_payload
        # (stream, payload) -> offset. Queue-only; never waits for an ack.
        self.ingest = ingest
        # Serializes per-stream RPCs (ingest / flush / close). Await does not take it,
        # so a slow wait cannot block ingest.
        self.lock = threading.Lock()


def _create_sdk(server_endpoint):
    """The mock is a local plaintext server that checks no credentials, so the SDK runs
    with no TLS (http://) and no auth headers. Both are testing-only knobs; see
    python/rust/Cargo.toml `testing` feature."""
    return ZerobusSdk(f"http://{server_endpoint}", "", None, True, no_tls=True)


def _ingest_row(stream, payload):
    return stream.ingest_record_offset(payload)


def _ingest_batch(stream, payload):
    return stream.ingest_batch(payload)


def check_slice_supported(protocol, fmt):
    """Return an (grpc.StatusCode, message) error for unsupported pairs, else None.

    Each TODO names the work that remains.
    """
    if protocol == pb.GRPC and fmt in (pb.PROTO, pb.AVRO):
        return None
    if protocol == pb.ARROW_FLIGHT and fmt == pb.ARROW:
        return None
    if protocol == pb.GRPC and fmt == pb.JSON:
        return (
            grpc.StatusCode.UNIMPLEMENTED,
            "TODO(conformance): GRPC + JSON not implemented in the Python harness yet",
        )
    if protocol == pb.GRPC and fmt == pb.ARROW:
        return (
            grpc.StatusCode.UNIMPLEMENTED,
            "TODO(conformance): GRPC + ARROW is not a valid pairing; ARROW rides ARROW_FLIGHT",
        )
    if protocol == pb.ARROW_FLIGHT:
        return (grpc.StatusCode.INVALID_ARGUMENT, "ARROW_FLIGHT carries only the ARROW format")
    return (grpc.StatusCode.INVALID_ARGUMENT, "protocol and format must be specified")


class HarnessService(pb_grpc.TestHarnessServicer):
    def __init__(self, shutdown_event: threading.Event):
        self._streams = {}  # stream_id -> _StreamEntry
        self._streams_lock = threading.Lock()  # guards the map only
        self._ids = itertools.count()
        self._shutdown = shutdown_event

    def _entry(self, context, stream_id):
        with self._streams_lock:
            entry = self._streams.get(stream_id)
        if entry is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"unknown stream_id '{stream_id}'")
        return entry

    def CreateStream(self, request, context):
        err = check_slice_supported(request.protocol, request.format)
        if err is not None:
            context.abort(*err)

        log.info(
            "CreateStream protocol=%s endpoint=%s table=%s",
            pb.Protocol.Name(request.protocol),
            request.server_endpoint,
            request.table_path,
        )
        if request.protocol == pb.ARROW_FLIGHT:
            entry = self._open_arrow_stream(request, context)
        else:
            entry = self._open_grpc_stream(request, context)

        with self._streams_lock:
            stream_id = f"stream-{next(self._ids)}"
            self._streams[stream_id] = entry
        return pb.CreateStreamResponse(stream_id=stream_id)

    def _open_grpc_stream(self, request, context):
        # Schema artifact per format. Both converters only build the SDK's input; the
        # SDK does the encoding.
        if request.format == pb.AVRO:
            try:
                to_payload = records.AvroRecordBuilder(request.avro_schema_json)
            except ValueError as e:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
            table_kwargs = {"avro_schema": request.avro_schema_json}
        else:
            try:
                resolved = records.descriptor_from_bytes(request.descriptor_proto_bytes)
            except ValueError as e:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
            to_payload = functools.partial(records.record_from_json, resolved.message_class)
            table_kwargs = {"descriptor_proto": resolved.file_descriptor_bytes}

        try:
            sdk = _create_sdk(request.server_endpoint)
            stream = sdk.create_stream(
                table_properties=TableProperties(request.table_path, **table_kwargs),
                no_auth=True,
            )
        except ValueError as e:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        except Exception as e:
            context.abort(grpc.StatusCode.INTERNAL, f"create_stream failed: {e}")
        return _StreamEntry(sdk, stream, to_payload, _ingest_row)

    def _open_arrow_stream(self, request, context):
        # The Arrow schema is the IPC schema message the orchestrator ships; batches are
        # built against it. No-auth goes through a headers provider with no headers.
        try:
            schema = records.arrow_schema_from_ipc(request.arrow_schema_ipc)
        except ValueError as e:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        to_payload = functools.partial(records.record_batch_from_json, schema)

        try:
            sdk = _create_sdk(request.server_endpoint)
            stream = sdk.create_arrow_stream_with_headers_provider(
                request.table_path, schema, _NoAuthHeadersProvider()
            )
        except ValueError as e:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        except Exception as e:
            context.abort(grpc.StatusCode.INTERNAL, f"create_arrow_stream failed: {e}")
        return _StreamEntry(sdk, stream, to_payload, _ingest_batch)

    def IngestRecord(self, request, context):
        entry = self._entry(context, request.stream_id)
        with entry.lock:
            try:
                payload = entry.to_payload(request.record_json)
            except ValueError as e:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
            try:
                # Queue only. Returns once the SDK has enqueued the record, not once it
                # is acknowledged. The SDK encodes the payload (protobuf, Avro, or Arrow) itself.
                offset = entry.ingest(entry.stream, payload)
            except Exception as e:
                context.abort(grpc.StatusCode.INTERNAL, f"ingest failed: {e}")
        return pb.IngestRecordResponse(offset=offset)

    def AwaitWriteCompleted(self, request, context):
        entry = self._entry(context, request.stream_id)
        # The sync SDK's wait_for_offset has no timeout, so run it on a worker thread and
        # join with the contract's deadline. A timed-out wait is reported in the body, not
        # as a gRPC error. The worker is not cancelled; it finishes on its own.
        # TODO(conformance): the worker thread outlives a timeout; revisit if we need
        # hard cancellation (the sync API has no cancel).
        outcome = {}

        def wait():
            try:
                entry.stream.wait_for_offset(request.offset)
            except Exception as e:
                outcome["error"] = str(e)

        worker = threading.Thread(target=wait, daemon=True)
        worker.start()
        worker.join(request.timeout_ms / 1000.0 if request.timeout_ms > 0 else None)
        if worker.is_alive():
            return pb.AwaitResponse(
                error_message=f"timed out after {request.timeout_ms} ms waiting for offset {request.offset}"
            )
        return pb.AwaitResponse(error_message=outcome.get("error", ""))

    def Flush(self, request, context):
        entry = self._entry(context, request.stream_id)
        with entry.lock:
            try:
                entry.stream.flush()
            except Exception as e:
                context.abort(grpc.StatusCode.INTERNAL, f"flush failed: {e}")
        return pb.Empty()

    def Close(self, request, context):
        # Remove first so new RPCs on this id see NOT_FOUND. The entry is dropped
        # even if the SDK close fails.
        with self._streams_lock:
            entry = self._streams.pop(request.stream_id, None)
        if entry is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"unknown stream_id '{request.stream_id}'")
        with entry.lock:
            try:
                entry.stream.close()
            except Exception as e:
                context.abort(grpc.StatusCode.INTERNAL, f"close failed: {e}")
        log.info("stream closed stream_id=%s", request.stream_id)
        return pb.Empty()

    def Shutdown(self, request, context):
        log.info("Shutdown RPC received")
        # The server finishes this response before exiting (graceful stop in __main__).
        self._shutdown.set()
        return pb.Empty()

    def close_all(self):
        """Best-effort close of streams left open at shutdown."""
        with self._streams_lock:
            entries = list(self._streams.values())
            self._streams.clear()
        for entry in entries:
            try:
                entry.stream.close()
            except Exception as e:  # shutdown path: log and continue
                log.warning("close at shutdown failed: %s", e)
