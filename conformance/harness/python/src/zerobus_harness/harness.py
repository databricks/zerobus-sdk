"""``TestHarness`` service: maps each contract RPC onto the Python Zerobus SDK.

Every handler is a pass-through: it converts the request into the SDK's input, calls the
SDK method, and reports the SDK's result. The harness adds no retries, waits, timeouts,
or error rewriting of its own. Cross-SDK differences are scenario data, not harness logic.

Wired: ``protocol=PROTOCOL_GRPC`` with ``format=RECORD_FORMAT_PROTO`` or
``format=RECORD_FORMAT_AVRO``, and ``protocol=PROTOCOL_ARROW_FLIGHT`` with
``format=RECORD_FORMAT_ARROW``. Every other combination returns
``UNIMPLEMENTED`` with a TODO (see :func:`check_slice_supported`).

Error model:
- An SDK exception (``ZerobusException``, including its ``NonRetriableException``
  subclass) is the SDK's rejection. Its message is reported verbatim in the response's
  ``error``, with ``retryable`` from :func:`_is_retryable`, and the RPC returns normally.
- A record or schema the harness cannot convert into SDK input is INVALID_ARGUMENT.
- An empty batch, which the SDK answers with no offset, is INVALID_ARGUMENT.
- An unknown ``stream_id`` is NOT_FOUND. A ``stream_id`` that was closed stays known;
  later calls reach the SDK, which reports its own closed-stream error.
- Any other exception is a harness or build fault (for example, a wheel built without the
  ``testing`` feature). It aborts the RPC with INTERNAL and is never reported as an
  SDK rejection.
"""

import functools
import itertools
import logging
import threading

import grpc

from zerobus.sdk.shared import HeadersProvider, NonRetriableException, ZerobusException
from zerobus.sdk.sync import TableProperties, ZerobusSdk

from . import records
from ._generated import test_harness_pb2 as pb
from ._generated import test_harness_pb2_grpc as pb_grpc

log = logging.getLogger("zerobus_harness")

# The SDK's own exception types. NonRetriableException subclasses ZerobusException.
_SDK_ERRORS = (ZerobusException, NonRetriableException)


class _NoAuthHeadersProvider(HeadersProvider):
    """Sends no authentication headers. The Python binding has no no-auth Arrow
    constructor, so the Arrow path passes this provider instead."""

    def get_headers(self):
        return []


class _StreamEntry:
    """One open SDK stream plus the converters and SDK ingest calls for its records."""

    def __init__(self, sdk, stream, to_payload, to_batch, ingest_one, ingest_many):
        # Keep the SDK alive for as long as the stream it created.
        self.sdk = sdk
        self.stream = stream
        # record_json (str) -> the SDK's single-record input.
        self.to_payload = to_payload
        # [record_json] -> the SDK's batch input (a list of payloads, or one RecordBatch).
        self.to_batch = to_batch
        # (stream, payload) -> offset. Queue-only; never waits for an ack.
        self.ingest_one = ingest_one
        # (stream, batch) -> offset via the SDK's batch ingest. None for an empty batch.
        self.ingest_many = ingest_many
        # Per-stream lock. The Python sync SDK is single-threaded per stream (see the
        # root CLAUDE.md), so Ingest, Flush, and Close take this lock one at a time. A slow
        # Flush holds it and blocks ingest on the same stream. AwaitWriteCompleted does not
        # take it.
        self.lock = threading.Lock()


def _is_retryable(exc):
    """Mirror the SDK's own classification. The Rust core maps every error through
    ``map_error`` (``python/rust/src/common.rs``): retryable errors raise
    ``ZerobusException``, fatal ones raise ``NonRetriableException``."""
    return isinstance(exc, ZerobusException) and not isinstance(exc, NonRetriableException)


def _sdk_error(exc):
    return pb.SdkError(message=str(exc), retryable=_is_retryable(exc))


def _fault(context, exc):
    """Abort the RPC for a non-SDK exception: a harness or build fault, not a rejection."""
    log.exception("harness fault")
    context.abort(grpc.StatusCode.INTERNAL, f"harness fault: {type(exc).__name__}: {exc}")


def _create_sdk(server_endpoint):
    """The mock is a local plaintext server with no credentials, so the SDK runs with
    no TLS and no auth (testing-only, gated behind the `testing` feature). A wheel built
    without that feature raises here, which is a harness fault."""
    return ZerobusSdk(f"http://{server_endpoint}", "", None, True, no_tls=True)


def _ingest_row(stream, payload):
    return stream.ingest_record_offset(payload)


def _ingest_rows(stream, payloads):
    return stream.ingest_records_offset(payloads)


def _ingest_batch(stream, batch):
    return stream.ingest_batch(batch)


def _per_record(to_payload):
    """Batch converter for SDKs whose batch input is a list of single-record payloads."""
    return lambda record_jsons: [to_payload(r) for r in record_jsons]


def check_slice_supported(protocol, fmt):
    """Return an (grpc.StatusCode, message) error for unsupported pairs, else None.

    Each TODO names the work that remains.
    """
    if protocol == pb.PROTOCOL_GRPC and fmt in (pb.RECORD_FORMAT_PROTO, pb.RECORD_FORMAT_AVRO):
        return None
    if protocol == pb.PROTOCOL_ARROW_FLIGHT and fmt == pb.RECORD_FORMAT_ARROW:
        return None
    if protocol == pb.PROTOCOL_GRPC and fmt == pb.RECORD_FORMAT_JSON:
        return (
            grpc.StatusCode.UNIMPLEMENTED,
            "TODO(conformance): GRPC + JSON not implemented in the Python harness yet",
        )
    if protocol == pb.PROTOCOL_GRPC and fmt == pb.RECORD_FORMAT_ARROW:
        return (
            grpc.StatusCode.UNIMPLEMENTED,
            "TODO(conformance): GRPC + ARROW is not a valid pairing; ARROW rides ARROW_FLIGHT",
        )
    if protocol == pb.PROTOCOL_ARROW_FLIGHT:
        return (grpc.StatusCode.INVALID_ARGUMENT, "ARROW_FLIGHT carries only the ARROW format")
    return (grpc.StatusCode.INVALID_ARGUMENT, "protocol and format must be specified")


class HarnessService(pb_grpc.TestHarnessServicer):
    def __init__(self, shutdown_event: threading.Event):
        self._streams = {}  # stream_id -> _StreamEntry; entries stay after Close
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
        try:
            if request.protocol == pb.PROTOCOL_ARROW_FLIGHT:
                entry = self._open_arrow_stream(request)
            else:
                entry = self._open_grpc_stream(request)
        except records.ConversionError as e:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        except _SDK_ERRORS as e:
            return pb.CreateStreamResponse(error=_sdk_error(e))
        except Exception as e:
            _fault(context, e)

        with self._streams_lock:
            stream_id = f"stream-{next(self._ids)}"
            self._streams[stream_id] = entry
        return pb.CreateStreamResponse(stream_id=stream_id)

    def _open_grpc_stream(self, request):
        # Schema artifact per format. Each converter only builds the SDK's input; the SDK
        # validates the schema and does the encoding.
        if request.format == pb.RECORD_FORMAT_AVRO:
            to_payload = records.AvroRecordBuilder(request.avro_schema_json)
            table_kwargs = {"avro_schema": request.avro_schema_json}
        else:
            resolved = records.descriptor_from_bytes(request.descriptor_proto_bytes)
            to_payload = functools.partial(records.record_from_json, resolved.message_class)
            table_kwargs = {"descriptor_proto": resolved.file_descriptor_bytes}

        sdk = _create_sdk(request.server_endpoint)
        stream = sdk.create_stream(
            table_properties=TableProperties(request.table_path, **table_kwargs),
            no_auth=True,
        )
        return _StreamEntry(sdk, stream, to_payload, _per_record(to_payload), _ingest_row, _ingest_rows)

    def _open_arrow_stream(self, request):
        # The Arrow schema is the IPC schema message the orchestrator ships; batches are
        # built against it. No-auth goes through a headers provider with no headers.
        schema = records.arrow_schema_from_ipc(request.arrow_schema_ipc)

        def to_payload(record_json):
            return records.record_batch_from_json(schema, [record_json])

        to_batch = functools.partial(records.record_batch_from_json, schema)

        sdk = _create_sdk(request.server_endpoint)
        stream = sdk.create_arrow_stream_with_headers_provider(
            request.table_path, schema, _NoAuthHeadersProvider()
        )
        return _StreamEntry(sdk, stream, to_payload, to_batch, _ingest_batch, _ingest_batch)

    def IngestRecord(self, request, context):
        entry = self._entry(context, request.stream_id)
        try:
            payload = entry.to_payload(request.record_json)
        except records.ConversionError as e:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        except Exception as e:
            _fault(context, e)
        with entry.lock:
            try:
                # Queue only. Returns once the SDK has enqueued the record, not once it
                # is acknowledged. The SDK encodes the payload (protobuf, Avro, or Arrow) itself.
                offset = entry.ingest_one(entry.stream, payload)
            except _SDK_ERRORS as e:
                return pb.IngestRecordResponse(error=_sdk_error(e))
            except Exception as e:
                _fault(context, e)
        return pb.IngestRecordResponse(offset=offset)

    def IngestRecords(self, request, context):
        entry = self._entry(context, request.stream_id)
        if not request.record_json:
            # Checked before the SDK: an Arrow zero-row batch is not None to the SDK, so
            # the offset check below would not catch it on that path.
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "IngestRecords requires at least one record")
        # The SDK's batch call takes the whole batch at once, so every record is converted
        # first. A record the harness cannot convert aborts before anything is queued.
        try:
            batch = entry.to_batch(list(request.record_json))
        except records.ConversionError as e:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        except Exception as e:
            _fault(context, e)
        with entry.lock:
            try:
                # Queue only, through the SDK's batch ingest. It returns one offset for the batch.
                offset = entry.ingest_many(entry.stream, batch)
            except _SDK_ERRORS as e:
                return pb.IngestRecordsResponse(error=_sdk_error(e))
            except Exception as e:
                _fault(context, e)
        if offset is None:
            # The SDK returns no offset for an empty batch. Report it as the caller's error
            # rather than inventing an offset.
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "IngestRecords requires at least one record")
        return pb.IngestRecordsResponse(offset=offset)

    def AwaitWriteCompleted(self, request, context):
        entry = self._entry(context, request.stream_id)
        # No harness-side deadline: the SDK's own wait (and its own error) is the result.
        try:
            entry.stream.wait_for_offset(request.offset)
        except _SDK_ERRORS as e:
            return pb.AwaitResponse(error=_sdk_error(e))
        except Exception as e:
            _fault(context, e)
        return pb.AwaitResponse()

    def Flush(self, request, context):
        entry = self._entry(context, request.stream_id)
        with entry.lock:
            try:
                entry.stream.flush()
            except _SDK_ERRORS as e:
                return pb.ActionResponse(error=_sdk_error(e))
            except Exception as e:
                _fault(context, e)
        return pb.ActionResponse()

    def Close(self, request, context):
        # The entry stays in the map, so a later call on this id reaches the SDK, which
        # reports its own closed-stream error. Only an id that was never created is NOT_FOUND.
        entry = self._entry(context, request.stream_id)
        with entry.lock:
            try:
                entry.stream.close()
            except _SDK_ERRORS as e:
                return pb.ActionResponse(error=_sdk_error(e))
            except Exception as e:
                _fault(context, e)
        log.info("stream closed stream_id=%s", request.stream_id)
        return pb.ActionResponse()

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
