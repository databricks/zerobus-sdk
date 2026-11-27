"""
Synchronous Zerobus SDK (Rust-backed).

This module provides a high-performance synchronous interface for ingesting records
into Databricks tables via the Zerobus service. The implementation is backed by a
Rust core for optimal performance while maintaining a Pythonic API.

Example:
    >>> import os
    >>> from zerobus.sdk.sync import ZerobusSdk, TableProperties
    >>> server_endpoint = os.environ["ZEROBUS_SERVER_ENDPOINT"]
    >>> workspace_url = os.environ["DATABRICKS_WORKSPACE_URL"]
    >>>
    >>> sdk = ZerobusSdk(
    ...     host=server_endpoint,
    ...     unity_catalog_url=workspace_url,
    ...     application_name="my-app/1.0",  # optional
    ... )
    >>>
    >>> props = TableProperties("catalog.schema.table")
    >>> stream = sdk.create_stream(
    ...     client_id="your-client-id",
    ...     client_secret="your-client-secret",
    ...     table_properties=props
    ... )
    >>>
    >>> # Optimized API - returns offset directly
    >>> offset = stream.ingest_record_offset('{"value": "record_data"}')
    >>>
    >>> # Batch API - returns one offset for the batch
    >>> batch_offset = stream.ingest_records_offset([
    ...     '{"value": "record1"}',
    ...     '{"value": "record2"}',
    ... ])
    >>>
    >>> stream.flush()  # Ensure all records are sent
    >>> stream.close()
"""

from typing import Iterator, Optional

# Import Rust-backed implementations
import zerobus._zerobus_core as _core
from zerobus.sdk.shared import multiplexed as _multiplexed
from zerobus.sdk.shared.auth import FederatedToken, PersonalAccessToken
from zerobus.sdk.shared.persistent import _PersistentStreamBuilderBase

# Import base Rust SDK classes
_RustZerobusSdk = _core.sync.ZerobusSdk
_RustZerobusStream = _core.sync.ZerobusStream
_RustMultiplexedStream = _core.sync.MultiplexedStream
PersistentStream = _core.sync.PersistentStream


class ZerobusStream:
    """
    Python wrapper around Rust ZerobusStream.

    Wraps the Rust implementation to provide iterator-based APIs for better
    compatibility with the old Python SDK. Handles Avro encoding transparently.
    """

    def __init__(self, rust_stream: _RustZerobusStream, avro_encoder=None):
        self._inner = rust_stream
        self._avro = avro_encoder  # AvroEncoder for an Avro stream, else None

    def _encode_payload(self, payload):
        """Encode payload to Avro bytes on an Avro stream; else pass through."""
        return payload if self._avro is None else self._avro.encode(payload)

    def _encode_payloads(self, payloads):
        """Encode a batch on an Avro stream; else pass through."""
        return payloads if self._avro is None else self._avro.encode_batch(payloads)

    # Forward all methods to Rust, converting iterables as needed
    def ingest_record(self, payload):
        """Ingest a record and return a RecordAcknowledgment (deprecated - use ingest_record_offset)."""
        payload = self._encode_payload(payload)
        return self._inner.ingest_record(payload)

    def ingest_record_offset(self, payload):
        """Submit record and return offset immediately (no waiting).

        Returns as soon as the record is queued; the SDK sends it and tracks its
        acknowledgment in the background. The idiomatic flow is to ingest in a loop
        and call ``flush()`` once to confirm durability (or use an ``AckCallback``).
        """
        payload = self._encode_payload(payload)
        return self._inner.ingest_record_offset(payload)

    def ingest_record_nowait(self, payload):
        """Submit record without waiting (fire-and-forget).

        Spawns a detached task and discards enqueue errors. ``flush()`` can complete
        before the task allocates an offset, so this is not a safe durability path.
        Prefer ``ingest_record_offset()`` or ``ingest_records_offset()``.
        """
        payload = self._encode_payload(payload)
        return self._inner.ingest_record_nowait(payload)

    def ingest_records_offset(self, payloads):
        """Submit batch of records and return final offset."""
        payloads = self._encode_payloads(payloads)
        return self._inner.ingest_records_offset(payloads)

    def ingest_records_nowait(self, payloads):
        """Submit batch of records without waiting.

        Same detached-task caveats as ``ingest_record_nowait()``. Prefer
        ``ingest_records_offset()``.
        """
        payloads = self._encode_payloads(payloads)
        return self._inner.ingest_records_nowait(payloads)

    def wait_for_offset(self, offset: int):
        """Block until a specific offset is acknowledged.

        Use when you need to confirm a specific record before continuing; acks are
        ordered, so waiting on the last offset returned confirms all prior records too.
        For bulk durability, prefer ingesting in a loop and calling ``flush()`` once.
        """
        return self._inner.wait_for_offset(offset)

    def flush(self):
        """Flush all pending records, blocking until they are acknowledged.

        The idiomatic way to confirm durability: ingest in a loop, then call ``flush()``
        once to confirm everything queued so far is committed.
        """
        return self._inner.flush()

    def close(self):
        """Close the stream."""
        return self._inner.close()

    def get_unacked_records(self) -> Iterator[bytes]:
        """
        Get iterator of unacknowledged records.

        Returns:
            Iterator[bytes]: Iterator yielding record payloads that have been ingested but not yet acknowledged.
        """
        records = self._inner.get_unacked_records()
        return iter(records)

    def get_unacked_batches(self) -> Iterator[list[bytes]]:
        """
        Get iterator of unacknowledged batches.

        Returns:
            Iterator[List[bytes]]: Iterator yielding batches, where each batch is a list of record payloads.
        """
        batches = self._inner.get_unacked_batches()
        return iter(batches)

    @property
    def stream_id(self):
        """Get the stream ID (placeholder)."""
        return self._inner.stream_id if hasattr(self._inner, "stream_id") else "stream-placeholder-id"

    def get_state(self):
        """Get the current stream state (placeholder)."""
        return self._inner.get_state() if hasattr(self._inner, "get_state") else 1


class MultiplexedStream:
    """
    A stream that distributes records round-robin across several sub-streams (Beta).

    Create one with ``ZerobusSdk.create_multiplexed_stream()``. Ingest in a loop, then call
    ``flush()`` once to confirm durability. Records keep their order within a sub-stream, but
    there is no order across sub-streams. Thread-safe: several threads may ingest, flush, and
    close one stream.

    Ingest methods return a message ID: an opaque ``int`` that identifies the sub-stream and
    the record's position in it. Message IDs are not ordered and may be negative. Pass one to
    ``wait_for_message_id()`` on this stream; do not compare them or use them with another
    stream.

    Once a call reports a sub-stream failure, ``is_closed`` is ``True`` and every later
    ingest, ``flush()``, and ``close()`` raises that error again, which may be a retriable
    ``ZerobusException``; ``wait_for_message_id()`` still reports only its own sub-stream.
    Retrying on this stream does not help: recover the unacknowledged records with
    ``get_unacked_records()`` or ``get_unacked_batches()`` and ingest them into a new stream.
    Handles Avro encoding transparently on an Avro stream.
    """

    def __init__(self, rust_stream: _RustMultiplexedStream, avro_encoder=None):
        self._inner = rust_stream
        self._avro = avro_encoder  # AvroEncoder for an Avro stream, else None

    def _encode_payload(self, payload):
        """Encode payload to Avro bytes on an Avro stream; else pass through."""
        return payload if self._avro is None else self._avro.encode(payload)

    def _encode_payloads(self, payloads):
        """Encode a batch on an Avro stream; else pass through."""
        return payloads if self._avro is None else self._avro.encode_batch(payloads)

    @property
    def is_closed(self) -> bool:
        """Whether the stream is closed, by ``close()`` or by a sub-stream failure.

        A ``close()`` or ``get_unacked_*()`` call that is in progress counts as closed.
        """
        return self._inner.is_closed

    def ingest_record(self, payload) -> int:
        """Queue a record on the next sub-stream and return its message ID.

        Returns as soon as the record is queued; the SDK sends it and tracks its
        acknowledgment in the background. The idiomatic flow is to ingest in a loop and call
        ``flush()`` once to confirm durability (or use an ``AckCallback``). If the selected
        sub-stream is at capacity, this waits for it to drain rather than rerouting, for at
        most 30 seconds; then it raises a retriable ``ZerobusException`` and the record is not
        queued, so ingest it again.
        """
        return self._inner.ingest_record(self._encode_payload(payload))

    def ingest_records(self, payloads) -> Optional[int]:
        """Queue a batch on one sub-stream and return one message ID for it.

        Returns ``None`` for an empty batch. On a closed or failed stream, an empty batch
        raises like any other. Waits for a full sub-stream as ``ingest_record()`` does, for at
        most 30 seconds.
        """
        return self._inner.ingest_records(self._encode_payloads(payloads))

    def wait_for_message_id(self, message_id: int) -> None:
        """Block until the record or batch behind ``message_id`` is acknowledged.

        Use when a specific record must be confirmed before continuing. Unlike offsets,
        message IDs are not ordered, so acknowledging one says nothing about the others. For
        bulk durability, ingest in a loop and call ``flush()`` once.
        """
        return self._inner.wait_for_message_id(message_id)

    def flush(self) -> None:
        """Flush every sub-stream, blocking until all pending records are acknowledged.

        The idiomatic way to confirm durability: ingest in a loop, then call ``flush()``
        once to confirm everything queued so far is committed.
        """
        return self._inner.flush()

    def close(self) -> None:
        """Flush pending records and close every sub-stream.

        When close returns, all callbacks have run, unless close waited longer than
        ``callback_max_wait_time_ms`` (default 5 s).
        """
        return self._inner.close()

    def get_unacked_records(self) -> Iterator[bytes]:
        """
        Get iterator of unacknowledged records, across every sub-stream.

        Closes the stream first if it is still open, so the result is complete. Avro
        records are returned as encoded bytes.

        Returns:
            Iterator[bytes]: Iterator yielding record payloads that were ingested but not acknowledged.
        """
        return iter(self._inner.get_unacked_records())

    def get_unacked_batches(self) -> Iterator[list[bytes]]:
        """
        Get iterator of unacknowledged batches, across every sub-stream.

        Closes the stream first if it is still open, so the result is complete.

        Returns:
            Iterator[List[bytes]]: Iterator yielding batches, where each batch is a list of record payloads.
        """
        return iter(self._inner.get_unacked_batches())


class ZerobusArrowStream:
    """
    Synchronous Arrow Flight stream for ingesting pyarrow RecordBatches.

    Example:
        >>> import pyarrow as pa
        >>> schema = pa.schema([("temp", pa.int32())])
        >>> stream = sdk.create_arrow_stream("catalog.schema.table", schema, client_id, client_secret)
        >>> batch = pa.record_batch({"temp": [22, 23]}, schema=schema)
        >>> offset = stream.ingest_batch(batch)
        >>> stream.flush()
        >>> stream.close()
    """

    def __init__(self, rust_stream):
        self._inner = rust_stream

    def ingest_batch(self, batch) -> int:
        """
        Ingest a pyarrow.RecordBatch or pyarrow.Table.

        Args:
            batch: A pyarrow.RecordBatch or pyarrow.Table to ingest.

        Returns:
            The offset ID assigned to this batch.
        """
        from zerobus.sdk.shared.arrow import _serialize_batch

        ipc_bytes = _serialize_batch(batch)
        return self._inner.ingest_batch(ipc_bytes)

    def wait_for_offset(self, offset: int):
        """Block until a specific offset is acknowledged.

        Use when you need to confirm a specific batch before continuing; acks are
        ordered, so waiting on the last offset returned confirms all prior batches too.
        For bulk durability, prefer ingesting batches in a loop and calling ``flush()``.
        """
        return self._inner.wait_for_offset(offset)

    def flush(self):
        """Flush all pending batches, blocking until they are acknowledged.

        The idiomatic way to confirm durability: ingest batches in a loop, then call
        ``flush()`` once to confirm everything queued so far is committed.
        """
        return self._inner.flush()

    def close(self):
        """Close the stream gracefully."""
        return self._inner.close()

    @property
    def is_closed(self) -> bool:
        """Check if the stream has been closed."""
        return self._inner.is_closed

    @property
    def table_name(self) -> str:
        """Get the table name."""
        return self._inner.table_name

    def get_unacked_batches(self) -> list:
        """
        Get unacknowledged batches as a list of pyarrow.RecordBatch.

        The stream must be closed before calling this method.

        Returns:
            List of pyarrow.RecordBatch objects.
        """
        from zerobus.sdk.shared.arrow import _deserialize_batch

        ipc_list = self._inner.get_unacked_batches()
        return [_deserialize_batch(ipc_bytes) for ipc_bytes in ipc_list]


class PersistentStreamBuilder(_PersistentStreamBuilderBase):
    """Fluent builder that creates or resumes a persistent (Eos) stream.

    Obtain one from ``ZerobusSdk.persistent_stream_builder()``, set the table, authentication
    and record format, then call ``build()`` or ``resume()``.

    Example:
        >>> stream = (
        ...     sdk.persistent_stream_builder()
        ...     .table("catalog.schema.events")
        ...     .oauth("client-id", "client-secret")
        ...     .json()
        ...     .build()
        ... )
        >>> stream_id = stream.stream_id  # persist this to resume after a restart
    """

    def build(self) -> PersistentStream:
        """Open a new persistent stream. The server assigns its ``stream_id``.

        Raises:
            ValueError: If the table, authentication, or record format is not set.
        """
        table_properties, settings = self._native_args()
        return self._sdk.open_persistent_stream(table_properties, settings)

    def resume(self, stream_id: str) -> PersistentStream:
        """Reconnect to an existing persistent stream by its ``stream_id``.

        The table and record format must match those the stream was created with. New
        records get offsets that start after ``resume_watermark``.

        Raises:
            ValueError: If the table, authentication, or record format is not set.
        """
        table_properties, settings = self._native_args()
        return self._sdk.open_persistent_stream(table_properties, settings, self._check_stream_id(stream_id))


class ZerobusSdk:
    """Python wrapper around Rust ZerobusSdk that provides unified create_stream API."""

    def __init__(
        self,
        host: str,
        unity_catalog_url: str,
        application_name: Optional[str] = None,
        connection_per_stream: bool = True,
    ):
        """
        Create a Zerobus SDK instance.

        Args:
            host: Zerobus server endpoint URL. For the format on each cloud, see
                https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id.
            unity_catalog_url: Databricks workspace URL used for Unity Catalog and OAuth.
            application_name: Optional caller identifier (conventionally
                "<product>/<version>") appended to the HTTP user-agent header on
                gRPC requests toward the Zerobus server.
            connection_per_stream: Whether each JSON/protobuf stream gets a
                dedicated gRPC connection. Defaults to True. Set to False to
                share one connection across streams.
        """
        self._inner = _RustZerobusSdk(host, unity_catalog_url, application_name, connection_per_stream)

    def create_arrow_stream(
        self, table_name: str, schema, client_id: str, client_secret: str, options=None
    ) -> ZerobusArrowStream:
        """
        Create an Arrow Flight stream with OAuth client credentials.

        Args:
            table_name: Fully qualified table name (catalog.schema.table).
            schema: A pyarrow.Schema defining the table schema.
            client_id: OAuth client ID.
            client_secret: OAuth client secret.
            options: Optional ArrowStreamConfigurationOptions.

        Returns:
            A ZerobusArrowStream ready for ingesting RecordBatches.
        """
        from zerobus.sdk.shared.arrow import _serialize_schema

        schema_bytes = _serialize_schema(schema)
        rust_stream = self._inner.create_arrow_stream(table_name, schema_bytes, client_id, client_secret, options)
        return ZerobusArrowStream(rust_stream)

    def create_arrow_stream_with_headers_provider(
        self, table_name: str, schema, headers_provider, options=None
    ) -> ZerobusArrowStream:
        """
        Create an Arrow Flight stream with a custom headers provider.

        Args:
            table_name: Fully qualified table name (catalog.schema.table).
            schema: A pyarrow.Schema defining the table schema.
            headers_provider: Custom headers provider for authentication.
            options: Optional ArrowStreamConfigurationOptions.

        Returns:
            A ZerobusArrowStream ready for ingesting RecordBatches.
        """
        from zerobus.sdk.shared.arrow import _serialize_schema

        schema_bytes = _serialize_schema(schema)
        rust_stream = self._inner.create_arrow_stream_with_headers_provider(
            table_name, schema_bytes, headers_provider, options
        )
        return ZerobusArrowStream(rust_stream)

    def recreate_arrow_stream(self, old_stream: ZerobusArrowStream) -> ZerobusArrowStream:
        """
        Recreate a closed Arrow stream with the same configuration,
        re-ingesting unacknowledged batches.

        Args:
            old_stream: The closed Arrow stream to recreate.

        Returns:
            A new ZerobusArrowStream.
        """
        rust_stream = self._inner.recreate_arrow_stream(old_stream._inner)
        return ZerobusArrowStream(rust_stream)

    def create_stream(
        self,
        client_id: str = None,
        client_secret: str = None,
        table_properties=None,
        options=None,
        headers_provider=None,
        *,
        auth=None,
    ):
        """
        Create a stream with OAuth, external-IdP federation, a personal access token,
        or a custom headers provider.

        Args:
            client_id: OAuth client ID
            client_secret: OAuth client secret
            table_properties: Table configuration
            options: Optional stream configuration
            headers_provider: Optional custom headers provider (if set, overrides OAuth)
            auth: Optional FederatedToken for external-IdP (e.g. Entra ID) federation,
                or PersonalAccessToken for a Databricks personal access token (Private
                Preview); when set, client_id/client_secret are not required.
        """
        if auth is not None and (client_id is not None or client_secret is not None):
            raise ValueError(
                "client_id/client_secret cannot be combined with auth=; "
                "pass table_properties as a keyword when using auth=, "
                "e.g. create_stream(table_properties=..., auth=...)"
            )
        if table_properties is None:
            raise ValueError("table_properties is required")

        from zerobus.sdk.shared.avro import make_encoder

        avro_encoder = make_encoder(table_properties.avro_schema)

        if isinstance(auth, PersonalAccessToken):
            rust_stream = self._inner.create_stream_pat(table_properties, auth.token, options)
        elif auth is not None:
            if not isinstance(auth, FederatedToken):
                raise ValueError("auth= must be a FederatedToken or PersonalAccessToken instance")
            # External-IdP federation (RFC 8693). Build a FRESH native supplier per
            # stream (not memoized on the FederatedToken, which would freeze the
            # first loop's task-locals and policy). allow_async=False: the sync SDK
            # cannot drive an awaitable, so an async callback is rejected as misuse.
            # Reuse still shares the cache via the stable _cache_identity.
            native_supplier = _core.IdpSupplier(auth.idp_token_supplier, False, auth._cache_identity)
            # The native supplier holds the callback (and any async context) on a
            # GC-visible holder that it references only weakly; create_stream_federated
            # moves the sole retained strong reference onto the stream, keeping a
            # self-referential owner/stream/callback cycle collectable.
            rust_stream = self._inner.create_stream_federated(
                table_properties, native_supplier, auth.databricks_client_id, options
            )
        elif headers_provider is not None:
            # Use custom headers provider (ignores client_id/client_secret)
            rust_stream = self._inner.create_stream_with_headers_provider(table_properties, headers_provider, options)
        else:
            # Use OAuth authentication
            if client_id is None or client_secret is None:
                raise ValueError("client_id and client_secret are required unless auth= or headers_provider= is given")
            rust_stream = self._inner.create_stream(client_id, client_secret, table_properties, options)
        return ZerobusStream(rust_stream, avro_encoder=avro_encoder)

    def create_multiplexed_stream(
        self,
        table_properties: _core.TableProperties,
        stream_count: int,
        *,
        auth,
        options: Optional[_core.StreamConfigurationOptions] = None,
    ) -> MultiplexedStream:
        """
        Open a multiplexed stream (Beta): ``stream_count`` sub-streams to one table, with
        records distributed round-robin across them.

        Use it when one stream is the throughput bottleneck and global ordering is not
        required. Opening is atomic: if any sub-stream fails to open, the others are closed
        and this raises. Ingest calls return a message ID instead of an offset, and an
        ``AckCallback`` set in ``MultiplexedStreamOptions`` receives that message ID in its
        ``offset`` argument.

        Args:
            table_properties: Table configuration. JSON, protobuf, and Avro are supported.
            stream_count: The number of sub-streams, from 1 to 64.
            auth: ``OAuth(client_id, client_secret)``, a ``HeadersProvider`` (or any object
                with a ``get_headers()`` method), a ``FederatedToken``, or a
                ``PersonalAccessToken``.
            options: Optional ``MultiplexedStreamOptions``. ``max_inflight_records`` is a
                budget for the whole stream; each sub-stream gets
                ``max_inflight_records // stream_count``, so it must be at least
                ``stream_count``. A plain ``StreamConfigurationOptions`` is accepted only
                without ``ack_callback``.

        Raises:
            ValueError: If an argument is missing, out of range, or of the wrong kind.
            NonRetriableException: If an option is invalid, such as an in-flight budget below
                ``stream_count`` (the error calls ``max_inflight_records`` by the core's name,
                ``max_inflight_requests``).
            ZerobusException: If a sub-stream fails to open.

        Example:
            >>> stream = sdk.create_multiplexed_stream(
            ...     TableProperties("catalog.schema.table"),
            ...     4,
            ...     auth=OAuth("client-id", "client-secret"),
            ... )
            >>> for record in records:
            ...     stream.ingest_record(record)
            >>> stream.flush()
            >>> stream.close()
        """
        from zerobus.sdk.shared.avro import make_encoder

        _multiplexed.check_arguments(table_properties, stream_count, auth, options)
        avro_encoder = make_encoder(table_properties.avro_schema)
        native = _multiplexed.call_native(self._inner, table_properties, stream_count, auth, options, allow_async=False)
        return MultiplexedStream(native, avro_encoder)

    def persistent_stream_builder(self) -> PersistentStreamBuilder:
        """
        Start building a persistent (Eos) stream.

        A persistent stream records its identity and committed offset on the server, so
        ``PersistentStreamBuilder.resume()`` can reconnect to it after a restart with
        exactly-once delivery. It supports JSON and protobuf records, with OAuth, a personal
        access token, or a custom headers provider.
        """
        return PersistentStreamBuilder(self._inner)

    def recreate_stream(self, old_stream: ZerobusStream):
        """Recreate a stream from an old stream, preserving the Avro encoder if set."""
        rust_stream = self._inner.recreate_stream(old_stream._inner)
        return ZerobusStream(rust_stream, avro_encoder=old_stream._avro)


# Direct re-exports
RecordAcknowledgment = _core.sync.RecordAcknowledgment

# Re-export common types for convenience
HeadersProvider = _core.HeadersProvider
RecordType = _core.RecordType
StreamConfigurationOptions = _core.StreamConfigurationOptions
MultiplexedStreamOptions = _core.MultiplexedStreamOptions
TableProperties = _core.TableProperties
AckCallback = _core.AckCallback
ZerobusException = _core.ZerobusException
NonRetriableException = _core.NonRetriableException

__all__ = [
    "ZerobusSdk",
    "ZerobusStream",
    "PersistentStream",
    "PersistentStreamBuilder",
    "ZerobusArrowStream",
    "RecordAcknowledgment",
    "TableProperties",
    "StreamConfigurationOptions",
    "MultiplexedStream",
    "MultiplexedStreamOptions",
    "RecordType",
    "AckCallback",
    "HeadersProvider",
    "ZerobusException",
    "NonRetriableException",
]
