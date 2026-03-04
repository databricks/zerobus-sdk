"""
Synchronous Zerobus SDK (Rust-backed).

This module provides a high-performance synchronous interface for ingesting records
into Databricks tables via the Zerobus service. The implementation is backed by a
Rust core for optimal performance while maintaining a Pythonic API.

Example:
    >>> from zerobus.sdk.sync import ZerobusSdk, TableProperties
    >>>
    >>> sdk = ZerobusSdk(
    ...     host="https://your-shard-id.zerobus.region.cloud.databricks.com",
    ...     unity_catalog_url="https://your-workspace.cloud.databricks.com"
    ... )
    >>>
    >>> props = TableProperties("catalog.schema.table")
    >>> stream = sdk.create_stream(
    ...     table_properties=props,
    ...     client_id="your-client-id",
    ...     client_secret="your-client-secret"
    ... )
    >>>
    >>> # Optimized API - returns offset directly
    >>> offset = stream.ingest_record_offset(b"record_data")
    >>>
    >>> # Batch API - returns one offset for the batch
    >>> offsets = stream.ingest_records_offset([b"record1", b"record2"])
    >>>
    >>> # Fire-and-forget for maximum throughput
    >>> stream.ingest_record_nowait(b"record_data")
    >>> stream.ingest_records_nowait([b"record1", b"record2"])
    >>>
    >>> stream.flush()  # Ensure all records are sent
    >>> stream.close()
    >>>
    >>> # Legacy API (deprecated) - returns acknowledgment object
    >>> ack = stream.ingest_record(b"record_data")
    >>> offset = ack.wait_for_ack(timeout_sec=30)
"""

from typing import Iterator

# Import Rust-backed implementations
import zerobus._zerobus_core as _core

# Import base Rust SDK classes
_RustZerobusSdk = _core.sync.ZerobusSdk
_RustZerobusStream = _core.sync.ZerobusStream


class ZerobusStream:
    """
    Python wrapper around Rust ZerobusStream.

    Wraps the Rust implementation to provide iterator-based APIs for better
    compatibility with the old Python SDK.
    """

    def __init__(self, rust_stream: _RustZerobusStream):
        self._inner = rust_stream

    # Forward all methods to Rust, converting iterables as needed
    def ingest_record(self, payload):
        """Ingest a record and return a RecordAcknowledgment (deprecated - use ingest_record_offset)."""
        return self._inner.ingest_record(payload)

    def ingest_record_offset(self, payload):
        """Submit record and return offset immediately (no waiting)."""
        return self._inner.ingest_record_offset(payload)

    def ingest_record_nowait(self, payload):
        """Submit record without waiting (fire-and-forget)."""
        return self._inner.ingest_record_nowait(payload)

    def ingest_records_offset(self, payloads):
        """Submit batch of records and return final offset."""
        return self._inner.ingest_records_offset(payloads)

    def ingest_records_nowait(self, payloads):
        """Submit batch of records without waiting."""
        return self._inner.ingest_records_nowait(payloads)

    def wait_for_offset(self, offset: int):
        """Wait for a specific offset to be acknowledged."""
        return self._inner.wait_for_offset(offset)

    def flush(self):
        """Flush all pending records."""
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

    def get_unacked_batches(self) -> Iterator[list]:
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


class ZerobusSdk:
    """Python wrapper around Rust ZerobusSdk that provides unified create_stream API."""

    def __init__(self, host: str, unity_catalog_url: str):
        self._inner = _RustZerobusSdk(host, unity_catalog_url)

    def create_stream(self, client_id: str, client_secret: str, table_properties, options=None, headers_provider=None):
        """
        Create a stream with OAuth authentication or custom headers provider.

        Args:
            client_id: OAuth client ID
            client_secret: OAuth client secret
            table_properties: Table configuration
            options: Optional stream configuration
            headers_provider: Optional custom headers provider (if set, overrides OAuth)
        """
        if headers_provider is not None:
            # Use custom headers provider (ignores client_id/client_secret)
            rust_stream = self._inner.create_stream_with_headers_provider(table_properties, headers_provider, options)
        else:
            # Use OAuth authentication
            rust_stream = self._inner.create_stream(client_id, client_secret, table_properties, options)
        return ZerobusStream(rust_stream)

    def recreate_stream(self, old_stream: ZerobusStream):
        """Recreate a stream from an old stream."""
        rust_stream = self._inner.recreate_stream(old_stream._inner)
        return ZerobusStream(rust_stream)


# Direct re-exports
RecordAcknowledgment = _core.sync.RecordAcknowledgment

# Re-export common types for convenience
HeadersProvider = _core.HeadersProvider
RecordType = _core.RecordType
StreamConfigurationOptions = _core.StreamConfigurationOptions
TableProperties = _core.TableProperties
AckCallback = _core.AckCallback
ZerobusException = _core.ZerobusException
NonRetriableException = _core.NonRetriableException

__all__ = [
    "ZerobusSdk",
    "ZerobusStream",
    "RecordAcknowledgment",
    "TableProperties",
    "StreamConfigurationOptions",
    "RecordType",
    "AckCallback",
    "HeadersProvider",
    "ZerobusException",
    "NonRetriableException",
]
