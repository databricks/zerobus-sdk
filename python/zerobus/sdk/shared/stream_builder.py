"""
Builders for ordinary and multiplexed streams, shared by the sync and async SDKs.

``ZerobusSdk.stream_builder()`` mirrors the Rust SDK's ``stream_builder()``: set the table,
authentication, record format, and stream settings, then call ``build()`` for an ordinary
stream, or ``multiplexed(n)`` and then ``build()`` for a multiplexed stream (Beta).
``validate()`` checks a configuration without opening anything. The sync and async builders
in ``zerobus.sdk.sync`` and ``zerobus.sdk.aio`` add the ``build()`` methods.

A multiplexed stream opens several sub-streams to one table and distributes records
round-robin across them, to raise throughput when one stream is the bottleneck. Records keep
their order within a sub-stream, but there is no order across sub-streams. Its ingest methods
return a message ID: an opaque ``int`` that identifies the sub-stream and the record's
position in it. Message IDs are not ordered and may be negative.

Both stream kinds support the JSON, protobuf, and Avro record formats, with OAuth or a custom
headers provider for authentication. For external-IdP federation, use
``ZerobusSdk.create_stream(auth=...)``.
"""

import copy
from typing import Optional

import zerobus._zerobus_core as _core
from zerobus.sdk.shared.builder import _int_in_range, _StreamBuilderBase

_MAX_MULTIPLEXED_STREAMS = 64


class _GrpcStreamBuilderBase(_StreamBuilderBase):
    """Collects the settings of an ordinary or multiplexed stream. Setters return the builder."""

    _FORMAT_METHODS = "json(), compiled_proto(...), or avro(...)"
    # The sync or async multiplexed builder class that multiplexed(n) returns.
    _MULTIPLEXED_BUILDER = None

    def __init__(self, native_sdk):
        super().__init__(native_sdk)
        self._avro_schema: Optional[str] = None

    def json(self):
        """Select the JSON record format."""
        self._avro_schema = None
        return super().json()

    def compiled_proto(self, descriptor):
        """Select the protobuf record format.

        Args:
            descriptor: The serialized ``FileDescriptorProto`` as ``bytes``, or a protobuf
                ``Descriptor`` object, as accepted by ``TableProperties``.
        """
        self._avro_schema = None
        return super().compiled_proto(descriptor)

    def avro(self, schema_json: str):
        """Select the Avro record format (Beta).

        Ingest dicts, which are encoded against the schema (requires fastavro: ``pip install
        databricks-zerobus-ingest-sdk[avro]``), or pre-encoded Avro datums as ``bytes``.

        Args:
            schema_json: The Avro writer schema as a JSON string.
        """
        if not isinstance(schema_json, str) or not schema_json.strip():
            raise ValueError("schema_json must be a non-empty string")
        self._avro_schema = schema_json
        self._descriptor = None
        self._json = False
        return self

    def ack_callback(self, callback):
        """Set an ``AckCallback`` that runs when the server acknowledges an ingest.

        Its ``offset`` argument receives the offset of the acknowledged ingest. Ordinary
        streams only: a multiplexed stream takes its callback through
        ``multiplexed(n).ack_callback(...)``.
        """
        return super().ack_callback(callback)

    def stream_paused_max_wait_time_ms(self, ms: Optional[int]):
        """Set the maximum wait in milliseconds during a graceful stream pause. ``None`` waits the
        full server-specified duration."""
        if ms is not None:
            _int_in_range("stream_paused_max_wait_time_ms", ms)
        self._settings["stream_paused_max_wait_time_ms_set"] = True
        self._settings["stream_paused_max_wait_time_ms"] = ms
        return self

    def multiplexed(self, stream_count: int):
        """Select a multiplexed stream (Beta) of ``stream_count`` sub-streams.

        This is a terminal step: configure the table, authentication, format, and stream
        settings first. The returned builder captures them as they are now and only offers
        ``ack_callback()`` and ``build()``.

        Use multiplexing when one stream is the throughput bottleneck and global ordering is
        not required. The ``max_inflight_requests`` budget is divided evenly across
        sub-streams, so it must be at least ``stream_count``.

        Args:
            stream_count: The number of sub-streams, from 1 to 64.
        """
        _int_in_range("stream_count", stream_count, _MAX_MULTIPLEXED_STREAMS, minimum=1)
        snapshot = copy.copy(self)
        snapshot._settings = dict(self._settings)
        return self._MULTIPLEXED_BUILDER(snapshot, stream_count)

    def validate(self) -> None:
        """Check the configuration of an ordinary stream without opening it.

        Runs the checks ``build()`` makes before connecting: the required settings, the
        descriptor, and the Rust core's own checks. As in the Rust SDK, it does not resolve
        authentication or parse the Avro writer schema, so ``build()`` can still fail. A plain
        method in the sync and async SDKs.

        Raises:
            ValueError: If the table, authentication, or record format is not set.
            NonRetriableException: If a setting is invalid.
        """
        table_properties, settings = self._native_args()
        self._sdk.validate_stream(table_properties, settings)

    def _format_selected(self) -> bool:
        return super()._format_selected() or self._avro_schema is not None

    def _native_args(self):
        """Validate the collected settings and return the table properties and native settings."""
        self._check_required()
        table_properties = _core.TableProperties(self._table_name, self._descriptor, self._avro_schema)
        settings = _core.StreamSettings(
            client_id=self._client_id,
            client_secret=self._client_secret,
            headers_provider=self._headers_provider,
            **self._settings,
        )
        return table_properties, settings


class _MultiplexedStreamBuilderBase:
    """Builds a multiplexed stream (Beta). Returned by ``multiplexed(n)`` on the stream builder."""

    def __init__(self, base: _GrpcStreamBuilderBase, stream_count: int):
        self._base = base
        self._stream_count = stream_count
        self._ack_callback = None

    def ack_callback(self, callback):
        """Set an ``AckCallback`` shared by every sub-stream.

        The callback's ``offset`` argument receives the message ID returned by the ingest
        call, not an offset. Sub-streams may call it concurrently and in no global order.
        """
        self._ack_callback = callback
        return self

    def validate(self) -> None:
        """Check the configuration of the multiplexed stream without opening any sub-stream.

        Runs the checks ``build()`` makes before connecting, including the Rust core's check
        that ``max_inflight_requests`` is at least the stream count. As in the Rust SDK, it
        does not resolve authentication or parse the Avro writer schema, so ``build()`` can
        still fail. A plain method in the sync and async SDKs.

        Raises:
            ValueError: If the table, authentication, or record format is not set, or if
                ``ack_callback()`` was set on the stream builder instead of here.
            NonRetriableException: If ``max_inflight_requests`` is below the stream count, or
                another setting is invalid.
        """
        table_properties, settings, stream_count, ack_callback = self._native_args()
        self._base._sdk.validate_stream(table_properties, settings, stream_count, ack_callback)

    def _native_args(self):
        """Validate the settings and return the arguments of the native open call."""
        if self._base._settings.get("ack_callback") is not None:
            raise ValueError(
                "ack_callback() on stream_builder() is only valid for ordinary streams; "
                "use multiplexed(n).ack_callback(...) for multiplexed streams"
            )
        table_properties, settings = self._base._native_args()
        return table_properties, settings, self._stream_count, self._ack_callback
