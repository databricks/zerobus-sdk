"""
Builder for streams, shared by the sync and async SDKs.

``ZerobusSdk.stream_builder()`` mirrors the Rust SDK's ``stream_builder()``: set the table,
authentication, record format, and stream settings, then call ``build()``. The sync and async
builders in ``zerobus.sdk.sync`` and ``zerobus.sdk.aio`` add the ``build()`` method.

Streams support the JSON, protobuf, and Avro record formats, with OAuth or a custom headers
provider for authentication. For external-IdP federation, use
``ZerobusSdk.create_stream(auth=...)``.
"""

from typing import Optional

import zerobus._zerobus_core as _core
from zerobus.sdk.shared.builder import _int_in_range, _StreamBuilderBase


class _GrpcStreamBuilderBase(_StreamBuilderBase):
    """Collects the settings of a stream. Setters return the builder."""

    _FORMAT_METHODS = "json(), compiled_proto(...), or avro(...)"

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

    def stream_paused_max_wait_time_ms(self, ms: Optional[int]):
        """Set the maximum wait in milliseconds during a graceful stream pause. ``None`` waits the
        full server-specified duration."""
        if ms is not None:
            _int_in_range("stream_paused_max_wait_time_ms", ms)
        self._settings["stream_paused_max_wait_time_ms_set"] = True
        self._settings["stream_paused_max_wait_time_ms"] = ms
        return self

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
