"""
Builder for persistent (Eos) streams, shared by the sync and async SDKs.

A persistent stream records its identity and committed offset on the server. After a
restart, ``resume(stream_id)`` reconnects to the same stream and delivery into Delta stays
exactly-once. The sync and async builders in ``zerobus.sdk.sync`` and ``zerobus.sdk.aio``
add the ``build()`` and ``resume()`` methods.

Persistent streams support the JSON and protobuf record formats, with OAuth or a custom
headers provider for authentication. The Avro format and external-IdP federation are not
supported.
"""

from typing import Any, Optional

import zerobus._zerobus_core as _core

_MAX_U32 = 2**32 - 1
_MAX_I64 = 2**63 - 1


def _int_in_range(name: str, value: int, maximum: int = _MAX_I64, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer between {minimum} and {maximum}")
    return value


class _PersistentStreamBuilderBase:
    """Collects the settings of a persistent stream. Setters return the builder."""

    def __init__(self, native_sdk):
        self._sdk = native_sdk
        self._table_name: Optional[str] = None
        self._client_id: Optional[str] = None
        self._client_secret: Optional[str] = None
        self._headers_provider = None
        self._descriptor: Any = None
        self._json = False
        self._settings = {}

    def table(self, table_name: str):
        """Set the fully qualified Unity Catalog table name (``catalog.schema.table``)."""
        self._table_name = table_name
        return self

    def oauth(self, client_id: str, client_secret: str):
        """Authenticate with OAuth client credentials. Replaces an earlier ``headers_provider()``."""
        self._client_id = client_id
        self._client_secret = client_secret
        self._headers_provider = None
        return self

    def headers_provider(self, provider):
        """Authenticate with a custom ``HeadersProvider``. Replaces an earlier ``oauth()``."""
        self._headers_provider = provider
        self._client_id = self._client_secret = None
        return self

    def json(self):
        """Select the JSON record format."""
        self._json = True
        self._descriptor = None
        return self

    def compiled_proto(self, descriptor):
        """Select the protobuf record format.

        Args:
            descriptor: The serialized ``FileDescriptorProto`` as ``bytes``, or a protobuf
                ``Descriptor`` object, as accepted by ``TableProperties``.
        """
        self._descriptor = descriptor
        self._json = False
        return self

    def recovery(self, enabled: bool):
        """Enable or disable automatic stream recovery."""
        if not isinstance(enabled, bool):
            raise ValueError("recovery must be a bool")
        self._settings["recovery"] = enabled
        return self

    def recovery_timeout_ms(self, ms: int):
        """Set the timeout in milliseconds for each recovery attempt."""
        self._settings["recovery_timeout_ms"] = _int_in_range("recovery_timeout_ms", ms)
        return self

    def recovery_backoff_ms(self, ms: int):
        """Set the backoff time in milliseconds between recovery retries."""
        self._settings["recovery_backoff_ms"] = _int_in_range("recovery_backoff_ms", ms)
        return self

    def recovery_retries(self, n: int):
        """Set the maximum number of recovery retry attempts."""
        self._settings["recovery_retries"] = _int_in_range("recovery_retries", n, _MAX_U32)
        return self

    def server_lack_of_ack_timeout_ms(self, ms: int):
        """Set the timeout in milliseconds for server acknowledgment."""
        self._settings["server_lack_of_ack_timeout_ms"] = _int_in_range("server_lack_of_ack_timeout_ms", ms)
        return self

    def flush_timeout_ms(self, ms: int):
        """Set the timeout in milliseconds for flush operations."""
        self._settings["flush_timeout_ms"] = _int_in_range("flush_timeout_ms", ms)
        return self

    def max_inflight_requests(self, n: int):
        """Set the maximum number of in-flight requests. Must be at least 1."""
        self._settings["max_inflight_requests"] = _int_in_range("max_inflight_requests", n, minimum=1)
        return self

    def max_ingest_payload_bytes(self, n: int):
        """Set the maximum total encoded record size in bytes for one ingest call."""
        self._settings["max_ingest_payload_bytes"] = _int_in_range("max_ingest_payload_bytes", n)
        return self

    def ack_callback(self, callback):
        """Set an ``AckCallback`` that runs when the server acknowledges an ingest."""
        self._settings["ack_callback"] = callback
        return self

    def callback_max_wait_time_ms(self, ms: Optional[int]):
        """Set the maximum wait in milliseconds for callbacks after close. ``None`` waits without limit."""
        if ms is not None:
            _int_in_range("callback_max_wait_time_ms", ms)
        self._settings["callback_max_wait_time_ms_set"] = True
        self._settings["callback_max_wait_time_ms"] = ms
        return self

    def _native_args(self):
        """Validate the collected settings and return the arguments of the native open call."""
        if not self._table_name:
            raise ValueError("table is required: call table(...) before build() or resume()")
        has_oauth = self._client_id is not None or self._client_secret is not None
        if has_oauth and (self._client_id is None or self._client_secret is None):
            raise ValueError("oauth requires both client_id and client_secret")
        if has_oauth == (self._headers_provider is not None):
            raise ValueError("authentication is required: call exactly one of oauth(...) or headers_provider(...)")
        if self._descriptor is None and not self._json:
            raise ValueError("record format is required: call json() or compiled_proto(...)")

        table_properties = _core.TableProperties(self._table_name, self._descriptor)
        settings = _core.PersistentStreamSettings(
            client_id=self._client_id,
            client_secret=self._client_secret,
            headers_provider=self._headers_provider,
            **self._settings,
        )
        return table_properties, settings

    @staticmethod
    def _check_stream_id(stream_id: str) -> str:
        if not isinstance(stream_id, str) or not stream_id:
            raise ValueError("stream_id must be a non-empty string")
        return stream_id
