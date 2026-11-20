"""
Settings shared by the fluent stream builders, ``stream_builder()`` and ``persistent_stream_builder()``.

Each builder collects its settings in pure Python and validates them before it opens a
stream, so the setters only check types and ranges. Unset settings are never forwarded, so
the Rust core keeps ownership of the defaults.
"""

from typing import Any, Optional

_MAX_U32 = 2**32 - 1
_MAX_I64 = 2**63 - 1


def _int_in_range(name: str, value: int, maximum: int = _MAX_I64, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer between {minimum} and {maximum}")
    return value


class _StreamBuilderBase:
    """Collects the table, authentication, record format, and stream settings. Setters return the builder."""

    # Named in validation errors: the methods that open the stream, and how to select a format.
    _OPEN_METHODS = "build()"
    _FORMAT_METHODS = "json() or compiled_proto(...)"
    # Named in the "authentication is required" error: the builder's authentication setters.
    _AUTH_METHODS = "oauth(...) or headers_provider(...)"

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

    def _clear_auth(self) -> None:
        """Forget the authentication method. Builders with more methods extend this."""
        self._client_id = self._client_secret = None
        self._headers_provider = None

    def _auth_methods_set(self) -> int:
        """Count the authentication methods that are set. Builders with more methods extend this."""
        has_oauth = self._client_id is not None or self._client_secret is not None
        return int(has_oauth) + int(self._headers_provider is not None)

    def oauth(self, client_id: str, client_secret: str):
        """Authenticate with OAuth client credentials. Replaces an earlier authentication setter."""
        self._clear_auth()
        self._client_id = client_id
        self._client_secret = client_secret
        return self

    def headers_provider(self, provider):
        """Authenticate with a custom ``HeadersProvider``. Replaces an earlier authentication setter."""
        self._clear_auth()
        self._headers_provider = provider
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

    def _format_selected(self) -> bool:
        """Whether a record format setter was called."""
        return self._descriptor is not None or self._json

    def _check_required(self):
        """Validate that the table, exactly one authentication method, and a format are set."""
        if not self._table_name:
            raise ValueError(f"table is required: call table(...) before {self._OPEN_METHODS}")
        has_oauth = self._client_id is not None or self._client_secret is not None
        if has_oauth and (self._client_id is None or self._client_secret is None):
            raise ValueError("oauth requires both client_id and client_secret")
        if self._auth_methods_set() != 1:
            raise ValueError(f"authentication is required: call exactly one of {self._AUTH_METHODS}")
        if not self._format_selected():
            raise ValueError(f"record format is required: call {self._FORMAT_METHODS}")
