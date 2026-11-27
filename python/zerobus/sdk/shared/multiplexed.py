"""
Argument checks and native dispatch for ``create_multiplexed_stream`` (Beta), shared by the
sync and async SDKs.

A multiplexed stream opens several sub-streams to one table and distributes records
round-robin across them. Its ingest methods return a message ID: an opaque ``int`` that
identifies the sub-stream and the record's position in it.
"""

import zerobus._zerobus_core as _core
from zerobus.sdk.shared.auth import FederatedToken, OAuth, PersonalAccessToken

MAX_STREAM_COUNT = 64

_AUTH_TYPES = (OAuth, _core.HeadersProvider, FederatedToken, PersonalAccessToken)


def check_arguments(table_properties, stream_count, auth, options) -> None:
    """Validate the arguments of ``create_multiplexed_stream`` before any native call.

    Raises:
        ValueError: If an argument is missing, out of range, or of the wrong kind.
    """
    if table_properties is None:
        raise ValueError("table_properties is required")
    if not isinstance(table_properties, _core.TableProperties):
        raise ValueError("table_properties must be a TableProperties")
    if isinstance(stream_count, bool) or not isinstance(stream_count, int) or not 1 <= stream_count <= MAX_STREAM_COUNT:
        raise ValueError(f"stream_count must be an integer between 1 and {MAX_STREAM_COUNT}")
    # Like create_stream's headers_provider, any object with a callable get_headers() works.
    if not isinstance(auth, _AUTH_TYPES) and not callable(getattr(auth, "get_headers", None)):
        raise ValueError(
            "auth= must be an OAuth, HeadersProvider, FederatedToken, or PersonalAccessToken instance, "
            "or an object with a get_headers() method"
        )
    if options is None:
        return
    if not isinstance(options, _core.StreamConfigurationOptions):
        raise ValueError("options must be a MultiplexedStreamOptions or StreamConfigurationOptions")
    if not isinstance(options, _core.MultiplexedStreamOptions) and options.ack_callback is not None:
        raise ValueError(
            "ack_callback on StreamConfigurationOptions is not supported for multiplexed streams; "
            "pass it in MultiplexedStreamOptions"
        )


def call_native(native_sdk, table_properties, stream_count, auth, options, allow_async: bool):
    """Call the native create function for ``auth``'s kind and return its result.

    The result is the native stream in the sync SDK and an awaitable in the async SDK. For a
    ``FederatedToken``, a fresh native ``IdpSupplier`` is built for this call, as
    ``create_stream`` does: ``allow_async`` is ``True`` only in the async SDK, where the
    supplier captures the running event loop.
    """
    if isinstance(auth, OAuth):
        return native_sdk.create_multiplexed_stream(
            auth.client_id, auth.client_secret, table_properties, stream_count, options
        )
    if isinstance(auth, PersonalAccessToken):
        return native_sdk.create_multiplexed_stream_pat(table_properties, auth.token, stream_count, options)
    if isinstance(auth, FederatedToken):
        supplier = _core.IdpSupplier(auth.idp_token_supplier, allow_async, auth._cache_identity)
        return native_sdk.create_multiplexed_stream_federated(
            table_properties, supplier, auth.databricks_client_id, stream_count, options
        )
    return native_sdk.create_multiplexed_stream_with_headers_provider(table_properties, auth, stream_count, options)
