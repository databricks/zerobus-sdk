"""Tests for personal access token (PAT) authentication (PersonalAccessToken).

The dispatch tests use a fake native ``_inner`` so no network or gRPC server is
needed. The native tests point at a refused local port: reaching a Zerobus error
there proves the token was accepted and forwarded to the Rust core. The header
shape the core sends is covered by the Rust unit tests.
"""

import asyncio
import dataclasses

import pytest

import zerobus
import zerobus.sdk.shared
from zerobus import (
    NonRetriableException,
    PersonalAccessToken,
    StreamConfigurationOptions,
    TableProperties,
    ZerobusException,
    ZerobusSdk,
)
from zerobus import _zerobus_core as core
from zerobus.sdk import aio, sync
from zerobus.sdk.aio import ZerobusSdk as AsyncZerobusSdk

TOKEN = "dapi-test-secret"
LOCAL_REFUSED = "http://127.0.0.1:1"
NO_RECOVERY = StreamConfigurationOptions(recovery=False)


class _FakeInner:
    """Records which native create_stream_* method the wrapper dispatched to."""

    def __init__(self):
        self.calls = []

    def create_stream_pat(self, table_properties, token, options):
        self.calls.append(("pat", table_properties, token, options))
        return object()

    def create_stream(self, client_id, client_secret, table_properties, options):
        self.calls.append(("oauth", client_id, client_secret, table_properties, options))
        return object()

    def create_stream_with_headers_provider(self, table_properties, headers_provider, options):
        self.calls.append(("headers", table_properties, headers_provider, options))
        return object()


class _AsyncFakeInner:
    def __init__(self):
        self.calls = []

    async def create_stream_pat(self, table_properties, token, options):
        self.calls.append(("pat", table_properties, token, options))
        return object()


class _Headers(core.HeadersProvider):
    def get_headers(self):
        return [("authorization", "Bearer token")]


def _props():
    return TableProperties("cat.sch.tbl")


def _sync_sdk_with_fake():
    sdk = ZerobusSdk(host="https://example", unity_catalog_url="https://example")
    fake = _FakeInner()
    sdk._inner = fake
    return sdk, fake


def _async_sdk_with_fake():
    sdk = AsyncZerobusSdk(host="https://example", unity_catalog_url="https://example")
    fake = _AsyncFakeInner()
    sdk._inner = fake
    return sdk, fake


class TestPersonalAccessToken:
    def test_exported(self):
        assert "PersonalAccessToken" in zerobus.__all__
        assert "PersonalAccessToken" in zerobus.sdk.shared.__all__
        assert zerobus.sdk.shared.PersonalAccessToken is PersonalAccessToken

    def test_holds_token(self):
        assert PersonalAccessToken(TOKEN).token == TOKEN
        assert PersonalAccessToken(token=TOKEN).token == TOKEN

    @pytest.mark.parametrize("bad", ["", None, b"dapi-bytes", 123])
    def test_rejects_empty_or_non_string_token(self, bad):
        with pytest.raises(ValueError):
            PersonalAccessToken(bad)

    def test_rejection_does_not_echo_the_value(self):
        with pytest.raises(ValueError) as info:
            PersonalAccessToken(b"dapi-bytes")
        assert "dapi-bytes" not in str(info.value)

    def test_repr_and_str_hide_token(self):
        auth = PersonalAccessToken(TOKEN)
        assert TOKEN not in repr(auth)
        assert TOKEN not in str(auth)
        assert "PersonalAccessToken" in repr(auth)

    def test_is_immutable(self):
        auth = PersonalAccessToken(TOKEN)
        with pytest.raises(dataclasses.FrozenInstanceError):
            auth.token = "other"


class TestCreateStreamDispatch:
    def test_sync_routes_to_native_pat(self):
        sdk, fake = _sync_sdk_with_fake()
        props = _props()
        options = object()
        sdk.create_stream(table_properties=props, options=options, auth=PersonalAccessToken(TOKEN))
        assert fake.calls == [("pat", props, TOKEN, options)]

    def test_async_routes_to_native_pat(self):
        sdk, fake = _async_sdk_with_fake()
        props = _props()
        asyncio.run(sdk.create_stream(table_properties=props, auth=PersonalAccessToken(TOKEN)))
        assert fake.calls == [("pat", props, TOKEN, None)]

    @pytest.mark.parametrize(
        "credentials", [{"client_id": "cid"}, {"client_secret": "secret"}, {"client_id": "cid", "client_secret": "s"}]
    )
    def test_sync_rejects_pat_with_client_credentials(self, credentials):
        sdk, fake = _sync_sdk_with_fake()
        with pytest.raises(ValueError, match="cannot be combined with auth="):
            sdk.create_stream(table_properties=_props(), auth=PersonalAccessToken(TOKEN), **credentials)
        assert fake.calls == []

    def test_async_rejects_pat_with_client_credentials(self):
        sdk, fake = _async_sdk_with_fake()
        with pytest.raises(ValueError, match="cannot be combined with auth="):
            asyncio.run(sdk.create_stream("cid", "secret", _props(), auth=PersonalAccessToken(TOKEN)))
        assert fake.calls == []

    def test_pat_takes_precedence_over_headers_provider(self):
        sdk, fake = _sync_sdk_with_fake()
        sdk.create_stream(table_properties=_props(), auth=PersonalAccessToken(TOKEN), headers_provider=_Headers())
        assert fake.calls[0][0] == "pat"

    def test_requires_table_properties(self):
        sdk, _fake = _sync_sdk_with_fake()
        with pytest.raises(ValueError, match="table_properties is required"):
            sdk.create_stream(auth=PersonalAccessToken(TOKEN))

    @pytest.mark.parametrize("bad_auth", [TOKEN, object()])
    def test_rejects_unknown_auth_type(self, bad_auth):
        # A bare string must not be mistaken for a token: only PersonalAccessToken is.
        sdk, fake = _sync_sdk_with_fake()
        with pytest.raises(ValueError, match="PersonalAccessToken"):
            sdk.create_stream(table_properties=_props(), auth=bad_auth)
        assert fake.calls == []


class TestNativeCreateStreamPat:
    def test_native_methods_present(self):
        assert hasattr(core.sync.ZerobusSdk, "create_stream_pat")
        assert hasattr(core.aio.ZerobusSdk, "create_stream_pat")

    def test_sync_reaches_network_without_leaking_token(self):
        sdk = ZerobusSdk(LOCAL_REFUSED, LOCAL_REFUSED)
        with pytest.raises(ZerobusException) as info:
            sdk.create_stream(table_properties=_props(), options=NO_RECOVERY, auth=PersonalAccessToken(TOKEN))
        assert TOKEN not in str(info.value)

    def test_async_reaches_network_without_leaking_token(self):
        sdk = AsyncZerobusSdk(LOCAL_REFUSED, LOCAL_REFUSED)
        with pytest.raises(ZerobusException) as info:
            asyncio.run(
                sdk.create_stream(table_properties=_props(), options=NO_RECOVERY, auth=PersonalAccessToken(TOKEN))
            )
        assert TOKEN not in str(info.value)

    def test_core_rejects_empty_token_as_non_retriable(self):
        # Bypasses the Python check to confirm the Rust core guards an empty token too.
        sdk = core.sync.ZerobusSdk(LOCAL_REFUSED, LOCAL_REFUSED)
        with pytest.raises(NonRetriableException):
            sdk.create_stream_pat(_props(), "", NO_RECOVERY)


@pytest.fixture(params=["sync", "aio"])
def mode(request):
    return request.param


@pytest.fixture
def persistent_builder(mode):
    sdk = (sync if mode == "sync" else aio).ZerobusSdk(LOCAL_REFUSED, LOCAL_REFUSED)
    return sdk.persistent_stream_builder().table("c.s.t").json()


class TestPersistentBuilder:
    def test_personal_access_token_passes_validation(self, persistent_builder):
        assert persistent_builder.personal_access_token(TOKEN) is persistent_builder
        persistent_builder._native_args()

    @pytest.mark.parametrize("bad", ["", None])
    def test_rejects_empty_token(self, persistent_builder, bad):
        with pytest.raises(ValueError):
            persistent_builder.personal_access_token(bad)

    def test_rejected_token_keeps_earlier_auth(self, persistent_builder):
        persistent_builder.oauth("id", "secret")
        with pytest.raises(ValueError):
            persistent_builder.personal_access_token("")
        assert persistent_builder._client_id == "id"
        persistent_builder._native_args()

    def test_last_auth_setter_wins(self, persistent_builder):
        persistent_builder.oauth("id", "secret").personal_access_token(TOKEN)
        assert persistent_builder._client_id is None and persistent_builder._client_secret is None
        assert persistent_builder._personal_access_token == TOKEN
        persistent_builder._native_args()

        persistent_builder.headers_provider(_Headers())
        assert persistent_builder._personal_access_token is None
        persistent_builder._native_args()

        persistent_builder.personal_access_token(TOKEN)
        assert persistent_builder._headers_provider is None
        persistent_builder._native_args()

        persistent_builder.oauth("id", "secret")
        assert persistent_builder._personal_access_token is None
        persistent_builder._native_args()

    def test_reaches_network(self, persistent_builder, mode):
        builder = persistent_builder.personal_access_token(TOKEN).recovery(False)
        with pytest.raises(ZerobusException) as info:
            if mode == "sync":
                builder.build()
            else:
                asyncio.run(builder.build())
        assert TOKEN not in str(info.value)

    def test_native_settings_require_exactly_one_auth_method(self):
        sdk = core.sync.ZerobusSdk(LOCAL_REFUSED, LOCAL_REFUSED)
        props = core.TableProperties("c.s.t")
        for settings in (
            core.PersistentStreamSettings(personal_access_token=TOKEN, client_id="id", client_secret="s"),
            core.PersistentStreamSettings(personal_access_token=TOKEN, headers_provider=_Headers()),
        ):
            with pytest.raises(ValueError, match="exactly one authentication method"):
                sdk.open_persistent_stream(props, settings)
