"""Tests for the external-IdP federation auth surface (FederatedToken).

These cover the pure-Python dispatch layer of ``create_stream`` with a fake
native ``_inner`` so no network or gRPC server is needed. The Rust core's
exchange/caching behavior is covered by the Rust unit tests; the Python->Rust
callback bridge is exercised end-to-end separately against a mock token
endpoint.
"""

import dataclasses

import pytest

import zerobus
from zerobus import FederatedToken, TableProperties, ZerobusSdk
from zerobus.sdk.aio import ZerobusSdk as AsyncZerobusSdk


class _FakeInner:
    """Records which native create_stream_* method the wrapper dispatched to."""

    def __init__(self):
        self.calls = []

    def create_stream_federated(self, table_properties, idp_supplier, idp_callback, databricks_client_id, options):
        self.calls.append(("federated", table_properties, idp_supplier, idp_callback, databricks_client_id, options))
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

    async def create_stream_federated(
        self, table_properties, idp_supplier, idp_callback, databricks_client_id, options
    ):
        self.calls.append(("federated", table_properties, idp_supplier, idp_callback, databricks_client_id, options))
        return object()

    async def create_stream(self, client_id, client_secret, table_properties, options):
        self.calls.append(("oauth", client_id, client_secret, table_properties, options))
        return object()

    async def create_stream_with_headers_provider(self, table_properties, headers_provider, options):
        self.calls.append(("headers", table_properties, headers_provider, options))
        return object()


def _sync_sdk_with_fake():
    sdk = ZerobusSdk(host="https://example", unity_catalog_url="https://example")
    fake = _FakeInner()
    sdk._inner = fake
    return sdk, fake


def _props():
    return TableProperties("cat.sch.tbl")


def test_federated_token_exported_and_constructs():
    assert "FederatedToken" in zerobus.__all__
    account = FederatedToken(idp_token_supplier=lambda: "tok")
    assert account.databricks_client_id is None
    workload = FederatedToken(idp_token_supplier=lambda: "tok", databricks_client_id="sp-uuid")
    assert workload.databricks_client_id == "sp-uuid"


def test_native_methods_present():
    import zerobus._zerobus_core as _core

    assert hasattr(_core.sync.ZerobusSdk, "create_stream_federated")
    assert hasattr(_core.aio.ZerobusSdk, "create_stream_federated")


def test_federated_token_owner_cycle_is_collectable():
    # A common pattern binds a supplier that is a method of some owner object:
    #     owner.auth = FederatedToken(idp_token_supplier=owner.get_token)
    # which forms a reference cycle owner -> auth -> bound method -> owner. That
    # cycle must remain collectable by the garbage collector. It is, because
    # FederatedToken holds no GC-invisible reference to the callback: its only link
    # is the plain Python `idp_token_supplier` attribute (the native supplier
    # handle is built per create_stream and never stored on the token), so the
    # collector can traverse and break the cycle. If a native handle (an object
    # wrapping the callback in a Rust Arc the GC cannot see) were ever stored back
    # on the token, this owner would leak.
    import gc
    import weakref

    class Owner:
        def __init__(self):
            self.auth = FederatedToken(idp_token_supplier=self.get_token)

        def get_token(self):
            return "tok"

    owner = Owner()
    # Sanity: the token stores no opaque native handle, only the plain callback.
    assert owner.auth.idp_token_supplier == owner.get_token
    ref = weakref.ref(owner)

    del owner
    gc.collect()
    assert ref() is None, "a FederatedToken must not keep its owner alive (no GC-invisible reference)"


def test_native_supplier_holds_callback_weakly():
    # The full leak Teodor flagged runs through the stream:
    #     owner.stream = sdk.create_stream(auth=FederatedToken(owner.get_token))
    # forms owner -> stream -> native supplier -> bound method -> owner. The stream
    # holds the sole strong, GC-visible reference to the callback (see the
    # __traverse__/__clear__ on the native ZerobusStream), while the native supplier
    # must hold it only WEAKLY, or that Rust-side (GC-invisible) strong reference
    # would pin the cycle and defeat the collector.
    #
    # This asserts the supplier half directly: a live IdpSupplier must NOT keep its
    # callback alive on its own. Under the previous strong capture this failed (the
    # Arc pinned the callback); with the weak reference the callback is collected
    # once its last real strong referrer is dropped.
    import gc
    import weakref

    import zerobus._zerobus_core as _core

    class Owner:
        def get_token(self):
            return "tok"

    owner = Owner()
    callback = owner.get_token
    ref = weakref.ref(callback)
    # The supplier is kept alive for the whole test; only it references `callback`
    # after the del below (weakly, if the fix holds).
    _supplier = _core.IdpSupplier(callback, False, "cache-id")

    del callback
    gc.collect()
    assert ref() is None, "the native IdpSupplier must hold the callback weakly, not pin it"
    # The supplier is still alive here, proving the callback died despite it.
    assert _supplier is not None


def test_create_stream_routes_auth_to_federated_account_level():
    sdk, fake = _sync_sdk_with_fake()

    def supplier():
        return "tok"

    auth = FederatedToken(idp_token_supplier=supplier)
    sdk.create_stream(table_properties=_props(), auth=auth)

    assert len(fake.calls) == 1
    kind, _tp, passed_supplier, passed_callback, client_id, _opts = fake.calls[0]
    assert kind == "federated"
    # A native supplier handle is passed (not the raw callback). The handle is
    # built fresh per stream (bound to this SDK's loop), while the FederatedToken
    # carries the stable cache identity that partitions the account-level cache.
    import zerobus._zerobus_core as _core

    assert isinstance(passed_supplier, _core.IdpSupplier)
    # The raw callback is also passed through, for the stream to hold strongly.
    assert passed_callback is supplier
    assert isinstance(auth._cache_identity, str) and auth._cache_identity
    assert client_id is None


def test_create_stream_routes_auth_to_federated_workload():
    sdk, fake = _sync_sdk_with_fake()
    sdk.create_stream(
        table_properties=_props(),
        auth=FederatedToken(idp_token_supplier=lambda: "tok", databricks_client_id="sp-uuid"),
    )
    kind, _tp, _sup, _cb, client_id, _opts = fake.calls[0]
    assert kind == "federated"
    assert client_id == "sp-uuid"


def test_federated_token_cache_identity_stable_and_distinct():
    # The account-level token cache is partitioned by the FederatedToken's stable
    # per-instance cache identity, NOT by the native handle: the handle is rebuilt
    # per stream (so each binds to its SDK's loop and sync/async policy — the N2
    # fix), while the cache identity stays constant for one instance (so its
    # streams share the exchanged token) and differs between instances (so they
    # isolate).
    sdk, fake = _sync_sdk_with_fake()
    a = FederatedToken(idp_token_supplier=lambda: "tok")
    b = FederatedToken(idp_token_supplier=lambda: "tok")

    sdk.create_stream(table_properties=_props(), auth=a)
    sdk.create_stream(table_properties=_props(), auth=b)
    sdk.create_stream(table_properties=_props(), auth=a)

    handle_a1, handle_b, handle_a2 = (fake.calls[0][2], fake.calls[1][2], fake.calls[2][2])
    assert handle_a1 is not None and handle_b is not None, "each stream must pass a supplier handle"
    # Handles are rebuilt per stream, never memoized (the N2 correctness fix).
    assert handle_a1 is not handle_a2, "each stream must build a fresh native handle bound to its SDK/loop"
    # Sharing/isolation is carried by the stable cache identity instead: constant
    # for one instance (its streams share), distinct between instances (isolate).
    assert a._cache_identity and b._cache_identity
    assert a._cache_identity != b._cache_identity, "distinct FederatedToken instances must isolate"


def test_federated_token_is_immutable():
    # FederatedToken is frozen: the cache identity is fixed at construction and
    # partitions the account-level token cache, so reassigning the callback on an
    # already-used instance would keep serving the first callback's cached token
    # (the new one only runs on a miss). A distinct callback must be a distinct
    # instance, so assignment must raise rather than silently mis-cache.
    auth = FederatedToken(idp_token_supplier=lambda: "tok")
    with pytest.raises(dataclasses.FrozenInstanceError):
        auth.idp_token_supplier = lambda: "other"
    with pytest.raises(dataclasses.FrozenInstanceError):
        auth.databricks_client_id = "sp-id"


def test_create_stream_oauth_path_unchanged():
    sdk, fake = _sync_sdk_with_fake()
    sdk.create_stream("cid", "secret", _props())
    assert fake.calls[0][0] == "oauth"
    assert fake.calls[0][1] == "cid"


def test_create_stream_oauth_four_positional_unchanged():
    # Protects the released positional signature (client_id, client_secret,
    # table_properties, options): all four must keep binding as before.
    sdk, fake = _sync_sdk_with_fake()
    options = object()
    sdk.create_stream("cid", "secret", _props(), options)
    kind, client_id, client_secret, _tp, passed_options = fake.calls[0]
    assert (kind, client_id, client_secret) == ("oauth", "cid", "secret")
    assert passed_options is options


def test_auth_is_keyword_only():
    # auth is keyword-only: a positional (here 6th) argument is rejected, so it
    # can never be confused with the positional OAuth/headers parameters.
    sdk, _fake = _sync_sdk_with_fake()
    with pytest.raises(TypeError):
        sdk.create_stream("cid", "secret", _props(), None, None, FederatedToken(idp_token_supplier=lambda: "tok"))


def test_auth_with_positional_table_properties_raises_helpful_error():
    # The federation footgun: passing table_properties positionally lands it in
    # client_id. The client_id/auth conflict must raise a message naming the fix
    # rather than the misleading "table_properties is required".
    sdk, _fake = _sync_sdk_with_fake()
    with pytest.raises(ValueError, match="cannot be combined with auth="):
        sdk.create_stream(_props(), auth=FederatedToken(idp_token_supplier=lambda: "tok"))


def test_auth_takes_precedence_over_headers_provider():
    sdk, fake = _sync_sdk_with_fake()
    sdk.create_stream(
        table_properties=_props(),
        auth=FederatedToken(idp_token_supplier=lambda: "tok"),
        headers_provider=object(),
    )
    assert fake.calls[0][0] == "federated"


def test_create_stream_requires_auth_or_credentials():
    sdk, _fake = _sync_sdk_with_fake()
    with pytest.raises(ValueError):
        sdk.create_stream(table_properties=_props())


def test_create_stream_requires_table_properties():
    sdk, _fake = _sync_sdk_with_fake()
    with pytest.raises(ValueError):
        sdk.create_stream(auth=FederatedToken(idp_token_supplier=lambda: "tok"))


@pytest.mark.asyncio
async def test_async_create_stream_routes_to_federated():
    sdk = AsyncZerobusSdk(host="https://example", unity_catalog_url="https://example")
    fake = _AsyncFakeInner()
    sdk._inner = fake

    await sdk.create_stream(
        table_properties=_props(),
        auth=FederatedToken(idp_token_supplier=lambda: "tok", databricks_client_id="sp"),
    )
    assert fake.calls[0][0] == "federated"
    assert fake.calls[0][4] == "sp"


@pytest.mark.asyncio
async def test_async_auth_with_positional_table_properties_raises_helpful_error():
    sdk = AsyncZerobusSdk(host="https://example", unity_catalog_url="https://example")
    sdk._inner = _AsyncFakeInner()
    with pytest.raises(ValueError, match="cannot be combined with auth="):
        await sdk.create_stream(_props(), auth=FederatedToken(idp_token_supplier=lambda: "tok"))
