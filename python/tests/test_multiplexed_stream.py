"""Tests for multiplexed streams (Beta) created by ``create_multiplexed_stream``.

These tests need only the built extension (no network, no credentials). The native tests point
at a refused local port: reaching a Zerobus error there proves the arguments crossed into the
Rust core. Ingestion, acknowledgments, and recovery against a server are covered by the Rust
core tests and by ``test_multiplexed_integration.py``.
"""

import asyncio
import dataclasses
import gc
import inspect
import io
import json
import time
import weakref

import pytest

import zerobus
import zerobus.sdk.shared
from zerobus import FederatedToken, OAuth, PersonalAccessToken
from zerobus import _zerobus_core as core
from zerobus.sdk import aio, sync

# Nothing listens here, so an open fails fast with a Zerobus error.
UNREACHABLE = ("http://127.0.0.1:1", "http://127.0.0.1:1")


class _Headers(core.HeadersProvider):
    def __init__(self):
        super().__init__()
        self.calls = 0

    def get_headers(self):
        self.calls += 1
        return [("authorization", "Bearer token")]


class _Ack(core.AckCallback):
    def on_ack(self, offset):
        pass


@pytest.fixture(params=["sync", "aio"])
def mode(request):
    return request.param


def _run(mode, body):
    """Run ``body(native_sdk)`` against the native SDK of ``mode``, inside an event loop for aio.

    The async natives need a running loop; in sync mode the loop only hosts the call.
    """
    native = (core.sync if mode == "sync" else core.aio).ZerobusSdk(*UNREACHABLE)

    async def run():
        return await body(native)

    return asyncio.run(run())


async def _maybe(result):
    """Await ``result`` when it comes from the async native SDK."""
    return await result if inspect.isawaitable(result) else result


def _create(native, mode, auth_kind, stream_count, options, headers=None, idp_calls=None):
    """Call the native create function for ``auth_kind``."""
    props = core.TableProperties("c.s.t")
    if auth_kind == "oauth":
        return native.create_multiplexed_stream("id", "secret", props, stream_count, options)
    if auth_kind == "headers":
        return native.create_multiplexed_stream_with_headers_provider(
            props, headers or _Headers(), stream_count, options
        )
    if auth_kind == "pat":
        return native.create_multiplexed_stream_pat(props, "dapi-token", stream_count, options)

    def idp_token():
        if idp_calls is not None:
            idp_calls.append(1)
        return "idp-token"

    supplier = core.IdpSupplier(idp_token, mode == "aio", "cache-id")
    return native.create_multiplexed_stream_federated(props, supplier, None, stream_count, options)


AUTH_KINDS = ["oauth", "headers", "federated", "pat"]


class TestMultiplexedStreamOptions:
    def test_is_a_stream_configuration_options(self):
        options = core.MultiplexedStreamOptions(recovery=False)
        assert isinstance(options, core.StreamConfigurationOptions)
        assert options.recovery is False
        assert options.ack_callback is None

    @pytest.mark.parametrize("base", [core.StreamConfigurationOptions, core.MultiplexedStreamOptions])
    def test_python_subclass_with_its_own_init(self, base):
        class Budgeted(base):
            def __init__(self, budget):
                super().__init__(max_inflight_records=budget, recovery=False)

        options = Budgeted(7)
        assert (options.max_inflight_records, options.recovery) == (7, False)

    def test_holds_the_ack_callback(self):
        callback = _Ack()
        assert core.MultiplexedStreamOptions(ack_callback=callback).ack_callback is callback

    def test_rejects_an_ack_callback_that_is_not_an_ack_callback(self):
        with pytest.raises(TypeError):
            core.MultiplexedStreamOptions(ack_callback=lambda message_id: None)


class TestStreamConfigurationOptions:
    """The ordinary options keep main's constructor contract now that Python can subclass them."""

    def test_defaults_are_unchanged(self):
        options = core.StreamConfigurationOptions()
        assert (
            options.max_inflight_records,
            options.recovery,
            options.recovery_timeout_ms,
            options.recovery_backoff_ms,
            options.recovery_retries,
            options.server_lack_of_ack_timeout_ms,
            options.flush_timeout_ms,
            options.record_type,
            options.stream_paused_max_wait_time_ms,
            options.callback_max_wait_time_ms,
            options.ack_callback,
        ) == (1_000_000, True, 15_000, 2_000, 4, 60_000, 300_000, core.RecordType.UNSPECIFIED, None, 5_000, None)

    def test_rejects_a_positional_argument(self):
        with pytest.raises(TypeError):
            core.StreamConfigurationOptions(False)

    def test_rejects_an_unknown_option(self):
        with pytest.raises(ValueError, match="Unknown configuration option: nope"):
            core.StreamConfigurationOptions(nope=1)

    @pytest.mark.parametrize("kwargs", [{"recovery": "yes"}, {"max_inflight_records": "many"}])
    def test_rejects_a_value_of_the_wrong_type(self, kwargs):
        with pytest.raises(TypeError):
            core.StreamConfigurationOptions(**kwargs)


class TestNativeCreate:
    def test_rejects_an_ordinary_ack_callback(self, mode):
        # The native call rejects a plain StreamConfigurationOptions with ack_callback before the network.
        async def body(native):
            options = core.StreamConfigurationOptions(ack_callback=_Ack())
            with pytest.raises(ValueError, match="MultiplexedStreamOptions"):
                native.create_multiplexed_stream_with_headers_provider(
                    core.TableProperties("c.s.t"), _Headers(), 2, options
                )

        _run(mode, body)

    @pytest.mark.parametrize("auth_kind", AUTH_KINDS)
    def test_every_auth_kind_reaches_the_network(self, mode, auth_kind):
        # The multiplexed callback goes the multiplexed route: the core rejects an ordinary
        # callback on a multiplexed build. recovery=False reports the failed connection at once.
        headers = _Headers()
        idp_calls = []
        options = core.MultiplexedStreamOptions(recovery=False, ack_callback=_Ack())

        async def body(native):
            with pytest.raises(core.ZerobusException) as info:
                await _maybe(_create(native, mode, auth_kind, 1, options, headers, idp_calls))
            return info.value

        error = _run(mode, body)
        assert not isinstance(error, core.NonRetriableException), error
        # The open asks for the headers or the IdP token once before it fails to connect.
        assert headers.calls == (1 if auth_kind == "headers" else 0)
        assert len(idp_calls) == (1 if auth_kind == "federated" else 0)

    @pytest.mark.parametrize("auth_kind", AUTH_KINDS)
    def test_options_and_stream_count_reach_the_core(self, mode, auth_kind):
        # The core divides max_inflight_records across sub-streams and rejects a budget below
        # the stream count before opening anything.
        options = core.MultiplexedStreamOptions(max_inflight_records=2)

        async def body(native):
            with pytest.raises(core.NonRetriableException):
                await _maybe(_create(native, mode, auth_kind, 4, options))

        _run(mode, body)

    def test_record_type_reaches_the_core(self, mode):
        options = core.MultiplexedStreamOptions(recovery=False, record_type=core.RecordType.PROTO)

        async def body(native):
            with pytest.raises(ValueError, match="conflicts with the schema"):
                await _maybe(_create(native, mode, "headers", 1, options))

        _run(mode, body)

    @pytest.mark.parametrize("with_callback", [False, True], ids=["no_callback", "callback"])
    def test_create_stream_accepts_multiplexed_options(self, mode, with_callback):
        # An ordinary stream takes a MultiplexedStreamOptions like its base class; its callback
        # then receives offsets.
        callback = _Ack() if with_callback else None
        options = core.MultiplexedStreamOptions(recovery=False, ack_callback=callback)

        async def body(native):
            with pytest.raises(core.ZerobusException) as info:
                await _maybe(native.create_stream("id", "secret", core.TableProperties("c.s.t"), options))
            return info.value

        error = _run(mode, body)
        assert not isinstance(error, core.NonRetriableException), error


def _assert_released(ref):
    """Wait until the object behind ``ref`` is collected; fail if it never is.

    An async open runs on a Rust task, which can release its references after ``asyncio.run()``
    has returned, and PyO3 applies a release made without the GIL the next time native code runs.
    So collect, call into the extension, and retry for a bounded time. A leak still fails.
    """
    deadline = time.monotonic() + 5
    gc.collect()
    while ref() is not None and time.monotonic() < deadline:
        core.TableProperties("c.s.t")  # native code applies pending releases
        time.sleep(0.01)
        gc.collect()
    assert ref() is None, "the callback owner is still alive: a native reference keeps it"


def test_failed_federated_create_does_not_keep_the_callback_owner_alive(mode):
    class Owner:
        def get_token(self):
            return "idp-token"

    def fail(owner):
        async def body(native):
            supplier = core.IdpSupplier(owner.get_token, mode == "aio", "cache-id")
            options = core.MultiplexedStreamOptions(recovery=False)
            try:
                await _maybe(
                    native.create_multiplexed_stream_federated(
                        core.TableProperties("c.s.t"), supplier, None, 1, options
                    )
                )
            except core.ZerobusException as exc:
                # The traceback keeps the frame that holds the native supplier.
                owner.err = exc

        _run(mode, body)

    owner = Owner()
    fail(owner)
    assert isinstance(owner.err, core.ZerobusException)
    ref = weakref.ref(owner)
    del owner
    _assert_released(ref)


# ---------------------------------------------------------------------------------------------
# The Python API: create_multiplexed_stream() checks its arguments, then calls one native per
# auth kind. A fake native SDK records those calls.
# ---------------------------------------------------------------------------------------------

TOKEN = "dapi-test-secret"


class _FakeNativeStream:
    """Records what the Python MultiplexedStream passes to the native stream."""

    def __init__(self, asynchronous):
        self._asynchronous = asynchronous
        self.ingested = []

    def result(self, value):
        if not self._asynchronous:
            return value

        async def done():
            return value

        return done()

    def ingest_record(self, payload):
        self.ingested.append(payload)
        return self.result(0)

    def ingest_records(self, payloads):
        self.ingested.append(payloads)
        return self.result(1)


class _FakeNativeSdk:
    """Records which native create function the Python layer calls, with its arguments."""

    def __init__(self, asynchronous):
        self.calls = []
        self.stream = _FakeNativeStream(asynchronous)

    def _record(self, kind, *args):
        self.calls.append((kind, *args))
        return self.stream.result(self.stream)

    def create_multiplexed_stream(self, client_id, client_secret, table_properties, stream_count, options):
        return self._record("oauth", client_id, client_secret, table_properties, stream_count, options)

    def create_multiplexed_stream_with_headers_provider(
        self, table_properties, headers_provider, stream_count, options
    ):
        return self._record("headers", table_properties, headers_provider, stream_count, options)

    def create_multiplexed_stream_federated(
        self, table_properties, idp_supplier, databricks_client_id, stream_count, options
    ):
        return self._record("federated", table_properties, idp_supplier, databricks_client_id, stream_count, options)

    def create_multiplexed_stream_pat(self, table_properties, token, stream_count, options):
        return self._record("pat", table_properties, token, stream_count, options)


def _sdk_with_fake(mode):
    sdk = (sync if mode == "sync" else aio).ZerobusSdk("https://example", "https://example")
    fake = _FakeNativeSdk(asynchronous=mode == "aio")
    sdk._inner = fake
    return sdk, fake


def _open(mode, sdk, *args, **kwargs):
    """Call create_multiplexed_stream, awaiting it in async mode."""
    if mode == "sync":
        return sdk.create_multiplexed_stream(*args, **kwargs)
    return asyncio.run(sdk.create_multiplexed_stream(*args, **kwargs))


class TestArgumentChecks:
    def test_rejects_a_table_properties_of_the_wrong_type(self, mode):
        sdk, fake = _sdk_with_fake(mode)
        with pytest.raises(ValueError, match="must be a TableProperties"):
            _open(mode, sdk, "c.s.t", 2, auth=OAuth("id", "secret"))
        assert fake.calls == []

    def test_requires_auth(self, mode):
        sdk, fake = _sdk_with_fake(mode)
        with pytest.raises(TypeError):
            _open(mode, sdk, core.TableProperties("c.s.t"), 2)
        assert fake.calls == []

    @pytest.mark.parametrize(
        "args, kwargs",
        [
            ((None, 2), {"auth": OAuth("id", "secret")}),
            ((core.TableProperties("c.s.t"), 2), {"auth": None}),
            ((core.TableProperties("c.s.t"), 2), {"auth": object()}),
            ((core.TableProperties("c.s.t"), 2), {"auth": OAuth("id", "secret"), "options": {}}),
        ],
        ids=["no_table", "no_auth", "unknown_auth", "options_not_options"],
    )
    def test_rejects_bad_arguments_before_any_native_call(self, mode, args, kwargs):
        sdk, fake = _sdk_with_fake(mode)
        with pytest.raises(ValueError):
            _open(mode, sdk, *args, **kwargs)
        assert fake.calls == []

    @pytest.mark.parametrize("stream_count", [0, 65, -1, True, "4", 2.0])
    def test_rejects_a_stream_count_outside_1_to_64(self, mode, stream_count):
        sdk, fake = _sdk_with_fake(mode)
        with pytest.raises(ValueError, match="stream_count must be an integer between 1 and 64"):
            _open(mode, sdk, core.TableProperties("c.s.t"), stream_count, auth=OAuth("id", "secret"))
        assert fake.calls == []

    @pytest.mark.parametrize("stream_count", [1, 64])
    def test_accepts_the_stream_count_bounds(self, mode, stream_count):
        sdk, fake = _sdk_with_fake(mode)
        _open(mode, sdk, core.TableProperties("c.s.t"), stream_count, auth=OAuth("id", "secret"))
        assert len(fake.calls) == 1

    def test_rejects_an_ordinary_ack_callback(self, mode):
        sdk, fake = _sdk_with_fake(mode)
        options = core.StreamConfigurationOptions(ack_callback=_Ack())
        with pytest.raises(ValueError, match="MultiplexedStreamOptions"):
            _open(mode, sdk, core.TableProperties("c.s.t"), 2, auth=OAuth("id", "secret"), options=options)
        assert fake.calls == []

    @pytest.mark.parametrize(
        "options",
        [None, core.StreamConfigurationOptions(), core.MultiplexedStreamOptions(ack_callback=_Ack())],
        ids=["omitted", "plain_without_callback", "multiplexed_with_callback"],
    )
    def test_passes_the_options_through(self, mode, options):
        sdk, fake = _sdk_with_fake(mode)
        _open(mode, sdk, core.TableProperties("c.s.t"), 2, auth=OAuth("id", "secret"), options=options)
        assert fake.calls[0][-1] is options


class TestAuthDispatch:
    def test_each_auth_kind_calls_its_native_with_unchanged_arguments(self, mode):
        headers = _Headers()
        props = core.TableProperties("c.s.t")
        options = core.MultiplexedStreamOptions()
        for auth, expected in (
            (OAuth("cid", "s"), ("oauth", "cid", "s", props, 3, options)),
            (headers, ("headers", props, headers, 3, options)),
            (PersonalAccessToken(TOKEN), ("pat", props, TOKEN, 3, options)),
        ):
            sdk, fake = _sdk_with_fake(mode)
            _open(mode, sdk, props, 3, auth=auth, options=options)
            assert fake.calls == [expected]

        # The federated supplier handle and client ID are checked in TestFederatedSupplier.
        sdk, fake = _sdk_with_fake(mode)
        _open(mode, sdk, props, 3, auth=FederatedToken(lambda: "idp-token"), options=options)
        assert len(fake.calls) == 1
        kind, table_properties, *_, stream_count, passed_options = fake.calls[0]
        assert (kind, table_properties, stream_count, passed_options) == ("federated", props, 3, options)

    def test_any_object_with_get_headers_is_a_headers_provider(self, mode):
        # As with create_stream's headers_provider, no HeadersProvider subclass is needed.
        class LooksLikeHeaders:
            def get_headers(self):
                return []

        provider = LooksLikeHeaders()
        props = core.TableProperties("c.s.t")
        sdk, fake = _sdk_with_fake(mode)
        _open(mode, sdk, props, 2, auth=provider)
        assert fake.calls == [("headers", props, provider, 2, None)]


class _SupplierSpy:
    """Records each native IdpSupplier the Python layer creates, and forwards to the real class."""

    def __init__(self, monkeypatch):
        # Saved before patching: isinstance() against the patched name would raise TypeError.
        self.real = core.IdpSupplier
        self.calls = []

        def supplier(callback, allow_async, cache_identity):
            handle = self.real(callback, allow_async, cache_identity)
            self.calls.append((callback, allow_async, cache_identity, handle))
            return handle

        monkeypatch.setattr(core, "IdpSupplier", supplier)


class TestFederatedSupplier:
    def test_builds_a_fresh_supplier_per_create(self, monkeypatch, mode):
        spy = _SupplierSpy(monkeypatch)
        sdk, fake = _sdk_with_fake(mode)
        token = FederatedToken(lambda: "idp-token")
        props = core.TableProperties("c.s.t")
        _open(mode, sdk, props, 2, auth=token)
        _open(mode, sdk, props, 2, auth=token)

        assert [call[:3] for call in spy.calls] == [
            (token.idp_token_supplier, mode == "aio", token._cache_identity)
        ] * 2
        first, second = (call[3] for call in spy.calls)
        assert first is not second
        assert isinstance(first, spy.real)
        assert [call[2] for call in fake.calls] == [first, second]

    def test_sync_create_inside_an_event_loop_keeps_the_sync_policy(self, monkeypatch):
        spy = _SupplierSpy(monkeypatch)
        sdk, _ = _sdk_with_fake("sync")

        async def create_in_loop():
            sdk.create_multiplexed_stream(core.TableProperties("c.s.t"), 2, auth=FederatedToken(lambda: "t"))

        asyncio.run(create_in_loop())
        assert [call[1] for call in spy.calls] == [False]

    @pytest.mark.parametrize("databricks_client_id", [None, "sp"], ids=["account", "workload"])
    def test_passes_the_databricks_client_id(self, mode, databricks_client_id):
        # None selects account-level federation; a client ID selects workload identity federation.
        sdk, fake = _sdk_with_fake(mode)
        token = FederatedToken(lambda: "t", databricks_client_id=databricks_client_id)
        _open(mode, sdk, core.TableProperties("c.s.t"), 2, auth=token)
        assert fake.calls[0][3] == databricks_client_id


class TestOAuth:
    def test_exported(self):
        assert "OAuth" in zerobus.__all__
        assert "OAuth" in zerobus.sdk.shared.__all__
        assert zerobus.sdk.shared.OAuth is OAuth

    def test_holds_the_credentials(self):
        assert (OAuth("id", "secret").client_id, OAuth("id", "secret").client_secret) == ("id", "secret")
        assert OAuth(client_id="id", client_secret="secret").client_secret == "secret"

    @pytest.mark.parametrize("client_id, client_secret", [("", "s"), ("id", ""), (None, "s"), ("id", b"s")])
    def test_rejects_empty_or_non_string_fields(self, client_id, client_secret):
        with pytest.raises(ValueError, match="must be a non-empty string"):
            OAuth(client_id, client_secret)

    def test_repr_hides_the_secret(self):
        assert "s3cr3t" not in repr(OAuth("id", "s3cr3t"))

    def test_is_immutable(self):
        with pytest.raises(dataclasses.FrozenInstanceError):
            OAuth("id", "secret").client_secret = "other"


AVRO_SCHEMA = json.dumps({"type": "record", "name": "R", "fields": [{"name": "a", "type": "string"}]})


class TestPayloadEncoding:
    def _ingest(self, mode, table_properties, calls):
        sdk, fake = _sdk_with_fake(mode)
        stream = _open(mode, sdk, table_properties, 2, auth=OAuth("id", "secret"))

        async def run():
            for method, payload in calls:
                result = getattr(stream, method)(payload)
                if mode == "aio":
                    await result

        asyncio.run(run())
        return fake.stream.ingested

    def test_avro_records_are_encoded(self, mode):
        import fastavro

        encoded = io.BytesIO()
        fastavro.schemaless_writer(encoded, fastavro.parse_schema(json.loads(AVRO_SCHEMA)), {"a": "x"})
        avro = encoded.getvalue()
        ingested = self._ingest(
            mode,
            core.TableProperties("c.s.t", avro_schema=AVRO_SCHEMA),
            [("ingest_record", {"a": "x"}), ("ingest_record", avro), ("ingest_records", [{"a": "x"}, {"a": "x"}])],
        )
        assert ingested == [avro, avro, [avro, avro]]

    def test_json_records_pass_through(self, mode):
        # The native stream extracts JSON strings and dicts itself.
        record = {"a": "x"}
        ingested = self._ingest(
            mode, core.TableProperties("c.s.t"), [("ingest_record", '{"a": "x"}'), ("ingest_record", record)]
        )
        assert ingested == ['{"a": "x"}', record]


class TestPublicSurface:
    def test_exports(self, mode):
        module = sync if mode == "sync" else aio
        for name in ("MultiplexedStream", "MultiplexedStreamOptions"):
            assert name in module.__all__
        assert module.MultiplexedStreamOptions is core.MultiplexedStreamOptions
        assert zerobus.MultiplexedStreamOptions is core.MultiplexedStreamOptions

    def test_async_methods_are_coroutine_functions(self):
        for name in (
            "ingest_record",
            "ingest_records",
            "wait_for_message_id",
            "flush",
            "close",
            "get_unacked_records",
            "get_unacked_batches",
        ):
            assert inspect.iscoroutinefunction(getattr(aio.MultiplexedStream, name)), name
        assert inspect.iscoroutinefunction(aio.ZerobusSdk.create_multiplexed_stream)

    def test_stream_has_message_id_methods_only(self):
        cls = sync.MultiplexedStream
        for name in ("is_closed", "ingest_record", "ingest_records", "wait_for_message_id", "flush", "close"):
            assert hasattr(cls, name), name
        for name in ("get_unacked_records", "get_unacked_batches"):
            assert hasattr(cls, name), name
        # Message IDs are not offsets; the offset-named methods would suggest ordering.
        for name in ("ingest_record_offset", "ingest_records_offset", "wait_for_offset", "ingest_record_nowait"):
            assert not hasattr(cls, name), name
