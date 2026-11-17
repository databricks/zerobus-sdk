"""
Tests for ``ZerobusSdk.stream_builder()``: ordinary streams and, after ``multiplexed(n)``,
multiplexed streams (Beta).

These tests need only the built extension (no network, no credentials). Ingestion,
acknowledgments, and recovery against a server are covered by the Rust core tests and by
``test_multiplexed_integration.py``.
"""

import asyncio
import gc
import inspect
import json
import re
import weakref

import pytest

from zerobus import FederatedToken
from zerobus import _zerobus_core as core
from zerobus.sdk import aio, sync

SDK_ARGS = ("https://localhost", "https://localhost")
# Nothing listens here, so an open fails fast with a Zerobus error.
UNREACHABLE = ("http://127.0.0.1:1", "http://127.0.0.1:1")
AVRO_SCHEMA = json.dumps({"type": "record", "name": "R", "fields": [{"name": "a", "type": "string"}]})


class _Headers(core.HeadersProvider):
    def get_headers(self):
        return [("authorization", "Bearer token")]


class _Ack(core.AckCallback):
    def on_ack(self, offset):
        pass


@pytest.fixture(params=["sync", "aio"])
def mode(request):
    return request.param


@pytest.fixture
def sdk(mode):
    return (sync if mode == "sync" else aio).ZerobusSdk(*SDK_ARGS)


def _open(mode, op):
    """Run `op`, awaiting it first in async mode."""
    if mode == "sync":
        return op()
    return asyncio.run(op())


def _unreachable_builder(mode):
    return (sync if mode == "sync" else aio).ZerobusSdk(*UNREACHABLE).stream_builder()


def _select_format(builder, record_format):
    from tests import row_pb2

    if record_format == "json":
        return builder.json()
    if record_format == "proto":
        return builder.compiled_proto(row_pb2.DESCRIPTOR.serialized_pb)
    return builder.avro(AVRO_SCHEMA)


@pytest.fixture(params=["ordinary", "multiplexed"])
def kind(request):
    return request.param


def _terminal(builder, kind, stream_count=2):
    """The builder whose build() opens the stream: the stream builder, or its multiplexed step.

    Use one sub-stream where build() reaches the network: it opens without the jittered start
    delay.
    """
    return builder if kind == "ordinary" else builder.multiplexed(stream_count)


class TestBuilderValidation:
    def test_build_requires_table(self, sdk, mode, kind):
        builder = _terminal(sdk.stream_builder().oauth("id", "secret").json(), kind)
        with pytest.raises(ValueError, match=r"table is required: call table\(...\) before build\(\)"):
            _open(mode, builder.build)

    def test_build_requires_auth(self, sdk, mode, kind):
        builder = _terminal(sdk.stream_builder().table("c.s.t").json(), kind)
        with pytest.raises(ValueError, match="authentication is required"):
            _open(mode, builder.build)

    def test_build_requires_both_oauth_credentials(self, sdk, mode, kind):
        builder = _terminal(sdk.stream_builder().table("c.s.t").oauth("id", None).json(), kind)
        with pytest.raises(ValueError, match="oauth requires both client_id and client_secret"):
            _open(mode, builder.build)

    def test_build_requires_auth_names_every_auth_setter(self, sdk, mode, kind):
        builder = _terminal(sdk.stream_builder().table("c.s.t").json(), kind)
        message = (
            "authentication is required: call exactly one of oauth(...), headers_provider(...), or federated_auth(...)"
        )
        with pytest.raises(ValueError, match=re.escape(message) + "$"):
            _open(mode, builder.build)

    def test_build_requires_format(self, sdk, mode, kind):
        builder = _terminal(sdk.stream_builder().table("c.s.t").oauth("id", "secret"), kind)
        with pytest.raises(
            ValueError, match=re.escape("record format is required: call json(), compiled_proto(...), or avro(...)")
        ):
            _open(mode, builder.build)

    def test_last_auth_setter_wins(self, sdk):
        builder = sdk.stream_builder().table("c.s.t").json()
        builder.oauth("id", "secret").headers_provider(_Headers())
        assert builder._client_id is None and builder._client_secret is None
        assert builder._headers_provider is not None
        builder._native_args()

        builder.oauth("id", "secret")
        assert builder._headers_provider is None
        builder._native_args()

    def test_last_format_setter_wins(self, sdk):
        from tests import row_pb2

        builder = sdk.stream_builder()
        builder.avro(AVRO_SCHEMA).json()
        assert builder._avro_schema is None and builder._json

        builder.avro(AVRO_SCHEMA).compiled_proto(row_pb2.DESCRIPTOR.serialized_pb)
        assert builder._avro_schema is None and builder._descriptor is not None

        builder.avro(AVRO_SCHEMA)
        assert builder._avro_schema == AVRO_SCHEMA
        assert builder._descriptor is None and not builder._json

    @pytest.mark.parametrize("schema", ["", "   ", None, 7])
    def test_avro_rejects_empty_schema(self, sdk, schema):
        with pytest.raises(ValueError, match=re.escape("schema_json must be a non-empty string")):
            sdk.stream_builder().avro(schema)

    @pytest.mark.parametrize(
        "setter",
        [
            "recovery_timeout_ms",
            "recovery_backoff_ms",
            "recovery_retries",
            "server_lack_of_ack_timeout_ms",
            "flush_timeout_ms",
            "max_inflight_requests",
            "max_ingest_payload_bytes",
            "callback_max_wait_time_ms",
            "stream_paused_max_wait_time_ms",
        ],
    )
    def test_numeric_setters_reject_negative(self, sdk, setter):
        with pytest.raises(ValueError, match=setter):
            getattr(sdk.stream_builder(), setter)(-1)

    def test_stream_paused_max_wait_time_rejects_values_beyond_native_range(self, sdk):
        # The other numeric setters are shared with the persistent builder and tested there.
        with pytest.raises(ValueError, match="stream_paused_max_wait_time_ms"):
            sdk.stream_builder().stream_paused_max_wait_time_ms(2**70)

    @pytest.mark.parametrize("setter", ["callback_max_wait_time_ms", "stream_paused_max_wait_time_ms"])
    def test_optional_waits_accept_none(self, sdk, setter):
        builder = getattr(sdk.stream_builder(), setter)(None)
        assert builder._settings[setter] is None and builder._settings[setter + "_set"]

    def test_setters_return_the_builder(self, sdk):
        builder = sdk.stream_builder()
        chained = (
            builder.table("c.s.t")
            .oauth("id", "secret")
            .avro(AVRO_SCHEMA)
            .json()
            .recovery(True)
            .recovery_timeout_ms(1)
            .recovery_backoff_ms(1)
            .recovery_retries(1)
            .server_lack_of_ack_timeout_ms(1)
            .flush_timeout_ms(1)
            .max_inflight_requests(2)
            .max_ingest_payload_bytes(1)
            .ack_callback(_Ack())
            .callback_max_wait_time_ms(None)
            .stream_paused_max_wait_time_ms(None)
            .federated_auth(FederatedToken(lambda: "tok"))
        )
        assert chained is builder


class TestMultiplexedStep:
    @pytest.mark.parametrize("count", [0, -1, 65, True, "4", 2.0])
    def test_rejects_out_of_range_stream_count(self, sdk, count):
        with pytest.raises(ValueError, match=re.escape("stream_count must be an integer between 1 and 64")):
            sdk.stream_builder().multiplexed(count)

    @pytest.mark.parametrize("count", [1, 64])
    def test_accepts_bounds(self, sdk, count):
        assert sdk.stream_builder().multiplexed(count)._stream_count == count

    def test_returns_a_builder_with_only_ack_callback_validate_and_build(self, sdk, mode):
        mux = sdk.stream_builder().multiplexed(2)
        assert isinstance(mux, (sync if mode == "sync" else aio).MultiplexedStreamBuilder)
        public = {name for name in dir(mux) if not name.startswith("_")}
        assert public == {"ack_callback", "validate", "build"}
        assert mux.ack_callback(_Ack()) is mux

    def test_captures_the_configuration_when_called(self, sdk):
        # Like the Rust builder, which multiplexed(n) consumes: later changes to the stream
        # builder do not reach a multiplexed step that was already selected.
        builder = sdk.stream_builder().table("c.s.t").oauth("id", "secret").json().recovery_retries(1)
        mux = builder.multiplexed(2)
        builder.table("other.s.t").recovery_retries(9).headers_provider(_Headers())
        assert mux._base._table_name == "c.s.t"
        assert mux._base._settings["recovery_retries"] == 1
        assert mux._base._client_id == "id"

    def test_rejects_the_ordinary_ack_callback(self, sdk, mode):
        mux = sdk.stream_builder().table("c.s.t").oauth("id", "secret").json().ack_callback(_Ack()).multiplexed(2)
        with pytest.raises(ValueError, match=r"use multiplexed\(n\)\.ack_callback"):
            _open(mode, mux.build)

    def test_accepts_an_unset_ordinary_ack_callback(self, mode):
        # ack_callback(None) sets no callback, so it doesn't make a multiplexed stream invalid.
        builder = _unreachable_builder(mode).table("c.s.t").headers_provider(_Headers()).json().ack_callback(None)
        # Without recovery the failed connection is reported at once; one sub-stream opens
        # without the jittered start delay.
        builder.recovery(False)
        mux = builder.multiplexed(1)
        assert mux.validate() is None
        with pytest.raises(core.ZerobusException) as info:
            _open(mode, mux.build)
        # It reaches the network instead of failing as a configuration error.
        assert not isinstance(info.value, core.NonRetriableException), info.value


class TestAckCallbackType:
    """A callback that is not an ``AckCallback`` is rejected before anything is opened."""

    def test_ordinary_stream_rejects_a_plain_function(self, mode):
        builder = _unreachable_builder(mode).table("c.s.t").headers_provider(_Headers()).json()
        builder.ack_callback(lambda offset: None)
        # A TypeError, not the ZerobusException of a failed connection.
        with pytest.raises(TypeError):
            _open(mode, builder.build)

    def test_multiplexed_stream_rejects_a_plain_function(self, mode):
        builder = _unreachable_builder(mode).table("c.s.t").headers_provider(_Headers()).json()
        mux = builder.multiplexed(2).ack_callback(lambda message_id: None)
        with pytest.raises(TypeError):
            _open(mode, mux.build)


class TestValidConfiguration:
    """A complete configuration passes validation and reaches the native layer.

    A ValueError, TypeError, or OverflowError here would mean the builder rejected or
    mis-forwarded a setting.
    """

    @staticmethod
    def _configured(builder):
        return (
            builder.table("c.s.t")
            .headers_provider(_Headers())
            .recovery(False)
            .recovery_timeout_ms(1)
            .recovery_backoff_ms(1)
            .recovery_retries(0)
            .server_lack_of_ack_timeout_ms(1000)
            .flush_timeout_ms(1000)
            .max_inflight_requests(10)
            .max_ingest_payload_bytes(1000)
            .callback_max_wait_time_ms(None)
            .stream_paused_max_wait_time_ms(100)
        )

    @pytest.mark.parametrize("record_format", ["json", "proto", "avro"])
    def test_ordinary_stream_reaches_network(self, mode, record_format):
        builder = _select_format(self._configured(_unreachable_builder(mode)), record_format).ack_callback(_Ack())
        with pytest.raises(core.ZerobusException) as info:
            _open(mode, builder.build)
        # A rejected setting fails as a non-retriable configuration error instead.
        assert not isinstance(info.value, core.NonRetriableException), info.value

    @pytest.mark.parametrize("record_format", ["json", "proto", "avro"])
    def test_multiplexed_stream_reaches_network(self, mode, record_format):
        # One sub-stream opens without the jittered start delay.
        builder = _select_format(self._configured(_unreachable_builder(mode)), record_format)
        mux = builder.multiplexed(1).ack_callback(_Ack())
        with pytest.raises(core.ZerobusException) as info:
            _open(mode, mux.build)
        assert not isinstance(info.value, core.NonRetriableException), info.value

    def test_stream_count_and_inflight_budget_reach_the_core(self, mode):
        # The Rust core divides max_inflight_requests across sub-streams and rejects a budget
        # below the stream count before opening anything, so its message shows both values
        # arrived intact.
        mux = (
            _unreachable_builder(mode)
            .table("c.s.t")
            .headers_provider(_Headers())
            .json()
            .max_inflight_requests(2)
            .multiplexed(4)
        )
        with pytest.raises(core.NonRetriableException, match=r"max_inflight_requests \(2\) .* stream_count \(4\)"):
            _open(mode, mux.build)


class _CountingHeaders(core.HeadersProvider):
    def __init__(self):
        super().__init__()
        self.calls = 0

    def get_headers(self):
        self.calls += 1
        return [("authorization", "Bearer token")]


class _RecordingNativeSdk:
    """Stands in for the native SDK and records each open and validate call it receives.

    In async mode the open calls return awaitables, as the native async SDK does.
    """

    def __init__(self, asynchronous):
        self._asynchronous = asynchronous
        self.calls = []

    def _record(self, method, args, kwargs):
        self.calls.append((method, args, kwargs))
        if method == "validate_stream":
            return None
        if not self._asynchronous:
            return object()

        async def opened():
            return object()

        return opened()

    def validate_stream(self, *args, **kwargs):
        return self._record("validate_stream", args, kwargs)

    def open_stream(self, *args, **kwargs):
        return self._record("open_stream", args, kwargs)

    def open_multiplexed_stream(self, *args, **kwargs):
        return self._record("open_multiplexed_stream", args, kwargs)

    @property
    def validate_calls(self):
        return [call for call in self.calls if call[0] == "validate_stream"]

    @property
    def last_kwargs(self):
        return self.calls[-1][2]


def _sdk_with_recording_native(mode):
    sdk = (sync if mode == "sync" else aio).ZerobusSdk(*SDK_ARGS)
    native = _RecordingNativeSdk(asynchronous=mode == "aio")
    sdk._inner = native
    return sdk, native


class TestValidate:
    """``validate()`` runs the checks of ``build()`` without opening anything, as in Rust."""

    @pytest.mark.parametrize("auth", ["oauth", "headers_provider"])
    def test_complete_configuration_validates_without_opening(self, mode, kind, auth):
        headers = _CountingHeaders()
        builder = _unreachable_builder(mode).table("c.s.t").json().recovery(False)
        builder = builder.oauth("id", "secret") if auth == "oauth" else builder.headers_provider(headers)
        target = _terminal(builder, kind, stream_count=1)
        # A plain method in the async SDK too: the call returns None, not an awaitable.
        assert not inspect.iscoroutinefunction(target.validate)
        assert target.validate() is None
        if auth == "headers_provider":
            # Authentication is not resolved, while build() does ask for the headers.
            assert headers.calls == 0
            with pytest.raises(core.ZerobusException):
                _open(mode, target.build)
            assert headers.calls >= 1

    @pytest.mark.parametrize(
        "configure, message",
        [
            (lambda b: b.oauth("id", "secret").json(), r"table is required: call table\(...\) before build\(\)"),
            (lambda b: b.table("c.s.t").json(), "authentication is required"),
            (lambda b: b.table("c.s.t").oauth("id", "secret"), r"record format is required: call json\(\)"),
            (lambda b: b.table("c.s.t").oauth("id", None).json(), "oauth requires both client_id and client_secret"),
        ],
        ids=["table", "auth", "format", "half_oauth"],
    )
    def test_python_checks_run_before_the_native_check(self, mode, kind, configure, message):
        sdk, native = _sdk_with_recording_native(mode)
        target = _terminal(configure(sdk.stream_builder()), kind)
        with pytest.raises(ValueError, match=message):
            target.validate()
        assert native.validate_calls == []

    def test_rejects_the_ordinary_ack_callback_before_the_native_check(self, mode):
        sdk, native = _sdk_with_recording_native(mode)
        builder = sdk.stream_builder().table("c.s.t").oauth("id", "secret").json()
        with pytest.raises(ValueError, match=r"use multiplexed\(n\)\.ack_callback"):
            builder.ack_callback(_Ack()).multiplexed(2).validate()
        assert native.validate_calls == []

        # A complete configuration does reach the native check.
        builder.ack_callback(None).multiplexed(2).validate()
        assert len(native.validate_calls) == 1

    def test_core_checks_reach_python(self, mode):
        builder = _unreachable_builder(mode).table("c.s.t").headers_provider(_Headers()).json()
        with pytest.raises(core.NonRetriableException, match=r"max_inflight_requests \(2\) .* stream_count \(4\)"):
            builder.max_inflight_requests(2).multiplexed(4).validate()
        # The core's default budget allows the largest stream count.
        assert (
            _unreachable_builder(mode).table("c.s.t").headers_provider(_Headers()).json().multiplexed(64).validate()
            is None
        )

    def test_native_validate_runs_the_core_checks(self, mode):
        # Every invalid ordinary configuration stops in Python first, so the native call is
        # tested directly to show it runs the core's ordinary StreamBuilder::validate().
        native = (core.sync if mode == "sync" else core.aio).ZerobusSdk(*UNREACHABLE)
        with pytest.raises(core.NonRetriableException, match="table name is required"):
            native.validate_stream(core.TableProperties(""), core.StreamSettings(headers_provider=_Headers()))
        settings = core.StreamSettings(headers_provider=_Headers(), ack_callback=_Ack())
        with pytest.raises(core.NonRetriableException, match="ack_callback is only valid for ordinary streams"):
            native.validate_stream(core.TableProperties("c.s.t"), settings, 2)

    def test_rejects_an_invalid_descriptor(self, mode, kind):
        builder = _unreachable_builder(mode).table("c.s.t").headers_provider(_Headers()).compiled_proto(b"\xff")
        with pytest.raises(ValueError, match="Invalid FileDescriptorProto bytes"):
            _terminal(builder, kind).validate()

    def test_does_not_parse_the_avro_schema(self, mode, kind):
        # As in Rust, validate() leaves the schema to build().
        target = _terminal(
            _unreachable_builder(mode).table("c.s.t").headers_provider(_Headers()).avro("not-json"), kind
        )
        assert target.validate() is None
        with pytest.raises(core.NonRetriableException, match="Avro schema parse failed"):
            _open(mode, target.build)

    def test_rejects_an_ack_callback_that_is_not_an_ack_callback(self, mode, kind):
        def target(callback):
            builder = _unreachable_builder(mode).table("c.s.t").headers_provider(_Headers()).json()
            if kind == "ordinary":
                return builder.ack_callback(callback)
            return builder.multiplexed(2).ack_callback(callback)

        with pytest.raises(TypeError):
            target(lambda offset: None).validate()
        assert target(_Ack()).validate() is None

    def test_consumes_nothing(self, mode, kind):
        builder = _unreachable_builder(mode).table("c.s.t").headers_provider(_Headers()).json().recovery(False)
        # One sub-stream opens without the jittered start delay.
        target = builder if kind == "ordinary" else builder.multiplexed(1)
        assert target.validate() is None
        assert target.validate() is None
        with pytest.raises(core.ZerobusException) as info:
            _open(mode, target.build)
        assert not isinstance(info.value, core.NonRetriableException), info.value


class _SupplierSpy:
    """Records each native IdpSupplier the builder creates, and the StreamSettings arguments.

    Both forward to the real classes. StreamSettings has no getters, so its arguments are read
    from this record, never from the object.
    """

    def __init__(self, monkeypatch):
        # Saved before patching: isinstance() against the patched name would raise TypeError.
        self.real_supplier = core.IdpSupplier
        real_settings = core.StreamSettings
        self.suppliers = []
        self.settings = []

        def supplier(callback, allow_async, cache_identity):
            handle = self.real_supplier(callback, allow_async, cache_identity)
            self.suppliers.append((callback, allow_async, cache_identity, handle))
            return handle

        def settings(**kwargs):
            self.settings.append(kwargs)
            return real_settings(**kwargs)

        monkeypatch.setattr(core, "IdpSupplier", supplier)
        monkeypatch.setattr(core, "StreamSettings", settings)


@pytest.fixture
def spy(monkeypatch):
    return _SupplierSpy(monkeypatch)


def _token(databricks_client_id=None):
    return FederatedToken(lambda: "idp-token", databricks_client_id=databricks_client_id)


class TestFederatedAuth:
    """``federated_auth()`` on ``stream_builder()``, as ``create_stream(auth=...)`` takes it."""

    def test_build_passes_a_fresh_supplier_per_call(self, spy, mode, kind):
        sdk, native = _sdk_with_recording_native(mode)
        token = _token()
        target = _terminal(sdk.stream_builder().table("c.s.t").json().federated_auth(token), kind)

        _open(mode, target.build)
        _open(mode, target.build)

        assert len(spy.suppliers) == 2
        first, second = (handle for *_, handle in spy.suppliers)
        for callback, allow_async, cache_identity, handle in spy.suppliers:
            assert callback is token.idp_token_supplier
            # Only the async SDK lets the callback be async.
            assert allow_async is (mode == "aio")
            assert cache_identity == token._cache_identity
            assert isinstance(handle, spy.real_supplier)
        assert first is not second
        assert [call[2]["idp_supplier"] for call in native.calls] == [first, second]

    def test_sync_build_inside_an_event_loop_keeps_the_sync_policy(self, spy, kind):
        sdk, _ = _sdk_with_recording_native("sync")
        target = _terminal(sdk.stream_builder().table("c.s.t").json().federated_auth(_token()), kind)

        async def build_in_loop():
            target.build()

        asyncio.run(build_in_loop())
        assert [allow_async for _, allow_async, _, _ in spy.suppliers] == [False]

    def test_validate_passes_its_own_supplier(self, spy, mode, kind):
        sdk, native = _sdk_with_recording_native(mode)
        token = _token()
        target = _terminal(sdk.stream_builder().table("c.s.t").json().federated_auth(token), kind)

        assert target.validate() is None
        _open(mode, target.build)

        assert [(c, a, i) for c, a, i, _ in spy.suppliers] == [
            (token.idp_token_supplier, mode == "aio", token._cache_identity)
        ] * 2
        validated, built = (handle for *_, handle in spy.suppliers)
        assert validated is not built
        assert native.calls[0][0] == "validate_stream" and native.calls[0][2]["idp_supplier"] is validated

    def test_async_validate_outside_an_event_loop(self, spy, kind):
        # The async supplier captures the running loop only when there is one, so validate()
        # works from synchronous code, and a later build() gets its own supplier.
        target = _terminal(
            _unreachable_builder("aio").table("c.s.t").json().recovery(False).federated_auth(_token()),
            kind,
            stream_count=1,
        )
        assert target.validate() is None
        with pytest.raises(core.ZerobusException):
            asyncio.run(target.build())
        assert len(spy.suppliers) == 2
        assert spy.suppliers[0][3] is not spy.suppliers[1][3]

    @pytest.mark.parametrize("databricks_client_id", [None, "sp"], ids=["account", "workload"])
    @pytest.mark.parametrize("call", ["build", "validate"])
    def test_passes_the_databricks_client_id(self, mode, kind, databricks_client_id, call):
        # None selects account-level federation; a client ID selects workload identity federation.
        sdk, native = _sdk_with_recording_native(mode)
        target = _terminal(
            sdk.stream_builder().table("c.s.t").json().federated_auth(_token(databricks_client_id)), kind
        )
        if call == "build":
            _open(mode, target.build)
        else:
            target.validate()
        assert native.last_kwargs["databricks_client_id"] == databricks_client_id

    @pytest.mark.parametrize(
        "earlier",
        [lambda b: b.oauth("id", "secret"), lambda b: b.headers_provider(_Headers())],
        ids=["oauth", "headers"],
    )
    def test_replaces_an_earlier_auth_setter(self, spy, mode, earlier):
        sdk, native = _sdk_with_recording_native(mode)
        builder = earlier(sdk.stream_builder().table("c.s.t").json()).federated_auth(_token())
        _open(mode, builder.build)

        assert native.last_kwargs["idp_supplier"] is spy.suppliers[-1][3]
        settings = spy.settings[-1]
        assert settings["client_id"] is None and settings["client_secret"] is None
        assert settings["headers_provider"] is None

    @pytest.mark.parametrize(
        "later", [lambda b: b.oauth("id", "secret"), lambda b: b.headers_provider(_Headers())], ids=["oauth", "headers"]
    )
    def test_is_replaced_by_a_later_auth_setter(self, spy, mode, later):
        sdk, native = _sdk_with_recording_native(mode)
        builder = later(sdk.stream_builder().table("c.s.t").json().federated_auth(_token("sp")))
        _open(mode, builder.build)

        assert native.last_kwargs["idp_supplier"] is None
        assert native.last_kwargs["databricks_client_id"] is None
        assert spy.suppliers == []

    @pytest.mark.parametrize(
        "change", [lambda b: b.federated_auth(_token("other")), lambda b: b.oauth("id", "secret")], ids=["fed", "oauth"]
    )
    def test_multiplexed_keeps_the_auth_set_when_it_was_called(self, spy, change):
        sdk, native = _sdk_with_recording_native("sync")
        token = _token("sp")
        builder = sdk.stream_builder().table("c.s.t").json().federated_auth(token)
        mux = builder.multiplexed(2)
        change(builder)
        mux.build()

        callback, _, cache_identity, handle = spy.suppliers[-1]
        assert callback is token.idp_token_supplier and cache_identity == token._cache_identity
        assert native.last_kwargs["idp_supplier"] is handle
        assert native.last_kwargs["databricks_client_id"] == "sp"

    def test_multiplexed_keeps_non_federated_auth(self, spy):
        sdk, native = _sdk_with_recording_native("sync")
        builder = sdk.stream_builder().table("c.s.t").json().oauth("id", "secret")
        mux = builder.multiplexed(2)
        builder.federated_auth(_token())
        mux.build()
        assert native.last_kwargs["idp_supplier"] is None
        assert spy.suppliers == []

    @pytest.mark.parametrize("token", ["tok", None, lambda: "tok"])
    def test_rejects_anything_but_a_federated_token(self, sdk, token):
        with pytest.raises(ValueError, match="token must be a FederatedToken instance"):
            sdk.stream_builder().federated_auth(token)

    def test_native_settings_allow_only_one_auth_method(self, mode):
        # One entry point per mode: open_stream, open_multiplexed_stream, and validate_stream
        # share this check.
        native = (core.sync if mode == "sync" else core.aio).ZerobusSdk(*SDK_ARGS)
        props = core.TableProperties("c.s.t")

        async def run():
            supplier = core.IdpSupplier(lambda: "tok", mode == "aio", "cache-id")
            for settings in (
                core.StreamSettings(headers_provider=_Headers()),
                core.StreamSettings(client_id="id", client_secret="s"),
                core.StreamSettings(client_id="id"),
                core.StreamSettings(client_secret="s"),
            ):
                # The async call raises before it returns an awaitable.
                with pytest.raises(ValueError, match="exactly one authentication method"):
                    native.open_stream(props, settings, idp_supplier=supplier)

        asyncio.run(run())

    def test_federated_configuration_reaches_the_network(self, mode, kind):
        calls = []

        def idp_token():
            calls.append(1)
            return "idp-token"

        builder = _unreachable_builder(mode).table("c.s.t").json().recovery(False)
        target = builder.federated_auth(FederatedToken(idp_token))
        target = target if kind == "ordinary" else target.multiplexed(1)

        assert target.validate() is None
        assert calls == []
        # A ValueError, TypeError, or RuntimeError would escape this: the configuration crossed
        # the bridge, and the token exchange failed to connect.
        with pytest.raises(core.ZerobusException) as info:
            _open(mode, target.build)
        assert not isinstance(info.value, core.NonRetriableException), info.value
        # build() reaches the Python callback once, for the token exchange.
        assert len(calls) == 1

    def test_failed_build_does_not_keep_the_callback_owner_alive(self, mode, kind):
        class Owner:
            def get_token(self):
                return "idp-token"

        def fail(owner):
            builder = _unreachable_builder(mode).table("c.s.t").json().recovery(False)
            target = _terminal(builder.federated_auth(FederatedToken(owner.get_token)), kind, stream_count=1)
            try:
                _open(mode, target.build)
            except core.ZerobusException as exc:
                # The traceback keeps the frame that built the native supplier.
                owner.err = exc

        owner = Owner()
        fail(owner)
        assert isinstance(owner.err, core.ZerobusException)
        ref = weakref.ref(owner)
        del owner
        gc.collect()
        assert ref() is None

    def test_failed_validate_does_not_keep_the_callback_owner_alive(self, mode):
        class Owner:
            def get_token(self):
                return "idp-token"

        def fail(owner):
            builder = _unreachable_builder(mode).table("c.s.t").json().max_inflight_requests(2)
            mux = builder.federated_auth(FederatedToken(owner.get_token)).multiplexed(4)
            try:
                mux.validate()
            except core.NonRetriableException as exc:
                owner.err = exc

        owner = Owner()
        fail(owner)
        assert "max_inflight_requests (2)" in str(owner.err)
        ref = weakref.ref(owner)
        del owner
        gc.collect()
        assert ref() is None

    def test_token_owner_cycle_through_the_builder_is_collectable(self, mode, kind):
        class Owner:
            def get_token(self):
                return "idp-token"

        owner = Owner()
        owner.auth = FederatedToken(owner.get_token)
        owner.builder = _terminal(_unreachable_builder(mode).table("c.s.t").json().federated_auth(owner.auth), kind)
        owner.builder.validate()
        ref = weakref.ref(owner)
        del owner
        gc.collect()
        assert ref() is None


class TestNativeValidation:
    """Checks the native layer enforces the same rules if called directly."""

    def test_requires_exactly_one_auth_method(self, mode):
        # The async open calls check the settings before they return an awaitable.
        props = core.TableProperties("c.s.t")

        async def run():
            native = (core.sync if mode == "sync" else core.aio).ZerobusSdk(*SDK_ARGS)
            for settings in (
                core.StreamSettings(),
                core.StreamSettings(client_id="id"),
                core.StreamSettings(client_id="id", client_secret="s", headers_provider=_Headers()),
            ):
                with pytest.raises(ValueError, match="exactly one authentication method"):
                    native.open_stream(props, settings)
                with pytest.raises(ValueError, match="exactly one authentication method"):
                    native.open_multiplexed_stream(props, settings, 2)

        asyncio.run(run())

    def test_rejects_the_ordinary_ack_callback_on_a_multiplexed_stream(self):
        # The Python builder catches this first; the core rejects it too.
        props = core.TableProperties("c.s.t")
        native = core.sync.ZerobusSdk(*UNREACHABLE)
        settings = core.StreamSettings(headers_provider=_Headers(), ack_callback=_Ack())
        with pytest.raises(core.NonRetriableException, match="ack_callback is only valid for ordinary streams"):
            native.open_multiplexed_stream(props, settings, 2)


class TestPublicSurface:
    def test_exports(self, mode):
        module = sync if mode == "sync" else aio
        for name in ("StreamBuilder", "MultiplexedStreamBuilder", "MultiplexedStream"):
            assert getattr(module, name).__name__ == name
            assert name in module.__all__
        # Both modes wrap the native class, which handles Avro encoding in Python.
        assert module.MultiplexedStream is not (core.sync if mode == "sync" else core.aio).MultiplexedStream

    def test_federated_auth_only_on_the_stream_builder(self, sdk):
        assert callable(sdk.stream_builder().federated_auth)
        # As in Rust, the persistent builder has no federated authentication.
        assert not hasattr(sdk.persistent_stream_builder(), "federated_auth")

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
        assert inspect.iscoroutinefunction(aio.StreamBuilder.build)
        assert inspect.iscoroutinefunction(aio.MultiplexedStreamBuilder.build)

    def test_multiplexed_stream_exposes_its_api(self):
        # The async class is checked by test_async_methods_are_coroutine_functions.
        cls = sync.MultiplexedStream
        for name in (
            "is_closed",
            "ingest_record",
            "ingest_records",
            "wait_for_message_id",
            "flush",
            "close",
            "get_unacked_records",
            "get_unacked_batches",
        ):
            assert hasattr(cls, name), name

    def test_multiplexed_stream_has_no_offset_methods(self, mode):
        # Message IDs are not offsets; the offset-named methods would suggest ordering.
        cls = (sync if mode == "sync" else aio).MultiplexedStream
        for name in ("ingest_record_offset", "ingest_records_offset", "wait_for_offset", "ingest_record_nowait"):
            assert not hasattr(cls, name), name
