"""
Tests for ``ZerobusSdk.stream_builder()``: ordinary streams and, after ``multiplexed(n)``,
multiplexed streams (Beta).

These tests need only the built extension (no network, no credentials). Ingestion,
acknowledgments, and recovery against a server are covered by the Rust core tests and by
``test_multiplexed_integration.py``.
"""

import asyncio
import inspect
import json
import re

import pytest

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


def _terminal(builder, kind):
    """The builder whose build() opens the stream: the stream builder, or its multiplexed step."""
    return builder if kind == "ordinary" else builder.multiplexed(2)


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
    """Stands in for the native SDK and records the validate_stream calls it receives."""

    def __init__(self):
        self.validate_calls = []

    def validate_stream(self, *args):
        self.validate_calls.append(args)


def _sdk_with_recording_native(mode):
    sdk = (sync if mode == "sync" else aio).ZerobusSdk(*SDK_ARGS)
    native = _RecordingNativeSdk()
    sdk._inner = native
    return sdk, native


class TestValidate:
    """``validate()`` runs the checks of ``build()`` without opening anything, as in Rust."""

    @pytest.mark.parametrize("auth", ["oauth", "headers_provider"])
    def test_complete_configuration_validates_without_opening(self, mode, kind, auth):
        headers = _CountingHeaders()
        builder = _unreachable_builder(mode).table("c.s.t").json()
        builder = builder.oauth("id", "secret") if auth == "oauth" else builder.headers_provider(headers)
        target = _terminal(builder, kind)
        # A plain method in the async SDK too: the call returns None, not an awaitable.
        assert not inspect.iscoroutinefunction(target.validate)
        assert target.validate() is None
        # Authentication is not resolved.
        assert headers.calls == 0

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

    def test_builders_expose_validate(self, mode):
        module = sync if mode == "sync" else aio
        for builder in (module.StreamBuilder, module.MultiplexedStreamBuilder):
            assert callable(getattr(builder, "validate", None)), builder
            # A plain method in the async SDK too, as in Rust.
            assert not inspect.iscoroutinefunction(builder.validate), builder

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
