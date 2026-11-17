"""
Tests for ``ZerobusSdk.stream_builder()``.

These tests need only the built extension (no network, no credentials). Ingestion against a
server is covered by the Rust core tests.
"""

import asyncio
import inspect
import json

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


class TestBuilderValidation:
    def test_build_requires_table(self, sdk, mode):
        builder = sdk.stream_builder().oauth("id", "secret").json()
        with pytest.raises(ValueError, match=r"table is required: call table\(...\) before build\(\)"):
            _open(mode, builder.build)

    def test_build_requires_auth(self, sdk, mode):
        builder = sdk.stream_builder().table("c.s.t").json()
        with pytest.raises(ValueError, match="authentication is required"):
            _open(mode, builder.build)

    def test_build_requires_format(self, sdk, mode):
        builder = sdk.stream_builder().table("c.s.t").oauth("id", "secret")
        with pytest.raises(ValueError, match=r"record format is required: call json\(\), compiled_proto"):
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
        with pytest.raises(ValueError, match="schema_json"):
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


class TestNativeValidation:
    """Checks the native layer enforces the same rules if called directly."""

    def test_requires_exactly_one_auth_method(self):
        props = core.TableProperties("c.s.t")
        native = core.sync.ZerobusSdk(*SDK_ARGS)
        for settings in (
            core.StreamSettings(),
            core.StreamSettings(client_id="id"),
            core.StreamSettings(client_id="id", client_secret="s", headers_provider=_Headers()),
        ):
            with pytest.raises(ValueError, match="exactly one authentication method"):
                native.open_stream(props, settings)


class TestPublicSurface:
    def test_exports(self, mode):
        module = sync if mode == "sync" else aio
        assert module.StreamBuilder.__name__ == "StreamBuilder"
        assert "StreamBuilder" in module.__all__

    def test_async_build_is_a_coroutine_function(self):
        assert inspect.iscoroutinefunction(aio.StreamBuilder.build)
