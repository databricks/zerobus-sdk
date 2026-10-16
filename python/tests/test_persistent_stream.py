"""
Tests for the persistent (Eos) stream builder.

These tests need only the built extension (no network, no credentials). The create and
resume round trip against a server is covered by the Rust core tests.
"""

import asyncio
import json

import pytest

from zerobus import _zerobus_core as core
from zerobus.sdk import aio, sync

SDK_ARGS = ("https://localhost", "https://localhost")


class _Headers(core.HeadersProvider):
    def get_headers(self):
        return [("authorization", "Bearer token")]


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


class TestBuilderValidation:
    def test_build_requires_table(self, sdk, mode):
        builder = sdk.persistent_stream_builder().oauth("id", "secret").json()
        with pytest.raises(ValueError, match="table is required"):
            _open(mode, builder.build)

    def test_build_requires_auth(self, sdk, mode):
        builder = sdk.persistent_stream_builder().table("c.s.t").json()
        with pytest.raises(ValueError, match="authentication is required"):
            _open(mode, builder.build)

    def test_build_rejects_both_auth_methods(self, sdk, mode):
        builder = (
            sdk.persistent_stream_builder().table("c.s.t").oauth("id", "secret").headers_provider(_Headers()).json()
        )
        with pytest.raises(ValueError, match="authentication is required"):
            _open(mode, builder.build)

    def test_build_requires_format(self, sdk, mode):
        builder = sdk.persistent_stream_builder().table("c.s.t").oauth("id", "secret")
        with pytest.raises(ValueError, match="record format is required"):
            _open(mode, builder.build)

    def test_resume_requires_stream_id(self, sdk, mode):
        builder = sdk.persistent_stream_builder().table("c.s.t").oauth("id", "secret").json()
        for bad in ("", None):
            with pytest.raises(ValueError, match="stream_id"):
                _open(mode, lambda bad=bad: builder.resume(bad))

    def test_resume_requires_same_settings_as_build(self, sdk, mode):
        builder = sdk.persistent_stream_builder().oauth("id", "secret").json()
        with pytest.raises(ValueError, match="table is required"):
            _open(mode, lambda: builder.resume("stream-1"))

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
        ],
    )
    def test_numeric_setters_reject_negative(self, sdk, setter):
        with pytest.raises(ValueError, match=setter):
            getattr(sdk.persistent_stream_builder(), setter)(-1)

    def test_numeric_setters_reject_values_beyond_native_range(self, sdk):
        with pytest.raises(ValueError, match="flush_timeout_ms"):
            sdk.persistent_stream_builder().flush_timeout_ms(2**70)
        with pytest.raises(ValueError, match="recovery_retries"):
            sdk.persistent_stream_builder().recovery_retries(2**32)

    def test_callback_max_wait_time_accepts_none(self, sdk):
        assert sdk.persistent_stream_builder().callback_max_wait_time_ms(None) is not None

    def test_setters_return_the_builder(self, sdk):
        builder = sdk.persistent_stream_builder()
        chained = (
            builder.table("c.s.t")
            .oauth("id", "secret")
            .json()
            .recovery(True)
            .recovery_timeout_ms(1)
            .recovery_backoff_ms(1)
            .recovery_retries(1)
            .server_lack_of_ack_timeout_ms(1)
            .flush_timeout_ms(1)
            .max_inflight_requests(1)
            .max_ingest_payload_bytes(1)
            .callback_max_wait_time_ms(1)
        )
        assert chained is builder


class _Ack(core.AckCallback):
    def on_ack(self, offset):
        pass


class TestValidConfiguration:
    """A complete configuration passes validation and reaches the network layer.

    The endpoint refuses connections, so the open fails with a Zerobus error. A
    ValueError, TypeError, or OverflowError here would mean the builder rejected or
    mis-forwarded a setting.
    """

    @staticmethod
    def _configured(builder):
        return (
            builder.table("c.s.t")
            .headers_provider(_Headers())
            .json()
            .recovery(False)
            .recovery_timeout_ms(1)
            .recovery_backoff_ms(1)
            .recovery_retries(0)
            .server_lack_of_ack_timeout_ms(1000)
            .flush_timeout_ms(1000)
            .max_inflight_requests(10)
            .max_ingest_payload_bytes(1000)
            .ack_callback(_Ack())
            .callback_max_wait_time_ms(None)
        )

    @pytest.mark.parametrize("resume_id", [None, "stream-1"])
    def test_sync_reaches_network(self, resume_id):
        builder = self._configured(
            sync.ZerobusSdk("http://127.0.0.1:1", "http://127.0.0.1:1").persistent_stream_builder()
        )
        with pytest.raises(core.ZerobusException):
            builder.build() if resume_id is None else builder.resume(resume_id)

    @pytest.mark.parametrize("resume_id", [None, "stream-1"])
    def test_async_reaches_network(self, resume_id):
        builder = self._configured(
            aio.ZerobusSdk("http://127.0.0.1:1", "http://127.0.0.1:1").persistent_stream_builder()
        )
        with pytest.raises(core.ZerobusException):
            asyncio.run(builder.build() if resume_id is None else builder.resume(resume_id))

    def test_proto_descriptor_reaches_network(self):
        from tests import row_pb2

        builder = (
            sync.ZerobusSdk("http://127.0.0.1:1", "http://127.0.0.1:1")
            .persistent_stream_builder()
            .table("c.s.t")
            .headers_provider(_Headers())
            .compiled_proto(row_pb2.DESCRIPTOR.serialized_pb)
            .recovery(False)
            .recovery_retries(0)
            .recovery_timeout_ms(1)
        )
        with pytest.raises(core.ZerobusException):
            builder.build()


class TestNativeValidation:
    """Checks the native layer enforces the same rules if called directly."""

    def test_avro_is_rejected(self):
        schema = json.dumps({"type": "record", "name": "R", "fields": [{"name": "a", "type": "string"}]})
        props = core.TableProperties("c.s.t", None, schema)
        settings = core.PersistentStreamSettings(client_id="id", client_secret="secret")
        with pytest.raises(ValueError, match="Avro"):
            core.sync.ZerobusSdk(*SDK_ARGS).open_persistent_stream(props, settings)

    def test_requires_exactly_one_auth_method(self):
        props = core.TableProperties("c.s.t")
        sdk = core.sync.ZerobusSdk(*SDK_ARGS)
        for settings in (
            core.PersistentStreamSettings(),
            core.PersistentStreamSettings(client_id="id"),
            core.PersistentStreamSettings(client_id="id", client_secret="s", headers_provider=_Headers()),
        ):
            with pytest.raises(ValueError, match="exactly one authentication method"):
                sdk.open_persistent_stream(props, settings)


class TestPublicSurface:
    def test_exports(self, mode):
        module = sync if mode == "sync" else aio
        assert module.PersistentStream is getattr(core, mode).PersistentStream
        assert module.PersistentStreamBuilder.__name__ == "PersistentStreamBuilder"

    def test_stream_exposes_resume_api(self, mode):
        cls = getattr(core, mode).PersistentStream
        for name in (
            "stream_id",
            "resume_watermark",
            "is_closed",
            "ingest_record_offset",
            "ingest_records_offset",
            "wait_for_offset",
            "flush",
            "close",
        ):
            assert hasattr(cls, name), name

    def test_stream_has_no_unsupported_methods(self, mode):
        cls = getattr(core, mode).PersistentStream
        for name in ("ingest_record", "ingest_record_nowait", "ingest_records_nowait", "get_unacked_records"):
            assert not hasattr(cls, name), name
