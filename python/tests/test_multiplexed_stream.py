"""Tests for multiplexed streams (Beta) created by ``create_multiplexed_stream``.

These tests need only the built extension (no network, no credentials). The native tests point
at a refused local port: reaching a Zerobus error there proves the arguments crossed into the
Rust core. Ingestion, acknowledgments, and recovery against a server are covered by the Rust
core tests and by ``test_multiplexed_integration.py``.
"""

import asyncio
import gc
import inspect
import time
import weakref

import pytest

from zerobus import _zerobus_core as core

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
