//! Multiplexed stream FFI surface (Beta).
//!
//! A multiplexed stream distributes ingestion round-robin across
//! `stream_count` gRPC sub-streams to raise aggregate throughput.
//!
//! Thread-safety and callback rules are on `CZerobusMultiplexedStream`.

use crate::common::*;
use crate::stream::{configure_stream_builder, StreamCreateAuth};
use async_trait::async_trait;
use databricks_zerobus_ingest_sdk::{
    AckCallback, HeadersProvider, MessageId, MultiplexedStream, ZerobusError, ZerobusResult,
    ZerobusSdk,
};
use prost::Message;
use std::collections::HashMap;
use std::os::raw::c_char;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

/// Opaque handle for a multiplexed stream (Beta). Created by
/// `zerobus_sdk_create_multiplexed_stream` or
/// `zerobus_sdk_create_multiplexed_stream_with_headers_provider` and released
/// by `zerobus_multiplexed_stream_free`.
///
/// Thread safety: every `zerobus_multiplexed_stream_*` call except `_free` may
/// run concurrently on the same handle from any thread. `_free` must not
/// overlap any other call on that handle.
///
/// Ack and headers callbacks run on the FFI's runtime threads. They must not
/// call `zerobus_multiplexed_stream_*` functions, and must not block on a
/// thread that is inside one: the call would fail, or deadlock while close
/// waits for callbacks to drain. Ack callbacks fire in order within each
/// sub-stream.
#[repr(C)]
pub struct CZerobusMultiplexedStream {
    _private: [u8; 0],
}

/// Concrete type behind `*mut CZerobusMultiplexedStream`, owned by a `Box`.
///
struct MuxHandle {
    /// `close` needs `&mut MultiplexedStream`, while the other calls need
    /// only `&`. The lock lets those share the mux and makes a close wait for
    /// them instead of aliasing a `&mut` with live `&` borrows.
    stream: Arc<RwLock<MultiplexedStream>>,
}

fn validate_mux_ptr<'a>(
    mux: *mut CZerobusMultiplexedStream,
) -> Result<&'a MuxHandle, &'static str> {
    if mux.is_null() {
        return Err("Multiplexed stream pointer is null");
    }
    Ok(unsafe { &*(mux as *const MuxHandle) })
}

/// Writes the outcome into `result` and returns the value, or `failure` on error.
fn complete<T>(result: *mut CResult, res: ZerobusResult<T>, failure: T) -> T {
    match res {
        Ok(value) => {
            write_success_result(result);
            value
        }
        Err(err) => {
            if !result.is_null() {
                unsafe {
                    *result = CResult::error(err);
                }
            }
            failure
        }
    }
}

unsafe fn c_str_arg(value: *const c_char) -> ZerobusResult<String> {
    c_str_to_string(value).map_err(|e| ZerobusError::InvalidArgument(e.to_string()))
}

unsafe fn decode_descriptor(
    bytes: *const u8,
    len: usize,
) -> ZerobusResult<Option<prost_types::DescriptorProto>> {
    if bytes.is_null() || len == 0 {
        return Ok(None);
    }
    prost_types::DescriptorProto::decode(std::slice::from_raw_parts(bytes, len))
        .map(Some)
        .map_err(|e| ZerobusError::InvalidArgument(e.to_string()))
}

/// C callback receiving an acknowledged message ID.
type MuxAckOnAck = extern "C" fn(message_id: i64, user_data: *mut std::ffi::c_void);
/// C callback receiving a failed message ID and the error text.
type MuxAckOnError =
    extern "C" fn(message_id: i64, error_message: *const c_char, user_data: *mut std::ffi::c_void);
/// C callback releasing the ack callbacks' `user_data`.
type MuxAckFree = extern "C" fn(user_data: *mut std::ffi::c_void);

/// Bridges the C ack callbacks to the mux's `AckCallback<MessageId>`; the C
/// callback's `i64` argument receives `MessageId::raw()`.
///
/// Owns `user_data` when `free_user_data` is set: `Drop` releases it once,
/// after the last sub-stream and background task holding the callback is
/// gone. The C caller cannot observe that moment otherwise, since sub-streams
/// are torn down in the background and can outlive
/// `zerobus_multiplexed_stream_free`.
struct MultiplexedCallbackAckCallback {
    inner: CallbackAckCallback,
    has_callback: bool,
    user_data: *mut std::ffi::c_void,
    free_user_data: Option<MuxAckFree>,
}

impl MultiplexedCallbackAckCallback {
    fn new(
        on_ack: Option<MuxAckOnAck>,
        on_error: Option<MuxAckOnError>,
        user_data: *mut std::ffi::c_void,
        free_user_data: Option<MuxAckFree>,
    ) -> Self {
        Self {
            inner: CallbackAckCallback::new(on_ack, on_error, user_data),
            has_callback: on_ack.is_some() || on_error.is_some(),
            user_data,
            free_user_data,
        }
    }
}

impl Drop for MultiplexedCallbackAckCallback {
    fn drop(&mut self) {
        if let Some(free) = self.free_user_data {
            let user_data = self.user_data;
            // Contain panics: unwinding across the FFI boundary is UB.
            if catch_unwind(AssertUnwindSafe(|| free(user_data))).is_err() {
                tracing::error!("mux ack free callback panicked; contained at FFI boundary");
            }
        }
    }
}

// Safety: `user_data` is only handed back to the caller's callbacks, whose
// thread safety is the caller's contract (documented on the create functions).
unsafe impl Send for MultiplexedCallbackAckCallback {}
unsafe impl Sync for MultiplexedCallbackAckCallback {}

impl AckCallback<MessageId> for MultiplexedCallbackAckCallback {
    fn on_ack(&self, message_id: MessageId) {
        self.inner.on_ack(message_id.raw());
    }

    fn on_error(&self, message_id: MessageId, error_message: &str) {
        self.inner.on_error(message_id.raw(), error_message);
    }
}

/// Queues calls into a C headers callback shared by every sub-stream.
///
/// A mux hands one custom provider to all of its sub-streams, which open and
/// recover independently. `CallbackHeadersProvider` rejects overlapping calls,
/// so this wrapper serializes them, keeping the contract that the C callback
/// need not be reentrant.
struct SerializedHeadersProvider {
    inner: CallbackHeadersProvider,
    lock: Mutex<()>,
}

impl SerializedHeadersProvider {
    fn new(inner: CallbackHeadersProvider) -> Self {
        Self {
            inner,
            lock: Mutex::new(()),
        }
    }
}

#[async_trait]
impl HeadersProvider for SerializedHeadersProvider {
    async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
        let _guard = self.lock.lock().await;
        self.inner.get_headers().await
    }
}

/// Rejects the ordinary-stream ack fields, which carry ordered offsets.
fn reject_options_ack_value(options: Option<&CStreamConfigurationOptions>) -> ZerobusResult<()> {
    if options.is_some_and(|c| c.ack_on_ack.is_some() || c.ack_on_error.is_some()) {
        return Err(ZerobusError::InvalidArgument(
            "options->ack_on_ack and options->ack_on_error are not supported for multiplexed \
             streams; pass the create function's ack_on_ack and ack_on_error instead"
                .to_string(),
        ));
    }
    Ok(())
}

async fn build_mux_from_parts(
    sdk_ref: &ZerobusSdk,
    table_name: String,
    descriptor_proto: Option<prost_types::DescriptorProto>,
    auth: StreamCreateAuth,
    stream_count: usize,
    options: Option<CStreamConfigurationOptions>,
    ack_callback: MultiplexedCallbackAckCallback,
) -> ZerobusResult<*mut CZerobusMultiplexedStream> {
    reject_options_ack_value(options.as_ref())?;
    let mut builder = configure_stream_builder(
        sdk_ref,
        table_name,
        descriptor_proto,
        auth,
        options.as_ref(),
    )?;
    if ack_callback.has_callback {
        builder = builder.multiplexed_ack_callback(Arc::new(ack_callback));
    } else {
        // Nothing will call it: free its `user_data` now.
        drop(ack_callback);
    }
    let mux = builder.multiplexed(stream_count).build().await?;
    let handle = MuxHandle {
        stream: Arc::new(RwLock::new(mux)),
    };
    Ok(Box::into_raw(Box::new(handle)) as *mut CZerobusMultiplexedStream)
}

/// Create a multiplexed stream with OAuth authentication (Beta).
///
/// Opens `stream_count` sub-streams (1 to 64) to the same table and routes
/// records round-robin across them. Use it when one stream is the throughput
/// bottleneck and global ordering is not required: order is preserved only
/// within each sub-stream. Construction is all-or-nothing; if any sub-stream
/// fails to open, the opened ones are closed and NULL is returned. Sub-stream
/// opens are staggered by a random delay of up to 1 second each, so creation
/// takes up to that much longer than a single stream.
///
/// `descriptor_proto_bytes` is a protobuf-encoded DescriptorProto, required
/// for protobuf streams. `options` may be NULL for defaults, which select
/// protobuf records; for JSON, pass options with `record_type = 2` and a NULL
/// descriptor. `max_inflight_requests` in `options` is a budget for the whole
/// mux: each sub-stream gets `max_inflight_requests / stream_count` (any
/// remainder is unused), so it must be at least `stream_count`.
///
/// `client_id` and `client_secret` are the OAuth client credentials and must
/// not be NULL. The SDK needs a Unity Catalog URL to mint the token, and the
/// sub-streams share it.
///
/// `ack_on_ack` and `ack_on_error` (either may be NULL) receive message IDs,
/// which are opaque: they can be negative and are not ordered, so do not
/// compare them. Each sub-stream delivers its callbacks on its own background
/// task, so callbacks from different sub-streams may run concurrently and in no
/// global order; `ack_user_data` must tolerate concurrent access.
/// `error_message` is valid only during the call. `options->ack_on_ack` and
/// `options->ack_on_error` must be NULL: they carry ordered offsets for
/// ordinary streams and are rejected here. `options->ack_user_data` is ignored;
/// pass the `ack_user_data` parameter instead. When close returns, all ack
/// callbacks have run, unless close waited longer than
/// `callback_max_wait_time_ms` (default 5 s; see
/// `zerobus_multiplexed_stream_close`).
///
/// Once this function is called, the FFI owns `ack_user_data` and invokes
/// `free_ack_user_data` (when set) exactly once, from any thread, after the
/// last sub-stream and background task releases the callbacks. That can be
/// after this function returns, even when it fails (a sub-stream that
/// started to open is torn down in the background), and after
/// `zerobus_multiplexed_stream_free` returns. Never free `ack_user_data`
/// yourself. With a NULL `free_ack_user_data` the caller keeps ownership, and
/// `ack_user_data` must stay valid for the rest of the process.
///
/// Returns NULL on error; see `result` for details. Free the handle with
/// `zerobus_multiplexed_stream_free`.
#[no_mangle]
pub extern "C" fn zerobus_sdk_create_multiplexed_stream(
    sdk: *mut CZerobusSdk,
    table_name: *const c_char,
    descriptor_proto_bytes: *const u8,
    descriptor_proto_len: usize,
    client_id: *const c_char,
    client_secret: *const c_char,
    stream_count: usize,
    options: *const CStreamConfigurationOptions,
    // Written inline (not via type aliases) so cbindgen emits nullable C
    // function pointers instead of opaque structs.
    ack_on_ack: Option<extern "C" fn(message_id: i64, user_data: *mut std::ffi::c_void)>,
    ack_on_error: Option<
        extern "C" fn(
            message_id: i64,
            error_message: *const c_char,
            user_data: *mut std::ffi::c_void,
        ),
    >,
    ack_user_data: *mut std::ffi::c_void,
    free_ack_user_data: Option<extern "C" fn(user_data: *mut std::ffi::c_void)>,
    result: *mut CResult,
) -> *mut CZerobusMultiplexedStream {
    ffi_guard(result, ptr::null_mut(), move || {
        // Take ownership of `ack_user_data` before any fallible work, so an
        // early return frees it exactly once.
        let ack_callback = MultiplexedCallbackAckCallback::new(
            ack_on_ack,
            ack_on_error,
            ack_user_data,
            free_ack_user_data,
        );
        let sdk_ref = match validate_sdk_ptr(sdk) {
            Ok(s) => s,
            Err(msg) => {
                write_error_result(result, msg, false);
                return ptr::null_mut();
            }
        };

        let res = RUNTIME.block_on(async {
            let auth = StreamCreateAuth::OAuth {
                client_id: unsafe { c_str_arg(client_id)? },
                client_secret: unsafe { c_str_arg(client_secret)? },
            };
            build_mux_from_parts(
                sdk_ref,
                unsafe { c_str_arg(table_name)? },
                unsafe { decode_descriptor(descriptor_proto_bytes, descriptor_proto_len)? },
                auth,
                stream_count,
                (!options.is_null()).then(|| unsafe { *options }),
                ack_callback,
            )
            .await
        });
        complete(result, res, ptr::null_mut())
    })
}

/// Create a multiplexed stream with a custom headers provider callback (Beta).
///
/// Behaves like `zerobus_sdk_create_multiplexed_stream`, but authenticates
/// with `headers_callback`. One provider is shared by every sub-stream; calls
/// into `headers_callback` are serialized, so it need not be reentrant.
/// Because sub-streams wait for each other's calls, time spent in the callback
/// counts against every sub-stream's `recovery_timeout_ms` open deadline. With
/// many sub-streams, a slow callback can make late ones time out, failing the
/// whole create, so keep it fast (for example, return a cached token).
///
/// Once this function is called, the FFI owns `user_data` and invokes
/// `free_user_data` (when set) exactly once, from any thread, after the last
/// sub-stream releases the provider. Unlike
/// `zerobus_sdk_create_stream_with_headers_provider`, that can be after this
/// function returns even when it fails, since sub-streams that started to
/// open are torn down in the background. Never free `user_data` yourself.
/// With a NULL `free_user_data` the caller keeps ownership, and `user_data`
/// must stay valid for the rest of the process.
/// The ack parameters, and ownership of `ack_user_data`, follow
/// `zerobus_sdk_create_multiplexed_stream`.
#[no_mangle]
pub extern "C" fn zerobus_sdk_create_multiplexed_stream_with_headers_provider(
    sdk: *mut CZerobusSdk,
    table_name: *const c_char,
    descriptor_proto_bytes: *const u8,
    descriptor_proto_len: usize,
    headers_callback: HeadersProviderCallback,
    user_data: *mut std::ffi::c_void,
    // Written inline (not via HeadersProviderFreeCallback) so cbindgen emits a
    // nullable C function pointer instead of an opaque struct.
    free_user_data: Option<extern "C" fn(user_data: *mut std::ffi::c_void)>,
    stream_count: usize,
    options: *const CStreamConfigurationOptions,
    ack_on_ack: Option<extern "C" fn(message_id: i64, user_data: *mut std::ffi::c_void)>,
    ack_on_error: Option<
        extern "C" fn(
            message_id: i64,
            error_message: *const c_char,
            user_data: *mut std::ffi::c_void,
        ),
    >,
    ack_user_data: *mut std::ffi::c_void,
    free_ack_user_data: Option<extern "C" fn(user_data: *mut std::ffi::c_void)>,
    result: *mut CResult,
) -> *mut CZerobusMultiplexedStream {
    ffi_guard(result, ptr::null_mut(), move || {
        // Construct the provider and the ack callback before any fallible
        // work: they own `user_data` and `ack_user_data`, so dropping them on
        // an early return frees each exactly once.
        let headers_provider: Arc<dyn HeadersProvider> = Arc::new(SerializedHeadersProvider::new(
            CallbackHeadersProvider::new(headers_callback, user_data, free_user_data),
        ));
        let ack_callback = MultiplexedCallbackAckCallback::new(
            ack_on_ack,
            ack_on_error,
            ack_user_data,
            free_ack_user_data,
        );

        let sdk_ref = match validate_sdk_ptr(sdk) {
            Ok(s) => s,
            Err(msg) => {
                write_error_result(result, msg, false);
                return ptr::null_mut();
            }
        };

        let res = RUNTIME.block_on(async {
            build_mux_from_parts(
                sdk_ref,
                unsafe { c_str_arg(table_name)? },
                unsafe { decode_descriptor(descriptor_proto_bytes, descriptor_proto_len)? },
                StreamCreateAuth::HeadersProvider { headers_provider },
                stream_count,
                (!options.is_null()).then(|| unsafe { *options }),
                ack_callback,
            )
            .await
        });
        complete(result, res, ptr::null_mut())
    })
}

/// Free a multiplexed stream handle (Beta).
///
/// Call `zerobus_multiplexed_stream_close` first to flush pending records;
/// freeing an open stream abandons them. Must not run concurrently with any
/// other call on `mux`, and `mux` must not be used afterwards.
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_free(mux: *mut CZerobusMultiplexedStream) {
    ffi_guard(ptr::null_mut(), (), move || {
        if !mux.is_null() {
            unsafe {
                drop(Box::from_raw(mux as *mut MuxHandle));
            }
        }
    })
}

/// Flush and close every sub-stream (Beta).
///
/// Waits for in-progress calls on this mux to return, then flushes and closes.
/// The handle must still be released with `zerobus_multiplexed_stream_free`.
///
/// Returns false with `result` set on failure; the mux is closed either way.
/// When close returns, all ack callbacks have run, unless close waited longer
/// than `callback_max_wait_time_ms` (default 5 s). With
/// `has_callback_max_wait_time_ms` false there is no limit. After it, queued
/// callbacks are dropped and a running one can outlive close.
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_close(
    mux: *mut CZerobusMultiplexedStream,
    result: *mut CResult,
) -> bool {
    ffi_guard(result, false, move || {
        let handle = match validate_mux_ptr(mux) {
            Ok(h) => h,
            Err(msg) => {
                write_error_result(result, msg, false);
                return false;
            }
        };
        let res = RUNTIME.block_on(async { handle.stream.write().await.close().await });
        complete(result, res.map(|()| true), false)
    })
}

/// Returns whether the mux is closed (Beta): either explicitly or because a
/// call on it observed a sub-stream failure. A failure that no call has
/// observed yet does not set it. Also returns true while a close is in
/// progress and for a null handle.
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_is_closed(
    mux: *mut CZerobusMultiplexedStream,
) -> bool {
    ffi_guard(ptr::null_mut(), true, move || match validate_mux_ptr(mux) {
        Ok(handle) => handle.stream.try_read().map_or(true, |mux| mux.is_closed()),
        Err(_) => true,
    })
}

#[cfg(test)]
mod multiplexed_tests {
    //! Pure FFI-layer tests: C argument checks, the ack bridge, and the
    //! serialized headers provider. Create tests run against
    //! the mock server in `rust/tests/src/multiplexed_ffi_tests.rs`.
    use super::*;
    use crate::zerobus_free_error_message;
    use std::ffi::{c_void, CStr};
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
    use std::time::Duration;

    // Short names for the exports, so each table row fits on one line.
    use super::zerobus_multiplexed_stream_close as close;

    const NULL_HANDLE: &str = "Multiplexed stream pointer is null";

    fn unwritten_result() -> CResult {
        CResult {
            success: true,
            error_message: ptr::null_mut(),
            is_retryable: false,
        }
    }

    /// `(success, retryable, message)`, freeing the message.
    fn take_full(result: &mut CResult) -> (bool, bool, String) {
        let mut message = String::new();
        if !result.error_message.is_null() {
            message = unsafe { CStr::from_ptr(result.error_message) }
                .to_string_lossy()
                .into_owned();
            zerobus_free_error_message(result.error_message);
            result.error_message = ptr::null_mut();
        }
        (result.success, result.is_retryable, message)
    }

    extern "C" fn empty_headers(_user_data: *mut c_void) -> CHeaders {
        CHeaders {
            headers: ptr::null_mut(),
            count: 0,
            error_message: ptr::null_mut(),
        }
    }

    /// Close checks its handle first, with a pinned, non-retryable error, and
    /// the return value reports the failure too; `is_closed` and `free`
    /// accept NULL.
    #[test]
    fn c_arguments_are_checked_before_the_handle() {
        let null = ptr::null_mut();
        let mut result = unwritten_result();
        assert!(!close(null, &mut result), "the return value must report it");
        let (success, retryable, message) = take_full(&mut result);
        assert!(!success && !retryable, "close: {message}");
        assert!(message.contains(NULL_HANDLE), "close: {message}");
        assert!(zerobus_multiplexed_stream_is_closed(null));
        zerobus_multiplexed_stream_free(null);
    }

    /// The ack bridge passes raw message IDs (incl. negative) unchanged,
    /// and a NULL side is a no-op.
    #[test]
    fn ack_bridge_passes_raw_ids_one_sided() {
        static ACKED: AtomicI64 = AtomicI64::new(0);
        static FAILED: AtomicI64 = AtomicI64::new(0);
        extern "C" fn on_ack(id: i64, _user_data: *mut c_void) {
            ACKED.store(id, Ordering::SeqCst);
        }
        extern "C" fn on_error(id: i64, _msg: *const c_char, _user_data: *mut c_void) {
            FAILED.store(id, Ordering::SeqCst);
        }
        let bridge =
            |ack, error| MultiplexedCallbackAckCallback::new(ack, error, ptr::null_mut(), None);
        let (acked, failed) = (
            || ACKED.load(Ordering::SeqCst),
            || FAILED.load(Ordering::SeqCst),
        );
        // Lane 63 sets the sign bit, so this raw ID is negative.
        let negative = MessageId::from_raw(((63u64 << 58) | 7) as i64);
        let error_id = MessageId::from_raw((1i64 << 58) | 9);
        assert!(negative.raw() < 0);

        let both = bridge(Some(on_ack), Some(on_error));
        both.on_ack(negative);
        both.on_error(error_id, "boom");
        assert_eq!((acked(), failed()), (negative.raw(), error_id.raw()));

        let id = MessageId::from_raw((1i64 << 58) | 3);
        let ack_only = bridge(Some(on_ack), None);
        ack_only.on_ack(id);
        ack_only.on_error(MessageId::from_raw(0), "ignored");
        assert_eq!(
            (acked(), failed()),
            (id.raw(), error_id.raw()),
            "on_error must not run"
        );

        let error_only = bridge(None, Some(on_error));
        error_only.on_ack(MessageId::from_raw(0));
        error_only.on_error(id, "boom");
        assert_eq!(
            (acked(), failed()),
            (id.raw(), id.raw()),
            "on_ack must not run"
        );
    }

    #[test]
    fn serialized_headers_provider_allows_concurrent_lanes() {
        static ACTIVE: AtomicUsize = AtomicUsize::new(0);
        static MAX_ACTIVE: AtomicUsize = AtomicUsize::new(0);
        extern "C" fn slow_headers(_user_data: *mut c_void) -> CHeaders {
            let active = ACTIVE.fetch_add(1, Ordering::SeqCst) + 1;
            MAX_ACTIVE.fetch_max(active, Ordering::SeqCst);
            // Hold the call long enough for an unserialized second one to overlap.
            std::thread::sleep(Duration::from_millis(20));
            ACTIVE.fetch_sub(1, Ordering::SeqCst);
            empty_headers(ptr::null_mut())
        }

        let provider = Arc::new(SerializedHeadersProvider::new(
            CallbackHeadersProvider::new(slow_headers, ptr::null_mut(), None),
        ));
        RUNTIME.block_on(async {
            let calls: Vec<_> = (0..4)
                .map(|_| {
                    let provider = Arc::clone(&provider);
                    tokio::spawn(async move { provider.get_headers().await })
                })
                .collect();
            for call in calls {
                let result = call.await.unwrap();
                assert!(result.is_ok(), "concurrent lane call failed: {result:?}");
            }
        });
        assert_eq!(MAX_ACTIVE.load(Ordering::SeqCst), 1);
    }
}
