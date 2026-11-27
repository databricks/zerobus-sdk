//! Multiplexed stream FFI surface (Beta).
//!
//! A multiplexed stream distributes ingestion round-robin across
//! `stream_count` gRPC sub-streams to raise aggregate throughput. Ingest calls
//! return a message ID: an opaque `i64` that packs the sub-stream index and
//! that sub-stream's offset. Pass it back to
//! `zerobus_multiplexed_stream_wait_for_message_id`; do not compare or order
//! message IDs, and only use them with the stream that produced them.
//!
//! Thread-safety and callback rules are on `CZerobusMultiplexedStream`.

use crate::common::*;
use crate::stream::{
    configure_stream_builder, encoded_records_to_c_array, invoke_bool_async_callback,
    invoke_offset_async_callback, invoke_record_array_async_callback, SendPtr, StreamCreateAuth,
};
use async_trait::async_trait;
use databricks_zerobus_ingest_sdk::stream_options::defaults;
use databricks_zerobus_ingest_sdk::{
    AckCallback, EncodedRecord, GrpcCode, GrpcStatus, HeadersProvider, MessageId,
    MultiplexedStream, ZerobusError, ZerobusResult, ZerobusSdk,
};
use prost::Message;
use std::collections::{BTreeMap, HashMap};
use std::ffi::CString;
use std::future::Future;
use std::os::raw::c_char;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify, OwnedRwLockReadGuard, RwLock};

/// Returned by ingest calls on failure; `result` carries the error.
const INGEST_ERROR: i64 = -1;
/// Returned by batch ingest calls when the batch is empty. Not a message ID.
const EMPTY_BATCH: i64 = -2;

/// Opaque handle for a multiplexed stream (Beta). Created by
/// `zerobus_sdk_create_multiplexed_stream`,
/// `zerobus_sdk_create_multiplexed_stream_with_headers_provider`, or their
/// `_async` variants, and released by `zerobus_multiplexed_stream_free`.
///
/// Thread safety: every `zerobus_multiplexed_stream_*` call except `_free` may
/// run concurrently on the same handle from any thread. `_free` must not
/// overlap any other call on that handle.
///
/// Ack and headers callbacks run on the FFI's runtime threads. They must not
/// call `zerobus_multiplexed_stream_*` functions, and must not block on a
/// thread that is inside one: the call would fail, or deadlock while close
/// waits for callbacks to drain. Ack callbacks fire once per ingest call (one
/// for a whole batch), in order within each sub-stream.
#[repr(C)]
pub struct CZerobusMultiplexedStream {
    _private: [u8; 0],
}

/// Concrete type behind `*mut CZerobusMultiplexedStream`, owned by a `Box`.
///
/// Background tasks clone `stream` and `pending`, never the handle, so they
/// keep working after `zerobus_multiplexed_stream_free` drops it.
struct MuxHandle {
    /// `close` and `get_unacked_records` need `&mut MultiplexedStream`, while
    /// ingest, flush, and wait need only `&`, possibly from `_async` tasks that
    /// hold their own owned read guard. The lock lets those
    /// share the mux and makes a close wait for them instead of aliasing a
    /// `&mut` with live `&` borrows.
    stream: Arc<RwLock<MultiplexedStream>>,
    /// `_async` ingests not yet queued into the core, so flush can wait for
    /// them.
    pending: Arc<PendingIngests>,
    /// `flush_timeout_ms`, which bounds a flush's wait for `pending`; the core
    /// flush then has its own `flush_timeout_ms`.
    flush_timeout: Duration,
}

/// Counts `_async` ingests per flush epoch.
///
/// A flush starts a new epoch and waits only for submissions from earlier
/// epochs, so producers that keep submitting cannot hold it off indefinitely.
#[derive(Default)]
struct PendingIngests {
    epochs: std::sync::Mutex<PendingEpochs>,
    drained: Notify,
}

#[derive(Default)]
struct PendingEpochs {
    current: u64,
    /// Submissions not yet queued, by epoch; empty epochs are removed.
    pending: BTreeMap<u64, usize>,
}

impl PendingIngests {
    fn epochs(&self) -> std::sync::MutexGuard<'_, PendingEpochs> {
        // The state stays consistent even if a holder panicked.
        self.epochs.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Registers a submission in the current epoch.
    fn begin(self: &Arc<Self>) -> PendingTicket {
        let mut epochs = self.epochs();
        let epoch = epochs.current;
        *epochs.pending.entry(epoch).or_default() += 1;
        PendingTicket {
            tracker: Arc::clone(self),
            epoch,
        }
    }

    /// Closes the current epoch for a flush and returns it.
    fn start_flush(&self) -> u64 {
        let mut epochs = self.epochs();
        let target = epochs.current;
        epochs.current += 1;
        target
    }

    /// Waits until every submission from epoch `target` or earlier is queued.
    async fn wait_drained(&self, target: u64) {
        loop {
            let mut notified = std::pin::pin!(self.drained.notified());
            // Register before checking, so a drain in between is not missed.
            notified.as_mut().enable();
            if self.epochs().pending.range(..=target).next().is_none() {
                return;
            }
            notified.await;
        }
    }
}

/// Marks one `_async` submission as queued (or rejected) when dropped.
struct PendingTicket {
    tracker: Arc<PendingIngests>,
    epoch: u64,
}

impl Drop for PendingTicket {
    fn drop(&mut self) {
        let drained = {
            let mut epochs = self.tracker.epochs();
            match epochs.pending.get_mut(&self.epoch) {
                Some(count) if *count > 1 => {
                    *count -= 1;
                    false
                }
                _ => {
                    epochs.pending.remove(&self.epoch);
                    true
                }
            }
        };
        if drained {
            self.tracker.drained.notify_waiters();
        }
    }
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

/// Copies a borrowed byte buffer; a null pointer is accepted only when `len` is
/// 0.
unsafe fn copy_bytes(data: *const u8, len: usize) -> Result<Vec<u8>, &'static str> {
    if len == 0 {
        Ok(Vec::new())
    } else if data.is_null() {
        Err("Invalid data pointer")
    } else if len > isize::MAX as usize {
        Err("Invalid data length")
    } else {
        Ok(std::slice::from_raw_parts(data, len).to_vec())
    }
}

/// The largest record count whose pointer or length array is a valid slice:
/// `slice::from_raw_parts` needs it to span at most `isize::MAX` bytes.
const MAX_RECORDS: usize = isize::MAX as usize / std::mem::size_of::<*const u8>();

/// Copies a C array of protobuf payloads so the caller may free them on return.
/// The arrays may be NULL only when `num_records` is 0.
unsafe fn copy_proto_records(
    records: *const *const u8,
    record_lens: *const usize,
    num_records: usize,
) -> Result<Vec<EncodedRecord>, &'static str> {
    if num_records == 0 {
        return Ok(Vec::new());
    }
    if num_records > MAX_RECORDS {
        return Err("Invalid record count");
    }
    let ptrs = std::slice::from_raw_parts(records, num_records);
    let lens = std::slice::from_raw_parts(record_lens, num_records);
    ptrs.iter()
        .zip(lens)
        .map(|(&data, &len)| copy_bytes(data, len).map(EncodedRecord::Proto))
        .collect()
}

/// Copies a C array of NUL-terminated JSON strings so the caller may free them
/// on return. The array may be NULL only when `num_records` is 0.
unsafe fn copy_json_records(
    json_records: *const *const c_char,
    num_records: usize,
) -> Result<Vec<EncodedRecord>, &'static str> {
    if num_records == 0 {
        return Ok(Vec::new());
    }
    if num_records > MAX_RECORDS {
        return Err("Invalid record count");
    }
    std::slice::from_raw_parts(json_records, num_records)
        .iter()
        .map(|&json| c_str_to_string(json).map(EncodedRecord::Json))
        .collect()
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
/// gone. The C caller cannot observe that moment otherwise, since `_async`
/// tasks can outlive `zerobus_multiplexed_stream_free`.
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

/// `reject_options_ack_value` for a C pointer that may be NULL.
fn reject_options_ack(options: *const CStreamConfigurationOptions) -> ZerobusResult<()> {
    reject_options_ack_value((!options.is_null()).then(|| unsafe { &*options }))
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
    let flush_timeout = Duration::from_millis(
        options
            .as_ref()
            .map_or(defaults::FLUSH_TIMEOUT_MS, |c| c.flush_timeout_ms),
    );
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
        pending: Arc::default(),
        flush_timeout,
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
/// `zerobus_multiplexed_stream_close`). `_async` completion callbacks can still
/// run after close returns.
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
/// freeing an open stream abandons them. Pending `_async` operations keep the
/// stream alive and still call back exactly once. Must not run concurrently
/// with any other call on `mux` (it may be called from an `_async` completion
/// callback), and `mux` must not be used afterwards.
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

/// Ingest a protobuf-encoded record into the next sub-stream (Beta).
///
/// `data` may be NULL only when `data_len` is 0.
///
/// Returns once the record is queued, with a message ID to pass to
/// `zerobus_multiplexed_stream_wait_for_message_id`. Waits for room (up to 30
/// seconds) when the chosen sub-stream is at capacity. Returns -1 on error.
/// Valid message IDs can be negative, so check `result->success` rather than
/// the sign of the return value.
///
/// A failed call did not queue the record, and it will not be returned by
/// `zerobus_multiplexed_stream_get_unacked_records`. If
/// `zerobus_multiplexed_stream_is_closed` is true, send it, together with the
/// unacknowledged records, on a new stream. Otherwise retry it only when
/// `result->is_retryable` is set; a non-retryable error on an open mux means
/// the record itself is invalid (for example, its type does not match the
/// stream).
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_ingest_proto_record(
    mux: *mut CZerobusMultiplexedStream,
    data: *const u8,
    data_len: usize,
    result: *mut CResult,
) -> i64 {
    ffi_guard(result, INGEST_ERROR, move || {
        if data.is_null() && data_len != 0 {
            write_error_result(result, "Invalid data pointer", false);
            return INGEST_ERROR;
        }
        let handle = match validate_mux_ptr(mux) {
            Ok(h) => h,
            Err(msg) => {
                write_error_result(result, msg, false);
                return INGEST_ERROR;
            }
        };

        let record = match unsafe { copy_bytes(data, data_len) } {
            Ok(bytes) => EncodedRecord::Proto(bytes),
            Err(e) => {
                write_error_result(result, e, false);
                return INGEST_ERROR;
            }
        };
        let res =
            RUNTIME.block_on(async { handle.stream.read().await.ingest_record(record).await });
        complete(result, res.map(|id| id.raw()), INGEST_ERROR)
    })
}

/// Ingest a JSON record into the next sub-stream (Beta).
///
/// Same return contract as `zerobus_multiplexed_stream_ingest_proto_record`.
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_ingest_json_record(
    mux: *mut CZerobusMultiplexedStream,
    json_data: *const c_char,
    result: *mut CResult,
) -> i64 {
    ffi_guard(result, INGEST_ERROR, move || {
        let handle = match validate_mux_ptr(mux) {
            Ok(h) => h,
            Err(msg) => {
                write_error_result(result, msg, false);
                return INGEST_ERROR;
            }
        };
        let record = match unsafe { c_str_to_string(json_data) } {
            Ok(json) => EncodedRecord::Json(json),
            Err(e) => {
                write_error_result(result, e, false);
                return INGEST_ERROR;
            }
        };

        let res =
            RUNTIME.block_on(async { handle.stream.read().await.ingest_record(record).await });
        complete(result, res.map(|id| id.raw()), INGEST_ERROR)
    })
}

/// Ingest a batch of protobuf-encoded records into a single sub-stream (Beta).
///
/// The whole batch lands on one sub-stream, and the returned message ID covers
/// all of it. To spread load across sub-streams, send at least `stream_count`
/// batches rather than one large one.
///
/// The arrays may be NULL only when `num_records` is 0. Returns -1 on error.
/// For an empty batch it returns -2 with `result->success` set; -2 is not a
/// message ID, so do not wait on it. On a closed mux an empty batch fails
/// with -1, like any other batch.
/// Valid message IDs can be negative, so check `result->success` rather than
/// the sign. Errors follow
/// `zerobus_multiplexed_stream_ingest_proto_record`: a failed call queued none
/// of the batch.
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_ingest_proto_records(
    mux: *mut CZerobusMultiplexedStream,
    records: *const *const u8,
    record_lens: *const usize,
    num_records: usize,
    result: *mut CResult,
) -> i64 {
    ffi_guard(result, INGEST_ERROR, move || {
        if num_records != 0 && (records.is_null() || record_lens.is_null()) {
            write_error_result(result, "Invalid records pointer", false);
            return INGEST_ERROR;
        }
        let handle = match validate_mux_ptr(mux) {
            Ok(h) => h,
            Err(msg) => {
                write_error_result(result, msg, false);
                return INGEST_ERROR;
            }
        };

        let batch = match unsafe { copy_proto_records(records, record_lens, num_records) } {
            Ok(batch) => batch,
            Err(e) => {
                write_error_result(result, e, false);
                return INGEST_ERROR;
            }
        };
        let res =
            RUNTIME.block_on(async { handle.stream.read().await.ingest_records(batch).await });
        complete(
            result,
            res.map(|id| id.map_or(EMPTY_BATCH, |id| id.raw())),
            INGEST_ERROR,
        )
    })
}

/// Ingest a batch of JSON records into a single sub-stream (Beta).
///
/// Same return contract as `zerobus_multiplexed_stream_ingest_proto_records`.
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_ingest_json_records(
    mux: *mut CZerobusMultiplexedStream,
    json_records: *const *const c_char,
    num_records: usize,
    result: *mut CResult,
) -> i64 {
    ffi_guard(result, INGEST_ERROR, move || {
        if num_records != 0 && json_records.is_null() {
            write_error_result(result, "Invalid records pointer", false);
            return INGEST_ERROR;
        }
        let handle = match validate_mux_ptr(mux) {
            Ok(h) => h,
            Err(msg) => {
                write_error_result(result, msg, false);
                return INGEST_ERROR;
            }
        };
        let batch = match unsafe { copy_json_records(json_records, num_records) } {
            Ok(batch) => batch,
            Err(e) => {
                write_error_result(result, e, false);
                return INGEST_ERROR;
            }
        };

        let res =
            RUNTIME.block_on(async { handle.stream.read().await.ingest_records(batch).await });
        complete(
            result,
            res.map(|id| id.map_or(EMPTY_BATCH, |id| id.raw())),
            INGEST_ERROR,
        )
    })
}

enum IngestPayload {
    Record(EncodedRecord),
    Batch(Vec<EncodedRecord>),
    /// Panics in `ingest`, so tests can reach the panic paths.
    #[cfg(test)]
    PanicForTest,
}

impl IngestPayload {
    /// Ingests the payload; returns its raw message ID, or `EMPTY_BATCH`.
    async fn ingest(self, mux: &MultiplexedStream) -> ZerobusResult<i64> {
        match self {
            IngestPayload::Record(record) => mux.ingest_record(record).await.map(|id| id.raw()),
            IngestPayload::Batch(batch) => mux
                .ingest_records(batch)
                .await
                .map(|id| id.map_or(EMPTY_BATCH, |id| id.raw())),
            #[cfg(test)]
            IngestPayload::PanicForTest => panic!("IngestPayload::PanicForTest"),
        }
    }
}

/// Takes a shared lock on the mux for a background task without blocking.
///
/// The guard is taken before the call returns to C, so a later close or
/// `get_unacked_records` waits until the spawned task is done with the mux.
/// `try_read_owned` fails only while one of those holds or waits for the write
/// lock; both close the mux, so the call reports it as closing.
fn read_now(handle: &MuxHandle) -> ZerobusResult<OwnedRwLockReadGuard<MultiplexedStream>> {
    Arc::clone(&handle.stream)
        .try_read_owned()
        .map_err(|_| ZerobusError::InvalidStateError("MultiplexedStream is closing".to_string()))
}

/// Wait until the record or batch behind `message_id` is acknowledged (Beta).
///
/// Returns false with `result` set if the sub-stream that owns `message_id`
/// fails or the wait exceeds `flush_timeout_ms`. A failure on another
/// sub-stream does not fail this wait. Pass only IDs returned by a successful
/// ingest on this same stream; -1, -2, and IDs from another stream are not
/// valid input and may fail or wait on an unrelated record.
///
/// For high throughput, prefer one `zerobus_multiplexed_stream_flush` after an
/// ingest loop over waiting on every message ID.
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_wait_for_message_id(
    mux: *mut CZerobusMultiplexedStream,
    message_id: i64,
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
        let res = RUNTIME.block_on(async {
            handle
                .stream
                .read()
                .await
                .wait_for_message_id(MessageId::from_raw(message_id))
                .await
        });
        complete(result, res.map(|()| true), false)
    })
}

/// Waits up to `timeout` for `drained` (the `_async` ingests a flush covers
/// to be queued), then runs the core flush with its own `flush_timeout_ms`.
/// The core flush is never cancelled, so a sub-stream failure keeps its
/// typed cause and closes the mux. If the drain timed out but the core flush
/// succeeded, some covered ingests were not flushed: reports the core's
/// flush-timeout error.
async fn drain_then_flush(
    timeout: Duration,
    drained: impl Future<Output = ()>,
    mux: &MultiplexedStream,
) -> ZerobusResult<()> {
    let drained = tokio::time::timeout(timeout, drained).await.is_ok();
    mux.flush().await?;
    if drained {
        Ok(())
    } else {
        Err(ZerobusError::StreamClosedError(GrpcStatus::new(
            GrpcCode::DeadlineExceeded,
            "Flush timed out",
        )))
    }
}

/// Wait until every record queued on every sub-stream is acknowledged (Beta).
///
/// Covers blocking ingests that have returned and every `_async` ingest
/// accepted before this call: it first waits, up to `flush_timeout_ms`, for
/// their background tasks to queue them, then waits up to `flush_timeout_ms`
/// again for the acknowledgments, so a flush can take up to twice
/// `flush_timeout_ms`. If the first wait times out, the flush still flushes
/// what is queued, then reports a timeout. A timeout does not mean the
/// records are lost: some may be acknowledged already, so recover with
/// `zerobus_multiplexed_stream_get_unacked_records`, which returns only the
/// ones that are not, instead of re-sending everything. `_async` ingests
/// submitted during the flush may not be covered; the next flush or close
/// covers them.
///
/// Returns false with `result` set on a timeout, on a closed mux, or once any
/// sub-stream has failed, even if the other sub-streams flushed. Check
/// `zerobus_multiplexed_stream_is_closed` to tell a failed mux from a
/// timeout.
///
/// A terminal sub-stream failure closes the mux for further ingestion; its
/// unacknowledged records remain available via
/// `zerobus_multiplexed_stream_get_unacked_records`.
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_flush(
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
        let res = RUNTIME.block_on(async {
            let target = handle.pending.start_flush();
            // Queues behind a close in progress; the timeout starts after it.
            let mux = handle.stream.read().await;
            let drained = handle.pending.wait_drained(target);
            drain_then_flush(handle.flush_timeout, drained, &mux).await
        });
        complete(result, res.map(|()| true), false)
    })
}

/// Flush and close every sub-stream (Beta).
///
/// Waits for in-progress ingest, flush, and wait calls on this mux to return,
/// and for every accepted `_async` ingest to be queued, then flushes those too.
/// On error, call `zerobus_multiplexed_stream_get_unacked_records` to recover
/// unacknowledged records. The handle must still be released with
/// `zerobus_multiplexed_stream_free`.
///
/// Returns false with `result` set on failure; the mux is closed either way.
/// When close returns, all ack callbacks have run, unless close waited longer
/// than `callback_max_wait_time_ms` (default 5 s). With
/// `has_callback_max_wait_time_ms` false there is no limit. After it, queued
/// callbacks are dropped and a running one can outlive close. `_async`
/// completion callbacks can still run after close returns. Close runs its own
/// flush, also after a flush that timed out, and a flush can itself take twice
/// `flush_timeout_ms`, so a flush followed by a close can wait up to three
/// times `flush_timeout_ms`.
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

/// Returns whether the mux is closed (Beta): either explicitly or because an
/// ingest, flush, or wait call on it observed a sub-stream failure. A failure
/// that no call has observed yet does not set it. Also returns true while a
/// close is in progress and for a null handle.
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_is_closed(
    mux: *mut CZerobusMultiplexedStream,
) -> bool {
    ffi_guard(ptr::null_mut(), true, move || match validate_mux_ptr(mux) {
        Ok(handle) => handle.stream.try_read().map_or(true, |mux| mux.is_closed()),
        Err(_) => true,
    })
}

/// Close the mux and return every record that was not acknowledged (Beta).
///
/// Errors from the implicit close are ignored so recoverable records are still
/// returned. Free the array with `zerobus_free_record_array`.
///
/// Each record's `data` holds `data_len` bytes with no NUL terminator. Copy a
/// JSON record (`is_json`) into a NUL-terminated buffer before passing it to
/// a JSON ingest.
///
/// Every call returns the same records, so re-send them once, from one
/// caller.
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_get_unacked_records(
    mux: *mut CZerobusMultiplexedStream,
    result: *mut CResult,
) -> CRecordArray {
    let empty = || CRecordArray {
        records: ptr::null_mut(),
        len: 0,
    };
    ffi_guard(result, empty(), move || {
        let handle = match validate_mux_ptr(mux) {
            Ok(h) => h,
            Err(msg) => {
                write_error_result(result, msg, false);
                return empty();
            }
        };
        let res = RUNTIME.block_on(async {
            let mut mux = handle.stream.write().await;
            Ok(mux.get_unacked_records().await?.collect::<Vec<_>>())
        });
        complete(result, res.and_then(encoded_records_to_c_array), empty())
    })
}

// Async variants of the multiplexed stream functions.

/// Lets the work this task just woke, such as a sub-stream's sender parked in
/// this worker's LIFO slot, run before a completion callback, which may block.
async fn yield_before_callback() {
    tokio::task::yield_now().await;
}

/// Spawns `work`, which ends by invoking a C completion callback. If `work`
/// panics first, `on_panic` reports the panic instead, so the callback still
/// runs exactly once and the caller is not left waiting. Guards and tickets
/// held by `work` are released while it unwinds.
fn spawn_with_panic_report<W>(work: W, on_panic: impl FnOnce(CResult) + Send + 'static)
where
    W: std::future::Future<Output = ()> + Send + 'static,
{
    RUNTIME.spawn(async move {
        if let Err(err) = RUNTIME.spawn(work).await {
            if err.is_panic() {
                let message = panic_message(err.into_panic().as_ref());
                on_panic(CResult {
                    success: false,
                    error_message: CString::new(message)
                        .unwrap_or_else(|_| {
                            CString::new("Rust panic caught at FFI boundary").unwrap()
                        })
                        .into_raw(),
                    is_retryable: false,
                });
            }
        }
    });
}

/// Completion callback for the async multiplexed creates (Beta).
///
/// `mux` is non-null on success, and the caller then owns it; it is null on
/// failure.
///
/// Every multiplexed `_async` function follows the same contract. It checks
/// its C arguments and returns at once: false with `result` set if a check
/// fails, in which case its callback never runs; otherwise true, and every
/// other error, including a closed mux, reaches the callback. Async ingests
/// report a close in progress as an error; async wait and flush wait for it,
/// as the blocking calls do.
/// The callback then runs exactly once, on a runtime thread, possibly before
/// the call returns: set up everything its `user_data` points to before the
/// call. The `result` it
/// receives is valid only during the call: do not free
/// `result->error_message` (the FFI frees it when the callback returns), and
/// copy it to keep it. The callback must be thread-safe and must not unwind.
/// It may call the `_async` functions, `zerobus_multiplexed_stream_is_closed`
/// and `zerobus_multiplexed_stream_free`, but no blocking function: no other
/// `zerobus_multiplexed_stream_*` call, no sync create, and no blocking
/// `zerobus_stream_*` call. Call `zerobus_multiplexed_stream_free` from a
/// callback only after the `_async` call that started it has returned, since
/// free must not overlap another call on the handle.
/// For an `_async` ingest, the record's ack callback can run before its
/// completion callback. Flush and close wait until accepted `_async` ingests
/// are queued, not for their completion callbacks, and a failed `_async`
/// ingest is not returned by `zerobus_multiplexed_stream_get_unacked_records`:
/// keep your copy of a record until its callback reports success.
/// The callback must not be NULL, and the `user_data` passed with it must
/// stay valid until it runs.
/// Callbacks run on a runtime worker thread: one that blocks holds that
/// thread, delaying other stream work scheduled on it, so return quickly and
/// hand slow work to your own thread.
pub type CreateMultiplexedStreamAsyncCallback = extern "C" fn(
    mux: *mut CZerobusMultiplexedStream,
    result: *const CResult,
    user_data: *mut std::ffi::c_void,
);

fn invoke_create_mux_async_callback(
    callback: CreateMultiplexedStreamAsyncCallback,
    mux: *mut CZerobusMultiplexedStream,
    result: CResult,
    user_data: *mut std::ffi::c_void,
) {
    if catch_unwind(AssertUnwindSafe(|| callback(mux, &result, user_data))).is_err() {
        tracing::error!("async multiplexed create callback panicked; contained at FFI boundary");
    }
    if !result.error_message.is_null() {
        unsafe {
            let _ = CString::from_raw(result.error_message);
        }
    }
}

/// Builds the mux on a runtime task and reports it through `callback`.
#[allow(clippy::too_many_arguments)]
fn spawn_create(
    sdk: *mut CZerobusSdk,
    table_name: String,
    descriptor_proto: Option<prost_types::DescriptorProto>,
    auth: StreamCreateAuth,
    stream_count: usize,
    options: Option<CStreamConfigurationOptions>,
    ack_callback: MultiplexedCallbackAckCallback,
    callback: CreateMultiplexedStreamAsyncCallback,
    callback_user_data: *mut std::ffi::c_void,
) {
    let sdk = SendPtr::new(sdk);
    let callback_user_data = SendPtr::new(callback_user_data);
    spawn_with_panic_report(
        async move {
            let res = match validate_sdk_ptr(sdk.get()) {
                Ok(sdk_ref) => {
                    build_mux_from_parts(
                        sdk_ref,
                        table_name,
                        descriptor_proto,
                        auth,
                        stream_count,
                        options,
                        ack_callback,
                    )
                    .await
                }
                Err(msg) => Err(ZerobusError::InvalidArgument(msg.to_string())),
            };
            let (mux, result) = match res {
                Ok(mux) => (mux, CResult::success()),
                Err(err) => (ptr::null_mut(), CResult::error(err)),
            };
            invoke_create_mux_async_callback(callback, mux, result, callback_user_data.get());
        },
        move |result| {
            invoke_create_mux_async_callback(
                callback,
                ptr::null_mut(),
                result,
                callback_user_data.get(),
            )
        },
    );
}

/// Create a multiplexed stream with OAuth authentication on a background
/// task (Beta).
///
/// Same arguments and ack-callback ownership as
/// `zerobus_sdk_create_multiplexed_stream`; strings, the descriptor, and
/// `options` are copied before this returns, but `sdk` must stay valid until
/// `callback` runs. A null `sdk`, invalid strings, an undecodable descriptor
/// and a set `options->ack_on_ack`/`ack_on_error` return false. Everything
/// else, including an out-of-range `stream_count`, `max_inflight_requests`
/// below `stream_count`, and a failed connect, reaches `callback` with a
/// null `mux`. `free_ack_user_data` can run before or after `callback`, so
/// do not share `callback_user_data` with `ack_user_data`.
///
/// Follows the multiplexed `_async` contract described on
/// `CreateMultiplexedStreamAsyncCallback`.
#[no_mangle]
pub extern "C" fn zerobus_sdk_create_multiplexed_stream_async(
    sdk: *mut CZerobusSdk,
    table_name: *const c_char,
    descriptor_proto_bytes: *const u8,
    descriptor_proto_len: usize,
    client_id: *const c_char,
    client_secret: *const c_char,
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
    callback: CreateMultiplexedStreamAsyncCallback,
    callback_user_data: *mut std::ffi::c_void,
    result: *mut CResult,
) -> bool {
    ffi_guard(result, false, move || {
        // Take ownership of `ack_user_data` before any fallible work, so an
        // early return frees it exactly once.
        let ack_callback = MultiplexedCallbackAckCallback::new(
            ack_on_ack,
            ack_on_error,
            ack_user_data,
            free_ack_user_data,
        );
        if let Err(msg) = validate_sdk_ptr(sdk) {
            write_error_result(result, msg, false);
            return false;
        }
        if let Err(err) = reject_options_ack(options) {
            return complete(result, Err(err), false);
        }
        let parts = (|| {
            let auth = StreamCreateAuth::OAuth {
                client_id: unsafe { c_str_arg(client_id)? },
                client_secret: unsafe { c_str_arg(client_secret)? },
            };
            ZerobusResult::Ok((
                unsafe { c_str_arg(table_name)? },
                unsafe { decode_descriptor(descriptor_proto_bytes, descriptor_proto_len)? },
                auth,
            ))
        })();
        let (table_name, descriptor_proto, auth) = match parts {
            Ok(parts) => parts,
            Err(err) => return complete(result, Err(err), false),
        };
        write_success_result(result);
        spawn_create(
            sdk,
            table_name,
            descriptor_proto,
            auth,
            stream_count,
            (!options.is_null()).then(|| unsafe { *options }),
            ack_callback,
            callback,
            callback_user_data,
        );
        true
    })
}

/// Create a multiplexed stream with a custom headers provider on a background
/// task (Beta).
///
/// Same arguments and ownership of `user_data` and `ack_user_data` as
/// `zerobus_sdk_create_multiplexed_stream_with_headers_provider`; otherwise
/// behaves like `zerobus_sdk_create_multiplexed_stream_async`.
/// `free_user_data` and `free_ack_user_data` can run before or after
/// `callback`, so do not share `callback_user_data` with `user_data` or
/// `ack_user_data`.
///
/// Follows the multiplexed `_async` contract described on
/// `CreateMultiplexedStreamAsyncCallback`.
#[no_mangle]
pub extern "C" fn zerobus_sdk_create_multiplexed_stream_with_headers_provider_async(
    sdk: *mut CZerobusSdk,
    table_name: *const c_char,
    descriptor_proto_bytes: *const u8,
    descriptor_proto_len: usize,
    headers_callback: HeadersProviderCallback,
    user_data: *mut std::ffi::c_void,
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
    callback: CreateMultiplexedStreamAsyncCallback,
    callback_user_data: *mut std::ffi::c_void,
    result: *mut CResult,
) -> bool {
    ffi_guard(result, false, move || {
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
        if let Err(msg) = validate_sdk_ptr(sdk) {
            write_error_result(result, msg, false);
            return false;
        }
        if let Err(err) = reject_options_ack(options) {
            return complete(result, Err(err), false);
        }
        let parts = (|| {
            ZerobusResult::Ok((unsafe { c_str_arg(table_name)? }, unsafe {
                decode_descriptor(descriptor_proto_bytes, descriptor_proto_len)?
            }))
        })();
        let (table_name, descriptor_proto) = match parts {
            Ok(parts) => parts,
            Err(err) => return complete(result, Err(err), false),
        };
        write_success_result(result);
        spawn_create(
            sdk,
            table_name,
            descriptor_proto,
            StreamCreateAuth::HeadersProvider { headers_provider },
            stream_count,
            (!options.is_null()).then(|| unsafe { *options }),
            ack_callback,
            callback,
            callback_user_data,
        );
        true
    })
}

/// Completion callback for the multiplexed `_async` ingests (Beta).
///
/// `message_id` is what the blocking ingest would return: an opaque message
/// ID (it can be negative and is not ordered, so do not compare IDs), -2 with
/// success for an empty batch, or -1 with the error in `result`. Same ABI as
/// `OffsetAsyncCallback`. Follows the multiplexed `_async` contract described
/// on `CreateMultiplexedStreamAsyncCallback`.
pub type MessageIdAsyncCallback =
    extern "C" fn(message_id: i64, result: *const CResult, user_data: *mut std::ffi::c_void);

/// Queues an ingest on a background task and reports its message ID.
///
/// The read guard and a flush ticket are taken before the call returns, so
/// flush, close and `get_unacked_records` cover the submission.
/// Both are released before `callback` runs.
fn spawn_ingest_async(
    handle: &MuxHandle,
    payload: IngestPayload,
    callback: MessageIdAsyncCallback,
    user_data: *mut std::ffi::c_void,
) {
    let user_data = SendPtr::new(user_data);
    let guard = match read_now(handle) {
        Ok(guard) => guard,
        Err(err) => {
            RUNTIME.spawn(async move {
                invoke_offset_async_callback(
                    callback,
                    INGEST_ERROR,
                    CResult::error(err),
                    user_data.get(),
                );
            });
            return;
        }
    };
    let ticket = handle.pending.begin();
    spawn_with_panic_report(
        async move {
            let res = payload.ingest(&guard).await;
            drop(ticket);
            drop(guard);
            yield_before_callback().await;
            let (id, result) = match res {
                Ok(id) => (id, CResult::success()),
                Err(err) => (INGEST_ERROR, CResult::error(err)),
            };
            invoke_offset_async_callback(callback, id, result, user_data.get());
        },
        move |result| invoke_offset_async_callback(callback, INGEST_ERROR, result, user_data.get()),
    );
}

/// Ingest a protobuf record on a background task and report its message ID
/// via `callback` (Beta).
///
/// Copies the record before returning. `callback` receives the message ID, or
/// -1 with the error, exactly as `zerobus_multiplexed_stream_ingest_proto_record`
/// would return it; a failed ingest queued nothing. Records accepted before a
/// call to flush, close or `get_unacked_records` (blocking or async) are
/// queued before it proceeds, so it covers them. A closed or closing mux is
/// reported through `callback`. `data` may be NULL only when `data_len` is 0.
///
/// `_async` ingests have no back-pressure: each accepted call starts a task,
/// so limit how many are outstanding. One that waits for capacity longer
/// than 30 seconds fails in its callback. Tasks can queue records in a
/// different order than the calls were made.
///
/// Follows the multiplexed `_async` contract described on
/// `CreateMultiplexedStreamAsyncCallback`.
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_ingest_proto_record_async(
    mux: *mut CZerobusMultiplexedStream,
    data: *const u8,
    data_len: usize,
    callback: MessageIdAsyncCallback,
    user_data: *mut std::ffi::c_void,
    result: *mut CResult,
) -> bool {
    ffi_guard(result, false, move || {
        if data.is_null() && data_len != 0 {
            write_error_result(result, "Invalid data pointer", false);
            return false;
        }
        let handle = match validate_mux_ptr(mux) {
            Ok(h) => h,
            Err(msg) => {
                write_error_result(result, msg, false);
                return false;
            }
        };
        let record = match unsafe { copy_bytes(data, data_len) } {
            Ok(bytes) => EncodedRecord::Proto(bytes),
            Err(e) => {
                write_error_result(result, e, false);
                return false;
            }
        };
        write_success_result(result);
        spawn_ingest_async(handle, IngestPayload::Record(record), callback, user_data);
        true
    })
}

/// Ingest a JSON record on a background task and report its message ID via
/// `callback` (Beta).
///
/// Same contract as `zerobus_multiplexed_stream_ingest_proto_record_async`.
///
/// Follows the multiplexed `_async` contract described on
/// `CreateMultiplexedStreamAsyncCallback`.
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_ingest_json_record_async(
    mux: *mut CZerobusMultiplexedStream,
    json_data: *const c_char,
    callback: MessageIdAsyncCallback,
    user_data: *mut std::ffi::c_void,
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
        let record = match unsafe { c_str_to_string(json_data) } {
            Ok(json) => EncodedRecord::Json(json),
            Err(e) => {
                write_error_result(result, e, false);
                return false;
            }
        };
        write_success_result(result);
        spawn_ingest_async(handle, IngestPayload::Record(record), callback, user_data);
        true
    })
}

/// Ingest a batch of protobuf records on a background task and report its
/// message ID via `callback` (Beta).
///
/// Copies every record before returning. `callback` receives what
/// `zerobus_multiplexed_stream_ingest_proto_records` would return: the
/// batch's message ID, -2 with success for an empty batch (not a message ID),
/// or -1 with the error. Otherwise as
/// `zerobus_multiplexed_stream_ingest_proto_record_async`.
///
/// Follows the multiplexed `_async` contract described on
/// `CreateMultiplexedStreamAsyncCallback`.
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_ingest_proto_records_async(
    mux: *mut CZerobusMultiplexedStream,
    records: *const *const u8,
    record_lens: *const usize,
    num_records: usize,
    callback: MessageIdAsyncCallback,
    user_data: *mut std::ffi::c_void,
    result: *mut CResult,
) -> bool {
    ffi_guard(result, false, move || {
        if num_records != 0 && (records.is_null() || record_lens.is_null()) {
            write_error_result(result, "Invalid records pointer", false);
            return false;
        }
        let handle = match validate_mux_ptr(mux) {
            Ok(h) => h,
            Err(msg) => {
                write_error_result(result, msg, false);
                return false;
            }
        };
        let batch = match unsafe { copy_proto_records(records, record_lens, num_records) } {
            Ok(batch) => batch,
            Err(e) => {
                write_error_result(result, e, false);
                return false;
            }
        };
        write_success_result(result);
        spawn_ingest_async(handle, IngestPayload::Batch(batch), callback, user_data);
        true
    })
}

/// Ingest a batch of JSON records on a background task and report its
/// message ID via `callback` (Beta).
///
/// Same contract as `zerobus_multiplexed_stream_ingest_proto_records_async`.
///
/// Follows the multiplexed `_async` contract described on
/// `CreateMultiplexedStreamAsyncCallback`.
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_ingest_json_records_async(
    mux: *mut CZerobusMultiplexedStream,
    json_records: *const *const c_char,
    num_records: usize,
    callback: MessageIdAsyncCallback,
    user_data: *mut std::ffi::c_void,
    result: *mut CResult,
) -> bool {
    ffi_guard(result, false, move || {
        if num_records != 0 && json_records.is_null() {
            write_error_result(result, "Invalid records pointer", false);
            return false;
        }
        let handle = match validate_mux_ptr(mux) {
            Ok(h) => h,
            Err(msg) => {
                write_error_result(result, msg, false);
                return false;
            }
        };
        let batch = match unsafe { copy_json_records(json_records, num_records) } {
            Ok(batch) => batch,
            Err(e) => {
                write_error_result(result, e, false);
                return false;
            }
        };
        write_success_result(result);
        spawn_ingest_async(handle, IngestPayload::Batch(batch), callback, user_data);
        true
    })
}

/// Wait for a message ID on a background task and report completion via
/// `callback` (Beta).
///
/// `callback` receives what `zerobus_multiplexed_stream_wait_for_message_id`
/// would return. Like it, the wait starts after a close in progress
/// finishes, and then reports the result on the closed mux. For high
/// throughput, prefer one flush after an ingest loop over waiting on every
/// message ID.
///
/// Follows the multiplexed `_async` contract described on
/// `CreateMultiplexedStreamAsyncCallback`.
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_wait_for_message_id_async(
    mux: *mut CZerobusMultiplexedStream,
    message_id: i64,
    callback: BoolAsyncCallback,
    user_data: *mut std::ffi::c_void,
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
        let stream = Arc::clone(&handle.stream);
        let user_data = SendPtr::new(user_data);
        write_success_result(result);
        spawn_with_panic_report(
            async move {
                // Queues behind a close in progress, as the blocking wait does.
                let res = stream
                    .read_owned()
                    .await
                    .wait_for_message_id(MessageId::from_raw(message_id))
                    .await;
                yield_before_callback().await;
                let (value, result) = match res {
                    Ok(()) => (true, CResult::success()),
                    Err(err) => (false, CResult::error(err)),
                };
                invoke_bool_async_callback(callback, value, result, user_data.get());
            },
            move |result| invoke_bool_async_callback(callback, false, result, user_data.get()),
        );
        true
    })
}

/// Flush on a background task and report completion via `callback` (Beta).
///
/// Covers the same records as `zerobus_multiplexed_stream_flush`: every
/// blocking and `_async` ingest accepted before this call. Like
/// the blocking flush, it starts after a close in progress finishes, and then
/// reports the result on the closed mux.
///
/// Follows the multiplexed `_async` contract described on
/// `CreateMultiplexedStreamAsyncCallback`.
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_flush_async(
    mux: *mut CZerobusMultiplexedStream,
    callback: BoolAsyncCallback,
    user_data: *mut std::ffi::c_void,
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
        // Fix the epoch now, so submissions accepted later are not waited for.
        let target = handle.pending.start_flush();
        let pending = Arc::clone(&handle.pending);
        let stream = Arc::clone(&handle.stream);
        let flush_timeout = handle.flush_timeout;
        let user_data = SendPtr::new(user_data);
        write_success_result(result);
        spawn_with_panic_report(
            async move {
                // Queues behind a close in progress, as the blocking flush does.
                let mux = stream.read_owned().await;
                let res = drain_then_flush(flush_timeout, pending.wait_drained(target), &mux).await;
                drop(mux);
                yield_before_callback().await;
                let (value, result) = match res {
                    Ok(()) => (true, CResult::success()),
                    Err(err) => (false, CResult::error(err)),
                };
                invoke_bool_async_callback(callback, value, result, user_data.get());
            },
            move |result| invoke_bool_async_callback(callback, false, result, user_data.get()),
        );
        true
    })
}

/// Flush and close every sub-stream on a background task and report
/// completion via `callback` (Beta).
///
/// Behaves like `zerobus_multiplexed_stream_close`, waiting for in-flight calls
/// and accepted `_async` ingests first. The close starts when its background
/// task runs, so right after this returns
/// `zerobus_multiplexed_stream_is_closed` can still be false, and calls made
/// then are accepted and covered by the close. Once the close has started,
/// `_async` ingests fail at once, and other calls wait for it and then report
/// on the closed mux. The handle must still be released with
/// `zerobus_multiplexed_stream_free`, which may happen before `callback`
/// runs.
///
/// Follows the multiplexed `_async` contract described on
/// `CreateMultiplexedStreamAsyncCallback`.
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_close_async(
    mux: *mut CZerobusMultiplexedStream,
    callback: BoolAsyncCallback,
    user_data: *mut std::ffi::c_void,
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
        let stream = Arc::clone(&handle.stream);
        let user_data = SendPtr::new(user_data);
        write_success_result(result);
        spawn_with_panic_report(
            async move {
                let res = stream.write_owned().await.close().await;
                yield_before_callback().await;
                let (value, result) = match res {
                    Ok(()) => (true, CResult::success()),
                    Err(err) => (false, CResult::error(err)),
                };
                invoke_bool_async_callback(callback, value, result, user_data.get());
            },
            move |result| invoke_bool_async_callback(callback, false, result, user_data.get()),
        );
        true
    })
}

/// Close the mux on a background task and report every unacknowledged record
/// via `callback` (Beta).
///
/// Behaves like `zerobus_multiplexed_stream_get_unacked_records`. On success
/// the callback owns the array and frees it with `zerobus_free_record_array`.
/// Like `zerobus_multiplexed_stream_close_async`, it starts when its
/// background task runs: calls made right after it returns can still be
/// accepted, and once it has started, `_async` ingests fail at once and other
/// calls wait for it.
///
/// Follows the multiplexed `_async` contract described on
/// `CreateMultiplexedStreamAsyncCallback`.
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_get_unacked_records_async(
    mux: *mut CZerobusMultiplexedStream,
    callback: RecordArrayAsyncCallback,
    user_data: *mut std::ffi::c_void,
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
        let stream = Arc::clone(&handle.stream);
        let user_data = SendPtr::new(user_data);
        write_success_result(result);
        spawn_with_panic_report(
            async move {
                let res = async {
                    let mut mux = stream.write_owned().await;
                    Ok(mux.get_unacked_records().await?.collect::<Vec<_>>())
                }
                .await;
                yield_before_callback().await;
                let (records, result) = match res.and_then(encoded_records_to_c_array) {
                    Ok(records) => (records, CResult::success()),
                    Err(err) => (
                        CRecordArray {
                            records: ptr::null_mut(),
                            len: 0,
                        },
                        CResult::error(err),
                    ),
                };
                invoke_record_array_async_callback(callback, records, result, user_data.get());
            },
            move |result| {
                invoke_record_array_async_callback(
                    callback,
                    CRecordArray {
                        records: ptr::null_mut(),
                        len: 0,
                    },
                    result,
                    user_data.get(),
                )
            },
        );
        true
    })
}

#[cfg(test)]
mod multiplexed_tests {
    //! Pure FFI-layer tests: C argument checks, record copying, the ack
    //! bridge, the serialized headers provider, the pending-ingest tracker,
    //! and the panic paths (they need the `cfg(test)` seam). Create and
    //! `_async` argument tests run against the mock server in
    //! `rust/tests/src/multiplexed_ffi_tests.rs`.
    use super::*;
    use crate::zerobus_free_error_message;
    use std::ffi::{c_void, CStr, CString};
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
    use std::time::Duration;

    // Short names for the exports, so each table row fits on one line.
    use super::{
        zerobus_multiplexed_stream_close as close, zerobus_multiplexed_stream_flush as flush,
        zerobus_multiplexed_stream_get_unacked_records as unacked,
        zerobus_multiplexed_stream_ingest_json_record as json1,
        zerobus_multiplexed_stream_ingest_json_records as jsons,
        zerobus_multiplexed_stream_ingest_proto_record as proto1,
        zerobus_multiplexed_stream_ingest_proto_records as protos,
        zerobus_multiplexed_stream_wait_for_message_id as wait,
    };

    const NULL_HANDLE: &str = "Multiplexed stream pointer is null";
    const BAD_DATA: &str = "Invalid data pointer";
    const BAD_RECORDS: &str = "Invalid records pointer";

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

    /// A blocking flush or close on `mux` on a thread, bounded to 2 s:
    /// `(returned, retryable, message)`.
    fn blocking_within_2s(
        mux: *mut CZerobusMultiplexedStream,
        call: extern "C" fn(*mut CZerobusMultiplexedStream, *mut CResult) -> bool,
        what: &str,
    ) -> (bool, bool, String) {
        let mux_addr = mux as usize;
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut result = unwritten_result();
            let returned = call(mux_addr as *mut _, &mut result);
            let (_, retryable, message) = take_full(&mut result);
            let _ = tx.send((returned, retryable, message));
        });
        rx.recv_timeout(Duration::from_secs(2))
            .unwrap_or_else(|_| panic!("{what} did not return within 2 s"))
    }

    /// A live 1-lane JSON mux on the mock server, for the panic-path tests.
    fn live_mux() -> (
        tokio::runtime::Runtime,
        *mut CZerobusSdk,
        *mut CZerobusMultiplexedStream,
    ) {
        use zerobus_test_support::mock_grpc::{start_mock_server, MockResponse};
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (server, url) = runtime.block_on(start_mock_server()).unwrap();
        // The mock opens the lane itself; offset 0 is acked.
        let ack = MockResponse::RecordAck {
            ack_up_to_offset: 0,
            delay_ms: 0,
        };
        runtime.block_on(server.inject_responses("ffi.mux.panic", vec![ack]));
        let endpoint = CString::new(url).unwrap();
        let builder = crate::zerobus_sdk_builder_new();
        crate::zerobus_sdk_builder_endpoint(builder, endpoint.as_ptr());
        crate::zerobus_sdk_builder_unity_catalog_url(builder, endpoint.as_ptr());
        crate::zerobus_sdk_builder_disable_tls(builder);
        let mut result = unwritten_result();
        let sdk = crate::zerobus_sdk_builder_build(builder, &mut result);
        assert!(result.success && !sdk.is_null());
        // JSON records; recovery stays on: with it off, close can report the
        // lane's end-of-stream as its own error.
        let mut options = crate::zerobus_get_default_config();
        options.record_type = 2;
        let table = CString::new("ffi.mux.panic").unwrap();
        let mux = zerobus_sdk_create_multiplexed_stream_with_headers_provider(
            sdk,
            table.as_ptr(),
            ptr::null(),
            0,
            empty_headers,
            ptr::null_mut(),
            None,
            1,
            &options,
            None,
            None,
            ptr::null_mut(),
            None,
            &mut result,
        );
        let (success, _, message) = take_full(&mut result);
        assert!(success && !mux.is_null(), "{message}");
        (runtime, sdk, mux)
    }

    /// Which argument a C-argument row passes as NULL (the mux is always
    /// NULL, so a pointer-argument error proves it is checked first).
    #[derive(Debug, Clone, Copy, PartialEq)]
    enum Null {
        Mux,
        Data,
        // A proto batch's lens array, checked in the same condition as its
        // records but in each export's own code.
        Lens,
    }

    #[derive(Debug, Clone, Copy)]
    enum Blocking {
        Proto,
        Json,
        Protos,
        Jsons,
        Wait,
        Flush,
        Close,
        Unacked,
    }

    /// Every blocking export checks its C arguments: its inline pointer check
    /// first, then the handle, each with a pinned, non-retryable error, and
    /// the return value reports the failure too. One row per check per
    /// export.
    #[test]
    fn c_arguments_are_checked_before_the_handle() {
        use Blocking::*;
        let (data, lens, json) = ([1u8], [1usize], CString::new("{}").unwrap());
        let (data_array, json_array) = ([data.as_ptr()], [json.as_ptr()]);
        let null = ptr::null_mut();
        // Whether the return value reports the failure.
        let run = |export: Blocking, arg: Null, r: &mut CResult| -> bool {
            let nul = arg == Null::Data;
            let data = if nul { ptr::null() } else { data.as_ptr() };
            let recs = if nul {
                ptr::null()
            } else {
                data_array.as_ptr()
            };
            let jrecs = if nul {
                ptr::null()
            } else {
                json_array.as_ptr()
            };
            let lens = if arg == Null::Lens {
                ptr::null()
            } else {
                lens.as_ptr()
            };
            let json = json.as_ptr();
            match export {
                Proto => proto1(null, data, 1, r) == INGEST_ERROR,
                Json => json1(null, json, r) == INGEST_ERROR,
                Protos => protos(null, recs, lens, 1, r) == INGEST_ERROR,
                Jsons => jsons(null, jrecs, 1, r) == INGEST_ERROR,
                Wait => !wait(null, 0, r),
                Flush => !flush(null, r),
                Close => !close(null, r),
                Unacked => {
                    let records = unacked(null, r);
                    records.records.is_null() && records.len == 0
                }
            }
        };
        let rows = [
            (Proto, Null::Mux, NULL_HANDLE),
            (Proto, Null::Data, BAD_DATA),
            (Json, Null::Mux, NULL_HANDLE),
            (Protos, Null::Mux, NULL_HANDLE),
            (Protos, Null::Data, BAD_RECORDS),
            (Protos, Null::Lens, BAD_RECORDS),
            (Jsons, Null::Mux, NULL_HANDLE),
            (Jsons, Null::Data, BAD_RECORDS),
            (Wait, Null::Mux, NULL_HANDLE),
            (Flush, Null::Mux, NULL_HANDLE),
            (Close, Null::Mux, NULL_HANDLE),
            (Unacked, Null::Mux, NULL_HANDLE),
        ];
        for (export, arg, expected) in rows {
            let what = format!("{export:?} {arg:?}=NULL");
            let mut result = unwritten_result();
            assert!(
                run(export, arg, &mut result),
                "{what}: the return value must report the failure"
            );
            let (success, retryable, message) = take_full(&mut result);
            assert!(!success && !retryable, "{what}: {message}");
            assert!(message.contains(expected), "{what}: {message}");
        }
        assert!(zerobus_multiplexed_stream_is_closed(null));
        zerobus_multiplexed_stream_free(null);
    }

    #[test]
    fn copy_proto_records_preserves_bytes_and_order() {
        let first = [0x00u8, 0xff, 0x01];
        let second = [0x7fu8];
        let ptrs = [first.as_ptr(), second.as_ptr()];
        let lens = [first.len(), second.len()];

        let records = unsafe { copy_proto_records(ptrs.as_ptr(), lens.as_ptr(), 2) }.unwrap();

        assert_eq!(records.len(), 2);
        assert!(matches!(&records[0], EncodedRecord::Proto(bytes) if bytes[..] == first));
        assert!(matches!(&records[1], EncodedRecord::Proto(bytes) if bytes[..] == second));

        // A count or length no slice can hold is rejected before any read
        // (the dangling pointers are never dereferenced).
        let (bytes, recs) = (
            ptr::NonNull::dangling().as_ptr(),
            ptr::NonNull::dangling().as_ptr(),
        );
        let too_long = isize::MAX as usize + 1;
        assert_eq!(
            unsafe { copy_bytes(bytes, too_long) },
            Err("Invalid data length")
        );
        let too_many = MAX_RECORDS + 1;
        let lens = ptr::NonNull::dangling().as_ptr();
        assert_eq!(
            unsafe { copy_proto_records(recs, lens, too_many) }.err(),
            Some("Invalid record count")
        );
        assert_eq!(
            unsafe { copy_json_records(ptr::NonNull::dangling().as_ptr(), too_many) }.err(),
            Some("Invalid record count")
        );
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

    /// A flush waits only for submissions from its own epoch or earlier: a
    /// submission accepted after the flush began does not hold it off.
    #[test]
    fn pending_ingests_wait_only_for_earlier_epochs() {
        let tracker = Arc::new(PendingIngests::default());
        let wait = |target| {
            RUNTIME.block_on(async {
                tokio::time::timeout(Duration::from_millis(200), tracker.wait_drained(target))
                    .await
                    .is_ok()
            })
        };
        let before = tracker.begin();
        let target = tracker.start_flush();
        let after = tracker.begin();
        assert!(!wait(target), "an earlier submission holds the flush");
        drop(before);
        assert!(wait(target), "a later submission must not hold the flush");
        drop(after);
        assert!(wait(tracker.start_flush()));

        // Two flushes waiting on one target both wake when it drains.
        let pending = tracker.begin();
        let target = tracker.start_flush();
        std::thread::scope(|scope| {
            let waiters: Vec<_> = (0..2)
                .map(|_| {
                    let tracker = Arc::clone(&tracker);
                    scope.spawn(move || {
                        RUNTIME.block_on(async {
                            tokio::time::timeout(
                                Duration::from_secs(2),
                                tracker.wait_drained(target),
                            )
                            .await
                            .is_ok()
                        })
                    })
                })
                .collect();
            // Let both waiters register before the target drains.
            std::thread::sleep(Duration::from_millis(100));
            drop(pending);
            for waiter in waiters {
                assert!(waiter.join().unwrap(), "every waiter on the target wakes");
            }
        });
    }

    type Report = (i64, bool, bool, String);

    /// `_async` ingest callback for the panic test: sends what it got to the
    /// `Sender` behind `user_data`.
    extern "C" fn send_report(id: i64, result: *const CResult, user_data: *mut c_void) {
        let tx = unsafe { &*(user_data as *const std::sync::mpsc::Sender<Report>) };
        let result = unsafe { &*result };
        // The FFI owns the message and frees it after the callback.
        let message = if result.error_message.is_null() {
            String::new()
        } else {
            unsafe { CStr::from_ptr(result.error_message) }
                .to_string_lossy()
                .into_owned()
        };
        let _ = tx.send((id, result.success, result.is_retryable, message));
    }

    /// A panic in background work (any async op, an async ingest) reaches
    /// its callback once as a non-retryable error, and releases what the
    /// work held. (A panicking async create is in the mock suite.)
    #[test]
    fn panics_in_background_work_are_reported_once() {
        const PANIC: &str = "Rust panic caught at FFI boundary";

        // Any async op: on_panic runs only for a panic, and once.
        let (tx, rx) = std::sync::mpsc::channel();
        let ok_tx = tx.clone();
        spawn_with_panic_report(async {}, move |result| {
            let _ = ok_tx.send(("normal work", result.success, String::new()));
        });
        spawn_with_panic_report(async { panic!("boom") }, move |mut result| {
            let (success, retryable, message) = take_full(&mut result);
            assert!(!retryable, "{message}");
            let _ = tx.send(("panicking work", success, message));
        });
        let reported = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the panic is reported");
        assert_eq!(
            reported,
            ("panicking work", false, format!("{PANIC}: boom"))
        );
        assert!(
            rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "on_panic runs only for a panic, and once"
        );

        // Async ingest: the callback gets INGEST_ERROR; the ticket and the
        // read guard are released, so flush and close don't hang.
        let (_runtime, sdk, mux) = live_mux();
        let (tx, rx) = std::sync::mpsc::channel::<Report>();
        let user_data = Box::leak(Box::new(tx)) as *mut _ as *mut c_void;
        let handle = validate_mux_ptr(mux).unwrap();
        spawn_ingest_async(handle, IngestPayload::PanicForTest, send_report, user_data);
        let (id, success, retryable, message) = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the panicking async ingest calls back");
        assert!(id == INGEST_ERROR && !success && !retryable, "{message}");
        assert!(message.starts_with(PANIC), "async ingest: {message}");
        assert!(
            rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "the callback runs once"
        );
        let (flushed, _, message) = blocking_within_2s(mux, flush, "flush after the panic");
        assert!(flushed, "{message}");
        let (closed, _, message) = blocking_within_2s(mux, close, "close after the panic");
        assert!(closed, "{message}");
        zerobus_multiplexed_stream_free(mux);
        crate::zerobus_sdk_free(sdk);
    }
}
