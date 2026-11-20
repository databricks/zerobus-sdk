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
use databricks_zerobus_ingest_sdk::{
    AckCallback, EncodedRecord, HeadersProvider, MessageId, MultiplexedStream, ZerobusError,
    ZerobusResult, ZerobusSdk,
};
use prost::Message;
use std::collections::{BTreeMap, HashMap};
use std::ffi::CString;
use std::os::raw::c_char;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify, OwnedRwLockReadGuard, RwLock};

/// Returned by ingest calls on failure; `result` carries the error.
const INGEST_ERROR: i64 = -1;
/// Returned by batch ingest calls when the batch is empty. Not a message ID.
const EMPTY_BATCH: i64 = -2;

/// Opaque handle for a multiplexed stream. Created by
/// `zerobus_sdk_create_multiplexed_stream[_with_headers_provider]` and
/// released by `zerobus_multiplexed_stream_free`. (Beta)
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
/// Background tasks clone `stream` and `nowait`, never the handle, so they
/// keep working after `zerobus_multiplexed_stream_free` drops it.
struct MuxHandle {
    /// `close` and `get_unacked_records` need `&mut MultiplexedStream`, while
    /// ingest, flush, and wait need only `&`, possibly from `_nowait` and
    /// `_async` tasks that hold their own owned read guard. The lock lets those
    /// share the mux and makes a close wait for them instead of aliasing a
    /// `&mut` with live `&` borrows.
    stream: Arc<RwLock<MultiplexedStream>>,
    /// `_nowait` and `_async` ingests not yet queued into the core, so flush
    /// can wait for them.
    pending: Arc<PendingIngests>,
}

/// Counts `_nowait` and `_async` ingests per flush epoch.
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
    /// First background `_nowait` ingest failure, by epoch, until a flush or
    /// close reports it.
    errors: BTreeMap<u64, ZerobusError>,
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
            error: None,
        }
    }

    /// Removes and returns the first recorded failure from epoch `up_to` or
    /// earlier (every epoch when `None`).
    fn take_error(&self, up_to: Option<u64>) -> Option<ZerobusError> {
        let mut epochs = self.epochs();
        let later = match up_to {
            Some(target) => epochs.errors.split_off(&(target + 1)),
            None => BTreeMap::new(),
        };
        std::mem::replace(&mut epochs.errors, later)
            .into_values()
            .next()
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

/// Marks one `_nowait` or `_async` submission as queued (or failed) when dropped.
struct PendingTicket {
    tracker: Arc<PendingIngests>,
    epoch: u64,
    /// Set when the background ingest failed; recorded for the next flush.
    error: Option<ZerobusError>,
}

impl Drop for PendingTicket {
    fn drop(&mut self) {
        let drained = {
            let mut epochs = self.tracker.epochs();
            // Record before the epoch can drain, so the flush waiting on it
            // sees the error.
            if let Some(error) = self.error.take() {
                epochs.errors.entry(self.epoch).or_insert(error);
            }
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
    } else {
        Ok(std::slice::from_raw_parts(data, len).to_vec())
    }
}

/// Copies a C array of protobuf payloads so the caller may free them on return.
unsafe fn copy_proto_records(
    records: *const *const u8,
    record_lens: *const usize,
    num_records: usize,
) -> Result<Vec<EncodedRecord>, &'static str> {
    let ptrs = std::slice::from_raw_parts(records, num_records);
    let lens = std::slice::from_raw_parts(record_lens, num_records);
    ptrs.iter()
        .zip(lens)
        .map(|(&data, &len)| copy_bytes(data, len).map(EncodedRecord::Proto))
        .collect()
}

/// Copies a C array of NUL-terminated JSON strings so the caller may free them
/// on return.
unsafe fn copy_json_records(
    json_records: *const *const c_char,
    num_records: usize,
) -> Result<Vec<EncodedRecord>, &'static str> {
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
/// gone. The C caller cannot observe that moment otherwise, since `_nowait`
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
/// `ack_on_ack` and `ack_on_error` (either may be NULL) receive message IDs.
/// Each sub-stream delivers its callbacks on its own background task, so
/// callbacks from different sub-streams may run concurrently and in no global
/// order; `ack_user_data` must tolerate concurrent access. `error_message` is
/// valid only during the call. `options->ack_on_ack` and
/// `options->ack_on_error` must be NULL: they carry ordered offsets for
/// ordinary streams and are rejected here.
///
/// Once this function is called, the FFI owns `ack_user_data` and invokes
/// `free_ack_user_data` (when set) exactly once, from any thread, after the
/// last sub-stream and background task releases the callbacks. That can be
/// after this function returns, even when it fails (a sub-stream that
/// started to open is torn down in the background), and after
/// `zerobus_multiplexed_stream_free` returns. Never free `ack_user_data`
/// yourself.
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
/// freeing an open stream abandons them. Background `_nowait` tasks keep the
/// stream alive only until they have queued their records; those records are
/// then abandoned too. Pending `_async` operations keep the stream alive and
/// still call back exactly once. Must not run concurrently with any other call
/// on `mux` (it may be called from an `_async` completion callback), and `mux`
/// must not be used afterwards.
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
        if data.is_null() {
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
/// Returns -1 on error. For an empty batch it returns -2 with
/// `result->success` set; -2 is not a message ID, so do not wait on it.
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
        if records.is_null() || record_lens.is_null() {
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
        if json_records.is_null() {
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

/// Like `read_now`, and also rejects a mux that is already closed, since a
/// `_nowait` call has no other way to report it.
fn lock_for_nowait(handle: &MuxHandle) -> ZerobusResult<OwnedRwLockReadGuard<MultiplexedStream>> {
    let guard = read_now(handle)?;
    if guard.is_closed() {
        return Err(ZerobusError::InvalidStateError(
            "MultiplexedStream is closed".to_string(),
        ));
    }
    Ok(guard)
}

/// Queues an ingest on a background task that holds the mux's read lock and
/// a flush ticket, both released once the core has queued (or rejected) it.
fn spawn_ingest(handle: &MuxHandle, payload: IngestPayload) -> ZerobusResult<()> {
    let guard = lock_for_nowait(handle)?;
    let ticket = handle.pending.begin();
    RUNTIME.spawn(async move {
        let mut ticket = ticket;
        // Stays set if the ingest panics, so the dropped record is reported.
        ticket.error = Some(ZerobusError::InvalidStateError(
            "a background _nowait ingest panicked; its record was dropped".to_string(),
        ));
        ticket.error = payload.ingest(&guard).await.err();
        // The ticket drops before the guard, so close sees the error too.
        drop(ticket);
    });
    Ok(())
}

/// Ingest a protobuf record without waiting for it to be queued
/// (fire-and-forget) (Beta).
///
/// Copies the record and returns immediately. `result` reports argument
/// validation errors and a closed or closing mux (non-retryable; re-send the
/// record on a new stream). Records submitted this way have no ordering
/// guarantee relative to each other.
///
/// A record accepted here is queued on a background task.
/// `zerobus_multiplexed_stream_flush`, `zerobus_multiplexed_stream_close` and
/// `zerobus_multiplexed_stream_get_unacked_records` first wait for every
/// record accepted before they were called to be queued, so they cover it
/// like a blocking ingest. If the background ingest itself fails (for example,
/// the chosen sub-stream stays at capacity for 30 seconds, or fails while the
/// task waits), the record is dropped and is not returned by
/// `zerobus_multiplexed_stream_get_unacked_records`. Instead, the first such
/// error (with its `is_retryable`) is returned by the next flush or close that
/// covers it and has no error of its own; that call clears it, so it is
/// reported once. Such an error means a record was dropped: retrying the flush
/// does not bring it back. After a sub-stream failure, flush and close return
/// that failure instead, so a dropped `_nowait` record can be missing from
/// `zerobus_multiplexed_stream_get_unacked_records` with no separate error.
/// Use the blocking or `_async` ingest calls when every record must be
/// recoverable.
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_ingest_proto_record_nowait(
    mux: *mut CZerobusMultiplexedStream,
    data: *const u8,
    data_len: usize,
    result: *mut CResult,
) {
    ffi_guard(result, (), move || {
        if data.is_null() {
            write_error_result(result, "Invalid data pointer", false);
            return;
        }
        let handle = match validate_mux_ptr(mux) {
            Ok(h) => h,
            Err(msg) => {
                write_error_result(result, msg, false);
                return;
            }
        };

        let record = match unsafe { copy_bytes(data, data_len) } {
            Ok(bytes) => EncodedRecord::Proto(bytes),
            Err(e) => {
                write_error_result(result, e, false);
                return;
            }
        };
        let res = spawn_ingest(handle, IngestPayload::Record(record));
        complete(result, res, ())
    })
}

/// Ingest a JSON record without waiting for it to be queued (fire-and-forget)
/// (Beta).
///
/// Same contract as `zerobus_multiplexed_stream_ingest_proto_record_nowait`.
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_ingest_json_record_nowait(
    mux: *mut CZerobusMultiplexedStream,
    json_data: *const c_char,
    result: *mut CResult,
) {
    ffi_guard(result, (), move || {
        let handle = match validate_mux_ptr(mux) {
            Ok(h) => h,
            Err(msg) => {
                write_error_result(result, msg, false);
                return;
            }
        };
        let record = match unsafe { c_str_to_string(json_data) } {
            Ok(json) => EncodedRecord::Json(json),
            Err(e) => {
                write_error_result(result, e, false);
                return;
            }
        };

        let res = spawn_ingest(handle, IngestPayload::Record(record));
        complete(result, res, ())
    })
}

/// Ingest a batch of protobuf records without waiting (fire-and-forget) (Beta).
///
/// Copies every record before returning, so the caller may release its buffers
/// immediately. Same error contract as
/// `zerobus_multiplexed_stream_ingest_proto_record_nowait`.
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_ingest_proto_records_nowait(
    mux: *mut CZerobusMultiplexedStream,
    records: *const *const u8,
    record_lens: *const usize,
    num_records: usize,
    result: *mut CResult,
) {
    ffi_guard(result, (), move || {
        if records.is_null() || record_lens.is_null() {
            write_error_result(result, "Invalid records pointer", false);
            return;
        }
        let handle = match validate_mux_ptr(mux) {
            Ok(h) => h,
            Err(msg) => {
                write_error_result(result, msg, false);
                return;
            }
        };
        if num_records > 0 {
            let batch = match unsafe { copy_proto_records(records, record_lens, num_records) } {
                Ok(batch) => batch,
                Err(e) => {
                    write_error_result(result, e, false);
                    return;
                }
            };
            let res = spawn_ingest(handle, IngestPayload::Batch(batch));
            complete(result, res, ())
        } else {
            // Nothing to queue, but report a closed mux like the blocking batch.
            let res = lock_for_nowait(handle).map(drop);
            complete(result, res, ())
        }
    })
}

/// Ingest a batch of JSON records without waiting (fire-and-forget) (Beta).
///
/// Same contract as `zerobus_multiplexed_stream_ingest_proto_records_nowait`.
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_ingest_json_records_nowait(
    mux: *mut CZerobusMultiplexedStream,
    json_records: *const *const c_char,
    num_records: usize,
    result: *mut CResult,
) {
    ffi_guard(result, (), move || {
        if json_records.is_null() {
            write_error_result(result, "Invalid records pointer", false);
            return;
        }
        let handle = match validate_mux_ptr(mux) {
            Ok(h) => h,
            Err(msg) => {
                write_error_result(result, msg, false);
                return;
            }
        };
        if num_records > 0 {
            let batch = match unsafe { copy_json_records(json_records, num_records) } {
                Ok(batch) => batch,
                Err(e) => {
                    write_error_result(result, e, false);
                    return;
                }
            };
            let res = spawn_ingest(handle, IngestPayload::Batch(batch));
            complete(result, res, ())
        } else {
            // Nothing to queue, but report a closed mux like the blocking batch.
            let res = lock_for_nowait(handle).map(drop);
            complete(result, res, ())
        }
    })
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

/// Wait until every record queued on every sub-stream is acknowledged (Beta).
///
/// Covers blocking ingests that have returned and every `_nowait` and `_async`
/// ingest accepted before this call: it first waits for their background tasks
/// to queue them, then for their acknowledgments. The tasks wait concurrently,
/// so with a sub-stream at capacity this phase can take up to 30 seconds after
/// the last accepted call, on top of `flush_timeout_ms`. `_nowait` records
/// submitted during the flush may not be covered; the next flush or close
/// covers them. If the flush itself fails, that error is returned and a stored
/// `_nowait` failure is kept for the next flush or close; otherwise, if a
/// covered `_nowait` ingest failed in the background, the first such failure is
/// returned and cleared, so it is reported once. That error means a record was
/// dropped, and retrying the flush does not bring it back.
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
            handle.pending.wait_drained(target).await;
            let flushed = handle.stream.read().await.flush().await;
            // The flush's own error wins and leaves a stored `_nowait` failure
            // for the next flush or close; only the call that returns it
            // clears it.
            flushed.and_then(|()| handle.pending.take_error(Some(target)).map_or(Ok(()), Err))
        });
        complete(result, res.map(|()| true), false)
    })
}

/// Flush and close every sub-stream (Beta).
///
/// Waits for in-progress ingest, flush, and wait calls on this mux to return,
/// and for every accepted `_nowait` and `_async` ingest to be queued, then
/// flushes those too. `_nowait` calls made after close starts report an error.
/// If close itself succeeds, a background `_nowait` failure that no flush has
/// reported yet is returned instead (and cleared). On error, call
/// `zerobus_multiplexed_stream_get_unacked_records` to recover unacknowledged
/// records; `_nowait` records dropped by a failed sub-stream are not among them
/// (see `zerobus_multiplexed_stream_ingest_proto_record_nowait`). The handle
/// must still be released with `zerobus_multiplexed_stream_free`.
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
        let res = RUNTIME.block_on(async {
            let closed = handle.stream.write().await.close().await;
            closed.and_then(|()| handle.pending.take_error(None).map_or(Ok(()), Err))
        });
        complete(result, res.map(|()| true), false)
    })
}

/// Returns whether the mux is closed, either explicitly or because an ingest,
/// flush, or wait call on it observed a sub-stream failure. A failure that no
/// call has observed yet does not set it. Also returns true while a close is
/// in progress and for a null handle (Beta).
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
/// failure. `result` is valid only for the duration of the call.
///
/// Every multiplexed `_async` function follows the same contract. It validates
/// its arguments and returns at once: false with `result` set only on an
/// invalid argument, in which case its callback never runs; otherwise true, and
/// every other error, including a closed mux, reaches the callback. Async
/// ingests report a close in progress as an error; async wait and flush wait
/// for it, as the blocking calls do.
/// The callback then runs exactly once, on a runtime thread. It must be
/// thread-safe, must not unwind, and must not call the blocking
/// `zerobus_multiplexed_stream_*` functions (calling `_async` ones is fine).
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
/// null `mux`.
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
        write_success_result(result);
        true
    })
}

/// Create a multiplexed stream with a custom headers provider on a background
/// task (Beta).
///
/// Same arguments and ownership of `user_data` and `ack_user_data` as
/// `zerobus_sdk_create_multiplexed_stream_with_headers_provider`; otherwise
/// behaves like `zerobus_sdk_create_multiplexed_stream_async`.
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
        write_success_result(result);
        true
    })
}

/// Queues an ingest on a background task and reports its message ID.
///
/// Like `_nowait`, the read guard and a flush ticket are taken before the call
/// returns, so flush, close and `get_unacked_records` cover the submission.
/// Both are released before `callback` runs.
fn spawn_ingest_async(
    handle: &MuxHandle,
    payload: IngestPayload,
    callback: OffsetAsyncCallback,
    user_data: *mut std::ffi::c_void,
) -> ZerobusResult<()> {
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
            return Ok(());
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
    Ok(())
}

/// Ingest a protobuf record on a background task and report its message ID
/// via `callback` (Beta).
///
/// Copies the record before returning. `callback` receives the message ID, or
/// -1 with the error, exactly as `zerobus_multiplexed_stream_ingest_proto_record`
/// would return it; a failed ingest queued nothing. Records accepted before a
/// call to flush, close or `get_unacked_records` (blocking or async) are
/// queued before it proceeds, so it covers them. A closed or closing mux is
/// reported through `callback`.
///
/// Follows the multiplexed `_async` contract described on
/// `CreateMultiplexedStreamAsyncCallback`.
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_ingest_proto_record_async(
    mux: *mut CZerobusMultiplexedStream,
    data: *const u8,
    data_len: usize,
    callback: OffsetAsyncCallback,
    user_data: *mut std::ffi::c_void,
    result: *mut CResult,
) -> bool {
    ffi_guard(result, false, move || {
        if data.is_null() {
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
        let res = spawn_ingest_async(handle, IngestPayload::Record(record), callback, user_data);
        complete(result, res.map(|()| true), false)
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
    callback: OffsetAsyncCallback,
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
        let res = spawn_ingest_async(handle, IngestPayload::Record(record), callback, user_data);
        complete(result, res.map(|()| true), false)
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
    callback: OffsetAsyncCallback,
    user_data: *mut std::ffi::c_void,
    result: *mut CResult,
) -> bool {
    ffi_guard(result, false, move || {
        if records.is_null() || record_lens.is_null() {
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
        let res = spawn_ingest_async(handle, IngestPayload::Batch(batch), callback, user_data);
        complete(result, res.map(|()| true), false)
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
    callback: OffsetAsyncCallback,
    user_data: *mut std::ffi::c_void,
    result: *mut CResult,
) -> bool {
    ffi_guard(result, false, move || {
        if json_records.is_null() {
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
        let res = spawn_ingest_async(handle, IngestPayload::Batch(batch), callback, user_data);
        complete(result, res.map(|()| true), false)
    })
}

/// Wait for a message ID on a background task and report completion via
/// `callback` (Beta).
///
/// `callback` receives what `zerobus_multiplexed_stream_wait_for_message_id`
/// would return. Like it, the wait starts after a close in progress
/// finishes, and then reports the result on the closed mux.
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
        write_success_result(result);
        true
    })
}

/// Flush on a background task and report completion via `callback` (Beta).
///
/// Covers the same records as `zerobus_multiplexed_stream_flush`: every
/// blocking, `_nowait` and `_async` ingest accepted before this call. Like
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
        let user_data = SendPtr::new(user_data);
        spawn_with_panic_report(
            async move {
                pending.wait_drained(target).await;
                // Queues behind a close in progress, as the blocking flush does.
                let flushed = stream.read_owned().await.flush().await;
                // As in the blocking flush: its own error wins and leaves a
                // stored `_nowait` failure for the next flush or close.
                let res =
                    flushed.and_then(|()| pending.take_error(Some(target)).map_or(Ok(()), Err));
                yield_before_callback().await;
                let (value, result) = match res {
                    Ok(()) => (true, CResult::success()),
                    Err(err) => (false, CResult::error(err)),
                };
                invoke_bool_async_callback(callback, value, result, user_data.get());
            },
            move |result| invoke_bool_async_callback(callback, false, result, user_data.get()),
        );
        write_success_result(result);
        true
    })
}

/// Flush and close every sub-stream on a background task and report
/// completion via `callback` (Beta).
///
/// Behaves like `zerobus_multiplexed_stream_close`, waiting for in-flight calls
/// and accepted `_nowait` and `_async` ingests first. Calls made after this
/// returns but before its background task takes the write lock are still
/// accepted, and covered by the close. The handle must still be released with
/// `zerobus_multiplexed_stream_free`, which may happen before `callback` runs.
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
        let pending = Arc::clone(&handle.pending);
        let user_data = SendPtr::new(user_data);
        spawn_with_panic_report(
            async move {
                let closed = stream.write_owned().await.close().await;
                let res = closed.and_then(|()| pending.take_error(None).map_or(Ok(()), Err));
                yield_before_callback().await;
                let (value, result) = match res {
                    Ok(()) => (true, CResult::success()),
                    Err(err) => (false, CResult::error(err)),
                };
                invoke_bool_async_callback(callback, value, result, user_data.get());
            },
            move |result| invoke_bool_async_callback(callback, false, result, user_data.get()),
        );
        write_success_result(result);
        true
    })
}

/// Close the mux on a background task and report every unacknowledged record
/// via `callback` (Beta).
///
/// Behaves like `zerobus_multiplexed_stream_get_unacked_records`. On success
/// the callback owns the array and frees it with `zerobus_free_record_array`.
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
        spawn_with_panic_report(
            async move {
                let res = async {
                    let mut mux = stream.write_owned().await;
                    Ok(mux.get_unacked_records().await?.collect::<Vec<_>>())
                }
                .await
                .and_then(encoded_records_to_c_array);
                let (records, result) = match res {
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
        write_success_result(result);
        true
    })
}

#[cfg(test)]
mod multiplexed_tests {
    use super::*;
    use crate::zerobus_free_error_message;
    use std::ffi::{c_void, CStr, CString};
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
    use std::time::Duration;

    // Short names for the exports, so each table row fits on one line.
    use super::{
        zerobus_multiplexed_stream_close as close,
        zerobus_multiplexed_stream_close_async as close_a,
        zerobus_multiplexed_stream_flush as flush,
        zerobus_multiplexed_stream_flush_async as flush_a,
        zerobus_multiplexed_stream_get_unacked_records as unacked,
        zerobus_multiplexed_stream_get_unacked_records_async as unacked_a,
        zerobus_multiplexed_stream_ingest_json_record as json1,
        zerobus_multiplexed_stream_ingest_json_record_async as json1_a,
        zerobus_multiplexed_stream_ingest_json_record_nowait as json1_nw,
        zerobus_multiplexed_stream_ingest_json_records as jsons,
        zerobus_multiplexed_stream_ingest_json_records_async as jsons_a,
        zerobus_multiplexed_stream_ingest_json_records_nowait as jsons_nw,
        zerobus_multiplexed_stream_ingest_proto_record as proto1,
        zerobus_multiplexed_stream_ingest_proto_record_async as proto1_a,
        zerobus_multiplexed_stream_ingest_proto_record_nowait as proto1_nw,
        zerobus_multiplexed_stream_ingest_proto_records as protos,
        zerobus_multiplexed_stream_ingest_proto_records_async as protos_a,
        zerobus_multiplexed_stream_ingest_proto_records_nowait as protos_nw,
        zerobus_multiplexed_stream_wait_for_message_id as wait,
        zerobus_multiplexed_stream_wait_for_message_id_async as wait_a,
    };

    const NULL_HANDLE: &str = "Multiplexed stream pointer is null";
    const BAD_DATA: &str = "Invalid data pointer";
    const BAD_RECORDS: &str = "Invalid records pointer";
    const OPT_ACK: &str =
        "options->ack_on_ack and options->ack_on_error are not supported for multiplexed streams";
    const STREAM_COUNT: &str = "stream_count must be between 1 and 64";
    const INFLIGHT: &str = "must be at least stream_count";
    /// How long a create may take to report a connect failure to
    /// `127.0.0.1:1`. On Windows a closed local port is not refused at once
    /// (each SYN is retried), and the OAuth path connects more than once.
    const CONNECT_FAILURE_WAIT: Duration = Duration::from_secs(60);

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

    fn sdk_at(url: &str) -> *mut CZerobusSdk {
        let endpoint = CString::new(url).unwrap();
        let builder = crate::zerobus_sdk_builder_new();
        crate::zerobus_sdk_builder_endpoint(builder, endpoint.as_ptr());
        crate::zerobus_sdk_builder_unity_catalog_url(builder, endpoint.as_ptr());
        crate::zerobus_sdk_builder_disable_tls(builder);
        let mut result = unwritten_result();
        let sdk = crate::zerobus_sdk_builder_build(builder, &mut result);
        assert!(result.success && !sdk.is_null());
        sdk
    }

    /// An SDK at 127.0.0.1:1: every connect fails (retryable).
    fn test_sdk() -> *mut CZerobusSdk {
        sdk_at("http://127.0.0.1:1")
    }

    fn json_options() -> CStreamConfigurationOptions {
        let mut options = crate::zerobus_get_default_config();
        options.record_type = 2;
        options
    }

    extern "C" fn empty_headers(_user_data: *mut c_void) -> CHeaders {
        CHeaders {
            headers: ptr::null_mut(),
            count: 0,
            error_message: ptr::null_mut(),
        }
    }

    fn wait_until_within(what: &str, timeout: Duration, condition: impl Fn() -> bool) {
        let deadline = std::time::Instant::now() + timeout;
        while !condition() {
            assert!(std::time::Instant::now() < deadline, "timed out: {what}");
            std::thread::sleep(Duration::from_millis(10));
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

    /// Counts frees of the `user_data` it is passed as. Leaked: the FFI may
    /// hold it until it calls the free callback.
    #[derive(Default)]
    struct FreeCount(AtomicUsize);

    impl FreeCount {
        fn leaked() -> &'static FreeCount {
            Box::leak(Box::default())
        }

        fn user_data(&'static self) -> *mut c_void {
            self as *const FreeCount as *mut c_void
        }

        fn assert_freed_once(&self, what: &str) {
            wait_until_within(what, Duration::from_secs(5), || {
                self.0.load(Ordering::SeqCst) >= 1
            });
            // Let a late second release land before asserting exactly once.
            std::thread::sleep(Duration::from_millis(100));
            assert_eq!(
                self.0.load(Ordering::SeqCst),
                1,
                "{what}: freed more than once"
            );
        }
    }

    extern "C" fn count_free(user_data: *mut c_void) {
        unsafe { &*(user_data as *const FreeCount) }
            .0
            .fetch_add(1, Ordering::SeqCst);
    }

    extern "C" fn noop_mux_ack(_id: i64, _user_data: *mut c_void) {}
    extern "C" fn noop_mux_error(_id: i64, _msg: *const c_char, _user_data: *mut c_void) {}

    /// One reported `_async` completion: `(value, success, retryable, message, mux)`.
    type Completion = (i64, bool, bool, String, usize);

    /// `user_data` for `_async` callbacks; records every completion.
    #[derive(Default)]
    struct AsyncSink(std::sync::Mutex<Vec<Completion>>);

    impl AsyncSink {
        fn leaked() -> &'static AsyncSink {
            Box::leak(Box::default())
        }

        fn user_data(&'static self) -> *mut c_void {
            self as *const AsyncSink as *mut c_void
        }

        fn count(&self) -> usize {
            self.0.lock().unwrap().len()
        }

        fn record(user_data: *mut c_void, value: i64, result: *const CResult, mux: usize) {
            let sink = unsafe { &*(user_data as *const AsyncSink) };
            let result = unsafe { &*result };
            let message = if result.error_message.is_null() {
                String::new()
            } else {
                unsafe { CStr::from_ptr(result.error_message) }
                    .to_string_lossy()
                    .into_owned()
            };
            let completion = (value, result.success, result.is_retryable, message, mux);
            sink.0.lock().unwrap().push(completion);
        }

        /// Waits up to `timeout` for the callback, then checks it ran once.
        fn one_within(&self, what: &str, timeout: Duration) -> Completion {
            wait_until_within(what, timeout, || self.count() >= 1);
            // Let a late second callback land before asserting exactly once.
            std::thread::sleep(Duration::from_millis(100));
            let completions = self.0.lock().unwrap();
            assert_eq!(completions.len(), 1, "{what}: callback ran more than once");
            completions[0].clone()
        }

        fn one(&self, what: &str) -> Completion {
            self.one_within(what, Duration::from_secs(5))
        }

        fn assert_never_called(&self, what: &str) {
            // Let a wrongly spawned callback land.
            std::thread::sleep(Duration::from_millis(200));
            assert_eq!(self.count(), 0, "{what}: callback must not run");
        }
    }

    extern "C" fn sink_create(
        mux: *mut CZerobusMultiplexedStream,
        result: *const CResult,
        user_data: *mut c_void,
    ) {
        AsyncSink::record(user_data, 0, result, mux as usize);
    }

    extern "C" fn sink_offset(id: i64, result: *const CResult, user_data: *mut c_void) {
        AsyncSink::record(user_data, id, result, 0);
    }

    extern "C" fn sink_bool(value: bool, result: *const CResult, user_data: *mut c_void) {
        AsyncSink::record(user_data, value as i64, result, 0);
    }

    extern "C" fn sink_records(
        records: CRecordArray,
        result: *const CResult,
        user_data: *mut c_void,
    ) {
        AsyncSink::record(user_data, records.len as i64, result, 0);
        crate::zerobus_free_record_array(records);
    }

    /// The sync part of an `_async` call: `(returned, success, retryable, message)`.
    fn sync_part(returned: bool, result: &mut CResult) -> (bool, bool, bool, String) {
        let (success, retryable, message) = take_full(result);
        (returned, success, retryable, message)
    }

    fn c_str(value: &Option<CString>) -> *const c_char {
        value.as_ref().map_or(ptr::null(), |s| s.as_ptr())
    }

    /// One create call; `None` strings are passed as NULL. Every create
    /// passes both mux ack callbacks and a counted `ack_user_data`.
    struct CreateCall {
        sdk: *mut CZerobusSdk,
        table: Option<CString>,
        descriptor: Vec<u8>,
        client_id: Option<CString>,
        client_secret: Option<CString>,
        stream_count: usize,
        options: CStreamConfigurationOptions,
    }

    impl CreateCall {
        /// Valid arguments for a 2-lane JSON mux on `sdk`, changed by `edit`.
        fn with(sdk: *mut CZerobusSdk, edit: impl FnOnce(&mut Self)) -> Self {
            let mut options = json_options();
            // Fail on the first connect error instead of retrying it.
            options.recovery = false;
            let mut call = Self {
                sdk,
                table: Some(CString::new("main.default.events").unwrap()),
                descriptor: Vec::new(),
                client_id: Some(CString::new("client-id").unwrap()),
                client_secret: Some(CString::new("client-secret").unwrap()),
                stream_count: 2,
                options,
            };
            edit(&mut call);
            call
        }

        fn valid(sdk: *mut CZerobusSdk) -> Self {
            Self::with(sdk, |_| {})
        }

        fn oauth_raw(
            &self,
            ack: Option<&'static FreeCount>,
            result: *mut CResult,
        ) -> *mut CZerobusMultiplexedStream {
            zerobus_sdk_create_multiplexed_stream(
                self.sdk,
                c_str(&self.table),
                self.descriptor.as_ptr(),
                self.descriptor.len(),
                c_str(&self.client_id),
                c_str(&self.client_secret),
                self.stream_count,
                &self.options,
                Some(noop_mux_ack),
                Some(noop_mux_error),
                ack.map_or(ptr::null_mut(), |a| a.user_data()),
                ack.map(|_| count_free as MuxAckFree),
                result,
            )
        }

        fn headers_raw(
            &self,
            headers: &'static FreeCount,
            ack: &'static FreeCount,
            result: *mut CResult,
        ) -> *mut CZerobusMultiplexedStream {
            zerobus_sdk_create_multiplexed_stream_with_headers_provider(
                self.sdk,
                c_str(&self.table),
                self.descriptor.as_ptr(),
                self.descriptor.len(),
                empty_headers,
                headers.user_data(),
                Some(count_free),
                self.stream_count,
                &self.options,
                Some(noop_mux_ack),
                Some(noop_mux_error),
                ack.user_data(),
                Some(count_free),
                result,
            )
        }

        /// The sync OAuth create, expecting failure: `(retryable, message)`.
        fn oauth(&self, ack: Option<&'static FreeCount>) -> (bool, String) {
            let mut result = unwritten_result();
            let mux = self.oauth_raw(ack, &mut result);
            Self::failure(mux, &mut result)
        }

        /// The sync headers create, expecting failure: `(retryable, message)`.
        fn headers(&self, headers: &'static FreeCount, ack: &'static FreeCount) -> (bool, String) {
            let mut result = unwritten_result();
            let mux = self.headers_raw(headers, ack, &mut result);
            Self::failure(mux, &mut result)
        }

        fn failure(mux: *mut CZerobusMultiplexedStream, result: &mut CResult) -> (bool, String) {
            assert!(mux.is_null(), "create unexpectedly returned a handle");
            let (success, retryable, message) = take_full(result);
            assert!(!success, "create reported success");
            (retryable, message)
        }

        fn oauth_async(
            &self,
            ack: &'static FreeCount,
            sink: &'static AsyncSink,
        ) -> (bool, bool, bool, String) {
            let mut result = unwritten_result();
            let returned = zerobus_sdk_create_multiplexed_stream_async(
                self.sdk,
                c_str(&self.table),
                self.descriptor.as_ptr(),
                self.descriptor.len(),
                c_str(&self.client_id),
                c_str(&self.client_secret),
                self.stream_count,
                &self.options,
                Some(noop_mux_ack),
                Some(noop_mux_error),
                ack.user_data(),
                Some(count_free),
                sink_create,
                sink.user_data(),
                &mut result,
            );
            sync_part(returned, &mut result)
        }

        fn headers_async(
            &self,
            headers: &'static FreeCount,
            ack: &'static FreeCount,
            sink: &'static AsyncSink,
        ) -> (bool, bool, bool, String) {
            let mut result = unwritten_result();
            let returned = zerobus_sdk_create_multiplexed_stream_with_headers_provider_async(
                self.sdk,
                c_str(&self.table),
                self.descriptor.as_ptr(),
                self.descriptor.len(),
                empty_headers,
                headers.user_data(),
                Some(count_free),
                self.stream_count,
                &self.options,
                Some(noop_mux_ack),
                Some(noop_mux_error),
                ack.user_data(),
                Some(count_free),
                sink_create,
                sink.user_data(),
                &mut result,
            );
            sync_part(returned, &mut result)
        }
    }

    /// Argument rows the FFI rejects before any I/O: `(case, call, OAuth only)`.
    fn create_rows(sdk: *mut CZerobusSdk) -> Vec<(&'static str, CreateCall, bool)> {
        let with = |edit: fn(&mut CreateCall)| CreateCall::with(sdk, edit);
        vec![
            ("sdk=NULL", with(|c| c.sdk = ptr::null_mut()), false),
            ("table_name=NULL", with(|c| c.table = None), false),
            ("client_id=NULL", with(|c| c.client_id = None), true),
            ("client_secret=NULL", with(|c| c.client_secret = None), true),
            // A truncated varint: not a decodable DescriptorProto.
            (
                "bad descriptor",
                with(|c| c.descriptor = vec![0xff; 3]),
                false,
            ),
        ]
    }

    /// The FFI-origin message expected for a `create_rows` case, if any.
    fn create_row_message(case: &str) -> Option<&'static str> {
        match case {
            "sdk=NULL" => Some("SDK pointer is null"),
            "bad descriptor" => None,
            _ => Some("Null pointer passed"),
        }
    }

    /// Rows for failures past the argument checks, shared by the sync and
    /// async create tests: `(case, call, retryable, message it contains)`.
    /// Core-origin wording is only checked with "contains the core text".
    fn config_rows(
        sdk: *mut CZerobusSdk,
    ) -> Vec<(&'static str, CreateCall, bool, Option<&'static str>)> {
        let with = |edit: fn(&mut CreateCall)| CreateCall::with(sdk, edit);
        let ack_on_ack = with(|c| c.options.ack_on_ack = Some(noop_mux_ack));
        let ack_on_error = with(|c| c.options.ack_on_error = Some(noop_mux_error));
        let low_inflight = with(|c| {
            c.stream_count = 3;
            c.options.max_inflight_requests = 2;
        });
        let zero_lanes = with(|c| c.stream_count = 0);
        vec![
            ("options->ack_on_ack", ack_on_ack, false, Some(OPT_ACK)),
            ("options->ack_on_error", ack_on_error, false, Some(OPT_ACK)),
            ("stream_count=0", zero_lanes, false, Some(STREAM_COUNT)),
            (
                "inflight < stream_count",
                low_inflight,
                false,
                Some(INFLIGHT),
            ),
            // `test_sdk()` points at 127.0.0.1:1: the one slow row.
            ("connect failure", CreateCall::valid(sdk), true, None),
        ]
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
        let call = CreateCall::with(sdk_at(&url), |c| {
            c.table = Some(CString::new("ffi.mux.panic").unwrap());
            c.stream_count = 1;
            // Recovery on: with it off, close can report the lane's EOF.
            c.options = json_options();
        });
        let mut result = unwritten_result();
        let mux = call.headers_raw(FreeCount::leaked(), FreeCount::leaked(), &mut result);
        let (success, _, message) = take_full(&mut result);
        assert!(success && !mux.is_null(), "{message}");
        (runtime, call.sdk, mux)
    }

    /// Which argument a C-argument row passes as NULL (the mux is always
    /// NULL, so a pointer-argument error proves it is checked first).
    #[derive(Debug, Clone, Copy, PartialEq)]
    enum Null {
        Mux,
        Data,
        Lens,
    }

    /// Inputs for the C-argument rows.
    struct Args {
        data: [u8; 1],
        lens: [usize; 1],
        json: CString,
    }

    impl Args {
        fn new() -> Self {
            Self {
                data: [1],
                lens: [1],
                json: CString::new("{}").unwrap(),
            }
        }

        /// `(data, lens)` with `null` applied.
        fn data_lens(&self, null: Null) -> (*const u8, *const usize) {
            let data = if null == Null::Data {
                ptr::null()
            } else {
                self.data.as_ptr()
            };
            let lens = if null == Null::Lens {
                ptr::null()
            } else {
                self.lens.as_ptr()
            };
            (data, lens)
        }
    }

    /// `ptr` as a one-element array pointer, or NULL for `Null::Data`.
    fn array_or_null<T>(array: &[*const T; 1], null: Null) -> *const *const T {
        if null == Null::Data {
            ptr::null()
        } else {
            array.as_ptr()
        }
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
        // _nowait group (pending decision).
        ProtoNowait,
        JsonNowait,
        ProtosNowait,
        JsonsNowait,
    }

    /// Every blocking export checks its C arguments: pointer arguments
    /// first, then the handle, each with a pinned, non-retryable error, and
    /// the return value reports the failure too.
    #[test]
    fn c_arguments_are_checked_before_the_handle() {
        use Blocking::*;
        let args = Args::new();
        let null = ptr::null_mut();
        let (data_array, json_array) = ([args.data.as_ptr()], [args.json.as_ptr()]);
        // Whether the return value reports the failure. A `_nowait` call
        // reports only through `result`, so its check always holds.
        let run = |export: Blocking, arg: Null, r: &mut CResult| -> bool {
            let (data, lens) = args.data_lens(arg);
            let (recs, jrecs) = (
                array_or_null(&data_array, arg),
                array_or_null(&json_array, arg),
            );
            let json = args.json.as_ptr();
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
                ProtoNowait => (proto1_nw(null, data, 1, r), true).1,
                JsonNowait => (json1_nw(null, json, r), true).1,
                ProtosNowait => (protos_nw(null, recs, lens, 1, r), true).1,
                JsonsNowait => (jsons_nw(null, jrecs, 1, r), true).1,
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
            // _nowait group (pending decision).
            (ProtoNowait, Null::Mux, NULL_HANDLE),
            (ProtoNowait, Null::Data, BAD_DATA),
            (JsonNowait, Null::Mux, NULL_HANDLE),
            (ProtosNowait, Null::Mux, NULL_HANDLE),
            (ProtosNowait, Null::Data, BAD_RECORDS),
            (ProtosNowait, Null::Lens, BAD_RECORDS),
            (JsonsNowait, Null::Mux, NULL_HANDLE),
            (JsonsNowait, Null::Data, BAD_RECORDS),
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
    }

    /// Every failing sync create reports the error (retryable only for a
    /// connect failure) and frees the headers and ack `user_data` once.
    #[test]
    fn sync_create_failures_report_and_free_once() {
        let sdk = test_sdk();
        let mut rows: Vec<_> = create_rows(sdk)
            .into_iter()
            .map(|(case, call, oauth_only)| {
                (case, call, oauth_only, false, create_row_message(case))
            })
            .collect();
        rows.extend(
            config_rows(sdk)
                .into_iter()
                .map(|(case, call, retryable, expected)| (case, call, false, retryable, expected)),
        );

        for (case, call, oauth_only, retryable, expected) in &rows {
            let check = |what: String, (got_retryable, message): (bool, String)| {
                assert_eq!(got_retryable, *retryable, "{what}: {message}");
                if let Some(expected) = expected {
                    assert!(message.contains(expected), "{what}: {message}");
                }
            };
            let ack = FreeCount::leaked();
            check(format!("OAuth {case}"), call.oauth(Some(ack)));
            ack.assert_freed_once(&format!("OAuth {case} ack user_data"));
            if !oauth_only {
                let (headers, ack) = (FreeCount::leaked(), FreeCount::leaked());
                check(format!("headers {case}"), call.headers(headers, ack));
                headers.assert_freed_once(&format!("headers {case} user_data"));
                ack.assert_freed_once(&format!("headers {case} ack user_data"));
            }
        }

        // A NULL `free_ack_user_data` is allowed.
        CreateCall::with(sdk, |c| c.table = None).oauth(None);
        // A NULL `result` is allowed; the NULL handle reports the failure.
        let call = CreateCall::with(sdk, |c| c.stream_count = 0);
        assert!(call.oauth_raw(None, ptr::null_mut()).is_null());
        crate::zerobus_sdk_free(sdk);
    }

    /// Bad arguments make every `_async` call return false at once; the
    /// callback never runs, and an async create frees its `user_data` once.
    #[test]
    fn async_calls_fail_fast_on_invalid_arguments() {
        let sdk = test_sdk();
        for (case, call, oauth_only) in &create_rows(sdk) {
            let (ack, sink) = (FreeCount::leaked(), AsyncSink::leaked());
            let (returned, success, retryable, message) = call.oauth_async(ack, sink);
            assert!(
                !returned && !success && !retryable,
                "OAuth async {case}: {message}"
            );
            if let Some(expected) = create_row_message(case) {
                assert!(message.contains(expected), "OAuth async {case}: {message}");
            }
            ack.assert_freed_once(&format!("OAuth async {case} ack user_data"));
            sink.assert_never_called(&format!("OAuth async {case}"));

            if !oauth_only {
                let (headers, ack, sink) = (
                    FreeCount::leaked(),
                    FreeCount::leaked(),
                    AsyncSink::leaked(),
                );
                let (returned, success, retryable, message) =
                    call.headers_async(headers, ack, sink);
                assert!(
                    !returned && !success && !retryable,
                    "headers async {case}: {message}"
                );
                headers.assert_freed_once(&format!("headers async {case} user_data"));
                ack.assert_freed_once(&format!("headers async {case} ack user_data"));
                sink.assert_never_called(&format!("headers async {case}"));
            }
        }
        crate::zerobus_sdk_free(sdk);

        #[derive(Debug, Clone, Copy)]
        enum Async {
            Proto,
            Json,
            Protos,
            Jsons,
            Wait,
            Flush,
            Close,
            Unacked,
        }
        use Async::*;
        let args = Args::new();
        let null = ptr::null_mut();
        let (data_array, json_array) = ([args.data.as_ptr()], [args.json.as_ptr()]);
        let run = |export: Async, arg: Null, ud: *mut c_void, r: &mut CResult| -> bool {
            let (data, lens) = args.data_lens(arg);
            let (recs, jrecs) = (
                array_or_null(&data_array, arg),
                array_or_null(&json_array, arg),
            );
            let (off, b) = (sink_offset, sink_bool);
            match export {
                Proto => proto1_a(null, data, 1, off, ud, r),
                Json => json1_a(null, args.json.as_ptr(), off, ud, r),
                Protos => protos_a(null, recs, lens, 1, off, ud, r),
                Jsons => jsons_a(null, jrecs, 1, off, ud, r),
                Wait => wait_a(null, 0, b, ud, r),
                Flush => flush_a(null, b, ud, r),
                Close => close_a(null, b, ud, r),
                Unacked => unacked_a(null, sink_records, ud, r),
            }
        };
        let rows = [
            (Proto, Null::Data, BAD_DATA),
            (Proto, Null::Mux, NULL_HANDLE),
            (Json, Null::Mux, NULL_HANDLE),
            (Protos, Null::Data, BAD_RECORDS),
            (Protos, Null::Lens, BAD_RECORDS),
            (Protos, Null::Mux, NULL_HANDLE),
            (Jsons, Null::Data, BAD_RECORDS),
            (Jsons, Null::Mux, NULL_HANDLE),
            (Wait, Null::Mux, NULL_HANDLE),
            (Flush, Null::Mux, NULL_HANDLE),
            (Close, Null::Mux, NULL_HANDLE),
            (Unacked, Null::Mux, NULL_HANDLE),
        ];
        let sinks: Vec<(String, &'static AsyncSink)> = rows
            .iter()
            .map(|&(export, arg, expected)| {
                let what = format!("{export:?}_async {arg:?}=NULL");
                let sink = AsyncSink::leaked();
                let mut result = unwritten_result();
                let returned = run(export, arg, sink.user_data(), &mut result);
                let (returned, success, retryable, message) = sync_part(returned, &mut result);
                assert!(!returned && !success && !retryable, "{what}: {message}");
                assert!(message.contains(expected), "{what}: {message}");
                (what, sink)
            })
            .collect();
        // Let a wrongly spawned callback land.
        std::thread::sleep(Duration::from_millis(200));
        for (what, sink) in sinks {
            assert_eq!(sink.count(), 0, "{what}: callback must not run");
        }
    }

    /// Async creates: `options->ack_on_*` is rejected synchronously (false,
    /// no callback); config and connect errors return true and reach the
    /// callback once with a NULL mux. Either way both `user_data` are freed
    /// once.
    #[test]
    fn async_create_failures_report_and_free_once() {
        let sdk = test_sdk();
        for (case, call, retryable, expected) in &config_rows(sdk) {
            let in_task = !case.starts_with("options->");
            let wait = if *retryable {
                CONNECT_FAILURE_WAIT
            } else {
                Duration::from_secs(5)
            };
            let check = |what: &str, sync: (bool, bool, bool, String), sink: &AsyncSink| {
                let (returned, success, sync_retryable, message) = sync;
                let (success, got_retryable, message) = if in_task {
                    assert!(returned && success, "{what}: {message}");
                    let (_, success, got_retryable, message, mux) = sink.one_within(what, wait);
                    assert_eq!(mux, 0, "{what}: the failure carries a NULL mux");
                    (success, got_retryable, message)
                } else {
                    assert!(!returned, "{what}: {message}");
                    sink.assert_never_called(what);
                    (success, sync_retryable, message)
                };
                assert!(!success, "{what}: {message}");
                assert_eq!(got_retryable, *retryable, "{what}: {message}");
                if let Some(expected) = expected {
                    assert!(message.contains(expected), "{what}: {message}");
                }
            };
            let (ack, sink) = (FreeCount::leaked(), AsyncSink::leaked());
            let what = format!("OAuth async {case}");
            check(&what, call.oauth_async(ack, sink), sink);
            ack.assert_freed_once(&format!("{what} ack user_data"));

            let (headers, ack, sink) = (
                FreeCount::leaked(),
                FreeCount::leaked(),
                AsyncSink::leaked(),
            );
            let what = format!("headers async {case}");
            check(&what, call.headers_async(headers, ack, sink), sink);
            headers.assert_freed_once(&format!("{what} user_data"));
            ack.assert_freed_once(&format!("{what} ack user_data"));
        }
        crate::zerobus_sdk_free(sdk);
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

    /// The tracker keeps the first failure (typed cause unchanged), hands
    /// it out once, and keeps a later epoch's failure for a later flush.
    #[test]
    fn pending_ingests_keep_first_error_per_epoch() {
        use databricks_zerobus_ingest_sdk::{GrpcCode, GrpcStatus};
        let tracker = Arc::new(PendingIngests::default());
        let fail = |error: ZerobusError| tracker.begin().error = Some(error);
        let invalid = |message: &str| ZerobusError::InvalidArgument(message.into());
        let taken = |target| match tracker.take_error(target) {
            Some(ZerobusError::InvalidArgument(m)) => Some(m),
            None => None,
            Some(other) => panic!("unexpected stored error: {other:?}"),
        };

        // A retryable typed cause comes back unchanged, once.
        fail(ZerobusError::StreamClosedError(GrpcStatus::new(
            GrpcCode::Unavailable,
            "x",
        )));
        match tracker.take_error(None).expect("the failure is stored") {
            ZerobusError::StreamClosedError(status) => {
                assert_eq!(
                    (status.code(), status.message()),
                    (GrpcCode::Unavailable, "x")
                );
            }
            other => panic!("stored error changed variant: {other:?}"),
        }
        assert!(tracker.take_error(None).is_none(), "taken once");

        // The first failure wins; the later one is dropped.
        fail(invalid("A"));
        fail(invalid("B"));
        assert_eq!(taken(None).as_deref(), Some("A"));
        assert_eq!(taken(None), None);

        // A failure accepted after a flush began belongs to the next flush.
        fail(invalid("before"));
        let target = tracker.start_flush();
        fail(invalid("after"));
        assert_eq!(taken(Some(target)).as_deref(), Some("before"));
        assert_eq!(
            taken(Some(target)),
            None,
            "a later epoch's error must wait for a later flush"
        );
        let next = tracker.start_flush();
        assert_eq!(taken(Some(next)).as_deref(), Some("after"));
    }

    /// A panic in background work (async create, any async op, async
    /// ingest) reaches its callback once as a non-retryable error, and
    /// releases what the work held.
    #[test]
    fn panics_in_background_work_are_reported_once() {
        const PANIC: &str = "Rust panic caught at FFI boundary";

        // Async create: max_inflight usize::MAX panics in the task.
        let sdk = test_sdk();
        let call = CreateCall::with(sdk, |c| {
            c.stream_count = 1;
            c.options.max_inflight_requests = usize::MAX;
        });
        let (ack, sink) = (FreeCount::leaked(), AsyncSink::leaked());
        let (returned, success, _, message) = call.oauth_async(ack, sink);
        assert!(returned && success, "{message}");
        let (_, success, retryable, message, mux) = sink.one("panicking async create");
        assert!(mux == 0 && !success && !retryable, "{message}");
        assert!(message.starts_with(PANIC), "async create: {message}");
        crate::zerobus_sdk_free(sdk);

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
        let sink = AsyncSink::leaked();
        let handle = validate_mux_ptr(mux).unwrap();
        spawn_ingest_async(
            handle,
            IngestPayload::PanicForTest,
            sink_offset,
            sink.user_data(),
        )
        .unwrap();
        let (id, success, retryable, message, _) = sink.one("panicking async ingest");
        assert!(id == INGEST_ERROR && !success && !retryable, "{message}");
        assert!(message.starts_with(PANIC), "async ingest: {message}");
        let (flushed, _, message) = blocking_within_2s(mux, flush, "flush after the panic");
        assert!(flushed, "{message}");
        let (closed, _, message) = blocking_within_2s(mux, close, "close after the panic");
        assert!(closed, "{message}");
        zerobus_multiplexed_stream_free(mux);
        crate::zerobus_sdk_free(sdk);
    }

    // _nowait group (pending decision).
    /// A panicking `_nowait` ingest is reported by the next flush or close,
    /// once; after a flush reports it, the mux keeps working.
    #[test]
    fn nowait_ingest_panic_is_reported_by_next_flush_or_close() {
        const NOWAIT_PANIC: &str = "a background _nowait ingest panicked; its record was dropped";
        type Blocking = extern "C" fn(*mut CZerobusMultiplexedStream, *mut CResult) -> bool;
        for (what, call) in [("flush", flush as Blocking), ("close", close as Blocking)] {
            let (_runtime, sdk, mux) = live_mux();
            spawn_ingest(validate_mux_ptr(mux).unwrap(), IngestPayload::PanicForTest).unwrap();

            let (returned, retryable, message) =
                blocking_within_2s(mux, call, &format!("{what} after the panic"));
            assert!(!returned && !retryable, "{what}: {message}");
            assert!(message.contains(NOWAIT_PANIC), "{what}: {message}");

            if what == "close" {
                assert!(zerobus_multiplexed_stream_is_closed(mux));
            } else {
                let json = CString::new(r#"{"id": 0}"#).unwrap();
                let mut result = unwritten_result();
                json1_nw(mux, json.as_ptr(), &mut result);
                assert!(take_full(&mut result).0, "a valid _nowait after the panic");
                let (flushed, _, message) =
                    blocking_within_2s(mux, flush, "flush after a valid _nowait");
                assert!(flushed, "reported once: {message}");
            }
            zerobus_multiplexed_stream_free(mux);
            crate::zerobus_sdk_free(sdk);
        }
    }
}
