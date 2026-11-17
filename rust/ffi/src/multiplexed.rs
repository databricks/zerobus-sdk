//! Multiplexed stream FFI surface (Beta).
//!
//! A multiplexed stream distributes ingestion round-robin across
//! `stream_count` gRPC sub-streams to raise aggregate throughput. Ingest calls
//! return a message ID: an opaque `i64` that packs the sub-stream index and
//! that sub-stream's offset. Pass it back to
//! `zerobus_multiplexed_stream_wait_for_message_id`; do not compare or order
//! message IDs, and only use them with the stream that produced them.
//!
//! Thread safety: every `zerobus_multiplexed_stream_*` call except `_free` may
//! run concurrently on the same handle from any thread. `_free` must not
//! overlap any other call on that handle.
//!
//! Ack and headers callbacks run on the FFI's runtime threads. They must not
//! call `zerobus_multiplexed_stream_*` functions, and must not block on a
//! thread that is inside one: the call would fail or deadlock.

use crate::common::*;
use crate::stream::{
    c_ack_callback, configure_stream_builder, encoded_records_to_c_array, StreamCreateAuth,
};
use async_trait::async_trait;
use databricks_zerobus_ingest_sdk::{
    AckCallback, EncodedRecord, HeadersProvider, MessageId, MultiplexedStream, ZerobusError,
    ZerobusResult, ZerobusSdk,
};
use prost::Message;
use std::collections::{BTreeMap, HashMap};
use std::os::raw::c_char;
use std::ptr;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify, OwnedRwLockReadGuard, RwLock};

/// Returned by ingest calls on failure; `result` carries the error.
const INGEST_ERROR: i64 = -1;
/// Returned by batch ingest calls when the batch is empty. Not a message ID.
const EMPTY_BATCH: i64 = -2;

/// Opaque handle for a multiplexed stream. Created by
/// `zerobus_sdk_create_multiplexed_stream[_with_headers_provider]` and
/// released by `zerobus_multiplexed_stream_free`.
#[repr(C)]
pub struct CZerobusMultiplexedStream {
    _private: [u8; 0],
}

/// Concrete type behind `*mut CZerobusMultiplexedStream`, held in an `Arc`.
struct MuxHandle {
    /// `close` and `get_unacked_records` need `&mut MultiplexedStream`, while
    /// ingest, flush, and wait need only `&`, possibly from `_nowait` tasks
    /// that hold their own owned read guard. The lock lets those share the mux
    /// and makes a close wait for them instead of aliasing a `&mut` with live
    /// `&` borrows.
    stream: Arc<RwLock<MultiplexedStream>>,
    /// `_nowait` submissions not yet queued into the core, so flush can wait
    /// for them.
    nowait: Arc<NowaitTracker>,
}

/// Counts `_nowait` submissions per flush epoch.
///
/// A flush starts a new epoch and waits only for submissions from earlier
/// epochs, so producers that keep submitting cannot hold it off indefinitely.
#[derive(Default)]
struct NowaitTracker {
    epochs: std::sync::Mutex<NowaitEpochs>,
    drained: Notify,
}

#[derive(Default)]
struct NowaitEpochs {
    current: u64,
    /// Submissions not yet queued, by epoch; empty epochs are removed.
    pending: BTreeMap<u64, usize>,
}

impl NowaitTracker {
    fn epochs(&self) -> std::sync::MutexGuard<'_, NowaitEpochs> {
        // The state stays consistent even if a holder panicked.
        self.epochs.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Registers a submission in the current epoch.
    fn begin(self: &Arc<Self>) -> NowaitTicket {
        let mut epochs = self.epochs();
        let epoch = epochs.current;
        *epochs.pending.entry(epoch).or_default() += 1;
        NowaitTicket {
            tracker: Arc::clone(self),
            epoch,
        }
    }

    /// Waits until every submission registered before this call is queued.
    async fn wait_for_accepted(&self) {
        let target = {
            let mut epochs = self.epochs();
            let target = epochs.current;
            epochs.current += 1;
            target
        };
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

/// Marks one `_nowait` submission as queued (or failed) when dropped.
struct NowaitTicket {
    tracker: Arc<NowaitTracker>,
    epoch: u64,
}

impl Drop for NowaitTicket {
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

/// Copies a borrowed byte buffer; a null pointer is accepted only when `len` is 0.
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

/// Copies a C array of NUL-terminated JSON strings so the caller may free them on return.
unsafe fn copy_json_records(
    json_records: *const *const c_char,
    num_records: usize,
) -> Result<Vec<EncodedRecord>, &'static str> {
    std::slice::from_raw_parts(json_records, num_records)
        .iter()
        .map(|&json| c_str_to_string(json).map(EncodedRecord::Json))
        .collect()
}

/// Bridges the C ack callbacks to the mux's `AckCallback<MessageId>`; the C
/// callback's `i64` argument receives `MessageId::raw()`.
struct MultiplexedCallbackAckCallback(CallbackAckCallback);

impl AckCallback<MessageId> for MultiplexedCallbackAckCallback {
    fn on_ack(&self, message_id: MessageId) {
        self.0.on_ack(message_id.raw());
    }

    fn on_error(&self, message_id: MessageId, error_message: &str) {
        self.0.on_error(message_id.raw(), error_message);
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

async fn build_mux_from_parts(
    sdk_ref: &ZerobusSdk,
    table_name: String,
    descriptor_proto: Option<prost_types::DescriptorProto>,
    auth: StreamCreateAuth,
    stream_count: usize,
    options: Option<CStreamConfigurationOptions>,
) -> ZerobusResult<*mut CZerobusMultiplexedStream> {
    let mut builder = configure_stream_builder(
        sdk_ref,
        table_name,
        descriptor_proto,
        auth,
        options.as_ref(),
    )?;
    if let Some(callback) = options.as_ref().and_then(c_ack_callback) {
        builder =
            builder.multiplexed_ack_callback(Arc::new(MultiplexedCallbackAckCallback(callback)));
    }
    let mux = builder.multiplexed(stream_count).build().await?;
    let handle = MuxHandle {
        stream: Arc::new(RwLock::new(mux)),
        nowait: Arc::default(),
    };
    Ok(Arc::into_raw(Arc::new(handle)) as *mut CZerobusMultiplexedStream)
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
/// Ack callbacks in `options` receive message IDs rather than offsets. Each
/// sub-stream delivers its callbacks on its own background task, so callbacks
/// from different sub-streams may run concurrently and in no global order;
/// `ack_user_data` must tolerate concurrent access.
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
    result: *mut CResult,
) -> *mut CZerobusMultiplexedStream {
    ffi_guard(result, ptr::null_mut(), move || {
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
/// Ownership of `user_data` follows
/// `zerobus_sdk_create_stream_with_headers_provider`: once this function is
/// called, the FFI owns it and invokes `free_user_data` (when set) exactly
/// once, on failure before returning, or on success after the last sub-stream
/// releases the provider.
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
    result: *mut CResult,
) -> *mut CZerobusMultiplexedStream {
    ffi_guard(result, ptr::null_mut(), move || {
        // Construct the provider before any fallible work: it owns `user_data`,
        // so dropping it on an early return frees `user_data` exactly once.
        let headers_provider: Arc<dyn HeadersProvider> = Arc::new(SerializedHeadersProvider::new(
            CallbackHeadersProvider::new(headers_callback, user_data, free_user_data),
        ));

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
            )
            .await
        });
        complete(result, res, ptr::null_mut())
    })
}

/// Free a multiplexed stream handle.
///
/// Call `zerobus_multiplexed_stream_close` first to flush pending records;
/// freeing an open stream abandons them. Background `_nowait` tasks keep the
/// stream alive only until they have queued their records; those records are
/// then abandoned too. Must not run concurrently with any other call on `mux`,
/// and `mux` must not be used afterwards.
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_free(mux: *mut CZerobusMultiplexedStream) {
    ffi_guard(ptr::null_mut(), (), move || {
        if !mux.is_null() {
            unsafe {
                drop(Arc::from_raw(mux as *const MuxHandle));
            }
        }
    })
}

/// Ingest a protobuf-encoded record into the next sub-stream.
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

/// Ingest a JSON record into the next sub-stream.
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

/// Ingest a batch of protobuf-encoded records into a single sub-stream.
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

/// Ingest a batch of JSON records into a single sub-stream.
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

enum NowaitPayload {
    Record(EncodedRecord),
    Batch(Vec<EncodedRecord>),
}

/// Takes a shared lock on the mux for a `_nowait` call without blocking.
///
/// The guard is taken before the call returns to C, so a later close or
/// `get_unacked_records` waits until the spawned task has queued its payload.
/// `try_read_owned` fails only while one of those holds or waits for the write
/// lock; both close the mux, so the call reports it as closing.
fn lock_for_nowait(handle: &MuxHandle) -> ZerobusResult<OwnedRwLockReadGuard<MultiplexedStream>> {
    let guard = Arc::clone(&handle.stream)
        .try_read_owned()
        .map_err(|_| ZerobusError::InvalidStateError("MultiplexedStream is closing".to_string()))?;
    if guard.is_closed() {
        return Err(ZerobusError::InvalidStateError(
            "MultiplexedStream is closed".to_string(),
        ));
    }
    Ok(guard)
}

/// Queues an ingest on a background task that holds the mux's read lock and
/// a flush ticket, both released once the core has queued (or rejected) it.
fn spawn_ingest(handle: &MuxHandle, payload: NowaitPayload) -> ZerobusResult<()> {
    let guard = lock_for_nowait(handle)?;
    let ticket = handle.nowait.begin();
    RUNTIME.spawn(async move {
        let _ticket = ticket;
        let _ = match payload {
            NowaitPayload::Record(record) => guard.ingest_record(record).await.map(Some),
            NowaitPayload::Batch(batch) => guard.ingest_records(batch).await,
        };
    });
    Ok(())
}

/// Ingest a protobuf record without waiting for it to be queued (fire-and-forget).
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
/// task waits), the record is dropped: it is not returned by
/// `zerobus_multiplexed_stream_get_unacked_records`, and only a sub-stream
/// failure surfaces from the next flush or close. Use the blocking ingest
/// calls when every record must be accounted for.
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
        let res = spawn_ingest(handle, NowaitPayload::Record(record));
        complete(result, res, ())
    })
}

/// Ingest a JSON record without waiting for it to be queued (fire-and-forget).
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

        let res = spawn_ingest(handle, NowaitPayload::Record(record));
        complete(result, res, ())
    })
}

/// Ingest a batch of protobuf records without waiting (fire-and-forget).
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
            let res = spawn_ingest(handle, NowaitPayload::Batch(batch));
            complete(result, res, ())
        } else {
            // Nothing to queue, but report a closed mux like the blocking batch.
            let res = lock_for_nowait(handle).map(drop);
            complete(result, res, ())
        }
    })
}

/// Ingest a batch of JSON records without waiting (fire-and-forget).
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
            let res = spawn_ingest(handle, NowaitPayload::Batch(batch));
            complete(result, res, ())
        } else {
            // Nothing to queue, but report a closed mux like the blocking batch.
            let res = lock_for_nowait(handle).map(drop);
            complete(result, res, ())
        }
    })
}

/// Wait until the record or batch behind `message_id` is acknowledged.
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

/// Wait until every record queued on every sub-stream is acknowledged.
///
/// Covers blocking ingests that have returned and every `_nowait` record
/// accepted before this call: it first waits for their background tasks to
/// queue them (up to 30 seconds each when a sub-stream is at capacity), then
/// for their acknowledgments. `_nowait` records submitted during the flush
/// are left for the next flush or close.
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
            handle.nowait.wait_for_accepted().await;
            handle.stream.read().await.flush().await
        });
        complete(result, res.map(|()| true), false)
    })
}

/// Flush and close every sub-stream.
///
/// Waits for in-progress ingest, flush, and wait calls on this mux to return,
/// and for every accepted `_nowait` record to be queued, then flushes those
/// too. `_nowait` calls made after close starts report an error. On error,
/// recover unacknowledged records with
/// `zerobus_multiplexed_stream_get_unacked_records`. The handle must still be
/// released with `zerobus_multiplexed_stream_free`.
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

/// Returns whether the mux is closed, either explicitly or because an ingest,
/// flush, or wait call on it observed a sub-stream failure. A failure that no
/// call has observed yet does not set it. Also returns true while a close is
/// in progress and for a null handle.
#[no_mangle]
pub extern "C" fn zerobus_multiplexed_stream_is_closed(
    mux: *mut CZerobusMultiplexedStream,
) -> bool {
    ffi_guard(ptr::null_mut(), true, move || match validate_mux_ptr(mux) {
        Ok(handle) => handle.stream.try_read().map_or(true, |mux| mux.is_closed()),
        Err(_) => true,
    })
}

/// Close the mux and return every record that was not acknowledged.
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

#[cfg(test)]
mod multiplexed_tests {
    use super::*;
    use crate::common::{ACK_CALLBACK_DROP_COUNT, ACK_DROP_SENTINEL_CREATE_FAIL_TESTS};
    use crate::zerobus_free_error_message;
    use std::ffi::{CStr, CString};
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
    use std::time::Duration;

    fn unwritten_result() -> CResult {
        CResult {
            success: true,
            error_message: ptr::null_mut(),
            is_retryable: false,
        }
    }

    /// Returns `(success, message)` and frees the message.
    fn take_result(result: &mut CResult) -> (bool, String) {
        let message = if result.error_message.is_null() {
            String::new()
        } else {
            let message = unsafe { CStr::from_ptr(result.error_message) }
                .to_string_lossy()
                .into_owned();
            zerobus_free_error_message(result.error_message);
            result.error_message = ptr::null_mut();
            message
        };
        (result.success, message)
    }

    fn test_sdk() -> *mut CZerobusSdk {
        let endpoint = CString::new("http://127.0.0.1:1").unwrap();
        let builder = crate::zerobus_sdk_builder_new();
        crate::zerobus_sdk_builder_endpoint(builder, endpoint.as_ptr());
        crate::zerobus_sdk_builder_unity_catalog_url(builder, endpoint.as_ptr());
        crate::zerobus_sdk_builder_disable_tls(builder);
        let mut result = unwritten_result();
        let sdk = crate::zerobus_sdk_builder_build(builder, &mut result);
        assert!(result.success && !sdk.is_null());
        sdk
    }

    fn json_options() -> CStreamConfigurationOptions {
        let mut options = crate::zerobus_get_default_config();
        options.record_type = 2;
        options
    }

    extern "C" fn empty_headers(_user_data: *mut std::ffi::c_void) -> CHeaders {
        CHeaders {
            headers: ptr::null_mut(),
            count: 0,
            error_message: ptr::null_mut(),
        }
    }

    /// Returns the handle and the unread `result`, so callers can check `is_retryable`.
    fn create_oauth_raw(
        sdk: *mut CZerobusSdk,
        table: &str,
        stream_count: usize,
        options: &CStreamConfigurationOptions,
    ) -> (*mut CZerobusMultiplexedStream, CResult) {
        let table = CString::new(table).unwrap();
        let client_id = CString::new("client-id").unwrap();
        let client_secret = CString::new("client-secret").unwrap();
        let mut result = unwritten_result();
        let mux = zerobus_sdk_create_multiplexed_stream(
            sdk,
            table.as_ptr(),
            ptr::null(),
            0,
            client_id.as_ptr(),
            client_secret.as_ptr(),
            stream_count,
            options,
            &mut result,
        );
        (mux, result)
    }

    fn create_oauth(
        sdk: *mut CZerobusSdk,
        table: &str,
        stream_count: usize,
        options: &CStreamConfigurationOptions,
    ) -> (*mut CZerobusMultiplexedStream, bool, String) {
        let (mux, mut result) = create_oauth_raw(sdk, table, stream_count, options);
        let (success, message) = take_result(&mut result);
        (mux, success, message)
    }

    /// Polls `condition` until it holds, failing after 5 seconds.
    fn wait_until(what: &str, condition: impl Fn() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !condition() {
            assert!(std::time::Instant::now() < deadline, "timed out: {what}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn null_handles_report_errors_without_panicking() {
        let null = ptr::null_mut();
        let data = [1u8];
        let json = CString::new("{}").unwrap();
        let mut result = unwritten_result();

        let id = zerobus_multiplexed_stream_ingest_proto_record(
            null,
            data.as_ptr(),
            data.len(),
            &mut result,
        );
        assert_eq!(id, INGEST_ERROR);
        assert_eq!(
            take_result(&mut result),
            (false, "Multiplexed stream pointer is null".into())
        );

        let id = zerobus_multiplexed_stream_ingest_json_record(null, json.as_ptr(), &mut result);
        assert_eq!(id, INGEST_ERROR);
        assert!(!take_result(&mut result).0);

        assert!(!zerobus_multiplexed_stream_wait_for_message_id(
            null,
            0,
            &mut result
        ));
        assert!(!take_result(&mut result).0);
        assert!(!zerobus_multiplexed_stream_flush(null, &mut result));
        assert!(!take_result(&mut result).0);
        assert!(!zerobus_multiplexed_stream_close(null, &mut result));
        assert!(!take_result(&mut result).0);

        let records = zerobus_multiplexed_stream_get_unacked_records(null, &mut result);
        assert!(records.records.is_null() && records.len == 0);
        assert!(!take_result(&mut result).0);

        zerobus_multiplexed_stream_ingest_json_record_nowait(null, json.as_ptr(), &mut result);
        assert!(!take_result(&mut result).0);

        assert!(zerobus_multiplexed_stream_is_closed(null));
        zerobus_multiplexed_stream_free(null);
    }

    #[test]
    fn null_handle_is_rejected_by_batch_and_nowait_ingest() {
        let null = ptr::null_mut();
        let data = [1u8];
        let proto_ptrs = [data.as_ptr()];
        let proto_lens = [data.len()];
        let json = CString::new("{}").unwrap();
        let json_ptrs = [json.as_ptr()];
        let null_handle = || (false, "Multiplexed stream pointer is null".to_string());
        let mut result = unwritten_result();

        let id = zerobus_multiplexed_stream_ingest_proto_records(
            null,
            proto_ptrs.as_ptr(),
            proto_lens.as_ptr(),
            1,
            &mut result,
        );
        assert_eq!(id, INGEST_ERROR);
        assert!(!result.is_retryable);
        assert_eq!(take_result(&mut result), null_handle());

        let id = zerobus_multiplexed_stream_ingest_json_records(
            null,
            json_ptrs.as_ptr(),
            1,
            &mut result,
        );
        assert_eq!(id, INGEST_ERROR);
        assert!(!result.is_retryable);
        assert_eq!(take_result(&mut result), null_handle());

        zerobus_multiplexed_stream_ingest_proto_record_nowait(
            null,
            data.as_ptr(),
            data.len(),
            &mut result,
        );
        assert!(!result.is_retryable);
        assert_eq!(take_result(&mut result), null_handle());

        zerobus_multiplexed_stream_ingest_proto_records_nowait(
            null,
            proto_ptrs.as_ptr(),
            proto_lens.as_ptr(),
            1,
            &mut result,
        );
        assert!(!result.is_retryable);
        assert_eq!(take_result(&mut result), null_handle());

        zerobus_multiplexed_stream_ingest_json_records_nowait(
            null,
            json_ptrs.as_ptr(),
            1,
            &mut result,
        );
        assert!(!result.is_retryable);
        assert_eq!(take_result(&mut result), null_handle());
    }

    /// Pointer arguments are checked before the handle, so a NULL handle
    /// reports the pointer error.
    #[test]
    fn null_record_pointers_are_rejected() {
        let null = ptr::null_mut();
        let data = [1u8];
        let proto_ptrs = [data.as_ptr()];
        let proto_lens = [data.len()];
        let invalid_data = || (false, "Invalid data pointer".to_string());
        let invalid_records = || (false, "Invalid records pointer".to_string());
        let mut result = unwritten_result();

        let id = zerobus_multiplexed_stream_ingest_proto_record(null, ptr::null(), 1, &mut result);
        assert_eq!(id, INGEST_ERROR);
        assert!(!result.is_retryable);
        assert_eq!(take_result(&mut result), invalid_data());

        zerobus_multiplexed_stream_ingest_proto_record_nowait(null, ptr::null(), 1, &mut result);
        assert!(!result.is_retryable);
        assert_eq!(take_result(&mut result), invalid_data());

        // NULL records with valid lengths, then valid records with NULL lengths.
        for (records, lens) in [
            (ptr::null(), proto_lens.as_ptr()),
            (proto_ptrs.as_ptr(), ptr::null()),
        ] {
            let id = zerobus_multiplexed_stream_ingest_proto_records(
                null,
                records,
                lens,
                1,
                &mut result,
            );
            assert_eq!(id, INGEST_ERROR);
            assert!(!result.is_retryable);
            assert_eq!(take_result(&mut result), invalid_records());

            zerobus_multiplexed_stream_ingest_proto_records_nowait(
                null,
                records,
                lens,
                1,
                &mut result,
            );
            assert!(!result.is_retryable);
            assert_eq!(take_result(&mut result), invalid_records());
        }

        let id = zerobus_multiplexed_stream_ingest_json_records(null, ptr::null(), 1, &mut result);
        assert_eq!(id, INGEST_ERROR);
        assert!(!result.is_retryable);
        assert_eq!(take_result(&mut result), invalid_records());

        zerobus_multiplexed_stream_ingest_json_records_nowait(null, ptr::null(), 1, &mut result);
        assert!(!result.is_retryable);
        assert_eq!(take_result(&mut result), invalid_records());
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

    #[test]
    fn create_rejects_invalid_stream_count_before_connecting() {
        let sdk = test_sdk();
        for stream_count in [0, 65] {
            let (mux, success, message) =
                create_oauth(sdk, "main.default.events", stream_count, &json_options());
            assert!(mux.is_null());
            assert!(!success);
            assert!(
                message.contains("stream_count must be between 1 and 64"),
                "unexpected error: {message}"
            );
        }
        crate::zerobus_sdk_free(sdk);
    }

    #[test]
    fn create_rejects_stream_count_above_inflight_budget() {
        let sdk = test_sdk();
        let mut options = json_options();
        options.max_inflight_requests = 2;
        let (mux, success, message) = create_oauth(sdk, "main.default.events", 3, &options);
        assert!(mux.is_null());
        assert!(!success);
        assert!(
            message.contains("must be at least stream_count"),
            "unexpected error: {message}"
        );
        crate::zerobus_sdk_free(sdk);
    }

    #[test]
    fn failed_create_releases_ack_callback() {
        extern "C" fn noop_ack(_id: i64, _user_data: *mut std::ffi::c_void) {}
        let _guard = crate::tests::ACK_DROP_TEST_LOCK.lock().unwrap();

        let sdk = test_sdk();
        let mut options = json_options();
        options.ack_on_ack = Some(noop_ack);
        options.ack_user_data =
            (&ACK_DROP_SENTINEL_CREATE_FAIL_TESTS as *const u8) as *mut std::ffi::c_void;

        let before = ACK_CALLBACK_DROP_COUNT.load(Ordering::SeqCst);
        // An empty table name fails validation after the callback is registered.
        let (mux, success, _) = create_oauth(sdk, "", 2, &options);
        let after = ACK_CALLBACK_DROP_COUNT.load(Ordering::SeqCst);

        assert!(mux.is_null() && !success);
        assert_eq!(
            after - before,
            1,
            "the ack callback must be dropped exactly once when creation fails"
        );
        crate::zerobus_sdk_free(sdk);
    }

    #[test]
    fn failed_create_with_headers_provider_frees_user_data_once() {
        static FREES: AtomicUsize = AtomicUsize::new(0);
        static SENTINEL: u8 = 0;
        extern "C" fn free_user_data(user_data: *mut std::ffi::c_void) {
            if ptr::eq(user_data as *const u8, &SENTINEL) {
                FREES.fetch_add(1, Ordering::SeqCst);
            }
        }

        let sdk = test_sdk();
        let table = CString::new("").unwrap();
        let options = json_options();
        let mut result = unwritten_result();
        let before = FREES.load(Ordering::SeqCst);
        let mux = zerobus_sdk_create_multiplexed_stream_with_headers_provider(
            sdk,
            table.as_ptr(),
            ptr::null(),
            0,
            empty_headers,
            (&SENTINEL as *const u8) as *mut std::ffi::c_void,
            Some(free_user_data),
            2,
            &options,
            &mut result,
        );
        assert!(mux.is_null());
        assert!(!take_result(&mut result).0);
        assert_eq!(FREES.load(Ordering::SeqCst) - before, 1);
        crate::zerobus_sdk_free(sdk);
    }

    #[test]
    fn create_rejects_invalid_arguments_before_connecting() {
        static FREES: AtomicUsize = AtomicUsize::new(0);
        static SENTINEL: u8 = 0;
        extern "C" fn free_user_data(user_data: *mut std::ffi::c_void) {
            if ptr::eq(user_data as *const u8, &SENTINEL) {
                FREES.fetch_add(1, Ordering::SeqCst);
            }
        }

        let sdk = test_sdk();
        let table = CString::new("main.default.events").unwrap();
        let client_id = CString::new("client-id").unwrap();
        let client_secret = CString::new("client-secret").unwrap();
        let options = json_options();
        // A truncated varint: not a decodable DescriptorProto.
        let malformed: &[u8] = &[0xff, 0xff, 0xff];
        let none: &[u8] = &[];
        let null_sdk = ptr::null_mut();
        let null_str = ptr::null();

        let oauth_cases = [
            (
                "sdk=NULL",
                null_sdk,
                table.as_ptr(),
                none,
                client_id.as_ptr(),
                client_secret.as_ptr(),
            ),
            (
                "table_name=NULL",
                sdk,
                null_str,
                none,
                client_id.as_ptr(),
                client_secret.as_ptr(),
            ),
            (
                "client_id=NULL",
                sdk,
                table.as_ptr(),
                none,
                null_str,
                client_secret.as_ptr(),
            ),
            (
                "client_secret=NULL",
                sdk,
                table.as_ptr(),
                none,
                client_id.as_ptr(),
                null_str,
            ),
            (
                "malformed descriptor",
                sdk,
                table.as_ptr(),
                malformed,
                client_id.as_ptr(),
                client_secret.as_ptr(),
            ),
        ];
        for (case, sdk_arg, table_arg, descriptor, id_arg, secret_arg) in oauth_cases {
            let mut result = unwritten_result();
            let mux = zerobus_sdk_create_multiplexed_stream(
                sdk_arg,
                table_arg,
                descriptor.as_ptr(),
                descriptor.len(),
                id_arg,
                secret_arg,
                2,
                &options,
                &mut result,
            );
            assert!(mux.is_null(), "OAuth {case}");
            let retryable = result.is_retryable;
            let (success, message) = take_result(&mut result);
            assert!(!success && !retryable, "OAuth {case}: {message}");
        }

        let headers_cases = [
            ("sdk=NULL", null_sdk, table.as_ptr(), none),
            ("table_name=NULL", sdk, null_str, none),
            ("malformed descriptor", sdk, table.as_ptr(), malformed),
        ];
        for (case, sdk_arg, table_arg, descriptor) in headers_cases {
            let before = FREES.load(Ordering::SeqCst);
            let mut result = unwritten_result();
            let mux = zerobus_sdk_create_multiplexed_stream_with_headers_provider(
                sdk_arg,
                table_arg,
                descriptor.as_ptr(),
                descriptor.len(),
                empty_headers,
                (&SENTINEL as *const u8) as *mut std::ffi::c_void,
                Some(free_user_data),
                2,
                &options,
                &mut result,
            );
            assert!(mux.is_null(), "headers {case}");
            let retryable = result.is_retryable;
            let (success, message) = take_result(&mut result);
            assert!(!success && !retryable, "headers {case}: {message}");
            assert_eq!(
                FREES.load(Ordering::SeqCst) - before,
                1,
                "headers {case}: user_data must be freed exactly once"
            );
        }

        // A NULL `result` is allowed; the failure is reported by the NULL handle.
        let mux = zerobus_sdk_create_multiplexed_stream(
            sdk,
            table.as_ptr(),
            ptr::null(),
            0,
            client_id.as_ptr(),
            client_secret.as_ptr(),
            0,
            &options,
            ptr::null_mut(),
        );
        assert!(mux.is_null());
        crate::zerobus_sdk_free(sdk);
    }

    #[test]
    fn create_that_fails_to_connect_is_retryable_and_releases_resources() {
        extern "C" fn noop_ack(_id: i64, _user_data: *mut std::ffi::c_void) {}
        static FREES: AtomicUsize = AtomicUsize::new(0);
        static SENTINEL: u8 = 0;
        extern "C" fn free_user_data(user_data: *mut std::ffi::c_void) {
            if ptr::eq(user_data as *const u8, &SENTINEL) {
                FREES.fetch_add(1, Ordering::SeqCst);
            }
        }
        let _guard = crate::tests::ACK_DROP_TEST_LOCK.lock().unwrap();

        // `test_sdk()` points at 127.0.0.1:1, so every sub-stream open fails.
        let sdk = test_sdk();
        let mut options = json_options();
        // Fail on the first connect error instead of retrying it.
        options.recovery = false;
        options.ack_on_ack = Some(noop_ack);
        options.ack_user_data =
            (&ACK_DROP_SENTINEL_CREATE_FAIL_TESTS as *const u8) as *mut std::ffi::c_void;

        let drops_before = ACK_CALLBACK_DROP_COUNT.load(Ordering::SeqCst);
        let (mux, mut result) = create_oauth_raw(sdk, "main.default.events", 2, &options);
        assert!(mux.is_null());
        let retryable = result.is_retryable;
        let (success, message) = take_result(&mut result);
        assert!(!success && retryable, "OAuth create: {message}");
        wait_until("OAuth create drops the ack callback", || {
            ACK_CALLBACK_DROP_COUNT.load(Ordering::SeqCst) - drops_before >= 1
        });
        // Let a late second release land before asserting exactly once.
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(
            ACK_CALLBACK_DROP_COUNT.load(Ordering::SeqCst) - drops_before,
            1
        );

        let table = CString::new("main.default.events").unwrap();
        let drops_before = ACK_CALLBACK_DROP_COUNT.load(Ordering::SeqCst);
        let frees_before = FREES.load(Ordering::SeqCst);
        let mut result = unwritten_result();
        let mux = zerobus_sdk_create_multiplexed_stream_with_headers_provider(
            sdk,
            table.as_ptr(),
            ptr::null(),
            0,
            empty_headers,
            (&SENTINEL as *const u8) as *mut std::ffi::c_void,
            Some(free_user_data),
            2,
            &options,
            &mut result,
        );
        assert!(mux.is_null());
        let retryable = result.is_retryable;
        let (success, message) = take_result(&mut result);
        assert!(!success && retryable, "headers create: {message}");
        wait_until("headers create releases its resources", || {
            ACK_CALLBACK_DROP_COUNT.load(Ordering::SeqCst) - drops_before >= 1
                && FREES.load(Ordering::SeqCst) - frees_before >= 1
        });
        // Let a late second release land before asserting exactly once.
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(
            ACK_CALLBACK_DROP_COUNT.load(Ordering::SeqCst) - drops_before,
            1
        );
        assert_eq!(FREES.load(Ordering::SeqCst) - frees_before, 1);
        crate::zerobus_sdk_free(sdk);
    }

    #[test]
    fn ack_callback_receives_raw_message_ids() {
        static LAST_ACK: AtomicI64 = AtomicI64::new(0);
        static LAST_ERROR: AtomicI64 = AtomicI64::new(0);
        extern "C" fn on_ack(id: i64, _user_data: *mut std::ffi::c_void) {
            LAST_ACK.store(id, Ordering::SeqCst);
        }
        extern "C" fn on_error(id: i64, _msg: *const c_char, _user_data: *mut std::ffi::c_void) {
            LAST_ERROR.store(id, Ordering::SeqCst);
        }

        let callback = MultiplexedCallbackAckCallback(CallbackAckCallback::new(
            Some(on_ack),
            Some(on_error),
            ptr::null_mut(),
        ));
        // Lane 63 sets the sign bit, so this raw ID is negative.
        let acked = MessageId::from_raw(((63u64 << 58) | 7) as i64);
        let failed = MessageId::from_raw((1i64 << 58) | 9);
        callback.on_ack(acked);
        callback.on_error(failed, "boom");

        assert_eq!(LAST_ACK.load(Ordering::SeqCst), acked.raw());
        assert!(acked.raw() < 0);
        assert_eq!(LAST_ERROR.load(Ordering::SeqCst), failed.raw());
    }

    #[test]
    fn serialized_headers_provider_allows_concurrent_lanes() {
        static ACTIVE: AtomicUsize = AtomicUsize::new(0);
        static MAX_ACTIVE: AtomicUsize = AtomicUsize::new(0);
        extern "C" fn slow_headers(_user_data: *mut std::ffi::c_void) -> CHeaders {
            let active = ACTIVE.fetch_add(1, Ordering::SeqCst) + 1;
            MAX_ACTIVE.fetch_max(active, Ordering::SeqCst);
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
