//! Drives the multiplexed stream C ABI against the mock gRPC server.
//!
//! The FFI blocks on its own global runtime, so these tests call it from plain
//! threads and run the mock server on a separate runtime.

#[allow(dead_code)]
mod mock_oauth;
mod utils;

use std::collections::HashSet;
use std::ffi::{c_void, CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use databricks_zerobus_ingest_sdk::MessageId;
use mock_oauth::{start_mock_oauth_server, MockTokenResponse};
use prost::Message;
use tokio::runtime::Runtime;
use tonic::Status;
use utils::create_test_descriptor_proto;
use zerobus_ffi::{
    zerobus_free_error_message, zerobus_free_record_array, zerobus_get_default_config,
    zerobus_multiplexed_stream_close, zerobus_multiplexed_stream_flush,
    zerobus_multiplexed_stream_free, zerobus_multiplexed_stream_get_unacked_records,
    zerobus_multiplexed_stream_ingest_json_record,
    zerobus_multiplexed_stream_ingest_json_record_nowait,
    zerobus_multiplexed_stream_ingest_json_records,
    zerobus_multiplexed_stream_ingest_json_records_nowait,
    zerobus_multiplexed_stream_ingest_proto_record,
    zerobus_multiplexed_stream_ingest_proto_record_nowait,
    zerobus_multiplexed_stream_ingest_proto_records,
    zerobus_multiplexed_stream_ingest_proto_records_nowait, zerobus_multiplexed_stream_is_closed,
    zerobus_multiplexed_stream_wait_for_message_id, zerobus_sdk_builder_build,
    zerobus_sdk_builder_disable_tls, zerobus_sdk_builder_endpoint, zerobus_sdk_builder_new,
    zerobus_sdk_builder_unity_catalog_url, zerobus_sdk_create_multiplexed_stream,
    zerobus_sdk_create_multiplexed_stream_with_headers_provider, zerobus_sdk_free, CHeaders,
    CRecordArray, CResult, CStreamConfigurationOptions, CZerobusMultiplexedStream, CZerobusSdk,
};
use zerobus_ffi::{
    zerobus_multiplexed_stream_close_async, zerobus_multiplexed_stream_flush_async,
    zerobus_multiplexed_stream_get_unacked_records_async,
    zerobus_multiplexed_stream_ingest_json_record_async,
    zerobus_multiplexed_stream_ingest_json_records_async,
    zerobus_multiplexed_stream_ingest_proto_record_async,
    zerobus_multiplexed_stream_ingest_proto_records_async,
    zerobus_multiplexed_stream_wait_for_message_id_async,
    zerobus_sdk_create_multiplexed_stream_async,
    zerobus_sdk_create_multiplexed_stream_with_headers_provider_async,
};
use zerobus_ffi::{
    zerobus_sdk_create_stream_with_headers_provider, zerobus_stream_close, zerobus_stream_flush,
    zerobus_stream_free, zerobus_stream_ingest_json_record,
};
use zerobus_test_support::mock_grpc::{
    start_mock_server, MockResponse, MockResponseGate, MockZerobusServer,
};

fn new_result() -> CResult {
    CResult {
        success: false,
        error_message: ptr::null_mut(),
        is_retryable: false,
    }
}

/// Returns the FFI error message (empty if none) and frees it.
fn take_message(result: &mut CResult) -> String {
    if result.error_message.is_null() {
        return String::new();
    }
    let message = unsafe { CStr::from_ptr(result.error_message) }
        .to_string_lossy()
        .into_owned();
    zerobus_free_error_message(result.error_message);
    result.error_message = ptr::null_mut();
    message
}

/// Panics with the FFI error message unless `result` reports success.
fn expect_success(result: &mut CResult, call: &str) {
    if !result.success {
        panic!("{call} failed: {}", take_message(result));
    }
}

/// Panics unless `result` reports failure; returns `(is_retryable, message)`.
fn expect_failure(result: &mut CResult, call: &str) -> (bool, String) {
    assert!(!result.success, "{call} unexpectedly succeeded");
    let retryable = result.is_retryable;
    (retryable, take_message(result))
}

/// Panics unless `result` reports a non-retryable failure; returns the message.
fn expect_permanent_failure(result: &mut CResult, call: &str) -> String {
    let (retryable, message) = expect_failure(result, call);
    assert!(!retryable, "{call} must not be retryable: {message}");
    message
}

fn stream_index(message_id: i64) -> usize {
    MessageId::from_raw(message_id).stream_index()
}

fn sub_offset(message_id: i64) -> i64 {
    MessageId::from_raw(message_id).sub_offset()
}

extern "C" fn empty_headers(_user_data: *mut c_void) -> CHeaders {
    CHeaders {
        headers: ptr::null_mut(),
        count: 0,
        error_message: ptr::null_mut(),
    }
}

/// Counts calls into, and frees of, a headers provider's `user_data`.
#[derive(Default)]
struct ProviderLog {
    calls: AtomicUsize,
    frees: AtomicUsize,
}

impl ProviderLog {
    /// Leaked so the FFI may keep `user_data` for as long as it likes.
    fn leaked() -> &'static ProviderLog {
        Box::leak(Box::default())
    }

    fn user_data(&'static self) -> *mut c_void {
        self as *const ProviderLog as *mut c_void
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn frees(&self) -> usize {
        self.frees.load(Ordering::SeqCst)
    }
}

extern "C" fn counting_headers(user_data: *mut c_void) -> CHeaders {
    let log = unsafe { &*(user_data as *const ProviderLog) };
    log.calls.fetch_add(1, Ordering::SeqCst);
    empty_headers(user_data)
}

extern "C" fn count_free(user_data: *mut c_void) {
    let log = unsafe { &*(user_data as *const ProviderLog) };
    log.frees.fetch_add(1, Ordering::SeqCst);
}

fn start_server() -> (Runtime, MockZerobusServer, String) {
    let runtime = Runtime::new().unwrap();
    let (server, url) = runtime.block_on(start_mock_server()).unwrap();
    (runtime, server, url)
}

fn create_sdk(server_url: &str) -> *mut CZerobusSdk {
    create_sdk_with_uc(server_url, "https://mock-uc.com")
}

fn create_sdk_with_uc(server_url: &str, uc_url: &str) -> *mut CZerobusSdk {
    let endpoint = CString::new(server_url).unwrap();
    let uc_url = CString::new(uc_url).unwrap();
    let builder = zerobus_sdk_builder_new();
    zerobus_sdk_builder_endpoint(builder, endpoint.as_ptr());
    zerobus_sdk_builder_unity_catalog_url(builder, uc_url.as_ptr());
    zerobus_sdk_builder_disable_tls(builder);
    let mut result = new_result();
    let sdk = zerobus_sdk_builder_build(builder, &mut result);
    expect_success(&mut result, "zerobus_sdk_builder_build");
    sdk
}

/// JSON records with recovery on. Without recovery, the core can report the
/// server's end-of-stream during close as close's own error, so tests that
/// expect a clean close keep recovery on.
fn json_options() -> CStreamConfigurationOptions {
    let mut options = zerobus_get_default_config();
    options.record_type = 2;
    options
}

/// JSON records without recovery, so an injected lane failure stays terminal.
fn json_options_terminal() -> CStreamConfigurationOptions {
    let mut options = json_options();
    options.recovery = false;
    options
}

/// Calls the headers-provider create; returns the handle and the unread result.
fn create_mux_raw(
    sdk: *mut CZerobusSdk,
    table: &str,
    descriptor: Option<&[u8]>,
    stream_count: usize,
    options: *const CStreamConfigurationOptions,
    provider: Option<&'static ProviderLog>,
) -> (*mut CZerobusMultiplexedStream, CResult) {
    create_mux_raw_with_ack(
        sdk,
        table,
        descriptor,
        stream_count,
        options,
        provider,
        None,
    )
}

/// Like `create_mux_raw`, with the mux ack callbacks recording into `ack_log`.
fn create_mux_raw_with_ack(
    sdk: *mut CZerobusSdk,
    table: &str,
    descriptor: Option<&[u8]>,
    stream_count: usize,
    options: *const CStreamConfigurationOptions,
    provider: Option<&'static ProviderLog>,
    ack_log: Option<&AckLog>,
) -> (*mut CZerobusMultiplexedStream, CResult) {
    let ack = ack_log.map_or_else(MuxAck::none, MuxAck::log);
    create_mux_raw_with_mux_ack(sdk, table, descriptor, stream_count, options, provider, ack)
}

/// The four mux ack parameters of a create call.
struct MuxAck {
    on_ack: Option<extern "C" fn(i64, *mut c_void)>,
    on_error: Option<extern "C" fn(i64, *const c_char, *mut c_void)>,
    user_data: *mut c_void,
    free_user_data: Option<extern "C" fn(*mut c_void)>,
}

impl MuxAck {
    fn none() -> Self {
        Self {
            on_ack: None,
            on_error: None,
            user_data: ptr::null_mut(),
            free_user_data: None,
        }
    }

    fn log(log: &AckLog) -> Self {
        Self {
            on_ack: Some(record_ack),
            on_error: Some(record_error),
            user_data: log as *const AckLog as *mut c_void,
            free_user_data: None,
        }
    }

    /// Hands `owner` to the FFI, with or without callbacks.
    fn owned(owner: &'static AckOwner, with_callbacks: bool) -> Self {
        Self {
            on_ack: with_callbacks.then_some(owner_on_ack as _),
            on_error: with_callbacks.then_some(owner_on_error as _),
            user_data: owner as *const AckOwner as *mut c_void,
            free_user_data: Some(owner_free),
        }
    }
}

fn create_mux_raw_with_mux_ack(
    sdk: *mut CZerobusSdk,
    table: &str,
    descriptor: Option<&[u8]>,
    stream_count: usize,
    options: *const CStreamConfigurationOptions,
    provider: Option<&'static ProviderLog>,
    ack: MuxAck,
) -> (*mut CZerobusMultiplexedStream, CResult) {
    let table = CString::new(table).unwrap();
    let (descriptor_ptr, descriptor_len) =
        descriptor.map_or((ptr::null(), 0), |d| (d.as_ptr(), d.len()));
    let (callback, user_data, free_user_data): (_, _, Option<extern "C" fn(*mut c_void)>) =
        match provider {
            Some(log) => (
                counting_headers as extern "C" fn(*mut c_void) -> CHeaders,
                log.user_data(),
                Some(count_free),
            ),
            None => (empty_headers as _, ptr::null_mut(), None),
        };
    let mut result = new_result();
    let mux = zerobus_sdk_create_multiplexed_stream_with_headers_provider(
        sdk,
        table.as_ptr(),
        descriptor_ptr,
        descriptor_len,
        callback,
        user_data,
        free_user_data,
        stream_count,
        options,
        ack.on_ack,
        ack.on_error,
        ack.user_data,
        ack.free_user_data,
        &mut result,
    );
    (mux, result)
}

fn create_mux(
    sdk: *mut CZerobusSdk,
    table: &str,
    descriptor: Option<&[u8]>,
    stream_count: usize,
    options: &CStreamConfigurationOptions,
) -> *mut CZerobusMultiplexedStream {
    let (mux, mut result) = create_mux_raw(sdk, table, descriptor, stream_count, options, None);
    expect_success(&mut result, "zerobus_sdk_create_multiplexed_stream");
    assert!(!mux.is_null());
    mux
}

/// Like `create_mux`, with the mux ack callbacks recording into `ack_log`.
fn create_mux_with_ack(
    sdk: *mut CZerobusSdk,
    table: &str,
    descriptor: Option<&[u8]>,
    stream_count: usize,
    options: &CStreamConfigurationOptions,
    ack_log: &AckLog,
) -> *mut CZerobusMultiplexedStream {
    let (mux, mut result) = create_mux_raw_with_ack(
        sdk,
        table,
        descriptor,
        stream_count,
        options,
        None,
        Some(ack_log),
    );
    expect_success(&mut result, "zerobus_sdk_create_multiplexed_stream");
    assert!(!mux.is_null());
    mux
}

/// Polls `condition` until it holds, failing after 5 seconds.
fn wait_until(what: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting: {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_writes(runtime: &Runtime, server: &MockZerobusServer, expected: u64) {
    wait_until(&format!("server receives {expected} records"), || {
        runtime.block_on(server.get_write_count()) >= expected
    });
}

fn json_records(ids: std::ops::Range<usize>) -> Vec<CString> {
    ids.map(|i| CString::new(format!(r#"{{"id": {i}}}"#)).unwrap())
        .collect()
}

/// Returns the JSON payloads of `records`, sorted, and frees the array.
fn take_json_payloads(records: CRecordArray) -> Vec<String> {
    let mut payloads: Vec<String> = (0..records.len)
        .map(|i| {
            let record = unsafe { &*records.records.add(i) };
            assert!(record.is_json);
            let bytes = unsafe { std::slice::from_raw_parts(record.data, record.data_len) };
            String::from_utf8(bytes.to_vec()).unwrap()
        })
        .collect();
    zerobus_free_record_array(records);
    payloads.sort();
    payloads
}

/// Closes `mux` and returns the payloads it never got acknowledged, sorted.
fn unacked_json_payloads(mux: *mut CZerobusMultiplexedStream) -> Vec<String> {
    let mut result = new_result();
    let records = zerobus_multiplexed_stream_get_unacked_records(mux, &mut result);
    expect_success(&mut result, "get_unacked_records");
    take_json_payloads(records)
}

fn sorted_strings<'a>(records: impl IntoIterator<Item = &'a CString>) -> Vec<String> {
    let mut strings: Vec<String> = records
        .into_iter()
        .map(|s| s.to_str().unwrap().to_owned())
        .collect();
    strings.sort();
    strings
}

#[derive(Default)]
struct AckLog {
    acks: Mutex<Vec<i64>>,
    errors: Mutex<Vec<i64>>,
}

impl AckLog {
    fn sorted_acks(&self) -> Vec<i64> {
        let mut acks = self.acks.lock().unwrap().clone();
        acks.sort();
        acks
    }
}

extern "C" fn record_ack(message_id: i64, user_data: *mut c_void) {
    let log = unsafe { &*(user_data as *const AckLog) };
    log.acks.lock().unwrap().push(message_id);
}

extern "C" fn record_error(message_id: i64, _message: *const c_char, user_data: *mut c_void) {
    let log = unsafe { &*(user_data as *const AckLog) };
    log.errors.lock().unwrap().push(message_id);
}

/// Mux ack `user_data` handed to the FFI: counts callbacks and frees, flags a
/// callback that runs after the free, and can hold a callback until released.
#[derive(Default)]
struct AckOwner {
    acks: AtomicUsize,
    errors: AtomicUsize,
    frees: AtomicUsize,
    callback_after_free: AtomicBool,
    block_callbacks: AtomicBool,
    callback_entered: AtomicBool,
    release_callbacks: AtomicBool,
}

impl AckOwner {
    /// Leaked: the FFI may call back into it until it frees it.
    fn leaked() -> &'static AckOwner {
        Box::leak(Box::default())
    }

    fn frees(&self) -> usize {
        self.frees.load(Ordering::SeqCst)
    }

    fn acks(&self) -> usize {
        self.acks.load(Ordering::SeqCst)
    }

    fn on_callback(&self) {
        if self.frees() > 0 {
            self.callback_after_free.store(true, Ordering::SeqCst);
        }
        if self.block_callbacks.load(Ordering::SeqCst) {
            self.callback_entered.store(true, Ordering::SeqCst);
            // Bounded, so a missing release fails the test instead of hanging it.
            let deadline = Instant::now() + Duration::from_secs(10);
            while !self.release_callbacks.load(Ordering::SeqCst) && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    }

    /// Asserts `user_data` was freed exactly once, with no callback after it.
    fn assert_freed_once(&self) {
        wait_until("ack user_data is freed", || self.frees() >= 1);
        // Let a late second release land before asserting exactly once.
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(self.frees(), 1, "ack user_data must be freed exactly once");
        assert!(
            !self.callback_after_free.load(Ordering::SeqCst),
            "a callback ran after ack user_data was freed"
        );
    }

    /// Asserts `user_data` is still owned after a teardown step has settled.
    fn assert_not_freed(&self, after: &str) {
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(self.frees(), 0, "ack user_data freed early, after {after}");
    }
}

extern "C" fn owner_on_ack(_message_id: i64, user_data: *mut c_void) {
    let owner = unsafe { &*(user_data as *const AckOwner) };
    owner.acks.fetch_add(1, Ordering::SeqCst);
    owner.on_callback();
}

extern "C" fn owner_on_error(_message_id: i64, _message: *const c_char, user_data: *mut c_void) {
    let owner = unsafe { &*(user_data as *const AckOwner) };
    owner.errors.fetch_add(1, Ordering::SeqCst);
    owner.on_callback();
}

extern "C" fn owner_free(user_data: *mut c_void) {
    let owner = unsafe { &*(user_data as *const AckOwner) };
    owner.frees.fetch_add(1, Ordering::SeqCst);
}

/// Single, batch, and fire-and-forget ingest on one sub-stream, followed by
/// one flush; the ack callback receives every returned message ID. Empty
/// batches return -2 without sending anything.
#[test]
fn proto_ingest_flush_and_ack_callback() {
    const TABLE: &str = "ffi.mux.proto";
    let (runtime, server, url) = start_server();
    let mut responses = vec![MockResponse::CreateStream {
        stream_id: "lane-0".into(),
        delay_ms: 0,
    }];
    responses.extend((0..4).map(|offset| MockResponse::RecordAck {
        ack_up_to_offset: offset,
        delay_ms: 0,
    }));
    runtime.block_on(server.inject_responses(TABLE, responses));

    let ack_log = Box::new(AckLog::default());
    let options = zerobus_get_default_config();

    let sdk = create_sdk(&url);
    let descriptor = create_test_descriptor_proto().unwrap().encode_to_vec();
    let mux = create_mux_with_ack(sdk, TABLE, Some(&descriptor), 1, &options, &ack_log);
    let mut result = new_result();

    let single = b"single";
    let single_id = zerobus_multiplexed_stream_ingest_proto_record(
        mux,
        single.as_ptr(),
        single.len(),
        &mut result,
    );
    expect_success(&mut result, "ingest_proto_record");

    let batch: [&[u8]; 2] = [b"batch-0", b"batch-1"];
    let ptrs: Vec<*const u8> = batch.iter().map(|r| r.as_ptr()).collect();
    let lens: Vec<usize> = batch.iter().map(|r| r.len()).collect();
    let batch_id = zerobus_multiplexed_stream_ingest_proto_records(
        mux,
        ptrs.as_ptr(),
        lens.as_ptr(),
        batch.len(),
        &mut result,
    );
    expect_success(&mut result, "ingest_proto_records");

    let nowait = b"nowait";
    zerobus_multiplexed_stream_ingest_proto_record_nowait(
        mux,
        nowait.as_ptr(),
        nowait.len(),
        &mut result,
    );
    expect_success(&mut result, "ingest_proto_record_nowait");

    let nowait_batch: [&[u8]; 2] = [b"nowait-batch-0", b"nowait-batch-1"];
    let nowait_ptrs: Vec<*const u8> = nowait_batch.iter().map(|r| r.as_ptr()).collect();
    let nowait_lens: Vec<usize> = nowait_batch.iter().map(|r| r.len()).collect();
    zerobus_multiplexed_stream_ingest_proto_records_nowait(
        mux,
        nowait_ptrs.as_ptr(),
        nowait_lens.as_ptr(),
        nowait_batch.len(),
        &mut result,
    );
    expect_success(&mut result, "ingest_proto_records_nowait");

    // Empty batches: non-NULL arrays with zero records.
    let empty_id = zerobus_multiplexed_stream_ingest_proto_records(
        mux,
        ptrs.as_ptr(),
        lens.as_ptr(),
        0,
        &mut result,
    );
    expect_success(&mut result, "ingest_proto_records (empty)");
    assert_eq!(empty_id, -2, "an empty batch returns -2, not a message ID");
    zerobus_multiplexed_stream_ingest_proto_records_nowait(
        mux,
        ptrs.as_ptr(),
        lens.as_ptr(),
        0,
        &mut result,
    );
    expect_success(&mut result, "ingest_proto_records_nowait (empty)");

    assert!(zerobus_multiplexed_stream_flush(mux, &mut result));
    expect_success(&mut result, "flush");
    assert_eq!(
        runtime.block_on(server.get_write_count()),
        6,
        "flush returns only after the _nowait records are sent"
    );
    assert!(!zerobus_multiplexed_stream_is_closed(mux));

    assert!(zerobus_multiplexed_stream_close(mux, &mut result));
    expect_success(&mut result, "close");
    assert!(zerobus_multiplexed_stream_is_closed(mux));

    let records = zerobus_multiplexed_stream_get_unacked_records(mux, &mut result);
    expect_success(&mut result, "get_unacked_records");
    assert_eq!(records.len, 0, "every record was acknowledged");
    zerobus_free_record_array(records);

    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);

    // Close drains the callback worker, so every ack has been delivered.
    let acks = ack_log.acks.lock().unwrap().clone();
    assert_eq!(acks.len(), 4, "one ack per non-empty ingest call: {acks:?}");
    assert!(acks.contains(&single_id) && acks.contains(&batch_id));
    assert!(ack_log.errors.lock().unwrap().is_empty());
    assert_eq!(runtime.block_on(server.get_write_count()), 6);
}

/// JSON records spread round-robin over two sub-streams. The server never
/// acknowledges, so every record comes back from `get_unacked_records`,
/// exactly once, with the bytes it was ingested with.
#[test]
fn json_round_robin_and_unacked_recovery() {
    const TABLE: &str = "ffi.mux.json";
    let (runtime, server, url) = start_server();

    let mut options = json_options();
    options.flush_timeout_ms = 500;

    let sdk = create_sdk(&url);
    let mux = create_mux(sdk, TABLE, None, 2, &options);
    let mut result = new_result();

    let blocking = json_records(0..4);
    let lanes: Vec<usize> = blocking
        .iter()
        .map(|json| {
            let id = zerobus_multiplexed_stream_ingest_json_record(mux, json.as_ptr(), &mut result);
            expect_success(&mut result, "ingest_json_record");
            stream_index(id)
        })
        .collect();
    assert_eq!(lanes, [0, 1, 0, 1], "records must alternate sub-streams");

    let batch = json_records(4..6);
    let batch_ptrs: Vec<*const c_char> = batch.iter().map(|s| s.as_ptr()).collect();
    zerobus_multiplexed_stream_ingest_json_records(
        mux,
        batch_ptrs.as_ptr(),
        batch_ptrs.len(),
        &mut result,
    );
    expect_success(&mut result, "ingest_json_records");

    // `_nowait` copies its input: the caller's buffers and pointer array are
    // overwritten as soon as each call returns.
    let nowait = json_records(6..9);
    let mut nowait_bufs: Vec<Vec<u8>> = nowait
        .iter()
        .map(|s| s.as_bytes_with_nul().to_vec())
        .collect();
    // Keeps the trailing NUL, so a late read would still see a valid string.
    let scrub = |buf: &mut Vec<u8>| {
        let len = buf.len();
        buf[..len - 1].fill(b'x');
    };
    let mut nowait_ptrs: Vec<*const c_char> = nowait_bufs[..2]
        .iter()
        .map(|b| b.as_ptr() as *const c_char)
        .collect();
    zerobus_multiplexed_stream_ingest_json_records_nowait(
        mux,
        nowait_ptrs.as_ptr(),
        nowait_ptrs.len(),
        &mut result,
    );
    expect_success(&mut result, "ingest_json_records_nowait");
    nowait_ptrs.fill(ptr::null());
    nowait_bufs[..2].iter_mut().for_each(scrub);

    zerobus_multiplexed_stream_ingest_json_record_nowait(
        mux,
        nowait_bufs[2].as_ptr() as *const c_char,
        &mut result,
    );
    expect_success(&mut result, "ingest_json_record_nowait");
    scrub(&mut nowait_bufs[2]);

    wait_for_writes(&runtime, &server, 9);

    let recovered = unacked_json_payloads(mux);
    assert!(zerobus_multiplexed_stream_is_closed(mux));
    assert_eq!(recovered.len(), 9, "each record returned exactly once");
    assert_eq!(
        recovered,
        sorted_strings(blocking.iter().chain(&batch).chain(&nowait))
    );

    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// The OAuth create mints a token from Unity Catalog and opens every lane.
#[test]
fn oauth_create_mints_token_and_opens_mux() {
    const TABLE: &str = "ffi.mux.oauth";
    let (runtime, server, url) = start_server();
    let (oauth, uc_url) = runtime.block_on(start_mock_oauth_server());
    runtime.block_on(oauth.set_responses(vec![MockTokenResponse::ok("token-1")]));
    // No CreateStream entry: the mock opens every lane and each lane acks its
    // offset 0.
    runtime.block_on(server.inject_responses(
        TABLE,
        vec![MockResponse::RecordAck {
            ack_up_to_offset: 0,
            delay_ms: 0,
        }],
    ));

    let sdk = create_sdk_with_uc(&url, &uc_url);
    let table = CString::new(TABLE).unwrap();
    let client_id = CString::new("client-id").unwrap();
    let client_secret = CString::new("client-secret").unwrap();
    let options = json_options();
    let mut result = new_result();
    let mux = zerobus_sdk_create_multiplexed_stream(
        sdk,
        table.as_ptr(),
        ptr::null(),
        0,
        client_id.as_ptr(),
        client_secret.as_ptr(),
        2,
        &options,
        None,
        None,
        ptr::null_mut(),
        None,
        &mut result,
    );
    expect_success(&mut result, "zerobus_sdk_create_multiplexed_stream");
    assert!(!mux.is_null());
    assert!(oauth.mint_count() >= 1, "the create must mint a token");

    for json in json_records(0..2) {
        zerobus_multiplexed_stream_ingest_json_record(mux, json.as_ptr(), &mut result);
        expect_success(&mut result, "ingest_json_record");
    }
    assert!(zerobus_multiplexed_stream_flush(mux, &mut result));
    expect_success(&mut result, "flush");
    assert!(zerobus_multiplexed_stream_close(mux, &mut result));
    expect_success(&mut result, "close");

    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// On success the headers provider's `user_data` is freed exactly once, after
/// the handle is freed and the last lane releases the provider.
#[test]
fn headers_user_data_freed_once_after_last_lane_releases_it() {
    const TABLE: &str = "ffi.mux.headers.free";
    let (_runtime, _server, url) = start_server();
    let provider = ProviderLog::leaked();

    let sdk = create_sdk(&url);
    let options = json_options();
    let (mux, mut result) = create_mux_raw(sdk, TABLE, None, 2, &options, Some(provider));
    expect_success(&mut result, "zerobus_sdk_create_multiplexed_stream");
    assert!(!mux.is_null());
    assert!(provider.calls() >= 2, "every lane fetches headers");
    assert_eq!(provider.frees(), 0, "an open mux still owns user_data");

    assert!(zerobus_multiplexed_stream_close(mux, &mut result));
    expect_success(&mut result, "close");
    assert_eq!(provider.frees(), 0, "the handle still owns user_data");

    zerobus_multiplexed_stream_free(mux);
    wait_until("user_data is freed", || provider.frees() >= 1);
    // Let a late second release land before asserting exactly once.
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(provider.frees(), 1, "user_data must be freed exactly once");
    zerobus_sdk_free(sdk);
}

/// One lane opens and shares the provider and the ack callbacks, the other
/// is rejected: the create fails, and both `user_data`s are still freed
/// exactly once.
#[test]
fn partial_open_failure_frees_shared_headers_provider_once() {
    const TABLE: &str = "ffi.mux.headers.partial";
    let (runtime, server, url) = start_server();
    runtime.block_on(server.inject_responses(
        TABLE,
        vec![
            MockResponse::CreateStream {
                stream_id: "lane-ok".into(),
                delay_ms: 0,
            },
            MockResponse::Error {
                status: Status::permission_denied("denied"),
                delay_ms: 0,
            },
        ],
    ));
    let provider = ProviderLog::leaked();
    let ack = AckOwner::leaked();

    let sdk = create_sdk(&url);
    let options = json_options_terminal();
    let (mux, mut result) = create_mux_raw_with_mux_ack(
        sdk,
        TABLE,
        None,
        2,
        &options,
        Some(provider),
        MuxAck::owned(ack, true),
    );
    assert!(mux.is_null(), "a partially opened mux must not be returned");
    expect_permanent_failure(&mut result, "zerobus_sdk_create_multiplexed_stream");

    wait_until("user_data is freed", || provider.frees() >= 1);
    // Let a late second release land before asserting exactly once.
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(provider.frees(), 1, "user_data must be freed exactly once");
    ack.assert_freed_once();
    zerobus_sdk_free(sdk);
}

/// A `_nowait` task keeps its own reference: freeing the handle while the
/// task waits for capacity doesn't release the mux (and the provider's
/// `user_data`) until the task ends.
#[test]
fn free_with_pending_nowait_keeps_mux_alive_until_task_ends() {
    const TABLE: &str = "ffi.mux.free.pending";
    let (runtime, server, url) = start_server();
    let gate = Arc::new(MockResponseGate::new());
    runtime.block_on(server.inject_responses(
        TABLE,
        vec![
            MockResponse::CreateStream {
                stream_id: "lane-0".into(),
                delay_ms: 0,
            },
            MockResponse::GatedRecordAck {
                ack_up_to_offset: 0,
                gate: Arc::clone(&gate),
            },
            MockResponse::RecordAck {
                ack_up_to_offset: 1,
                delay_ms: 0,
            },
        ],
    ));
    let provider = ProviderLog::leaked();

    let sdk = create_sdk(&url);
    let mut options = json_options();
    options.max_inflight_requests = 1;
    let (mux, mut result) = create_mux_raw(sdk, TABLE, None, 1, &options, Some(provider));
    expect_success(&mut result, "zerobus_sdk_create_multiplexed_stream");

    let [r0, r1]: [CString; 2] = json_records(0..2).try_into().unwrap();
    zerobus_multiplexed_stream_ingest_json_record(mux, r0.as_ptr(), &mut result);
    expect_success(&mut result, "ingest_json_record");
    wait_for_writes(&runtime, &server, 1);

    // r0's ack is gated, so with one in-flight slot r1 waits for capacity.
    zerobus_multiplexed_stream_ingest_json_record_nowait(mux, r1.as_ptr(), &mut result);
    expect_success(&mut result, "ingest_json_record_nowait");
    zerobus_multiplexed_stream_free(mux);
    // Teardown would release the provider asynchronously, so give a wrongly
    // dropped mux time to do it before checking that it didn't.
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(provider.frees(), 0, "the pending task keeps the mux alive");
    assert_eq!(runtime.block_on(server.get_write_count()), 1);

    gate.release();
    wait_until("the task's reference releases the provider", || {
        provider.frees() >= 1
    });
    // Let a late second release land before asserting exactly once.
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(provider.frees(), 1, "user_data must be freed exactly once");
    zerobus_sdk_free(sdk);
}

/// NULL `options` selects the defaults (protobuf). A NULL element with a
/// nonzero length fails the whole batch, and none of it is sent.
#[test]
fn null_options_default_to_proto_and_null_element_rejects_whole_batch() {
    const TABLE: &str = "ffi.mux.proto.defaults";
    let (runtime, server, url) = start_server();
    runtime.block_on(server.inject_responses(
        TABLE,
        vec![
            MockResponse::CreateStream {
                stream_id: "lane-0".into(),
                delay_ms: 0,
            },
            MockResponse::RecordAck {
                ack_up_to_offset: 0,
                delay_ms: 0,
            },
        ],
    ));

    let sdk = create_sdk(&url);
    let descriptor = create_test_descriptor_proto().unwrap().encode_to_vec();
    let (mux, mut result) = create_mux_raw(sdk, TABLE, Some(&descriptor), 1, ptr::null(), None);
    expect_success(&mut result, "zerobus_sdk_create_multiplexed_stream");
    assert!(!mux.is_null());

    let record = b"proto";
    zerobus_multiplexed_stream_ingest_proto_record(mux, record.as_ptr(), record.len(), &mut result);
    expect_success(&mut result, "ingest_proto_record");
    assert!(zerobus_multiplexed_stream_flush(mux, &mut result));
    expect_success(&mut result, "flush");

    let valid = b"valid";
    let ptrs = [valid.as_ptr(), ptr::null()];
    let lens = [valid.len(), 4];
    let id = zerobus_multiplexed_stream_ingest_proto_records(
        mux,
        ptrs.as_ptr(),
        lens.as_ptr(),
        2,
        &mut result,
    );
    assert_eq!(id, -1);
    let message = expect_permanent_failure(&mut result, "ingest_proto_records");
    assert_eq!(message, "Invalid data pointer");

    zerobus_multiplexed_stream_ingest_proto_records_nowait(
        mux,
        ptrs.as_ptr(),
        lens.as_ptr(),
        2,
        &mut result,
    );
    let message = expect_permanent_failure(&mut result, "ingest_proto_records_nowait");
    assert_eq!(message, "Invalid data pointer");
    assert!(!zerobus_multiplexed_stream_is_closed(mux));

    assert!(zerobus_multiplexed_stream_close(mux, &mut result));
    expect_success(&mut result, "close");
    let records = zerobus_multiplexed_stream_get_unacked_records(mux, &mut result);
    expect_success(&mut result, "get_unacked_records");
    assert_eq!(records.len, 0, "no element of a rejected batch is queued");
    zerobus_free_record_array(records);
    assert_eq!(
        runtime.block_on(server.get_write_count()),
        1,
        "no element of a rejected batch is sent"
    );

    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// With two lanes, ack callbacks and `wait_for_message_id` use message IDs
/// (lane plus sub-offset), not plain offsets.
#[test]
fn multi_lane_acks_and_waits_use_message_ids() {
    const TABLE: &str = "ffi.mux.message.ids";
    let (runtime, server, url) = start_server();
    // No CreateStream entry: the mock opens every lane and each lane acks only
    // its offset 0.
    runtime.block_on(server.inject_responses(
        TABLE,
        vec![MockResponse::RecordAck {
            ack_up_to_offset: 0,
            delay_ms: 0,
        }],
    ));

    let ack_log = Box::new(AckLog::default());
    let mut options = json_options();
    options.flush_timeout_ms = 500;

    let sdk = create_sdk(&url);
    let mux = create_mux_with_ack(sdk, TABLE, None, 2, &options, &ack_log);
    let mut result = new_result();

    let ids: Vec<i64> = json_records(0..4)
        .iter()
        .map(|json| {
            let id = zerobus_multiplexed_stream_ingest_json_record(mux, json.as_ptr(), &mut result);
            expect_success(&mut result, "ingest_json_record");
            id
        })
        .collect();
    let placement: Vec<(usize, i64)> = ids
        .iter()
        .map(|&id| (stream_index(id), sub_offset(id)))
        .collect();
    assert_eq!(placement, [(0, 0), (1, 0), (0, 1), (1, 1)]);

    assert!(zerobus_multiplexed_stream_wait_for_message_id(
        mux,
        ids[1],
        &mut result
    ));
    expect_success(&mut result, "wait_for_message_id (lane 1, offset 0)");

    // Lane 1 never acks offset 1, so this wait times out.
    assert!(!zerobus_multiplexed_stream_wait_for_message_id(
        mux,
        ids[3],
        &mut result
    ));
    expect_failure(&mut result, "wait_for_message_id (lane 1, offset 1)");

    // Lane 63 doesn't exist; its index sets the sign bit.
    assert!(!zerobus_multiplexed_stream_wait_for_message_id(
        mux,
        63i64 << 58,
        &mut result
    ));
    expect_permanent_failure(&mut result, "wait_for_message_id (lane 63)");

    // Close fails on the unacked records; only the callbacks matter here.
    zerobus_multiplexed_stream_close(mux, &mut result);
    take_message(&mut result);
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);

    let mut expected = vec![ids[0], ids[1]];
    expected.sort();
    assert_eq!(ack_log.sorted_acks(), expected);
}

/// An empty batch on an open mux returns -2. Once closed, the mux rejects
/// ingest (even an empty batch) and flush.
#[test]
fn empty_json_batch_returns_sentinel_and_closed_mux_rejects_work() {
    const TABLE: &str = "ffi.mux.closed";
    let (_runtime, _server, url) = start_server();
    let sdk = create_sdk(&url);
    let mux = create_mux(sdk, TABLE, None, 1, &json_options());
    let mut result = new_result();

    let json = CString::new(r#"{"id": 0}"#).unwrap();
    let ptrs = [json.as_ptr()];
    let id = zerobus_multiplexed_stream_ingest_json_records(mux, ptrs.as_ptr(), 0, &mut result);
    expect_success(&mut result, "ingest_json_records (empty)");
    assert_eq!(id, -2, "an empty batch returns -2, not a message ID");
    zerobus_multiplexed_stream_ingest_json_records_nowait(mux, ptrs.as_ptr(), 0, &mut result);
    expect_success(&mut result, "ingest_json_records_nowait (empty)");

    assert!(zerobus_multiplexed_stream_close(mux, &mut result));
    expect_success(&mut result, "close");

    let id = zerobus_multiplexed_stream_ingest_json_record(mux, json.as_ptr(), &mut result);
    assert_eq!(id, -1);
    expect_permanent_failure(&mut result, "ingest_json_record on a closed mux");

    // The closed check runs before the empty-batch check.
    let id = zerobus_multiplexed_stream_ingest_json_records(mux, ptrs.as_ptr(), 0, &mut result);
    assert_eq!(id, -1);
    expect_permanent_failure(&mut result, "ingest_json_records (empty) on a closed mux");

    assert!(!zerobus_multiplexed_stream_flush(mux, &mut result));
    expect_permanent_failure(&mut result, "flush on a closed mux");

    // `_nowait` calls report the closed mux instead of dropping the record.
    let closed = "Stream is in invalid state: MultiplexedStream is closed";
    zerobus_multiplexed_stream_ingest_json_record_nowait(mux, json.as_ptr(), &mut result);
    let message = expect_permanent_failure(&mut result, "ingest_json_record_nowait");
    assert_eq!(message, closed);
    zerobus_multiplexed_stream_ingest_json_records_nowait(mux, ptrs.as_ptr(), 1, &mut result);
    let message = expect_permanent_failure(&mut result, "ingest_json_records_nowait");
    assert_eq!(message, closed);
    zerobus_multiplexed_stream_ingest_json_records_nowait(mux, ptrs.as_ptr(), 0, &mut result);
    let message = expect_permanent_failure(&mut result, "ingest_json_records_nowait (empty)");
    assert_eq!(message, closed);

    let proto = b"proto";
    let proto_ptrs = [proto.as_ptr()];
    let proto_lens = [proto.len()];
    zerobus_multiplexed_stream_ingest_proto_record_nowait(
        mux,
        proto.as_ptr(),
        proto.len(),
        &mut result,
    );
    let message = expect_permanent_failure(&mut result, "ingest_proto_record_nowait");
    assert_eq!(message, closed);
    zerobus_multiplexed_stream_ingest_proto_records_nowait(
        mux,
        proto_ptrs.as_ptr(),
        proto_lens.as_ptr(),
        1,
        &mut result,
    );
    let message = expect_permanent_failure(&mut result, "ingest_proto_records_nowait");
    assert_eq!(message, closed);

    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// Invalid input fails only the call that passed it: the mux stays open,
/// and no element of a rejected batch is queued or sent.
#[test]
fn argument_errors_fail_only_that_call() {
    const TABLE: &str = "ffi.mux.bad.args";
    let (runtime, server, url) = start_server();
    runtime.block_on(server.inject_responses(
        TABLE,
        vec![
            MockResponse::CreateStream {
                stream_id: "lane-0".into(),
                delay_ms: 0,
            },
            MockResponse::RecordAck {
                ack_up_to_offset: 0,
                delay_ms: 0,
            },
        ],
    ));

    let ack_log = Box::new(AckLog::default());
    let options = json_options();
    let sdk = create_sdk(&url);
    let mux = create_mux_with_ack(sdk, TABLE, None, 1, &options, &ack_log);
    let mut result = new_result();

    let rejected = CString::new(r#"{"rejected": true}"#).unwrap();
    let invalid_utf8 = c"\xff".as_ptr();
    let bad_records: [(&str, *const c_char); 2] =
        [("NULL", ptr::null()), ("invalid UTF-8", invalid_utf8)];
    for (case, bad) in bad_records {
        let id = zerobus_multiplexed_stream_ingest_json_record(mux, bad, &mut result);
        assert_eq!(id, -1);
        expect_permanent_failure(&mut result, &format!("ingest_json_record ({case})"));

        let batch = [rejected.as_ptr(), bad];
        let id =
            zerobus_multiplexed_stream_ingest_json_records(mux, batch.as_ptr(), 2, &mut result);
        assert_eq!(id, -1);
        expect_permanent_failure(&mut result, &format!("ingest_json_records ({case})"));

        zerobus_multiplexed_stream_ingest_json_record_nowait(mux, bad, &mut result);
        expect_permanent_failure(&mut result, &format!("ingest_json_record_nowait ({case})"));

        zerobus_multiplexed_stream_ingest_json_records_nowait(mux, batch.as_ptr(), 2, &mut result);
        expect_permanent_failure(&mut result, &format!("ingest_json_records_nowait ({case})"));
    }

    // Rejected by the core rather than the FFI: wrong record type for the stream.
    let proto = b"proto";
    let id = zerobus_multiplexed_stream_ingest_proto_record(
        mux,
        proto.as_ptr(),
        proto.len(),
        &mut result,
    );
    assert_eq!(id, -1);
    expect_permanent_failure(&mut result, "ingest_proto_record on a JSON mux");
    assert!(!zerobus_multiplexed_stream_is_closed(mux));

    let valid = CString::new(r#"{"id": 0}"#).unwrap();
    let valid_id = zerobus_multiplexed_stream_ingest_json_record(mux, valid.as_ptr(), &mut result);
    expect_success(&mut result, "ingest_json_record");
    assert!(zerobus_multiplexed_stream_flush(mux, &mut result));
    expect_success(&mut result, "flush");

    assert!(unacked_json_payloads(mux).is_empty());
    assert_eq!(
        runtime.block_on(server.get_write_count()),
        1,
        "only the valid record reaches the server"
    );
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
    assert_eq!(ack_log.sorted_acks(), [valid_id]);
}

/// A terminal sub-stream failure surfaces from flush, closes the mux, makes
/// close report the cached failure, and leaves the record recoverable.
#[test]
fn sub_stream_failure_closes_mux_and_keeps_records_recoverable() {
    const TABLE: &str = "ffi.mux.failure";
    let (runtime, server, url) = start_server();
    runtime.block_on(server.inject_responses(
        TABLE,
        vec![
            MockResponse::CreateStream {
                stream_id: "lane-0".into(),
                delay_ms: 0,
            },
            MockResponse::Error {
                status: Status::permission_denied("denied"),
                delay_ms: 0,
            },
        ],
    ));

    let sdk = create_sdk(&url);
    let mux = create_mux(sdk, TABLE, None, 1, &json_options_terminal());
    let mut result = new_result();

    let record = CString::new(r#"{"id": 0}"#).unwrap();
    zerobus_multiplexed_stream_ingest_json_record(mux, record.as_ptr(), &mut result);
    expect_success(&mut result, "ingest_json_record");

    assert!(!zerobus_multiplexed_stream_flush(mux, &mut result));
    expect_permanent_failure(&mut result, "flush after a sub-stream failure");
    assert!(zerobus_multiplexed_stream_is_closed(mux));

    let later = CString::new(r#"{"id": 1}"#).unwrap();
    let id = zerobus_multiplexed_stream_ingest_json_record(mux, later.as_ptr(), &mut result);
    assert_eq!(id, -1);
    expect_failure(&mut result, "ingest_json_record after a sub-stream failure");
    zerobus_multiplexed_stream_ingest_json_record_nowait(mux, later.as_ptr(), &mut result);
    let message = expect_permanent_failure(
        &mut result,
        "ingest_json_record_nowait after a sub-stream failure",
    );
    assert!(
        message.contains("MultiplexedStream is closed"),
        "unexpected error: {message}"
    );

    assert!(!zerobus_multiplexed_stream_close(mux, &mut result));
    expect_permanent_failure(&mut result, "close after a sub-stream failure");

    assert_eq!(unacked_json_payloads(mux), sorted_strings([&record]));
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// Many C threads may ingest through one handle at the same time.
#[test]
fn concurrent_callers_share_one_handle() {
    const TABLE: &str = "ffi.mux.concurrent";
    const THREADS: usize = 4;
    const PER_THREAD: usize = 25;
    let (_runtime, _server, url) = start_server();

    let mut options = json_options();
    options.flush_timeout_ms = 500;
    let sdk = create_sdk(&url);
    let mux = create_mux(sdk, TABLE, None, 2, &options);
    // Raw pointers aren't `Send`; the C contract allows sharing the handle.
    let mux_addr = mux as usize;

    let barrier = Arc::new(Barrier::new(THREADS));
    let workers: Vec<_> = (0..THREADS)
        .map(|t| {
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let mux = mux_addr as *mut CZerobusMultiplexedStream;
                let records = json_records(t * PER_THREAD..(t + 1) * PER_THREAD);
                let mut result = new_result();
                barrier.wait();
                records
                    .iter()
                    .map(|json| {
                        let id = zerobus_multiplexed_stream_ingest_json_record(
                            mux,
                            json.as_ptr(),
                            &mut result,
                        );
                        expect_success(&mut result, "ingest_json_record");
                        id
                    })
                    .collect::<Vec<i64>>()
            })
        })
        .collect();
    let ids: Vec<i64> = workers
        .into_iter()
        .flat_map(|worker| worker.join().unwrap())
        .collect();

    let total = THREADS * PER_THREAD;
    assert_eq!(ids.iter().collect::<HashSet<_>>().len(), total);
    let lanes: HashSet<usize> = ids.iter().map(|&id| stream_index(id)).collect();
    assert_eq!(lanes, HashSet::from([0, 1]), "both lanes are used");

    let expected = sorted_strings(&json_records(0..total));
    assert_eq!(unacked_json_payloads(mux), expected);
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// While a flush holds the handle, a close queues behind it and
/// `is_closed` turns true. A blocking or `_nowait` ingest that arrives after
/// that fails instead of being queued and lost.
#[test]
fn ingest_after_queued_close_fails_instead_of_being_lost() {
    const TABLE: &str = "ffi.mux.close.queued";
    let (runtime, server, url) = start_server();
    let gate = Arc::new(MockResponseGate::new());
    runtime.block_on(server.inject_responses(
        TABLE,
        vec![
            MockResponse::CreateStream {
                stream_id: "lane-0".into(),
                delay_ms: 0,
            },
            MockResponse::GatedRecordAck {
                ack_up_to_offset: 0,
                gate: Arc::clone(&gate),
            },
        ],
    ));

    let sdk = create_sdk(&url);
    let mux = create_mux(sdk, TABLE, None, 1, &json_options());
    let mux_addr = mux as usize;
    let mut result = new_result();

    let r0 = CString::new(r#"{"id": 0}"#).unwrap();
    zerobus_multiplexed_stream_ingest_json_record(mux, r0.as_ptr(), &mut result);
    expect_success(&mut result, "ingest_json_record");
    wait_for_writes(&runtime, &server, 1);

    // The flush waits on the gated ack while holding the handle.
    let flusher = std::thread::spawn(move || {
        let mut result = new_result();
        zerobus_multiplexed_stream_flush(mux_addr as *mut _, &mut result);
        take_message(&mut result);
    });
    assert!(!zerobus_multiplexed_stream_is_closed(mux));

    let closer = std::thread::spawn(move || {
        let mut result = new_result();
        let closed = zerobus_multiplexed_stream_close(mux_addr as *mut _, &mut result);
        expect_success(&mut result, "close");
        closed
    });
    wait_until("close is queued", || {
        zerobus_multiplexed_stream_is_closed(mux)
    });

    // The gate is still closed, so close can't have finished yet.
    let r1 = CString::new(r#"{"id": 1}"#).unwrap();
    zerobus_multiplexed_stream_ingest_json_record_nowait(mux, r1.as_ptr(), &mut result);
    let message = expect_permanent_failure(
        &mut result,
        "ingest_json_record_nowait after close was queued",
    );
    assert_eq!(
        message,
        "Stream is in invalid state: MultiplexedStream is closing"
    );

    // Only decides whether the ingest below waits behind the close or finds
    // the mux already closed; both must fail.
    let releaser = {
        let gate = Arc::clone(&gate);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            gate.release();
        })
    };

    let r2 = CString::new(r#"{"id": 2}"#).unwrap();
    let id = zerobus_multiplexed_stream_ingest_json_record(mux, r2.as_ptr(), &mut result);
    assert_eq!(id, -1);
    expect_permanent_failure(&mut result, "ingest_json_record after close was queued");

    releaser.join().unwrap();
    flusher.join().unwrap();
    assert!(closer.join().unwrap());

    assert!(unacked_json_payloads(mux).is_empty());
    assert_eq!(
        runtime.block_on(server.get_write_count()),
        1,
        "the rejected records never reach the server"
    );
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// Every `_nowait` record accepted while the mux is open is sent and
/// covered by a close issued right after it, never silently dropped.
#[test]
fn nowait_records_accepted_before_close_are_never_lost() {
    const TABLE: &str = "ffi.mux.nowait.close";
    const RECORDS: usize = 200;
    let (runtime, server, url) = start_server();

    let mut options = json_options();
    options.flush_timeout_ms = 500;
    let sdk = create_sdk(&url);
    let mux = create_mux(sdk, TABLE, None, 1, &options);
    let mut result = new_result();

    let records = json_records(0..RECORDS);
    let accepted: Vec<&CString> = records
        .iter()
        .filter(|json| {
            zerobus_multiplexed_stream_ingest_json_record_nowait(mux, json.as_ptr(), &mut result);
            let accepted = result.success;
            take_message(&mut result);
            accepted
        })
        .collect();
    assert_eq!(accepted.len(), RECORDS, "an open mux accepts every record");

    // The server never acks, so close fails on the flush timeout; only what
    // it covers matters here.
    zerobus_multiplexed_stream_close(mux, &mut result);
    take_message(&mut result);
    wait_for_writes(&runtime, &server, RECORDS as u64);
    assert_eq!(unacked_json_payloads(mux), sorted_strings(accepted));

    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// A `_nowait` call returns at once while another call (here a flush) holds
/// the handle, and its record is still sent and acknowledged.
#[test]
fn nowait_does_not_wait_for_in_flight_flush() {
    const TABLE: &str = "ffi.mux.nowait.fast";
    let (runtime, server, url) = start_server();
    let gate = Arc::new(MockResponseGate::new());
    runtime.block_on(server.inject_responses(
        TABLE,
        vec![
            MockResponse::CreateStream {
                stream_id: "lane-0".into(),
                delay_ms: 0,
            },
            MockResponse::GatedRecordAck {
                ack_up_to_offset: 0,
                gate: Arc::clone(&gate),
            },
            MockResponse::RecordAck {
                ack_up_to_offset: 1,
                delay_ms: 0,
            },
        ],
    ));

    let ack_log = Box::new(AckLog::default());
    let options = json_options();
    let sdk = create_sdk(&url);
    let mux = create_mux_with_ack(sdk, TABLE, None, 1, &options, &ack_log);
    let mux_addr = mux as usize;
    let mut result = new_result();

    let r0 = CString::new(r#"{"id": 0}"#).unwrap();
    let r0_id = zerobus_multiplexed_stream_ingest_json_record(mux, r0.as_ptr(), &mut result);
    expect_success(&mut result, "ingest_json_record");
    wait_for_writes(&runtime, &server, 1);

    // The flush waits on the gated ack while holding the handle.
    let flusher = std::thread::spawn(move || {
        let mut result = new_result();
        let flushed = zerobus_multiplexed_stream_flush(mux_addr as *mut _, &mut result);
        expect_success(&mut result, "flush");
        flushed
    });
    // No hook shows when the flush holds the lock; give it time to, so the
    // `_nowait` call below runs against a held lock in nearly every run.
    std::thread::sleep(Duration::from_millis(100));

    // Releases the gate when told to, or after 5 s, so a `_nowait` call that
    // blocks on the flush fails the test instead of hanging it.
    let gate_released = Arc::new(AtomicBool::new(false));
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let releaser = {
        let gate = Arc::clone(&gate);
        let gate_released = Arc::clone(&gate_released);
        std::thread::spawn(move || {
            let _ = release_rx.recv_timeout(Duration::from_secs(5));
            gate_released.store(true, Ordering::SeqCst);
            gate.release();
        })
    };

    let r1 = CString::new(r#"{"id": 1}"#).unwrap();
    zerobus_multiplexed_stream_ingest_json_record_nowait(mux, r1.as_ptr(), &mut result);
    expect_success(&mut result, "ingest_json_record_nowait");
    assert!(
        !gate_released.load(Ordering::SeqCst),
        "_nowait returned only after the in-flight flush was unblocked"
    );

    release_tx.send(()).unwrap();
    releaser.join().unwrap();
    assert!(flusher.join().unwrap());
    wait_for_writes(&runtime, &server, 2);
    assert!(zerobus_multiplexed_stream_flush(mux, &mut result));
    expect_success(&mut result, "flush");
    assert!(zerobus_multiplexed_stream_close(mux, &mut result));
    expect_success(&mut result, "close");
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);

    let acks = ack_log.sorted_acks();
    assert_eq!(acks.len(), 2, "r0 and r1 are both acknowledged: {acks:?}");
    assert!(acks.contains(&r0_id));
}

/// A loop of `_nowait` calls followed by one flush: the flush waits for every
/// accepted record to be sent and acknowledged, without a close.
#[test]
fn flush_after_nowait_loop_covers_every_accepted_record() {
    const TABLE: &str = "ffi.mux.nowait.flush";
    const RECORDS: usize = 50;
    let (runtime, server, url) = start_server();
    let mut responses = vec![MockResponse::CreateStream {
        stream_id: "lane-0".into(),
        delay_ms: 0,
    }];
    responses.extend((0..RECORDS as i64).map(|offset| MockResponse::RecordAck {
        ack_up_to_offset: offset,
        delay_ms: 0,
    }));
    runtime.block_on(server.inject_responses(TABLE, responses));

    let ack_log = Box::new(AckLog::default());
    let sdk = create_sdk(&url);
    let mux = create_mux_with_ack(sdk, TABLE, None, 1, &json_options(), &ack_log);
    let mut result = new_result();

    for json in json_records(0..RECORDS) {
        zerobus_multiplexed_stream_ingest_json_record_nowait(mux, json.as_ptr(), &mut result);
        expect_success(&mut result, "ingest_json_record_nowait");
    }
    assert!(zerobus_multiplexed_stream_flush(mux, &mut result));
    expect_success(&mut result, "flush");
    assert_eq!(
        runtime.block_on(server.get_write_count()),
        RECORDS as u64,
        "flush returns only after every accepted record is sent"
    );

    wait_until("every record is acknowledged", || {
        ack_log.acks.lock().unwrap().len() >= RECORDS
    });
    // Let a late duplicate ack land before asserting exactly once.
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(ack_log.acks.lock().unwrap().len(), RECORDS);
    assert!(!zerobus_multiplexed_stream_is_closed(mux));

    assert!(unacked_json_payloads(mux).is_empty());
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// A flush waits for a `_nowait` record accepted before it, even while that
/// record is still waiting for in-flight capacity.
#[test]
fn flush_waits_for_nowait_blocked_on_capacity() {
    const TABLE: &str = "ffi.mux.nowait.capacity";
    let (runtime, server, url) = start_server();
    let gate = Arc::new(MockResponseGate::new());
    runtime.block_on(server.inject_responses(
        TABLE,
        vec![
            MockResponse::CreateStream {
                stream_id: "lane-0".into(),
                delay_ms: 0,
            },
            MockResponse::GatedRecordAck {
                ack_up_to_offset: 0,
                gate: Arc::clone(&gate),
            },
            MockResponse::RecordAck {
                ack_up_to_offset: 1,
                delay_ms: 0,
            },
        ],
    ));

    let ack_log = Box::new(AckLog::default());
    let mut options = json_options();
    options.max_inflight_requests = 1;
    let sdk = create_sdk(&url);
    let mux = create_mux_with_ack(sdk, TABLE, None, 1, &options, &ack_log);
    let mux_addr = mux as usize;
    let mut result = new_result();

    let [r0, r1]: [CString; 2] = json_records(0..2).try_into().unwrap();
    let r0_id = zerobus_multiplexed_stream_ingest_json_record(mux, r0.as_ptr(), &mut result);
    expect_success(&mut result, "ingest_json_record");
    wait_for_writes(&runtime, &server, 1);

    // r0's ack is gated, so with one in-flight slot r1 waits for capacity.
    zerobus_multiplexed_stream_ingest_json_record_nowait(mux, r1.as_ptr(), &mut result);
    expect_success(&mut result, "ingest_json_record_nowait");

    let (flushed, writes_at_return) = std::thread::scope(|scope| {
        let flusher = scope.spawn(|| {
            let mut result = new_result();
            let flushed = zerobus_multiplexed_stream_flush(mux_addr as *mut _, &mut result);
            expect_success(&mut result, "flush");
            (flushed, runtime.block_on(server.get_write_count()))
        });
        // No hook shows when the flush has started; give it time to, so r1 is
        // still waiting for capacity when it does.
        std::thread::sleep(Duration::from_millis(200));
        gate.release();
        flusher.join().unwrap()
    });
    assert!(flushed);
    assert_eq!(writes_at_return, 2, "flush returned before r1 was sent");

    wait_until("r0 and r1 are acknowledged", || {
        ack_log.acks.lock().unwrap().len() >= 2
    });
    let acks = ack_log.sorted_acks();
    assert_eq!(acks.len(), 2, "{acks:?}");
    assert!(acks.contains(&r0_id));

    assert!(zerobus_multiplexed_stream_close(mux, &mut result));
    expect_success(&mut result, "close");
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// A flush returns even while another thread keeps submitting `_nowait`
/// records, and every accepted record stays recoverable.
#[test]
fn flush_returns_under_concurrent_nowait_producer() {
    const TABLE: &str = "ffi.mux.nowait.producer";
    let (_runtime, _server, url) = start_server();

    let mut options = json_options();
    options.flush_timeout_ms = 1000;
    let sdk = create_sdk(&url);
    let mux = create_mux(sdk, TABLE, None, 1, &options);
    let mux_addr = mux as usize;

    let stop = Arc::new(AtomicBool::new(false));
    let produced = Arc::new(AtomicUsize::new(0));
    let producer = {
        let stop = Arc::clone(&stop);
        let produced = Arc::clone(&produced);
        std::thread::spawn(move || {
            let mux = mux_addr as *mut CZerobusMultiplexedStream;
            let mut result = new_result();
            let mut accepted = Vec::new();
            let mut i = 0;
            while !stop.load(Ordering::SeqCst) {
                let json = CString::new(format!(r#"{{"id": {i}}}"#)).unwrap();
                i += 1;
                zerobus_multiplexed_stream_ingest_json_record_nowait(
                    mux,
                    json.as_ptr(),
                    &mut result,
                );
                if result.success {
                    accepted.push(json.into_string().unwrap());
                }
                take_message(&mut result);
                produced.fetch_add(1, Ordering::SeqCst);
                // Paces the producer so the recovered set stays small.
                std::thread::sleep(Duration::from_millis(1));
            }
            accepted
        })
    };
    wait_until("the producer is running", || {
        produced.load(Ordering::SeqCst) > 0
    });

    // The server never acks, so the flush ends on its timeout; it must not
    // keep waiting for records submitted after it began.
    let (flushed_tx, flushed_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut result = new_result();
        zerobus_multiplexed_stream_flush(mux_addr as *mut _, &mut result);
        take_message(&mut result);
        let _ = flushed_tx.send(());
    });
    let flush_returned = flushed_rx.recv_timeout(Duration::from_secs(5)).is_ok();
    stop.store(true, Ordering::SeqCst);
    let mut accepted = producer.join().unwrap();
    assert!(flush_returned, "flush did not return within 5 s");

    accepted.sort();
    assert!(!accepted.is_empty());
    assert_eq!(unacked_json_payloads(mux), accepted);
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// The FFI owns the mux ack `user_data`: it frees it exactly once, only after
/// the handle is freed, and never calls back after that.
#[test]
fn ack_user_data_freed_once_after_last_callback() {
    const TABLE: &str = "ffi.mux.ack.free";
    let (runtime, server, url) = start_server();
    // No CreateStream entry: the mock opens every lane and each lane acks its
    // offset 0.
    runtime.block_on(server.inject_responses(
        TABLE,
        vec![MockResponse::RecordAck {
            ack_up_to_offset: 0,
            delay_ms: 0,
        }],
    ));
    let ack = AckOwner::leaked();

    let sdk = create_sdk(&url);
    let (mux, mut result) = create_mux_raw_with_mux_ack(
        sdk,
        TABLE,
        None,
        2,
        &json_options(),
        None,
        MuxAck::owned(ack, true),
    );
    expect_success(&mut result, "zerobus_sdk_create_multiplexed_stream");
    assert_eq!(ack.frees(), 0, "an open mux owns ack user_data");

    for json in json_records(0..2) {
        zerobus_multiplexed_stream_ingest_json_record(mux, json.as_ptr(), &mut result);
        expect_success(&mut result, "ingest_json_record");
    }
    assert!(zerobus_multiplexed_stream_flush(mux, &mut result));
    expect_success(&mut result, "flush");
    wait_until("both lanes ack", || ack.acks() >= 2);
    assert_eq!(ack.frees(), 0, "an open mux owns ack user_data");

    assert!(zerobus_multiplexed_stream_close(mux, &mut result));
    expect_success(&mut result, "close");
    ack.assert_not_freed("close");

    zerobus_multiplexed_stream_free(mux);
    ack.assert_freed_once();
    assert_eq!(ack.acks(), 2);
    zerobus_sdk_free(sdk);
}

/// With both callbacks NULL, `free_ack_user_data` is still called exactly
/// once: the FFI owns `ack_user_data` from the call on.
#[test]
fn ack_user_data_without_callbacks_is_still_freed_once() {
    const TABLE: &str = "ffi.mux.ack.none";
    let (_runtime, _server, url) = start_server();
    let ack = AckOwner::leaked();

    let sdk = create_sdk(&url);
    let (mux, mut result) = create_mux_raw_with_mux_ack(
        sdk,
        TABLE,
        None,
        1,
        &json_options(),
        None,
        MuxAck::owned(ack, false),
    );
    expect_success(&mut result, "zerobus_sdk_create_multiplexed_stream");
    ack.assert_freed_once();

    assert!(zerobus_multiplexed_stream_close(mux, &mut result));
    expect_success(&mut result, "close");
    zerobus_multiplexed_stream_free(mux);
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(ack.frees(), 1, "ack user_data must be freed exactly once");
    zerobus_sdk_free(sdk);
}

/// A `_nowait` task still waiting for capacity holds the ack callbacks, so
/// freeing the handle doesn't free the ack `user_data` until the task ends.
#[test]
fn free_with_pending_nowait_defers_ack_user_data_free() {
    const TABLE: &str = "ffi.mux.ack.pending";
    let (runtime, server, url) = start_server();
    let gate = Arc::new(MockResponseGate::new());
    runtime.block_on(server.inject_responses(
        TABLE,
        vec![
            MockResponse::CreateStream {
                stream_id: "lane-0".into(),
                delay_ms: 0,
            },
            MockResponse::GatedRecordAck {
                ack_up_to_offset: 0,
                gate: Arc::clone(&gate),
            },
            MockResponse::RecordAck {
                ack_up_to_offset: 1,
                delay_ms: 0,
            },
        ],
    ));
    let ack = AckOwner::leaked();

    let sdk = create_sdk(&url);
    let mut options = json_options();
    options.max_inflight_requests = 1;
    let (mux, mut result) = create_mux_raw_with_mux_ack(
        sdk,
        TABLE,
        None,
        1,
        &options,
        None,
        MuxAck::owned(ack, true),
    );
    expect_success(&mut result, "zerobus_sdk_create_multiplexed_stream");

    let [r0, r1]: [CString; 2] = json_records(0..2).try_into().unwrap();
    zerobus_multiplexed_stream_ingest_json_record(mux, r0.as_ptr(), &mut result);
    expect_success(&mut result, "ingest_json_record");
    wait_for_writes(&runtime, &server, 1);

    // r0's ack is gated, so with one in-flight slot r1 waits for capacity.
    zerobus_multiplexed_stream_ingest_json_record_nowait(mux, r1.as_ptr(), &mut result);
    expect_success(&mut result, "ingest_json_record_nowait");
    zerobus_multiplexed_stream_free(mux);
    ack.assert_not_freed("free with a pending _nowait");

    gate.release();
    ack.assert_freed_once();
    zerobus_sdk_free(sdk);
}

/// A callback still running after close gave up waiting for it keeps the ack
/// `user_data` alive: it is freed only once that callback returns.
#[test]
fn ack_user_data_free_waits_for_running_callback() {
    const TABLE: &str = "ffi.mux.ack.running";
    let (runtime, server, url) = start_server();
    runtime.block_on(server.inject_responses(
        TABLE,
        vec![
            MockResponse::CreateStream {
                stream_id: "lane-0".into(),
                delay_ms: 0,
            },
            MockResponse::RecordAck {
                ack_up_to_offset: 0,
                delay_ms: 0,
            },
        ],
    ));
    let ack = AckOwner::leaked();
    ack.block_callbacks.store(true, Ordering::SeqCst);

    let sdk = create_sdk(&url);
    let mut options = json_options();
    // Close stops waiting for callbacks after 50 ms.
    options.callback_max_wait_time_ms = 50;
    options.has_callback_max_wait_time_ms = true;
    let (mux, mut result) = create_mux_raw_with_mux_ack(
        sdk,
        TABLE,
        None,
        1,
        &options,
        None,
        MuxAck::owned(ack, true),
    );
    expect_success(&mut result, "zerobus_sdk_create_multiplexed_stream");

    let record = CString::new(r#"{"id": 0}"#).unwrap();
    zerobus_multiplexed_stream_ingest_json_record(mux, record.as_ptr(), &mut result);
    expect_success(&mut result, "ingest_json_record");
    wait_until("the ack callback is running", || {
        ack.callback_entered.load(Ordering::SeqCst)
    });

    zerobus_multiplexed_stream_close(mux, &mut result);
    take_message(&mut result);
    zerobus_multiplexed_stream_free(mux);
    ack.assert_not_freed("close and free with a callback still running");

    ack.release_callbacks.store(true, Ordering::SeqCst);
    ack.assert_freed_once();
    zerobus_sdk_free(sdk);
}

/// A flush waits for `_nowait` records accepted before it, but not for one
/// accepted while it is already running; the next flush covers that one.
#[test]
fn flush_does_not_wait_for_nowait_accepted_after_it_began() {
    const TABLE: &str = "ffi.mux.nowait.epoch";
    let (runtime, server, url) = start_server();
    let (gate_a, gate_b) = (
        Arc::new(MockResponseGate::new()),
        Arc::new(MockResponseGate::new()),
    );
    runtime.block_on(server.inject_responses(
        TABLE,
        vec![
            MockResponse::CreateStream {
                stream_id: "lane-0".into(),
                delay_ms: 0,
            },
            MockResponse::GatedRecordAck {
                ack_up_to_offset: 0,
                gate: Arc::clone(&gate_a),
            },
            MockResponse::GatedRecordAck {
                ack_up_to_offset: 1,
                gate: Arc::clone(&gate_b),
            },
            MockResponse::RecordAck {
                ack_up_to_offset: 2,
                delay_ms: 0,
            },
        ],
    ));

    let sdk = create_sdk(&url);
    let mut options = json_options();
    options.max_inflight_requests = 1;
    let mux = create_mux(sdk, TABLE, None, 1, &options);
    let mux_addr = mux as usize;
    let mut result = new_result();

    // r0 fills the only in-flight slot; its ack waits on gate A.
    let r0 = CString::new(r#"{"id": 0}"#).unwrap();
    zerobus_multiplexed_stream_ingest_json_record(mux, r0.as_ptr(), &mut result);
    expect_success(&mut result, "ingest_json_record (r0)");
    wait_for_writes(&runtime, &server, 1);

    // rw is a blocking ingest waiting for capacity, ahead of r1 below.
    let waiter = std::thread::spawn(move || {
        let rw = CString::new(r#"{"id": "w"}"#).unwrap();
        let mut result = new_result();
        zerobus_multiplexed_stream_ingest_json_record(mux_addr as *mut _, rw.as_ptr(), &mut result);
        expect_success(&mut result, "ingest_json_record (rw)");
    });
    std::thread::sleep(Duration::from_millis(100));

    let (flushed_tx, flushed_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut result = new_result();
        let flushed = zerobus_multiplexed_stream_flush(mux_addr as *mut _, &mut result);
        take_message(&mut result);
        let _ = flushed_tx.send(flushed);
    });
    // No hook shows that the flush has taken its snapshot of accepted
    // records. If r1 were accepted before it, a correct flush would wait for
    // r1 and the 2 s bound below would fail the test.
    std::thread::sleep(Duration::from_millis(500));

    let r1 = CString::new(r#"{"id": 1}"#).unwrap();
    zerobus_multiplexed_stream_ingest_json_record_nowait(mux, r1.as_ptr(), &mut result);
    expect_success(&mut result, "ingest_json_record_nowait (r1)");

    // rw takes the freed slot and its ack waits on gate B, so r1 stays pending.
    gate_a.release();
    let flushed = flushed_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("flush waited for a _nowait accepted after it began");
    assert!(flushed);
    // The flush waits only for r0, so rw may or may not be sent yet; r1 can't
    // be, since rw holds the only slot until gate B.
    assert!(
        runtime.block_on(server.get_write_count()) <= 2,
        "r1 must still be pending when the first flush returns"
    );
    waiter.join().unwrap();
    wait_for_writes(&runtime, &server, 2);
    assert_eq!(runtime.block_on(server.get_write_count()), 2);

    gate_b.release();
    assert!(zerobus_multiplexed_stream_flush(mux, &mut result));
    expect_success(&mut result, "second flush");
    assert_eq!(
        runtime.block_on(server.get_write_count()),
        3,
        "the second flush covers r1"
    );

    assert!(zerobus_multiplexed_stream_close(mux, &mut result));
    expect_success(&mut result, "close");
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// Core message for a record whose type doesn't match the stream.
const RECORD_TYPE_MISMATCH: &str = "Record type does not match stream configuration";

/// Runs a flush or close on a thread; returns `(returned, retryable,
/// message)`. Fails the test if the call doesn't return within 2 s.
fn call_within_2s(
    mux: *mut CZerobusMultiplexedStream,
    call: extern "C" fn(*mut CZerobusMultiplexedStream, *mut CResult) -> bool,
    what: &str,
) -> (bool, bool, String) {
    let mux_addr = mux as usize;
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut result = new_result();
        let returned = call(mux_addr as *mut _, &mut result);
        let retryable = result.is_retryable;
        let _ = tx.send((returned, retryable, take_message(&mut result)));
    });
    rx.recv_timeout(Duration::from_secs(2))
        .unwrap_or_else(|_| panic!("{what} did not return within 2 s"))
}

/// Asserts a flush or close reported the stored record-type failure.
fn expect_record_type_failure(outcome: (bool, bool, String), what: &str) {
    let (returned, retryable, message) = outcome;
    assert!(!returned, "{what} must report the stored _nowait failure");
    assert!(!retryable, "{what}: {message}");
    assert!(message.contains(RECORD_TYPE_MISMATCH), "{what}: {message}");
}

fn expect_returned_true(outcome: (bool, bool, String), what: &str) {
    let (returned, _, message) = outcome;
    assert!(returned, "{what} failed: {message}");
}

/// A JSON mux whose server acks offsets `0..acks`.
fn json_mux_with_acks(
    runtime: &Runtime,
    server: &MockZerobusServer,
    url: &str,
    table: &str,
    acks: i64,
) -> (*mut CZerobusSdk, *mut CZerobusMultiplexedStream) {
    let mut responses = vec![MockResponse::CreateStream {
        stream_id: "lane-0".into(),
        delay_ms: 0,
    }];
    responses.extend((0..acks).map(|offset| MockResponse::RecordAck {
        ack_up_to_offset: offset,
        delay_ms: 0,
    }));
    runtime.block_on(server.inject_responses(table, responses));
    let sdk = create_sdk(url);
    let mux = create_mux(sdk, table, None, 1, &json_options());
    (sdk, mux)
}

/// Queues a protobuf record on a JSON mux: accepted, then rejected by the
/// core inside the background task.
fn nowait_wrong_type(mux: *mut CZerobusMultiplexedStream) {
    let proto = b"proto";
    let mut result = new_result();
    zerobus_multiplexed_stream_ingest_proto_record_nowait(
        mux,
        proto.as_ptr(),
        proto.len(),
        &mut result,
    );
    expect_success(&mut result, "ingest_proto_record_nowait (wrong type)");
}

/// A background `_nowait` failure is reported once by the next flush, isn't
/// cleared by a later success, and a new failure is reported again.
#[test]
fn flush_reports_background_nowait_failure_once() {
    const TABLE: &str = "ffi.mux.nowait.error";
    let (runtime, server, url) = start_server();
    let (sdk, mux) = json_mux_with_acks(&runtime, &server, &url, TABLE, 2);
    let mut result = new_result();

    nowait_wrong_type(mux);
    let r0 = CString::new(r#"{"id": 0}"#).unwrap();
    zerobus_multiplexed_stream_ingest_json_record_nowait(mux, r0.as_ptr(), &mut result);
    expect_success(&mut result, "ingest_json_record_nowait");

    let flush = zerobus_multiplexed_stream_flush;
    expect_record_type_failure(call_within_2s(mux, flush, "flush"), "first flush");
    expect_returned_true(call_within_2s(mux, flush, "flush"), "second flush");
    assert!(!zerobus_multiplexed_stream_is_closed(mux));

    nowait_wrong_type(mux);
    expect_record_type_failure(
        call_within_2s(mux, flush, "flush"),
        "flush after a new failure",
    );

    let r1 = CString::new(r#"{"id": 1}"#).unwrap();
    zerobus_multiplexed_stream_ingest_json_record(mux, r1.as_ptr(), &mut result);
    expect_success(&mut result, "ingest_json_record");
    expect_returned_true(
        call_within_2s(mux, flush, "flush"),
        "flush after a valid ingest",
    );

    assert!(zerobus_multiplexed_stream_close(mux, &mut result));
    expect_success(&mut result, "close");
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// Several background failures before a flush are reported as one.
#[test]
fn flush_reports_several_nowait_failures_once() {
    const TABLE: &str = "ffi.mux.nowait.errors";
    let (runtime, server, url) = start_server();
    let (sdk, mux) = json_mux_with_acks(&runtime, &server, &url, TABLE, 0);

    nowait_wrong_type(mux);
    nowait_wrong_type(mux);
    let flush = zerobus_multiplexed_stream_flush;
    expect_record_type_failure(call_within_2s(mux, flush, "flush"), "first flush");
    expect_returned_true(call_within_2s(mux, flush, "flush"), "second flush");

    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// A batch `_nowait` that fails in the background is reported too.
#[test]
fn flush_reports_background_nowait_batch_failure() {
    const TABLE: &str = "ffi.mux.nowait.batch.error";
    let (runtime, server, url) = start_server();
    let (sdk, mux) = json_mux_with_acks(&runtime, &server, &url, TABLE, 0);

    let batch: [&[u8]; 2] = [b"proto-0", b"proto-1"];
    let ptrs: Vec<*const u8> = batch.iter().map(|r| r.as_ptr()).collect();
    let lens: Vec<usize> = batch.iter().map(|r| r.len()).collect();
    let mut result = new_result();
    zerobus_multiplexed_stream_ingest_proto_records_nowait(
        mux,
        ptrs.as_ptr(),
        lens.as_ptr(),
        2,
        &mut result,
    );
    expect_success(&mut result, "ingest_proto_records_nowait (wrong type)");
    expect_record_type_failure(
        call_within_2s(mux, zerobus_multiplexed_stream_flush, "flush"),
        "flush",
    );

    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// Close reports a background failure no flush has reported, and still
/// closes the mux.
#[test]
fn close_reports_unreported_nowait_failure() {
    const TABLE: &str = "ffi.mux.nowait.close.error";
    let (runtime, server, url) = start_server();
    let (sdk, mux) = json_mux_with_acks(&runtime, &server, &url, TABLE, 0);

    nowait_wrong_type(mux);
    expect_record_type_failure(
        call_within_2s(mux, zerobus_multiplexed_stream_close, "close"),
        "close",
    );
    assert!(zerobus_multiplexed_stream_is_closed(mux));
    let mut result = new_result();
    let records = zerobus_multiplexed_stream_get_unacked_records(mux, &mut result);
    expect_success(&mut result, "get_unacked_records");
    assert_eq!(records.len, 0, "the rejected record is not recoverable");
    zerobus_free_record_array(records);

    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// A flush or close that fails on its own reports its own error first. The
/// stored background failure is kept and reported by the next flush.
#[test]
fn own_flush_error_wins_over_stored_nowait_failure() {
    const TABLE: &str = "ffi.mux.nowait.precedence";
    let gated_mux = |table: &str| {
        let (runtime, server, url) = start_server();
        let gate = Arc::new(MockResponseGate::new());
        runtime.block_on(server.inject_responses(
            table,
            vec![
                MockResponse::CreateStream {
                    stream_id: "lane-0".into(),
                    delay_ms: 0,
                },
                MockResponse::GatedRecordAck {
                    ack_up_to_offset: 0,
                    gate: Arc::clone(&gate),
                },
            ],
        ));
        let mut options = json_options();
        options.flush_timeout_ms = 500;
        let sdk = create_sdk(&url);
        let mux = create_mux(sdk, table, None, 1, &options);

        // A stored failure, then r0, whose ack waits on the gate.
        nowait_wrong_type(mux);
        let r0 = CString::new(r#"{"id": 0}"#).unwrap();
        let mut result = new_result();
        let r0_id = zerobus_multiplexed_stream_ingest_json_record(mux, r0.as_ptr(), &mut result);
        expect_success(&mut result, "ingest_json_record (r0)");
        (runtime, server, sdk, mux, gate, r0_id)
    };

    let (_runtime, _server, sdk, mux, gate, r0_id) = gated_mux(TABLE);
    let flush = zerobus_multiplexed_stream_flush;
    let (returned, _, message) = call_within_2s(mux, flush, "flush");
    assert!(!returned, "the flush times out on r0");
    assert!(
        !message.contains(RECORD_TYPE_MISMATCH),
        "the flush's own timeout must win: {message}"
    );
    assert!(
        message.contains("timed out"),
        "the flush must fail on its own timeout: {message}"
    );

    gate.release();
    let mut result = new_result();
    assert!(zerobus_multiplexed_stream_wait_for_message_id(
        mux,
        r0_id,
        &mut result
    ));
    expect_success(&mut result, "wait_for_message_id (r0)");
    expect_record_type_failure(call_within_2s(mux, flush, "flush"), "second flush");
    expect_returned_true(call_within_2s(mux, flush, "flush"), "third flush");
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);

    // Close: its own timeout wins, and it still closes.
    let (_runtime, _server, sdk, mux, gate, _) = gated_mux(&format!("{TABLE}.close"));
    let (returned, _, message) = call_within_2s(mux, zerobus_multiplexed_stream_close, "close");
    assert!(!returned, "close times out on r0");
    assert!(
        !message.contains(RECORD_TYPE_MISMATCH),
        "close's own timeout must win: {message}"
    );
    assert!(
        message.contains("timed out"),
        "close must fail on its own timeout: {message}"
    );
    assert!(zerobus_multiplexed_stream_is_closed(mux));
    gate.release();
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// Two flushes racing after one background failure: exactly one reports it.
#[test]
fn concurrent_flushes_report_nowait_failure_once() {
    const TABLE: &str = "ffi.mux.nowait.race";
    let (runtime, server, url) = start_server();
    let (sdk, mux) = json_mux_with_acks(&runtime, &server, &url, TABLE, 0);
    let mux_addr = mux as usize;

    nowait_wrong_type(mux);
    let barrier = Arc::new(Barrier::new(2));
    let (tx, rx) = std::sync::mpsc::channel();
    for _ in 0..2 {
        let (barrier, tx) = (Arc::clone(&barrier), tx.clone());
        std::thread::spawn(move || {
            let mut result = new_result();
            barrier.wait();
            let flushed = zerobus_multiplexed_stream_flush(mux_addr as *mut _, &mut result);
            let _ = tx.send((flushed, take_message(&mut result)));
        });
    }
    let outcomes: Vec<(bool, String)> = (0..2)
        .map(|_| {
            rx.recv_timeout(Duration::from_secs(2))
                .expect("a flush did not return within 2 s")
        })
        .collect();
    let failures: Vec<&String> = outcomes
        .iter()
        .filter(|(flushed, _)| !flushed)
        .map(|(_, message)| message)
        .collect();
    assert_eq!(
        failures.len(),
        1,
        "exactly one flush reports it: {outcomes:?}"
    );
    assert!(
        failures[0].contains(RECORD_TYPE_MISMATCH),
        "{}",
        failures[0]
    );

    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// A lane failure that also fails a waiting `_nowait` keeps the lane's typed
/// cause: flush reports it as retryable, and the queued record stays
/// recoverable.
#[test]
fn lane_failure_cause_survives_failed_nowait() {
    const TABLE: &str = "ffi.mux.nowait.lane.down";
    let (runtime, server, url) = start_server();
    let gate = Arc::new(MockResponseGate::new());
    let sent = Arc::new(tokio::sync::Notify::new());
    runtime.block_on(server.inject_responses(
        TABLE,
        vec![
            MockResponse::CreateStream {
                stream_id: "lane-0".into(),
                delay_ms: 0,
            },
            MockResponse::GatedError {
                status: Status::unavailable("lane down"),
                gate: Arc::clone(&gate),
                sent: Arc::clone(&sent),
            },
        ],
    ));

    let sdk = create_sdk(&url);
    let mut options = json_options_terminal();
    options.max_inflight_requests = 1;
    let mux = create_mux(sdk, TABLE, None, 1, &options);
    let mut result = new_result();

    let [r0, r1]: [CString; 2] = json_records(0..2).try_into().unwrap();
    zerobus_multiplexed_stream_ingest_json_record(mux, r0.as_ptr(), &mut result);
    expect_success(&mut result, "ingest_json_record (r0)");
    wait_for_writes(&runtime, &server, 1);
    // r1 waits for the only in-flight slot.
    zerobus_multiplexed_stream_ingest_json_record_nowait(mux, r1.as_ptr(), &mut result);
    expect_success(&mut result, "ingest_json_record_nowait (r1)");

    // `sent` is signalled with notify_waiters, which keeps no permit, so
    // register before releasing the gate.
    runtime.block_on(async {
        let notified = sent.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        gate.release();
        tokio::time::timeout(Duration::from_secs(5), notified)
            .await
            .expect("the mock did not send the lane error");
    });

    let (flushed, retryable, message) =
        call_within_2s(mux, zerobus_multiplexed_stream_flush, "flush");
    assert!(!flushed);
    assert!(
        retryable,
        "an Unavailable lane failure is retryable: {message}"
    );
    assert!(message.contains("lane down"), "{message}");

    assert_eq!(unacked_json_payloads(mux), sorted_strings([&r0]));
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

// ---------------------------------------------------------------------------
// `_async` functions
// ---------------------------------------------------------------------------

/// One reported `_async` completion.
#[derive(Clone, Debug, Default)]
struct Completion {
    /// Message ID, bool as 0/1, or record count, depending on the call.
    value: i64,
    success: bool,
    retryable: bool,
    message: String,
    /// JSON payloads of a `get_unacked_records_async` result, sorted.
    payloads: Vec<String>,
    mux: usize,
}

/// `user_data` for `_async` callbacks: records every completion, and can hold
/// a callback until released.
#[derive(Default)]
struct AsyncSink {
    completions: Mutex<Vec<Completion>>,
    block: AtomicBool,
    entered: AtomicBool,
    release: AtomicBool,
}

impl AsyncSink {
    /// Leaked: a callback may run after the test body has moved on.
    fn leaked() -> &'static AsyncSink {
        Box::leak(Box::default())
    }

    fn user_data(&'static self) -> *mut c_void {
        self as *const AsyncSink as *mut c_void
    }

    fn count(&self) -> usize {
        self.completions.lock().unwrap().len()
    }

    /// Waits for `n` callbacks, then checks no extra one arrives.
    fn wait_for(&self, n: usize, what: &str) -> Vec<Completion> {
        wait_until(what, || self.count() >= n);
        // Let a late extra callback land before asserting the count.
        std::thread::sleep(Duration::from_millis(100));
        let completions = self.completions.lock().unwrap().clone();
        assert_eq!(completions.len(), n, "{what}: wrong number of callbacks");
        completions
    }

    fn one(&self, what: &str) -> Completion {
        self.wait_for(1, what).remove(0)
    }

    fn assert_never_called(&self, what: &str) {
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(self.count(), 0, "{what}: callback must not run");
    }

    fn push(user_data: *mut c_void, result: *const CResult, completion: Completion) {
        let sink = unsafe { &*(user_data as *const AsyncSink) };
        let result = unsafe { &*result };
        let message = if result.error_message.is_null() {
            String::new()
        } else {
            unsafe { CStr::from_ptr(result.error_message) }
                .to_string_lossy()
                .into_owned()
        };
        sink.completions.lock().unwrap().push(Completion {
            success: result.success,
            retryable: result.is_retryable,
            message,
            ..completion
        });
        if sink.block.load(Ordering::SeqCst) {
            sink.entered.store(true, Ordering::SeqCst);
            // Bounded, so a missing release fails the test instead of hanging it.
            let deadline = Instant::now() + Duration::from_secs(5);
            while !sink.release.load(Ordering::SeqCst) && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    }
}

extern "C" fn sink_offset(id: i64, result: *const CResult, user_data: *mut c_void) {
    let completion = Completion {
        value: id,
        ..Default::default()
    };
    AsyncSink::push(user_data, result, completion);
}

extern "C" fn sink_bool(value: bool, result: *const CResult, user_data: *mut c_void) {
    let completion = Completion {
        value: value as i64,
        ..Default::default()
    };
    AsyncSink::push(user_data, result, completion);
}

extern "C" fn sink_records(records: CRecordArray, result: *const CResult, user_data: *mut c_void) {
    let completion = Completion {
        value: records.len as i64,
        payloads: take_json_payloads(records),
        ..Default::default()
    };
    AsyncSink::push(user_data, result, completion);
}

extern "C" fn sink_create(
    mux: *mut CZerobusMultiplexedStream,
    result: *const CResult,
    user_data: *mut c_void,
) {
    let completion = Completion {
        mux: mux as usize,
        ..Default::default()
    };
    AsyncSink::push(user_data, result, completion);
}

/// The synchronous part of an `_async` call: `(returned, retryable, message)`.
fn sync_part(returned: bool, result: &mut CResult) -> (bool, bool, String) {
    let retryable = result.is_retryable;
    (returned, retryable, take_message(result))
}

fn json_record_async(
    mux: *mut CZerobusMultiplexedStream,
    json: &CString,
    sink: &'static AsyncSink,
) -> (bool, bool, String) {
    let mut result = new_result();
    let returned = zerobus_multiplexed_stream_ingest_json_record_async(
        mux,
        json.as_ptr(),
        sink_offset,
        sink.user_data(),
        &mut result,
    );
    sync_part(returned, &mut result)
}

fn flush_async(
    mux: *mut CZerobusMultiplexedStream,
    sink: &'static AsyncSink,
) -> (bool, bool, String) {
    let mut result = new_result();
    let returned =
        zerobus_multiplexed_stream_flush_async(mux, sink_bool, sink.user_data(), &mut result);
    sync_part(returned, &mut result)
}

fn close_async(
    mux: *mut CZerobusMultiplexedStream,
    sink: &'static AsyncSink,
) -> (bool, bool, String) {
    let mut result = new_result();
    let returned =
        zerobus_multiplexed_stream_close_async(mux, sink_bool, sink.user_data(), &mut result);
    sync_part(returned, &mut result)
}

fn get_unacked_async(
    mux: *mut CZerobusMultiplexedStream,
    sink: &'static AsyncSink,
) -> (bool, bool, String) {
    let mut result = new_result();
    let returned = zerobus_multiplexed_stream_get_unacked_records_async(
        mux,
        sink_records,
        sink.user_data(),
        &mut result,
    );
    sync_part(returned, &mut result)
}

fn wait_async(
    mux: *mut CZerobusMultiplexedStream,
    message_id: i64,
    sink: &'static AsyncSink,
) -> (bool, bool, String) {
    let mut result = new_result();
    let returned = zerobus_multiplexed_stream_wait_for_message_id_async(
        mux,
        message_id,
        sink_bool,
        sink.user_data(),
        &mut result,
    );
    sync_part(returned, &mut result)
}

type AsyncCall = Box<dyn Fn(&'static AsyncSink) -> (bool, bool, String)>;

/// The six `_async` calls that need the read lock: four ingests, a wait on
/// `message_id`, and a flush.
fn async_readers(
    mux: *mut CZerobusMultiplexedStream,
    message_id: i64,
) -> Vec<(&'static str, AsyncCall)> {
    let mux_addr = mux as usize;
    let mux = move || mux_addr as *mut CZerobusMultiplexedStream;
    let json = Arc::new(CString::new(r#"{"id": "async"}"#).unwrap());
    let proto: &'static [u8] = b"proto";
    let (json1, json2) = (Arc::clone(&json), Arc::clone(&json));
    vec![
        (
            "proto_record_async",
            Box::new(move |sink: &'static AsyncSink| {
                let mut result = new_result();
                let returned = zerobus_multiplexed_stream_ingest_proto_record_async(
                    mux(),
                    proto.as_ptr(),
                    proto.len(),
                    sink_offset,
                    sink.user_data(),
                    &mut result,
                );
                sync_part(returned, &mut result)
            }) as AsyncCall,
        ),
        (
            "json_record_async",
            Box::new(move |sink| json_record_async(mux(), &json1, sink)),
        ),
        (
            "proto_records_async",
            Box::new(move |sink| {
                let (ptrs, lens) = ([proto.as_ptr()], [proto.len()]);
                let mut result = new_result();
                let returned = zerobus_multiplexed_stream_ingest_proto_records_async(
                    mux(),
                    ptrs.as_ptr(),
                    lens.as_ptr(),
                    1,
                    sink_offset,
                    sink.user_data(),
                    &mut result,
                );
                sync_part(returned, &mut result)
            }),
        ),
        (
            "json_records_async",
            Box::new(move |sink| {
                let ptrs = [json2.as_ptr()];
                let mut result = new_result();
                let returned = zerobus_multiplexed_stream_ingest_json_records_async(
                    mux(),
                    ptrs.as_ptr(),
                    1,
                    sink_offset,
                    sink.user_data(),
                    &mut result,
                );
                sync_part(returned, &mut result)
            }),
        ),
        (
            "wait_for_message_id_async",
            Box::new(move |sink| wait_async(mux(), message_id, sink)),
        ),
        (
            "flush_async",
            Box::new(move |sink| flush_async(mux(), sink)),
        ),
    ]
}

/// A 1-lane JSON mux whose first record r0 is ingested and has its ack held
/// by `gate`; `then` follows the gated ack in the injected responses.
struct GatedMux {
    runtime: Runtime,
    server: MockZerobusServer,
    sdk: *mut CZerobusSdk,
    mux: *mut CZerobusMultiplexedStream,
    gate: Arc<MockResponseGate>,
    r0_id: i64,
}

impl GatedMux {
    fn new(
        table: &str,
        options: &CStreamConfigurationOptions,
        ack: MuxAck,
        then: Vec<MockResponse>,
    ) -> Self {
        let (runtime, server, url) = start_server();
        let gate = Arc::new(MockResponseGate::new());
        let mut responses = vec![
            MockResponse::CreateStream {
                stream_id: "lane-0".into(),
                delay_ms: 0,
            },
            MockResponse::GatedRecordAck {
                ack_up_to_offset: 0,
                gate: Arc::clone(&gate),
            },
        ];
        responses.extend(then);
        runtime.block_on(server.inject_responses(table, responses));
        let sdk = create_sdk(&url);
        let (mux, mut result) =
            create_mux_raw_with_mux_ack(sdk, table, None, 1, options, None, ack);
        expect_success(&mut result, "zerobus_sdk_create_multiplexed_stream");
        let r0 = CString::new(r#"{"id": 0}"#).unwrap();
        let r0_id = zerobus_multiplexed_stream_ingest_json_record(mux, r0.as_ptr(), &mut result);
        expect_success(&mut result, "ingest_json_record (r0)");
        wait_for_writes(&runtime, &server, 1);
        Self {
            runtime,
            server,
            sdk,
            mux,
            gate,
            r0_id,
        }
    }
}

/// Async creates return at once; the callback brings a working handle.
#[test]
fn async_creates_return_before_lanes_open() {
    let (runtime, server, url) = start_server();
    let (oauth, uc_url) = runtime.block_on(start_mock_oauth_server());
    runtime.block_on(oauth.set_responses(vec![MockTokenResponse::ok("token-1")]));
    let sdk = create_sdk_with_uc(&url, &uc_url);

    for use_oauth in [false, true] {
        let table = if use_oauth {
            "ffi.mux.async_oauth"
        } else {
            "ffi.mux.async_headers"
        };
        // Each lane's open takes a second; each lane acks its offset 0.
        runtime.block_on(server.inject_responses(
            table,
            vec![
                MockResponse::CreateStream {
                    stream_id: "lane".into(),
                    delay_ms: 1000,
                },
                MockResponse::RecordAck {
                    ack_up_to_offset: 0,
                    delay_ms: 0,
                },
            ],
        ));
        let ack_log: &'static AckLog = Box::leak(Box::default());
        let sink = AsyncSink::leaked();
        let options = json_options();
        let table_c = CString::new(table).unwrap();
        let ack = MuxAck::log(ack_log);
        let mut result = new_result();
        let started = Instant::now();
        let returned = if use_oauth {
            let (id, secret) = (CString::new("id").unwrap(), CString::new("secret").unwrap());
            zerobus_sdk_create_multiplexed_stream_async(
                sdk,
                table_c.as_ptr(),
                ptr::null(),
                0,
                id.as_ptr(),
                secret.as_ptr(),
                2,
                &options,
                ack.on_ack,
                ack.on_error,
                ack.user_data,
                ack.free_user_data,
                sink_create,
                sink.user_data(),
                &mut result,
            )
        } else {
            zerobus_sdk_create_multiplexed_stream_with_headers_provider_async(
                sdk,
                table_c.as_ptr(),
                ptr::null(),
                0,
                empty_headers,
                ptr::null_mut(),
                None,
                2,
                &options,
                ack.on_ack,
                ack.on_error,
                ack.user_data,
                ack.free_user_data,
                sink_create,
                sink.user_data(),
                &mut result,
            )
        };
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "{table}: create blocked"
        );
        expect_success(&mut result, table);
        assert!(returned);

        let created = sink.one(&format!("{table} create callback"));
        assert!(
            created.success && created.mux != 0,
            "{table}: {}",
            created.message
        );
        let mux = created.mux as *mut CZerobusMultiplexedStream;
        let mut ids: Vec<i64> = json_records(0..2)
            .iter()
            .map(|json| {
                let id =
                    zerobus_multiplexed_stream_ingest_json_record(mux, json.as_ptr(), &mut result);
                expect_success(&mut result, "ingest_json_record");
                id
            })
            .collect();
        assert!(zerobus_multiplexed_stream_flush(mux, &mut result));
        expect_success(&mut result, "flush");
        assert!(zerobus_multiplexed_stream_close(mux, &mut result));
        expect_success(&mut result, "close");
        zerobus_multiplexed_stream_free(mux);
        ids.sort();
        assert_eq!(
            ack_log.sorted_acks(),
            ids,
            "{table}: acks carry the returned IDs"
        );
    }
    zerobus_sdk_free(sdk);
}

/// Async ingests report distinct message IDs, copy their input before
/// returning, and report an empty batch as -2.
#[test]
fn async_json_ingest_reports_ids_and_copies_input() {
    const TABLE: &str = "ffi.mux.async.json";
    let (_runtime, _server, url) = start_server();
    let mut options = json_options();
    options.flush_timeout_ms = 500;
    let sdk = create_sdk(&url);
    let mux = create_mux(sdk, TABLE, None, 2, &options);
    let originals = json_records(0..4);
    let mut bufs: Vec<Vec<u8>> = originals
        .iter()
        .map(|s| s.as_bytes_with_nul().to_vec())
        .collect();
    let scrub = |buf: &mut Vec<u8>| {
        let len = buf.len();
        buf[..len - 1].fill(b'x');
    };
    let sinks: Vec<&'static AsyncSink> = (0..4).map(|_| AsyncSink::leaked()).collect();
    let mut result = new_result();

    for i in 0..2 {
        let returned = zerobus_multiplexed_stream_ingest_json_record_async(
            mux,
            bufs[i].as_ptr() as *const c_char,
            sink_offset,
            sinks[i].user_data(),
            &mut result,
        );
        expect_success(&mut result, "ingest_json_record_async");
        assert!(returned);
        scrub(&mut bufs[i]);
    }
    let mut ptrs: Vec<*const c_char> = bufs[2..]
        .iter()
        .map(|b| b.as_ptr() as *const c_char)
        .collect();
    let returned = zerobus_multiplexed_stream_ingest_json_records_async(
        mux,
        ptrs.as_ptr(),
        2,
        sink_offset,
        sinks[2].user_data(),
        &mut result,
    );
    expect_success(&mut result, "ingest_json_records_async");
    assert!(returned);
    ptrs.fill(ptr::null());
    bufs[2..].iter_mut().for_each(scrub);

    let empty_ptrs = [originals[0].as_ptr()];
    let returned = zerobus_multiplexed_stream_ingest_json_records_async(
        mux,
        empty_ptrs.as_ptr(),
        0,
        sink_offset,
        sinks[3].user_data(),
        &mut result,
    );
    expect_success(&mut result, "ingest_json_records_async (empty)");
    assert!(returned);

    let ids: Vec<i64> = sinks[..3]
        .iter()
        .map(|sink| {
            let done = sink.one("ingest callback");
            assert!(done.success, "{}", done.message);
            assert!(stream_index(done.value) < 2);
            done.value
        })
        .collect();
    assert_eq!(
        ids.iter().collect::<HashSet<_>>().len(),
        3,
        "distinct IDs: {ids:?}"
    );
    let empty = sinks[3].one("empty batch callback");
    assert!(empty.success && empty.value == -2, "{empty:?}");

    assert_eq!(unacked_json_payloads(mux), sorted_strings(&originals));
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// Returns the protobuf payloads of `records`, sorted, and frees the array.
fn take_proto_payloads(records: CRecordArray) -> Vec<Vec<u8>> {
    let mut payloads: Vec<Vec<u8>> = (0..records.len)
        .map(|i| {
            let record = unsafe { &*records.records.add(i) };
            assert!(!record.is_json);
            unsafe { std::slice::from_raw_parts(record.data, record.data_len) }.to_vec()
        })
        .collect();
    zerobus_free_record_array(records);
    payloads.sort();
    payloads
}

/// The protobuf async ingests copy their bytes, pointers and lengths.
#[test]
fn async_proto_ingest_reports_ids_and_copies_input() {
    const TABLE: &str = "ffi.mux.async.proto";
    let (_runtime, _server, url) = start_server();
    let mut options = zerobus_get_default_config();
    options.recovery = false;
    options.flush_timeout_ms = 500;
    let sdk = create_sdk(&url);
    let descriptor = create_test_descriptor_proto().unwrap().encode_to_vec();
    let mux = create_mux(sdk, TABLE, Some(&descriptor), 1, &options);
    let originals: Vec<Vec<u8>> = vec![vec![0x00, 0xff, 0x01], vec![0x7f], vec![0x10, 0x20]];
    let mut bufs = originals.clone();
    let sinks: Vec<&'static AsyncSink> = (0..3).map(|_| AsyncSink::leaked()).collect();
    let mut result = new_result();

    let returned = zerobus_multiplexed_stream_ingest_proto_record_async(
        mux,
        bufs[0].as_ptr(),
        bufs[0].len(),
        sink_offset,
        sinks[0].user_data(),
        &mut result,
    );
    expect_success(&mut result, "ingest_proto_record_async");
    assert!(returned);
    bufs[0].fill(0xee);

    let mut ptrs: Vec<*const u8> = bufs[1..].iter().map(|b| b.as_ptr()).collect();
    let mut lens: Vec<usize> = bufs[1..].iter().map(|b| b.len()).collect();
    let returned = zerobus_multiplexed_stream_ingest_proto_records_async(
        mux,
        ptrs.as_ptr(),
        lens.as_ptr(),
        2,
        sink_offset,
        sinks[1].user_data(),
        &mut result,
    );
    expect_success(&mut result, "ingest_proto_records_async");
    assert!(returned);
    ptrs.fill(ptr::null());
    lens.fill(0);
    bufs[1..].iter_mut().for_each(|b| b.fill(0xee));

    let returned = zerobus_multiplexed_stream_ingest_proto_records_async(
        mux,
        originals.as_ptr() as *const *const u8,
        [0usize].as_ptr(),
        0,
        sink_offset,
        sinks[2].user_data(),
        &mut result,
    );
    expect_success(&mut result, "ingest_proto_records_async (empty)");
    assert!(returned);

    let first = sinks[0].one("proto_record_async callback");
    let batch = sinks[1].one("proto_records_async callback");
    assert!(first.success && batch.success, "{first:?} {batch:?}");
    assert_ne!(first.value, batch.value);
    let empty = sinks[2].one("empty proto batch callback");
    assert!(empty.success && empty.value == -2, "{empty:?}");

    let records = zerobus_multiplexed_stream_get_unacked_records(mux, &mut result);
    expect_success(&mut result, "get_unacked_records");
    let mut expected = originals.clone();
    expected.sort();
    assert_eq!(take_proto_payloads(records), expected);
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// An async ingest returns while its record still waits for capacity.
#[test]
fn async_ingest_returns_while_blocked_on_capacity() {
    let mut options = json_options();
    options.max_inflight_requests = 1;
    let gated = GatedMux::new(
        "ffi.mux.async.capacity",
        &options,
        MuxAck::none(),
        vec![MockResponse::RecordAck {
            ack_up_to_offset: 1,
            delay_ms: 0,
        }],
    );

    // Releases the gate when told to, or after 5 s, so an async ingest that
    // blocks fails the test instead of hanging it.
    let released = Arc::new(AtomicBool::new(false));
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let releaser = {
        let (gate, released) = (Arc::clone(&gated.gate), Arc::clone(&released));
        std::thread::spawn(move || {
            let _ = release_rx.recv_timeout(Duration::from_secs(5));
            released.store(true, Ordering::SeqCst);
            gate.release();
        })
    };

    let sink = AsyncSink::leaked();
    let r1 = CString::new(r#"{"id": 1}"#).unwrap();
    let (returned, _, message) = json_record_async(gated.mux, &r1, sink);
    assert!(returned, "{message}");
    assert!(
        !released.load(Ordering::SeqCst),
        "the async ingest waited for capacity"
    );
    sink.assert_never_called("an ingest still waiting for capacity");

    release_tx.send(()).unwrap();
    releaser.join().unwrap();
    let done = sink.one("r1 callback");
    assert!(done.success, "{}", done.message);
    let mut result = new_result();
    assert!(zerobus_multiplexed_stream_close(gated.mux, &mut result));
    expect_success(&mut result, "close");
    zerobus_multiplexed_stream_free(gated.mux);
    zerobus_sdk_free(gated.sdk);
}

/// Arguments rejected before queueing return false, never call back, and
/// queue none of the batch.
#[test]
fn async_ingest_rejects_bad_input_without_callback() {
    const TABLE: &str = "ffi.mux.async.bad.args";
    let (runtime, server, url) = start_server();
    let (sdk, mux) = json_mux_with_acks(&runtime, &server, &url, TABLE, 1);
    let valid = CString::new(r#"{"id": 0}"#).unwrap();
    let invalid_utf8 = c"\xff".as_ptr();
    let proto = b"proto";
    let mut result = new_result();
    let mut sinks = Vec::new();

    let mut check = |what: &str, call: &dyn Fn(*mut c_void, &mut CResult) -> bool| {
        let sink = AsyncSink::leaked();
        let returned = call(sink.user_data(), &mut result);
        assert!(!returned, "{what} must be rejected");
        expect_permanent_failure(&mut result, what);
        sinks.push((what.to_string(), sink));
    };
    check("json_record_async(NULL)", &|ud, r| {
        zerobus_multiplexed_stream_ingest_json_record_async(mux, ptr::null(), sink_offset, ud, r)
    });
    for (case, bad) in [("NULL", ptr::null()), ("invalid UTF-8", invalid_utf8)] {
        let batch = [valid.as_ptr(), bad];
        check(&format!("json_records_async([valid, {case}])"), &|ud, r| {
            zerobus_multiplexed_stream_ingest_json_records_async(
                mux,
                batch.as_ptr(),
                2,
                sink_offset,
                ud,
                r,
            )
        });
    }
    let (ptrs, lens) = ([proto.as_ptr(), ptr::null()], [proto.len(), 4]);
    check("proto_records_async([valid, NULL])", &|ud, r| {
        zerobus_multiplexed_stream_ingest_proto_records_async(
            mux,
            ptrs.as_ptr(),
            lens.as_ptr(),
            2,
            sink_offset,
            ud,
            r,
        )
    });
    std::thread::sleep(Duration::from_millis(200));
    for (what, sink) in &sinks {
        assert_eq!(sink.count(), 0, "{what}: callback must not run");
    }

    let id = zerobus_multiplexed_stream_ingest_json_record(mux, valid.as_ptr(), &mut result);
    expect_success(&mut result, "ingest_json_record");
    assert!(zerobus_multiplexed_stream_flush(mux, &mut result));
    expect_success(&mut result, "flush");
    assert_eq!(
        runtime.block_on(server.get_write_count()),
        1,
        "only the valid record is sent"
    );
    assert!(unacked_json_payloads(mux).is_empty());
    assert!(id >= 0);
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// A wrong-type async ingest reports its error through its own callback,
/// once, and leaves nothing for the next flush.
#[test]
fn async_ingest_reports_wrong_type_per_call_not_on_flush() {
    const TABLE: &str = "ffi.mux.async.wrong.type";
    let (runtime, server, url) = start_server();
    let (sdk, mux) = json_mux_with_acks(&runtime, &server, &url, TABLE, 1);
    let proto = b"proto";
    let (single, batch) = (AsyncSink::leaked(), AsyncSink::leaked());
    let mut result = new_result();

    assert!(zerobus_multiplexed_stream_ingest_proto_record_async(
        mux,
        proto.as_ptr(),
        proto.len(),
        sink_offset,
        single.user_data(),
        &mut result,
    ));
    expect_success(&mut result, "ingest_proto_record_async");
    let (ptrs, lens) = ([proto.as_ptr()], [proto.len()]);
    assert!(zerobus_multiplexed_stream_ingest_proto_records_async(
        mux,
        ptrs.as_ptr(),
        lens.as_ptr(),
        1,
        sink_offset,
        batch.user_data(),
        &mut result,
    ));
    expect_success(&mut result, "ingest_proto_records_async");
    for (what, sink) in [
        ("proto_record_async", single),
        ("proto_records_async", batch),
    ] {
        let done = sink.one(what);
        assert!(
            done.value == -1 && !done.success && !done.retryable,
            "{what}: {done:?}"
        );
        assert!(
            done.message.contains(RECORD_TYPE_MISMATCH),
            "{what}: {}",
            done.message
        );
    }

    let valid = CString::new(r#"{"id": 0}"#).unwrap();
    zerobus_multiplexed_stream_ingest_json_record(mux, valid.as_ptr(), &mut result);
    expect_success(&mut result, "ingest_json_record");
    let flush = zerobus_multiplexed_stream_flush;
    expect_returned_true(
        call_within_2s(mux, flush, "flush"),
        "flush after per-call errors",
    );
    assert!(zerobus_multiplexed_stream_close(mux, &mut result));
    expect_success(&mut result, "close");
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// `wait_for_message_id_async` reports what the blocking wait would return.
#[test]
fn async_wait_reports_through_callback() {
    const TABLE: &str = "ffi.mux.async.wait";
    let (runtime, server, url) = start_server();
    // Each lane acks only its offset 0.
    runtime.block_on(server.inject_responses(
        TABLE,
        vec![MockResponse::RecordAck {
            ack_up_to_offset: 0,
            delay_ms: 0,
        }],
    ));
    let mut options = json_options();
    options.flush_timeout_ms = 500;
    let sdk = create_sdk(&url);
    let mux = create_mux(sdk, TABLE, None, 2, &options);
    let mut result = new_result();
    let ids: Vec<i64> = json_records(0..4)
        .iter()
        .map(|json| {
            let id = zerobus_multiplexed_stream_ingest_json_record(mux, json.as_ptr(), &mut result);
            expect_success(&mut result, "ingest_json_record");
            id
        })
        .collect();

    for (what, id, acked) in [
        ("lane 0 offset 0", ids[0], true),
        ("lane 1 offset 0", ids[1], true),
        ("lane 1 offset 1", ids[3], false),
    ] {
        let sink = AsyncSink::leaked();
        assert!(wait_async(mux, id, sink).0, "{what}");
        let done = sink.one(what);
        assert_eq!(done.value == 1, acked, "{what}: {done:?}");
        assert_eq!(done.success, acked, "{what}: {done:?}");
    }
    let sink = AsyncSink::leaked();
    assert!(wait_async(mux, 63i64 << 58, sink).0);
    let done = sink.one("lane 63");
    assert!(
        done.value == 0 && !done.success && !done.retryable,
        "{done:?}"
    );

    zerobus_multiplexed_stream_close(mux, &mut result);
    take_message(&mut result);
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// After a lane failure, async flush, close and get_unacked_records report
/// through their callbacks, in the documented recovery order.
#[test]
fn async_flush_close_and_unacked_report_lane_failure() {
    const TABLE: &str = "ffi.mux.async.failure";
    let (runtime, server, url) = start_server();
    runtime.block_on(server.inject_responses(
        TABLE,
        vec![
            MockResponse::CreateStream {
                stream_id: "lane-0".into(),
                delay_ms: 0,
            },
            MockResponse::Error {
                status: Status::permission_denied("denied"),
                delay_ms: 0,
            },
        ],
    ));
    let sdk = create_sdk(&url);
    let mux = create_mux(sdk, TABLE, None, 1, &json_options_terminal());
    let record = CString::new(r#"{"id": 0}"#).unwrap();
    let mut result = new_result();
    zerobus_multiplexed_stream_ingest_json_record(mux, record.as_ptr(), &mut result);
    expect_success(&mut result, "ingest_json_record");

    let sink = AsyncSink::leaked();
    assert!(flush_async(mux, sink).0);
    let done = sink.one("flush_async");
    assert!(
        done.value == 0 && !done.success && !done.retryable,
        "{done:?}"
    );

    let sink = AsyncSink::leaked();
    assert!(close_async(mux, sink).0);
    let done = sink.one("close_async");
    assert!(
        done.value == 0 && !done.success && !done.retryable,
        "{done:?}"
    );

    let sink = AsyncSink::leaked();
    assert!(get_unacked_async(mux, sink).0);
    let done = sink.one("get_unacked_records_async");
    assert!(done.success, "{}", done.message);
    assert_eq!(done.payloads, sorted_strings([&record]));
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// Every async ingest accepted before a close is covered by it.
#[test]
fn async_ingests_accepted_before_close_are_never_lost() {
    const TABLE: &str = "ffi.mux.async.close";
    const RECORDS: usize = 200;
    let (_runtime, _server, url) = start_server();
    let mut options = json_options();
    options.flush_timeout_ms = 500;
    let sdk = create_sdk(&url);
    let mux = create_mux(sdk, TABLE, None, 1, &options);

    let sink = AsyncSink::leaked();
    let records = json_records(0..RECORDS);
    for json in &records {
        let (returned, _, message) = json_record_async(mux, json, sink);
        assert!(returned, "{message}");
    }
    let mut result = new_result();
    zerobus_multiplexed_stream_close(mux, &mut result);
    take_message(&mut result);

    let completions = sink.wait_for(RECORDS, "every async ingest calls back");
    assert!(
        completions.iter().all(|c| c.success),
        "an accepted ingest failed"
    );
    assert_eq!(unacked_json_payloads(mux), sorted_strings(&records));
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// While close or get_unacked_records holds or waits for the write lock, the
/// async readers return true at once and report a closing error through
/// exactly one callback, before the writer finishes.
#[test]
fn async_calls_while_closing_report_via_callback() {
    for writer in ["close", "get_unacked_records"] {
        for queued in [true, false] {
            let what = format!("{writer}, {}", if queued { "queued" } else { "held" });
            let gated = GatedMux::new(
                "ffi.mux.async.closing",
                &json_options(),
                MuxAck::none(),
                vec![],
            );
            let mux_addr = gated.mux as usize;

            // Queued: a flush holds the read lock and the writer waits.
            // Held: the writer holds the lock, blocked in its own flush.
            let flusher = queued.then(|| {
                std::thread::spawn(move || {
                    let mut result = new_result();
                    zerobus_multiplexed_stream_flush(mux_addr as *mut _, &mut result);
                    take_message(&mut result);
                })
            });
            if queued {
                std::thread::sleep(Duration::from_millis(100));
            }
            // Returns the payloads get_unacked_records recovered (none for close).
            let writer_thread = std::thread::spawn(move || {
                let mut result = new_result();
                let mux = mux_addr as *mut CZerobusMultiplexedStream;
                let recovered = if writer == "close" {
                    zerobus_multiplexed_stream_close(mux, &mut result);
                    Vec::new()
                } else {
                    take_json_payloads(zerobus_multiplexed_stream_get_unacked_records(
                        mux,
                        &mut result,
                    ))
                };
                take_message(&mut result);
                recovered
            });
            wait_until("the writer takes or queues for the lock", || {
                zerobus_multiplexed_stream_is_closed(gated.mux)
            });

            for (call_name, call) in async_readers(gated.mux, gated.r0_id) {
                let sink = AsyncSink::leaked();
                let started = Instant::now();
                let (returned, _, message) = call(sink);
                assert!(
                    started.elapsed() < Duration::from_secs(1),
                    "{what}: {call_name} blocked"
                );
                assert!(returned, "{what}: {call_name} must return true: {message}");
                // The gate is still closed, so the writer has not finished.
                let completions = sink.wait_for(1, call_name);
                let completion = &completions[0];
                assert!(
                    !completion.success && !completion.retryable,
                    "{what}: {call_name}: {}",
                    completion.message
                );
                assert!(
                    completion.message.contains("MultiplexedStream is closing"),
                    "{what}: {call_name}: {}",
                    completion.message
                );
            }

            gated.gate.release();
            let recovered = writer_thread.join().unwrap();
            if let Some(flusher) = flusher {
                flusher.join().unwrap();
            }
            // Only r0 was ever sent (and acked); no rejected record is
            // recoverable.
            assert!(recovered.is_empty(), "{what}: {recovered:?}");
            assert!(unacked_json_payloads(gated.mux).is_empty(), "{what}");
            assert_eq!(
                gated.runtime.block_on(gated.server.get_write_count()),
                1,
                "{what}: a rejected record reached the server"
            );
            zerobus_multiplexed_stream_free(gated.mux);
            zerobus_sdk_free(gated.sdk);
        }
    }
}

/// With only a flush holding the read lock, the async readers return at once.
#[test]
fn async_calls_return_promptly_while_flush_holds_lock() {
    let mut options = json_options();
    options.flush_timeout_ms = 500;
    // The two JSON ingests get offsets 1 and 2 (the proto ones are rejected).
    let acks = (1..=2)
        .map(|offset| MockResponse::RecordAck {
            ack_up_to_offset: offset,
            delay_ms: 0,
        })
        .collect();
    let gated = GatedMux::new("ffi.mux.async.reader", &options, MuxAck::none(), acks);
    let mux_addr = gated.mux as usize;
    let flusher = std::thread::spawn(move || {
        let mut result = new_result();
        zerobus_multiplexed_stream_flush(mux_addr as *mut _, &mut result);
        take_message(&mut result);
    });
    // No hook shows when the flush holds the lock; give it time to.
    std::thread::sleep(Duration::from_millis(100));

    let mut sinks = Vec::new();
    for (call_name, call) in async_readers(gated.mux, gated.r0_id) {
        let sink = AsyncSink::leaked();
        let started = Instant::now();
        let (returned, _, message) = call(sink);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{call_name} blocked"
        );
        assert!(returned, "{call_name}: {message}");
        sinks.push((call_name, sink));
    }

    gated.gate.release();
    flusher.join().unwrap();
    let mut ids = HashSet::new();
    for (call_name, sink) in sinks {
        let done = sink.one(call_name);
        match call_name {
            // A protobuf record on the JSON mux, rejected in the task.
            "proto_record_async" | "proto_records_async" => {
                assert!(done.value == -1 && !done.success, "{call_name}: {done:?}");
                assert!(
                    done.message.contains(RECORD_TYPE_MISMATCH),
                    "{call_name}: {done:?}"
                );
            }
            "json_record_async" | "json_records_async" => {
                assert!(done.success, "{call_name}: {done:?}");
                assert!(
                    ids.insert(done.value),
                    "{call_name}: duplicate ID {}",
                    done.value
                );
            }
            // r0 and both JSON ingests are acked.
            _ => assert!(done.success && done.value == 1, "{call_name}: {done:?}"),
        }
    }
    let mut result = new_result();
    assert!(zerobus_multiplexed_stream_close(gated.mux, &mut result));
    expect_success(&mut result, "close");
    zerobus_multiplexed_stream_free(gated.mux);
    zerobus_sdk_free(gated.sdk);
}

/// close_async and get_unacked_records_async return at once while an async
/// ingest holds the read lock, and complete only after it does.
#[test]
fn async_close_waits_for_reader_without_blocking_caller() {
    for writer in ["close_async", "get_unacked_records_async"] {
        let mut options = json_options();
        options.max_inflight_requests = 1;
        options.flush_timeout_ms = 500;
        // The close variant acks r1; get_unacked_records leaves it unacked.
        let then = if writer == "close_async" {
            vec![MockResponse::RecordAck {
                ack_up_to_offset: 1,
                delay_ms: 0,
            }]
        } else {
            vec![]
        };
        let gated = GatedMux::new("ffi.mux.async.writer", &options, MuxAck::none(), then);

        // r1 waits for the only in-flight slot, holding the read lock.
        let (r1_sink, writer_sink) = (AsyncSink::leaked(), AsyncSink::leaked());
        let r1 = CString::new(r#"{"id": 1}"#).unwrap();
        assert!(json_record_async(gated.mux, &r1, r1_sink).0);
        let started = Instant::now();
        let (returned, _, message) = if writer == "close_async" {
            close_async(gated.mux, writer_sink)
        } else {
            get_unacked_async(gated.mux, writer_sink)
        };
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{writer} blocked the caller"
        );
        assert!(returned, "{writer}: {message}");
        writer_sink.assert_never_called(&format!("{writer} while r1 holds the lock"));

        gated.gate.release();
        let r1_done = r1_sink.one("r1 callback");
        assert!(r1_done.success, "{}", r1_done.message);
        let done = writer_sink.one(writer);
        if writer == "close_async" {
            assert!(done.success && done.value == 1, "{done:?}");
            wait_for_writes(&gated.runtime, &gated.server, 2);
        } else {
            assert!(done.success, "{}", done.message);
            assert_eq!(done.payloads, sorted_strings([&r1]));
        }
        zerobus_multiplexed_stream_free(gated.mux);
        zerobus_sdk_free(gated.sdk);
    }
}

/// close_async and get_unacked_records_async release the write lock before
/// their callback: a flush called while the callback runs isn't blocked.
#[test]
fn async_close_releases_write_guard_before_callback() {
    for writer in ["close_async", "get_unacked_records_async"] {
        let (_runtime, _server, url) = start_server();
        let sdk = create_sdk(&url);
        let mux = create_mux(sdk, "ffi.mux.async.guard", None, 1, &json_options());
        let sink = AsyncSink::leaked();
        sink.block.store(true, Ordering::SeqCst);
        let (returned, _, message) = if writer == "close_async" {
            close_async(mux, sink)
        } else {
            get_unacked_async(mux, sink)
        };
        assert!(returned, "{writer}: {message}");
        wait_until("the callback is running", || {
            sink.entered.load(Ordering::SeqCst)
        });

        let (flushed, _, message) = call_within_1s(mux, zerobus_multiplexed_stream_flush, "flush");
        assert!(!flushed, "{writer}: flush on a closed mux: {message}");

        sink.release.store(true, Ordering::SeqCst);
        zerobus_multiplexed_stream_free(mux);
        zerobus_sdk_free(sdk);
    }
}

/// Like `call_within_2s`, with a 1 s bound.
fn call_within_1s(
    mux: *mut CZerobusMultiplexedStream,
    call: extern "C" fn(*mut CZerobusMultiplexedStream, *mut CResult) -> bool,
    what: &str,
) -> (bool, bool, String) {
    let mux_addr = mux as usize;
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut result = new_result();
        let returned = call(mux_addr as *mut _, &mut result);
        let retryable = result.is_retryable;
        let _ = tx.send((returned, retryable, take_message(&mut result)));
    });
    rx.recv_timeout(Duration::from_secs(1))
        .unwrap_or_else(|_| panic!("{what} did not return within 1 s"))
}

/// A pending async flush, close or get_unacked_records keeps working after
/// the handle is freed, and calls back once with the right result.
#[test]
fn in_flight_async_op_keeps_handle_valid_across_free() {
    for op in ["flush_async", "close_async", "get_unacked_records_async"] {
        let gated = GatedMux::new(
            "ffi.mux.async.free",
            &json_options(),
            MuxAck::none(),
            vec![],
        );
        let sink = AsyncSink::leaked();
        let (returned, _, message) = match op {
            "flush_async" => flush_async(gated.mux, sink),
            "close_async" => close_async(gated.mux, sink),
            _ => get_unacked_async(gated.mux, sink),
        };
        assert!(returned, "{op}: {message}");
        zerobus_multiplexed_stream_free(gated.mux);

        gated.gate.release();
        let done = sink.one(op);
        assert!(done.success, "{op}: {}", done.message);
        match op {
            "get_unacked_records_async" => assert!(done.payloads.is_empty(), "{done:?}"),
            _ => assert_eq!(done.value, 1, "{op}: {done:?}"),
        }
        zerobus_sdk_free(gated.sdk);
    }
}

/// Async callbacks fire exactly once each when many threads ingest at once.
#[test]
fn async_callbacks_fire_once_each_under_concurrency() {
    const THREADS: usize = 4;
    const PER_THREAD: usize = 25;
    let (_runtime, _server, url) = start_server();
    let mut options = json_options();
    options.flush_timeout_ms = 500;
    let sdk = create_sdk(&url);
    let mux = create_mux(sdk, "ffi.mux.async.concurrent", None, 2, &options);
    let mux_addr = mux as usize;
    let sink = AsyncSink::leaked();

    let barrier = Arc::new(Barrier::new(THREADS));
    let workers: Vec<_> = (0..THREADS)
        .map(|t| {
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let records = json_records(t * PER_THREAD..(t + 1) * PER_THREAD);
                barrier.wait();
                for json in &records {
                    let (returned, _, message) = json_record_async(mux_addr as *mut _, json, sink);
                    assert!(returned, "{message}");
                }
            })
        })
        .collect();
    workers.into_iter().for_each(|w| w.join().unwrap());

    let total = THREADS * PER_THREAD;
    let completions = sink.wait_for(total, "every async ingest calls back");
    assert!(completions.iter().all(|c| c.success));
    let ids: HashSet<i64> = completions.iter().map(|c| c.value).collect();
    assert_eq!(ids.len(), total, "distinct message IDs");
    assert_eq!(
        unacked_json_payloads(mux),
        sorted_strings(&json_records(0..total))
    );
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// A flush covers async ingests accepted before it, like `_nowait` ones.
#[test]
fn flush_covers_async_ingests_accepted_before_it() {
    const TABLE: &str = "ffi.mux.async.flush";
    const RECORDS: usize = 50;
    let (runtime, server, url) = start_server();
    let mut responses = vec![MockResponse::CreateStream {
        stream_id: "lane-0".into(),
        delay_ms: 0,
    }];
    responses.extend((0..RECORDS as i64).map(|offset| MockResponse::RecordAck {
        ack_up_to_offset: offset,
        delay_ms: 0,
    }));
    runtime.block_on(server.inject_responses(TABLE, responses));
    let ack_log: &'static AckLog = Box::leak(Box::default());
    let sdk = create_sdk(&url);
    let mux = create_mux_with_ack(sdk, TABLE, None, 1, &json_options(), ack_log);
    let sink = AsyncSink::leaked();

    for json in json_records(0..RECORDS) {
        assert!(json_record_async(mux, &json, sink).0);
    }
    let mut result = new_result();
    assert!(zerobus_multiplexed_stream_flush(mux, &mut result));
    expect_success(&mut result, "flush");
    assert_eq!(
        runtime.block_on(server.get_write_count()),
        RECORDS as u64,
        "flush returns only after every accepted async ingest is sent"
    );
    let completions = sink.wait_for(RECORDS, "every async ingest calls back");
    assert!(completions.iter().all(|c| c.success));
    wait_until("every record is acknowledged", || {
        ack_log.acks.lock().unwrap().len() >= RECORDS
    });

    assert!(zerobus_multiplexed_stream_close(mux, &mut result));
    expect_success(&mut result, "close");
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// A flush returns while another thread keeps submitting async ingests.
#[test]
fn flush_returns_under_concurrent_async_producer() {
    let (_runtime, _server, url) = start_server();
    let mut options = json_options();
    options.flush_timeout_ms = 1000;
    let sdk = create_sdk(&url);
    let mux = create_mux(sdk, "ffi.mux.async.producer", None, 1, &options);
    let mux_addr = mux as usize;
    let sink = AsyncSink::leaked();

    let stop = Arc::new(AtomicBool::new(false));
    let produced = Arc::new(AtomicUsize::new(0));
    let producer = {
        let (stop, produced) = (Arc::clone(&stop), Arc::clone(&produced));
        std::thread::spawn(move || {
            let mut accepted = Vec::new();
            let mut i = 0;
            while !stop.load(Ordering::SeqCst) {
                let json = CString::new(format!(r#"{{"id": {i}}}"#)).unwrap();
                i += 1;
                if json_record_async(mux_addr as *mut _, &json, sink).0 {
                    accepted.push(json.into_string().unwrap());
                }
                produced.fetch_add(1, Ordering::SeqCst);
                // Paces the producer so the recovered set stays small.
                std::thread::sleep(Duration::from_millis(1));
            }
            accepted
        })
    };
    wait_until("the producer is running", || {
        produced.load(Ordering::SeqCst) > 0
    });

    let flush_returned = returns_within(
        mux,
        zerobus_multiplexed_stream_flush,
        Duration::from_secs(5),
    );
    stop.store(true, Ordering::SeqCst);
    let mut accepted = producer.join().unwrap();
    assert!(flush_returned, "flush did not return within 5 s");

    accepted.sort();
    assert!(!accepted.is_empty());
    assert_eq!(unacked_json_payloads(mux), accepted);
    sink.wait_for(accepted.len(), "every accepted async ingest calls back");
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// Runs `call` on a thread; reports whether it returned within `bound`.
fn returns_within(
    mux: *mut CZerobusMultiplexedStream,
    call: extern "C" fn(*mut CZerobusMultiplexedStream, *mut CResult) -> bool,
    bound: Duration,
) -> bool {
    let mux_addr = mux as usize;
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut result = new_result();
        call(mux_addr as *mut _, &mut result);
        take_message(&mut result);
        let _ = tx.send(());
    });
    rx.recv_timeout(bound).is_ok()
}

/// flush_async and close_async report a stored background `_nowait` failure.
#[test]
fn async_flush_and_close_report_stored_nowait_failure() {
    for op in ["flush_async", "close_async"] {
        let (runtime, server, url) = start_server();
        let (sdk, mux) = json_mux_with_acks(&runtime, &server, &url, "ffi.mux.async.stored", 0);
        nowait_wrong_type(mux);
        let sink = AsyncSink::leaked();
        let (returned, _, message) = if op == "flush_async" {
            flush_async(mux, sink)
        } else {
            close_async(mux, sink)
        };
        assert!(returned, "{op}: {message}");
        let done = sink.one(op);
        assert!(
            done.value == 0 && !done.success && !done.retryable,
            "{op}: {done:?}"
        );
        assert!(
            done.message.contains(RECORD_TYPE_MISMATCH),
            "{op}: {}",
            done.message
        );
        if op == "close_async" {
            assert!(zerobus_multiplexed_stream_is_closed(mux));
        }
        zerobus_multiplexed_stream_free(mux);
        zerobus_sdk_free(sdk);
    }
}

/// A pending flush_async holds the ack callbacks, so freeing the handle
/// doesn't free the ack `user_data` until it completes.
#[test]
fn free_with_pending_flush_async_defers_ack_user_data_free() {
    let ack = AckOwner::leaked();
    let gated = GatedMux::new(
        "ffi.mux.async.ack.pending",
        &json_options(),
        MuxAck::owned(ack, true),
        vec![],
    );
    let sink = AsyncSink::leaked();
    assert!(flush_async(gated.mux, sink).0);
    zerobus_multiplexed_stream_free(gated.mux);
    ack.assert_not_freed("free with a pending flush_async");

    gated.gate.release();
    let done = sink.one("flush_async");
    assert!(done.success && done.value == 1, "{done:?}");
    ack.assert_freed_once();
    zerobus_sdk_free(gated.sdk);
}

/// Async creates own the headers and ack `user_data`: on success both are
/// held until free; on a partial open both are freed once by the callback.
#[test]
fn async_creates_own_headers_and_ack_user_data() {
    // Success: nothing is freed until the handle is.
    let (_runtime, _server, url) = start_server();
    let sdk = create_sdk(&url);
    let (provider, ack, sink) = (
        ProviderLog::leaked(),
        AckOwner::leaked(),
        AsyncSink::leaked(),
    );
    assert!(
        headers_create_async(
            sdk,
            "ffi.mux.async.own",
            2,
            &json_options(),
            provider,
            ack,
            sink
        )
        .0
    );
    let created = sink.one("create callback");
    assert!(created.success && created.mux != 0, "{}", created.message);
    let mux = created.mux as *mut CZerobusMultiplexedStream;
    let mut result = new_result();
    assert!(zerobus_multiplexed_stream_close(mux, &mut result));
    expect_success(&mut result, "close");
    ack.assert_not_freed("close");
    assert_eq!(provider.frees(), 0, "the handle still owns user_data");
    zerobus_multiplexed_stream_free(mux);
    ack.assert_freed_once();
    wait_until("user_data is freed", || provider.frees() >= 1);
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(provider.frees(), 1);
    zerobus_sdk_free(sdk);

    // Partial open (T9 setup): both freed once by the time the callback ran.
    let (runtime, server, url) = start_server();
    let (oauth, uc_url) = runtime.block_on(start_mock_oauth_server());
    runtime.block_on(oauth.set_responses(vec![MockTokenResponse::ok("token-1")]));
    let sdk = create_sdk_with_uc(&url, &uc_url);
    for use_oauth in [false, true] {
        let table = if use_oauth {
            "ffi.mux.async_partial_oauth"
        } else {
            "ffi.mux.async_partial"
        };
        runtime.block_on(server.inject_responses(
            table,
            vec![
                MockResponse::CreateStream {
                    stream_id: "lane-ok".into(),
                    delay_ms: 0,
                },
                MockResponse::Error {
                    status: Status::permission_denied("denied"),
                    delay_ms: 0,
                },
            ],
        ));
        let (provider, ack, sink) = (
            ProviderLog::leaked(),
            AckOwner::leaked(),
            AsyncSink::leaked(),
        );
        let returned = if use_oauth {
            oauth_create_async(sdk, table, 2, &json_options_terminal(), ack, sink).0
        } else {
            headers_create_async(sdk, table, 2, &json_options_terminal(), provider, ack, sink).0
        };
        assert!(returned, "{table}");
        let created = sink.one(&format!("{table} create callback"));
        assert!(
            created.mux == 0 && !created.success && !created.retryable,
            "{table}: {created:?}"
        );
        assert!(
            created.message.contains("denied"),
            "{table}: must fail on the rejected lane: {}",
            created.message
        );
        ack.assert_freed_once();
        if !use_oauth {
            wait_until("user_data is freed", || provider.frees() >= 1);
            std::thread::sleep(Duration::from_millis(100));
            assert_eq!(provider.frees(), 1, "{table}: user_data freed exactly once");
        }
    }
    zerobus_sdk_free(sdk);
}

fn headers_create_async(
    sdk: *mut CZerobusSdk,
    table: &str,
    stream_count: usize,
    options: &CStreamConfigurationOptions,
    provider: &'static ProviderLog,
    ack: &'static AckOwner,
    sink: &'static AsyncSink,
) -> (bool, bool, String) {
    let table = CString::new(table).unwrap();
    let ack = MuxAck::owned(ack, true);
    let mut result = new_result();
    let returned = zerobus_sdk_create_multiplexed_stream_with_headers_provider_async(
        sdk,
        table.as_ptr(),
        ptr::null(),
        0,
        counting_headers,
        provider.user_data(),
        Some(count_free),
        stream_count,
        options,
        ack.on_ack,
        ack.on_error,
        ack.user_data,
        ack.free_user_data,
        sink_create,
        sink.user_data(),
        &mut result,
    );
    sync_part(returned, &mut result)
}

fn oauth_create_async(
    sdk: *mut CZerobusSdk,
    table: &str,
    stream_count: usize,
    options: &CStreamConfigurationOptions,
    ack: &'static AckOwner,
    sink: &'static AsyncSink,
) -> (bool, bool, String) {
    let table = CString::new(table).unwrap();
    let (id, secret) = (CString::new("id").unwrap(), CString::new("secret").unwrap());
    let ack = MuxAck::owned(ack, true);
    let mut result = new_result();
    let returned = zerobus_sdk_create_multiplexed_stream_async(
        sdk,
        table.as_ptr(),
        ptr::null(),
        0,
        id.as_ptr(),
        secret.as_ptr(),
        stream_count,
        options,
        ack.on_ack,
        ack.on_error,
        ack.user_data,
        ack.free_user_data,
        sink_create,
        sink.user_data(),
        &mut result,
    );
    sync_part(returned, &mut result)
}

type BlockingCall = Box<dyn Fn() -> (bool, bool, String)>;

/// The blocking counterparts of `async_readers`, in the same order; each
/// returns `(success, retryable, message)`.
fn blocking_readers(mux: *mut CZerobusMultiplexedStream, message_id: i64) -> Vec<BlockingCall> {
    let mux_addr = mux as usize;
    let mux = move || mux_addr as *mut CZerobusMultiplexedStream;
    let outcome = |result: &mut CResult| {
        let (success, retryable) = (result.success, result.is_retryable);
        (success, retryable, take_message(result))
    };
    let json = Arc::new(CString::new(r#"{"id": "async"}"#).unwrap());
    let proto: &'static [u8] = b"proto";
    let (json1, json2) = (Arc::clone(&json), Arc::clone(&json));
    vec![
        Box::new(move || {
            let mut result = new_result();
            zerobus_multiplexed_stream_ingest_proto_record(
                mux(),
                proto.as_ptr(),
                proto.len(),
                &mut result,
            );
            outcome(&mut result)
        }),
        Box::new(move || {
            let mut result = new_result();
            zerobus_multiplexed_stream_ingest_json_record(mux(), json1.as_ptr(), &mut result);
            outcome(&mut result)
        }),
        Box::new(move || {
            let (ptrs, lens) = ([proto.as_ptr()], [proto.len()]);
            let mut result = new_result();
            zerobus_multiplexed_stream_ingest_proto_records(
                mux(),
                ptrs.as_ptr(),
                lens.as_ptr(),
                1,
                &mut result,
            );
            outcome(&mut result)
        }),
        Box::new(move || {
            let ptrs = [json2.as_ptr()];
            let mut result = new_result();
            zerobus_multiplexed_stream_ingest_json_records(mux(), ptrs.as_ptr(), 1, &mut result);
            outcome(&mut result)
        }),
        Box::new(move || {
            let mut result = new_result();
            zerobus_multiplexed_stream_wait_for_message_id(mux(), message_id, &mut result);
            outcome(&mut result)
        }),
        Box::new(move || {
            let mut result = new_result();
            zerobus_multiplexed_stream_flush(mux(), &mut result);
            outcome(&mut result)
        }),
    ]
}

/// On a closed mux (closed explicitly, or by a retryable lane failure) each
/// async reader returns true and calls back once with the error its blocking
/// counterpart returns. Nothing it carried is sent or recoverable.
#[test]
fn async_calls_on_closed_mux_report_like_blocking_calls() {
    for lane_failure in [false, true] {
        let what = if lane_failure {
            "lane-failure close"
        } else {
            "plain close"
        };
        let table = if lane_failure {
            "ffi.mux.async.closed.failure"
        } else {
            "ffi.mux.async.closed"
        };
        let (runtime, server, url) = start_server();
        if lane_failure {
            runtime.block_on(server.inject_responses(
                table,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "lane-0".into(),
                        delay_ms: 0,
                    },
                    MockResponse::Error {
                        status: Status::unavailable("lane down"),
                        delay_ms: 0,
                    },
                ],
            ));
        }
        // The lane failure must stay terminal; the plain close needs recovery on.
        let mut options = if lane_failure {
            json_options_terminal()
        } else {
            json_options()
        };
        options.flush_timeout_ms = 500;
        let sdk = create_sdk(&url);
        let mux = create_mux(sdk, table, None, 1, &options);
        let mut result = new_result();

        let r0 = CString::new(r#"{"id": 0}"#).unwrap();
        let r0_id = if lane_failure {
            let id = zerobus_multiplexed_stream_ingest_json_record(mux, r0.as_ptr(), &mut result);
            expect_success(&mut result, "ingest_json_record (r0)");
            assert!(!zerobus_multiplexed_stream_flush(mux, &mut result));
            let (retryable, message) = expect_failure(&mut result, "flush after the lane failure");
            assert!(
                retryable,
                "an Unavailable lane failure is retryable: {message}"
            );
            id
        } else {
            assert!(zerobus_multiplexed_stream_close(mux, &mut result));
            expect_success(&mut result, "close");
            0
        };
        assert!(zerobus_multiplexed_stream_is_closed(mux));
        let writes_before = runtime.block_on(server.get_write_count());

        for ((call_name, call), blocking) in async_readers(mux, r0_id)
            .into_iter()
            .zip(blocking_readers(mux, r0_id))
        {
            let sink = AsyncSink::leaked();
            let (returned, _, message) = call(sink);
            assert!(returned, "{what}: {call_name} must return true: {message}");
            let done = sink.one(&format!("{what}: {call_name}"));
            let (success, retryable, expected) = blocking();
            assert!(
                !success,
                "{what}: blocking {call_name} succeeded on a closed mux"
            );
            assert!(!done.success, "{what}: {call_name}: {}", done.message);
            assert_eq!(
                (done.retryable, done.message.as_str()),
                (retryable, expected.as_str()),
                "{what}: {call_name} must report what its blocking counterpart returns"
            );
        }

        assert_eq!(
            runtime.block_on(server.get_write_count()),
            writes_before,
            "{what}: a rejected record reached the server"
        );
        let expected = if lane_failure {
            sorted_strings([&r0])
        } else {
            Vec::new()
        };
        assert_eq!(unacked_json_payloads(mux), expected, "{what}");
        zerobus_multiplexed_stream_free(mux);
        zerobus_sdk_free(sdk);
    }
}

/// An ordinary (non-multiplexed) stream still delivers acks to the C
/// callbacks set in `options`; the bindings' ordinary ack path depends on it.
#[test]
fn ordinary_stream_delivers_acks_to_options_callbacks() {
    const TABLE: &str = "ffi.ordinary.acks";
    let (runtime, server, url) = start_server();
    runtime.block_on(server.inject_responses(
        TABLE,
        vec![
            MockResponse::CreateStream {
                stream_id: "s".into(),
                delay_ms: 0,
            },
            MockResponse::RecordAck {
                ack_up_to_offset: 0,
                delay_ms: 0,
            },
            MockResponse::RecordAck {
                ack_up_to_offset: 1,
                delay_ms: 0,
            },
        ],
    ));
    let ack_log = Box::new(AckLog::default());
    let mut options = json_options();
    options.ack_on_ack = Some(record_ack);
    options.ack_on_error = Some(record_error);
    options.ack_user_data = &*ack_log as *const AckLog as *mut c_void;

    let sdk = create_sdk(&url);
    let table = CString::new(TABLE).unwrap();
    let mut result = new_result();
    let stream = zerobus_sdk_create_stream_with_headers_provider(
        sdk,
        table.as_ptr(),
        ptr::null(),
        0,
        empty_headers,
        ptr::null_mut(),
        None,
        &options,
        &mut result,
    );
    expect_success(
        &mut result,
        "zerobus_sdk_create_stream_with_headers_provider",
    );
    assert!(!stream.is_null());

    let mut offsets: Vec<i64> = json_records(0..2)
        .iter()
        .map(|json| {
            let offset = zerobus_stream_ingest_json_record(stream, json.as_ptr(), &mut result);
            expect_success(&mut result, "zerobus_stream_ingest_json_record");
            offset
        })
        .collect();
    assert!(zerobus_stream_flush(stream, &mut result));
    expect_success(&mut result, "zerobus_stream_flush");
    assert!(zerobus_stream_close(stream, &mut result));
    expect_success(&mut result, "zerobus_stream_close");
    zerobus_stream_free(stream);
    zerobus_sdk_free(sdk);

    // Close drains the callback worker, so every ack has been delivered.
    offsets.sort();
    assert_eq!(ack_log.sorted_acks(), offsets);
    assert!(ack_log.errors.lock().unwrap().is_empty());
}

/// flush_async covers a `_nowait` or `_async` ingest accepted before it,
/// even one still waiting for in-flight capacity.
#[test]
fn flush_async_waits_for_ingest_blocked_on_capacity() {
    for kind in ["_nowait", "_async"] {
        let (runtime, server, url) = start_server();
        let (gate_r0, gate_r1) = (
            Arc::new(MockResponseGate::new()),
            Arc::new(MockResponseGate::new()),
        );
        runtime.block_on(server.inject_responses(
            "ffi.mux.flush.async.capacity",
            vec![
                MockResponse::CreateStream {
                    stream_id: "lane-0".into(),
                    delay_ms: 0,
                },
                MockResponse::GatedRecordAck {
                    ack_up_to_offset: 0,
                    gate: Arc::clone(&gate_r0),
                },
                MockResponse::GatedRecordAck {
                    ack_up_to_offset: 1,
                    gate: Arc::clone(&gate_r1),
                },
            ],
        ));
        let mut options = json_options();
        options.max_inflight_requests = 1;
        let sdk = create_sdk(&url);
        let mux = create_mux(sdk, "ffi.mux.flush.async.capacity", None, 1, &options);
        let mut result = new_result();
        let [r0, r1]: [CString; 2] = json_records(0..2).try_into().unwrap();
        zerobus_multiplexed_stream_ingest_json_record(mux, r0.as_ptr(), &mut result);
        expect_success(&mut result, "ingest_json_record (r0)");
        wait_for_writes(&runtime, &server, 1);

        // r1 waits for the only in-flight slot.
        let ingest_sink = AsyncSink::leaked();
        if kind == "_nowait" {
            zerobus_multiplexed_stream_ingest_json_record_nowait(mux, r1.as_ptr(), &mut result);
            expect_success(&mut result, "ingest_json_record_nowait (r1)");
        } else {
            assert!(json_record_async(mux, &r1, ingest_sink).0);
        }
        let flush_sink = AsyncSink::leaked();
        assert!(flush_async(mux, flush_sink).0, "{kind}");

        // r0's ack lets r1 be sent; r1's ack is still held.
        gate_r0.release();
        wait_for_writes(&runtime, &server, 2);
        flush_sink.assert_never_called(&format!("{kind}: flush_async before r1 is acked"));

        gate_r1.release();
        let done = flush_sink.one(&format!("{kind}: flush_async"));
        assert!(done.success && done.value == 1, "{kind}: {done:?}");
        if kind == "_async" {
            assert!(ingest_sink.one("r1 async ingest").success);
        }
        assert!(zerobus_multiplexed_stream_close(mux, &mut result));
        expect_success(&mut result, "close");
        zerobus_multiplexed_stream_free(mux);
        zerobus_sdk_free(sdk);
    }
}

/// An async ingest releases its flush ticket and read guard before its
/// callback runs: a flush and a close called while the callback is still
/// running don't wait for it.
///
/// The ingest is rejected in its task (a protobuf record on a JSON mux), so
/// nothing is queued. A queued record would wake the lane's sender on the
/// worker the blocked callback occupies, and that send would stall until
/// the callback returns, which is a separate issue from the one tested here.
#[test]
fn async_ingest_releases_ticket_and_guard_before_callback() {
    const TABLE: &str = "ffi.mux.async.release";
    let (runtime, server, url) = start_server();
    let (sdk, mux) = json_mux_with_acks(&runtime, &server, &url, TABLE, 0);

    let sink = AsyncSink::leaked();
    sink.block.store(true, Ordering::SeqCst);
    let proto = b"proto";
    let mut result = new_result();
    assert!(zerobus_multiplexed_stream_ingest_proto_record_async(
        mux,
        proto.as_ptr(),
        proto.len(),
        sink_offset,
        sink.user_data(),
        &mut result,
    ));
    expect_success(&mut result, "ingest_proto_record_async");
    wait_until("the ingest callback is running", || {
        sink.entered.load(Ordering::SeqCst)
    });

    let flush = zerobus_multiplexed_stream_flush;
    let (flushed, _, message) = call_within_1s(mux, flush, "flush during the callback");
    assert!(flushed, "{message}");
    let close = zerobus_multiplexed_stream_close;
    let (closed, _, message) = call_within_1s(mux, close, "close during the callback");
    assert!(closed, "{message}");

    sink.release.store(true, Ordering::SeqCst);
    let done = sink.one("ingest callback");
    assert!(
        done.value == -1 && done.message.contains(RECORD_TYPE_MISMATCH),
        "{done:?}"
    );
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// A mux created with only `ack_on_error` reports a failed record to it.
#[test]
fn error_only_ack_callback_receives_failed_records() {
    const TABLE: &str = "ffi.mux.ack.error.only";
    let (runtime, server, url) = start_server();
    runtime.block_on(server.inject_responses(
        TABLE,
        vec![
            MockResponse::CreateStream {
                stream_id: "lane-0".into(),
                delay_ms: 0,
            },
            MockResponse::Error {
                status: Status::permission_denied("denied"),
                delay_ms: 0,
            },
        ],
    ));
    let ack_log: &'static AckLog = Box::leak(Box::default());
    let ack = MuxAck {
        on_ack: None,
        ..MuxAck::log(ack_log)
    };
    let sdk = create_sdk(&url);
    let (mux, mut result) =
        create_mux_raw_with_mux_ack(sdk, TABLE, None, 1, &json_options_terminal(), None, ack);
    expect_success(&mut result, "zerobus_sdk_create_multiplexed_stream");

    let record = CString::new(r#"{"id": 0}"#).unwrap();
    let id = zerobus_multiplexed_stream_ingest_json_record(mux, record.as_ptr(), &mut result);
    expect_success(&mut result, "ingest_json_record");
    assert!(!zerobus_multiplexed_stream_flush(mux, &mut result));
    take_message(&mut result);
    zerobus_multiplexed_stream_close(mux, &mut result);
    take_message(&mut result);

    wait_until("on_error receives the failed record", || {
        !ack_log.errors.lock().unwrap().is_empty()
    });
    assert_eq!(*ack_log.errors.lock().unwrap(), [id]);
    assert!(ack_log.acks.lock().unwrap().is_empty());
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// A blocking `_async` ingest callback doesn't hold up its own record: the
/// record still reaches the server, and a flush still completes, while the
/// callback runs.
#[test]
fn blocking_async_callback_does_not_hold_its_own_send() {
    for batch in [false, true] {
        let what = if batch {
            "json_records_async"
        } else {
            "json_record_async"
        };
        let (runtime, server, url) = start_server();
        let (sdk, mux) = json_mux_with_acks(&runtime, &server, &url, "ffi.mux.async.blocking", 1);
        let sink = AsyncSink::leaked();
        // The sink holds the callback for at most 5 s.
        sink.block.store(true, Ordering::SeqCst);

        let records = json_records(0..if batch { 2 } else { 1 });
        let mut result = new_result();
        let returned = if batch {
            let ptrs: Vec<*const c_char> = records.iter().map(|r| r.as_ptr()).collect();
            zerobus_multiplexed_stream_ingest_json_records_async(
                mux,
                ptrs.as_ptr(),
                ptrs.len(),
                sink_offset,
                sink.user_data(),
                &mut result,
            )
        } else {
            zerobus_multiplexed_stream_ingest_json_record_async(
                mux,
                records[0].as_ptr(),
                sink_offset,
                sink.user_data(),
                &mut result,
            )
        };
        expect_success(&mut result, what);
        assert!(returned, "{what}");
        wait_until("the ingest callback is running", || {
            sink.entered.load(Ordering::SeqCst)
        });

        let expected = records.len() as u64;
        let deadline = Instant::now() + Duration::from_secs(1);
        while runtime.block_on(server.get_write_count()) < expected {
            assert!(
                Instant::now() < deadline,
                "{what}: the record wasn't sent while its callback ran"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let flush = zerobus_multiplexed_stream_flush;
        let (flushed, _, message) = call_within_1s(mux, flush, "flush during the callback");
        assert!(flushed, "{what}: {message}");

        sink.release.store(true, Ordering::SeqCst);
        assert!(sink.one(what).success, "{what}");
        assert!(zerobus_multiplexed_stream_close(mux, &mut result));
        expect_success(&mut result, "close");
        zerobus_multiplexed_stream_free(mux);
        zerobus_sdk_free(sdk);
    }
}
