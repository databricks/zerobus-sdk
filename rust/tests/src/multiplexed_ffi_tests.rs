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
use std::sync::atomic::{AtomicUsize, Ordering};
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

/// JSON records, no recovery, so an injected failure stays terminal.
fn json_options() -> CStreamConfigurationOptions {
    let mut options = zerobus_get_default_config();
    options.record_type = 2;
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

fn with_ack_log(options: &mut CStreamConfigurationOptions, ack_log: &AckLog) {
    options.ack_on_ack = Some(record_ack);
    options.ack_on_error = Some(record_error);
    options.ack_user_data = ack_log as *const AckLog as *mut c_void;
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
    let mut options = zerobus_get_default_config();
    options.recovery = false;
    with_ack_log(&mut options, &ack_log);

    let sdk = create_sdk(&url);
    let descriptor = create_test_descriptor_proto().unwrap().encode_to_vec();
    let mux = create_mux(sdk, TABLE, Some(&descriptor), 1, &options);
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

    // Fire-and-forget records are queued asynchronously; wait until they are
    // sent so the flush below covers them.
    wait_for_writes(&runtime, &server, 6);

    assert!(zerobus_multiplexed_stream_flush(mux, &mut result));
    expect_success(&mut result, "flush");
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
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(provider.frees(), 1, "user_data must be freed exactly once");
    zerobus_sdk_free(sdk);
}

/// One lane opens and shares the provider, the other is rejected: the create
/// fails, and the shared provider's `user_data` is still freed exactly once.
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

    let sdk = create_sdk(&url);
    let options = json_options();
    let (mux, mut result) = create_mux_raw(sdk, TABLE, None, 2, &options, Some(provider));
    assert!(mux.is_null(), "a partially opened mux must not be returned");
    expect_permanent_failure(&mut result, "zerobus_sdk_create_multiplexed_stream");

    wait_until("user_data is freed", || provider.frees() >= 1);
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(provider.frees(), 1, "user_data must be freed exactly once");
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
    with_ack_log(&mut options, &ack_log);

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
    let mut options = json_options();
    with_ack_log(&mut options, &ack_log);
    let sdk = create_sdk(&url);
    let mux = create_mux(sdk, TABLE, None, 1, &options);
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
    let mux = create_mux(sdk, TABLE, None, 1, &json_options());
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
/// `is_closed` turns true. An ingest that arrives after that fails instead
/// of being queued and lost.
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
        "the rejected record never reaches the server"
    );
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}
