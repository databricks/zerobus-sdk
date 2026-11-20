//! Drives the multiplexed stream C ABI against the mock gRPC server.
//!
//! The FFI blocks on its own global runtime, so these tests call it from plain
//! threads and run the mock server on a separate runtime. They test the FFI
//! layer only: C arguments, ID passing, ownership, locks, the pending-ingest
//! tracker, `_async`, and error mapping. The core mux behavior is tested in
//! `multiplexed_stream_tests.rs`.

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

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

type Mux = *mut CZerobusMultiplexedStream;

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

/// Panics unless `result` reports a non-retryable failure; returns the message.
fn expect_permanent_failure(result: &mut CResult, call: &str) -> String {
    assert!(!result.success, "{call} unexpectedly succeeded");
    let retryable = result.is_retryable;
    let message = take_message(result);
    assert!(!retryable, "{call} must not be retryable: {message}");
    message
}

/// Runs a call that reports through a `CResult`: `(returned, retryable,
/// message)`.
fn outcome(f: impl FnOnce(*mut CResult) -> bool) -> (bool, bool, String) {
    let mut result = new_result();
    let returned = f(&mut result as *mut CResult);
    (returned, result.is_retryable, take_message(&mut result))
}

/// Runs a call whose outcome shows only in its `CResult`: `(success,
/// retryable, message)`.
fn blocking(f: impl FnOnce(*mut CResult)) -> (bool, bool, String) {
    let mut result = new_result();
    f(&mut result as *mut CResult);
    (
        result.success,
        result.is_retryable,
        take_message(&mut result),
    )
}

/// Runs a call that must succeed and returns its value.
fn ok<T>(what: &str, f: impl FnOnce(*mut CResult) -> T) -> T {
    let mut result = new_result();
    let value = f(&mut result as *mut CResult);
    expect_success(&mut result, what);
    value
}

fn stream_index(message_id: i64) -> usize {
    MessageId::from_raw(message_id).stream_index()
}

/// Core message for a record whose type doesn't match the stream.
const RECORD_TYPE_MISMATCH: &str = "Record type does not match stream configuration";

/// Polls `condition` until it holds, failing after 5 seconds.
fn wait_until(what: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting: {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Runs `f` on a thread; fails the test unless it returns within `bound`.
fn within<T: Send + 'static>(
    bound: Duration,
    what: &str,
    f: impl FnOnce() -> T + Send + 'static,
) -> T {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(bound)
        .unwrap_or_else(|_| panic!("{what} did not return within {bound:?}"))
}

/// Runs `f` here and fails unless it returns within 1 s: for calls that must
/// not block.
fn prompt<T>(what: &str, f: impl FnOnce() -> T) -> T {
    let started = Instant::now();
    let value = f();
    assert!(started.elapsed() < Duration::from_secs(1), "{what} blocked");
    value
}

type MuxCall = extern "C" fn(Mux, *mut CResult) -> bool;

/// A flush or close on a thread, bounded by `bound`: `(returned, retryable,
/// message)`.
fn call_within(bound: Duration, mux: Mux, call: MuxCall, what: &str) -> (bool, bool, String) {
    let mux_addr = mux as usize;
    within(bound, what, move || {
        outcome(|r| call(mux_addr as *mut _, r))
    })
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

/// Leaked test state whose address is handed to the FFI as `user_data`; the
/// FFI may call back into it until it frees it.
trait Leaked: Default + 'static {
    fn leaked() -> &'static Self {
        Box::leak(Box::default())
    }

    fn user_data(&'static self) -> *mut c_void {
        self as *const Self as *mut c_void
    }
}

/// Counts a headers provider's `get_headers` calls and `user_data` frees.
#[derive(Default)]
struct FreeCounter {
    calls: AtomicUsize,
    frees: AtomicUsize,
}

impl Leaked for FreeCounter {}

impl FreeCounter {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn frees(&self) -> usize {
        self.frees.load(Ordering::SeqCst)
    }

    fn assert_freed_once(&self, what: &str) {
        wait_until(&format!("{what}: user_data is freed"), || self.frees() >= 1);
        // Let a late second release land before asserting exactly once.
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(self.frees(), 1, "{what}: user_data freed exactly once");
    }
}

extern "C" fn empty_headers(_user_data: *mut c_void) -> CHeaders {
    CHeaders {
        headers: ptr::null_mut(),
        count: 0,
        error_message: ptr::null_mut(),
    }
}

extern "C" fn counting_headers(user_data: *mut c_void) -> CHeaders {
    let counter = unsafe { &*(user_data as *const FreeCounter) };
    counter.calls.fetch_add(1, Ordering::SeqCst);
    empty_headers(user_data)
}

extern "C" fn count_free(user_data: *mut c_void) {
    let counter = unsafe { &*(user_data as *const FreeCounter) };
    counter.frees.fetch_add(1, Ordering::SeqCst);
}

type HeadersArgs = (
    extern "C" fn(*mut c_void) -> CHeaders,
    *mut c_void,
    Option<extern "C" fn(*mut c_void)>,
);

/// The headers-provider arguments of a create: counting into `provider`
/// (freed by the FFI), or empty headers with no `user_data`.
fn headers_args(provider: Option<&'static FreeCounter>) -> HeadersArgs {
    match provider {
        Some(counter) => (counting_headers, counter.user_data(), Some(count_free)),
        None => (empty_headers, ptr::null_mut(), None),
    }
}

/// Mux ack `user_data`: records the IDs acked and failed, counts frees, flags
/// a callback that runs after the free, and can hold a callback until
/// released.
#[derive(Default)]
struct AckLog {
    acks: Mutex<Vec<i64>>,
    errors: Mutex<Vec<i64>>,
    frees: AtomicUsize,
    callback_after_free: AtomicBool,
    block_callbacks: AtomicBool,
    callback_entered: AtomicBool,
    release_callbacks: AtomicBool,
}

impl Leaked for AckLog {}

impl AckLog {
    fn sorted_acks(&self) -> Vec<i64> {
        let mut acks = self.acks.lock().unwrap().clone();
        acks.sort();
        acks
    }

    fn ack_count(&self) -> usize {
        self.acks.lock().unwrap().len()
    }

    fn errors(&self) -> Vec<i64> {
        self.errors.lock().unwrap().clone()
    }

    fn frees(&self) -> usize {
        self.frees.load(Ordering::SeqCst)
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

extern "C" fn log_ack(message_id: i64, user_data: *mut c_void) {
    let log = unsafe { &*(user_data as *const AckLog) };
    log.acks.lock().unwrap().push(message_id);
    log.on_callback();
}

extern "C" fn log_error(message_id: i64, _message: *const c_char, user_data: *mut c_void) {
    let log = unsafe { &*(user_data as *const AckLog) };
    log.errors.lock().unwrap().push(message_id);
    log.on_callback();
}

extern "C" fn log_free(user_data: *mut c_void) {
    let log = unsafe { &*(user_data as *const AckLog) };
    log.frees.fetch_add(1, Ordering::SeqCst);
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

    /// Records into `log`, which the test keeps (no free hook).
    fn log(log: &AckLog) -> Self {
        Self {
            on_ack: Some(log_ack),
            on_error: Some(log_error),
            user_data: log as *const AckLog as *mut c_void,
            free_user_data: None,
        }
    }

    /// Hands `log` to the FFI with the free hook, with or without callbacks.
    fn owned(log: &'static AckLog, with_callbacks: bool) -> Self {
        Self {
            on_ack: with_callbacks.then_some(log_ack as _),
            on_error: with_callbacks.then_some(log_error as _),
            user_data: log.user_data(),
            free_user_data: Some(log_free),
        }
    }
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
    ok("zerobus_sdk_builder_build", |r| {
        zerobus_sdk_builder_build(builder, r)
    })
}

fn free_both(sdk: *mut CZerobusSdk, mux: Mux) {
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
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

/// `options` changed by `f`.
fn with(
    mut options: CStreamConfigurationOptions,
    f: impl FnOnce(&mut CStreamConfigurationOptions),
) -> CStreamConfigurationOptions {
    f(&mut options);
    options
}

/// `json_options()` with a 500 ms flush, for records that stay unacked.
fn json_short_flush() -> CStreamConfigurationOptions {
    with(json_options(), |o| o.flush_timeout_ms = 500)
}

fn lane_open() -> MockResponse {
    MockResponse::CreateStream {
        stream_id: "lane-0".into(),
        delay_ms: 0,
    }
}

fn ack(offset: i64) -> MockResponse {
    MockResponse::RecordAck {
        ack_up_to_offset: offset,
        delay_ms: 0,
    }
}

fn gated_ack(offset: i64, gate: &Arc<MockResponseGate>) -> MockResponse {
    MockResponse::GatedRecordAck {
        ack_up_to_offset: offset,
        gate: Arc::clone(gate),
    }
}

fn lane_error(status: Status) -> MockResponse {
    MockResponse::Error {
        status,
        delay_ms: 0,
    }
}

/// One lane that opens and acks offsets `0..acks`.
fn lane_with_acks(acks: i64) -> Vec<MockResponse> {
    std::iter::once(lane_open())
        .chain((0..acks).map(ack))
        .collect()
}

/// A lane that opens and then fails with `PermissionDenied` on its first record.
fn failing_lane() -> Vec<MockResponse> {
    vec![lane_open(), lane_error(Status::permission_denied("denied"))]
}

/// The headers-provider create; returns the handle and the unread result.
fn create_mux_raw(
    sdk: *mut CZerobusSdk,
    table: &str,
    descriptor: Option<&[u8]>,
    stream_count: usize,
    options: *const CStreamConfigurationOptions,
    provider: Option<&'static FreeCounter>,
    ack: MuxAck,
) -> (Mux, CResult) {
    let table = CString::new(table).unwrap();
    let (descriptor_ptr, descriptor_len) =
        descriptor.map_or((ptr::null(), 0), |d| (d.as_ptr(), d.len()));
    let (callback, user_data, free_user_data) = headers_args(provider);
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

/// A working headers-provider mux; panics if the create fails.
fn create_mux_with(
    sdk: *mut CZerobusSdk,
    table: &str,
    descriptor: Option<&[u8]>,
    stream_count: usize,
    options: *const CStreamConfigurationOptions,
    ack: MuxAck,
) -> Mux {
    let (mux, mut result) =
        create_mux_raw(sdk, table, descriptor, stream_count, options, None, ack);
    expect_success(&mut result, "zerobus_sdk_create_multiplexed_stream");
    assert!(!mux.is_null());
    mux
}

fn create_mux(
    sdk: *mut CZerobusSdk,
    table: &str,
    descriptor: Option<&[u8]>,
    stream_count: usize,
    options: &CStreamConfigurationOptions,
) -> Mux {
    create_mux_with(
        sdk,
        table,
        descriptor,
        stream_count,
        options,
        MuxAck::none(),
    )
}

/// A JSON mux whose server acks offsets `0..acks`.
fn json_mux_with_acks(
    runtime: &Runtime,
    server: &MockZerobusServer,
    url: &str,
    table: &str,
    acks: i64,
) -> (*mut CZerobusSdk, Mux) {
    runtime.block_on(server.inject_responses(table, lane_with_acks(acks)));
    let sdk = create_sdk(url);
    let mux = create_mux(sdk, table, None, 1, &json_options());
    (sdk, mux)
}

/// A mock server (with `responses` injected for `table`), an SDK pointed at
/// it, and one JSON headers-provider mux over `lanes` lanes.
struct Fixture {
    runtime: Runtime,
    server: MockZerobusServer,
    sdk: *mut CZerobusSdk,
    mux: Mux,
}

impl Fixture {
    fn new(
        table: &str,
        responses: Vec<MockResponse>,
        lanes: usize,
        options: &CStreamConfigurationOptions,
        ack: MuxAck,
    ) -> Self {
        let (runtime, server, url) = start_server();
        if !responses.is_empty() {
            runtime.block_on(server.inject_responses(table, responses));
        }
        let sdk = create_sdk(&url);
        let mux = create_mux_with(sdk, table, None, lanes, options, ack);
        Self {
            runtime,
            server,
            sdk,
            mux,
        }
    }

    fn writes(&self) -> u64 {
        self.runtime.block_on(self.server.get_write_count())
    }

    fn wait_for_writes(&self, expected: u64) {
        wait_for_writes(&self.runtime, &self.server, expected);
    }

    fn free(self) {
        free_both(self.sdk, self.mux);
    }
}

/// A 1-lane JSON mux whose first record r0 is ingested and has its ack held
/// by `gate`; `then` follows the gated ack in the injected responses.
struct GatedMux {
    fixture: Fixture,
    gate: Arc<MockResponseGate>,
    r0_id: i64,
}

impl std::ops::Deref for GatedMux {
    type Target = Fixture;

    fn deref(&self) -> &Fixture {
        &self.fixture
    }
}

impl GatedMux {
    fn new(
        table: &str,
        options: &CStreamConfigurationOptions,
        ack: MuxAck,
        then: Vec<MockResponse>,
    ) -> Self {
        let gate = Arc::new(MockResponseGate::new());
        let mut responses = vec![lane_open(), gated_ack(0, &gate)];
        responses.extend(then);
        let fixture = Fixture::new(table, responses, 1, options, ack);
        let r0_id = ingest_json(fixture.mux, &json_record(0));
        fixture.wait_for_writes(1);
        Self {
            fixture,
            gate,
            r0_id,
        }
    }

    fn free(self) {
        self.fixture.free();
    }
}

fn wait_for_writes(runtime: &Runtime, server: &MockZerobusServer, expected: u64) {
    wait_until(&format!("server receives {expected} records"), || {
        runtime.block_on(server.get_write_count()) >= expected
    });
}

fn json_record(id: usize) -> CString {
    CString::new(format!(r#"{{"id": {id}}}"#)).unwrap()
}

fn json_records(ids: std::ops::Range<usize>) -> Vec<CString> {
    ids.map(json_record).collect()
}

fn ingest_json(mux: Mux, json: &CString) -> i64 {
    Ingest::Json(json.as_ptr()).ok(mux)
}

/// Overwrites a NUL-terminated buffer, keeping the NUL, so a late read would
/// still see a valid string.
fn scrub(buf: &mut [u8]) {
    let len = buf.len();
    buf[..len - 1].fill(b'x');
}

/// Returns the payloads of `records`, sorted, and frees the array; checks
/// each record's JSON flag.
fn take_payloads(records: CRecordArray, json: bool) -> Vec<Vec<u8>> {
    let mut payloads: Vec<Vec<u8>> = (0..records.len)
        .map(|i| {
            let record = unsafe { &*records.records.add(i) };
            assert_eq!(record.is_json, json);
            unsafe { std::slice::from_raw_parts(record.data, record.data_len) }.to_vec()
        })
        .collect();
    zerobus_free_record_array(records);
    payloads.sort();
    payloads
}

fn take_json_payloads(records: CRecordArray) -> Vec<String> {
    take_payloads(records, true)
        .into_iter()
        .map(|p| String::from_utf8(p).unwrap())
        .collect()
}

/// Closes `mux` and returns the payloads it never got acknowledged, sorted.
fn unacked_json_payloads(mux: Mux) -> Vec<String> {
    take_json_payloads(ok("get_unacked_records", |r| {
        zerobus_multiplexed_stream_get_unacked_records(mux, r)
    }))
}

fn sorted_strings<'a>(records: impl IntoIterator<Item = &'a CString>) -> Vec<String> {
    let mut strings: Vec<String> = records
        .into_iter()
        .map(|s| s.to_str().unwrap().to_owned())
        .collect();
    strings.sort();
    strings
}

fn close_ok(mux: Mux) {
    assert!(ok("close", |r| zerobus_multiplexed_stream_close(mux, r)));
}

fn flush_ok(mux: Mux) {
    assert!(ok("flush", |r| zerobus_multiplexed_stream_flush(mux, r)));
}

/// Closes `mux`, ignoring the result (used where the close may fail on
/// unacked records and only what follows matters).
fn close_quietly(mux: Mux) {
    outcome(|r| zerobus_multiplexed_stream_close(mux, r));
}

/// One of the four ingest calls with its input, run blocking or `_async`.
#[derive(Clone, Copy)]
enum Ingest<'a> {
    Proto(&'a [u8]),
    /// Pointers and lengths; the count is the number of pointers.
    Protos(&'a [*const u8], &'a [usize]),
    Json(*const c_char),
    Jsons(&'a [*const c_char]),
}

impl Ingest<'_> {
    fn name(self) -> &'static str {
        match self {
            Ingest::Proto(_) => "proto_record",
            Ingest::Protos(..) => "proto_records",
            Ingest::Json(_) => "json_record",
            Ingest::Jsons(_) => "json_records",
        }
    }

    /// The blocking ingest: `(id, success, retryable, message)`.
    fn call(self, mux: Mux) -> (i64, bool, bool, String) {
        let mut result = new_result();
        let r = &mut result as *mut CResult;
        let id = match self {
            Ingest::Proto(data) => {
                zerobus_multiplexed_stream_ingest_proto_record(mux, data.as_ptr(), data.len(), r)
            }
            Ingest::Protos(ptrs, lens) => zerobus_multiplexed_stream_ingest_proto_records(
                mux,
                ptrs.as_ptr(),
                lens.as_ptr(),
                ptrs.len(),
                r,
            ),
            Ingest::Json(json) => zerobus_multiplexed_stream_ingest_json_record(mux, json, r),
            Ingest::Jsons(jsons) => {
                zerobus_multiplexed_stream_ingest_json_records(mux, jsons.as_ptr(), jsons.len(), r)
            }
        };
        (
            id,
            result.success,
            result.is_retryable,
            take_message(&mut result),
        )
    }

    /// The blocking ingest, which must succeed; returns its ID.
    fn ok(self, mux: Mux) -> i64 {
        let (id, success, _, message) = self.call(mux);
        assert!(success, "{} failed: {message}", self.name());
        id
    }

    /// The `_async` ingest: `(returned, retryable, message)`.
    fn call_async(self, mux: Mux, sink: &'static AsyncSink) -> (bool, bool, String) {
        let ud = sink.user_data();
        outcome(|r| match self {
            Ingest::Proto(data) => zerobus_multiplexed_stream_ingest_proto_record_async(
                mux,
                data.as_ptr(),
                data.len(),
                sink_offset,
                ud,
                r,
            ),
            Ingest::Protos(ptrs, lens) => zerobus_multiplexed_stream_ingest_proto_records_async(
                mux,
                ptrs.as_ptr(),
                lens.as_ptr(),
                ptrs.len(),
                sink_offset,
                ud,
                r,
            ),
            Ingest::Json(json) => {
                zerobus_multiplexed_stream_ingest_json_record_async(mux, json, sink_offset, ud, r)
            }
            Ingest::Jsons(jsons) => zerobus_multiplexed_stream_ingest_json_records_async(
                mux,
                jsons.as_ptr(),
                jsons.len(),
                sink_offset,
                ud,
                r,
            ),
        })
    }

    /// The `_async` ingest, which must be accepted.
    fn start(self, mux: Mux, sink: &'static AsyncSink) {
        let (returned, _, message) = self.call_async(mux, sink);
        assert!(
            returned && message.is_empty(),
            "{}_async: {message}",
            self.name()
        );
    }
}

// ---------------------------------------------------------------------------
// `_async` helpers
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

impl Leaked for AsyncSink {}

impl AsyncSink {
    /// A sink that holds its callback (for at most 5 s) until released.
    fn blocking() -> &'static Self {
        let sink = Self::leaked();
        sink.block.store(true, Ordering::SeqCst);
        sink
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

    fn wait_entered(&self) {
        wait_until("the callback is running", || {
            self.entered.load(Ordering::SeqCst)
        });
    }

    fn release(&self) {
        self.release.store(true, Ordering::SeqCst);
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

extern "C" fn sink_create(mux: Mux, result: *const CResult, user_data: *mut c_void) {
    let completion = Completion {
        mux: mux as usize,
        ..Default::default()
    };
    AsyncSink::push(user_data, result, completion);
}

fn wait_async(mux: Mux, message_id: i64, sink: &'static AsyncSink) -> (bool, bool, String) {
    outcome(|r| {
        zerobus_multiplexed_stream_wait_for_message_id_async(
            mux,
            message_id,
            sink_bool,
            sink.user_data(),
            r,
        )
    })
}

/// `flush_async`, `close_async` or `get_unacked_records_async` by name.
fn async_op(op: &str, mux: Mux, sink: &'static AsyncSink) -> (bool, bool, String) {
    let ud = sink.user_data();
    outcome(|r| match op {
        "flush_async" => zerobus_multiplexed_stream_flush_async(mux, sink_bool, ud, r),
        "close_async" => zerobus_multiplexed_stream_close_async(mux, sink_bool, ud, r),
        "get_unacked_records_async" => {
            zerobus_multiplexed_stream_get_unacked_records_async(mux, sink_records, ud, r)
        }
        _ => unreachable!("{op}"),
    })
}

/// One of the six calls that need the read lock: four ingests, a wait, and a
/// flush.
#[derive(Clone, Copy)]
enum Reader {
    Ingest(Ingest<'static>),
    Wait(i64),
    Flush,
}

impl Reader {
    fn name(self) -> &'static str {
        match self {
            Reader::Ingest(Ingest::Proto(_)) => "proto_record_async",
            Reader::Ingest(Ingest::Protos(..)) => "proto_records_async",
            Reader::Ingest(Ingest::Json(_)) => "json_record_async",
            Reader::Ingest(Ingest::Jsons(_)) => "json_records_async",
            Reader::Wait(_) => "wait_for_message_id_async",
            Reader::Flush => "flush_async",
        }
    }

    fn call_async(self, mux: Mux, sink: &'static AsyncSink) -> (bool, bool, String) {
        match self {
            Reader::Ingest(ingest) => ingest.call_async(mux, sink),
            Reader::Wait(id) => wait_async(mux, id, sink),
            Reader::Flush => async_op("flush_async", mux, sink),
        }
    }

    /// The blocking counterpart: `(success, retryable, message)`.
    fn call_blocking(self, mux: Mux) -> (bool, bool, String) {
        match self {
            Reader::Ingest(ingest) => {
                let (_, success, retryable, message) = ingest.call(mux);
                (success, retryable, message)
            }
            Reader::Wait(id) => blocking(|r| {
                zerobus_multiplexed_stream_wait_for_message_id(mux, id, r);
            }),
            Reader::Flush => blocking(|r| {
                zerobus_multiplexed_stream_flush(mux, r);
            }),
        }
    }
}

/// The six readers, each ingest with one protobuf or JSON record; the wait is
/// on `message_id`.
fn readers(message_id: i64) -> [Reader; 6] {
    let proto: &'static [u8] = b"proto";
    let json: &'static CString = Box::leak(Box::new(CString::new(r#"{"id": "async"}"#).unwrap()));
    let proto_ptrs: &'static [*const u8] = Box::leak(Box::new([proto.as_ptr()]));
    let proto_lens: &'static [usize] = Box::leak(Box::new([proto.len()]));
    let jsons: &'static [*const c_char] = Box::leak(Box::new([json.as_ptr()]));
    [
        Reader::Ingest(Ingest::Proto(proto)),
        Reader::Ingest(Ingest::Json(json.as_ptr())),
        Reader::Ingest(Ingest::Protos(proto_ptrs, proto_lens)),
        Reader::Ingest(Ingest::Jsons(jsons)),
        Reader::Wait(message_id),
        Reader::Flush,
    ]
}

/// The async create, OAuth or headers provider, with `ack` handed to it.
#[allow(clippy::too_many_arguments)]
fn create_async(
    sdk: *mut CZerobusSdk,
    oauth: bool,
    table: &str,
    stream_count: usize,
    options: &CStreamConfigurationOptions,
    provider: Option<&'static FreeCounter>,
    ack: MuxAck,
    sink: &'static AsyncSink,
) -> (bool, bool, String) {
    let table = CString::new(table).unwrap();
    let (id, secret) = (CString::new("id").unwrap(), CString::new("secret").unwrap());
    let (callback, user_data, free_user_data) = headers_args(provider);
    outcome(|r| {
        if oauth {
            zerobus_sdk_create_multiplexed_stream_async(
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
                r,
            )
        } else {
            zerobus_sdk_create_multiplexed_stream_with_headers_provider_async(
                sdk,
                table.as_ptr(),
                ptr::null(),
                0,
                callback,
                user_data,
                free_user_data,
                stream_count,
                options,
                ack.on_ack,
                ack.on_error,
                ack.user_data,
                ack.free_user_data,
                sink_create,
                sink.user_data(),
                r,
            )
        }
    })
}

/// Waits for an async create's callback; returns the handle it brought.
fn created(sink: &AsyncSink, what: &str) -> Mux {
    let created = sink.one(&format!("{what} create callback"));
    assert!(
        created.success && created.mux != 0,
        "{what}: {}",
        created.message
    );
    created.mux as Mux
}

// ---------------------------------------------------------------------------
// Blocking calls
// ---------------------------------------------------------------------------

/// The blocking ingests marshal their input and IDs across the ABI: protobuf
/// (with NULL options, which select protobuf) and JSON, single and batch; an
/// empty batch returns -2; `get_unacked_records` returns every unacked
/// payload exactly once, and a zero-length result array is freed too.
#[test]
fn blocking_ingest_paths_marshal_ids_and_payloads() {
    // Protobuf with NULL options; the server acks both calls.
    let (runtime, server, url) = start_server();
    runtime.block_on(server.inject_responses("ffi.mux.proto", lane_with_acks(2)));
    let ack_log = AckLog::leaked();
    let sdk = create_sdk(&url);
    let descriptor = create_test_descriptor_proto().unwrap().encode_to_vec();
    let mux = create_mux_with(
        sdk,
        "ffi.mux.proto",
        Some(&descriptor),
        1,
        ptr::null(),
        MuxAck::log(ack_log),
    );
    let batch: [&[u8]; 2] = [b"batch-0", b"batch-1"];
    let (ptrs, lens): (Vec<_>, Vec<_>) = batch.iter().map(|r| (r.as_ptr(), r.len())).unzip();
    let mut ids = vec![
        Ingest::Proto(b"single").ok(mux),
        Ingest::Protos(&ptrs, &lens).ok(mux),
    ];
    let empty = Ingest::Protos(&[], &[]).ok(mux);
    assert_eq!(empty, -2, "an empty batch returns -2, not a message ID");
    flush_ok(mux);
    assert_eq!(runtime.block_on(server.get_write_count()), 3);
    close_ok(mux);
    let records = ok("get_unacked_records", |r| {
        zerobus_multiplexed_stream_get_unacked_records(mux, r)
    });
    assert_eq!(records.len, 0, "every record was acknowledged");
    zerobus_free_record_array(records);
    free_both(sdk, mux);
    // Close drains the callback worker, so every ack has been delivered.
    ids.sort();
    assert_eq!(ack_log.sorted_acks(), ids);
    assert!(ack_log.errors().is_empty());

    // JSON over two lanes; the server never acks, so all of it is recovered.
    let f = Fixture::new(
        "ffi.mux.json",
        vec![],
        2,
        &json_short_flush(),
        MuxAck::none(),
    );
    let records = json_records(0..4);
    let ptrs: Vec<*const c_char> = records.iter().map(|s| s.as_ptr()).collect();
    ingest_json(f.mux, &records[0]);
    ingest_json(f.mux, &records[1]);
    Ingest::Jsons(&ptrs[2..]).ok(f.mux);
    let empty = Ingest::Jsons(&[]).ok(f.mux);
    assert_eq!(empty, -2, "an empty batch returns -2, not a message ID");
    f.wait_for_writes(4);
    let recovered = unacked_json_payloads(f.mux);
    assert_eq!(recovered.len(), 4, "each record returned exactly once");
    assert_eq!(recovered, sorted_strings(&records));
    f.free();
}

/// Message IDs cross the ABI intact: the ack callback gets the IDs ingest
/// returned, and both waits pass the lane and the sub-offset through.
#[test]
fn message_ids_cross_the_abi_for_acks_and_waits() {
    // No CreateStream entry: the mock opens every lane and each lane acks only
    // its offset 0.
    let ack_log = AckLog::leaked();
    let f = Fixture::new(
        "ffi.mux.message.ids",
        vec![ack(0)],
        2,
        &json_short_flush(),
        MuxAck::log(ack_log),
    );
    let ids: Vec<i64> = json_records(0..4)
        .iter()
        .map(|json| ingest_json(f.mux, json))
        .collect();
    assert_eq!(stream_index(ids[1]), 1);

    // (ID, acked): lane 1 offset 0 is acked, lane 1 offset 1 never is, and
    // lane 63 doesn't exist (its index sets the sign bit).
    let lane_63 = 63i64 << 58;
    for (id, acked) in [(ids[1], true), (ids[3], false), (lane_63, false)] {
        let (waited, retryable, message) =
            outcome(|r| zerobus_multiplexed_stream_wait_for_message_id(f.mux, id, r));
        assert_eq!(waited, acked, "wait({id}): {message}");
        let sink = AsyncSink::leaked();
        assert!(wait_async(f.mux, id, sink).0);
        let done = sink.one(&format!("wait_async({id})"));
        assert_eq!((done.value == 1, done.success), (acked, acked), "{done:?}");
        if id == lane_63 {
            assert!(!retryable && !message.is_empty(), "lane 63: {message}");
            assert!(done.value == 0 && !done.retryable, "{done:?}");
        }
    }

    close_quietly(f.mux);
    f.free();
    let mut expected = vec![ids[0], ids[1]];
    expected.sort();
    assert_eq!(ack_log.sorted_acks(), expected);
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
    runtime.block_on(server.inject_responses(TABLE, vec![ack(0)]));
    let sdk = create_sdk_with_uc(&url, &uc_url);
    let (table, id, secret) = (
        CString::new(TABLE).unwrap(),
        CString::new("client-id").unwrap(),
        CString::new("client-secret").unwrap(),
    );
    let mux = ok("zerobus_sdk_create_multiplexed_stream", |r| {
        zerobus_sdk_create_multiplexed_stream(
            sdk,
            table.as_ptr(),
            ptr::null(),
            0,
            id.as_ptr(),
            secret.as_ptr(),
            2,
            &json_options(),
            None,
            None,
            ptr::null_mut(),
            None,
            r,
        )
    });
    assert!(oauth.mint_count() >= 1, "the create must mint a token");
    // Two ingests land on two lanes, so stream_count reached the core.
    let lanes: HashSet<usize> = json_records(0..2)
        .iter()
        .map(|json| stream_index(ingest_json(mux, json)))
        .collect();
    assert_eq!(lanes.len(), 2);
    flush_ok(mux);
    close_ok(mux);
    free_both(sdk, mux);
}

/// Invalid C arguments fail only the call that passed them, sync or async:
/// an async call returns false and never calls back, none of a rejected batch
/// is queued, sent or recovered, and the mux stays usable.
#[test]
fn argument_errors_fail_only_that_call() {
    let ack_log = AckLog::leaked();
    let f = Fixture::new(
        "ffi.mux.bad.args",
        lane_with_acks(1),
        1,
        &json_options(),
        MuxAck::log(ack_log),
    );
    let valid = json_record(0);
    let invalid_utf8 = c"\xff".as_ptr();
    let proto = b"proto";
    let (proto_ptrs, proto_lens) = ([proto.as_ptr(), ptr::null()], [proto.len(), 4]);
    let null_batch = [valid.as_ptr(), ptr::null()];
    let utf8_batch = [valid.as_ptr(), invalid_utf8];
    // (what, call, also run `_async`)
    let rows = [
        ("json_record(NULL)", Ingest::Json(ptr::null()), true),
        (
            "json_record(invalid UTF-8)",
            Ingest::Json(invalid_utf8),
            false,
        ),
        (
            "json_records([valid, NULL])",
            Ingest::Jsons(&null_batch),
            true,
        ),
        (
            "json_records([valid, invalid UTF-8])",
            Ingest::Jsons(&utf8_batch),
            true,
        ),
        (
            "proto_records([valid, NULL])",
            Ingest::Protos(&proto_ptrs, &proto_lens),
            true,
        ),
    ];

    let mut sinks = Vec::new();
    for (what, ingest, with_async) in rows {
        let (id, _, retryable, message) = ingest.call(f.mux);
        let mut messages = vec![(what.to_string(), message)];
        assert_eq!(id, -1, "{what} must be rejected");
        if with_async {
            let sink = AsyncSink::leaked();
            let (returned, async_retryable, message) = ingest.call_async(f.mux, sink);
            assert!(!returned, "{what} async must be rejected");
            assert!(!async_retryable, "{what} async: {message}");
            messages.push((format!("{what} async"), message));
            sinks.push((what, sink));
        }
        for (what, message) in messages {
            assert!(!retryable && !message.is_empty(), "{what}: {message}");
            if what.starts_with("proto_records") {
                assert_eq!(message, "Invalid data pointer", "{what}");
            }
        }
    }
    // Let a wrongly scheduled callback land before checking.
    std::thread::sleep(Duration::from_millis(200));
    for (what, sink) in &sinks {
        assert_eq!(sink.count(), 0, "{what} async: callback must not run");
    }
    assert!(!zerobus_multiplexed_stream_is_closed(f.mux));

    let valid_id = ingest_json(f.mux, &valid);
    flush_ok(f.mux);
    assert!(unacked_json_payloads(f.mux).is_empty());
    assert_eq!(f.writes(), 1, "only the valid record reaches the server");
    f.free();
    assert_eq!(ack_log.sorted_acks(), [valid_id]);
}

/// The FFI frees the headers and ack `user_data` exactly once: on success
/// only at free, sync and async; at create when both ack callbacks are NULL;
/// after a partial open, with no handle. An `on_error`-only callback is
/// installed too.
#[test]
fn ffi_owned_user_data_is_freed_once() {
    // (a) Success with callbacks, sync and async: nothing is freed until free.
    for use_async in [false, true] {
        let what = if use_async { "async" } else { "sync" };
        let (runtime, server, url) = start_server();
        // No CreateStream entry: each lane acks its offset 0.
        runtime.block_on(server.inject_responses("ffi.mux.own", vec![ack(0)]));
        let (provider, ack_log) = (FreeCounter::leaked(), AckLog::leaked());
        let (sdk, options) = (create_sdk(&url), json_options());
        let ack = MuxAck::owned(ack_log, true);
        let mux = if use_async {
            let sink = AsyncSink::leaked();
            let args = (sdk, false, "ffi.mux.own", 2, &options, Some(provider));
            assert!(create_async(args.0, args.1, args.2, args.3, args.4, args.5, ack, sink).0);
            created(sink, what)
        } else {
            let (mux, mut result) =
                create_mux_raw(sdk, "ffi.mux.own", None, 2, &options, Some(provider), ack);
            expect_success(&mut result, "create");
            mux
        };
        assert!(provider.calls() >= 2, "{what}: every lane fetches headers");
        json_records(0..2).iter().for_each(|json| {
            ingest_json(mux, json);
        });
        flush_ok(mux);
        wait_until("both lanes ack", || ack_log.ack_count() >= 2);
        close_ok(mux);
        ack_log.assert_not_freed(&format!("{what} close"));
        assert_eq!(provider.frees(), 0, "{what}: the handle owns user_data");
        zerobus_multiplexed_stream_free(mux);
        ack_log.assert_freed_once();
        provider.assert_freed_once(what);
        zerobus_sdk_free(sdk);
    }

    // (b) Both ack callbacks NULL: the ack user_data is freed at create.
    let ack_log = AckLog::leaked();
    let ack = MuxAck::owned(ack_log, false);
    let f = Fixture::new("ffi.mux.ack.none", vec![], 1, &json_options(), ack);
    ack_log.assert_freed_once();
    close_ok(f.mux);
    f.free();
    // Let a late second release land before asserting exactly once.
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(
        ack_log.frees(),
        1,
        "ack user_data must be freed exactly once"
    );

    // (c) Partial open: one lane opens and shares both user_datas, the other
    // is rejected. No handle; each is freed once.
    let (runtime, server, url) = start_server();
    let (oauth, uc_url) = runtime.block_on(start_mock_oauth_server());
    runtime.block_on(oauth.set_responses(vec![MockTokenResponse::ok("token-1")]));
    let sdk = create_sdk_with_uc(&url, &uc_url);
    let options = json_options_terminal();
    for (what, use_async, use_oauth) in [
        ("sync headers", false, false),
        ("async headers", true, false),
        ("async OAuth", true, true),
    ] {
        let table = format!("ffi.mux.partial_{}", what.replace(' ', "_"));
        runtime.block_on(server.inject_responses(&table, failing_lane()));
        let (provider, ack_log) = (FreeCounter::leaked(), AckLog::leaked());
        let ack = MuxAck::owned(ack_log, true);
        let (mux, failed, message) = if use_async {
            let sink = AsyncSink::leaked();
            let provider = (!use_oauth).then_some(provider);
            assert!(create_async(sdk, use_oauth, &table, 2, &options, provider, ack, sink).0);
            let done = sink.one(&format!("{what} create callback"));
            (done.mux, !done.success && !done.retryable, done.message)
        } else {
            let (mux, mut result) =
                create_mux_raw(sdk, &table, None, 2, &options, Some(provider), ack);
            (
                mux as usize,
                true,
                expect_permanent_failure(&mut result, what),
            )
        };
        assert!(mux == 0 && failed, "{what}: {message}");
        assert!(message.contains("denied"), "{what}: {message}");
        ack_log.assert_freed_once();
        if !use_oauth {
            provider.assert_freed_once(what);
        }
    }
    zerobus_sdk_free(sdk);

    // (d) Only `on_error` set: a failed record still reaches it.
    let ack_log = AckLog::leaked();
    let ack = MuxAck {
        on_ack: None,
        ..MuxAck::log(ack_log)
    };
    let table = "ffi.mux.ack.error.only";
    let f = Fixture::new(table, failing_lane(), 1, &json_options_terminal(), ack);
    let id = ingest_json(f.mux, &json_record(0));
    assert!(!outcome(|r| zerobus_multiplexed_stream_flush(f.mux, r)).0);
    close_quietly(f.mux);
    wait_until("on_error receives the failed record", || {
        !ack_log.errors().is_empty()
    });
    assert_eq!(ack_log.errors(), [id]);
    assert_eq!(ack_log.ack_count(), 0);
    f.free();
}

/// Pending work keeps the ack `user_data` (and the handle) alive across
/// free: a callback still running after close stopped waiting, and a pending
/// `flush_async`, `close_async` or `get_unacked_records_async`, which still
/// calls back once with the right result.
#[test]
fn pending_work_and_running_callbacks_defer_frees() {
    // A running callback; close stops waiting for it after 50 ms.
    let ack_log = AckLog::leaked();
    ack_log.block_callbacks.store(true, Ordering::SeqCst);
    let options = with(json_options(), |o| {
        o.callback_max_wait_time_ms = 50;
        o.has_callback_max_wait_time_ms = true;
    });
    let ack = MuxAck::owned(ack_log, true);
    let f = Fixture::new("ffi.mux.ack.running", lane_with_acks(1), 1, &options, ack);
    ingest_json(f.mux, &json_record(0));
    wait_until("the ack callback is running", || {
        ack_log.callback_entered.load(Ordering::SeqCst)
    });
    close_quietly(f.mux);
    zerobus_multiplexed_stream_free(f.mux);
    ack_log.assert_not_freed("close and free with a callback still running");
    ack_log.release_callbacks.store(true, Ordering::SeqCst);
    ack_log.assert_freed_once();
    zerobus_sdk_free(f.sdk);

    // A pending async op (r0's ack is gated).
    for op in ["flush_async", "close_async", "get_unacked_records_async"] {
        let ack_log = AckLog::leaked();
        let ack = MuxAck::owned(ack_log, true);
        let gated = GatedMux::new("ffi.mux.async.free", &json_options(), ack, vec![]);
        let sink = AsyncSink::leaked();
        expect_returned_true(async_op(op, gated.mux, sink), op);
        zerobus_multiplexed_stream_free(gated.mux);
        ack_log.assert_not_freed(&format!("free with a pending {op}"));

        gated.gate.release();
        let done = sink.one(op);
        assert!(done.success, "{op}: {}", done.message);
        match op {
            "get_unacked_records_async" => assert!(done.payloads.is_empty(), "{done:?}"),
            _ => assert_eq!(done.value, 1, "{op}: {done:?}"),
        }
        ack_log.assert_freed_once();
        zerobus_sdk_free(gated.sdk);
    }
}

/// Many C threads share one handle, with blocking and async ingests at the
/// same time: every ID is distinct, every async callback fires once, and
/// every record is recovered.
#[test]
fn concurrent_callers_share_one_handle() {
    const THREADS: usize = 4;
    const PER_THREAD: usize = 25;
    let f = Fixture::new(
        "ffi.mux.concurrent",
        vec![],
        2,
        &json_short_flush(),
        MuxAck::none(),
    );
    // Raw pointers aren't `Send`; the C contract allows sharing the handle.
    let mux_addr = f.mux as usize;
    let sink = AsyncSink::leaked();
    let barrier = Arc::new(Barrier::new(THREADS));
    let workers: Vec<_> = (0..THREADS)
        .map(|t| {
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let mux = mux_addr as Mux;
                let records = json_records(t * PER_THREAD..(t + 1) * PER_THREAD);
                barrier.wait();
                // Even threads ingest blocking, odd ones async.
                let mut ids = Vec::new();
                for json in &records {
                    if t % 2 == 0 {
                        ids.push(ingest_json(mux, json));
                    } else {
                        Ingest::Json(json.as_ptr()).start(mux, sink);
                    }
                }
                ids
            })
        })
        .collect();
    let mut ids: Vec<i64> = workers
        .into_iter()
        .flat_map(|worker| worker.join().unwrap())
        .collect();

    let total = THREADS * PER_THREAD;
    let completions = sink.wait_for(total / 2, "every async ingest calls back");
    assert!(completions.iter().all(|c| c.success));
    ids.extend(completions.iter().map(|c| c.value));
    assert_eq!(ids.iter().collect::<HashSet<_>>().len(), total);
    let lanes: HashSet<usize> = ids.iter().map(|&id| stream_index(id)).collect();
    assert_eq!(lanes, HashSet::from([0, 1]), "both lanes are used");
    let all = sorted_strings(&json_records(0..total));
    assert_eq!(unacked_json_payloads(f.mux), all);
    f.free();
}

// ---------------------------------------------------------------------------
// Locks: calls racing a writer
// ---------------------------------------------------------------------------

/// Runs `writer` ("close" or "get_unacked_records") on a thread; the thread
/// returns `(success, message, recovered payloads)`.
fn spawn_writer(
    writer: &'static str,
    mux: Mux,
) -> std::thread::JoinHandle<(bool, String, Vec<String>)> {
    let mux_addr = mux as usize;
    std::thread::spawn(move || {
        let mux = mux_addr as Mux;
        let mut result = new_result();
        let recovered = if writer == "close" {
            zerobus_multiplexed_stream_close(mux, &mut result);
            Vec::new()
        } else {
            take_json_payloads(zerobus_multiplexed_stream_get_unacked_records(
                mux,
                &mut result,
            ))
        };
        (result.success, take_message(&mut result), recovered)
    })
}

/// A flush on a thread, which then holds the read lock.
fn spawn_flush(mux: Mux) -> std::thread::JoinHandle<()> {
    let mux_addr = mux as usize;
    let flusher = std::thread::spawn(move || {
        outcome(|r| zerobus_multiplexed_stream_flush(mux_addr as Mux, r));
    });
    // No hook shows when the flush holds the lock; give it time to.
    std::thread::sleep(Duration::from_millis(100));
    flusher
}

/// Calls racing `close` or `get_unacked_records`, queued behind a flush or
/// holding the lock in their own flush. The readers return at once: async
/// ingests report "closing" through one callback before the writer finishes;
/// the async wait and flush wait for the writer, then report the real
/// outcome; a blocking ingest fails. No rejected record is sent or
/// recovered. An async wait on the record the writer is flushing succeeds.
/// With no writer, a flush holding the read lock doesn't block async readers.
#[test]
fn calls_racing_a_close() {
    for writer in ["close", "get_unacked_records"] {
        for queued in [true, false] {
            let what = format!("{writer}, {}", if queued { "queued" } else { "held" });
            let gated = GatedMux::new("ffi.mux.closing", &json_options(), MuxAck::none(), vec![]);
            // Queued: a flush holds the read lock and the writer waits.
            // Held: the writer holds the lock, blocked in its own flush.
            let flusher = queued.then(|| spawn_flush(gated.mux));
            let writer_thread = spawn_writer(writer, gated.mux);
            wait_until("the writer takes or queues for the lock", || {
                zerobus_multiplexed_stream_is_closed(gated.mux)
            });

            let mut waiting = Vec::new();
            for reader in readers(gated.r0_id) {
                let name = reader.name();
                let sink = AsyncSink::leaked();
                let (returned, _, message) = prompt(name, || reader.call_async(gated.mux, sink));
                assert!(returned, "{what}: {name} must return true: {message}");
                if matches!(reader, Reader::Wait(_) | Reader::Flush) {
                    waiting.push((name, sink));
                    continue;
                }
                // The gate is still closed, so the writer has not finished.
                let done = sink.one(name);
                assert!(!done.success && !done.retryable, "{what}: {name}: {done:?}");
                assert!(
                    done.message.contains("MultiplexedStream is closing"),
                    "{what}: {name}: {done:?}"
                );
            }
            for (name, sink) in &waiting {
                sink.assert_never_called(&format!("{what}: {name} before release"));
            }

            // Decides only whether the blocking ingest below waits behind the
            // writer or finds the mux already closed; both must fail.
            let gate = Arc::clone(&gated.gate);
            let releaser = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(200));
                gate.release();
            });
            let late = json_record(2);
            let (id, _, retryable, message) = Ingest::Json(late.as_ptr()).call(gated.mux);
            assert!(
                id == -1 && !retryable && !message.is_empty(),
                "{what}: {message}"
            );
            releaser.join().unwrap();
            let (_, _, recovered) = writer_thread.join().unwrap();
            if let Some(flusher) = flusher {
                flusher.join().unwrap();
            }

            for (name, sink) in waiting {
                let done = sink.one(name);
                if name == "wait_for_message_id_async" {
                    // The writer's flush acked r0, and the wait checks the
                    // acked offset first.
                    assert!(done.success && done.value == 1, "{what}: {done:?}");
                    continue;
                }
                let (_, retryable, message) = Reader::Flush.call_blocking(gated.mux);
                assert!(!done.success && done.value == 0, "{what}: {done:?}");
                assert_eq!(
                    (done.retryable, done.message.as_str()),
                    (retryable, message.as_str()),
                    "{what}: flush_async must match the blocking flush"
                );
                assert!(message.contains("MultiplexedStream is closed"), "{message}");
            }
            // Only r0 was ever sent (and acked); no rejected record is
            // recoverable.
            assert!(recovered.is_empty(), "{what}: {recovered:?}");
            assert!(unacked_json_payloads(gated.mux).is_empty(), "{what}");
            assert_eq!(gated.writes(), 1, "{what}: a rejected record was sent");
            gated.free();
        }
    }

    // An async wait made while the writer holds the lock, on r0 (acked
    // before) and on r1 (the record the writer's flush waits for).
    for writer in ["close", "get_unacked_records"] {
        for target in ["r0", "r1"] {
            let what = format!("{writer}, wait on {target}");
            let gate = Arc::new(MockResponseGate::new());
            let responses = vec![lane_open(), ack(0), gated_ack(1, &gate)];
            let f = Fixture::new(
                "ffi.mux.wait.closing",
                responses,
                1,
                &json_options(),
                MuxAck::none(),
            );
            let r0_id = ingest_json(f.mux, &json_record(0));
            assert!(ok("wait r0", |r| {
                zerobus_multiplexed_stream_wait_for_message_id(f.mux, r0_id, r)
            }));
            let r1_id = ingest_json(f.mux, &json_record(1));
            f.wait_for_writes(2);
            let writer_thread = spawn_writer(writer, f.mux);
            wait_until("the writer takes the lock", || {
                zerobus_multiplexed_stream_is_closed(f.mux)
            });
            // No hook shows when the writer is inside its flush; give it time to.
            std::thread::sleep(Duration::from_millis(100));

            let sink = AsyncSink::leaked();
            let id = if target == "r0" { r0_id } else { r1_id };
            let (returned, _, message) = prompt(&what, || wait_async(f.mux, id, sink));
            assert!(returned, "{what}: {message}");
            sink.assert_never_called(&format!("{what}: before release"));
            gate.release();
            let (success, message, recovered) = writer_thread.join().unwrap();
            assert!(success, "{what}: {writer}: {message}");
            assert!(recovered.is_empty(), "{what}: {recovered:?}");
            let done = sink.one(&what);
            assert!(done.success && done.value == 1, "{what}: {done:?}");
            f.free();
        }
    }

    // No writer: a flush holds the read lock, and every async reader still
    // returns at once. The two JSON ingests get offsets 1 and 2.
    let options = json_short_flush();
    let gated = GatedMux::new(
        "ffi.mux.reader",
        &options,
        MuxAck::none(),
        vec![ack(1), ack(2)],
    );
    let flusher = spawn_flush(gated.mux);
    let sinks: Vec<_> = readers(gated.r0_id)
        .into_iter()
        .map(|reader| {
            let sink = AsyncSink::leaked();
            let returned = prompt(reader.name(), || reader.call_async(gated.mux, sink));
            expect_returned_true(returned, reader.name());
            (reader, sink)
        })
        .collect();
    gated.gate.release();
    flusher.join().unwrap();
    for (reader, sink) in sinks {
        let (name, done) = (reader.name(), sink.one(reader.name()));
        match reader {
            // A protobuf record on the JSON mux, rejected in the task.
            Reader::Ingest(Ingest::Proto(_) | Ingest::Protos(..)) => assert!(
                done.value == -1 && done.message.contains(RECORD_TYPE_MISMATCH),
                "{name}: {done:?}"
            ),
            Reader::Ingest(_) => assert!(done.success, "{name}: {done:?}"),
            _ => assert!(done.success && done.value == 1, "{name}: {done:?}"),
        }
    }
    close_ok(gated.mux);
    gated.free();
}

/// `close_async` and `get_unacked_records_async` don't block the caller
/// while a reader holds the lock, and finish after it; they release the
/// write lock before their callback, so a flush during it isn't blocked.
#[test]
fn async_close_and_unacked_wait_for_readers() {
    for writer in ["close_async", "get_unacked_records_async"] {
        let options = with(json_short_flush(), |o| o.max_inflight_requests = 1);
        // The close variant acks r1; get_unacked_records leaves it unacked.
        let then = if writer == "close_async" {
            vec![ack(1)]
        } else {
            vec![]
        };
        let gated = GatedMux::new("ffi.mux.async.writer", &options, MuxAck::none(), then);

        // r1 waits for the only in-flight slot, holding the read lock.
        let (r1_sink, writer_sink) = (AsyncSink::leaked(), AsyncSink::leaked());
        let r1 = json_record(1);
        Ingest::Json(r1.as_ptr()).start(gated.mux, r1_sink);
        let (returned, _, message) = prompt(writer, || async_op(writer, gated.mux, writer_sink));
        assert!(returned, "{writer}: {message}");
        writer_sink.assert_never_called(&format!("{writer} while r1 holds the lock"));

        gated.gate.release();
        assert!(r1_sink.one("r1 callback").success);
        let done = writer_sink.one(writer);
        assert!(done.success, "{done:?}");
        if writer == "close_async" {
            assert_eq!(done.value, 1, "{done:?}");
        } else {
            assert_eq!(done.payloads, sorted_strings([&r1]));
        }
        gated.free();

        // The callback blocks; a flush meanwhile returns within 1 s.
        let f = Fixture::new(
            "ffi.mux.async.guard",
            vec![],
            1,
            &json_options(),
            MuxAck::none(),
        );
        let sink = AsyncSink::blocking();
        expect_returned_true(async_op(writer, f.mux, sink), writer);
        sink.wait_entered();
        let flush = zerobus_multiplexed_stream_flush;
        let (flushed, _, message) = call_within(Duration::from_secs(1), f.mux, flush, "flush");
        assert!(!flushed, "{writer}: flush on a closed mux: {message}");
        sink.release();
        f.free();
    }
}

// ---------------------------------------------------------------------------
// `_async` calls
// ---------------------------------------------------------------------------

/// Async creates return at once; the callback brings a working handle whose
/// acks carry the returned IDs.
#[test]
fn async_creates_return_before_lanes_open() {
    let (runtime, server, url) = start_server();
    let (oauth, uc_url) = runtime.block_on(start_mock_oauth_server());
    runtime.block_on(oauth.set_responses(vec![MockTokenResponse::ok("token-1")]));
    let sdk = create_sdk_with_uc(&url, &uc_url);
    let options = json_options();
    for (table, use_oauth) in [
        ("ffi.mux.async_headers", false),
        ("ffi.mux.async_oauth", true),
    ] {
        // Each lane's open takes a second; each lane acks its offset 0.
        let open = MockResponse::CreateStream {
            stream_id: "lane".into(),
            delay_ms: 1000,
        };
        runtime.block_on(server.inject_responses(table, vec![open, ack(0)]));
        let (ack_log, sink) = (AckLog::leaked(), AsyncSink::leaked());
        let started = Instant::now();
        let ack = MuxAck::log(ack_log);
        let (returned, _, message) =
            create_async(sdk, use_oauth, table, 2, &options, None, ack, sink);
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "{table}: create blocked"
        );
        assert!(returned, "{table}: {message}");

        let mux = created(sink, table);
        let mut ids: Vec<i64> = json_records(0..2)
            .iter()
            .map(|json| ingest_json(mux, json))
            .collect();
        flush_ok(mux);
        close_ok(mux);
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

/// The four async ingests report distinct IDs, copy their input before
/// returning, report an empty batch as (-2, success), and return while their
/// record still waits for capacity.
#[test]
fn async_ingests_report_ids_copy_input_and_return_early() {
    // JSON: single and batch, then an empty batch; input scrubbed on return.
    let f = Fixture::new(
        "ffi.mux.async.json",
        vec![],
        2,
        &json_short_flush(),
        MuxAck::none(),
    );
    let originals = json_records(0..3);
    let mut bufs: Vec<Vec<u8>> = originals
        .iter()
        .map(|s| s.as_bytes_with_nul().to_vec())
        .collect();
    let mut ptrs: Vec<*const c_char> = bufs.iter().map(|b| b.as_ptr() as *const c_char).collect();
    let sinks = [
        AsyncSink::leaked(),
        AsyncSink::leaked(),
        AsyncSink::leaked(),
    ];
    Ingest::Json(ptrs[0]).start(f.mux, sinks[0]);
    Ingest::Jsons(&ptrs[1..]).start(f.mux, sinks[1]);
    ptrs.fill(ptr::null());
    bufs.iter_mut().for_each(|b| scrub(b));
    Ingest::Jsons(&[]).start(f.mux, sinks[2]);
    let ids: HashSet<i64> = sinks[..2]
        .iter()
        .map(|sink| {
            let done = sink.one("JSON ingest callback");
            assert!(done.success, "{}", done.message);
            done.value
        })
        .collect();
    assert_eq!(ids.len(), 2, "distinct IDs: {ids:?}");
    let empty = sinks[2].one("empty JSON batch callback");
    assert!(empty.success && empty.value == -2, "{empty:?}");
    assert_eq!(unacked_json_payloads(f.mux), sorted_strings(&originals));
    f.free();

    // Protobuf: single and batch copy bytes, pointers and lengths.
    let (_runtime, _server, url) = start_server();
    let options = with(zerobus_get_default_config(), |o| {
        o.recovery = false;
        o.flush_timeout_ms = 500;
    });
    let sdk = create_sdk(&url);
    let descriptor = create_test_descriptor_proto().unwrap().encode_to_vec();
    let mux = create_mux(sdk, "ffi.mux.async.proto", Some(&descriptor), 1, &options);
    let originals: Vec<Vec<u8>> = vec![vec![0x00, 0xff, 0x01], vec![0x7f], vec![0x10, 0x20]];
    let mut bufs = originals.clone();
    let (mut ptrs, mut lens): (Vec<_>, Vec<_>) =
        bufs[1..].iter().map(|b| (b.as_ptr(), b.len())).unzip();
    let sinks = [
        AsyncSink::leaked(),
        AsyncSink::leaked(),
        AsyncSink::leaked(),
    ];
    Ingest::Proto(&bufs[0]).start(mux, sinks[0]);
    Ingest::Protos(&ptrs, &lens).start(mux, sinks[1]);
    ptrs.fill(ptr::null());
    lens.fill(0);
    bufs.iter_mut().for_each(|b| b.fill(0xee));
    Ingest::Protos(&[], &[]).start(mux, sinks[2]);
    let (first, batch) = (sinks[0].one("proto single"), sinks[1].one("proto batch"));
    assert!(first.success && batch.success, "{first:?} {batch:?}");
    assert_ne!(first.value, batch.value);
    let empty = sinks[2].one("empty proto batch callback");
    assert!(empty.success && empty.value == -2, "{empty:?}");
    let records = ok("get_unacked_records", |r| {
        zerobus_multiplexed_stream_get_unacked_records(mux, r)
    });
    let mut expected = originals;
    expected.sort();
    assert_eq!(take_payloads(records, false), expected);
    free_both(sdk, mux);

    // Returns while blocked on capacity: r0's ack is gated, one slot.
    let options = with(json_options(), |o| o.max_inflight_requests = 1);
    let gated = GatedMux::new(
        "ffi.mux.async.capacity",
        &options,
        MuxAck::none(),
        vec![ack(1)],
    );
    let sink = AsyncSink::leaked();
    let mux_addr = gated.mux as usize;
    let returned = within(
        Duration::from_secs(1),
        "an async ingest blocked on capacity",
        move || {
            let r1 = json_record(1);
            Ingest::Json(r1.as_ptr())
                .call_async(mux_addr as Mux, sink)
                .0
        },
    );
    assert!(returned);
    sink.assert_never_called("an ingest still waiting for capacity");
    gated.gate.release();
    assert!(sink.one("r1 callback").success);
    close_ok(gated.mux);
    gated.free();
}

/// Each async call reports through its callback what its blocking
/// counterpart returns: on a mux closed by `close` or by a retryable lane
/// failure, and an in-task wrong record type is reported to that call only,
/// not stored for the next flush.
#[test]
fn async_errors_are_reported_per_call() {
    for lane_failure in [false, true] {
        let what = if lane_failure {
            "lane failure"
        } else {
            "plain close"
        };
        // The lane failure must stay terminal; the plain close needs recovery on.
        let (table, responses, options) = if lane_failure {
            let failing = vec![lane_open(), lane_error(Status::unavailable("lane down"))];
            (
                "ffi.mux.async.closed.failure",
                failing,
                json_options_terminal(),
            )
        } else {
            ("ffi.mux.async.closed", vec![], json_options())
        };
        let options = with(options, |o| o.flush_timeout_ms = 500);
        let f = Fixture::new(table, responses, 1, &options, MuxAck::none());
        let r0 = json_record(0);
        let r0_id = if lane_failure {
            let id = ingest_json(f.mux, &r0);
            let (flushed, retryable, message) =
                outcome(|r| zerobus_multiplexed_stream_flush(f.mux, r));
            assert!(!flushed, "flush after the lane failure");
            assert!(
                retryable,
                "an Unavailable lane failure is retryable: {message}"
            );
            id
        } else {
            close_ok(f.mux);
            0
        };
        assert!(zerobus_multiplexed_stream_is_closed(f.mux));
        let writes_before = f.writes();

        for reader in readers(r0_id) {
            let name = reader.name();
            let sink = AsyncSink::leaked();
            let (returned, _, message) = reader.call_async(f.mux, sink);
            assert!(returned, "{what}: {name} must return true: {message}");
            let done = sink.one(&format!("{what}: {name}"));
            let (success, retryable, expected) = reader.call_blocking(f.mux);
            assert!(!success, "{what}: blocking {name} succeeded");
            assert!(!done.success, "{what}: {name}: {}", done.message);
            assert_eq!(
                (done.retryable, done.message.as_str()),
                (retryable, expected.as_str()),
                "{what}: {name} must report what its blocking counterpart returns"
            );
        }
        assert_eq!(
            f.writes(),
            writes_before,
            "{what}: a rejected record reached the server"
        );
        let expected = if lane_failure {
            sorted_strings([&r0])
        } else {
            Vec::new()
        };
        assert_eq!(unacked_json_payloads(f.mux), expected, "{what}");
        f.free();
    }

    // A wrong record type, found in the task: one callback for that call.
    let (runtime, server, url) = start_server();
    let (sdk, mux) = json_mux_with_acks(&runtime, &server, &url, "ffi.mux.async.wrong.type", 1);
    let proto: &[u8] = b"proto";
    let (ptrs, lens) = ([proto.as_ptr()], [proto.len()]);
    for ingest in [Ingest::Proto(proto), Ingest::Protos(&ptrs, &lens)] {
        let sink = AsyncSink::leaked();
        ingest.start(mux, sink);
        let (what, done) = (ingest.name(), sink.one(ingest.name()));
        assert!(
            done.value == -1 && !done.success && !done.retryable,
            "{what}: {done:?}"
        );
        assert!(
            done.message.contains(RECORD_TYPE_MISMATCH),
            "{what}: {done:?}"
        );
    }
    ingest_json(mux, &json_record(0));
    let flush = call_within(
        Duration::from_secs(2),
        mux,
        zerobus_multiplexed_stream_flush,
        "flush",
    );
    expect_returned_true(flush, "flush after per-call errors");
    close_ok(mux);
    free_both(sdk, mux);
}

/// A flush covers the async ingests accepted before it: a loop of them, one
/// still waiting for capacity (with `flush_async`), a concurrent producer
/// (the flush still returns), and a close right after them (none lost).
#[test]
fn flush_covers_async_ingests_accepted_before_it() {
    // A loop, then one flush: every record is sent before it returns.
    const RECORDS: usize = 50;
    let ack_log = AckLog::leaked();
    let responses = lane_with_acks(RECORDS as i64);
    let ack = MuxAck::log(ack_log);
    let f = Fixture::new("ffi.mux.async.flush", responses, 1, &json_options(), ack);
    let sink = AsyncSink::leaked();
    for json in json_records(0..RECORDS) {
        Ingest::Json(json.as_ptr()).start(f.mux, sink);
    }
    flush_ok(f.mux);
    assert_eq!(
        f.writes(),
        RECORDS as u64,
        "flush returns only after every accepted async ingest is sent"
    );
    let completions = sink.wait_for(RECORDS, "every async ingest calls back");
    assert!(completions.iter().all(|c| c.success));
    wait_until("every record is acknowledged", || {
        ack_log.ack_count() >= RECORDS
    });
    close_ok(f.mux);
    f.free();

    // One still waiting for capacity: flush_async covers it.
    let (gate_r0, gate_r1) = (
        Arc::new(MockResponseGate::new()),
        Arc::new(MockResponseGate::new()),
    );
    let responses = vec![lane_open(), gated_ack(0, &gate_r0), gated_ack(1, &gate_r1)];
    let options = with(json_options(), |o| o.max_inflight_requests = 1);
    let f = Fixture::new(
        "ffi.mux.flush.async.capacity",
        responses,
        1,
        &options,
        MuxAck::none(),
    );
    let [r0, r1]: [CString; 2] = json_records(0..2).try_into().unwrap();
    ingest_json(f.mux, &r0);
    f.wait_for_writes(1);
    let (ingest_sink, flush_sink) = (AsyncSink::leaked(), AsyncSink::leaked());
    Ingest::Json(r1.as_ptr()).start(f.mux, ingest_sink);
    expect_returned_true(async_op("flush_async", f.mux, flush_sink), "flush_async");
    // r0's ack lets r1 be sent; r1's ack is still held.
    gate_r0.release();
    f.wait_for_writes(2);
    flush_sink.assert_never_called("flush_async before r1 is acked");
    gate_r1.release();
    let done = flush_sink.one("flush_async");
    assert!(done.success && done.value == 1, "{done:?}");
    assert!(ingest_sink.one("r1 async ingest").success);
    close_ok(f.mux);
    f.free();

    // A concurrent producer: the flush returns within 5 s.
    let options = with(json_options(), |o| o.flush_timeout_ms = 1000);
    let f = Fixture::new(
        "ffi.mux.async.producer",
        vec![],
        1,
        &options,
        MuxAck::none(),
    );
    let mux_addr = f.mux as usize;
    let sink = AsyncSink::leaked();
    let stop = Arc::new(AtomicBool::new(false));
    let produced = Arc::new(AtomicUsize::new(0));
    let producer = {
        let (stop, produced) = (Arc::clone(&stop), Arc::clone(&produced));
        std::thread::spawn(move || {
            let mut accepted = Vec::new();
            for i in 0.. {
                if stop.load(Ordering::SeqCst) {
                    break;
                }
                let json = json_record(i);
                if Ingest::Json(json.as_ptr())
                    .call_async(mux_addr as Mux, sink)
                    .0
                {
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
    let flush = zerobus_multiplexed_stream_flush;
    call_within(
        Duration::from_secs(5),
        f.mux,
        flush,
        "flush under a concurrent producer",
    );
    stop.store(true, Ordering::SeqCst);
    let mut accepted = producer.join().unwrap();
    accepted.sort();
    assert!(!accepted.is_empty());
    assert_eq!(unacked_json_payloads(f.mux), accepted);
    sink.wait_for(accepted.len(), "every accepted async ingest calls back");
    f.free();

    // A close right after 200 async ingests: none is lost.
    const CLOSE_RECORDS: usize = 200;
    let f = Fixture::new(
        "ffi.mux.async.close",
        vec![],
        1,
        &json_short_flush(),
        MuxAck::none(),
    );
    let sink = AsyncSink::leaked();
    let records = json_records(0..CLOSE_RECORDS);
    for json in &records {
        Ingest::Json(json.as_ptr()).start(f.mux, sink);
    }
    // The server never acks, so close fails on the flush timeout.
    close_quietly(f.mux);
    let completions = sink.wait_for(CLOSE_RECORDS, "every async ingest calls back");
    assert!(
        completions.iter().all(|c| c.success),
        "an accepted ingest failed"
    );
    assert_eq!(unacked_json_payloads(f.mux), sorted_strings(&records));
    f.free();
}

/// An async ingest releases its flush ticket and read guard before its
/// callback runs: a flush and a close called while the callback is still
/// running don't wait for it.
///
/// The ingest is rejected in its task (a protobuf record on a JSON mux), so
/// nothing is queued; `blocking_async_callback_does_not_hold_its_own_send`
/// covers a queued record.
#[test]
fn async_ingest_releases_ticket_and_guard_before_callback() {
    let (runtime, server, url) = start_server();
    let (sdk, mux) = json_mux_with_acks(&runtime, &server, &url, "ffi.mux.async.release", 0);
    let sink = AsyncSink::blocking();
    Ingest::Proto(b"proto").start(mux, sink);
    sink.wait_entered();
    for (what, call) in [
        ("flush", zerobus_multiplexed_stream_flush as MuxCall),
        ("close", zerobus_multiplexed_stream_close),
    ] {
        let (returned, _, message) = call_within(Duration::from_secs(1), mux, call, what);
        assert!(returned, "{what} during the callback: {message}");
    }
    sink.release();
    let done = sink.one("ingest callback");
    assert!(
        done.value == -1 && done.message.contains(RECORD_TYPE_MISMATCH),
        "{done:?}"
    );
    free_both(sdk, mux);
}

/// A blocking `_async` ingest callback doesn't hold up its own record: the
/// record still reaches the server, and a flush still completes, while the
/// callback runs.
#[test]
fn blocking_async_callback_does_not_hold_its_own_send() {
    for count in [1, 2] {
        let f = Fixture::new(
            "ffi.mux.async.blocking",
            lane_with_acks(1),
            1,
            &json_options(),
            MuxAck::none(),
        );
        let sink = AsyncSink::blocking();
        let records = json_records(0..count);
        let ptrs: Vec<*const c_char> = records.iter().map(|r| r.as_ptr()).collect();
        let ingest = if count == 1 {
            Ingest::Json(ptrs[0])
        } else {
            Ingest::Jsons(&ptrs)
        };
        let what = ingest.name();
        ingest.start(f.mux, sink);
        sink.wait_entered();
        let deadline = Instant::now() + Duration::from_secs(1);
        while f.writes() < count as u64 {
            assert!(
                Instant::now() < deadline,
                "{what}: the record wasn't sent while its callback ran"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let flush = zerobus_multiplexed_stream_flush;
        let during = "flush during the callback";
        let (flushed, _, message) = call_within(Duration::from_secs(1), f.mux, flush, during);
        assert!(flushed, "{what}: {message}");
        sink.release();
        assert!(sink.one(what).success, "{what}");
        close_ok(f.mux);
        f.free();
    }
}

/// An ordinary (non-multiplexed) stream still delivers acks to the C
/// callbacks set in `options`; the bindings' ordinary ack path depends on it.
#[test]
fn ordinary_stream_delivers_acks_to_options_callbacks() {
    const TABLE: &str = "ffi.ordinary.acks";
    let (runtime, server, url) = start_server();
    runtime.block_on(server.inject_responses(TABLE, lane_with_acks(2)));
    let ack_log = AckLog::leaked();
    let options = with(json_options(), |o| {
        o.ack_on_ack = Some(log_ack);
        o.ack_on_error = Some(log_error);
        o.ack_user_data = ack_log.user_data();
    });
    let sdk = create_sdk(&url);
    let table = CString::new(TABLE).unwrap();
    let stream = ok("zerobus_sdk_create_stream_with_headers_provider", |r| {
        zerobus_sdk_create_stream_with_headers_provider(
            sdk,
            table.as_ptr(),
            ptr::null(),
            0,
            empty_headers,
            ptr::null_mut(),
            None,
            &options,
            r,
        )
    });
    let mut offsets: Vec<i64> = json_records(0..2)
        .iter()
        .map(|json| {
            ok("zerobus_stream_ingest_json_record", |r| {
                zerobus_stream_ingest_json_record(stream, json.as_ptr(), r)
            })
        })
        .collect();
    assert!(ok("zerobus_stream_flush", |r| zerobus_stream_flush(
        stream, r
    )));
    assert!(ok("zerobus_stream_close", |r| zerobus_stream_close(
        stream, r
    )));
    zerobus_stream_free(stream);
    zerobus_sdk_free(sdk);
    // Close drains the callback worker, so every ack has been delivered.
    offsets.sort();
    assert_eq!(ack_log.sorted_acks(), offsets);
    assert!(ack_log.errors().is_empty());
}

// ---------------------------------------------------------------------------
// _nowait group (pending decision)
//
// Every test below covers only the `_nowait` functions. If `_nowait` is
// dropped, this whole section goes; if it stays, it gets compacted.
// ---------------------------------------------------------------------------

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

/// `_nowait` marshals protobuf and JSON, single and batch, and copies its
/// input before returning; an empty batch succeeds; a NULL element or an
/// invalid record fails the whole call; a flush covers what was accepted.
#[test]
fn nowait_marshals_input_and_rejects_bad_input() {
    // Protobuf, with NULL options.
    let (runtime, server, url) = start_server();
    runtime.block_on(server.inject_responses("ffi.mux.nowait.proto", lane_with_acks(2)));
    let sdk = create_sdk(&url);
    let descriptor = create_test_descriptor_proto().unwrap().encode_to_vec();
    let mux = create_mux_with(
        sdk,
        "ffi.mux.nowait.proto",
        Some(&descriptor),
        1,
        ptr::null(),
        MuxAck::none(),
    );
    let mut result = new_result();
    let single = b"nowait";
    zerobus_multiplexed_stream_ingest_proto_record_nowait(
        mux,
        single.as_ptr(),
        single.len(),
        &mut result,
    );
    expect_success(&mut result, "ingest_proto_record_nowait");
    let batch: [&[u8]; 2] = [b"nowait-batch-0", b"nowait-batch-1"];
    let ptrs: Vec<*const u8> = batch.iter().map(|r| r.as_ptr()).collect();
    let lens: Vec<usize> = batch.iter().map(|r| r.len()).collect();
    zerobus_multiplexed_stream_ingest_proto_records_nowait(
        mux,
        ptrs.as_ptr(),
        lens.as_ptr(),
        2,
        &mut result,
    );
    expect_success(&mut result, "ingest_proto_records_nowait");
    zerobus_multiplexed_stream_ingest_proto_records_nowait(
        mux,
        ptrs.as_ptr(),
        lens.as_ptr(),
        0,
        &mut result,
    );
    expect_success(&mut result, "ingest_proto_records_nowait (empty)");
    let (bad_ptrs, bad_lens) = ([single.as_ptr(), ptr::null()], [single.len(), 4]);
    zerobus_multiplexed_stream_ingest_proto_records_nowait(
        mux,
        bad_ptrs.as_ptr(),
        bad_lens.as_ptr(),
        2,
        &mut result,
    );
    let message =
        expect_permanent_failure(&mut result, "ingest_proto_records_nowait ([valid, NULL])");
    assert_eq!(message, "Invalid data pointer");
    flush_ok(mux);
    assert_eq!(
        runtime.block_on(server.get_write_count()),
        3,
        "flush returns only after the _nowait records are sent"
    );
    close_ok(mux);
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);

    // JSON: the caller's buffers and pointer array are overwritten as soon as
    // each call returns; bad input is rejected.
    let (runtime, server, url) = start_server();
    let mut options = json_options();
    options.flush_timeout_ms = 500;
    let sdk = create_sdk(&url);
    let mux = create_mux(sdk, "ffi.mux.nowait.json", None, 1, &options);
    let records = json_records(0..3);
    let mut bufs: Vec<Vec<u8>> = records
        .iter()
        .map(|s| s.as_bytes_with_nul().to_vec())
        .collect();
    let mut ptrs: Vec<*const c_char> = bufs[..2]
        .iter()
        .map(|b| b.as_ptr() as *const c_char)
        .collect();
    zerobus_multiplexed_stream_ingest_json_records_nowait(mux, ptrs.as_ptr(), 2, &mut result);
    expect_success(&mut result, "ingest_json_records_nowait");
    ptrs.fill(ptr::null());
    bufs[..2].iter_mut().for_each(|b| scrub(b));
    zerobus_multiplexed_stream_ingest_json_record_nowait(
        mux,
        bufs[2].as_ptr() as *const c_char,
        &mut result,
    );
    expect_success(&mut result, "ingest_json_record_nowait");
    scrub(&mut bufs[2]);
    let valid = CString::new(r#"{"rejected": true}"#).unwrap();
    for (case, bad) in [("NULL", ptr::null()), ("invalid UTF-8", c"\xff".as_ptr())] {
        zerobus_multiplexed_stream_ingest_json_record_nowait(mux, bad, &mut result);
        expect_permanent_failure(&mut result, &format!("ingest_json_record_nowait ({case})"));
        let batch = [valid.as_ptr(), bad];
        zerobus_multiplexed_stream_ingest_json_records_nowait(mux, batch.as_ptr(), 2, &mut result);
        expect_permanent_failure(&mut result, &format!("ingest_json_records_nowait ({case})"));
    }
    wait_for_writes(&runtime, &server, 3);
    assert_eq!(unacked_json_payloads(mux), sorted_strings(&records));
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}

/// `_nowait` reports a closing or closed mux instead of dropping the record:
/// "closing" while a close is queued, and "closed" afterwards, even for an
/// empty batch.
#[test]
fn nowait_reports_closing_and_closed_mux() {
    let gated = GatedMux::new(
        "ffi.mux.nowait.closing",
        &json_options(),
        MuxAck::none(),
        vec![],
    );
    let mux_addr = gated.mux as usize;
    let flusher = std::thread::spawn(move || {
        let mut result = new_result();
        zerobus_multiplexed_stream_flush(mux_addr as *mut _, &mut result);
        take_message(&mut result);
    });
    let closer = std::thread::spawn(move || {
        let mut result = new_result();
        zerobus_multiplexed_stream_close(mux_addr as *mut _, &mut result);
        take_message(&mut result);
    });
    wait_until("close is queued", || {
        zerobus_multiplexed_stream_is_closed(gated.mux)
    });
    let r1 = CString::new(r#"{"id": 1}"#).unwrap();
    let mut result = new_result();
    zerobus_multiplexed_stream_ingest_json_record_nowait(gated.mux, r1.as_ptr(), &mut result);
    let message = expect_permanent_failure(&mut result, "ingest_json_record_nowait (closing)");
    assert_eq!(
        message,
        "Stream is in invalid state: MultiplexedStream is closing"
    );
    gated.gate.release();
    flusher.join().unwrap();
    closer.join().unwrap();
    assert_eq!(
        gated.writes(),
        1,
        "the rejected record never reaches the server"
    );

    // Closed: every `_nowait` call reports it.
    let closed = "Stream is in invalid state: MultiplexedStream is closed";
    let ptrs = [r1.as_ptr()];
    let proto = b"proto";
    let (proto_ptrs, proto_lens) = ([proto.as_ptr()], [proto.len()]);
    let mux = gated.mux;
    type NowaitCall<'a> = (&'a str, Box<dyn Fn(&mut CResult) + 'a>);
    let calls: [NowaitCall; 5] = [
        (
            "json_record_nowait",
            Box::new(|r| zerobus_multiplexed_stream_ingest_json_record_nowait(mux, r1.as_ptr(), r)),
        ),
        (
            "json_records_nowait",
            Box::new(|r| {
                zerobus_multiplexed_stream_ingest_json_records_nowait(mux, ptrs.as_ptr(), 1, r)
            }),
        ),
        (
            "json_records_nowait (empty)",
            Box::new(|r| {
                zerobus_multiplexed_stream_ingest_json_records_nowait(mux, ptrs.as_ptr(), 0, r)
            }),
        ),
        (
            "proto_record_nowait",
            Box::new(|r| {
                zerobus_multiplexed_stream_ingest_proto_record_nowait(
                    mux,
                    proto.as_ptr(),
                    proto.len(),
                    r,
                )
            }),
        ),
        (
            "proto_records_nowait",
            Box::new(|r| {
                zerobus_multiplexed_stream_ingest_proto_records_nowait(
                    mux,
                    proto_ptrs.as_ptr(),
                    proto_lens.as_ptr(),
                    1,
                    r,
                )
            }),
        ),
    ];
    for (what, call) in &calls {
        call(&mut result);
        let message = expect_permanent_failure(&mut result, what);
        assert_eq!(message, closed, "{what}");
    }
    gated.free();
}

/// flush_async covers a `_nowait` ingest accepted before it, even one still
/// waiting for in-flight capacity.
#[test]
fn flush_async_waits_for_nowait_blocked_on_capacity() {
    let (runtime, server, url) = start_server();
    let (gate_r0, gate_r1) = (
        Arc::new(MockResponseGate::new()),
        Arc::new(MockResponseGate::new()),
    );
    let gated_ack = |offset, gate: &Arc<MockResponseGate>| MockResponse::GatedRecordAck {
        ack_up_to_offset: offset,
        gate: Arc::clone(gate),
    };
    runtime.block_on(server.inject_responses(
        "ffi.mux.nowait.flush.async",
        vec![lane_open(), gated_ack(0, &gate_r0), gated_ack(1, &gate_r1)],
    ));
    let mut options = json_options();
    options.max_inflight_requests = 1;
    let sdk = create_sdk(&url);
    let mux = create_mux(sdk, "ffi.mux.nowait.flush.async", None, 1, &options);
    let [r0, r1]: [CString; 2] = json_records(0..2).try_into().unwrap();
    ingest_json(mux, &r0);
    wait_for_writes(&runtime, &server, 1);
    // r1 waits for the only in-flight slot.
    let mut result = new_result();
    zerobus_multiplexed_stream_ingest_json_record_nowait(mux, r1.as_ptr(), &mut result);
    expect_success(&mut result, "ingest_json_record_nowait (r1)");
    let flush_sink = AsyncSink::leaked();
    assert!(async_op("flush_async", mux, flush_sink).0);
    // r0's ack lets r1 be sent; r1's ack is still held.
    gate_r0.release();
    wait_for_writes(&runtime, &server, 2);
    flush_sink.assert_never_called("flush_async before r1 is acked");
    gate_r1.release();
    let done = flush_sink.one("flush_async");
    assert!(done.success && done.value == 1, "{done:?}");
    close_ok(mux);
    zerobus_multiplexed_stream_free(mux);
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
    let provider = FreeCounter::leaked();

    let sdk = create_sdk(&url);
    let mut options = json_options();
    options.max_inflight_requests = 1;
    let (mux, mut result) = create_mux_raw(
        sdk,
        TABLE,
        None,
        1,
        &options,
        Some(provider),
        MuxAck::none(),
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
    let mux = create_mux_with(sdk, TABLE, None, 1, &options, MuxAck::log(&ack_log));
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
    let mux = create_mux_with(sdk, TABLE, None, 1, &json_options(), MuxAck::log(&ack_log));
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
    let mux = create_mux_with(sdk, TABLE, None, 1, &options, MuxAck::log(&ack_log));
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
    let ack = AckLog::leaked();

    let sdk = create_sdk(&url);
    let mut options = json_options();
    options.max_inflight_requests = 1;
    let (mux, mut result) = create_mux_raw(
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
    expect_record_type_failure(
        call_within(Duration::from_secs(2), mux, flush, "flush"),
        "first flush",
    );
    expect_returned_true(
        call_within(Duration::from_secs(2), mux, flush, "flush"),
        "second flush",
    );
    assert!(!zerobus_multiplexed_stream_is_closed(mux));

    nowait_wrong_type(mux);
    expect_record_type_failure(
        call_within(Duration::from_secs(2), mux, flush, "flush"),
        "flush after a new failure",
    );

    let r1 = CString::new(r#"{"id": 1}"#).unwrap();
    zerobus_multiplexed_stream_ingest_json_record(mux, r1.as_ptr(), &mut result);
    expect_success(&mut result, "ingest_json_record");
    expect_returned_true(
        call_within(Duration::from_secs(2), mux, flush, "flush"),
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
    expect_record_type_failure(
        call_within(Duration::from_secs(2), mux, flush, "flush"),
        "first flush",
    );
    expect_returned_true(
        call_within(Duration::from_secs(2), mux, flush, "flush"),
        "second flush",
    );

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
        call_within(
            Duration::from_secs(2),
            mux,
            zerobus_multiplexed_stream_flush,
            "flush",
        ),
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
        call_within(
            Duration::from_secs(2),
            mux,
            zerobus_multiplexed_stream_close,
            "close",
        ),
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
    let (returned, _, message) = call_within(Duration::from_secs(2), mux, flush, "flush");
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
    expect_record_type_failure(
        call_within(Duration::from_secs(2), mux, flush, "flush"),
        "second flush",
    );
    expect_returned_true(
        call_within(Duration::from_secs(2), mux, flush, "flush"),
        "third flush",
    );
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);

    // Close: its own timeout wins, and it still closes.
    let (_runtime, _server, sdk, mux, gate, _) = gated_mux(&format!("{TABLE}.close"));
    let (returned, _, message) = call_within(
        Duration::from_secs(2),
        mux,
        zerobus_multiplexed_stream_close,
        "close",
    );
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

    let (flushed, retryable, message) = call_within(
        Duration::from_secs(2),
        mux,
        zerobus_multiplexed_stream_flush,
        "flush",
    );
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

/// flush_async and close_async report a stored background `_nowait` failure.
#[test]
fn async_flush_and_close_report_stored_nowait_failure() {
    for op in ["flush_async", "close_async"] {
        let (runtime, server, url) = start_server();
        let (sdk, mux) = json_mux_with_acks(&runtime, &server, &url, "ffi.mux.async.stored", 0);
        nowait_wrong_type(mux);
        let sink = AsyncSink::leaked();
        let (returned, _, message) = if op == "flush_async" {
            async_op("flush_async", mux, sink)
        } else {
            async_op("close_async", mux, sink)
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
