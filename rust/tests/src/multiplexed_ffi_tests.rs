//! Drives the multiplexed stream C ABI against the mock gRPC server.
//!
//! The FFI blocks on its own global runtime, so these tests call it from plain
//! threads and run the mock server on a separate runtime. They test the FFI
//! layer only: C arguments, ID passing, ownership, and error mapping. The core mux
//! behavior is tested in `multiplexed_stream_tests.rs`.

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
    zerobus_multiplexed_stream_is_closed, zerobus_multiplexed_stream_wait_for_message_id,
    zerobus_sdk_builder_build, zerobus_sdk_builder_disable_tls, zerobus_sdk_builder_endpoint,
    zerobus_sdk_builder_new, zerobus_sdk_builder_unity_catalog_url,
    zerobus_sdk_create_multiplexed_stream,
    zerobus_sdk_create_multiplexed_stream_with_headers_provider, zerobus_sdk_free, CHeaders,
    CRecordArray, CResult, CStreamConfigurationOptions, CZerobusMultiplexedStream, CZerobusSdk,
};
use zerobus_ffi::{
    zerobus_sdk_create_stream_with_headers_provider, zerobus_stream_close, zerobus_stream_flush,
    zerobus_stream_free, zerobus_stream_ingest_json_record,
};
use zerobus_test_support::mock_grpc::{start_mock_server, MockResponse, MockZerobusServer};
// Short names for the ingest exports, so each call fits on one line.
use zerobus_ffi::{
    zerobus_multiplexed_stream_ingest_json_record as json1,
    zerobus_multiplexed_stream_ingest_json_records as jsons,
    zerobus_multiplexed_stream_ingest_proto_record as proto1,
    zerobus_multiplexed_stream_ingest_proto_records as protos,
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
/// message)`. `result.success` starts true, so a failure path that forgets to
/// clear it is caught: it must match the return value.
fn outcome(f: impl FnOnce(*mut CResult) -> bool) -> (bool, bool, String) {
    let mut result = CResult {
        success: true,
        ..new_result()
    };
    let returned = f(&mut result as *mut CResult);
    assert_eq!(
        result.success, returned,
        "result.success must match the return value"
    );
    (returned, result.is_retryable, take_message(&mut result))
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
const NULL_SDK: &str = "SDK pointer is null";
const NULL_ARG: &str = "Null pointer passed";
const OPTIONS_ACK: &str =
    "options->ack_on_ack and options->ack_on_error are not supported for multiplexed streams";

/// Polls `condition` until it holds, failing after 5 seconds.
fn wait_until(what: &str, condition: impl Fn() -> bool) {
    wait_until_within(Duration::from_secs(5), what, condition);
}

fn wait_until_within(bound: Duration, what: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + bound;
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting: {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
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

/// Mux ack `user_data`: records the IDs acked and failed (with each error
/// text), counts frees, flags a callback that runs after the free, and can
/// hold a callback until released.
#[derive(Default)]
struct AckLog {
    acks: Mutex<Vec<i64>>,
    errors: Mutex<Vec<(i64, String)>>,
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

    fn errors(&self) -> Vec<(i64, String)> {
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

    /// Asserts `user_data` is still owned after a settle (a negative check).
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

extern "C" fn log_error(message_id: i64, message: *const c_char, user_data: *mut c_void) {
    let log = unsafe { &*(user_data as *const AckLog) };
    let text = unsafe { CStr::from_ptr(message) }.to_string_lossy();
    log.errors.lock().unwrap().push((message_id, text.into()));
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

/// Which create function a `Create` calls.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Auth {
    OAuth,
    Headers,
}

/// One mux create call. A NULL `sdk`, a `None` table, client ID or client
/// secret, and `None` options are passed as NULL, so the argument checks can
/// be driven too.
struct Create {
    sdk: *mut CZerobusSdk,
    table: Option<CString>,
    descriptor: Option<Vec<u8>>,
    client_id: Option<CString>,
    client_secret: Option<CString>,
    stream_count: usize,
    options: Option<CStreamConfigurationOptions>,
    /// Counts the headers provider's calls and its `user_data` frees.
    provider: Option<&'static FreeCounter>,
}

impl Create {
    /// A JSON mux over `stream_count` lanes, with `json_options()`.
    fn new(sdk: *mut CZerobusSdk, table: &str, stream_count: usize) -> Self {
        Self {
            sdk,
            table: Some(CString::new(table).unwrap()),
            descriptor: None,
            client_id: Some(CString::new("client-id").unwrap()),
            client_secret: Some(CString::new("client-secret").unwrap()),
            stream_count,
            options: Some(json_options()),
            provider: None,
        }
    }

    fn with(mut self, edit: impl FnOnce(&mut Self)) -> Self {
        edit(&mut self);
        self
    }

    fn table(&self) -> *const c_char {
        self.table.as_ref().map_or(ptr::null(), |t| t.as_ptr())
    }

    fn descriptor(&self) -> (*const u8, usize) {
        self.descriptor
            .as_ref()
            .map_or((ptr::null(), 0), |d| (d.as_ptr(), d.len()))
    }

    fn options(&self) -> *const CStreamConfigurationOptions {
        self.options.as_ref().map_or(ptr::null(), |o| o)
    }

    fn client_id(&self) -> *const c_char {
        self.client_id.as_ref().map_or(ptr::null(), |c| c.as_ptr())
    }

    fn client_secret(&self) -> *const c_char {
        self.client_secret
            .as_ref()
            .map_or(ptr::null(), |c| c.as_ptr())
    }

    /// The sync create; the outcome is in `result`.
    fn sync(&self, auth: Auth, ack: MuxAck, result: *mut CResult) -> Mux {
        let ((descriptor, len), secret) = (self.descriptor(), self.client_secret());
        let (callback, user_data, free) = headers_args(self.provider);
        match auth {
            Auth::OAuth => zerobus_sdk_create_multiplexed_stream(
                self.sdk,
                self.table(),
                descriptor,
                len,
                self.client_id(),
                secret,
                self.stream_count,
                self.options(),
                ack.on_ack,
                ack.on_error,
                ack.user_data,
                ack.free_user_data,
                result,
            ),
            Auth::Headers => zerobus_sdk_create_multiplexed_stream_with_headers_provider(
                self.sdk,
                self.table(),
                descriptor,
                len,
                callback,
                user_data,
                free,
                self.stream_count,
                self.options(),
                ack.on_ack,
                ack.on_error,
                ack.user_data,
                ack.free_user_data,
                result,
            ),
        }
    }
}

/// An SDK whose endpoint and UC URL are `127.0.0.1:1`: every connect fails,
/// retryably.
fn unreachable_sdk() -> *mut CZerobusSdk {
    create_sdk_with_uc("http://127.0.0.1:1", "http://127.0.0.1:1")
}

/// A 2-lane create on `sdk` changed by `edit`, with recovery off so a
/// connect error is final.
fn failing_create(sdk: *mut CZerobusSdk, edit: fn(&mut Create)) -> Create {
    Create::new(sdk, "main.default.events", 2).with(|c| {
        c.options = Some(json_options_terminal());
        edit(c);
    })
}

/// A create row's fresh headers counter (set on `create` for a headers
/// create) and ack log, handed to the FFI with its free hook.
fn owned_user_data(
    create: Create,
    auth: Auth,
) -> (Create, &'static FreeCounter, &'static AckLog, MuxAck) {
    let (provider, ack_log) = (FreeCounter::leaked(), AckLog::leaked());
    let create = create.with(|c| c.provider = (auth == Auth::Headers).then_some(provider));
    (create, provider, ack_log, MuxAck::owned(ack_log, true))
}

/// Asserts a create row freed both `user_data` exactly once.
fn assert_row_freed(auth: Auth, provider: &FreeCounter, ack_log: &AckLog, what: &str) {
    ack_log.assert_freed_once();
    if auth == Auth::Headers {
        provider.assert_freed_once(what);
    }
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
    let create = Create::new(sdk, table, stream_count).with(|c| {
        c.descriptor = descriptor.map(<[u8]>::to_vec);
        c.options = unsafe { options.as_ref() }.copied();
    });
    let mux = ok("create", |r| create.sync(Auth::Headers, ack, r));
    assert!(!mux.is_null());
    mux
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

/// One of the four ingest calls with its input.
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
    /// `result.success` starts true, and must be false exactly when the ID is
    /// -1.
    fn call(self, mux: Mux) -> (i64, bool, bool, String) {
        let mut result = CResult {
            success: true,
            ..new_result()
        };
        let r = &mut result as *mut CResult;
        let id = match self {
            Ingest::Proto(d) => proto1(mux, d.as_ptr(), d.len(), r),
            Ingest::Protos(p, l) => protos(mux, p.as_ptr(), l.as_ptr(), p.len(), r),
            Ingest::Json(json) => json1(mux, json, r),
            Ingest::Jsons(j) => jsons(mux, j.as_ptr(), j.len(), r),
        };
        assert_eq!(
            result.success,
            id != -1,
            "{}: success vs ID {id}",
            self.name()
        );
        let message = take_message(&mut result);
        (id, result.success, result.is_retryable, message)
    }

    /// The blocking ingest, which must succeed; returns its ID.
    fn ok(self, mux: Mux) -> i64 {
        let (id, success, _, message) = self.call(mux);
        assert!(success, "{} failed: {message}", self.name());
        id
    }
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
    runtime.block_on(server.inject_responses("ffi.mux.proto", lane_with_acks(3)));
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
    // NULL is valid with length 0: an empty single record, and NULL arrays
    // for an empty batch.
    ids.push(ok("proto_record(NULL, 0)", |r| {
        proto1(mux, ptr::null(), 0, r)
    }));
    let null_batch = ok("proto_records(NULL, NULL, 0)", |r| {
        protos(mux, ptr::null(), ptr::null(), 0, r)
    });
    assert_eq!(
        null_batch, -2,
        "NULL arrays with count 0 are an empty batch"
    );
    flush_ok(mux);
    assert_eq!(runtime.block_on(server.get_write_count()), 4);
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
    let null_batch = ok("json_records(NULL, 0)", |r| jsons(f.mux, ptr::null(), 0, r));
    assert_eq!(
        null_batch, -2,
        "a NULL array with count 0 is an empty batch"
    );
    f.wait_for_writes(4);
    let recovered = unacked_json_payloads(f.mux);
    assert_eq!(recovered.len(), 4, "each record returned exactly once");
    assert_eq!(recovered, sorted_strings(&records));
    f.free();
}

/// Message IDs cross the ABI intact: the ack callback gets the IDs ingest
/// returned, and the wait passes the lane and the sub-offset through, also on
/// a real lane >= 32, whose IDs are negative.
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
        if id == lane_63 {
            assert!(!retryable && !message.is_empty(), "lane 63: {message}");
        }
    }

    close_quietly(f.mux);
    f.free();
    let mut expected = vec![ids[0], ids[1]];
    expected.sort();
    assert_eq!(ack_log.sorted_acks(), expected);

    // 33 lanes, one record each: a real lane-32 ID is acked and waited on.
    let ack_log = AckLog::leaked();
    let log = MuxAck::log(ack_log);
    let f = Fixture::new("ffi.mux.lane32", vec![ack(0)], 33, &json_short_flush(), log);
    let ids: Vec<i64> = json_records(0..33)
        .iter()
        .map(|json| ingest_json(f.mux, json))
        .collect();
    let lane_32 = *ids.iter().find(|&&id| stream_index(id) >= 32).unwrap();
    assert!(lane_32 < 0, "a lane >= 32 ID is negative: {lane_32}");
    let (waited, _, message) =
        outcome(|r| zerobus_multiplexed_stream_wait_for_message_id(f.mux, lane_32, r));
    assert!(waited, "wait on lane {}: {message}", stream_index(lane_32));
    close_quietly(f.mux);
    f.free();
    assert!(ack_log.sorted_acks().contains(&lane_32));
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
    let create = Create::new(sdk, TABLE, 2);
    let mux = ok("OAuth create", |r| {
        create.sync(Auth::OAuth, MuxAck::none(), r)
    });
    assert_eq!(oauth.mint_count(), 1, "the lanes share one token");
    let auth = runtime.block_on(server.get_auth_headers());
    assert_eq!(auth.len(), 2, "one stream per lane: {auth:?}");
    for lane in &auth {
        assert_eq!(lane.authorization.as_deref(), Some("Bearer token-1"));
        assert_eq!(lane.table_name.as_deref(), Some(TABLE));
    }
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

/// Every failing sync create reports its error (retryable only for a
/// connect failure) and frees the headers and ack `user_data` once. Each
/// export checks its SDK pointer; the string, descriptor and options checks
/// are shared, so they have one row each.
#[test]
fn sync_create_failures_report_and_free_once() {
    use Auth::*;
    type Row = (
        &'static str,
        fn(&mut Create),
        &'static [Auth],
        bool,
        &'static str,
    );
    let rows: [Row; 8] = [
        (
            "sdk=NULL",
            |c| c.sdk = ptr::null_mut(),
            &[OAuth, Headers],
            false,
            NULL_SDK,
        ),
        ("table=NULL", |c| c.table = None, &[OAuth], false, NULL_ARG),
        (
            "client_id=NULL",
            |c| c.client_id = None,
            &[OAuth],
            false,
            NULL_ARG,
        ),
        (
            "client_secret=NULL",
            |c| c.client_secret = None,
            &[OAuth],
            false,
            NULL_ARG,
        ),
        // A truncated varint, not a DescriptorProto.
        (
            "bad descriptor",
            |c| c.descriptor = Some(vec![0xff; 3]),
            &[OAuth],
            false,
            "failed to decode Protobuf message",
        ),
        (
            "options->ack_on_ack",
            |c| c.options.as_mut().unwrap().ack_on_ack = Some(log_ack),
            &[OAuth],
            false,
            OPTIONS_ACK,
        ),
        (
            "options->ack_on_error",
            |c| c.options.as_mut().unwrap().ack_on_error = Some(log_error),
            &[OAuth],
            false,
            OPTIONS_ACK,
        ),
        ("connect failure", |_| {}, &[OAuth, Headers], true, ""),
    ];
    let sdk = unreachable_sdk();
    for (case, edit, auths, retryable, expected) in rows {
        for &auth in auths {
            let what = format!("{auth:?} {case}");
            let (create, provider, ack_log, ack) = owned_user_data(failing_create(sdk, edit), auth);
            let mut result = new_result();
            let mux = create.sync(auth, ack, &mut result);
            assert!(mux.is_null() && !result.success, "{what}");
            assert_eq!(result.is_retryable, retryable, "{what}");
            let message = take_message(&mut result);
            assert!(message.contains(expected), "{what}: {message}");
            assert_row_freed(auth, provider, ack_log, &what);
        }
    }

    // A NULL `free_ack_user_data` and a NULL `result` are allowed.
    let log = AckLog::default();
    let mut result = new_result();
    let create = failing_create(sdk, |c| c.table = None);
    assert!(create.sync(OAuth, MuxAck::log(&log), &mut result).is_null());
    take_message(&mut result);
    let create = failing_create(sdk, |c| c.stream_count = 0);
    assert!(create
        .sync(OAuth, MuxAck::none(), ptr::null_mut())
        .is_null());
    zerobus_sdk_free(sdk);
}

/// Invalid C arguments fail only the call that passed them: none of a
/// rejected batch is queued, sent or recovered, and the mux stays usable.
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
    let rows = [
        (
            "json_record(NULL)",
            Ingest::Json(ptr::null()),
            "Null pointer passed",
        ),
        (
            "json_record(invalid UTF-8)",
            Ingest::Json(invalid_utf8),
            "Invalid UTF-8 string",
        ),
        (
            "json_records([valid, NULL])",
            Ingest::Jsons(&null_batch),
            "Null pointer passed",
        ),
        (
            "json_records([valid, invalid UTF-8])",
            Ingest::Jsons(&utf8_batch),
            "Invalid UTF-8 string",
        ),
        (
            "proto_records([valid, NULL])",
            Ingest::Protos(&proto_ptrs, &proto_lens),
            "Invalid data pointer",
        ),
        // A protobuf record on this JSON mux: the core's error, mapped by the
        // blocking call to -1 and a non-retryable result.
        (
            "proto_record on a JSON mux",
            Ingest::Proto(proto),
            "Record type does not match",
        ),
    ];

    for (what, ingest, expected) in rows {
        let (id, _, retryable, message) = ingest.call(f.mux);
        assert_eq!(id, -1, "{what} must be rejected");
        assert!(!retryable, "{what}: {message}");
        assert!(message.contains(expected), "{what}: {message}");
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
/// only at free; at create when both ack callbacks are NULL;
/// after a partial open, with no handle. An `on_error`-only callback is
/// installed too.
#[test]
fn ffi_owned_user_data_is_freed_once() {
    // (a) Success with callbacks: nothing is freed until free.
    {
        let what = "sync";
        let (runtime, server, url) = start_server();
        // No CreateStream entry: each lane acks its offset 0.
        runtime.block_on(server.inject_responses("ffi.mux.own", vec![ack(0)]));
        let (provider, ack_log) = (FreeCounter::leaked(), AckLog::leaked());
        let (sdk, options) = (create_sdk(&url), json_options());
        let ack = MuxAck::owned(ack_log, true);
        let create = Create::new(sdk, "ffi.mux.own", 2).with(|c| {
            c.options = Some(options);
            c.provider = Some(provider);
        });
        let mux = ok("create", |r| create.sync(Auth::Headers, ack, r));
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
    assert!(
        !zerobus_multiplexed_stream_is_closed(f.mux),
        "open after create"
    );
    close_ok(f.mux);
    assert!(
        zerobus_multiplexed_stream_is_closed(f.mux),
        "closed after close"
    );
    close_ok(f.mux); // A second close is fine.
    f.free();
    ack_log.assert_freed_once();

    // (c) Partial open: one lane opens and shares both user_datas, the other
    // is rejected. No handle; each is freed once.
    let (runtime, server, url) = start_server();
    let (oauth, uc_url) = runtime.block_on(start_mock_oauth_server());
    runtime.block_on(oauth.set_responses(vec![MockTokenResponse::ok("token-1")]));
    let sdk = create_sdk_with_uc(&url, &uc_url);
    let options = json_options_terminal();
    {
        let what = "sync headers";
        let table = "ffi.mux.partial_sync_headers";
        runtime.block_on(server.inject_responses(table, failing_lane()));
        let (provider, ack_log) = (FreeCounter::leaked(), AckLog::leaked());
        let ack = MuxAck::owned(ack_log, true);
        let create = Create::new(sdk, table, 2).with(|c| {
            c.options = Some(options);
            c.provider = Some(provider);
        });
        let mut result = new_result();
        let mux = create.sync(Auth::Headers, ack, &mut result);
        let message = expect_permanent_failure(&mut result, what);
        assert!(mux.is_null(), "{what}: {message}");
        assert!(message.contains("denied"), "{what}: {message}");
        ack_log.assert_freed_once();
        provider.assert_freed_once(what);
    }
    zerobus_sdk_free(sdk);

    // (d) Only `on_error` set: a failed record still reaches it. The lane
    // fails for good: flush and close report it (close still closes), and
    // the protobuf record is recovered byte for byte (a NUL and a non-UTF-8
    // byte included).
    let ack_log = AckLog::leaked();
    let ack = MuxAck {
        on_ack: None,
        ..MuxAck::log(ack_log)
    };
    let table = "ffi.mux.ack.error.only";
    let (runtime, server, url) = start_server();
    runtime.block_on(server.inject_responses(table, failing_lane()));
    let sdk = create_sdk(&url);
    let descriptor = create_test_descriptor_proto().unwrap().encode_to_vec();
    let options = with(json_options_terminal(), |o| o.record_type = 1);
    let mux = create_mux_with(sdk, table, Some(&descriptor), 1, &options, ack);
    let record: &[u8] = b"a\0\xffb";
    let id = Ingest::Proto(record).ok(mux);
    let (flushed, retryable, message) = outcome(|r| zerobus_multiplexed_stream_flush(mux, r));
    assert!(!flushed && !retryable, "flush: {message}");
    assert!(message.contains("denied"), "flush: {message}");
    let (closed, _, message) = outcome(|r| zerobus_multiplexed_stream_close(mux, r));
    assert!(!closed && !message.is_empty(), "close reports the failure");
    assert!(
        zerobus_multiplexed_stream_is_closed(mux),
        "closed either way"
    );
    wait_until("on_error receives the failed record", || {
        !ack_log.errors().is_empty()
    });
    let errors = ack_log.errors();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(errors[0].0, id);
    assert!(errors[0].1.contains("denied"), "on_error text: {errors:?}");
    assert_eq!(ack_log.ack_count(), 0);
    let recovered = take_payloads(
        ok("get_unacked_records", |r| {
            zerobus_multiplexed_stream_get_unacked_records(mux, r)
        }),
        false,
    );
    assert_eq!(recovered, [record.to_vec()]);
    free_both(sdk, mux);
}

/// Pending work keeps the ack `user_data` alive across free: a callback still
/// running after close stopped waiting.
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
}

/// Many C threads share one handle, ingesting at the same time: every ID is
/// distinct, and every record is recovered.
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
    let barrier = Arc::new(Barrier::new(THREADS));
    let workers: Vec<_> = (0..THREADS)
        .map(|t| {
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let mux = mux_addr as Mux;
                let records = json_records(t * PER_THREAD..(t + 1) * PER_THREAD);
                barrier.wait();
                records
                    .iter()
                    .map(|json| ingest_json(mux, json))
                    .collect::<Vec<_>>()
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
    let all = sorted_strings(&json_records(0..total));
    assert_eq!(unacked_json_payloads(f.mux), all);
    f.free();
}

// ---------------------------------------------------------------------------
// Ordinary streams
// ---------------------------------------------------------------------------

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
