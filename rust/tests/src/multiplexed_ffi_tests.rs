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
    zerobus_multiplexed_stream_is_closed, zerobus_multiplexed_stream_wait_for_message_id,
    zerobus_sdk_builder_build, zerobus_sdk_builder_disable_tls, zerobus_sdk_builder_endpoint,
    zerobus_sdk_builder_new, zerobus_sdk_builder_unity_catalog_url,
    zerobus_sdk_create_multiplexed_stream,
    zerobus_sdk_create_multiplexed_stream_with_headers_provider, zerobus_sdk_free, CHeaders,
    CRecordArray, CResult, CStreamConfigurationOptions, CZerobusMultiplexedStream, CZerobusSdk,
};
use zerobus_ffi::{
    zerobus_multiplexed_stream_close_async, zerobus_multiplexed_stream_flush_async,
    zerobus_multiplexed_stream_get_unacked_records_async,
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
// Short names for the ingest exports, so each call fits on one line.
use zerobus_ffi::{
    zerobus_multiplexed_stream_ingest_json_record as json1,
    zerobus_multiplexed_stream_ingest_json_record_async as json1_a,
    zerobus_multiplexed_stream_ingest_json_records as jsons,
    zerobus_multiplexed_stream_ingest_json_records_async as jsons_a,
    zerobus_multiplexed_stream_ingest_proto_record as proto1,
    zerobus_multiplexed_stream_ingest_proto_record_async as proto1_a,
    zerobus_multiplexed_stream_ingest_proto_records as protos,
    zerobus_multiplexed_stream_ingest_proto_records_async as protos_a,
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

/// Prefix of the core message for a record whose type does not match the
/// stream (matched by prefix so a core rewording of the tail does not break it).
const RECORD_TYPE_MISMATCH: &str = "Record type does not match";
const NULL_HANDLE: &str = "Multiplexed stream pointer is null";
const NULL_SDK: &str = "SDK pointer is null";
const NULL_ARG: &str = "Null pointer passed";
const BAD_DATA: &str = "Invalid data pointer";
const BAD_RECORDS: &str = "Invalid records pointer";
const OPTIONS_ACK: &str =
    "options->ack_on_ack and options->ack_on_error are not supported for multiplexed streams";
/// How long a create may take to report a connect failure to `127.0.0.1:1`.
/// On Windows a closed local port is not refused at once (each SYN is
/// retried), and the OAuth path connects more than once.
const CONNECT_FAILURE_WAIT: Duration = Duration::from_secs(60);

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
    /// The `_async` create's callback frees the new handle inside it.
    free_in_callback: bool,
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
            free_in_callback: false,
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

    /// The `_async` create: `(returned, retryable, message)`.
    fn start(&self, auth: Auth, ack: MuxAck, sink: &'static AsyncSink) -> (bool, bool, String) {
        let ((descriptor, len), secret) = (self.descriptor(), self.client_secret());
        let (callback, user_data, free) = headers_args(self.provider);
        let sink_create = if self.free_in_callback {
            sink_create_and_free
        } else {
            sink_create
        };
        outcome(|r| match auth {
            Auth::OAuth => zerobus_sdk_create_multiplexed_stream_async(
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
                sink_create,
                sink.user_data(),
                r,
            ),
            Auth::Headers => zerobus_sdk_create_multiplexed_stream_with_headers_provider_async(
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
                sink_create,
                sink.user_data(),
                r,
            ),
        })
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

    /// The `_async` ingest: `(returned, retryable, message)`.
    fn call_async(self, mux: Mux, sink: &'static AsyncSink) -> (bool, bool, String) {
        let (ud, cb) = (sink.user_data(), sink_offset);
        outcome(|r| match self {
            Ingest::Proto(d) => proto1_a(mux, d.as_ptr(), d.len(), cb, ud, r),
            Ingest::Protos(p, l) => protos_a(mux, p.as_ptr(), l.as_ptr(), p.len(), cb, ud, r),
            Ingest::Json(json) => json1_a(mux, json, cb, ud, r),
            Ingest::Jsons(j) => jsons_a(mux, j.as_ptr(), j.len(), cb, ud, r),
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

    /// `one`, waiting up to `bound` for the callback to start.
    fn one_within(&self, bound: Duration, what: &str) -> Completion {
        wait_until_within(bound, what, || self.count() >= 1);
        self.one(what)
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

/// A create callback that frees the new handle inside the callback, which the
/// docs allow.
extern "C" fn sink_create_and_free(mux: Mux, result: *const CResult, user_data: *mut c_void) {
    if !mux.is_null() {
        zerobus_multiplexed_stream_free(mux);
    }
    sink_create(mux, result, user_data);
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
/// returned, and both waits pass the lane and the sub-offset through, also on
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
    // (what, call, message part, also run `_async`)
    let rows = [
        (
            "json_record(NULL)",
            Ingest::Json(ptr::null()),
            "Null pointer passed",
            true,
        ),
        (
            "json_record(invalid UTF-8)",
            Ingest::Json(invalid_utf8),
            "Invalid UTF-8 string",
            false,
        ),
        (
            "json_records([valid, NULL])",
            Ingest::Jsons(&null_batch),
            "Null pointer passed",
            true,
        ),
        (
            "json_records([valid, invalid UTF-8])",
            Ingest::Jsons(&utf8_batch),
            "Invalid UTF-8 string",
            true,
        ),
        (
            "proto_records([valid, NULL])",
            Ingest::Protos(&proto_ptrs, &proto_lens),
            "Invalid data pointer",
            true,
        ),
        // A protobuf record on this JSON mux: the core's error, mapped by the
        // blocking call to -1 and a non-retryable result. (The async path
        // reports it in its task.)
        (
            "proto_record on a JSON mux",
            Ingest::Proto(proto),
            "Record type does not match",
            false,
        ),
    ];

    let mut sinks = Vec::new();
    for (what, ingest, expected, with_async) in rows {
        let (id, _, retryable, message) = ingest.call(f.mux);
        let mut messages = vec![(what.to_string(), retryable, message)];
        assert_eq!(id, -1, "{what} must be rejected");
        if with_async {
            let sink = AsyncSink::leaked();
            let (returned, async_retryable, message) = ingest.call_async(f.mux, sink);
            assert!(!returned, "{what} async must be rejected");
            messages.push((format!("{what} async"), async_retryable, message));
            sinks.push((what, sink));
        }
        for (what, retryable, message) in messages {
            assert!(!retryable, "{what}: {message}");
            assert!(message.contains(expected), "{what}: {message}");
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
        let create = Create::new(sdk, "ffi.mux.own", 2).with(|c| {
            c.options = Some(options);
            c.provider = Some(provider);
        });
        let mux = if use_async {
            let sink = AsyncSink::leaked();
            assert!(create.start(Auth::Headers, ack, sink).0);
            created(sink, what)
        } else {
            ok("create", |r| create.sync(Auth::Headers, ack, r))
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
    for (what, use_async, use_oauth) in [
        ("sync headers", false, false),
        ("async headers", true, false),
        ("async OAuth", true, true),
    ] {
        let table = format!("ffi.mux.partial_{}", what.replace(' ', "_"));
        runtime.block_on(server.inject_responses(&table, failing_lane()));
        let (provider, ack_log) = (FreeCounter::leaked(), AckLog::leaked());
        let ack = MuxAck::owned(ack_log, true);
        let auth = if use_oauth {
            Auth::OAuth
        } else {
            Auth::Headers
        };
        let create = Create::new(sdk, &table, 2).with(|c| {
            c.options = Some(options);
            c.provider = (!use_oauth).then_some(provider);
        });
        let (mux, failed, message) = if use_async {
            let sink = AsyncSink::leaked();
            assert!(create.start(auth, ack, sink).0);
            let done = sink.one(&format!("{what} create callback"));
            (done.mux, !done.success && !done.retryable, done.message)
        } else {
            let mut result = new_result();
            let mux = create.sync(auth, ack, &mut result);
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

    // A pending async call (r0's ack is gated; with one slot, the async
    // ingest waits for capacity).
    for op in [
        "flush_async",
        "close_async",
        "get_unacked_records_async",
        "json_record_async",
        "wait_for_message_id_async",
    ] {
        let ack_log = AckLog::leaked();
        let ack = MuxAck::owned(ack_log, true);
        let options = with(json_options(), |o| o.max_inflight_requests = 1);
        let gated = GatedMux::new("ffi.mux.async.free", &options, ack, vec![]);
        let sink = AsyncSink::leaked();
        let r1 = json_record(1);
        let started = match op {
            "json_record_async" => Ingest::Json(r1.as_ptr()).call_async(gated.mux, sink),
            "wait_for_message_id_async" => wait_async(gated.mux, gated.r0_id, sink),
            _ => async_op(op, gated.mux, sink),
        };
        expect_returned_true(started, op);
        zerobus_multiplexed_stream_free(gated.mux);
        ack_log.assert_not_freed(&format!("free with a pending {op}"));

        gated.gate.release();
        let done = sink.one(op);
        assert!(done.success, "{op}: {}", done.message);
        match op {
            "get_unacked_records_async" => assert!(done.payloads.is_empty(), "{done:?}"),
            // An ID, not a bool.
            "json_record_async" => {}
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

/// Calls racing `close`, queued behind a flush or holding the lock in its
/// own flush. The readers return at once: async ingests report "closing"
/// through one callback before the writer finishes; the async wait and flush
/// wait for the writer, then report the real outcome; a blocking ingest
/// fails. No rejected record is sent or recovered. An async wait on the
/// record the writer is flushing succeeds. `get_unacked_records` takes the
/// same write path, so it gets one row (an async ingest sees "closing").
/// With no writer, a flush holding the read lock doesn't block async readers.
#[test]
fn calls_racing_a_close() {
    for (writer, queued) in [
        ("close", true),
        ("close", false),
        ("get_unacked_records", false),
    ] {
        let what = format!("{writer}, {}", if queued { "queued" } else { "held" });
        let only_close = writer == "close";
        let gated = GatedMux::new("ffi.mux.closing", &json_options(), MuxAck::none(), vec![]);
        // Queued: a flush holds the read lock and the writer waits.
        // Held: the writer holds the lock, blocked in its own flush.
        let flusher = queued.then(|| spawn_flush(gated.mux));
        let writer_thread = spawn_writer(writer, gated.mux);
        wait_until("the writer takes or queues for the lock", || {
            zerobus_multiplexed_stream_is_closed(gated.mux)
        });

        let mut waiting = Vec::new();
        let readers = readers(gated.r0_id);
        let readers = if only_close {
            &readers[..]
        } else {
            &readers[1..2]
        };
        for &reader in readers {
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
        if only_close {
            let late = json_record(2);
            let (id, _, retryable, message) = Ingest::Json(late.as_ptr()).call(gated.mux);
            assert!(
                id == -1 && !retryable && !message.is_empty(),
                "{what}: {message}"
            );
        }
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

    // An async wait made while close holds the lock, on r0 (acked before)
    // and on r1 (the record close's flush waits for).
    for target in ["r0", "r1"] {
        let what = format!("close, wait on {target}");
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
        let writer_thread = spawn_writer("close", f.mux);
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
        assert!(success, "{what}: {message}");
        assert!(recovered.is_empty(), "{what}: {recovered:?}");
        let done = sink.one(&what);
        assert!(done.success && done.value == 1, "{what}: {done:?}");
        f.free();
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

/// Bad C arguments make every `_async` call return false at once and never
/// call back; an async create frees its `user_data` once. One row per
/// argument check per export.
#[test]
fn async_calls_fail_fast_on_invalid_arguments() {
    use Auth::*;
    type Row = (&'static str, fn(&mut Create), &'static [Auth], &'static str);
    let rows: [Row; 4] = [
        (
            "sdk=NULL",
            |c| c.sdk = ptr::null_mut(),
            &[OAuth, Headers],
            NULL_SDK,
        ),
        ("table=NULL", |c| c.table = None, &[OAuth], NULL_ARG),
        ("client_id=NULL", |c| c.client_id = None, &[OAuth], NULL_ARG),
        // A truncated varint, not a DescriptorProto.
        (
            "bad descriptor",
            |c| c.descriptor = Some(vec![0xff; 3]),
            &[OAuth],
            "failed to decode Protobuf message",
        ),
    ];
    let sdk = unreachable_sdk();
    for (case, edit, auths, expected) in rows {
        for &auth in auths {
            let what = format!("{auth:?} async {case}");
            let (create, provider, ack_log, ack) = owned_user_data(failing_create(sdk, edit), auth);
            let sink = AsyncSink::leaked();
            let (returned, retryable, message) = create.start(auth, ack, sink);
            assert!(!returned && !retryable, "{what}: {message}");
            assert!(message.contains(expected), "{what}: {message}");
            assert_row_freed(auth, provider, ack_log, &what);
            sink.assert_never_called(&what);
        }
    }
    zerobus_sdk_free(sdk);

    // The other `_async` calls: a NULL handle, and a NULL pointer argument,
    // which is checked first.
    let null: Mux = ptr::null_mut();
    let mut results = Vec::new();
    for reader in readers(0) {
        let sink = AsyncSink::leaked();
        let outcome = reader.call_async(null, sink);
        results.push((reader.name().to_owned(), outcome, NULL_HANDLE, sink));
    }
    for op in ["close_async", "get_unacked_records_async"] {
        let sink = AsyncSink::leaked();
        results.push((op.to_owned(), async_op(op, null, sink), NULL_HANDLE, sink));
    }
    let (data, lens, cb) = ([1u8], [1usize], sink_offset);
    let records = [data.as_ptr()];
    type Call<'a> = Box<dyn Fn(*mut c_void, *mut CResult) -> bool + 'a>;
    let null_pointer: [(&str, Call, &str); 4] = [
        (
            "proto_record_async data=NULL",
            Box::new(|ud, r| proto1_a(null, ptr::null(), 1, cb, ud, r)),
            BAD_DATA,
        ),
        (
            "proto_records_async records=NULL",
            Box::new(|ud, r| protos_a(null, ptr::null(), lens.as_ptr(), 1, cb, ud, r)),
            BAD_RECORDS,
        ),
        (
            "proto_records_async record_lens=NULL",
            Box::new(|ud, r| protos_a(null, records.as_ptr(), ptr::null(), 1, cb, ud, r)),
            BAD_RECORDS,
        ),
        (
            "json_records_async records=NULL",
            Box::new(|ud, r| jsons_a(null, ptr::null(), 1, cb, ud, r)),
            BAD_RECORDS,
        ),
    ];
    for (name, call, expected) in null_pointer {
        let sink = AsyncSink::leaked();
        let outcome = outcome(|r| call(sink.user_data(), r));
        results.push((name.to_owned(), outcome, expected, sink));
    }
    // Let a wrongly spawned callback land.
    std::thread::sleep(Duration::from_millis(200));
    for (name, (returned, retryable, message), expected, sink) in results {
        assert!(!returned && !retryable, "{name}: {message}");
        assert!(message.contains(expected), "{name}: {message}");
        assert_eq!(sink.count(), 0, "{name}: callback must not run");
    }
}

/// Async create failures: `options->ack_on_*` is rejected at once (false,
/// no callback); a core build error, a connect failure and a panic in the
/// create task return true and reach the callback once with a NULL mux.
/// Either way both `user_data` are freed once.
#[test]
fn async_create_failures_report_and_free_once() {
    use Auth::*;
    const PANIC: &str = "Rust panic caught at FFI boundary";
    // (case, edit, auths, reported in the task, retryable, message part)
    type Row = (
        &'static str,
        fn(&mut Create),
        &'static [Auth],
        bool,
        bool,
        &'static str,
    );
    let rows: [Row; 5] = [
        (
            "options->ack_on_ack",
            |c| c.options.as_mut().unwrap().ack_on_ack = Some(log_ack),
            &[OAuth],
            false,
            false,
            OPTIONS_ACK,
        ),
        // Core build errors reach the callback (matched by the core text's
        // prefix): both ends of the stream_count range.
        (
            "stream_count=0",
            |c| c.stream_count = 0,
            &[OAuth],
            true,
            false,
            "stream_count must be",
        ),
        (
            "stream_count=65",
            |c| c.stream_count = 65,
            &[OAuth],
            true,
            false,
            "stream_count must be",
        ),
        ("connect failure", |_| {}, &[OAuth, Headers], true, true, ""),
        // max_inflight usize::MAX overflows in the core: the task panics.
        (
            "panic in the task",
            |c| {
                c.stream_count = 1;
                c.options.as_mut().unwrap().max_inflight_requests = usize::MAX;
            },
            &[OAuth],
            true,
            false,
            PANIC,
        ),
    ];
    let sdk = unreachable_sdk();
    for (case, edit, auths, in_task, retryable, expected) in rows {
        for &auth in auths {
            let what = format!("{auth:?} async {case}");
            let (create, provider, ack_log, ack) = owned_user_data(failing_create(sdk, edit), auth);
            let sink = AsyncSink::leaked();
            let (returned, sync_retryable, message) = create.start(auth, ack, sink);
            let (got_retryable, message) = if in_task {
                assert!(returned, "{what}: {message}");
                let bound = if retryable {
                    CONNECT_FAILURE_WAIT
                } else {
                    Duration::from_secs(5)
                };
                let done = sink.one_within(bound, &what);
                assert!(done.mux == 0 && !done.success, "{what}: {done:?}");
                (done.retryable, done.message)
            } else {
                assert!(!returned, "{what}: {message}");
                sink.assert_never_called(&what);
                (sync_retryable, message)
            };
            assert_eq!(got_retryable, retryable, "{what}: {message}");
            assert!(message.contains(expected), "{what}: {message}");
            assert_row_freed(auth, provider, ack_log, &what);
        }
    }
    zerobus_sdk_free(sdk);
}

/// Async creates return at once; the callback brings a working handle whose
/// acks carry the returned IDs.
#[test]
fn async_creates_return_before_lanes_open() {
    let (runtime, server, url) = start_server();
    let (oauth, uc_url) = runtime.block_on(start_mock_oauth_server());
    runtime.block_on(oauth.set_responses(vec![MockTokenResponse::ok("token-1")]));
    let sdk = create_sdk_with_uc(&url, &uc_url);
    for (table, auth) in [
        ("ffi.mux.async_headers", Auth::Headers),
        ("ffi.mux.async_oauth", Auth::OAuth),
    ] {
        // Each lane's open takes a second; each lane acks its offset 0.
        let open = MockResponse::CreateStream {
            stream_id: "lane".into(),
            delay_ms: 1000,
        };
        runtime.block_on(server.inject_responses(table, vec![open, ack(0)]));
        let (ack_log, sink) = (AckLog::leaked(), AsyncSink::leaked());
        let ack = MuxAck::log(ack_log);
        let (returned, _, message) = Create::new(sdk, table, 2).start(auth, ack, sink);
        // The lanes open only after a second, so no callback yet means the
        // create returned before they opened.
        assert_eq!(
            sink.count(),
            0,
            "{table}: create blocked until the lanes opened"
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

    // A DescriptorProto with NULL options, and the handle freed inside its
    // own success callback (the docs allow it).
    let descriptor = create_test_descriptor_proto().unwrap().encode_to_vec();
    let create = Create::new(sdk, "ffi.mux.async_proto", 1).with(|c| {
        c.descriptor = Some(descriptor);
        c.options = None;
        c.free_in_callback = true;
    });
    let sink = AsyncSink::leaked();
    let (returned, _, message) = create.start(Auth::Headers, MuxAck::none(), sink);
    assert!(returned, "async proto create: {message}");
    created(sink, "async proto create");
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
    let null_sink = AsyncSink::leaked();
    let ud = null_sink.user_data();
    expect_returned_true(
        outcome(|r| jsons_a(f.mux, ptr::null(), 0, sink_offset, ud, r)),
        "json_records_async(NULL, 0)",
    );
    let null_batch = null_sink.one("json_records_async(NULL, 0)");
    assert!(
        null_batch.success && null_batch.value == -2,
        "{null_batch:?}"
    );
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
    // NULL is valid with length 0: NULL arrays for an empty batch.
    let null_sink = AsyncSink::leaked();
    let ud = null_sink.user_data();
    expect_returned_true(
        outcome(|r| protos_a(mux, ptr::null(), ptr::null(), 0, sink_offset, ud, r)),
        "proto_records_async(NULL, NULL, 0)",
    );
    let null_batch = null_sink.one("proto_records_async(NULL, NULL, 0)");
    assert!(
        null_batch.success && null_batch.value == -2,
        "{null_batch:?}"
    );
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
        if lane_failure {
            let sink = AsyncSink::leaked();
            expect_returned_true(async_op("close_async", f.mux, sink), "close_async");
            let done = sink.one(&format!("{what}: close_async"));
            assert!(
                !done.success && done.message.contains("lane down"),
                "{what}: close_async reports the cause: {done:?}"
            );
        }
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

/// A terminal lane failure that arrives after the async drain used most of
/// flush_timeout_ms: the flush still reports the lane's own non-retryable
/// error, not a timeout, because the core flush gets its own budget after the
/// drain; and the mux is closed afterwards. Both the blocking flush and
/// `flush_async`.
#[test]
fn flush_reports_terminal_failure_during_async_drain() {
    for use_async in [false, true] {
        let what = if use_async { "flush_async" } else { "flush" };
        // Timeline from the flush's start (flush_timeout_ms = 2,000): r0's ack
        // at about 1.5 s ends the drain (r1 is then sent); r1's request gets
        // the lane error 1 s later, at about 2.5 s. A drain and core flush
        // sharing one 2 s budget would report a timeout instead.
        let responses = vec![
            lane_open(),
            MockResponse::RecordAck {
                ack_up_to_offset: 0,
                delay_ms: 1500,
            },
            MockResponse::Error {
                status: Status::permission_denied("lane denied"),
                delay_ms: 1000,
            },
        ];
        let options = with(json_options_terminal(), |o| {
            o.max_inflight_requests = 1;
            o.flush_timeout_ms = 2000;
        });
        let f = Fixture::new(
            "ffi.mux.flush.terminal",
            responses,
            1,
            &options,
            MuxAck::none(),
        );
        let [r0, r1]: [CString; 2] = json_records(0..2).try_into().unwrap();
        ingest_json(f.mux, &r0);
        f.wait_for_writes(1);
        // r1 waits for capacity, so the flush has a pending async ingest.
        let ingest_sink = AsyncSink::leaked();
        Ingest::Json(r1.as_ptr()).start(f.mux, ingest_sink);

        let (returned, retryable, message) = if use_async {
            let sink = AsyncSink::leaked();
            expect_returned_true(async_op("flush_async", f.mux, sink), what);
            let done = sink.one_within(Duration::from_secs(5), what);
            (done.success, done.retryable, done.message)
        } else {
            call_within(
                Duration::from_secs(5),
                f.mux,
                zerobus_multiplexed_stream_flush,
                what,
            )
        };
        assert!(!returned && !retryable, "{what}: {message}");
        assert!(message.contains("lane denied"), "{what}: {message}");
        assert!(
            !message.contains("timed out"),
            "{what}: a timeout instead of the lane's error: {message}"
        );
        assert!(
            zerobus_multiplexed_stream_is_closed(f.mux),
            "{what}: the mux must be closed after a terminal failure"
        );
        ingest_sink.one("r1 async ingest");
        f.free();
    }
}

/// A flush covers the async ingests accepted before it, through the shared
/// ingest tracker: every record of a loop is sent before it returns, an
/// ingest still waiting for capacity is covered by `flush_async`, and a flush
/// under a concurrent producer still returns. The epoch rule is a unit test
/// in rust/ffi/src/multiplexed.rs.
#[test]
fn flush_covers_async_ingests_accepted_before_it() {
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
    let mut ids: Vec<i64> = completions.iter().map(|c| c.value).collect();
    assert!(ok("wait_for_message_id", |r| {
        zerobus_multiplexed_stream_wait_for_message_id(f.mux, ids[0], r)
    }));
    wait_until("every record is acknowledged", || {
        ack_log.ack_count() >= RECORDS
    });
    close_ok(f.mux);
    f.free();
    ids.sort();
    assert_eq!(
        ack_log.sorted_acks(),
        ids,
        "the acks carry the async ingest IDs"
    );

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
    // Nothing is acked, so the flush ends at its 1 s timeout.
    let (flushed, _, message) = call_within(
        Duration::from_secs(5),
        f.mux,
        flush,
        "flush under a concurrent producer",
    );
    assert!(!flushed && message.contains("timed out"), "{message}");
    stop.store(true, Ordering::SeqCst);
    let mut accepted = producer.join().unwrap();
    accepted.sort();
    assert!(!accepted.is_empty());
    assert_eq!(unacked_json_payloads(f.mux), accepted);
    sink.wait_for(accepted.len(), "every accepted async ingest calls back");
    f.free();

    // flush_timeout_ms bounds the wait for a pending async ingest, then the
    // core flush times out on its own, so the flush ends within twice the
    // timeout: r0 is never acked and r1 waits for capacity.
    let options = with(json_options(), |o| {
        o.max_inflight_requests = 1;
        o.flush_timeout_ms = 300;
    });
    let f = Fixture::new("ffi.mux.async.timeout", vec![], 1, &options, MuxAck::none());
    ingest_json(f.mux, &json_record(0));
    let r1 = json_record(1);
    Ingest::Json(r1.as_ptr()).start(f.mux, AsyncSink::leaked());
    let (flushed, _, message) = call_within(
        Duration::from_secs(2),
        f.mux,
        flush,
        "flush with a pending async ingest",
    );
    assert!(!flushed, "the flush times out");
    assert!(message.contains("Flush timed out"), "{message}");
    f.free();
}

/// One blocking call made from a callback: (call, failed by its return value,
/// result.success, retryable, message).
type GuardedCall = (&'static str, bool, bool, bool, String);

/// What a mux ack callback saw when it called the API from inside itself.
#[derive(Default)]
struct CallbackCalls {
    mux: AtomicUsize,
    finished: AtomicBool,
    blocking: Mutex<Vec<GuardedCall>>,
    flush_async_accepted: AtomicBool,
    flush_async: AsyncSink,
}

impl Leaked for CallbackCalls {}

/// Once, from inside a callback: every guarded blocking call, then
/// `flush_async`. No asserts here: a panic would cross the C boundary.
fn call_the_api_from_callback(user_data: *mut c_void) {
    let calls = unsafe { &*(user_data as *const CallbackCalls) };
    if calls.finished.load(Ordering::SeqCst) || !calls.blocking.lock().unwrap().is_empty() {
        return;
    }
    let mux = calls.mux.load(Ordering::SeqCst) as Mux;
    let (data, lens, json) = ([1u8], [1usize], json_record(9));
    let (data_ptrs, json_ptrs) = ([data.as_ptr()], [json.as_ptr()]);
    type Guarded<'a> = (&'static str, Box<dyn Fn(*mut CResult) -> bool + 'a>);
    let guarded: [Guarded; 8] = [
        (
            "ingest_proto_record",
            Box::new(|r| proto1(mux, data.as_ptr(), 1, r) == -1),
        ),
        (
            "ingest_json_record",
            Box::new(|r| json1(mux, json.as_ptr(), r) == -1),
        ),
        (
            "ingest_proto_records",
            Box::new(|r| protos(mux, data_ptrs.as_ptr(), lens.as_ptr(), 1, r) == -1),
        ),
        (
            "ingest_json_records",
            Box::new(|r| jsons(mux, json_ptrs.as_ptr(), 1, r) == -1),
        ),
        (
            "wait_for_message_id",
            Box::new(|r| !zerobus_multiplexed_stream_wait_for_message_id(mux, 0, r)),
        ),
        (
            "flush",
            Box::new(|r| !zerobus_multiplexed_stream_flush(mux, r)),
        ),
        (
            "close",
            Box::new(|r| !zerobus_multiplexed_stream_close(mux, r)),
        ),
        (
            "get_unacked_records",
            Box::new(|r| {
                let records = zerobus_multiplexed_stream_get_unacked_records(mux, r);
                records.records.is_null() && records.len == 0
            }),
        ),
    ];
    let mut blocking = Vec::new();
    for (call, run) in guarded {
        let mut result = new_result();
        result.success = true;
        let failed = run(&mut result);
        let success = result.success;
        let (retryable, message) = (result.is_retryable, take_message(&mut result));
        blocking.push((call, failed, success, retryable, message));
    }
    *calls.blocking.lock().unwrap() = blocking;
    let mut result = new_result();
    let sink = &calls.flush_async as *const AsyncSink as *mut c_void;
    let accepted = zerobus_multiplexed_stream_flush_async(mux, sink_bool, sink, &mut result);
    take_message(&mut result);
    calls.flush_async_accepted.store(accepted, Ordering::SeqCst);
    calls.finished.store(true, Ordering::SeqCst);
}

extern "C" fn call_the_api_from_ack(_id: i64, user_data: *mut c_void) {
    call_the_api_from_callback(user_data);
}

extern "C" fn call_the_api_from_error(_id: i64, _message: *const c_char, user_data: *mut c_void) {
    call_the_api_from_callback(user_data);
}

/// From inside an ack callback (on_ack and on_error), every guarded blocking
/// call fails at once with a fixed, non-retryable error and does not close the
/// mux, while an `_async` call is accepted and calls back; the close
/// afterwards still finishes, and the rejected ingests are never sent.
#[test]
fn blocking_calls_from_an_ack_callback_fail_at_once() {
    const IN_CALLBACK: &str = "Blocking multiplexed stream calls (ingest, wait_for_message_id, \
                               flush, close, get_unacked_records) cannot be called from an ack \
                               callback";
    for on_error in [false, true] {
        let what = if on_error { "on_error" } else { "on_ack" };
        let calls = CallbackCalls::leaked();
        let mux_ack = MuxAck {
            on_ack: (!on_error).then_some(call_the_api_from_ack as _),
            on_error: on_error.then_some(call_the_api_from_error as _),
            user_data: calls.user_data(),
            free_user_data: None,
        };
        // on_ack: r0 is acked. on_error: r0's lane fails, terminally.
        let (responses, options) = if on_error {
            let denied = lane_error(Status::permission_denied("denied"));
            (vec![lane_open(), denied], json_options_terminal())
        } else {
            (vec![ack(0)], json_options())
        };
        let f = Fixture::new("ffi.mux.callback.scope", responses, 1, &options, mux_ack);
        calls.mux.store(f.mux as usize, Ordering::SeqCst);
        ingest_json(f.mux, &json_record(0));
        wait_until(&format!("{what}: the callback finishes its calls"), || {
            calls.finished.load(Ordering::SeqCst)
        });
        let blocking = calls.blocking.lock().unwrap().clone();
        assert_eq!(blocking.len(), 8, "{what}");
        for (call, failed, success, retryable, message) in &blocking {
            let during = format!("{what}: {call} from the callback");
            assert!(*failed && !*success && !*retryable, "{during}: {message}");
            assert!(message.starts_with(IN_CALLBACK), "{during}: {message}");
        }
        assert!(calls.flush_async_accepted.load(Ordering::SeqCst), "{what}");
        let done = calls
            .flush_async
            .one(&format!("{what}: flush_async from the callback"));
        if on_error {
            // The lane failed, so close reports that failure; it still ends.
            let mut result = new_result();
            zerobus_multiplexed_stream_close(f.mux, &mut result);
            take_message(&mut result);
        } else {
            assert!(done.success, "{done:?}");
            assert!(
                !zerobus_multiplexed_stream_is_closed(f.mux),
                "the rejected close must not close the mux"
            );
            close_ok(f.mux);
        }
        assert_eq!(
            f.writes(),
            1,
            "{what}: the rejected ingests were never sent"
        );
        f.free();
    }
}

/// A blocking `_async` ingest callback holds nothing the mux needs: with an
/// in-task rejection (nothing queued), the ticket and read guard are released,
/// so a flush and a close during the callback return within 1 s; with a
/// queued record (single and batch), the record is still sent and a flush
/// still returns within 1 s.
#[test]
fn blocking_async_callback_holds_nothing_back() {
    let json = json_records(0..2);
    let ptrs: Vec<*const c_char> = json.iter().map(|r| r.as_ptr()).collect();
    let rows = [
        // A protobuf record on a JSON mux: rejected in the task.
        (Ingest::Proto(b"proto"), 0),
        (Ingest::Json(ptrs[0]), 1),
        (Ingest::Jsons(&ptrs), 2),
    ];
    for (ingest, records) in rows {
        let what = ingest.name();
        let acks = lane_with_acks(1);
        let f = Fixture::new(
            "ffi.mux.async.blocking",
            acks,
            1,
            &json_options(),
            MuxAck::none(),
        );
        let sink = AsyncSink::blocking();
        ingest.start(f.mux, sink);
        sink.wait_entered();
        let deadline = Instant::now() + Duration::from_secs(1);
        while f.writes() < records {
            assert!(
                Instant::now() < deadline,
                "{what}: the record wasn't sent while its callback ran"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let mut calls = vec![("flush", zerobus_multiplexed_stream_flush as MuxCall)];
        if records == 0 {
            calls.push(("close", zerobus_multiplexed_stream_close));
        }
        for (call_name, call) in calls {
            let during = format!("{what}: {call_name} during the callback");
            let (returned, _, message) = call_within(Duration::from_secs(1), f.mux, call, &during);
            assert!(returned, "{during}: {message}");
        }
        sink.release();
        let done = sink.one(what);
        if records == 0 {
            assert!(
                done.value == -1 && done.message.contains(RECORD_TYPE_MISMATCH),
                "{done:?}"
            );
        } else {
            assert!(done.success, "{what}: {done:?}");
            close_ok(f.mux);
        }
        f.free();
    }
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
