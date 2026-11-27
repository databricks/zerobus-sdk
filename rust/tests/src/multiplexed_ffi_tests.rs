//! Drives the multiplexed stream C ABI against the mock gRPC server.
//!
//! The FFI blocks on its own global runtime, so these tests call it from plain
//! threads and run the mock server on a separate runtime. They test the FFI
//! layer only: C arguments, ownership, and error mapping. The core mux
//! behavior is tested in `multiplexed_stream_tests.rs`.

#[allow(dead_code)]
mod mock_oauth;
mod utils;

use std::ffi::{c_void, CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use mock_oauth::{start_mock_oauth_server, MockTokenResponse};
use prost::Message;
use tokio::runtime::Runtime;
use tonic::Status;
use zerobus_ffi::{
    zerobus_free_error_message, zerobus_get_default_config, zerobus_multiplexed_stream_close,
    zerobus_multiplexed_stream_free, zerobus_multiplexed_stream_is_closed,
    zerobus_sdk_builder_build, zerobus_sdk_builder_disable_tls, zerobus_sdk_builder_endpoint,
    zerobus_sdk_builder_new, zerobus_sdk_builder_unity_catalog_url,
    zerobus_sdk_create_multiplexed_stream_with_headers_provider, zerobus_sdk_free, CHeaders,
    CResult, CStreamConfigurationOptions, CZerobusMultiplexedStream, CZerobusSdk,
};
use zerobus_ffi::{
    zerobus_sdk_create_stream_with_headers_provider, zerobus_stream_close, zerobus_stream_flush,
    zerobus_stream_free, zerobus_stream_ingest_json_record,
};
use zerobus_test_support::mock_grpc::{start_mock_server, MockResponse, MockZerobusServer};

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

/// Runs a call that must succeed and returns its value.
fn ok<T>(what: &str, f: impl FnOnce(*mut CResult) -> T) -> T {
    let mut result = new_result();
    let value = f(&mut result as *mut CResult);
    expect_success(&mut result, what);
    value
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
    Headers,
}

/// One mux create call. A NULL `sdk`, a `None` table, and `None` options are
/// passed as NULL, so the argument checks can be driven too.
struct Create {
    sdk: *mut CZerobusSdk,
    table: Option<CString>,
    descriptor: Option<Vec<u8>>,
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

    /// The sync create; the outcome is in `result`.
    fn sync(&self, auth: Auth, ack: MuxAck, result: *mut CResult) -> Mux {
        let (descriptor, len) = self.descriptor();
        let (callback, user_data, free) = headers_args(self.provider);
        match auth {
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
    // Held only to keep the mock server running while the test uses the mux.
    #[allow(dead_code)]
    runtime: Runtime,
    #[allow(dead_code)]
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

    fn free(self) {
        free_both(self.sdk, self.mux);
    }
}

fn json_record(id: usize) -> CString {
    CString::new(format!(r#"{{"id": {id}}}"#)).unwrap()
}

fn json_records(ids: std::ops::Range<usize>) -> Vec<CString> {
    ids.map(json_record).collect()
}

fn close_ok(mux: Mux) {
    assert!(ok("close", |r| zerobus_multiplexed_stream_close(mux, r)));
}

// ---------------------------------------------------------------------------
// Blocking calls
// ---------------------------------------------------------------------------

/// Every failing sync create reports its error (retryable only for a
/// connect failure) and frees the headers and ack `user_data` once.
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
    let rows: [Row; 6] = [
        (
            "sdk=NULL",
            |c| c.sdk = ptr::null_mut(),
            &[Headers],
            false,
            NULL_SDK,
        ),
        (
            "table=NULL",
            |c| c.table = None,
            &[Headers],
            false,
            NULL_ARG,
        ),
        // A truncated varint, not a DescriptorProto.
        (
            "bad descriptor",
            |c| c.descriptor = Some(vec![0xff; 3]),
            &[Headers],
            false,
            "failed to decode Protobuf message",
        ),
        (
            "options->ack_on_ack",
            |c| c.options.as_mut().unwrap().ack_on_ack = Some(log_ack),
            &[Headers],
            false,
            OPTIONS_ACK,
        ),
        (
            "options->ack_on_error",
            |c| c.options.as_mut().unwrap().ack_on_error = Some(log_error),
            &[Headers],
            false,
            OPTIONS_ACK,
        ),
        ("connect failure", |_| {}, &[Headers], true, ""),
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
    assert!(create
        .sync(Headers, MuxAck::log(&log), &mut result)
        .is_null());
    take_message(&mut result);
    let create = failing_create(sdk, |c| c.stream_count = 0);
    assert!(create
        .sync(Headers, MuxAck::none(), ptr::null_mut())
        .is_null());
    zerobus_sdk_free(sdk);
}

/// The FFI frees the headers and ack `user_data` exactly once: on success
/// only at free; at create when both ack callbacks are NULL;
/// after a partial open, with no handle.
#[test]
fn ffi_owned_user_data_is_freed_once() {
    // (a) Success with callbacks: nothing is freed until free. This create
    // passes a DescriptorProto and NULL options: the FFI decodes it, and the
    // record type defaults to proto.
    {
        let what = "sync";
        let (runtime, server, url) = start_server();
        // No CreateStream entry: each lane acks its offset 0.
        runtime.block_on(server.inject_responses("ffi.mux.own", vec![ack(0)]));
        let (provider, ack_log) = (FreeCounter::leaked(), AckLog::leaked());
        let sdk = create_sdk(&url);
        let descriptor = utils::create_test_descriptor_proto()
            .unwrap()
            .encode_to_vec();
        let ack = MuxAck::owned(ack_log, true);
        let create = Create::new(sdk, "ffi.mux.own", 2).with(|c| {
            c.descriptor = Some(descriptor);
            c.options = None;
            c.provider = Some(provider);
        });
        let mux = ok("create", |r| create.sync(Auth::Headers, ack, r));
        assert!(provider.calls() >= 2, "{what}: every lane fetches headers");
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
