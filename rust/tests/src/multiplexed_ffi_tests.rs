//! Drives the multiplexed stream C ABI against the mock gRPC server.
//!
//! The FFI blocks on its own global runtime, so these tests call it from plain
//! threads and run the mock server on a separate runtime.

mod utils;

use std::collections::HashSet;
use std::ffi::{c_void, CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use prost::Message;
use tokio::runtime::Runtime;
use utils::create_test_descriptor_proto;
use zerobus_ffi::{
    zerobus_free_error_message, zerobus_free_record_array, zerobus_get_default_config,
    zerobus_multiplexed_stream_close, zerobus_multiplexed_stream_flush,
    zerobus_multiplexed_stream_free, zerobus_multiplexed_stream_get_unacked_records,
    zerobus_multiplexed_stream_ingest_json_record,
    zerobus_multiplexed_stream_ingest_json_records_nowait,
    zerobus_multiplexed_stream_ingest_proto_record,
    zerobus_multiplexed_stream_ingest_proto_record_nowait,
    zerobus_multiplexed_stream_ingest_proto_records, zerobus_multiplexed_stream_is_closed,
    zerobus_sdk_builder_build, zerobus_sdk_builder_disable_tls, zerobus_sdk_builder_endpoint,
    zerobus_sdk_builder_new, zerobus_sdk_builder_unity_catalog_url,
    zerobus_sdk_create_multiplexed_stream_with_headers_provider, zerobus_sdk_free, CHeaders,
    CResult, CStreamConfigurationOptions, CZerobusMultiplexedStream, CZerobusSdk,
};
use zerobus_test_support::mock_grpc::{start_mock_server, MockResponse, MockZerobusServer};

const STREAM_BITS: u32 = 6;

fn new_result() -> CResult {
    CResult {
        success: false,
        error_message: ptr::null_mut(),
        is_retryable: false,
    }
}

/// Panics with the FFI error message unless `result` reports success.
fn expect_success(result: &mut CResult, call: &str) {
    if !result.success {
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
        panic!("{call} failed: {message}");
    }
}

fn stream_index(message_id: i64) -> u64 {
    (message_id as u64) >> (64 - STREAM_BITS)
}

extern "C" fn empty_headers(_user_data: *mut c_void) -> CHeaders {
    CHeaders {
        headers: ptr::null_mut(),
        count: 0,
        error_message: ptr::null_mut(),
    }
}

fn start_server() -> (Runtime, MockZerobusServer, String) {
    let runtime = Runtime::new().unwrap();
    let (server, url) = runtime.block_on(start_mock_server()).unwrap();
    (runtime, server, url)
}

fn create_sdk(server_url: &str) -> *mut CZerobusSdk {
    let endpoint = CString::new(server_url).unwrap();
    let uc_url = CString::new("https://mock-uc.com").unwrap();
    let builder = zerobus_sdk_builder_new();
    zerobus_sdk_builder_endpoint(builder, endpoint.as_ptr());
    zerobus_sdk_builder_unity_catalog_url(builder, uc_url.as_ptr());
    zerobus_sdk_builder_disable_tls(builder);
    let mut result = new_result();
    let sdk = zerobus_sdk_builder_build(builder, &mut result);
    expect_success(&mut result, "zerobus_sdk_builder_build");
    sdk
}

fn create_mux(
    sdk: *mut CZerobusSdk,
    table: &str,
    descriptor: Option<&[u8]>,
    stream_count: usize,
    options: &CStreamConfigurationOptions,
) -> *mut CZerobusMultiplexedStream {
    let table = CString::new(table).unwrap();
    let (descriptor_ptr, descriptor_len) =
        descriptor.map_or((ptr::null(), 0), |d| (d.as_ptr(), d.len()));
    let mut result = new_result();
    let mux = zerobus_sdk_create_multiplexed_stream_with_headers_provider(
        sdk,
        table.as_ptr(),
        descriptor_ptr,
        descriptor_len,
        empty_headers,
        ptr::null_mut(),
        None,
        stream_count,
        options,
        &mut result,
    );
    expect_success(&mut result, "zerobus_sdk_create_multiplexed_stream");
    assert!(!mux.is_null());
    mux
}

fn wait_for_writes(runtime: &Runtime, server: &MockZerobusServer, expected: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while runtime.block_on(server.get_write_count()) < expected {
        assert!(
            Instant::now() < deadline,
            "server did not receive {expected} records"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[derive(Default)]
struct AckLog {
    acks: Mutex<Vec<i64>>,
    errors: Mutex<Vec<i64>>,
}

extern "C" fn record_ack(message_id: i64, user_data: *mut c_void) {
    let log = unsafe { &*(user_data as *const AckLog) };
    log.acks.lock().unwrap().push(message_id);
}

extern "C" fn record_error(message_id: i64, _message: *const c_char, user_data: *mut c_void) {
    let log = unsafe { &*(user_data as *const AckLog) };
    log.errors.lock().unwrap().push(message_id);
}

/// Single, batch, and fire-and-forget ingest on one sub-stream, followed by
/// one flush; the ack callback receives every returned message ID.
#[test]
fn proto_ingest_flush_and_ack_callback() {
    const TABLE: &str = "ffi.mux.proto";
    let (runtime, server, url) = start_server();
    let mut responses = vec![MockResponse::CreateStream {
        stream_id: "lane-0".into(),
        delay_ms: 0,
    }];
    responses.extend((0..3).map(|offset| MockResponse::RecordAck {
        ack_up_to_offset: offset,
        delay_ms: 0,
    }));
    runtime.block_on(server.inject_responses(TABLE, responses));

    let ack_log = Box::new(AckLog::default());
    let mut options = zerobus_get_default_config();
    options.recovery = false;
    options.ack_on_ack = Some(record_ack);
    options.ack_on_error = Some(record_error);
    options.ack_user_data = &*ack_log as *const AckLog as *mut c_void;

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
    // Fire-and-forget records are queued asynchronously; wait until it is sent
    // so the flush below covers it.
    wait_for_writes(&runtime, &server, 4);

    assert!(zerobus_multiplexed_stream_flush(mux, &mut result));
    expect_success(&mut result, "flush");
    assert!(!zerobus_multiplexed_stream_is_closed(mux));

    assert!(zerobus_multiplexed_stream_close(mux, &mut result));
    expect_success(&mut result, "close");
    assert!(zerobus_multiplexed_stream_is_closed(mux));
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);

    // Close drains the callback worker, so every ack has been delivered.
    let acks = ack_log.acks.lock().unwrap().clone();
    assert_eq!(acks.len(), 3, "one ack per ingest call: {acks:?}");
    assert!(acks.contains(&single_id) && acks.contains(&batch_id));
    assert!(ack_log.errors.lock().unwrap().is_empty());
}

/// JSON records spread round-robin over two sub-streams. The server never
/// acknowledges, so every record comes back from `get_unacked_records`.
#[test]
fn json_round_robin_and_unacked_recovery() {
    const TABLE: &str = "ffi.mux.json";
    let (runtime, server, url) = start_server();

    let mut options = zerobus_get_default_config();
    options.record_type = 2;
    options.recovery = false;
    options.flush_timeout_ms = 500;

    let sdk = create_sdk(&url);
    let mux = create_mux(sdk, TABLE, None, 2, &options);
    let mut result = new_result();

    let blocking: Vec<CString> = (0..4)
        .map(|i| CString::new(format!(r#"{{"id": {i}}}"#)).unwrap())
        .collect();
    let lanes: Vec<u64> = blocking
        .iter()
        .map(|json| {
            let id = zerobus_multiplexed_stream_ingest_json_record(mux, json.as_ptr(), &mut result);
            expect_success(&mut result, "ingest_json_record");
            stream_index(id)
        })
        .collect();
    assert_eq!(lanes, [0, 1, 0, 1], "records must alternate sub-streams");

    let nowait: Vec<CString> = (4..6)
        .map(|i| CString::new(format!(r#"{{"id": {i}}}"#)).unwrap())
        .collect();
    let nowait_ptrs: Vec<*const c_char> = nowait.iter().map(|s| s.as_ptr()).collect();
    zerobus_multiplexed_stream_ingest_json_records_nowait(
        mux,
        nowait_ptrs.as_ptr(),
        nowait_ptrs.len(),
        &mut result,
    );
    expect_success(&mut result, "ingest_json_records_nowait");
    wait_for_writes(&runtime, &server, 6);

    let records = zerobus_multiplexed_stream_get_unacked_records(mux, &mut result);
    expect_success(&mut result, "get_unacked_records");
    assert!(zerobus_multiplexed_stream_is_closed(mux));

    let recovered: HashSet<String> = (0..records.len)
        .map(|i| {
            let record = unsafe { &*records.records.add(i) };
            assert!(record.is_json);
            let bytes = unsafe { std::slice::from_raw_parts(record.data, record.data_len) };
            String::from_utf8(bytes.to_vec()).unwrap()
        })
        .collect();
    let expected: HashSet<String> = blocking
        .iter()
        .chain(&nowait)
        .map(|s| s.to_str().unwrap().to_owned())
        .collect();
    assert_eq!(recovered, expected);

    zerobus_free_record_array(records);
    zerobus_multiplexed_stream_free(mux);
    zerobus_sdk_free(sdk);
}
