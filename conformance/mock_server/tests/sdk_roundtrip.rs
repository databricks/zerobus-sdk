//! Mock self-tests: drive the real Rust SDK against the conformance mock, so `make test`
//! exercises the mock's gRPC behavior (decode limit, fault injection, recovery) in CI.

use std::sync::Arc;

use databricks_zerobus_ingest_sdk::{NoAuthHeadersProvider, NoTlsConfig, ZerobusSdk};
use zerobus_mock_server::start_server;

const TABLE: &str = "cat.schema.table";

async fn sdk_for(url: String) -> ZerobusSdk {
    ZerobusSdk::builder()
        .endpoint(url)
        .unity_catalog_url("https://mock-uc.com")
        .tls_config(Arc::new(NoTlsConfig))
        .build()
        .expect("build sdk")
}

/// A ~5 MiB JSON record flushes successfully: the mock raises tonic's 4 MiB decode limit to
/// 10 MiB to match the SDK's ceiling.
#[tokio::test]
async fn large_json_record_flushes() {
    let server = start_server("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let sdk = sdk_for(server.url()).await;
    let stream = sdk
        .stream_builder()
        .table(TABLE)
        .headers_provider(Arc::new(NoAuthHeadersProvider))
        .json()
        .max_inflight_requests(100)
        .recovery(false)
        .build()
        .await
        .expect("create stream");

    let big = "x".repeat(5 * 1024 * 1024);
    let record = format!(r#"{{"id":1,"blob":"{big}"}}"#);
    stream.ingest_record_offset(record).await.expect("ingest");
    stream
        .flush()
        .await
        .expect("flush should succeed under the 10 MiB decode limit");

    assert_eq!(server.state().captured_record_count(), 1);
    server.shutdown().await.unwrap();
}

/// A non-retryable injected error fails `flush`, even with recovery enabled: `InvalidArgument`
/// is unretriable in the SDK, so the terminal path is actually exercised.
#[tokio::test]
async fn non_retryable_injected_error_fails_flush() {
    let server = start_server("127.0.0.1:0".parse().unwrap()).await.unwrap();
    // Ack offset 0, then abort terminally on offset 1.
    server.state().arm_inject(0, false);
    let sdk = sdk_for(server.url()).await;
    let stream = sdk
        .stream_builder()
        .table(TABLE)
        .headers_provider(Arc::new(NoAuthHeadersProvider))
        .json()
        .max_inflight_requests(100)
        .recovery(true)
        .build()
        .await
        .expect("create stream");

    stream
        .ingest_record_offset(r#"{"id":0}"#.to_string())
        .await
        .expect("ingest 0");
    stream
        .ingest_record_offset(r#"{"id":1}"#.to_string())
        .await
        .expect("ingest 1");
    let err = stream
        .flush()
        .await
        .expect_err("flush must fail on a non-retryable error");
    assert!(!err.is_retryable(), "expected non-retryable, got: {err:?}");
    server.shutdown().await.unwrap();
}

/// A retryable injected error recovers: with recovery on, the SDK reconnects and resends, so
/// `flush` succeeds and the mock saw a second connection (`call_count >= 2`).
#[tokio::test]
async fn retryable_injected_error_recovers() {
    let server = start_server("127.0.0.1:0".parse().unwrap()).await.unwrap();
    // Ack offset 0, then a transient abort on offset 1; the SDK should reconnect.
    server.state().arm_inject(0, true);
    let sdk = sdk_for(server.url()).await;
    let stream = sdk
        .stream_builder()
        .table(TABLE)
        .headers_provider(Arc::new(NoAuthHeadersProvider))
        .json()
        .max_inflight_requests(100)
        .recovery(true)
        .build()
        .await
        .expect("create stream");

    stream
        .ingest_record_offset(r#"{"id":0}"#.to_string())
        .await
        .expect("ingest 0");
    stream
        .ingest_record_offset(r#"{"id":1}"#.to_string())
        .await
        .expect("ingest 1");
    stream
        .flush()
        .await
        .expect("flush should succeed after recovery");

    assert!(
        server.state().call_count() >= 2,
        "expected a reconnect, call_count={}",
        server.state().call_count()
    );
    server.shutdown().await.unwrap();
}

/// An Arrow Flight batch flushes with the SDK's DEFAULT options (zstd IPC compression): the
/// mock counts rows from the IPC record-batch header without decoding the body, so a zstd
/// batch is counted and does not hang `flush()` (the row count never stays 0).
#[tokio::test]
async fn arrow_flight_batch_flushes() {
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    let server = start_server("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let sdk = sdk_for(server.url()).await;

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let stream = sdk
        .stream_builder()
        .table(TABLE)
        .headers_provider(Arc::new(NoAuthHeadersProvider))
        .arrow(Arc::clone(&schema))
        .recovery(false)
        .flush_timeout_ms(5000)
        .build_arrow()
        .await
        .expect("create arrow stream");

    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();
    stream.ingest_batch(batch).await.expect("ingest batch");
    stream
        .flush()
        .await
        .expect("flush should succeed (zstd batch counted from the IPC header)");

    assert_eq!(
        server.state().captured_record_count(),
        3,
        "all 3 rows must be counted (not frames), got {}",
        server.state().captured_record_count()
    );
    server.shutdown().await.unwrap();
}
