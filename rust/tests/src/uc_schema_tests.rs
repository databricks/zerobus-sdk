//! Tests for fetching a table's schema from Unity Catalog, directly or when
//! building a dynamic protobuf stream, against local HTTP and gRPC mocks.

mod mock_grpc;

use std::sync::{Arc, Mutex};

use databricks_zerobus_ingest_sdk::uc_schema::fetch_message_descriptor;
use databricks_zerobus_ingest_sdk::{
    databricks::zerobus::RecordType, ProtoBytes, ZerobusError, ZerobusSdk,
};
use mock_grpc::{start_mock_server, MockResponse};
use prost::Message as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const TABLE: &str = "main.sales.orders";

/// One recorded request: its target path and `authorization` header.
type Recorded = Vec<(String, String)>;

/// A running mock. Serves `POST /oidc/v1/token` with a fixed token, then the
/// table route with `schema_status`/`schema_body`. Dropping it stops the loop.
struct MockUc {
    url: String,
    requests: Arc<Mutex<Recorded>>,
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

/// Start a mock replying to the schema route with `schema_status` and `schema_body`.
async fn start_mock(schema_status: u16, schema_body: impl Into<String>) -> MockUc {
    let schema_body: Arc<str> = Arc::from(schema_body.into());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Recorded::new()));
    let (tx, mut rx) = tokio::sync::oneshot::channel();

    let recorded = Arc::clone(&requests);
    tokio::spawn(async move {
        loop {
            let sock = tokio::select! {
                a = listener.accept() => a,
                _ = &mut rx => break,
            };
            let Ok((mut sock, _)) = sock else { break };
            let recorded = Arc::clone(&recorded);
            let schema_body = Arc::clone(&schema_body);
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let head = String::from_utf8_lossy(&buf[..n]);
                let target = head
                    .lines()
                    .next()
                    .and_then(|l| l.split_whitespace().nth(1))
                    .unwrap_or_default()
                    .to_string();
                let auth = header(&head, "authorization");
                recorded.lock().unwrap().push((target.clone(), auth));

                let (status, body) = if target.contains("/oidc/") {
                    (
                        200,
                        r#"{"access_token":"tok-123","token_type":"Bearer","expires_in":3600}"#
                            .to_string(),
                    )
                } else {
                    (schema_status, schema_body.to_string())
                };
                let resp = format!(
                    "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });

    MockUc {
        url,
        requests,
        _shutdown: tx,
    }
}

fn header(head: &str, name: &str) -> String {
    head.lines()
        .find(|l| l.to_ascii_lowercase().starts_with(&format!("{name}:")))
        .and_then(|l| l.split_once(':'))
        .map(|(_, v)| v.trim().to_string())
        .unwrap_or_default()
}

fn table_json() -> &'static str {
    r#"{"name":"orders","catalog_name":"main","schema_name":"sales","columns":[
        {"name":"id","type_name":"BIGINT","type_text":"bigint","type_json":"","nullable":false,"position":0},
        {"name":"customer","type_name":"STRING","type_text":"string","type_json":"","nullable":true,"position":1}
    ]}"#
}

fn table_json_with_padding(padding_len: usize) -> String {
    let padding = "x".repeat(padding_len);
    format!(
        r#"{{"name":"orders","catalog_name":"main","schema_name":"sales","padding":"{padding}","columns":[
        {{"name":"id","type_name":"BIGINT","type_text":"bigint","type_json":"","nullable":false,"position":0}},
        {{"name":"customer","type_name":"STRING","type_text":"string","type_json":"","nullable":true,"position":1}}
    ]}}"#
    )
}

#[tokio::test]
async fn fetches_descriptor_and_sends_expected_requests() {
    let mock = start_mock(200, table_json()).await;

    let descriptor = fetch_message_descriptor(&mock.url, TABLE, "cid", "csec")
        .await
        .expect("fetch should succeed");

    // Descriptor: message name is <schema>_<table>, field numbers are position + 1.
    assert_eq!(descriptor.name(), "SalesOrders");
    assert_eq!(descriptor.get_field_by_name("id").unwrap().number(), 1);
    assert_eq!(
        descriptor.get_field_by_name("customer").unwrap().number(),
        2
    );

    // Request shapes: Basic-auth token mint, then a bearer schema request at the
    // expected path.
    let reqs = mock.requests.lock().unwrap().clone();
    assert_eq!(reqs.len(), 2, "expected a token then a schema request");
    assert!(reqs[0].0.starts_with("/oidc/v1/token"), "got {}", reqs[0].0);
    assert!(reqs[0].1.starts_with("Basic "), "got {}", reqs[0].1);
    assert_eq!(reqs[1].0, format!("/api/2.1/unity-catalog/tables/{TABLE}"));
    assert_eq!(reqs[1].1, "Bearer tok-123");
}

#[tokio::test]
async fn builder_fetches_schema_and_ingests_dynamic_records(
) -> Result<(), Box<dyn std::error::Error>> {
    let mock = start_mock(200, table_json()).await;
    let (grpc, grpc_url) = start_mock_server().await?;
    grpc.inject_responses(
        TABLE,
        vec![
            MockResponse::CreateStream {
                stream_id: "uc-schema-stream".into(),
                delay_ms: 0,
            },
            MockResponse::RecordAck {
                ack_up_to_offset: 2,
                delay_ms: 0,
            },
        ],
    )
    .await;
    let sdk = ZerobusSdk::builder()
        .endpoint(grpc_url)
        .unity_catalog_url(&mock.url)
        .no_tls()
        .build()?;
    let builder = sdk
        .stream_builder()
        .table("old.schema.table")
        .oauth("old-client", "old-secret")
        .json()
        .dynamic_proto_uc_schema()
        .table(TABLE)
        .oauth("cid", "csec")
        .recovery(false);
    builder.validate()?;
    assert!(mock.requests.lock().unwrap().is_empty());
    let mut stream = builder.build().await?;
    let descriptor = stream.message_descriptor()?;
    assert_eq!(descriptor.name(), "SalesOrders");
    assert_eq!(descriptor.get_field_by_name("id").unwrap().number(), 1);
    assert_eq!(
        descriptor.get_field_by_name("customer").unwrap().number(),
        2
    );

    let creates = grpc.get_create_requests().await;
    assert_eq!(creates.len(), 1);
    assert_eq!(creates[0].table_name.as_deref(), Some(TABLE));
    assert_eq!(creates[0].record_type, Some(RecordType::Proto as i32));
    let wire_descriptor =
        prost_types::DescriptorProto::decode(creates[0].descriptor_proto.as_deref().unwrap())?;
    assert_eq!(&wire_descriptor, descriptor.descriptor_proto());

    for id in 0..3i64 {
        let mut record = stream.new_record()?;
        record.set("id", id)?.set("customer", "Alice")?;
        stream
            .ingest_record_offset(ProtoBytes(record.encode()?))
            .await?;
    }
    stream.flush().await?;
    assert_eq!(grpc.get_write_count().await, 3);
    stream.close().await?;
    {
        let requests = mock.requests.lock().unwrap();
        assert_eq!(
            requests.len(),
            3,
            "metadata token, schema, and ingestion token"
        );
        assert_eq!(requests[0].1, "Basic Y2lkOmNzZWM=");
        assert_eq!(
            requests[1].0,
            format!("/api/2.1/unity-catalog/tables/{TABLE}")
        );
        assert_eq!(requests[1].1, "Bearer tok-123");
    }

    let mut next_stream = sdk
        .stream_builder()
        .table(TABLE)
        .oauth("cid", "csec")
        .dynamic_proto_uc_schema()
        .recovery(false)
        .build()
        .await?;
    next_stream.close().await?;
    assert_eq!(
        mock.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, _)| path.starts_with("/api/2.1/unity-catalog/tables/"))
            .count(),
        2,
        "each build fetches a fresh schema"
    );
    Ok(())
}

#[tokio::test]
async fn builder_propagates_schema_errors_before_connecting_grpc(
) -> Result<(), Box<dyn std::error::Error>> {
    let (grpc, grpc_url) = start_mock_server().await?;
    for status in [404, 503] {
        let mock = start_mock(status, "schema unavailable").await;
        let sdk = ZerobusSdk::builder()
            .endpoint(&grpc_url)
            .unity_catalog_url(&mock.url)
            .no_tls()
            .build()?;
        let result = sdk
            .stream_builder()
            .table(TABLE)
            .oauth("cid", "csec")
            .dynamic_proto_uc_schema()
            .recovery(false)
            .build()
            .await;
        assert!(
            matches!(result, Err(ZerobusError::SchemaFetchError { retryable, .. }) if retryable == (status == 503))
        );
        assert_eq!(mock.requests.lock().unwrap().len(), 2);
    }
    assert_eq!(grpc.get_connection_count().await, 0);
    assert!(grpc.get_create_requests().await.is_empty());
    Ok(())
}

#[tokio::test]
async fn builder_format_override_skips_schema_fetch() -> Result<(), Box<dyn std::error::Error>> {
    let mock = start_mock(200, table_json()).await;
    let (grpc, grpc_url) = start_mock_server().await?;
    let sdk = ZerobusSdk::builder()
        .endpoint(grpc_url)
        .unity_catalog_url(&mock.url)
        .no_tls()
        .build()?;
    let mut stream = sdk
        .stream_builder()
        .table(TABLE)
        .dynamic_proto_uc_schema()
        .json()
        .no_auth()
        .build()
        .await?;
    assert!(mock.requests.lock().unwrap().is_empty());
    let creates = grpc.get_create_requests().await;
    assert_eq!(creates.len(), 1);
    assert_eq!(creates[0].record_type, Some(RecordType::Json as i32));
    assert!(creates[0].descriptor_proto.is_none());
    stream.close().await?;
    Ok(())
}

#[tokio::test]
async fn schema_request_404_is_non_retryable_schema_fetch_error() {
    let mock = start_mock(404, "table not found").await;

    let err = fetch_message_descriptor(&mock.url, TABLE, "cid", "csec")
        .await
        .unwrap_err();
    match &err {
        ZerobusError::SchemaFetchError {
            message, retryable, ..
        } => {
            assert!(message.contains("404"), "got: {message}");
            assert!(message.contains("table not found"), "got: {message}");
            assert!(!retryable);
        }
        other => panic!("expected SchemaFetchError, got {other:?}"),
    }
    assert!(!err.is_retryable());
}

#[tokio::test]
async fn schema_request_503_is_retryable_schema_fetch_error() {
    let mock = start_mock(503, "service unavailable").await;

    let err = fetch_message_descriptor(&mock.url, TABLE, "cid", "csec")
        .await
        .unwrap_err();
    match &err {
        ZerobusError::SchemaFetchError {
            message, retryable, ..
        } => {
            assert!(message.contains("503"), "got: {message}");
            assert!(message.contains("service unavailable"), "got: {message}");
            assert!(*retryable);
        }
        other => panic!("expected SchemaFetchError, got {other:?}"),
    }
    assert!(err.is_retryable());
}

#[tokio::test]
async fn schema_request_429_is_retryable_schema_fetch_error() {
    let mock = start_mock(429, "rate limit exceeded").await;

    let err = fetch_message_descriptor(&mock.url, TABLE, "cid", "csec")
        .await
        .unwrap_err();
    match &err {
        ZerobusError::SchemaFetchError {
            message, retryable, ..
        } => {
            assert!(message.contains("429"), "got: {message}");
            assert!(message.contains("rate limit exceeded"), "got: {message}");
            assert!(*retryable);
        }
        other => panic!("expected SchemaFetchError, got {other:?}"),
    }
    assert!(err.is_retryable());
}

#[tokio::test]
async fn oversized_schema_response_is_rejected() {
    // 8 MiB + 1 byte body
    let oversized_body = table_json_with_padding(8 * 1024 * 1024 + 1);
    let mock = start_mock(200, oversized_body).await;

    let err = fetch_message_descriptor(&mock.url, TABLE, "cid", "csec")
        .await
        .unwrap_err();
    match &err {
        ZerobusError::SchemaFetchError {
            message, retryable, ..
        } => {
            assert!(
                message.contains("size limit") || message.contains("exceeded"),
                "got: {message}"
            );
            assert!(!retryable);
        }
        other => panic!("expected SchemaFetchError, got {other:?}"),
    }
    assert!(!err.is_retryable());
}

#[tokio::test]
async fn invalid_table_name_fails_before_any_request() {
    let mock = start_mock(200, table_json()).await;

    match fetch_message_descriptor(&mock.url, "not.qualified", "cid", "csec").await {
        Err(ZerobusError::InvalidTableName(_)) => {}
        other => panic!("expected InvalidTableName, got {other:?}"),
    }
    assert!(
        mock.requests.lock().unwrap().is_empty(),
        "must not hit the network"
    );
}

#[tokio::test]
async fn empty_columns_returns_non_retryable_schema_fetch_error() {
    let empty_schema =
        r#"{"name":"orders","catalog_name":"main","schema_name":"sales","columns":[]}"#;
    let mock = start_mock(200, empty_schema).await;

    let err = fetch_message_descriptor(&mock.url, TABLE, "cid", "csec")
        .await
        .unwrap_err();
    match &err {
        ZerobusError::SchemaFetchError {
            message, retryable, ..
        } => {
            assert!(message.contains("no columns"), "got: {message}");
            assert!(!retryable);
        }
        other => panic!("expected SchemaFetchError, got {other:?}"),
    }
    assert!(!err.is_retryable());
}

#[tokio::test]
async fn insecure_http_endpoint_fails_before_any_request() {
    let err = fetch_message_descriptor("http://insecure-host.example.com", TABLE, "cid", "csec")
        .await
        .unwrap_err();
    match err {
        ZerobusError::InvalidUCEndpointError(msg) => {
            assert!(
                msg.contains("http is not permitted for non-loopback hosts"),
                "got: {msg}"
            );
        }
        other => panic!("expected InvalidUCEndpointError, got {other:?}"),
    }
}
