//! Integration tests for the unary `InsertRecords` client against the mock server.

#[allow(dead_code)]
mod mock_oauth;
mod utils;

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use databricks_zerobus_ingest_sdk::{
    GrpcCode, GrpcStatus, HeadersProvider, JsonString, JsonValue, ProtoBytes, UnaryClient,
    UnaryClientBuilder, ZerobusError, ZerobusResult, ZerobusSdk,
};
use mock_oauth::{start_mock_oauth_server, MockTokenResponse};
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tonic::Status;
use utils::setup_tracing;
use zerobus_test_support::mock_grpc::{
    start_mock_server, MockInsertResponse, MockResponse, MockZerobusServer, ReceivedInsert,
};

const TABLE_NAME: &str = "test_catalog.test_schema.test_table";
const OTHER_TABLE_NAME: &str = "test_catalog.test_schema.other_table";

/// A headers provider whose token changes after every `invalidate()`: it sends
/// `Bearer token-0` until the first invalidation, then `Bearer token-1`, and so on.
/// Its first `failed_fetches` calls fail with `TokenFetchError`.
struct TestProvider {
    table_name: String,
    failed_fetches: usize,
    get_headers_calls: AtomicUsize,
    invalidations: AtomicUsize,
    get_headers_delay: Duration,
    hang_on_invalidate: bool,
}

impl TestProvider {
    fn new(table_name: &str) -> Arc<Self> {
        Arc::new(Self::with(table_name, Duration::ZERO, false))
    }

    fn with(table_name: &str, get_headers_delay: Duration, hang_on_invalidate: bool) -> Self {
        Self {
            table_name: table_name.to_string(),
            failed_fetches: 0,
            get_headers_calls: AtomicUsize::new(0),
            invalidations: AtomicUsize::new(0),
            get_headers_delay,
            hang_on_invalidate,
        }
    }

    fn failing(table_name: &str, failed_fetches: usize) -> Arc<Self> {
        Arc::new(Self {
            failed_fetches,
            ..Self::with(table_name, Duration::ZERO, false)
        })
    }

    fn get_headers_calls(&self) -> usize {
        self.get_headers_calls.load(Ordering::SeqCst)
    }

    fn invalidations(&self) -> usize {
        self.invalidations.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl HeadersProvider for TestProvider {
    async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
        let call = self.get_headers_calls.fetch_add(1, Ordering::SeqCst);
        if call < self.failed_fetches {
            return Err(ZerobusError::TokenFetchError(
                "token endpoint unreachable".to_string(),
            ));
        }
        tokio::time::sleep(self.get_headers_delay).await;
        let generation = self.invalidations();
        let mut headers = HashMap::new();
        headers.insert("authorization", format!("Bearer token-{generation}"));
        headers.insert("x-databricks-zerobus-table-name", self.table_name.clone());
        Ok(headers)
    }

    async fn invalidate(&self) {
        self.invalidations.fetch_add(1, Ordering::SeqCst);
        if self.hang_on_invalidate {
            std::future::pending::<()>().await;
        }
    }
}

/// A TCP proxy between the SDK and the mock server, to break connections.
///
/// `black_hole()` makes every connection open at that moment swallow all
/// traffic in both directions while staying open, like a dropped NAT entry.
/// `sever()` closes them. Connections accepted afterwards forward normally.
struct FaultProxy {
    url: String,
    accepted: Arc<AtomicUsize>,
    connections: Arc<Mutex<Vec<ProxiedConnection>>>,
}

struct ProxiedConnection {
    black_hole: Arc<AtomicBool>,
    pumps: [JoinHandle<()>; 2],
}

impl FaultProxy {
    async fn start(server_url: &str) -> Self {
        let upstream: SocketAddr = server_url
            .trim_start_matches("http://")
            .parse()
            .expect("mock server address");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let url = format!("http://{}", listener.local_addr().unwrap());
        let accepted = Arc::new(AtomicUsize::new(0));
        let connections = Arc::new(Mutex::new(Vec::new()));
        {
            let accepted = Arc::clone(&accepted);
            let connections = Arc::clone(&connections);
            tokio::spawn(async move {
                while let Ok((client, _)) = listener.accept().await {
                    accepted.fetch_add(1, Ordering::SeqCst);
                    let server = TcpStream::connect(upstream)
                        .await
                        .expect("connect upstream");
                    let black_hole = Arc::new(AtomicBool::new(false));
                    let (client_read, client_write) = client.into_split();
                    let (server_read, server_write) = server.into_split();
                    let pumps = [
                        tokio::spawn(pump(client_read, server_write, Arc::clone(&black_hole))),
                        tokio::spawn(pump(server_read, client_write, Arc::clone(&black_hole))),
                    ];
                    connections
                        .lock()
                        .unwrap()
                        .push(ProxiedConnection { black_hole, pumps });
                }
            });
        }
        Self {
            url,
            accepted,
            connections,
        }
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    fn black_hole(&self) {
        for connection in self.connections.lock().unwrap().iter() {
            connection.black_hole.store(true, Ordering::SeqCst);
        }
    }

    fn sever(&self) {
        for connection in self.connections.lock().unwrap().drain(..) {
            for pump in connection.pumps {
                pump.abort();
            }
        }
    }
}

/// Copies bytes from `from` to `to`, discarding them once `black_hole` is set.
async fn pump(mut from: OwnedReadHalf, mut to: OwnedWriteHalf, black_hole: Arc<AtomicBool>) {
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        if black_hole.load(Ordering::SeqCst) {
            continue;
        }
        if to.write_all(&buf[..n]).await.is_err() {
            break;
        }
    }
    let _ = to.shutdown().await;
}

fn sdk(endpoint: &str) -> ZerobusSdk {
    sdk_builder(endpoint).build().expect("build SDK")
}

fn sdk_builder(endpoint: &str) -> databricks_zerobus_ingest_sdk::ZerobusSdkBuilder {
    ZerobusSdk::builder()
        .endpoint(endpoint)
        .unity_catalog_url("https://mock-uc.com")
        .no_tls()
}

/// A unary client for `TABLE_NAME` with a short backoff, adjusted by `configure`.
async fn client(
    sdk: &ZerobusSdk,
    provider: &Arc<TestProvider>,
    configure: impl FnOnce(UnaryClientBuilder<'_>) -> UnaryClientBuilder<'_>,
) -> UnaryClient {
    let builder = sdk
        .unary_client_builder()
        .table(provider.table_name.clone())
        .headers_provider(Arc::clone(provider) as Arc<dyn HeadersProvider>)
        .json()
        .recovery_backoff_ms(10);
    configure(builder)
        .build()
        .await
        .expect("build unary client")
}

/// Fails the test instead of hanging it if `call` never completes.
async fn bounded<T>(call: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), call)
        .await
        .expect("the call must finish within 10 s; the attempt timeout or keepalive did not fire")
}

fn status_of(error: ZerobusError) -> GrpcStatus {
    match error {
        ZerobusError::UnaryIngestError(status) => status,
        other => panic!("expected UnaryIngestError, got {other:?}"),
    }
}

fn invalid_argument(error: ZerobusError) -> String {
    match error {
        ZerobusError::InvalidArgument(msg) => msg,
        other => panic!("expected InvalidArgument, got {other:?}"),
    }
}

fn body(request: &ReceivedInsert) -> &str {
    std::str::from_utf8(&request.records_json).expect("UTF-8 body")
}

fn header<'a>(request: &'a ReceivedInsert, name: &str) -> Option<&'a str> {
    request
        .metadata
        .get(name)
        .map(|value| value.to_str().expect("ASCII header"))
}

fn unavailable() -> MockInsertResponse {
    error(Status::unavailable("server busy"))
}

fn error(status: Status) -> MockInsertResponse {
    MockInsertResponse::Error {
        status,
        delay_ms: 0,
    }
}

async fn requests(server: &MockZerobusServer) -> Vec<ReceivedInsert> {
    server.get_insert_requests().await
}

fn records(ids: std::ops::Range<i32>) -> Vec<String> {
    ids.map(|id| format!(r#"{{"id":{id}}}"#)).collect()
}

// ─── Request contents ─────────────────────────────────────────────────────────

#[tokio::test]
async fn sends_the_batch_as_one_json_array() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    let sdk = sdk(&url);
    let client = client(&sdk, &TestProvider::new(TABLE_NAME), |b| b).await;

    client
        .ingest_records(vec![
            r#"{"id":1,"name":"a"}"#.to_string(),
            r#"{"id":2,"name":"b"}"#.to_string(),
        ])
        .await
        .unwrap();

    let requests = requests(&server).await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].table_name.as_deref(), Some(TABLE_NAME));
    assert_eq!(
        body(&requests[0]),
        r#"[{"id":1,"name":"a"},{"id":2,"name":"b"}]"#
    );
}

#[tokio::test]
async fn accepts_every_json_record_type() {
    #[derive(Serialize)]
    struct Row {
        id: i64,
        big: i64,
    }

    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    let sdk = sdk(&url);
    let client = client(&sdk, &TestProvider::new(TABLE_NAME), |b| b).await;

    client
        .ingest_records(vec![JsonValue(Row {
            id: 1,
            big: i64::MAX,
        })])
        .await
        .unwrap();
    client
        .ingest_records(vec![JsonString(r#"{"id":2}"#.to_string())])
        .await
        .unwrap();
    client
        .ingest_records(vec![r#"{"id":3}"#.to_string()])
        .await
        .unwrap();

    let bodies: Vec<_> = requests(&server)
        .await
        .iter()
        .map(|r| body(r).to_string())
        .collect();
    assert_eq!(
        bodies,
        [
            format!(r#"[{{"id":1,"big":{}}}]"#, i64::MAX),
            r#"[{"id":2}]"#.to_string(),
            r#"[{"id":3}]"#.to_string(),
        ]
    );
}

#[tokio::test]
async fn sends_the_provider_headers_and_the_user_agent() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    let sdk = sdk_builder(&url)
        .application_name("unary-test-app")
        .build()
        .unwrap();
    let client = client(&sdk, &TestProvider::new(TABLE_NAME), |b| b).await;

    client.ingest_records(records(0..1)).await.unwrap();

    let requests = requests(&server).await;
    let request = &requests[0];
    assert_eq!(header(request, "authorization"), Some("Bearer token-0"));
    assert_eq!(
        header(request, "x-databricks-zerobus-table-name"),
        Some(TABLE_NAME)
    );
    let user_agent = header(request, "user-agent").expect("user-agent");
    assert!(
        user_agent.starts_with("zerobus-sdk-rs/") && user_agent.contains("unary-test-app"),
        "{user_agent}"
    );
}

#[tokio::test]
async fn empty_batch_sends_nothing() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    let sdk = sdk(&url);
    let provider = TestProvider::new(TABLE_NAME);
    let client = client(&sdk, &provider, |b| b).await;

    client.ingest_records(Vec::<String>::new()).await.unwrap();

    assert!(requests(&server).await.is_empty());
    assert_eq!(provider.get_headers_calls(), 0);
}

#[tokio::test]
async fn ingest_record_sends_a_batch_of_one() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    let sdk = sdk(&url);
    let client = client(&sdk, &TestProvider::new(TABLE_NAME), |b| b).await;

    client
        .ingest_record(r#"{"id":7}"#.to_string())
        .await
        .unwrap();

    let requests = requests(&server).await;
    assert_eq!(requests.len(), 1);
    assert_eq!(body(&requests[0]), r#"[{"id":7}]"#);
}

#[tokio::test]
async fn json_lines_sends_one_record_per_line_with_the_same_values() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    let sdk = sdk(&url);
    let client = client(&sdk, &TestProvider::new(TABLE_NAME), |b| b.json_lines()).await;
    let batch = vec![
        "{\n  \"msg\": \"a\\nb\",\n  \"id\": 1\n}".to_string(),
        "{\r\n  \"id\": 2\r\n}".to_string(),
        r#"{"id":3}"#.to_string(),
    ];

    client.ingest_records(batch.clone()).await.unwrap();

    let requests = requests(&server).await;
    let sent = body(&requests[0]);
    assert!(!sent.starts_with('['), "{sent}");
    assert!(!sent.contains('\r') && !sent.ends_with('\n'), "{sent:?}");
    let lines: Vec<&str> = sent.split('\n').collect();
    assert_eq!(lines.len(), batch.len(), "{sent:?}");
    for (line, record) in lines.iter().zip(&batch) {
        let sent_value: serde_json::Value = serde_json::from_str(line).unwrap();
        let original: serde_json::Value = serde_json::from_str(record).unwrap();
        assert_eq!(sent_value, original);
    }
    assert!(
        lines[0].contains(r#""a\nb""#),
        "escaped newline kept: {}",
        lines[0]
    );
}

// ─── Client-side validation ───────────────────────────────────────────────────

#[tokio::test]
async fn invalid_batches_fail_before_anything_is_sent() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    let sdk = sdk(&url);
    let provider = TestProvider::new(TABLE_NAME);
    let client = client(&sdk, &provider, |b| b.max_ingest_payload_bytes(64)).await;

    let msg = invalid_argument(
        client
            .ingest_record(ProtoBytes(vec![1, 2]))
            .await
            .unwrap_err(),
    );
    assert!(
        msg.contains("record 0") && msg.contains("only JSON"),
        "{msg}"
    );

    let msg = invalid_argument(
        client
            .ingest_records(vec![r#"{"id":1}"#.to_string(), "not json".to_string()])
            .await
            .unwrap_err(),
    );
    assert!(msg.contains("record 1"), "{msg}");

    let msg = invalid_argument(
        client
            .ingest_records(vec![r#"{"a":1},{"a":2}"#.to_string()])
            .await
            .unwrap_err(),
    );
    assert!(msg.contains("record 0"), "{msg}");

    let msg = invalid_argument(client.ingest_records(records(0..10)).await.unwrap_err());
    assert!(msg.contains("exceeds the configured limit of 64"), "{msg}");

    assert!(requests(&server).await.is_empty());
    assert_eq!(provider.get_headers_calls(), 0);
}

#[tokio::test]
async fn payload_limit_counts_the_whole_body() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    let sdk = sdk(&url);
    let provider = TestProvider::new(TABLE_NAME);
    // `[{"id":0},{"id":1}]` is 19 bytes; its JSON Lines form is 17.
    let array = client(&sdk, &provider, |b| b.max_ingest_payload_bytes(19)).await;
    let lines = client(&sdk, &provider, |b| {
        b.json_lines().max_ingest_payload_bytes(17)
    })
    .await;
    let too_small = client(&sdk, &provider, |b| b.max_ingest_payload_bytes(18)).await;

    array.ingest_records(records(0..2)).await.unwrap();
    lines.ingest_records(records(0..2)).await.unwrap();
    invalid_argument(too_small.ingest_records(records(0..2)).await.unwrap_err());

    assert_eq!(requests(&server).await.len(), 2);
}

// ─── Retries ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn retries_retryable_errors_until_success() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    server
        .inject_insert_responses(vec![
            unavailable(),
            error(Status::internal("boom")),
            error(Status::deadline_exceeded("slow")),
        ])
        .await;
    let sdk = sdk(&url);
    let client = client(&sdk, &TestProvider::new(TABLE_NAME), |b| {
        b.recovery_retries(4)
    })
    .await;

    client.ingest_records(records(0..3)).await.unwrap();

    let requests = requests(&server).await;
    assert_eq!(requests.len(), 4);
    assert!(requests
        .iter()
        .all(|r| r.records_json == requests[0].records_json));
}

#[tokio::test]
async fn exhausted_retries_return_the_last_status() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    server
        .inject_insert_responses(vec![unavailable(); 10])
        .await;
    let sdk = sdk(&url);
    let client = client(&sdk, &TestProvider::new(TABLE_NAME), |b| {
        b.recovery_retries(3).recovery_backoff_ms(0)
    })
    .await;

    let error = client.ingest_records(records(0..1)).await.unwrap_err();

    assert!(error.is_retryable());
    assert!(
        error.to_string().contains("InsertRecords failed"),
        "{error}"
    );
    let status = status_of(error);
    assert_eq!(status.code(), GrpcCode::Unavailable);
    assert_eq!(status.message(), "server busy");
    assert_eq!(requests(&server).await.len(), 4);
}

#[tokio::test]
async fn does_not_retry_non_retryable_codes() {
    setup_tracing();
    for status in [
        Status::invalid_argument("bad record"),
        Status::not_found("no table"),
        Status::out_of_range("too large"),
        Status::unimplemented("no rpc"),
    ] {
        let (server, url) = start_mock_server().await.unwrap();
        let code = status.code();
        server.inject_insert_responses(vec![error(status)]).await;
        let sdk = sdk(&url);
        let client = client(&sdk, &TestProvider::new(TABLE_NAME), |b| {
            b.recovery_retries(4)
        })
        .await;

        let error = client.ingest_records(records(0..1)).await.unwrap_err();

        assert!(!error.is_retryable(), "{code:?}");
        assert_eq!(status_of(error).code() as i32, code as i32);
        assert_eq!(requests(&server).await.len(), 1, "{code:?}");
    }
}

#[tokio::test]
async fn each_call_has_its_own_retry_budget() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    server
        .inject_insert_responses(vec![
            unavailable(),
            MockInsertResponse::Ok { delay_ms: 0 },
            unavailable(),
        ])
        .await;
    let sdk = sdk(&url);
    let client = client(&sdk, &TestProvider::new(TABLE_NAME), |b| {
        b.recovery_retries(1)
    })
    .await;

    client.ingest_records(records(0..1)).await.unwrap();
    client.ingest_records(records(1..2)).await.unwrap();

    assert_eq!(requests(&server).await.len(), 4);
}

// ─── Authentication ───────────────────────────────────────────────────────────

#[tokio::test]
async fn auth_rejection_invalidates_and_retries_with_a_fresh_credential() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    server
        .inject_insert_responses(vec![error(Status::unauthenticated("expired"))])
        .await;
    let sdk = sdk(&url);
    let provider = TestProvider::new(TABLE_NAME);
    let client = client(&sdk, &provider, |b| b.recovery_retries(3)).await;

    client.ingest_records(records(0..1)).await.unwrap();

    let requests = requests(&server).await;
    assert_eq!(requests.len(), 2);
    assert_eq!(
        header(&requests[0], "authorization"),
        Some("Bearer token-0")
    );
    assert_eq!(
        header(&requests[1], "authorization"),
        Some("Bearer token-1")
    );
    assert_eq!(provider.invalidations(), 1);
}

#[tokio::test]
async fn permission_denied_is_an_auth_rejection() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    server
        .inject_insert_responses(vec![error(Status::permission_denied("no grant"))])
        .await;
    let sdk = sdk(&url);
    let provider = TestProvider::new(TABLE_NAME);
    let client = client(&sdk, &provider, |b| b.recovery_retries(3)).await;

    client.ingest_records(records(0..1)).await.unwrap();

    assert_eq!(requests(&server).await.len(), 2);
    assert_eq!(provider.invalidations(), 1);
}

#[tokio::test]
async fn second_auth_rejection_is_terminal() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    server
        .inject_insert_responses(vec![error(Status::unauthenticated("expired")); 3])
        .await;
    let sdk = sdk(&url);
    let provider = TestProvider::new(TABLE_NAME);
    let client = client(&sdk, &provider, |b| b.recovery_retries(4)).await;

    let error = client.ingest_records(records(0..1)).await.unwrap_err();

    assert_eq!(status_of(error).code(), GrpcCode::Unauthenticated);
    assert_eq!(requests(&server).await.len(), 2);
    assert_eq!(provider.invalidations(), 2);
}

#[tokio::test]
async fn auth_retry_budget_resets_on_every_call() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    server
        .inject_insert_responses(vec![
            error(Status::unauthenticated("expired")),
            MockInsertResponse::Ok { delay_ms: 0 },
            error(Status::unauthenticated("expired")),
        ])
        .await;
    let sdk = sdk(&url);
    let provider = TestProvider::new(TABLE_NAME);
    let client = client(&sdk, &provider, |b| b.recovery_retries(1)).await;

    client.ingest_records(records(0..1)).await.unwrap();
    client.ingest_records(records(1..2)).await.unwrap();

    assert_eq!(requests(&server).await.len(), 4);
    assert_eq!(provider.invalidations(), 2);
}

#[tokio::test]
async fn auth_and_transient_errors_share_the_retry_budget() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    server
        .inject_insert_responses(vec![
            error(Status::unauthenticated("expired")),
            unavailable(),
        ])
        .await;
    let sdk = sdk(&url);

    let provider = TestProvider::new(TABLE_NAME);
    let client = client(&sdk, &provider, |b| b.recovery_retries(2)).await;
    client.ingest_records(records(0..1)).await.unwrap();
    assert_eq!(requests(&server).await.len(), 3);

    server.reset().await;
    server
        .inject_insert_responses(vec![
            error(Status::unauthenticated("expired")),
            unavailable(),
            unavailable(),
        ])
        .await;
    let error = client.ingest_records(records(0..1)).await.unwrap_err();
    assert_eq!(status_of(error).code(), GrpcCode::Unavailable);
    assert_eq!(requests(&server).await.len(), 3);
}

#[tokio::test]
async fn auth_rejection_is_not_retried_without_retries() {
    setup_tracing();
    for (recovery, retries) in [(true, 0), (false, 3)] {
        let (server, url) = start_mock_server().await.unwrap();
        server
            .inject_insert_responses(vec![error(Status::unauthenticated("expired"))])
            .await;
        let sdk = sdk(&url);
        let provider = TestProvider::new(TABLE_NAME);
        let client = client(&sdk, &provider, |b| {
            b.recovery(recovery).recovery_retries(retries)
        })
        .await;

        let error = client.ingest_records(records(0..1)).await.unwrap_err();

        assert_eq!(status_of(error).code(), GrpcCode::Unauthenticated);
        assert_eq!(requests(&server).await.len(), 1);
        // The rejected credential is dropped even when no retry follows, so the
        // caller's next call gets a fresh one.
        assert_eq!(provider.invalidations(), 1);
    }
}

#[tokio::test]
async fn failed_header_fetch_is_retried_and_returned_as_is() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    let sdk = sdk(&url);
    let provider = TestProvider::failing(TABLE_NAME, usize::MAX);
    let failing = client(&sdk, &provider, |b| b.recovery_retries(2)).await;

    let error = failing.ingest_records(records(0..1)).await.unwrap_err();

    assert!(
        matches!(error, ZerobusError::TokenFetchError(_)),
        "{error:?}"
    );
    assert_eq!(provider.get_headers_calls(), 3);
    assert_eq!(provider.invalidations(), 0);
    assert!(requests(&server).await.is_empty());

    // Once the provider recovers, the next attempt sends the batch.
    let provider = TestProvider::failing(TABLE_NAME, 1);
    let recovering = client(&sdk, &provider, |b| b.recovery_retries(2)).await;
    recovering.ingest_records(records(0..1)).await.unwrap();
    assert_eq!(provider.get_headers_calls(), 2);
    assert_eq!(requests(&server).await.len(), 1);
}

#[tokio::test]
async fn stalled_invalidation_keeps_the_auth_rejection() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    server
        .inject_insert_responses(vec![error(Status::unauthenticated("expired"))])
        .await;
    let sdk = sdk(&url);
    let provider = Arc::new(TestProvider::with(TABLE_NAME, Duration::ZERO, true));
    let client = client(&sdk, &provider, |b| {
        b.recovery_retries(0).server_lack_of_ack_timeout_ms(500)
    })
    .await;

    let started = Instant::now();
    let error = bounded(client.ingest_records(records(0..1)))
        .await
        .unwrap_err();

    assert_eq!(status_of(error).code(), GrpcCode::Unauthenticated);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(provider.invalidations(), 1);
}

#[tokio::test]
async fn oauth_client_mints_once_and_remints_after_a_rejection() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    let (oauth, uc_url) = start_mock_oauth_server().await;
    oauth
        .set_responses(vec![
            MockTokenResponse::ok("token-a"),
            MockTokenResponse::ok("token-b"),
        ])
        .await;
    let sdk = ZerobusSdk::builder()
        .endpoint(&url)
        .unity_catalog_url(uc_url)
        .no_tls()
        .build()
        .unwrap();
    let client = sdk
        .unary_client_builder()
        .table(TABLE_NAME)
        .oauth("client-id", "client-secret")
        .json()
        .recovery_backoff_ms(10)
        .build()
        .await
        .unwrap();

    client.ingest_records(records(0..1)).await.unwrap();
    client.ingest_records(records(1..2)).await.unwrap();
    assert_eq!(oauth.mint_count(), 1, "the cached token is reused");

    server
        .inject_insert_responses(vec![error(Status::unauthenticated("revoked"))])
        .await;
    client.ingest_records(records(2..3)).await.unwrap();

    let requests = requests(&server).await;
    let tokens: Vec<_> = requests
        .iter()
        .map(|r| header(r, "authorization").unwrap())
        .collect();
    assert_eq!(
        tokens,
        [
            "Bearer token-a",
            "Bearer token-a",
            "Bearer token-a",
            "Bearer token-b"
        ]
    );
    assert!(requests
        .iter()
        .all(|r| header(r, "x-databricks-zerobus-table-name") == Some(TABLE_NAME)));
    assert_eq!(oauth.mint_count(), 2);
}

#[tokio::test]
async fn no_auth_sends_no_credentials() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    let sdk = sdk(&url);
    let client = sdk
        .unary_client_builder()
        .table(TABLE_NAME)
        .no_auth()
        .json()
        .build()
        .await
        .unwrap();

    client.ingest_records(records(0..1)).await.unwrap();

    let requests = requests(&server).await;
    assert_eq!(header(&requests[0], "authorization"), None);
    assert_eq!(requests[0].table_name.as_deref(), Some(TABLE_NAME));
}

// ─── Timeouts ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn attempt_timeout_reports_deadline_exceeded() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    server
        .inject_insert_responses(vec![MockInsertResponse::Hang])
        .await;
    let sdk = sdk(&url);
    let client = client(&sdk, &TestProvider::new(TABLE_NAME), |b| {
        b.recovery(false).server_lack_of_ack_timeout_ms(1_000)
    })
    .await;

    let started = Instant::now();
    let error = bounded(client.ingest_records(records(0..1)))
        .await
        .unwrap_err();
    let elapsed = started.elapsed();

    assert!(error.is_retryable());
    assert_eq!(status_of(error).code(), GrpcCode::DeadlineExceeded);
    // Below twice the timeout, loose enough for a loaded CI runner.
    assert!(
        elapsed >= Duration::from_secs(1) && elapsed < Duration::from_millis(1_900),
        "{elapsed:?}"
    );
    assert_eq!(requests(&server).await.len(), 1);
}

#[tokio::test]
async fn timed_out_attempt_is_retried() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    server
        .inject_insert_responses(vec![MockInsertResponse::Hang])
        .await;
    let sdk = sdk(&url);
    let client = client(&sdk, &TestProvider::new(TABLE_NAME), |b| {
        b.recovery_retries(2).server_lack_of_ack_timeout_ms(300)
    })
    .await;

    bounded(client.ingest_records(records(0..1))).await.unwrap();

    assert_eq!(requests(&server).await.len(), 2);
}

#[tokio::test]
async fn late_success_is_reported_as_a_timeout() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    server
        .inject_insert_responses(vec![MockInsertResponse::Ok { delay_ms: 800 }])
        .await;
    let sdk = sdk(&url);
    let client = client(&sdk, &TestProvider::new(TABLE_NAME), |b| {
        b.recovery(false).server_lack_of_ack_timeout_ms(300)
    })
    .await;

    // The server may still write this batch: a timeout leaves it unknown.
    let error = bounded(client.ingest_records(records(0..1)))
        .await
        .unwrap_err();

    assert_eq!(status_of(error).code(), GrpcCode::DeadlineExceeded);
}

#[tokio::test]
async fn response_within_the_timeout_succeeds() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    server
        .inject_insert_responses(vec![MockInsertResponse::Ok { delay_ms: 200 }])
        .await;
    let sdk = sdk(&url);
    let client = client(&sdk, &TestProvider::new(TABLE_NAME), |b| {
        b.recovery(false).server_lack_of_ack_timeout_ms(1_000)
    })
    .await;

    client.ingest_records(records(0..1)).await.unwrap();
}

#[tokio::test]
async fn slow_headers_provider_counts_toward_the_attempt_timeout() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    let sdk = sdk(&url);
    let provider = Arc::new(TestProvider::with(
        TABLE_NAME,
        Duration::from_secs(5),
        false,
    ));
    let client = client(&sdk, &provider, |b| {
        b.recovery(false).server_lack_of_ack_timeout_ms(300)
    })
    .await;

    let started = Instant::now();
    let error = bounded(client.ingest_records(records(0..1)))
        .await
        .unwrap_err();

    assert_eq!(status_of(error).code(), GrpcCode::DeadlineExceeded);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    assert!(requests(&server).await.is_empty());
}

// ─── Connections ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn build_sends_nothing_and_the_first_call_connects() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    let proxy = FaultProxy::start(&url).await;
    let sdk = sdk(&proxy.url);
    let client = client(&sdk, &TestProvider::new(TABLE_NAME), |b| b).await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(proxy.accepted(), 0);

    client.ingest_records(records(0..1)).await.unwrap();
    assert_eq!(proxy.accepted(), 1);
    assert_eq!(requests(&server).await.len(), 1);
}

#[tokio::test]
async fn unary_clients_of_one_sdk_share_one_connection() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    let sdk = sdk(&url);
    let first = client(&sdk, &TestProvider::new(TABLE_NAME), |b| b).await;
    let second = client(&sdk, &TestProvider::new(OTHER_TABLE_NAME), |b| {
        b.json_lines()
    })
    .await;

    for id in 0..3 {
        first.ingest_records(records(id..id + 1)).await.unwrap();
        second.ingest_records(records(id..id + 1)).await.unwrap();
    }

    assert_eq!(requests(&server).await.len(), 6);
    assert_eq!(server.get_insert_connection_count().await, 1);
}

#[tokio::test]
async fn separate_sdks_use_separate_connections() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    let (first_sdk, second_sdk) = (sdk(&url), sdk(&url));
    let provider = TestProvider::new(TABLE_NAME);
    let first = client(&first_sdk, &provider, |b| b).await;
    let second = client(&second_sdk, &provider, |b| b).await;

    first.ingest_records(records(0..1)).await.unwrap();
    second.ingest_records(records(1..2)).await.unwrap();

    assert_eq!(server.get_insert_connection_count().await, 2);
}

#[tokio::test]
async fn unary_and_streams_never_share_a_connection() {
    setup_tracing();
    for connection_per_stream in [false, true] {
        let (server, url) = start_mock_server().await.unwrap();
        server
            .inject_responses(
                TABLE_NAME,
                vec![MockResponse::CreateStream {
                    stream_id: "stream".to_string(),
                    delay_ms: 0,
                }],
            )
            .await;
        let sdk = sdk_builder(&url)
            .connection_per_stream(connection_per_stream)
            .build()
            .unwrap();
        let provider = TestProvider::new(TABLE_NAME);
        let mut stream = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::clone(&provider) as Arc<dyn HeadersProvider>)
            .json()
            .build()
            .await
            .unwrap();
        let unary = client(&sdk, &provider, |b| b).await;

        unary.ingest_records(records(0..1)).await.unwrap();

        let stream_peers = server.get_connection_addresses().await;
        let unary_peers: HashSet<_> = requests(&server)
            .await
            .iter()
            .filter_map(|r| r.remote_addr)
            .collect();
        assert_eq!(stream_peers.len(), 1);
        assert_eq!(unary_peers.len(), 1);
        assert!(
            stream_peers.is_disjoint(&unary_peers),
            "connection_per_stream={connection_per_stream}"
        );
        stream.close().await.unwrap();
    }
}

#[tokio::test]
async fn reconnects_after_the_connection_is_closed() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    let proxy = FaultProxy::start(&url).await;
    let sdk = sdk(&proxy.url);
    let client = client(&sdk, &TestProvider::new(TABLE_NAME), |b| {
        b.recovery_retries(3)
    })
    .await;

    client.ingest_records(records(0..1)).await.unwrap();
    proxy.sever();
    tokio::time::sleep(Duration::from_millis(100)).await;
    client.ingest_records(records(1..2)).await.unwrap();

    assert_eq!(proxy.accepted(), 2);
    assert_eq!(server.get_insert_connection_count().await, 2);
}

#[tokio::test]
async fn keepalive_replaces_a_silently_dropped_connection() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    let proxy = FaultProxy::start(&url).await;
    let sdk = sdk_builder(&proxy.url)
        .unary_keepalive(Duration::from_millis(300), Duration::from_millis(300))
        .build()
        .unwrap();
    let client = client(&sdk, &TestProvider::new(TABLE_NAME), |b| {
        b.recovery_retries(2).server_lack_of_ack_timeout_ms(10_000)
    })
    .await;

    bounded(client.ingest_records(records(0..1))).await.unwrap();
    proxy.black_hole();
    let started = Instant::now();
    bounded(client.ingest_records(records(1..2))).await.unwrap();
    let elapsed = started.elapsed();

    // The keepalive, not the 10 s attempt timeout, detected the dead connection.
    assert!(elapsed < Duration::from_secs(3), "{elapsed:?}");
    assert_eq!(proxy.accepted(), 2);
    // The black hole swallowed the first attempt of the second call.
    assert_eq!(requests(&server).await.len(), 2);
}

#[tokio::test]
async fn recovers_when_the_attempt_timeout_is_shorter_than_the_keepalive() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    let proxy = FaultProxy::start(&url).await;
    let sdk = sdk_builder(&proxy.url)
        .unary_keepalive(Duration::from_millis(1_000), Duration::from_millis(500))
        .build()
        .unwrap();
    let client = client(&sdk, &TestProvider::new(TABLE_NAME), |b| {
        b.recovery_retries(6)
            .recovery_backoff_ms(100)
            .server_lack_of_ack_timeout_ms(600)
    })
    .await;

    bounded(client.ingest_records(records(0..1))).await.unwrap();
    proxy.black_hole();
    bounded(client.ingest_records(records(1..2))).await.unwrap();

    assert_eq!(proxy.accepted(), 2);
    assert_eq!(requests(&server).await.len(), 2);
}

#[tokio::test]
async fn without_a_keepalive_ping_a_dropped_connection_only_times_out() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    let proxy = FaultProxy::start(&url).await;
    // The default keepalive (30 s) cannot fire within these attempts.
    let sdk = sdk(&proxy.url);
    let client = client(&sdk, &TestProvider::new(TABLE_NAME), |b| {
        b.recovery_retries(1).server_lack_of_ack_timeout_ms(300)
    })
    .await;

    bounded(client.ingest_records(records(0..1))).await.unwrap();
    proxy.black_hole();
    let error = bounded(client.ingest_records(records(1..2)))
        .await
        .unwrap_err();

    assert_eq!(status_of(error).code(), GrpcCode::DeadlineExceeded);
    assert_eq!(
        proxy.accepted(),
        1,
        "both attempts used the dead connection"
    );
    assert_eq!(requests(&server).await.len(), 1);
}

// ─── Retry timing ─────────────────────────────────────────────────────────────

/// The time between consecutive calls the mock received.
fn arrival_gaps(requests: &[ReceivedInsert]) -> Vec<Duration> {
    requests
        .windows(2)
        .map(|pair| pair[1].received_at - pair[0].received_at)
        .collect()
}

#[tokio::test]
async fn every_retry_waits_the_backoff() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    server
        .inject_insert_responses(vec![
            unavailable(),
            error(Status::internal("boom")),
            unavailable(),
        ])
        .await;
    let sdk = sdk(&url);
    let backoff = Duration::from_millis(500);
    let client = client(&sdk, &TestProvider::new(TABLE_NAME), |b| {
        b.recovery_retries(4).recovery_backoff_ms(500)
    })
    .await;

    client.ingest_records(records(0..1)).await.unwrap();

    // The upper bound is one extra backoff, loose enough for a loaded CI runner.
    let requests = requests(&server).await;
    assert_eq!(requests.len(), 4);
    for (i, gap) in arrival_gaps(&requests).into_iter().enumerate() {
        assert!(
            gap >= backoff && gap < 2 * backoff,
            "gap {i}: {gap:?} is not within [{backoff:?}, {:?})",
            2 * backoff
        );
    }
}

type Configure = fn(UnaryClientBuilder<'_>) -> UnaryClientBuilder<'_>;

#[tokio::test]
async fn errors_that_are_not_retried_return_without_waiting() {
    setup_tracing();
    let cases: [(&str, Status, Configure); 4] = [
        ("non-retryable code", Status::invalid_argument("bad"), |b| b),
        ("recovery off", Status::unavailable("busy"), |b| {
            b.recovery(false)
        }),
        ("no retries", Status::unavailable("busy"), |b| {
            b.recovery_retries(0)
        }),
        (
            "auth rejection without retries",
            Status::unauthenticated("expired"),
            |b| b.recovery_retries(0),
        ),
    ];
    for (name, status, configure) in cases {
        let (server, url) = start_mock_server().await.unwrap();
        server.inject_insert_responses(vec![error(status)]).await;
        let sdk = sdk(&url);
        let client = client(&sdk, &TestProvider::new(TABLE_NAME), |b| {
            configure(b.recovery_retries(4).recovery_backoff_ms(2_000))
        })
        .await;

        let started = Instant::now();
        client.ingest_records(records(0..1)).await.unwrap_err();

        // Well below the 2 s backoff, so any wait would show.
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{name}: {:?}",
            started.elapsed()
        );
        assert_eq!(requests(&server).await.len(), 1, "{name}");
    }
}

// ─── Multithreading ───────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_calls_on_a_multi_thread_runtime_arrive_intact() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    server
        .inject_insert_responses(
            (0..64)
                .map(|i| MockInsertResponse::Ok {
                    delay_ms: i % 5 * 10,
                })
                .collect(),
        )
        .await;
    let sdk = sdk(&url);
    let client = Arc::new(client(&sdk, &TestProvider::new(TABLE_NAME), |b| b).await);

    let calls: Vec<_> = (0..64)
        .map(|task| {
            let client = Arc::clone(&client);
            tokio::spawn(
                async move { client.ingest_records(records(task * 5..task * 5 + 5)).await },
            )
        })
        .collect();
    for call in calls {
        call.await.unwrap().unwrap();
    }

    let received: HashSet<String> = requests(&server)
        .await
        .iter()
        .map(|r| body(r).to_string())
        .collect();
    let expected: HashSet<String> = (0..64)
        .map(|task| format!("[{}]", records(task * 5..task * 5 + 5).join(",")))
        .collect();
    assert_eq!(received, expected);
    assert_eq!(server.get_insert_connection_count().await, 1);
}

#[test]
fn calls_from_many_os_threads_through_one_client() {
    setup_tracing();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let (server, client) = runtime.block_on(async {
        let (server, url) = start_mock_server().await.unwrap();
        let sdk = sdk(&url);
        let client = client(&sdk, &TestProvider::new(TABLE_NAME), |b| b).await;
        (server, Arc::new(client))
    });

    // Each OS thread blocks on the shared runtime, as a synchronous binding does.
    let threads: Vec<_> = (0..16)
        .map(|thread| {
            let handle = runtime.handle().clone();
            let client = Arc::clone(&client);
            std::thread::spawn(move || {
                for call in 0..5 {
                    let first = thread * 100 + call * 10;
                    handle
                        .block_on(client.ingest_records(records(first..first + 3)))
                        .unwrap();
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }

    runtime.block_on(async {
        let received: HashSet<String> = requests(&server)
            .await
            .iter()
            .map(|r| body(r).to_string())
            .collect();
        let expected: HashSet<String> = (0..16)
            .flat_map(|thread| (0..5).map(move |call| thread * 100 + call * 10))
            .map(|first| format!("[{}]", records(first..first + 3).join(",")))
            .collect();
        assert_eq!(received, expected);
        assert_eq!(server.get_insert_connection_count().await, 1);
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_calls_have_independent_retry_budgets() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    let sdk = sdk(&url);
    // Table i fails i times before it succeeds; with 4 retries, table 5 fails.
    let tables: Vec<String> = (0..6).map(|i| format!("cat.schema.table_{i}")).collect();
    for (failures, table) in tables.iter().enumerate() {
        server
            .inject_insert_responses_for_table(table, vec![unavailable(); failures])
            .await;
    }
    let mut calls = Vec::new();
    for table in &tables {
        let client = client(&sdk, &TestProvider::new(table), |b| {
            b.recovery_retries(4).recovery_backoff_ms(100)
        })
        .await;
        calls.push(tokio::spawn(async move {
            client.ingest_records(records(0..1)).await
        }));
    }

    let results: Vec<_> = futures::future::join_all(calls).await;

    let requests = requests(&server).await;
    for (failures, (table, result)) in tables.iter().zip(results).enumerate() {
        let attempts = requests
            .iter()
            .filter(|r| r.table_name.as_deref() == Some(table.as_str()))
            .count();
        assert_eq!(attempts, (failures + 1).min(5), "{table}");
        assert_eq!(result.unwrap().is_ok(), failures <= 4, "{table}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_retrying_client_does_not_delay_another() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    server
        .inject_insert_responses_for_table(TABLE_NAME, vec![unavailable(); 3])
        .await;
    let sdk = sdk(&url);
    let retrying = client(&sdk, &TestProvider::new(TABLE_NAME), |b| {
        b.recovery_retries(4).recovery_backoff_ms(500)
    })
    .await;
    let healthy = client(&sdk, &TestProvider::new(OTHER_TABLE_NAME), |b| b).await;

    let started = Instant::now();
    let retrying_call = tokio::spawn(async move {
        retrying.ingest_records(records(0..1)).await.unwrap();
        started.elapsed()
    });
    for id in 0..10 {
        let call_started = Instant::now();
        healthy.ingest_records(records(id..id + 1)).await.unwrap();
        // Well below the other client's 500 ms backoff.
        assert!(
            call_started.elapsed() < Duration::from_millis(400),
            "{:?}",
            call_started.elapsed()
        );
    }
    let healthy_done = started.elapsed();
    let retrying_done = retrying_call.await.unwrap();

    assert!(
        retrying_done >= Duration::from_millis(1_500),
        "{retrying_done:?}"
    );
    assert!(
        healthy_done < Duration::from_millis(1_500),
        "{healthy_done:?}"
    );
    let requests = requests(&server).await;
    let count = |table: &str| {
        requests
            .iter()
            .filter(|r| r.table_name.as_deref() == Some(table))
            .count()
    };
    assert_eq!(count(TABLE_NAME), 4);
    assert_eq!(count(OTHER_TABLE_NAME), 10);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_clients_keep_their_own_table() {
    setup_tracing();
    let (server, url) = start_mock_server().await.unwrap();
    let sdk = sdk(&url);
    let first = Arc::new(client(&sdk, &TestProvider::new(TABLE_NAME), |b| b).await);
    let second = Arc::new(client(&sdk, &TestProvider::new(OTHER_TABLE_NAME), |b| b).await);

    let mut calls = Vec::new();
    for id in 0..10 {
        for (client, offset) in [(Arc::clone(&first), 0), (Arc::clone(&second), 1000)] {
            calls.push(tokio::spawn(async move {
                client
                    .ingest_records(records(offset + id..offset + id + 1))
                    .await
            }));
        }
    }
    for call in calls {
        call.await.unwrap().unwrap();
    }

    let requests = requests(&server).await;
    assert_eq!(requests.len(), 20);
    for request in &requests {
        let table = request.table_name.as_deref().unwrap();
        assert_eq!(
            header(request, "x-databricks-zerobus-table-name"),
            Some(table)
        );
        let value: serde_json::Value = serde_json::from_str(body(request)).unwrap();
        let id = value[0]["id"].as_i64().unwrap();
        assert_eq!(table == OTHER_TABLE_NAME, id >= 1000, "{table} got id {id}");
    }
}
