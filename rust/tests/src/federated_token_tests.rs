//! End-to-end tests for external-IdP token federation (RFC 8693) through the
//! public stream builder.
//!
//! Each test builds real streams with `.federated_auth(...)`. The SDK exchanges
//! the supplier's IdP token at a loopback mock Unity Catalog token endpoint
//! (`mock_oauth`), which records every request, and then opens the stream on the
//! mock gRPC server, which records the `authorization` header each stream
//! arrived with. The tests count supplier calls and token exchanges, because
//! caching bugs show up only in counts, never as errors.

mod mock_oauth;
mod utils;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use databricks_zerobus_ingest_sdk::{
    IdpTokenCallback, IdpTokenSupplier, NoTlsConfig, ZerobusError, ZerobusResult, ZerobusSdk,
};

use futures::future::join_all;
use mock_oauth::{start_mock_oauth_server, MockOAuthServer, MockTokenResponse, RecordedRequest};
use utils::setup_tracing;
use zerobus_test_support::mock_grpc::{start_mock_server, MockResponse, MockZerobusServer};

const TABLE_NAME: &str = "catalog.schema.orders";
const OTHER_TABLE: &str = "catalog.schema.events";
const SP_CLIENT_ID: &str = "11111111-2222-3333-4444-555555555555";
const OTHER_SP_CLIENT_ID: &str = "66666666-7777-8888-9999-000000000000";
const TOKEN_EXCHANGE_GRANT: &str = "urn:ietf:params:oauth:grant-type:token-exchange";
const JWT_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:jwt";

/// A supplier that records how often the SDK called it.
struct CountingSupplier {
    supplier: IdpTokenSupplier,
    calls: Arc<AtomicUsize>,
}

impl CountingSupplier {
    /// `reply(n)` gives the result of the `n`th call (0-based); `delay` models a
    /// slow IdP round-trip.
    fn new<F>(delay: Duration, reply: F) -> Self
    where
        F: Fn(usize) -> ZerobusResult<String> + Send + Sync + 'static,
    {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&calls);
        let reply = Arc::new(reply);
        let call: IdpTokenCallback = Arc::new(move || {
            let result = reply(counter.fetch_add(1, Ordering::SeqCst));
            Box::pin(async move {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                result
            })
        });
        Self {
            supplier: IdpTokenSupplier::new(call),
            calls,
        }
    }

    /// Returns `idp_jwt(label, n)` on the `n`th call.
    fn jwts(label: &'static str) -> Self {
        Self::new(Duration::ZERO, move |n| Ok(idp_jwt(label, n)))
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

/// A realistic ~1.5 KB JWT-shaped IdP token, distinct per `(label, n)` and the
/// same on every call, so a test can rebuild the token it expects. Its `exp`
/// (2100-01-01) never caps the exchanged token's TTL.
fn idp_jwt(label: &str, n: usize) -> String {
    let exp = 4_102_444_800u64;
    let header = r#"{"alg":"RS256","kid":"test-key-1","typ":"JWT"}"#;
    let payload = format!(
        r#"{{"aud":"api://databricks","iss":"https://login.microsoftonline.com/tenant/v2.0","sub":"{label}-{n}","oid":"{label}","exp":{exp},"pad":"{}"}}"#,
        "x".repeat(700)
    );
    let signature: Vec<u8> = (0..256u32).map(|i| (i * 7 + n as u32) as u8).collect();
    format!(
        "{}.{}.{}",
        base64url(header.as_bytes()),
        base64url(payload.as_bytes()),
        base64url(&signature)
    )
}

/// Unpadded base64url, as used in JWT segments.
fn base64url(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |acc, (i, &b)| acc | (b as u32) << (16 - 8 * i));
        for i in 0..=chunk.len() {
            out.push(ALPHABET[(n >> (18 - 6 * i) & 0x3f) as usize] as char);
        }
    }
    out
}

fn build_sdk(grpc_url: String, uc_url: String) -> ZerobusSdk {
    ZerobusSdk::builder()
        .endpoint(grpc_url)
        .unity_catalog_url(uc_url)
        .tls_config(Arc::new(NoTlsConfig))
        .build()
        .expect("SDK should build")
}

/// `count` successful `CreateStream` replies for one table.
async fn accept_streams(grpc: &MockZerobusServer, table: &str, count: usize) {
    let responses = (0..count)
        .map(|i| MockResponse::CreateStream {
            stream_id: format!("{table}-s{i}"),
            delay_ms: 0,
        })
        .collect();
    grpc.inject_responses(table, responses).await;
}

/// `n` one-hour tokens `dbx-token-0`, `dbx-token-1`, ...
async fn grant_tokens(oauth: &MockOAuthServer, n: usize) {
    let responses = (0..n)
        .map(|i| MockTokenResponse::ok(format!("dbx-token-{i}")))
        .collect();
    oauth.set_responses(responses).await;
}

/// Builds one JSON stream with recovery off and returns the build result.
async fn federated_stream(
    sdk: &ZerobusSdk,
    table: &str,
    supplier: &IdpTokenSupplier,
    client_id: Option<&str>,
) -> ZerobusResult<()> {
    sdk.stream_builder()
        .table(table)
        .federated_auth(supplier.clone(), client_id.map(str::to_string))
        .json()
        .recovery(false)
        .build()
        .await
        .map(|_| ())
}

/// The bearer token each stream on the mock gRPC server arrived with.
async fn stream_bearers(grpc: &MockZerobusServer) -> Vec<String> {
    grpc.get_stream_auth()
        .await
        .into_iter()
        .map(|auth| {
            auth.authorization
                .as_deref()
                .and_then(|value| value.strip_prefix("Bearer "))
                .expect("every stream must carry a Bearer authorization")
                .to_string()
        })
        .collect()
}

/// Asserts one exchange request matches the RFC 8693 contract.
fn assert_exchange_request(
    request: &RecordedRequest,
    subject_token: &str,
    client_id: Option<&str>,
) {
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/oidc/v1/token");
    assert_eq!(
        request.header("content-type"),
        Some("application/x-www-form-urlencoded")
    );
    assert_eq!(
        request.header("authorization"),
        None,
        "the token exchange must not carry client-credentials auth"
    );
    assert_eq!(request.form_value("grant_type"), Some(TOKEN_EXCHANGE_GRANT));
    assert_eq!(
        request.form_value("subject_token_type"),
        Some(JWT_TOKEN_TYPE)
    );
    let sent = request
        .form_value("subject_token")
        .expect("the exchange must carry subject_token");
    assert!(
        sent == subject_token,
        "subject_token must be byte-identical to the supplier's token \
         (sent {} bytes, supplier returned {} bytes)",
        sent.len(),
        subject_token.len()
    );
    assert_eq!(
        request.form_value("client_id"),
        client_id,
        "client_id is sent only for workload identity federation"
    );
}

/// The exchange request on the wire, and the minted token reaching gRPC.
mod wire_contract_tests {
    use super::*;

    async fn exchange_request_and_bearer(client_id: Option<&str>) {
        setup_tracing();
        let (grpc, grpc_url) = start_mock_server().await.unwrap();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        grant_tokens(&oauth, 1).await;
        accept_streams(&grpc, TABLE_NAME, 1).await;
        let sdk = build_sdk(grpc_url, uc_url);
        let supplier = CountingSupplier::jwts("wire");

        federated_stream(&sdk, TABLE_NAME, &supplier.supplier, client_id)
            .await
            .expect("federated stream should build");

        let requests = oauth.requests().await;
        assert_eq!(requests.len(), 1, "one exchange");
        assert_exchange_request(&requests[0], &idp_jwt("wire", 0), client_id);

        let auth = grpc.get_stream_auth().await;
        assert_eq!(auth.len(), 1);
        assert_eq!(
            auth[0].authorization.as_deref(),
            Some("Bearer dbx-token-0"),
            "the stream must use the exchanged token"
        );
        assert_eq!(auth[0].table_name.as_deref(), Some(TABLE_NAME));
    }

    #[tokio::test]
    async fn account_level_exchange_request_and_bearer() {
        exchange_request_and_bearer(None).await;
    }

    #[tokio::test]
    async fn workload_identity_exchange_request_and_bearer() {
        exchange_request_and_bearer(Some(SP_CLIENT_ID)).await;
    }

    #[tokio::test]
    async fn exchange_is_scoped_like_oauth_and_cached_separately() {
        setup_tracing();
        let (grpc, grpc_url) = start_mock_server().await.unwrap();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        grant_tokens(&oauth, 2).await;
        accept_streams(&grpc, TABLE_NAME, 2).await;
        let sdk = build_sdk(grpc_url, uc_url);
        let supplier = CountingSupplier::jwts("scope");

        sdk.stream_builder()
            .table(TABLE_NAME)
            .oauth(SP_CLIENT_ID, "test-client-secret")
            .json()
            .recovery(false)
            .build()
            .await
            .expect("OAuth stream should build");
        federated_stream(&sdk, TABLE_NAME, &supplier.supplier, Some(SP_CLIENT_ID))
            .await
            .expect("federated stream should build");

        let requests = oauth.requests().await;
        assert_eq!(
            requests.len(),
            2,
            "OAuth and federation on one table and client_id must not share a token"
        );
        let (oauth_request, exchange) = (&requests[0], &requests[1]);
        assert_eq!(
            oauth_request.form_value("grant_type"),
            Some("client_credentials")
        );
        assert_eq!(
            exchange.form_value("grant_type"),
            Some(TOKEN_EXCHANGE_GRANT)
        );
        for key in ["scope", "resource", "authorization_details"] {
            assert!(
                exchange.form_value(key).is_some(),
                "the exchange must carry {key}"
            );
            assert_eq!(
                exchange.form_value(key),
                oauth_request.form_value(key),
                "{key} must match the OAuth grant for the same table"
            );
        }
        assert_eq!(
            stream_bearers(&grpc).await,
            ["dbx-token-0", "dbx-token-1"],
            "each stream must use its own grant's token"
        );
    }
}

/// When the supplier is called and which exchanged token each stream gets.
mod caching_tests {
    use super::*;

    #[tokio::test]
    async fn one_supplier_shares_one_exchange_across_streams() {
        setup_tracing();
        let (grpc, grpc_url) = start_mock_server().await.unwrap();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        grant_tokens(&oauth, 2).await;
        accept_streams(&grpc, TABLE_NAME, 3).await;
        let sdk = build_sdk(grpc_url, uc_url);
        let supplier = CountingSupplier::jwts("shared");

        for _ in 0..3 {
            federated_stream(&sdk, TABLE_NAME, &supplier.supplier, None)
                .await
                .expect("federated stream should build");
        }

        assert_eq!(
            supplier.calls(),
            1,
            "a cache hit must not call the supplier"
        );
        assert_eq!(oauth.mint_count(), 1, "a cache hit must not exchange again");
        assert_eq!(stream_bearers(&grpc).await, ["dbx-token-0"; 3]);
    }

    #[tokio::test]
    async fn distinct_account_level_suppliers_never_share_a_token() {
        setup_tracing();
        let (grpc, grpc_url) = start_mock_server().await.unwrap();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        grant_tokens(&oauth, 2).await;
        accept_streams(&grpc, TABLE_NAME, 3).await;
        let sdk = build_sdk(grpc_url, uc_url);
        let alice = CountingSupplier::jwts("alice");
        let bob = CountingSupplier::jwts("bob");

        federated_stream(&sdk, TABLE_NAME, &alice.supplier, None)
            .await
            .unwrap();
        federated_stream(&sdk, TABLE_NAME, &bob.supplier, None)
            .await
            .unwrap();
        federated_stream(&sdk, TABLE_NAME, &alice.supplier, None)
            .await
            .unwrap();

        assert_eq!((alice.calls(), bob.calls()), (1, 1));
        let requests = oauth.requests().await;
        assert_eq!(requests.len(), 2, "one exchange per identity");
        assert_exchange_request(&requests[0], &idp_jwt("alice", 0), None);
        assert_exchange_request(&requests[1], &idp_jwt("bob", 0), None);
        assert_eq!(
            stream_bearers(&grpc).await,
            ["dbx-token-0", "dbx-token-1", "dbx-token-0"],
            "each identity's streams must use only that identity's token"
        );
    }

    #[tokio::test]
    async fn workload_identity_is_keyed_by_client_id() {
        setup_tracing();
        let (grpc, grpc_url) = start_mock_server().await.unwrap();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        grant_tokens(&oauth, 2).await;
        accept_streams(&grpc, TABLE_NAME, 3).await;
        let sdk = build_sdk(grpc_url, uc_url);
        let first = CountingSupplier::jwts("wif-a");
        let second = CountingSupplier::jwts("wif-b");
        let other = CountingSupplier::jwts("wif-c");

        federated_stream(&sdk, TABLE_NAME, &first.supplier, Some(SP_CLIENT_ID))
            .await
            .unwrap();
        // A different supplier for the same service principal shares its token.
        federated_stream(&sdk, TABLE_NAME, &second.supplier, Some(SP_CLIENT_ID))
            .await
            .unwrap();
        federated_stream(&sdk, TABLE_NAME, &other.supplier, Some(OTHER_SP_CLIENT_ID))
            .await
            .unwrap();

        assert_eq!((first.calls(), second.calls(), other.calls()), (1, 0, 1));
        let requests = oauth.requests().await;
        assert_eq!(requests.len(), 2, "one exchange per service principal");
        assert_exchange_request(&requests[0], &idp_jwt("wif-a", 0), Some(SP_CLIENT_ID));
        assert_exchange_request(&requests[1], &idp_jwt("wif-c", 0), Some(OTHER_SP_CLIENT_ID));
        assert_eq!(
            stream_bearers(&grpc).await,
            ["dbx-token-0", "dbx-token-0", "dbx-token-1"]
        );
    }

    #[tokio::test]
    async fn exchanged_tokens_are_per_table() {
        setup_tracing();
        let (grpc, grpc_url) = start_mock_server().await.unwrap();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        grant_tokens(&oauth, 2).await;
        accept_streams(&grpc, TABLE_NAME, 1).await;
        accept_streams(&grpc, OTHER_TABLE, 1).await;
        let sdk = build_sdk(grpc_url, uc_url);
        let supplier = CountingSupplier::jwts("tables");

        federated_stream(&sdk, TABLE_NAME, &supplier.supplier, None)
            .await
            .unwrap();
        federated_stream(&sdk, OTHER_TABLE, &supplier.supplier, None)
            .await
            .unwrap();

        assert_eq!(supplier.calls(), 2);
        assert_eq!(oauth.mint_count(), 2, "a token is scoped to one table");
        let auth = grpc.get_stream_auth().await;
        assert_eq!(auth[1].table_name.as_deref(), Some(OTHER_TABLE));
        assert_eq!(auth[1].authorization.as_deref(), Some("Bearer dbx-token-1"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_creates_call_a_slow_supplier_once() {
        const BUILDS: usize = 8;
        setup_tracing();
        let (grpc, grpc_url) = start_mock_server().await.unwrap();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        grant_tokens(&oauth, 2).await;
        accept_streams(&grpc, TABLE_NAME, BUILDS).await;
        let sdk = Arc::new(build_sdk(grpc_url, uc_url));
        let supplier = Arc::new(CountingSupplier::new(Duration::from_millis(500), |n| {
            Ok(idp_jwt("slow", n))
        }));

        let builds: Vec<_> = (0..BUILDS)
            .map(|_| {
                let sdk = Arc::clone(&sdk);
                let supplier = Arc::clone(&supplier);
                tokio::spawn(async move {
                    federated_stream(&sdk, TABLE_NAME, &supplier.supplier, None).await
                })
            })
            .collect();
        for (i, result) in join_all(builds).await.into_iter().enumerate() {
            let result = result.expect("build task must not panic");
            assert!(result.is_ok(), "concurrent stream {i}: {:?}", result.err());
        }

        assert_eq!(
            supplier.calls(),
            1,
            "concurrent creates must call the supplier once"
        );
        assert_eq!(
            oauth.mint_count(),
            1,
            "concurrent creates must exchange once"
        );
    }
}

/// Supplier failures, refresh, rejection and token-endpoint failures.
mod lifecycle_tests {
    use super::*;

    #[tokio::test]
    async fn supplier_error_is_retryable_and_not_cached() {
        setup_tracing();
        let (grpc, grpc_url) = start_mock_server().await.unwrap();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        grant_tokens(&oauth, 1).await;
        accept_streams(&grpc, TABLE_NAME, 1).await;
        let sdk = build_sdk(grpc_url, uc_url);
        // The first call fails as a transient IdP outage; the second succeeds.
        let supplier = CountingSupplier::new(Duration::ZERO, |n| match n {
            0 => Err(ZerobusError::TokenFetchError("IdP unavailable".to_string())),
            _ => Ok(idp_jwt("flaky", n)),
        });

        let err = federated_stream(&sdk, TABLE_NAME, &supplier.supplier, None)
            .await
            .expect_err("a supplier error must fail the build");
        assert!(err.is_retryable(), "a supplier error is retryable: {err:?}");
        assert!(err.to_string().contains("IdP unavailable"), "got {err}");
        assert_eq!(
            oauth.mint_count(),
            0,
            "nothing is exchanged without a token"
        );

        federated_stream(&sdk, TABLE_NAME, &supplier.supplier, None)
            .await
            .expect("the next create must call the supplier again and succeed");
        assert_eq!(
            supplier.calls(),
            2,
            "a failed supplier call must not be cached"
        );
        assert_exchange_request(&oauth.requests().await[0], &idp_jwt("flaky", 1), None);
    }

    #[tokio::test]
    async fn empty_token_is_permanent_and_never_sent() {
        for token in ["", "  \n"] {
            setup_tracing();
            let (_grpc, grpc_url) = start_mock_server().await.unwrap();
            let (oauth, uc_url) = start_mock_oauth_server().await;
            let sdk = build_sdk(grpc_url, uc_url);
            let supplier = CountingSupplier::new(Duration::ZERO, move |_| Ok(token.to_string()));

            let err = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .federated_auth(supplier.supplier.clone(), None)
                .json()
                .recovery(true)
                .recovery_retries(3)
                .recovery_backoff_ms(0)
                .build()
                .await
                .err()
                .expect("an empty IdP token must fail the build");

            assert!(
                matches!(err, ZerobusError::InvalidArgument(_)),
                "{token:?}: got {err:?}"
            );
            assert!(err.to_string().contains("empty token"), "got {err}");
            assert_eq!(supplier.calls(), 1, "{token:?} must not be retried");
            assert_eq!(oauth.mint_count(), 0, "{token:?} must never be sent");
        }
    }

    #[tokio::test]
    async fn refresh_calls_the_supplier_again_and_exchanges_its_new_token() {
        setup_tracing();
        let (grpc, grpc_url) = start_mock_server().await.unwrap();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        // dbx-token-0 lives 4s; the refresh lead is min(300s, 4s/2) = 2s, so after
        // 3s the next stream refreshes it.
        oauth
            .set_responses(vec![
                MockTokenResponse::ok("dbx-token-0").with_expires_in(Some("4")),
                MockTokenResponse::ok("dbx-token-1"),
            ])
            .await;
        accept_streams(&grpc, TABLE_NAME, 3).await;
        let sdk = build_sdk(grpc_url, uc_url);
        let supplier = CountingSupplier::jwts("refresh");

        federated_stream(&sdk, TABLE_NAME, &supplier.supplier, Some(SP_CLIENT_ID))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;
        federated_stream(&sdk, TABLE_NAME, &supplier.supplier, Some(SP_CLIENT_ID))
            .await
            .unwrap();
        federated_stream(&sdk, TABLE_NAME, &supplier.supplier, Some(SP_CLIENT_ID))
            .await
            .unwrap();

        assert_eq!(
            supplier.calls(),
            2,
            "the refresh must call the supplier again"
        );
        let requests = oauth.requests().await;
        assert_eq!(requests.len(), 2, "a cold exchange plus one refresh");
        assert_exchange_request(&requests[1], &idp_jwt("refresh", 1), Some(SP_CLIENT_ID));
        assert_eq!(
            stream_bearers(&grpc).await,
            ["dbx-token-0", "dbx-token-1", "dbx-token-1"],
            "streams after the refresh must use the new token"
        );
    }

    #[tokio::test]
    async fn auth_rejection_reexchanges_once() {
        setup_tracing();
        let (grpc, grpc_url) = start_mock_server().await.unwrap();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        grant_tokens(&oauth, 2).await;
        grpc.inject_responses(
            TABLE_NAME,
            vec![
                MockResponse::Error {
                    status: tonic::Status::unauthenticated("stale token"),
                    delay_ms: 0,
                },
                MockResponse::CreateStream {
                    stream_id: "s1".to_string(),
                    delay_ms: 0,
                },
            ],
        )
        .await;
        let sdk = build_sdk(grpc_url, uc_url);
        let supplier = CountingSupplier::jwts("rejected");

        sdk.stream_builder()
            .table(TABLE_NAME)
            .federated_auth(supplier.supplier.clone(), None)
            .json()
            .recovery(true)
            .recovery_retries(1)
            .recovery_backoff_ms(0)
            .build()
            .await
            .expect("the retry after a rejection should succeed");

        assert_eq!(
            supplier.calls(),
            2,
            "the rejected token must be re-exchanged"
        );
        assert_eq!(oauth.mint_count(), 2);
        assert_exchange_request(&oauth.requests().await[1], &idp_jwt("rejected", 1), None);
        assert_eq!(stream_bearers(&grpc).await, ["dbx-token-0", "dbx-token-1"]);
    }

    #[tokio::test]
    async fn repeated_auth_rejection_stops_after_one_reexchange() {
        setup_tracing();
        let (grpc, grpc_url) = start_mock_server().await.unwrap();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        grant_tokens(&oauth, 3).await;
        let rejection = || MockResponse::Error {
            status: tonic::Status::unauthenticated("stale token"),
            delay_ms: 0,
        };
        grpc.inject_responses(TABLE_NAME, vec![rejection(), rejection(), rejection()])
            .await;
        let sdk = build_sdk(grpc_url, uc_url);
        let supplier = CountingSupplier::jwts("revoked");

        let err = sdk
            .stream_builder()
            .table(TABLE_NAME)
            .federated_auth(supplier.supplier.clone(), None)
            .json()
            .recovery(true)
            .recovery_retries(3)
            .recovery_backoff_ms(0)
            .build()
            .await
            .err()
            .expect("repeated rejection must fail the build");

        assert!(!err.is_retryable(), "got {err:?}");
        assert_eq!(supplier.calls(), 2, "one re-exchange, then give up");
        assert_eq!(oauth.mint_count(), 2);
    }

    // Real time: the exchange is real socket I/O (see oauth_token_tests).
    #[tokio::test]
    async fn hung_refresh_falls_back_to_the_cached_token() {
        setup_tracing();
        let (grpc, grpc_url) = start_mock_server().await.unwrap();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        // A 6s token enters its 3s refresh window after 4s, leaving ~2s of life,
        // more than the 500ms refresh cap (recovery_timeout_ms / 2).
        oauth
            .set_responses(vec![
                MockTokenResponse::ok("dbx-token-0").with_expires_in(Some("6")),
                MockTokenResponse::Hang,
            ])
            .await;
        accept_streams(&grpc, TABLE_NAME, 2).await;
        let sdk = build_sdk(grpc_url, uc_url);
        let supplier = CountingSupplier::jwts("hung");
        // The cold first stream keeps the default timeout, because a cold
        // exchange can take over a second on a loaded CI runner.
        federated_stream(&sdk, TABLE_NAME, &supplier.supplier, None)
            .await
            .expect("first stream");
        tokio::time::sleep(Duration::from_secs(4)).await;
        sdk.stream_builder()
            .table(TABLE_NAME)
            .federated_auth(supplier.supplier.clone(), None)
            .json()
            .recovery(false)
            .recovery_timeout_ms(1000)
            .build()
            .await
            .expect("a hung refresh must fall back to the cached token");

        assert_eq!(supplier.calls(), 2, "the refresh must call the supplier");
        assert_eq!(oauth.mint_count(), 2, "the refresh must reach the endpoint");
        assert_eq!(stream_bearers(&grpc).await, ["dbx-token-0", "dbx-token-0"]);
    }

    #[tokio::test]
    async fn exchange_client_error_is_permanent_and_carries_the_reason() {
        for status in [400, 401] {
            setup_tracing();
            let (_grpc, grpc_url) = start_mock_server().await.unwrap();
            let (oauth, uc_url) = start_mock_oauth_server().await;
            oauth
                .set_responses(vec![MockTokenResponse::error(
                    status,
                    "federation policy mismatch",
                )])
                .await;
            let sdk = build_sdk(grpc_url, uc_url);
            let supplier = CountingSupplier::jwts("policy");

            let err = sdk
                .stream_builder()
                .table(TABLE_NAME)
                .federated_auth(supplier.supplier.clone(), Some(SP_CLIENT_ID.to_string()))
                .json()
                .recovery(true)
                .recovery_retries(3)
                .recovery_backoff_ms(0)
                .build()
                .await
                .err()
                .expect("a 4xx exchange must fail the build");

            assert!(
                matches!(err, ZerobusError::InvalidUCTokenError(_)),
                "HTTP {status}: got {err:?}"
            );
            assert!(!err.is_retryable(), "HTTP {status} is permanent");
            let message = err.to_string();
            assert!(
                message.contains("federation policy mismatch"),
                "HTTP {status}: the server's reason must surface, got {message}"
            );
            assert!(
                !message.contains(&idp_jwt("policy", 0)),
                "the error must not contain the IdP token"
            );
            assert_eq!(oauth.mint_count(), 1, "HTTP {status} must not be retried");
            assert_eq!(supplier.calls(), 1);
        }
    }

    #[tokio::test]
    async fn exchange_server_error_is_retried() {
        setup_tracing();
        let (grpc, grpc_url) = start_mock_server().await.unwrap();
        let (oauth, uc_url) = start_mock_oauth_server().await;
        oauth
            .set_responses(vec![
                MockTokenResponse::error(500, "token endpoint unavailable"),
                MockTokenResponse::ok("dbx-token-1"),
            ])
            .await;
        accept_streams(&grpc, TABLE_NAME, 1).await;
        let sdk = build_sdk(grpc_url, uc_url);
        let supplier = CountingSupplier::jwts("retry");

        let err = federated_stream(&sdk, TABLE_NAME, &supplier.supplier, None)
            .await
            .expect_err("with recovery off, the 500 fails the build");
        assert!(err.is_retryable(), "HTTP 500 is retryable: {err:?}");
        assert!(
            !err.to_string().contains(&idp_jwt("retry", 0)),
            "the error must not contain the IdP token"
        );

        federated_stream(&sdk, TABLE_NAME, &supplier.supplier, None)
            .await
            .expect("the next create must exchange again and succeed");
        assert_eq!(oauth.mint_count(), 2, "the failed exchange was not cached");
        assert_eq!(supplier.calls(), 2);
        assert_eq!(stream_bearers(&grpc).await, ["dbx-token-1"]);
    }
}
