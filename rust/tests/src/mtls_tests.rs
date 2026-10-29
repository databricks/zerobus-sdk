//! End-to-end mutual TLS test for `TlsSettings::with_client_identity_pem`.
//!
//! The mock server requires a client certificate. The client uses a custom CA
//! to trust the server and presents its own identity. This file is a separate
//! test binary on purpose: `proxy_tests` sets process-wide proxy variables, and
//! this test must not see them.

mod utils;

use std::sync::Arc;
use std::time::Duration;

use databricks_zerobus_ingest_sdk::{TlsSettings, ZerobusSdk};
use rcgen::{generate_simple_self_signed, CertifiedKey};
use tracing::info;
use utils::{setup_tracing, TestHeadersProvider};
use zerobus_test_support::mock_grpc::{start_mock_mtls_server, MockResponse};

const TABLE_NAME: &str = "test_catalog.test_schema.test_table";

/// Opens a stream over `settings`, ingests one record, and closes the stream.
async fn ingest_one_record(
    server_url: &str,
    settings: TlsSettings,
) -> Result<(), Box<dyn std::error::Error>> {
    let sdk = ZerobusSdk::builder()
        .endpoint(server_url)
        .unity_catalog_url("https://mock-uc.com")
        .tls_config(Arc::new(settings))
        .build()?;

    let mut stream = sdk
        .stream_builder()
        .table(TABLE_NAME)
        .headers_provider(Arc::new(TestHeadersProvider::default()))
        .json()
        .max_inflight_requests(10)
        .recovery(false)
        .build()
        .await?;

    stream
        .ingest_record_offset(r#"{"id": 1, "message": "mtls test"}"#.to_string())
        .await?;
    stream.flush().await?;
    stream.close().await?;

    Ok(())
}

#[tokio::test]
async fn client_identity_opens_a_stream_and_a_missing_identity_does_not(
) -> Result<(), Box<dyn std::error::Error>> {
    setup_tracing();
    info!("Starting client_identity_opens_a_stream_and_a_missing_identity_does_not");

    let CertifiedKey {
        cert: client_cert,
        key_pair: client_key,
    } = generate_simple_self_signed(vec!["zerobus-test-client".to_string()])?;
    let client_cert_pem = client_cert.pem();

    let (mock_server, server_url, server_ca_pem) =
        start_mock_mtls_server(client_cert_pem.as_bytes()).await?;
    mock_server
        .inject_responses(
            TABLE_NAME,
            vec![
                MockResponse::CreateStream {
                    stream_id: "mtls_test_stream".to_string(),
                    delay_ms: 0,
                },
                MockResponse::RecordAck {
                    ack_up_to_offset: 0,
                    delay_ms: 0,
                },
            ],
        )
        .await;

    let trust_server = || {
        TlsSettings::new()
            .without_native_roots()
            .with_ca_certificate_pem(server_ca_pem.clone())
            .with_domain_name("localhost")
    };

    // Without a client identity, the server rejects the handshake. This case runs
    // first: a rejected handshake never reaches the injected responses, so a
    // wrongly accepted connection would use them and fail the assertion below.
    let without_identity = tokio::time::timeout(
        Duration::from_secs(30),
        ingest_one_record(&server_url, trust_server()),
    )
    .await
    .map_err(|_| "the stream without a client identity timed out")?;
    let error =
        without_identity.expect_err("the server must reject a client that presents no certificate");
    info!("Stream without a client identity failed as expected: {error}");

    // With the client identity, the same server accepts the connection.
    let with_identity = trust_server()
        .with_client_identity_pem(client_cert_pem.clone(), client_key.serialize_pem());
    tokio::time::timeout(
        Duration::from_secs(30),
        ingest_one_record(&server_url, with_identity),
    )
    .await
    .map_err(|_| "the stream with a client identity timed out")??;

    Ok(())
}
