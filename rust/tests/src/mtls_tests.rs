//! End-to-end mutual TLS tests for `TlsSettings` client identities.
//!
//! The mock server requires a client certificate. The client uses a custom CA
//! to trust the server and presents its own identity. This file is a separate
//! test binary on purpose: `proxy_tests` sets process-wide proxy variables, and
//! these tests must not see them.

mod utils;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use databricks_zerobus_ingest_sdk::{
    HeadersProvider, TlsConfig, TlsSettings, ZerobusResult, ZerobusSdk,
};
use rcgen::{generate_simple_self_signed, CertifiedKey};
use tracing::info;
use utils::{setup_tracing, TestHeadersProvider};
use zerobus_test_support::mock_grpc::{start_mock_mtls_server, MockResponse, MockZerobusServer};

const TABLE_NAME: &str = "test_catalog.test_schema.test_table";

/// Fails the test unless `error` is the server's TLS alert from a rejected
/// handshake, so a failure for any other reason doesn't count as a rejection.
fn assert_tls_rejection(error: &dyn std::error::Error) {
    let message = error.to_string();
    assert!(
        message.contains("received fatal alert"),
        "expected a TLS handshake rejection, got: {message}"
    );
}

fn sdk_over(
    server_url: &str,
    settings: TlsSettings,
) -> Result<ZerobusSdk, Box<dyn std::error::Error>> {
    Ok(ZerobusSdk::builder()
        .endpoint(server_url)
        .unity_catalog_url("https://mock-uc.com")
        .tls_config(Arc::new(settings))
        .build()?)
}

/// Opens a stream over `settings`, ingests one record, and closes the stream.
async fn ingest_one_record(
    server_url: &str,
    settings: TlsSettings,
) -> Result<(), Box<dyn std::error::Error>> {
    ingest_one_record_with(&sdk_over(server_url, settings)?).await
}

/// Opens a stream on `sdk`, ingests one record, and closes the stream.
async fn ingest_one_record_with(sdk: &ZerobusSdk) -> Result<(), Box<dyn std::error::Error>> {
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
    assert_tls_rejection(error.as_ref());
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

/// A mutual TLS mock server that trusts one client identity, and identity files
/// that start out holding an identity the server doesn't trust.
struct RotationFixture {
    _mock_server: MockZerobusServer,
    server_url: String,
    server_ca_pem: Vec<u8>,
    _dir: tempfile::TempDir,
    cert_path: PathBuf,
    key_path: PathBuf,
    trusted_cert_pem: String,
    trusted_key_pem: String,
}

impl RotationFixture {
    async fn start(stream_id: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let trusted = generate_simple_self_signed(vec!["zerobus-test-client".to_string()])?;
        let untrusted = generate_simple_self_signed(vec!["zerobus-untrusted-client".to_string()])?;
        let trusted_cert_pem = trusted.cert.pem();

        let (mock_server, server_url, server_ca_pem) =
            start_mock_mtls_server(trusted_cert_pem.as_bytes()).await?;
        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: stream_id.to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let dir = tempfile::tempdir()?;
        let cert_path = dir.path().join("tls.crt");
        let key_path = dir.path().join("tls.key");
        std::fs::write(&cert_path, untrusted.cert.pem())?;
        std::fs::write(&key_path, untrusted.key_pair.serialize_pem())?;

        Ok(Self {
            _mock_server: mock_server,
            server_url,
            server_ca_pem,
            _dir: dir,
            cert_path,
            key_path,
            trusted_cert_pem,
            trusted_key_pem: trusted.key_pair.serialize_pem(),
        })
    }

    fn settings(&self) -> TlsSettings {
        TlsSettings::new()
            .without_native_roots()
            .with_ca_certificate_pem(self.server_ca_pem.clone())
            .with_domain_name("localhost")
            .with_client_identity_files(&self.cert_path, &self.key_path)
    }

    /// Replaces the identity files with the identity the server trusts.
    fn rotate(&self) -> std::io::Result<()> {
        std::fs::write(&self.cert_path, &self.trusted_cert_pem)?;
        std::fs::write(&self.key_path, &self.trusted_key_pem)
    }
}

/// Opens a stream on `sdk` with the original identity files, which the server
/// rejects, replaces the files, and opens the next stream on the same SDK.
async fn assert_next_stream_picks_up_rotation(
    sdk: &ZerobusSdk,
    fixture: &RotationFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    // A wrongly accepted untrusted identity would consume the injected responses
    // and succeed here.
    let before_rotation =
        tokio::time::timeout(Duration::from_secs(30), ingest_one_record_with(sdk))
            .await
            .map_err(|_| "the stream before rotation timed out")?;
    let error =
        before_rotation.expect_err("the server must reject the identity in the original files");
    assert_tls_rejection(error.as_ref());
    info!("Stream before rotation failed as expected: {error}");

    fixture.rotate()?;

    tokio::time::timeout(Duration::from_secs(30), ingest_one_record_with(sdk))
        .await
        .map_err(|_| "the stream after rotation timed out")??;
    Ok(())
}

/// Each stream gets its own channel by default, so replacing the identity files
/// takes effect on the next stream of the same SDK.
#[tokio::test]
async fn replaced_client_identity_files_take_effect_on_the_next_stream(
) -> Result<(), Box<dyn std::error::Error>> {
    setup_tracing();
    info!("Starting replaced_client_identity_files_take_effect_on_the_next_stream");

    let fixture = RotationFixture::start("mtls_rotation_stream").await?;
    let sdk = sdk_over(&fixture.server_url, fixture.settings())?;

    assert_next_stream_picks_up_rotation(&sdk, &fixture).await
}

/// With `connection_per_stream` off, streams share one channel. Before the SDK
/// reuses it, it reads the identity files again and opens a new channel when
/// they changed.
#[tokio::test]
async fn shared_channel_picks_up_replaced_identity_files_on_the_next_stream(
) -> Result<(), Box<dyn std::error::Error>> {
    setup_tracing();
    info!("Starting shared_channel_picks_up_replaced_identity_files_on_the_next_stream");

    let fixture = RotationFixture::start("mtls_shared_rotation_stream").await?;
    let sdk = ZerobusSdk::builder()
        .endpoint(&fixture.server_url)
        .unity_catalog_url("https://mock-uc.com")
        .tls_config(Arc::new(fixture.settings()))
        .connection_per_stream(false)
        .build()?;

    assert_next_stream_picks_up_rotation(&sdk, &fixture).await
}

/// Replaces the identity files the first time a stream asks for headers. An
/// attempt asks after it has its channel and before the TLS handshake, so the
/// attempt that triggers the rotation still presents the old identity.
struct RotateOnFirstHeaders {
    fixture: Arc<RotationFixture>,
    calls: AtomicUsize,
}

#[async_trait]
impl HeadersProvider for RotateOnFirstHeaders {
    async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.fixture
                .rotate()
                .expect("replacing the identity files must succeed");
        }
        TestHeadersProvider::default().get_headers().await
    }
}

/// Recovery asks for its channel again on every attempt, so identity files that
/// were replaced during a failed attempt are used by the next attempt.
#[tokio::test]
async fn recovery_picks_up_identity_files_replaced_during_a_failed_attempt(
) -> Result<(), Box<dyn std::error::Error>> {
    setup_tracing();
    info!("Starting recovery_picks_up_identity_files_replaced_during_a_failed_attempt");

    let fixture = Arc::new(RotationFixture::start("mtls_recovery_stream").await?);
    let sdk = sdk_over(&fixture.server_url, fixture.settings())?;
    let headers_provider = Arc::new(RotateOnFirstHeaders {
        fixture: Arc::clone(&fixture),
        calls: AtomicUsize::new(0),
    });

    let build = sdk
        .stream_builder()
        .table(TABLE_NAME)
        .headers_provider(Arc::clone(&headers_provider) as Arc<dyn HeadersProvider>)
        .json()
        .max_inflight_requests(10)
        .recovery(true)
        .recovery_retries(3)
        .recovery_backoff_ms(100)
        .build();
    let mut stream = tokio::time::timeout(Duration::from_secs(30), build)
        .await
        .map_err(|_| "opening the stream timed out")??;

    // The first attempt failed with the old identity and a later attempt opened
    // the stream with the new one.
    assert!(headers_provider.calls.load(Ordering::SeqCst) >= 2);

    stream
        .ingest_record_offset(r#"{"id": 1, "message": "mtls recovery test"}"#.to_string())
        .await?;
    stream.flush().await?;
    stream.close().await?;
    Ok(())
}

/// Returns the fixture's settings, and replaces the identity files on the second
/// call. The SDK reads the files after each call, so the first attempt reads the
/// original files.
struct RotateOnSecondTlsCall {
    fixture: Arc<RotationFixture>,
    calls: AtomicUsize,
}

impl TlsConfig for RotateOnSecondTlsCall {
    fn tls_settings(&self) -> ZerobusResult<TlsSettings> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
            self.fixture
                .rotate()
                .expect("replacing the identity files must succeed");
        }
        Ok(self.fixture.settings())
    }
}

/// A stream opens its first channel inside the retry loop, so a certificate and
/// key caught in the middle of a rotation are retried instead of failing the build.
#[tokio::test]
async fn stream_creation_retries_identity_files_caught_mid_rotation(
) -> Result<(), Box<dyn std::error::Error>> {
    setup_tracing();
    info!("Starting stream_creation_retries_identity_files_caught_mid_rotation");

    let fixture = Arc::new(RotationFixture::start("mtls_mid_rotation_stream").await?);
    // Only the certificate is replaced yet, so it doesn't match the key.
    std::fs::write(&fixture.cert_path, &fixture.trusted_cert_pem)?;
    let tls_config = Arc::new(RotateOnSecondTlsCall {
        fixture: Arc::clone(&fixture),
        calls: AtomicUsize::new(0),
    });
    let sdk = ZerobusSdk::builder()
        .endpoint(&fixture.server_url)
        .unity_catalog_url("https://mock-uc.com")
        .tls_config(Arc::clone(&tls_config) as Arc<dyn TlsConfig>)
        .build()?;

    let build = sdk
        .stream_builder()
        .table(TABLE_NAME)
        .headers_provider(Arc::new(TestHeadersProvider::default()))
        .json()
        .max_inflight_requests(10)
        .recovery(true)
        .recovery_retries(3)
        .recovery_backoff_ms(100)
        .build();
    let mut stream = tokio::time::timeout(Duration::from_secs(30), build)
        .await
        .map_err(|_| "opening the stream timed out")??;

    assert!(tls_config.calls.load(Ordering::SeqCst) >= 2);

    stream
        .ingest_record_offset(r#"{"id": 1, "message": "mtls mid-rotation test"}"#.to_string())
        .await?;
    stream.flush().await?;
    stream.close().await?;
    Ok(())
}
