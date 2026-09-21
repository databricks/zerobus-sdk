use super::*;
use std::collections::HashMap;

struct NoopAckCallback;

impl<Id> AckCallback<Id> for NoopAckCallback {
    fn on_ack(&self, _offset_id: Id) {}

    fn on_error(&self, _offset_id: Id, _error_message: &str) {}
}

fn test_sdk() -> ZerobusSdk {
    ZerobusSdk::new_with_config(
        "http://localhost:1234".to_string(),
        "http://localhost:5678".to_string(),
        "test-workspace".to_string(),
        Arc::new(crate::tls_config::SecureTlsConfig::new()),
        None,
        Arc::from(crate::DEFAULT_SDK_IDENTIFIER),
        true,
        crate::token_cache::DEFAULT_REFRESH_BUFFER,
        true,
    )
}

#[test]
fn multiplexed_validates_lane_count_capacity_and_jitter() {
    let sdk = test_sdk();
    for (lanes, capacity, valid) in [
        (0, 100, false),
        (65, 100, false),
        (2, 1, false),
        (1, 1, true),
        (64, 64, true),
    ] {
        let builder = sdk
            .stream_builder()
            .table("t")
            .oauth("a", "b")
            .json()
            .max_inflight_requests(capacity)
            .multiplexed(lanes);
        assert_eq!(builder.validate().is_ok(), valid);
        if valid {
            assert_eq!(
                builder.max_inflight_requests_per_stream(),
                Some(capacity / lanes)
            );
            let delays = builder.sample_jitter_delays();
            assert_eq!(delays.len(), lanes);
            assert!(delays
                .iter()
                .all(|delay| *delay <= Duration::from_millis(MAX_MULTIPLEXED_JITTER_MS)));
            if lanes == 1 {
                assert_eq!(delays, [Duration::ZERO]);
            }
        }
    }
}

#[tokio::test]
async fn callback_modes_validate_before_opening_connections() {
    let sdk = test_sdk();
    let builder = || sdk.stream_builder().table("t").oauth("a", "b").json();
    assert!(builder()
        .ack_callback(Arc::new(NoopAckCallback))
        .validate()
        .is_ok());
    assert!(builder()
        .multiplexed_ack_callback(Arc::new(NoopAckCallback))
        .multiplexed(2)
        .validate()
        .is_ok());

    let ordinary = builder().multiplexed_ack_callback(Arc::new(NoopAckCallback));
    assert!(ordinary.validate().is_err());
    assert!(matches!(
        ordinary.build().await,
        Err(ZerobusError::InvalidArgument(_))
    ));
    let mux = builder()
        .ack_callback(Arc::new(NoopAckCallback))
        .multiplexed(2);
    assert!(mux.validate().is_err());
    assert!(matches!(
        mux.build().await,
        Err(ZerobusError::InvalidArgument(_))
    ));
}

#[cfg(feature = "avro")]
#[tokio::test]
async fn multiplexed_rejects_avro_before_opening_connections() {
    let sdk = test_sdk();
    let builder = sdk
        .stream_builder()
        .table("t")
        .oauth("a", "b")
        .avro(r#""string""#)
        .multiplexed(2);
    assert!(
        matches!(builder.validate(), Err(ZerobusError::InvalidArgument(message)) if message.contains("Avro"))
    );
    assert!(
        matches!(builder.build().await, Err(ZerobusError::InvalidArgument(message)) if message.contains("Avro"))
    );
}

#[test]
fn json_oauth_builder() {
    let sdk = test_sdk();
    let _builder = sdk
        .stream_builder()
        .table("catalog.schema.table")
        .oauth("cid", "csec")
        .json()
        .max_inflight_requests(100);
}

#[test]
fn compiled_proto_headers_provider() {
    struct StubProvider;
    #[async_trait::async_trait]
    impl HeadersProvider for StubProvider {
        async fn get_headers(&self) -> crate::ZerobusResult<HashMap<&'static str, String>> {
            Ok(HashMap::new())
        }
    }

    let sdk = test_sdk();
    let provider: Arc<dyn HeadersProvider> = Arc::new(StubProvider);
    let _builder = sdk
        .stream_builder()
        .table("catalog.schema.table")
        .headers_provider(provider)
        .compiled_proto(prost_types::DescriptorProto::default());
}

#[test]
fn dynamic_proto_sets_format_and_validates() {
    let sdk = test_sdk();
    let md = crate::message_descriptor(&prost_types::DescriptorProto {
        name: Some("T".to_string()),
        ..Default::default()
    })
    .unwrap();
    let builder = sdk
        .stream_builder()
        .table("t")
        .oauth("a", "b")
        .dynamic_proto(md);
    assert!(format!("{builder:?}").contains("DynamicProto"));
    builder.validate().expect("validation should succeed");
}

#[cfg(feature = "avro")]
#[test]
fn avro_sets_format_and_validates() {
    let sdk = test_sdk();
    let builder = sdk
        .stream_builder()
        .table("t")
        .oauth("a", "b")
        .avro(r#"{"type":"record","name":"R","fields":[]}"#);
    assert!(format!("{builder:?}").contains("Avro"));
    builder.validate().expect("validation should succeed");
}

#[cfg(feature = "avro")]
#[tokio::test]
async fn avro_rejects_invalid_schema_at_build() {
    let sdk = test_sdk();
    // Empty and malformed writer schemas are parsed and rejected at build(),
    // before any connection is attempted.
    for bad in ["", "{"] {
        let result = sdk
            .stream_builder()
            .table("t")
            .oauth("a", "b")
            .avro(bad)
            .build()
            .await;
        match result {
            Err(ZerobusError::AvroSchemaParseError(_)) => {}
            Err(e) => panic!("expected AvroSchemaParseError, got {e:?}"),
            Ok(_) => panic!("expected AvroSchemaParseError, got Ok"),
        }
    }
}

#[test]
fn any_order_format_before_auth() {
    let sdk = test_sdk();
    let _builder = sdk
        .stream_builder()
        .table("catalog.schema.table")
        .json()
        .oauth("cid", "csec")
        .max_inflight_requests(100);
}

#[test]
fn any_order_config_before_format() {
    let sdk = test_sdk();
    let _builder = sdk
        .stream_builder()
        .table("catalog.schema.table")
        .max_inflight_requests(100)
        .recovery(false)
        .oauth("cid", "csec")
        .json();
}

#[test]
fn config_setters_chain() {
    let sdk = test_sdk();
    let _builder = sdk
        .stream_builder()
        .table("t")
        .oauth("a", "b")
        .json()
        .recovery(false)
        .recovery_timeout_ms(10_000)
        .recovery_backoff_ms(1_000)
        .recovery_retries(3)
        .server_lack_of_ack_timeout_ms(30_000)
        .flush_timeout_ms(60_000)
        .max_inflight_requests(500)
        .stream_paused_max_wait_time_ms(Some(5_000))
        .callback_max_wait_time_ms(None);
}

#[test]
fn default_config_without_setters() {
    let sdk = test_sdk();
    let builder = sdk.stream_builder().table("t").oauth("a", "b").json();
    assert_eq!(builder.grpc_config.max_inflight_requests, 1_000_000);
    assert!(builder.grpc_config.recovery);
    assert_eq!(
        builder.grpc_config.max_ingest_payload_bytes,
        crate::stream_options::defaults::MAX_INGEST_PAYLOAD_BYTES
    );
    assert!(builder.grpc_config.max_ingest_payload_bytes < 10 * 1024 * 1024);
}

#[test]
fn max_ingest_payload_bytes_override() {
    let sdk = test_sdk();
    let builder = sdk
        .stream_builder()
        .table("t")
        .oauth("a", "b")
        .json()
        .max_ingest_payload_bytes(5 * 1024 * 1024);
    assert_eq!(
        builder.grpc_config.max_ingest_payload_bytes,
        5 * 1024 * 1024
    );
}

#[tokio::test]
async fn build_without_auth_returns_error() {
    let sdk = test_sdk();
    let result = sdk.stream_builder().table("t").json().build().await;
    match result {
        Err(ZerobusError::InvalidArgument(msg)) => {
            assert!(msg.contains("authentication is required"));
        }
        _ => panic!("expected InvalidArgument error"),
    }
}

#[tokio::test]
async fn build_without_table_returns_error() {
    let sdk = test_sdk();
    let result = sdk.stream_builder().oauth("a", "b").json().build().await;
    match result {
        Err(ZerobusError::InvalidArgument(msg)) => {
            assert!(msg.contains("table name is required"));
        }
        _ => panic!("expected InvalidArgument error"),
    }
}

#[tokio::test]
async fn build_without_format_returns_error() {
    let sdk = test_sdk();
    let result = sdk
        .stream_builder()
        .table("t")
        .oauth("a", "b")
        .build()
        .await;
    match result {
        Err(ZerobusError::InvalidArgument(msg)) => {
            assert!(msg.contains("record format is required"));
        }
        _ => panic!("expected InvalidArgument error"),
    }
}

#[test]
fn debug_impl_works() {
    let sdk = test_sdk();
    let builder = sdk.stream_builder().table("t").oauth("a", "b").json();
    let debug_str = format!("{:?}", builder);
    assert!(debug_str.contains("StreamBuilder"));
    assert!(debug_str.contains("OAuth"));
    assert!(debug_str.contains("Json"));
}

#[tokio::test]
async fn resolve_headers_provider_with_custom_provider() {
    struct TestProvider;

    #[async_trait::async_trait]
    impl HeadersProvider for TestProvider {
        async fn get_headers(&self) -> crate::ZerobusResult<HashMap<&'static str, String>> {
            let mut h = HashMap::new();
            h.insert("x-test", "value".to_string());
            Ok(h)
        }
    }

    let sdk = test_sdk();
    let builder = sdk
        .stream_builder()
        .table("catalog.schema.table")
        .headers_provider(Arc::new(TestProvider))
        .json();

    let provider = builder.resolve_headers_provider().unwrap();
    let headers = provider.get_headers().await.unwrap();
    assert_eq!(headers.get("x-test").unwrap(), "value");
}

#[cfg(feature = "testing")]
#[tokio::test]
async fn no_auth_resolves_to_no_auth_provider() {
    let sdk = test_sdk();
    let builder = sdk
        .stream_builder()
        .table("catalog.schema.table")
        .no_auth()
        .json();
    let provider = builder.resolve_headers_provider().unwrap();
    let headers = provider.get_headers().await.unwrap();
    assert!(headers.is_empty());
}

#[cfg(feature = "testing")]
#[tokio::test]
async fn no_auth_plus_no_tls_chain() {
    let sdk = crate::ZerobusSdkBuilder::new()
        .endpoint("http://localhost:1234")
        .no_tls()
        .build()
        .expect("sdk should build with no_tls");
    let builder = sdk
        .stream_builder()
        .table("catalog.schema.table")
        .no_auth()
        .json();
    builder.validate().expect("validation should succeed");
    let provider = builder.resolve_headers_provider().unwrap();
    let headers = provider.get_headers().await.unwrap();
    assert!(headers.is_empty());
}

#[tokio::test]
async fn resolve_headers_provider_with_oauth() {
    let sdk = test_sdk();
    let builder = sdk
        .stream_builder()
        .table("catalog.schema.table")
        .oauth("my-client-id", "my-secret")
        .json();

    let _provider = builder.resolve_headers_provider().unwrap();
}

#[cfg(feature = "arrow-flight")]
#[test]
fn arrow_builder() {
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};

    let sdk = test_sdk();
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        DataType::Int32,
        false,
    )]));
    let _builder = sdk
        .stream_builder()
        .table("t")
        .oauth("a", "b")
        .arrow(schema)
        .max_inflight_batches(500)
        .connection_timeout_ms(10_000);
}

#[cfg(feature = "arrow-flight")]
#[tokio::test]
async fn arrow_builder_rejects_ack_callback() {
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};

    let sdk = test_sdk();
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        DataType::Int32,
        false,
    )]));
    let result = sdk
        .stream_builder()
        .table("t")
        .oauth("a", "b")
        .arrow(schema)
        .ack_callback(Arc::new(NoopAckCallback))
        .build_arrow()
        .await;

    match result {
        Err(ZerobusError::InvalidArgument(msg)) => {
            assert!(msg.contains("ack_callback"));
            assert!(msg.contains("Arrow Flight"));
        }
        _ => panic!("expected InvalidArgument error"),
    }
}

#[cfg(feature = "arrow-flight")]
#[tokio::test]
async fn build_rejects_stats_exporter_on_non_arrow_stream() {
    let (exporter, _rx) = crate::stats::channel_exporter(std::num::NonZeroUsize::new(4).unwrap());
    let sdk = test_sdk();
    let builder = sdk
        .stream_builder()
        .table("t")
        .oauth("a", "b")
        .json()
        .stats_exporter(exporter);

    // validate() must reject the same misconfiguration build() does.
    match builder.validate() {
        Err(ZerobusError::InvalidArgument(msg)) => {
            assert!(msg.contains("stats_exporter"));
            assert!(msg.contains("Arrow"));
        }
        other => panic!("expected InvalidArgument from validate(), got {other:?}"),
    }

    match builder.build().await {
        Err(ZerobusError::InvalidArgument(msg)) => {
            assert!(msg.contains("stats_exporter"));
            assert!(msg.contains("Arrow"));
        }
        _ => panic!("expected InvalidArgument error"),
    }
}

#[cfg(feature = "arrow-flight")]
#[tokio::test]
async fn arrow_builder_reports_format_error_before_ack_callback_error() {
    let sdk = test_sdk();
    let result = sdk
        .stream_builder()
        .table("t")
        .oauth("a", "b")
        .json()
        .ack_callback(Arc::new(NoopAckCallback))
        .build_arrow()
        .await;

    match result {
        Err(ZerobusError::InvalidArgument(msg)) => {
            assert_eq!(
                msg,
                "non-Arrow format requires .build() instead of .build_arrow()"
            );
        }
        _ => panic!("expected non-Arrow format InvalidArgument error"),
    }
}

#[cfg(feature = "arrow-flight")]
#[test]
fn shared_setters_write_to_arrow_config() {
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};

    let sdk = test_sdk();
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        DataType::Int32,
        false,
    )]));
    let builder = sdk
        .stream_builder()
        .table("t")
        .oauth("a", "b")
        .arrow(schema)
        .recovery(false)
        .recovery_timeout_ms(5_000)
        .recovery_backoff_ms(500)
        .recovery_retries(2)
        .server_lack_of_ack_timeout_ms(10_000)
        .flush_timeout_ms(20_000)
        .stream_paused_max_wait_time_ms(Some(5_000));
    assert!(!builder.arrow_config.recovery);
    assert_eq!(builder.arrow_config.recovery_timeout_ms, 5_000);
    assert_eq!(builder.arrow_config.recovery_backoff_ms, 500);
    assert_eq!(builder.arrow_config.recovery_retries, 2);
    assert_eq!(builder.arrow_config.server_lack_of_ack_timeout_ms, 10_000);
    assert_eq!(builder.arrow_config.flush_timeout_ms, 20_000);
    assert_eq!(
        builder.arrow_config.stream_paused_max_wait_time_ms,
        Some(5_000)
    );
}
