#[allow(dead_code)]
mod mock_arrow_flight;
mod utils;

use std::sync::Arc;
use std::time::{Duration, Instant};

use databricks_zerobus_ingest_sdk::{NoTlsConfig, ZerobusSdk};
use tracing::info;
use zerobus_test_support::mock_grpc::{start_mock_server, MockResponse};

use crate::mock_arrow_flight::{start_mock_flight_server, MockFlightResponse};
use crate::utils::{
    create_test_arrow_schema, create_test_descriptor_proto, create_test_record_batch,
    setup_tracing, TestHeadersProvider,
};

const TABLE_NAME: &str = "test_catalog.test_schema.test_table";

async fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(Instant::now() < deadline, "condition not reached in time");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn ids_ten() -> Vec<i64> {
    (0..10).collect()
}

fn names_ten() -> Vec<Option<&'static str>> {
    vec![Some("x"); 10]
}

fn sdk_for(server_url: &str) -> Result<ZerobusSdk, Box<dyn std::error::Error>> {
    Ok(ZerobusSdk::builder()
        .endpoint(server_url.to_string())
        .unity_catalog_url("https://mock-uc.com")
        .tls_config(Arc::new(NoTlsConfig))
        .build()?)
}

mod grpc_stream_stats {
    use super::*;

    #[tokio::test]
    async fn in_flight_rises_on_ingest_and_falls_on_flush() -> Result<(), Box<dyn std::error::Error>>
    {
        setup_tracing();
        info!("Starting grpc in_flight_rises_on_ingest_and_falls_on_flush");

        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "stats_stream".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 2,
                        delay_ms: 500,
                    },
                ],
            )
            .await;

        let stream = sdk_for(&server_url)?
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .build()
            .await?;

        assert_eq!(stream.stats().records_in_flight, 0);
        for i in 0..3 {
            stream
                .ingest_record_offset(format!("record-{i}").into_bytes())
                .await?;
        }
        let pending = stream.stats();
        assert_eq!(pending.records_in_flight, 3);
        assert_eq!(pending.last_acked_offset, None);

        stream.flush().await?;
        let acked = stream.stats();
        assert_eq!(acked.records_in_flight, 0);
        assert_eq!(acked.last_acked_offset, Some(2));
        assert_eq!(acked.reconnects, 0);
        assert!(acked.last_error.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn batch_counts_each_record() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting grpc batch_counts_each_record");

        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "stats_batch_stream".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 500,
                    },
                ],
            )
            .await;

        let stream = sdk_for(&server_url)?
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .build()
            .await?;

        let records: Vec<Vec<u8>> = (0..4).map(|i| format!("row-{i}").into_bytes()).collect();
        let offset = stream.ingest_records_offset(records).await?;
        assert_eq!(offset, Some(0));
        assert_eq!(stream.stats().records_in_flight, 4);

        stream.flush().await?;
        assert_eq!(stream.stats().records_in_flight, 0);
        assert_eq!(stream.stats().last_acked_offset, Some(0));
        Ok(())
    }

    #[tokio::test]
    async fn forced_drop_counts_reconnect_and_records_last_error(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting grpc forced_drop_counts_reconnect_and_records_last_error");

        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "stats_drop_first".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                    },
                    MockResponse::Error {
                        status: tonic::Status::unavailable("connection dropped by test"),
                        delay_ms: 100,
                    },
                    MockResponse::CreateStream {
                        stream_id: "stats_drop_second".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let stream = sdk_for(&server_url)?
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(true)
            .recovery_timeout_ms(5000)
            .recovery_backoff_ms(100)
            .build()
            .await?;

        stream.ingest_record_offset(b"first".to_vec()).await?;
        let second = stream.ingest_record_offset(b"second".to_vec()).await?;
        stream.wait_for_offset(second).await?;

        let stats = stream.stats();
        assert_eq!(stats.reconnects, 1);
        assert_eq!(stats.records_in_flight, 0);
        assert_eq!(stats.last_acked_offset, Some(second));
        let error = stats.last_error.expect("drop must record the last error");
        assert!(error.message.contains("connection dropped by test"));
        assert!(error.is_retryable);
        Ok(())
    }

    #[tokio::test]
    async fn terminal_failure_records_last_error_without_reconnect(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting grpc terminal_failure_records_last_error_without_reconnect");

        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "stats_terminal".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::Error {
                        status: tonic::Status::unavailable("terminal failure by test"),
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let stream = sdk_for(&server_url)?
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        let offset = stream.ingest_record_offset(b"doomed".to_vec()).await?;
        assert!(stream.wait_for_offset(offset).await.is_err());

        let stats = stream.stats();
        assert_eq!(stats.reconnects, 0);
        assert_eq!(stats.last_acked_offset, None);
        let error = stats.last_error.expect("terminal failure must be recorded");
        assert!(error.message.contains("terminal failure by test"));
        Ok(())
    }
}

mod arrow_stream_stats {
    use super::*;

    #[tokio::test]
    async fn in_flight_rises_on_ingest_and_falls_on_full_ack(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting arrow in_flight_rises_on_ingest_and_falls_on_full_ack");

        let (mock_server, server_url) = start_mock_flight_server().await?;
        let schema = create_test_arrow_schema();
        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockFlightResponse::BatchAck {
                    ack_up_to_offset: 0,
                    delay_ms: 500,
                    ack_up_to_records: 10,
                }],
            )
            .await;

        let stream = sdk_for(&server_url)?
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .arrow(schema.clone())
            .recovery(false)
            .build_arrow()
            .await?;

        let batch = create_test_record_batch(schema.clone(), ids_ten(), names_ten());
        let offset = stream.ingest_batch(batch).await?;

        let pending = stream.stats();
        assert_eq!(pending.records_in_flight, 10);
        assert_eq!(pending.last_acked_offset, None);

        stream.wait_for_offset(offset).await?;
        let acked = stream.stats();
        assert_eq!(acked.records_in_flight, 0);
        assert_eq!(acked.last_acked_offset, Some(offset));
        Ok(())
    }

    #[tokio::test]
    async fn partial_batch_ack_counts_records_exactly() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting arrow partial_batch_ack_counts_records_exactly");

        let (mock_server, server_url) = start_mock_flight_server().await?;
        let schema = create_test_arrow_schema();
        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockFlightResponse::BatchAck {
                    ack_up_to_offset: 0,
                    delay_ms: 0,
                    ack_up_to_records: 4,
                }],
            )
            .await;

        let stream = sdk_for(&server_url)?
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .arrow(schema.clone())
            .recovery(false)
            .build_arrow()
            .await?;

        let batch = create_test_record_batch(schema.clone(), ids_ten(), names_ten());
        stream.ingest_batch(batch).await?;

        // Four of the ten records are acknowledged; the batch is not complete, so no
        // offset is reported as acknowledged.
        wait_until(|| stream.stats().records_in_flight == 6).await;
        assert_eq!(stream.stats().last_acked_offset, None);
        Ok(())
    }

    #[tokio::test]
    async fn forced_drop_counts_reconnect_and_records_last_error(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        info!("Starting arrow forced_drop_counts_reconnect_and_records_last_error");

        let (mock_server, server_url) = start_mock_flight_server().await?;
        let schema = create_test_arrow_schema();
        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    // First connection acks batch 0, then drops.
                    MockFlightResponse::BatchAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                        ack_up_to_records: 1,
                    },
                    MockFlightResponse::CloseStream { delay_ms: 0 },
                    // Reconnect replays only the unacknowledged batch. Wire offsets restart
                    // on a new connection, so the replayed batch is offset 0 here.
                    MockFlightResponse::BatchAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                        ack_up_to_records: 1,
                    },
                ],
            )
            .await;

        let stream = sdk_for(&server_url)?
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .arrow(schema.clone())
            .recovery(true)
            .recovery_timeout_ms(5000)
            .recovery_backoff_ms(100)
            .build_arrow()
            .await?;

        let first = create_test_record_batch(schema.clone(), vec![1], vec![Some("a")]);
        stream.ingest_batch(first).await?;
        let second = create_test_record_batch(schema.clone(), vec![2], vec![Some("b")]);
        let second_offset = stream.ingest_batch(second).await?;
        stream.wait_for_offset(second_offset).await?;

        let stats = stream.stats();
        assert_eq!(stats.reconnects, 1);
        assert_eq!(stats.records_in_flight, 0);
        assert_eq!(stats.last_acked_offset, Some(second_offset));
        let error = stats.last_error.expect("drop must record the last error");
        assert!(error.is_retryable);
        Ok(())
    }
}
