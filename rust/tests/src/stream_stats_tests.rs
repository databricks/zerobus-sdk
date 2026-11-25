#[allow(dead_code)]
mod mock_arrow_flight;
mod utils;

use std::sync::Arc;
use std::time::{Duration, Instant};

use databricks_zerobus_ingest_sdk::{NoTlsConfig, ZerobusError, ZerobusSdk};
use tracing::info;
use zerobus_test_support::mock_grpc::{start_mock_server, MockResponse, MockResponseGate};

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn immediate_acks_on_multi_thread_leave_no_phantom_in_flight(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        const RECORDS: i64 = 2000;

        let (mock_server, server_url) = start_mock_server().await?;
        let mut responses = vec![MockResponse::CreateStream {
            stream_id: "stats_mt_stream".to_string(),
            delay_ms: 0,
        }];
        responses.extend((0..RECORDS).map(|i| MockResponse::RecordAck {
            ack_up_to_offset: i,
            delay_ms: 0,
        }));
        mock_server.inject_responses(TABLE_NAME, responses).await;

        let stream = sdk_for(&server_url)?
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(1000)
            .build()
            .await?;

        let mut last = -1;
        for i in 0..RECORDS {
            last = stream
                .ingest_record_offset(format!("row-{i}").into_bytes())
                .await?;
        }
        stream.flush().await?;

        let stats = stream.stats();
        assert_eq!(stats.records_in_flight, 0);
        assert_eq!(stats.last_acked_offset, Some(last));
        Ok(())
    }

    #[tokio::test]
    async fn record_is_counted_before_it_reaches_the_sender(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "stats_admission".to_string(),
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
            .build()
            .await?;

        let (reached, proceed) = stream.arm_pre_enqueue_barrier().await;
        let ingest = stream.ingest_record_offset(b"admitted".to_vec());
        let observe = async {
            reached.notified().await;
            let in_flight = stream.stats().records_in_flight;
            proceed.notify_one();
            in_flight
        };
        let (offset, in_flight) = tokio::join!(ingest, observe);
        assert_eq!(
            in_flight, 1,
            "the record must be counted before the sender can see it"
        );
        stream.wait_for_offset(offset?).await?;
        assert_eq!(stream.stats().records_in_flight, 0);
        Ok(())
    }

    #[tokio::test]
    async fn clean_close_does_not_record_last_error() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "stats_clean_close".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                    },
                ],
            )
            .await;

        let mut stream = sdk_for(&server_url)?
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .compiled_proto(create_test_descriptor_proto().unwrap_or_default())
            .max_inflight_requests(100)
            .recovery(false)
            .build()
            .await?;

        let reader = stream.stats_reader();
        let offset = stream.ingest_record_offset(b"only".to_vec()).await?;
        stream.wait_for_offset(offset).await?;
        stream.close().await?;

        let stats = stream.stats();
        assert_eq!(stats.records_in_flight, 0);
        assert!(
            stats.last_error.is_none(),
            "clean close recorded {:?}",
            stats.last_error
        );
        // The reader outlives the closed stream and reports its last values.
        assert_eq!(reader.stats().last_acked_offset, Some(offset));
        assert!(reader.stats().last_error.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn graceful_rotation_then_server_eof_is_not_recorded(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "stats_rotation_first".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                    },
                    // The server announces a rotation and ends the stream straight after it.
                    MockResponse::CloseStreamSignalAndEnd {
                        duration_seconds: 5,
                        delay_ms: 0,
                    },
                    // Wire offsets restart on the new connection, so the replayed record is 0.
                    MockResponse::CreateStream {
                        stream_id: "stats_rotation_second".to_string(),
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

        let first = stream.ingest_record_offset(b"first".to_vec()).await?;
        stream.wait_for_offset(first).await?;
        let second = stream.ingest_record_offset(b"second".to_vec()).await?;
        stream.wait_for_offset(second).await?;

        let stats = stream.stats();
        assert_eq!(stats.reconnects, 1);
        assert_eq!(stats.records_in_flight, 0);
        assert_eq!(stats.last_acked_offset, Some(second));
        assert!(
            stats.last_error.is_none(),
            "graceful rotation recorded {:?}",
            stats.last_error
        );
        Ok(())
    }

    #[tokio::test]
    async fn unrequested_server_close_with_recovery_is_recorded(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "stats_eos_first".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                    },
                    // The server ends the response stream with OK status.
                    MockResponse::EndStream { delay_ms: 0 },
                    // Wire offsets restart on the new connection, so the replayed record is 0.
                    MockResponse::CreateStream {
                        stream_id: "stats_eos_second".to_string(),
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

        let first = stream.ingest_record_offset(b"first".to_vec()).await?;
        stream.wait_for_offset(first).await?;
        let second = stream.ingest_record_offset(b"second".to_vec()).await?;
        stream.wait_for_offset(second).await?;

        let stats = stream.stats();
        assert_eq!(
            stats.reconnects, 1,
            "the unrequested close must trigger a recovery"
        );
        assert_eq!(stats.records_in_flight, 0);
        assert_eq!(stats.last_acked_offset, Some(second));
        let error = stats
            .last_error
            .expect("unrequested server close must be recorded");
        assert!(error.message.contains("closed by server without errors"));
        Ok(())
    }

    #[tokio::test]
    async fn unrequested_server_close_without_recovery_is_recorded(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        let (mock_server, server_url) = start_mock_server().await?;
        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockResponse::CreateStream {
                        stream_id: "stats_eos_terminal".to_string(),
                        delay_ms: 0,
                    },
                    MockResponse::RecordAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                    },
                    MockResponse::EndStream { delay_ms: 0 },
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

        let first = stream.ingest_record_offset(b"first".to_vec()).await?;
        stream.wait_for_offset(first).await?;
        let second = stream.ingest_record_offset(b"second".to_vec()).await?;
        assert!(stream.wait_for_offset(second).await.is_err());

        let stats = stream.stats();
        assert_eq!(stats.reconnects, 0);
        assert_eq!(stats.last_acked_offset, Some(first));
        // The unacknowledged record is retained after the terminal close.
        assert_eq!(stats.records_in_flight, 1);
        let error = stats
            .last_error
            .expect("unrequested server close must be recorded");
        assert!(error.message.contains("closed by server without errors"));
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
        // The failed record is retained, so it stays counted.
        assert_eq!(stats.records_in_flight, 1);
        let error = stats.last_error.expect("terminal failure must be recorded");
        assert!(error.message.contains("terminal failure by test"));
        assert!(matches!(error.error, ZerobusError::StreamClosedError(_)));

        assert!(stream
            .ingest_record_offset(b"rejected".to_vec())
            .await
            .is_err());
        assert_eq!(stream.stats().records_in_flight, 1);
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

    // A graceful server rotation is routine: it counts as a reconnect and is not an error.
    #[tokio::test]
    async fn server_rotation_counts_reconnect_without_last_error(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        let (mock_server, server_url) = start_mock_flight_server().await?;
        let schema = create_test_arrow_schema();
        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    MockFlightResponse::GracefulClose {
                        duration_ms: 100,
                        delay_ms: 0,
                        ack_up_to_offset: None,
                        ack_up_to_records: None,
                    },
                    // The rotated connection acknowledges the replayed batch.
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

        let batch = create_test_record_batch(schema.clone(), vec![1], vec![Some("a")]);
        let offset = stream.ingest_batch(batch).await?;
        stream.wait_for_offset(offset).await?;

        let stats = stream.stats();
        assert_eq!(stats.reconnects, 1);
        assert!(
            stats.last_error.is_none(),
            "rotation recorded {:?}",
            stats.last_error
        );
        assert_eq!(stats.records_in_flight, 0);
        Ok(())
    }

    // The close signal carries a partial ack, so the rotation is waiting for the second row
    // when the close is requested. The interrupted rotation is routine and must not be recorded.
    #[tokio::test]
    async fn close_during_rotation_does_not_record_last_error(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        let (mock_server, server_url) = start_mock_flight_server().await?;
        let schema = create_test_arrow_schema();
        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockFlightResponse::GracefulClose {
                    duration_ms: 5000,
                    delay_ms: 0,
                    ack_up_to_offset: Some(0),
                    ack_up_to_records: Some(1),
                }],
            )
            .await;

        let mut stream = sdk_for(&server_url)?
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .arrow(schema.clone())
            .recovery(true)
            .recovery_timeout_ms(5000)
            .recovery_backoff_ms(100)
            .build_arrow()
            .await?;

        let applied = stream.arm_ack_applied_notify().await;
        stream
            .ingest_batch(create_test_record_batch(
                schema.clone(),
                vec![1, 2],
                vec![Some("a"), Some("b")],
            ))
            .await?;
        tokio::time::timeout(Duration::from_secs(10), applied.notified())
            .await
            .expect("close signal ack must be applied");

        let _ = stream.close().await;
        let stats = stream.stats();
        assert_eq!(stats.records_in_flight, 1);
        assert!(
            stats.last_error.is_none(),
            "interrupted rotation recorded {:?}",
            stats.last_error
        );
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

    #[tokio::test]
    async fn partial_ack_survives_replay() -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        let (mock_server, server_url) = start_mock_flight_server().await?;
        let schema = create_test_arrow_schema();
        let replay_gate = Arc::new(MockResponseGate::new());
        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![
                    // First connection acknowledges 4 of the 10 rows of batch 0, then drops
                    // when batch 1 arrives.
                    MockFlightResponse::BatchAck {
                        ack_up_to_offset: 0,
                        delay_ms: 0,
                        ack_up_to_records: 4,
                    },
                    MockFlightResponse::CloseStream { delay_ms: 0 },
                    // Replay sends the 6-row suffix of batch 0 as wire offset 0, then batch 1.
                    // The suffix ack is held until the test releases the gate.
                    MockFlightResponse::GatedBatchAck {
                        ack_up_to_offset: 0,
                        ack_up_to_records: 6,
                        gate: Arc::clone(&replay_gate),
                    },
                    MockFlightResponse::BatchAck {
                        ack_up_to_offset: 1,
                        delay_ms: 0,
                        ack_up_to_records: 7,
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

        stream
            .ingest_batch(create_test_record_batch(
                schema.clone(),
                ids_ten(),
                names_ten(),
            ))
            .await?;
        wait_until(|| stream.stats().records_in_flight == 6).await;

        let second = stream
            .ingest_batch(create_test_record_batch(
                schema.clone(),
                vec![11],
                vec![Some("y")],
            ))
            .await?;
        wait_until(|| stream.stats().reconnects == 1).await;
        assert_eq!(
            stream.stats().records_in_flight,
            7,
            "the 6-row suffix and the new batch are in flight after replay"
        );

        replay_gate.release();
        stream.wait_for_offset(second).await?;
        let stats = stream.stats();
        assert_eq!(stats.records_in_flight, 0);
        assert_eq!(stats.last_acked_offset, Some(second));
        assert!(stats.last_error.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn terminal_failure_keeps_rows_in_flight_and_records_last_error(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        let (mock_server, server_url) = start_mock_flight_server().await?;
        let schema = create_test_arrow_schema();
        mock_server
            .inject_responses(
                TABLE_NAME,
                vec![MockFlightResponse::Error {
                    status: tonic::Status::unavailable("terminal failure by test"),
                    delay_ms: 0,
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

        let offset = stream
            .ingest_batch(create_test_record_batch(
                schema.clone(),
                ids_ten(),
                names_ten(),
            ))
            .await?;
        assert!(stream.wait_for_offset(offset).await.is_err());

        let stats = stream.stats();
        assert_eq!(stats.records_in_flight, 10);
        assert_eq!(stats.last_acked_offset, None);
        let error = stats.last_error.expect("terminal failure must be recorded");
        assert!(error.message.contains("terminal failure by test"));
        Ok(())
    }

    #[tokio::test]
    async fn withdrawn_send_failure_leaves_no_rows_in_flight(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        let (_mock_server, server_url) = start_mock_flight_server().await?;
        let schema = create_test_arrow_schema();

        let stream = sdk_for(&server_url)?
            .stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .arrow(schema.clone())
            .recovery(false)
            .build_arrow()
            .await?;

        stream.replace_batch_sender_with_closed_channel().await;
        let result = stream
            .ingest_batch(create_test_record_batch(
                schema.clone(),
                ids_ten(),
                names_ten(),
            ))
            .await;
        assert!(result.is_err());
        let stats = stream.stats();
        assert_eq!(stats.records_in_flight, 0);
        assert_eq!(stats.last_acked_offset, None);
        Ok(())
    }
}

mod persistent_stream_stats {
    use super::*;

    #[tokio::test]
    async fn reconciled_resume_clears_in_flight_and_advances_offset(
    ) -> Result<(), Box<dyn std::error::Error>> {
        setup_tracing();
        let (mock_server, server_url) = start_mock_server().await?;
        let mut stream = sdk_for(&server_url)?
            .persistent_stream_builder()
            .table(TABLE_NAME)
            .headers_provider(Arc::new(TestHeadersProvider::default()))
            .json()
            .recovery(true)
            .recovery_backoff_ms(0)
            .recovery_timeout_ms(1_000)
            .recovery_retries(2)
            .flush_timeout_ms(5_000)
            .build()
            .await?;
        let reader = stream.stats_reader();
        mock_server.fail_next_persistent_ack_after_commit();

        let records: Vec<String> = (0..4).map(|i| format!(r#"{{"id":{i}}}"#)).collect();
        let offset = stream
            .ingest_records_offset(records)
            .await?
            .expect("batch has an offset");
        stream.wait_for_offset(offset).await?;

        let stats = stream.stats();
        assert_eq!(stats.records_in_flight, 0);
        assert_eq!(stats.last_acked_offset, Some(offset));
        assert_eq!(stats.reconnects, 1);
        assert!(stats.last_error.is_some(), "the lost ACK drop must be kept");
        let reader_stats = reader.stats();
        assert_eq!(reader_stats.records_in_flight, 0);
        assert_eq!(reader_stats.last_acked_offset, Some(offset));
        stream.close().await?;
        Ok(())
    }
}
