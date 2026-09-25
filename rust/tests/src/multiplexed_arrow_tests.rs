use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use databricks_zerobus_ingest_sdk::{
    channel_exporter, MessageId, NoTlsConfig, StreamStat, ZerobusError, ZerobusSdk,
};
use futures::poll;
use tonic::Status;

use crate::mock_arrow_flight::{start_mock_flight_server, MockFlightResponse};
use crate::utils::{
    create_test_arrow_schema, create_test_record_batch, record_batch_to_ipc_bytes,
    run_with_paused_time_watchdog, TestHeadersProvider,
};

const TABLE: &str = "catalog.schema.arrow_mux";

fn sdk(endpoint: String) -> ZerobusSdk {
    ZerobusSdk::builder()
        .endpoint(endpoint)
        .unity_catalog_url("https://mock-uc.com")
        .tls_config(Arc::new(NoTlsConfig))
        .build()
        .unwrap()
}

fn batch(ids: &[i64]) -> RecordBatch {
    create_test_record_batch(
        create_test_arrow_schema(),
        ids.to_vec(),
        vec![Some("row"); ids.len()],
    )
}

fn ids(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

fn builder(sdk: &ZerobusSdk) -> databricks_zerobus_ingest_sdk::StreamBuilder<'_> {
    sdk.stream_builder()
        .table(TABLE)
        .headers_provider(Arc::new(TestHeadersProvider::default()))
        .arrow(create_test_arrow_schema())
        .flush_timeout_ms(2_000)
}

#[tokio::test]
async fn round_robin_ipc_message_ids_and_shared_telemetry() {
    let (server, endpoint) = start_mock_flight_server().await.unwrap();
    let sdk = sdk(endpoint);
    let (exporter, mut events) = channel_exporter(std::num::NonZeroUsize::new(100).unwrap());
    let mut mux = builder(&sdk)
        .stats_exporter(exporter)
        .multiplexed(2)
        .build_arrow()
        .await
        .unwrap();
    let mut messages = Vec::new();
    for i in 0..6 {
        let data = batch(&[i * 2, i * 2 + 1]);
        let id = if i % 2 == 0 {
            mux.ingest_batch(data).await.unwrap()
        } else {
            mux.ingest_ipc_batch(record_batch_to_ipc_bytes(&data))
                .await
                .unwrap()
        };
        assert_eq!(id.stream_index(), i as usize % 2);
        assert_eq!(id.sub_offset(), i / 2);
        assert_eq!(MessageId::from_raw(id.raw()), id);
        messages.push(id);
    }
    mux.flush().await.unwrap();
    // Verify handles after the pipelined ingestion and flush have completed.
    for id in messages {
        mux.wait_for_message_id(id).await.unwrap();
    }
    let invalid_lane = MessageId::from_raw((63_u64 << 58) as i64);
    assert!(matches!(
        mux.wait_for_message_id(invalid_lane).await,
        Err(ZerobusError::InvalidArgument(_))
    ));
    mux.close().await.unwrap();
    mux.close().await.unwrap();
    assert!(mux.is_closed());
    assert!(mux.ingest_batch(batch(&[99])).await.is_err());
    assert!(mux.get_unacked_batches().await.unwrap().is_empty());
    assert_eq!(server.connection_count(), 2);
    assert_eq!(server.get_total_records_received().await, 12);
    let mut ack_offsets = Vec::new();
    while let Ok(event) = events.try_recv() {
        if let StreamStat::BatchAcked { offset } = event {
            ack_offsets.push(offset);
        }
    }
    ack_offsets.sort();
    assert_eq!(ack_offsets, [0, 0, 1, 1, 2, 2]);
}

#[tokio::test]
async fn invalid_batches_do_not_poison_or_consume_lane_offsets() {
    let (_, endpoint) = start_mock_flight_server().await.unwrap();
    let sdk = sdk(endpoint);
    let mut mux = builder(&sdk).multiplexed(2).build_arrow().await.unwrap();
    assert!(matches!(
        mux.ingest_batch(batch(&[])).await,
        Err(ZerobusError::InvalidArgument(_))
    ));
    assert!(matches!(
        mux.ingest_ipc_batch(vec![1, 2, 3].into()).await,
        Err(ZerobusError::InvalidArgument(_))
    ));
    let wrong = RecordBatch::try_from_iter(vec![(
        "different",
        Arc::new(Int64Array::from(vec![1])) as arrow_array::ArrayRef,
    )])
    .unwrap();
    assert!(matches!(
        mux.ingest_batch(wrong).await,
        Err(ZerobusError::InvalidArgument(_))
    ));
    for _ in 0..2 {
        assert_eq!(mux.ingest_batch(batch(&[1])).await.unwrap().sub_offset(), 0);
    }
    assert!(!mux.is_closed());
    mux.flush().await.unwrap();
    mux.close().await.unwrap();
}

#[tokio::test]
async fn standalone_reserved_ingest_rejects_before_close_finalization() {
    let (_, endpoint) = start_mock_flight_server().await.unwrap();
    let sdk = sdk(endpoint);
    let stream = Arc::new(builder(&sdk).build_arrow().await.unwrap());
    let (ingest_reached, release_ingest) = stream.arm_ingest_admission_barrier().await;
    let (close_reached, release_close) = stream.arm_close_finalize_barrier().await;
    let ingest = {
        let stream = stream.clone();
        tokio::spawn(async move { stream.ingest_batch(batch(&[1])).await })
    };
    ingest_reached.notified().await;
    let close = {
        let stream = stream.clone();
        tokio::spawn(async move { stream.close_concurrently_for_test().await })
    };
    close_reached.notified().await;
    release_ingest.notify_one();
    let error = crate::utils::run_with_paused_time_watchdog(ingest)
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("closing or closed"), "{error:?}");
    assert!(
        !close.is_finished(),
        "ingest must not wait for the parked finalizer"
    );
    release_close.notify_one();
    close.await.unwrap().unwrap();
    assert!(stream.get_unacked_batches().await.unwrap().is_empty());
}

#[tokio::test]
async fn standalone_capacity_wait_rejects_when_close_starts() {
    let (server, endpoint) = start_mock_flight_server().await.unwrap();
    server
        .inject_responses(
            TABLE,
            vec![crate::mock_arrow_flight::MockFlightResponse::HoldResponseAfterRequestEof],
        )
        .await;
    let sdk = sdk(endpoint);
    let stream = builder(&sdk)
        .max_inflight_batches(1)
        .build_arrow()
        .await
        .unwrap();
    tokio::time::pause();
    stream.ingest_batch(batch(&[1])).await.unwrap();
    let ingest = stream.ingest_batch(batch(&[2]));
    let close = stream.close_concurrently_for_test();
    tokio::pin!(ingest, close);
    assert!(futures::poll!(&mut ingest).is_pending());
    assert!(futures::poll!(&mut close).is_pending());
    match futures::poll!(&mut ingest) {
        std::task::Poll::Ready(Err(error)) => {
            assert!(error.to_string().contains("closing or closed"), "{error:?}")
        }
        other => panic!("capacity waiter must reject without awaiting finalization: {other:?}"),
    }
    assert!(close.await.is_err());
    let retained = stream.get_unacked_batches().await.unwrap();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].num_rows(), 1);
}

#[tokio::test]
async fn terminal_poison_preserves_healthy_wait_and_flush_and_partial_suffixes() {
    let (server, endpoint) = start_mock_flight_server().await.unwrap();
    server
        .inject_connection_scripts(
            TABLE,
            vec![
                vec![
                    MockFlightResponse::BatchAck {
                        ack_up_to_offset: 0,
                        ack_up_to_records: 1,
                        delay_ms: 0,
                    },
                    MockFlightResponse::Error {
                        status: Status::permission_denied("mux terminal"),
                        delay_ms: 0,
                    },
                ],
                vec![],
            ],
        )
        .await;
    let sdk = sdk(endpoint);
    let mut mux = builder(&sdk)
        .recovery(false)
        .multiplexed(2)
        .build_arrow()
        .await
        .unwrap();
    let mut messages = Vec::new();
    for data in [
        batch(&[1, 2, 3]),
        batch(&[11, 12, 13]),
        batch(&[4]),
        batch(&[14]),
    ] {
        messages.push(mux.ingest_batch(data).await.unwrap());
    }
    let result = mux.flush().await.unwrap_err();
    assert!(result.to_string().contains("mux terminal"), "{result:?}");
    assert!(mux.is_closed());
    let mut healthy = 0;
    for id in messages {
        if mux.wait_for_message_id(id).await.is_ok() {
            healthy += 1;
        }
    }
    assert_eq!(
        healthy, 2,
        "the healthy lane's messages remain acknowledged"
    );
    let ingest_error = mux.ingest_batch(batch(&[99])).await.unwrap_err();
    assert_eq!(ingest_error.to_string(), result.to_string());
    assert_eq!(
        mux.close().await.unwrap_err().to_string(),
        result.to_string()
    );
    let failed = mux.get_unacked_batches().await.unwrap();
    let rows: Vec<_> = failed.iter().map(ids).collect();
    assert!(
        rows == [vec![2, 3], vec![4]] || rows == [vec![12, 13], vec![14]],
        "{rows:?}"
    );
    assert_eq!(
        mux.get_unacked_batches()
            .await
            .unwrap()
            .iter()
            .map(ids)
            .collect::<Vec<_>>(),
        rows
    );
}

#[tokio::test]
async fn retryable_lane_failure_replays_without_poisoning_mux() {
    let (server, endpoint) = start_mock_flight_server().await.unwrap();
    server
        .inject_connection_scripts(
            TABLE,
            vec![
                vec![
                    MockFlightResponse::BatchAck {
                        ack_up_to_offset: 0,
                        ack_up_to_records: 1,
                        delay_ms: 0,
                    },
                    MockFlightResponse::Error {
                        status: Status::unavailable("retry me"),
                        delay_ms: 0,
                    },
                ],
                vec![],
            ],
        )
        .await;
    let sdk = sdk(endpoint);
    let mut mux = builder(&sdk)
        .recovery_backoff_ms(1)
        .multiplexed(2)
        .build_arrow()
        .await
        .unwrap();
    for data in [
        batch(&[1, 2, 3]),
        batch(&[11, 12, 13]),
        batch(&[4]),
        batch(&[14]),
    ] {
        mux.ingest_batch(data).await.unwrap();
    }
    mux.flush().await.unwrap();
    assert!(!mux.is_closed());
    assert_eq!(server.connection_count(), 3);
    // Eight original rows plus only the failed lane's three unacknowledged rows.
    assert_eq!(server.get_total_records_received().await, 11);
    mux.close().await.unwrap();
    assert!(mux.get_unacked_batches().await.unwrap().is_empty());
}

#[tokio::test]
async fn capacity_is_mux_wide_waits_on_selected_lane_and_releases_on_cancellation() {
    let (server, endpoint) = start_mock_flight_server().await.unwrap();
    server
        .inject_responses(
            TABLE,
            vec![MockFlightResponse::BatchAck {
                ack_up_to_offset: 0,
                ack_up_to_records: 1,
                delay_ms: 10_000,
            }],
        )
        .await;
    let sdk = sdk(endpoint);
    // floor(3 / 2) = one pending batch per lane; one budget slot stays unused.
    let mut mux = builder(&sdk)
        .max_inflight_batches(3)
        .multiplexed(2)
        .build_arrow()
        .await
        .unwrap();
    tokio::time::pause();
    run_with_paused_time_watchdog(async {
        mux.ingest_batch(batch(&[0])).await.unwrap();
        server.delayed_ack_armed().notified().await;
        mux.ingest_batch(batch(&[1])).await.unwrap();
        server.delayed_ack_armed().notified().await;
        // The third batch cannot be queued despite its encoder channel draining.
        {
            let blocked = mux.ingest_batch(batch(&[2]));
            tokio::pin!(blocked);
            assert!(poll!(&mut blocked).is_pending());
        }
        let next = mux.ingest_batch(batch(&[3]));
        tokio::pin!(next);
        assert!(poll!(&mut next).is_pending());
        tokio::time::advance(Duration::from_millis(10_001)).await;
        let id = next.await.unwrap();
        assert_eq!((id.stream_index(), id.sub_offset()), (1, 1));
        mux.flush().await.unwrap();
    })
    .await;
    mux.close().await.unwrap();
    assert_eq!(
        server.get_total_records_received().await,
        3,
        "cancelled input must never be retained"
    );
}

#[tokio::test]
async fn capacity_timeout_is_nonterminal_and_does_not_admit_input() {
    let (server, endpoint) = start_mock_flight_server().await.unwrap();
    server
        .inject_responses(
            TABLE,
            vec![MockFlightResponse::BatchAck {
                ack_up_to_offset: 0,
                ack_up_to_records: 1,
                delay_ms: 40_000,
            }],
        )
        .await;
    let sdk = sdk(endpoint);
    let mut mux = builder(&sdk)
        .max_inflight_batches(2)
        .server_lack_of_ack_timeout_ms(120_000)
        .multiplexed(2)
        .build_arrow()
        .await
        .unwrap();
    tokio::time::pause();
    run_with_paused_time_watchdog(async {
        mux.ingest_batch(batch(&[0])).await.unwrap();
        server.delayed_ack_armed().notified().await;
        mux.ingest_batch(batch(&[1])).await.unwrap();
        server.delayed_ack_armed().notified().await;
        let blocked = mux.ingest_batch(batch(&[2]));
        tokio::pin!(blocked);
        assert!(poll!(&mut blocked).is_pending());
        tokio::time::advance(Duration::from_millis(30_001)).await;
        assert!(matches!(
            blocked.await,
            Err(ZerobusError::ConnectionTimeout(_))
        ));
        assert!(!mux.is_closed());
        tokio::time::advance(Duration::from_millis(10_001)).await;
        mux.flush().await.unwrap();
    })
    .await;
    mux.close().await.unwrap();
    assert_eq!(server.get_total_records_received().await, 2);
}

#[tokio::test]
async fn cancelled_close_keeps_original_deadline_and_unacked_batches() {
    let (server, endpoint) = start_mock_flight_server().await.unwrap();
    server
        .inject_responses(TABLE, vec![MockFlightResponse::HoldResponseAfterRequestEof])
        .await;
    let sdk = sdk(endpoint);
    let mut mux = builder(&sdk).multiplexed(2).build_arrow().await.unwrap();
    mux.ingest_batch(batch(&[0])).await.unwrap();
    mux.ingest_batch(batch(&[1])).await.unwrap();
    tokio::time::pause();
    {
        let close = mux.close();
        tokio::pin!(close);
        assert!(poll!(&mut close).is_pending());
    }
    tokio::time::advance(Duration::from_millis(2_001)).await;
    let result = mux.close().await.unwrap_err();
    assert!(result.to_string().contains("timed out"), "{result:?}");
    let failed = mux.get_unacked_batches().await.unwrap();
    assert_eq!(
        failed.iter().map(ids).collect::<Vec<_>>(),
        [vec![0], vec![1]]
    );
    assert_eq!(
        mux.close().await.unwrap_err().to_string(),
        result.to_string()
    );
}

#[tokio::test]
async fn failed_construction_closes_successfully_opened_arrow_lanes() {
    let (server, endpoint) = start_mock_flight_server().await.unwrap();
    server
        .inject_connection_scripts(
            TABLE,
            vec![
                vec![],
                vec![MockFlightResponse::FailSetup {
                    status: Status::permission_denied("construction failure"),
                }],
            ],
        )
        .await;
    let sdk = sdk(endpoint);
    let error = builder(&sdk)
        .recovery(false)
        .multiplexed(2)
        .build_arrow()
        .await
        .err()
        .unwrap();
    assert!(
        error.to_string().contains("construction failure"),
        "{error:?}"
    );
    assert_eq!(server.connection_count(), 2);
    assert_eq!(server.get_request_half_close_count(), 1);
}

#[tokio::test]
async fn poison_wakes_capacity_waiters_on_both_failed_and_healthy_lanes() {
    let (server, endpoint) = start_mock_flight_server().await.unwrap();
    server
        .inject_connection_scripts(
            TABLE,
            vec![
                vec![MockFlightResponse::Error {
                    status: Status::permission_denied("poison waiters"),
                    delay_ms: 0,
                }],
                vec![MockFlightResponse::BatchAck {
                    ack_up_to_offset: 0,
                    ack_up_to_records: 1,
                    delay_ms: 10_000,
                }],
            ],
        )
        .await;
    let sdk = sdk(endpoint);
    let mut mux = builder(&sdk)
        .recovery(false)
        .max_inflight_batches(2)
        .multiplexed(2)
        .build_arrow()
        .await
        .unwrap();
    tokio::time::pause();
    run_with_paused_time_watchdog(async {
        let a = mux.ingest_batch(batch(&[0])).await.unwrap();
        let b = mux.ingest_batch(batch(&[1])).await.unwrap();
        let blocked_a = mux.ingest_batch(batch(&[2]));
        let blocked_b = mux.ingest_batch(batch(&[3]));
        tokio::pin!(blocked_a, blocked_b);
        assert!(poll!(&mut blocked_a).is_pending());
        assert!(poll!(&mut blocked_b).is_pending());
        let failure = futures::future::select(
            Box::pin(mux.wait_for_message_id(a)),
            Box::pin(mux.wait_for_message_id(b)),
        )
        .await;
        let error = match failure {
            futures::future::Either::Left((result, _))
            | futures::future::Either::Right((result, _)) => result.unwrap_err(),
        };
        assert!(error.to_string().contains("poison waiters"));
        for result in [blocked_a.await, blocked_b.await] {
            assert_eq!(result.unwrap_err().to_string(), error.to_string());
        }
        server.delayed_ack_armed().notified().await;
        tokio::time::advance(Duration::from_millis(10_001)).await;
        assert_eq!(
            mux.flush().await.unwrap_err().to_string(),
            error.to_string()
        );
    })
    .await;
    assert!(mux.close().await.is_err());
    assert_eq!(server.get_total_records_received().await, 2);
    assert_eq!(mux.get_unacked_batches().await.unwrap().len(), 1);
}

struct GatedHeaders {
    calls: std::sync::atomic::AtomicUsize,
    allowed: usize,
    reached: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl databricks_zerobus_ingest_sdk::HeadersProvider for GatedHeaders {
    async fn get_headers(
        &self,
    ) -> databricks_zerobus_ingest_sdk::ZerobusResult<std::collections::HashMap<&'static str, String>>
    {
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) >= self.allowed {
            self.reached.notify_one();
            std::future::pending::<()>().await;
        }
        Ok(std::collections::HashMap::new())
    }
}

#[tokio::test]
async fn cancelled_construction_releases_open_lane_and_pending_headers() {
    let (server, endpoint) = start_mock_flight_server().await.unwrap();
    let sdk = sdk(endpoint);
    let provider = Arc::new(GatedHeaders {
        calls: std::sync::atomic::AtomicUsize::new(0),
        allowed: 1,
        reached: tokio::sync::Notify::new(),
    });
    let build = {
        let provider = provider.clone();
        tokio::spawn(async move {
            builder(&sdk)
                .headers_provider(provider)
                .multiplexed(2)
                .build_arrow()
                .await
        })
    };
    provider.reached.notified().await;
    run_with_paused_time_watchdog(async {
        while server.connection_count() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    build.abort();
    assert!(matches!(build.await, Err(e) if e.is_cancelled()));
    run_with_paused_time_watchdog(async {
        while Arc::strong_count(&provider) != 1 {
            tokio::task::yield_now().await;
        }
        while server.get_request_half_close_count() + server.get_request_reset_count() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
}

#[tokio::test]
async fn close_interrupts_lane_recovery_and_retains_its_trigger_and_batches() {
    let (server, endpoint) = start_mock_flight_server().await.unwrap();
    server
        .inject_connection_scripts(
            TABLE,
            vec![
                vec![MockFlightResponse::Error {
                    status: Status::unavailable("interrupted recovery"),
                    delay_ms: 0,
                }],
                vec![],
            ],
        )
        .await;
    let sdk = sdk(endpoint);
    let provider = Arc::new(GatedHeaders {
        calls: std::sync::atomic::AtomicUsize::new(0),
        allowed: 2,
        reached: tokio::sync::Notify::new(),
    });
    let mut mux = builder(&sdk)
        .headers_provider(provider.clone())
        .recovery_backoff_ms(1)
        .multiplexed(2)
        .build_arrow()
        .await
        .unwrap();
    mux.ingest_batch(batch(&[0])).await.unwrap();
    mux.ingest_batch(batch(&[1])).await.unwrap();
    provider.reached.notified().await;
    let result = run_with_paused_time_watchdog(mux.close())
        .await
        .unwrap_err();
    assert!(
        result.to_string().contains("interrupted recovery"),
        "{result:?}"
    );
    let failed = mux.get_unacked_batches().await.unwrap();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].num_rows(), 1);
}
