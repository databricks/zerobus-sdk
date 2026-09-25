use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use databricks_zerobus_ingest_sdk::{
    channel_exporter, MessageId, NoTlsConfig, StreamStat, ZerobusError, ZerobusSdk,
};

use crate::mock_arrow_flight::start_mock_flight_server;
use crate::utils::{
    create_test_arrow_schema, create_test_record_batch, record_batch_to_ipc_bytes,
    TestHeadersProvider,
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
