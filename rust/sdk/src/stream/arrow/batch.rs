//! Pending Arrow batch and IPC materialization mechanics.
//!
//! Pending ranges are connection-relative and retain their backpressure permit
//! until acknowledgment, replay elimination, or terminal finalization.

use std::io::Cursor;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use arrow_ipc::{reader::StreamReader, writer::IpcWriteOptions, CompressionType, MetadataVersion};
use bytes::Bytes;
use tokio::sync::OwnedSemaphorePermit;
use tokio::time::{Duration, Instant};
use tracing::debug;

use super::{
    configured_deadline, ArrowGrpcCompression, ArrowStreamConfigurationOptions, OutboundBatch,
    RecordBatch,
};
use crate::errors::ZerobusError;
use crate::offset_generator::OffsetId;
use crate::ZerobusResult;

/// A pending batch waiting for acknowledgment.
pub(super) struct PendingBatch {
    batch: RecordBatch,
    /// Offset ID assigned by the client for this batch.
    offset_id: OffsetId,
    /// Cumulative record count before this batch.
    start_record: u64,
    /// Cumulative record count after this batch.
    /// Batch is fully acked when `acked_records >= end_record`.
    end_record: u64,
    /// Time this batch most recently became pending on the active connection.
    /// Only replay onto a replacement connection refreshes this timestamp.
    enqueued_at: Instant,
    /// Shared with outbound copies across reconnects. The encoder advances this
    /// counter on the first data frame, so preparing or queuing a replay does not
    /// count as a transmission. Absent when telemetry is disabled.
    send_attempts: Option<Arc<AtomicU32>>,
    /// Backpressure permit; dropping it frees one `max_inflight_batches` slot.
    _permit: OwnedSemaphorePermit,
}

/// Stable identity used to confirm that a fired ACK deadline still belongs to the
/// same oldest pending batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PendingBatchIdentity {
    offset_id: OffsetId,
    start_record: u64,
    end_record: u64,
}

/// The oldest submitted batch and its absolute acknowledgment deadline.
#[derive(Clone, Copy, Debug)]
pub(super) struct PendingAckDeadline {
    pub(super) identity: PendingBatchIdentity,
    pub(super) deadline: Instant,
}

impl PendingBatch {
    pub(super) fn new(
        batch: RecordBatch,
        offset_id: OffsetId,
        start_record: u64,
        end_record: u64,
        send_attempts: Option<Arc<AtomicU32>>,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            batch,
            offset_id,
            start_record,
            end_record,
            enqueued_at: Instant::now(),
            send_attempts,
            _permit: permit,
        }
    }

    pub(super) fn offset_id(&self) -> OffsetId {
        self.offset_id
    }

    pub(super) fn send_attempts(&self) -> Option<Arc<AtomicU32>> {
        self.send_attempts.clone()
    }

    pub(super) fn is_fully_acknowledged(&self, acked_records: u64) -> bool {
        acked_records >= self.end_record
    }

    fn identity(&self) -> PendingBatchIdentity {
        PendingBatchIdentity {
            offset_id: self.offset_id,
            start_record: self.start_record,
            end_record: self.end_record,
        }
    }

    /// Returns the batch portion not durably acknowledged, avoiding duplicate retry of an
    /// acknowledged prefix.
    ///
    /// Returns `None` when fully acknowledged, the original batch when fully unacknowledged,
    /// or a sliced suffix when partially acknowledged.
    pub(super) fn unacknowledged_suffix(
        &self,
        acked_before_disconnect: u64,
    ) -> Option<RecordBatch> {
        if self.start_record >= acked_before_disconnect {
            return Some(self.batch.clone());
        }

        let records_already_acked =
            (acked_before_disconnect - self.start_record).min(self.batch.num_rows() as u64);
        let remaining_rows = self
            .batch
            .num_rows()
            .saturating_sub(records_already_acked as usize);

        if remaining_rows == 0 {
            None
        } else {
            debug!(target: super::LOG_TARGET,
                offset_id = self.offset_id,
                total_rows = self.batch.num_rows(),
                records_already_acked = records_already_acked,
                remaining_rows = remaining_rows,
                "Slicing partially-acked batch for recovery"
            );
            Some(
                self.batch
                    .slice(records_already_acked as usize, remaining_rows),
            )
        }
    }

    #[cfg(test)]
    pub(super) fn record_range(&self) -> (u64, u64) {
        (self.start_record, self.end_record)
    }

    #[cfg(test)]
    pub(super) fn record_count(&self) -> usize {
        self.batch.num_rows()
    }

    pub(super) fn refresh_enqueued_at(&mut self, enqueued_at: Instant) {
        self.enqueued_at = enqueued_at;
    }

    #[cfg(test)]
    pub(super) fn enqueued_at(&self) -> Instant {
        self.enqueued_at
    }
}

/// Calculates the absolute deadline for the oldest pending batch submitted on the
/// active connection. Batches buffered after a graceful-close pause have ranges at
/// or beyond `submitted_records` and are not awaiting an ACK from that connection.
pub(super) fn oldest_pending_ack_deadline(
    pending: &[PendingBatch],
    submitted_records: u64,
    ack_timeout: Duration,
) -> ZerobusResult<Option<PendingAckDeadline>> {
    let Some(batch) = pending
        .iter()
        .find(|batch| batch.start_record < submitted_records)
    else {
        return Ok(None);
    };
    let deadline = configured_deadline(
        batch.enqueued_at,
        ack_timeout,
        "server_lack_of_ack_timeout_ms",
    )?;
    Ok(Some(PendingAckDeadline {
        identity: batch.identity(),
        deadline,
    }))
}

/// Gives every replayed batch one shared ACK-budget origin immediately before
/// acknowledgment processing resumes on the replacement connection.
pub(super) fn refresh_pending_ack_deadlines(
    pending: &mut [PendingBatch],
    replay_completed_at: Instant,
) {
    for batch in pending {
        batch.refresh_enqueued_at(replay_completed_at);
    }
}

/// Rebuilds pending batches with connection-relative ranges and transfers each retained
/// batch's backpressure permit to its replacement.
pub(super) fn rebuild_pending_for_replay(
    pending: &mut Vec<PendingBatch>,
    acked_before_disconnect: u64,
) -> (Vec<OutboundBatch>, u64) {
    let mut rebuilt = Vec::with_capacity(pending.len());
    let mut replay = Vec::with_capacity(pending.len());
    let mut cumulative_records = 0;

    for mut pending_batch in pending.drain(..) {
        let Some(batch) = pending_batch.unacknowledged_suffix(acked_before_disconnect) else {
            debug!(target: super::LOG_TARGET, offset_id = pending_batch.offset_id, "Skipping fully-acked batch");
            continue;
        };

        let record_count = batch.num_rows() as u64;
        let start_record = cumulative_records;
        let end_record = cumulative_records + record_count;
        cumulative_records = end_record;
        replay.push(OutboundBatch {
            offset: pending_batch.offset_id,
            send_attempts: pending_batch.send_attempts(),
            batch: batch.clone(),
        });
        pending_batch.batch = batch;
        pending_batch.start_record = start_record;
        pending_batch.end_record = end_record;
        rebuilt.push(pending_batch);
    }

    *pending = rebuilt;
    (replay, cumulative_records)
}

/// Deserialises Arrow IPC stream bytes into a [`RecordBatch`].
#[allow(clippy::result_large_err)]
pub(super) fn materialize_ipc(bytes: &Bytes) -> ZerobusResult<RecordBatch> {
    let mut reader = StreamReader::try_new(Cursor::new(bytes.as_ref()), None).map_err(|e| {
        ZerobusError::InvalidArgument(format!("IPC: invalid Arrow IPC stream: {e}"))
    })?;
    let batch = match reader.next() {
        None => {
            return Err(ZerobusError::InvalidArgument(
                "IPC stream contains no RecordBatch".into(),
            ));
        }
        Some(Err(e)) => {
            return Err(ZerobusError::InvalidArgument(format!(
                "IPC: record batch read failed: {e}"
            )));
        }
        Some(Ok(b)) => b,
    };
    match reader.next() {
        None => Ok(batch),
        Some(Ok(_)) => Err(ZerobusError::InvalidArgument(
            "IPC stream must contain exactly one RecordBatch (found extra batch)".into(),
        )),
        Some(Err(e)) => Err(ZerobusError::InvalidArgument(format!(
            "IPC: trailing message read failed: {e}"
        ))),
    }
}

/// Alignment of IPC buffers in compressed payloads, the smallest that the IPC format allows.
const COMPRESSED_IPC_ALIGNMENT: usize = 8;

/// Builds [`IpcWriteOptions`] for the given optional compression codec.
///
/// With compression, 64-byte alignment gives readers no benefit, so compressed payloads
/// use [`COMPRESSED_IPC_ALIGNMENT`].
#[allow(clippy::result_large_err)]
pub(super) fn make_ipc_write_options(
    compression: Option<CompressionType>,
) -> ZerobusResult<IpcWriteOptions> {
    match compression {
        None => Ok(IpcWriteOptions::default()),
        Some(c) => compact_ipc_write_options(Some(c)),
    }
}

/// Builds V5 [`IpcWriteOptions`] with [`COMPRESSED_IPC_ALIGNMENT`] and the given buffer codec.
#[allow(clippy::result_large_err)]
fn compact_ipc_write_options(
    compression: Option<CompressionType>,
) -> ZerobusResult<IpcWriteOptions> {
    IpcWriteOptions::try_new(COMPRESSED_IPC_ALIGNMENT, false, MetadataVersion::V5)
        .and_then(|options| options.try_with_compression(compression))
        .map_err(|e| {
            ZerobusError::InvalidArgument(format!(
                "Failed to configure Arrow IPC write options: {e}"
            ))
        })
}

/// Builds the [`IpcWriteOptions`] a stream encodes its batches with.
///
/// With gRPC compression on, each whole message is compressed, so `ipc_compression` is
/// ignored: compressing buffers twice costs CPU without reducing size. Those buffers also
/// use [`COMPRESSED_IPC_ALIGNMENT`], because wider padding only adds bytes for the server
/// to decompress.
#[allow(clippy::result_large_err)]
pub(super) fn stream_ipc_write_options(
    options: &ArrowStreamConfigurationOptions,
) -> ZerobusResult<IpcWriteOptions> {
    match options.grpc_compression {
        ArrowGrpcCompression::None => make_ipc_write_options(options.ipc_compression),
        ArrowGrpcCompression::Zstd => compact_ipc_write_options(None),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::Int32Array;
    use arrow_flight::encode::FlightDataEncoderBuilder;
    use arrow_ipc::CompressionType;
    use arrow_schema::{DataType, Field, Schema};
    use futures::TryStreamExt;
    use tokio::sync::Semaphore;
    use tokio::time::{Duration, Instant};

    use crate::ZerobusError;

    use super::{
        make_ipc_write_options, oldest_pending_ack_deadline, rebuild_pending_for_replay,
        refresh_pending_ack_deadlines, stream_ipc_write_options, ArrowGrpcCompression,
        ArrowStreamConfigurationOptions, PendingBatch, RecordBatch,
    };

    #[allow(clippy::manual_div_ceil)]
    fn latest_whole_second_instant(start: Instant) -> Instant {
        let mut low = 0_u64;
        let mut high = u64::MAX;
        while low < high {
            let mid = ((low as u128 + high as u128 + 1) / 2) as u64;
            if start.checked_add(Duration::from_secs(mid)).is_some() {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        assert!(low < u64::MAX, "test requires a bounded Instant range");
        start
            .checked_add(Duration::from_secs(low))
            .expect("largest whole-second Instant")
    }

    fn pending_batch(rows: i32, start_record: u64, end_record: u64) -> PendingBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let values: Vec<i32> = (0..rows).collect();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap();
        PendingBatch::new(
            batch,
            0,
            start_record,
            end_record,
            None,
            Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
        )
    }

    #[test]
    fn recovery_slice_keeps_only_unacknowledged_suffix() {
        let batch = pending_batch(5, 10, 15);
        let suffix = batch
            .unacknowledged_suffix(12)
            .expect("unacknowledged suffix");
        assert_eq!(suffix.num_rows(), 3);
    }

    #[test]
    fn recovery_slice_drops_fully_acknowledged_batch() {
        let batch = pending_batch(5, 10, 15);
        assert!(batch.unacknowledged_suffix(15).is_none());
    }

    #[test]
    fn replay_rebuild_preserves_pending_timestamp() {
        let mut batch = pending_batch(5, 0, 5);
        let original = Instant::now() - Duration::from_secs(1);
        batch.refresh_enqueued_at(original);
        let mut pending = vec![batch];

        let (_replay, _records) = rebuild_pending_for_replay(&mut pending, 0);

        assert_eq!(pending[0].enqueued_at(), original);
    }

    #[test]
    fn replay_deadlines_share_completion_timestamp() {
        let mut pending = vec![pending_batch(1, 0, 1), pending_batch(1, 1, 2)];
        let replay_completed_at = Instant::now();

        refresh_pending_ack_deadlines(&mut pending, replay_completed_at);

        assert!(pending
            .iter()
            .all(|batch| batch.enqueued_at() == replay_completed_at));
    }

    #[test]
    fn unrepresentable_ack_deadline_is_rejected() {
        let mut batch = pending_batch(1, 0, 1);
        batch.refresh_enqueued_at(latest_whole_second_instant(Instant::now()));

        let error = oldest_pending_ack_deadline(&[batch], 1, Duration::from_secs(1))
            .expect_err("an unrepresentable configured deadline must be rejected");
        match error {
            ZerobusError::InvalidArgument(message) => {
                assert!(message.contains("server_lack_of_ack_timeout_ms"));
            }
            other => panic!("expected an invalid-argument error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn zstd_ipc_buffers_are_8_byte_aligned() {
        let batch =
            RecordBatch::try_from_iter([("a", Arc::new(Int32Array::from(vec![1, 2, 3])) as _)])
                .unwrap();
        let frames: Vec<_> = FlightDataEncoderBuilder::new()
            .with_options(make_ipc_write_options(Some(CompressionType::ZSTD)).unwrap())
            .build(futures::stream::iter([Ok(batch)]))
            .try_collect()
            .await
            .unwrap();
        let message = arrow_ipc::root_as_message(&frames[1].data_header).unwrap();
        let buffers = message.header_as_record_batch().unwrap().buffers().unwrap();
        assert!(buffers.iter().all(|b| b.offset() % 8 == 0));
        assert!(buffers.iter().any(|b| b.offset() % 64 != 0));
    }

    /// Encodes a small batch with a stream's IPC options and returns the record-batch
    /// message's buffer offsets and whether it carries IPC compression.
    async fn encode_with_stream_options(
        options: &ArrowStreamConfigurationOptions,
    ) -> (Vec<i64>, bool) {
        let batch =
            RecordBatch::try_from_iter([("a", Arc::new(Int32Array::from(vec![1, 2, 3])) as _)])
                .unwrap();
        let frames: Vec<_> = FlightDataEncoderBuilder::new()
            .with_options(stream_ipc_write_options(options).unwrap())
            .build(futures::stream::iter([Ok(batch)]))
            .try_collect()
            .await
            .unwrap();
        let message = arrow_ipc::root_as_message(&frames[1].data_header).unwrap();
        let record_batch = message.header_as_record_batch().unwrap();
        let offsets = record_batch
            .buffers()
            .unwrap()
            .iter()
            .map(|b| b.offset())
            .collect();
        (offsets, record_batch.compression().is_some())
    }

    #[tokio::test]
    async fn grpc_compression_overrides_ipc_compression_with_8_byte_alignment() {
        for ipc_compression in [
            None,
            Some(CompressionType::ZSTD),
            Some(CompressionType::LZ4_FRAME),
        ] {
            let options = ArrowStreamConfigurationOptions {
                ipc_compression,
                grpc_compression: ArrowGrpcCompression::Zstd,
                ..Default::default()
            };
            let (offsets, ipc_compressed) = encode_with_stream_options(&options).await;
            assert!(!ipc_compressed, "{ipc_compression:?} must be ignored");
            assert!(offsets.iter().all(|o| o % 8 == 0));
            assert!(offsets.iter().any(|o| o % 64 != 0));
        }
    }

    #[tokio::test]
    async fn without_grpc_compression_ipc_options_are_unchanged() {
        let compressed = ArrowStreamConfigurationOptions::default();
        assert!(encode_with_stream_options(&compressed).await.1);

        let uncompressed = ArrowStreamConfigurationOptions {
            ipc_compression: None,
            ..Default::default()
        };
        let (offsets, ipc_compressed) = encode_with_stream_options(&uncompressed).await;
        assert!(!ipc_compressed);
        assert!(offsets.iter().all(|o| o % 64 == 0));
    }
}
