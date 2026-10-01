use arrow_array::RecordBatch;
use bytes::Bytes;

use super::{MessageId, MuxCore};
use crate::{ZerobusArrowStream, ZerobusResult};

/// Distributes whole Arrow batches round-robin across Arrow Flight streams.
///
/// Create with `.arrow(schema).multiplexed(n).build_arrow()`. The mux-wide
/// `max_inflight_batches` budget is divided evenly using integer division; the
/// remainder is unused. A selected lane waits up to 30 seconds for capacity.
/// There is no global ordering of batches, message IDs, or recovery results.
///
/// Each lane retains Arrow's recovery and close behavior. Once an operation
/// observes a terminal lane failure, ingestion is rejected across the mux.
/// Healthy lanes remain alive until close or drop; their message waits remain
/// independent of sibling failures. A shared stats exporter receives lane-local
/// events whose offsets may overlap. Acknowledgment callbacks are unsupported.
///
/// ```no_run
/// # use databricks_zerobus_ingest_sdk::{MultiplexedArrowStream, ZerobusError};
/// # use arrow_array::RecordBatch;
/// # async fn ingest(mut stream: MultiplexedArrowStream, batches: Vec<RecordBatch>) -> Result<(), ZerobusError> {
/// for batch in batches {
///     stream.ingest_batch(batch).await?;
/// }
/// stream.flush().await?;
/// stream.close().await?;
/// # Ok(())
/// # }
/// ```
///
/// # Beta
///
/// Multiplexed streams are a Beta API. Requires the `arrow-flight` feature.
pub struct MultiplexedArrowStream {
    core: MuxCore<ZerobusArrowStream>,
}

impl MultiplexedArrowStream {
    pub(crate) fn from_streams(streams: Vec<ZerobusArrowStream>) -> Self {
        Self {
            core: MuxCore::from_streams(streams),
        }
    }

    /// Queues a whole batch on the next lane and returns its lane and local offset.
    ///
    /// Use `flush()` after queuing batches to await durability. Empty batches and
    /// mismatched schemas return `InvalidArgument` without poisoning the mux.
    pub async fn ingest_batch(&self, batch: RecordBatch) -> ZerobusResult<MessageId> {
        self.core
            .ingest(|stream| {
                stream.validate_batch(&batch)?;
                Ok(batch)
            })
            .await
    }

    /// Decodes an Arrow IPC stream containing exactly one batch and queues it.
    /// Prefer `ingest_batch` when a `RecordBatch` is already available.
    pub async fn ingest_ipc_batch(&self, ipc_bytes: Bytes) -> ZerobusResult<MessageId> {
        self.core
            .ingest(|stream| stream.prepare_ipc_batch(&ipc_bytes))
            .await
    }

    /// Waits for all lanes' flush attempts, including healthy lanes after poison.
    /// A stored terminal mux failure takes precedence over other flush errors.
    pub async fn flush(&self) -> ZerobusResult<()> {
        self.core.flush().await
    }

    /// Waits for this message's lane only. A sibling failure does not fail the wait.
    /// IDs must belong to this mux; IDs have no global ordering.
    pub async fn wait_for_message_id(&self, id: MessageId) -> ZerobusResult<()> {
        self.core.wait_for_message_id(id).await
    }

    /// Closes every lane concurrently through its Arrow close supervisor.
    ///
    /// Each lane keeps its original close deadline and interrupts active recovery.
    /// Retrying a cancelled close resumes teardown. A recorded mux failure takes
    /// precedence; otherwise the first observed lane-close error is returned and
    /// additional errors are logged. All lanes are closed even when one fails.
    pub async fn close(&mut self) -> ZerobusResult<()> {
        self.core.close().await
    }

    /// Returns whether explicit close or an observed terminal lane failure stopped ingestion.
    pub fn is_closed(&self) -> bool {
        self.core.is_closed()
    }

    /// Closes all lanes, ignoring close errors, then returns unacknowledged batches.
    ///
    /// Partially acknowledged batches contain only their unacknowledged suffix.
    /// Repeated calls return the same snapshot. Lane grouping does not reproduce
    /// global submission order; dropping without close does not retain batches.
    pub async fn get_unacked_batches(&mut self) -> ZerobusResult<Vec<RecordBatch>> {
        self.core.get_unacked_batches().await
    }
}
