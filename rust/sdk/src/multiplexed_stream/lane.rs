//! Private transport contract. `async_trait` keeps this compatible with the core MSRV.
//!
//! Reservations own capacity until admission or drop. Admission must check the mux
//! under the lane's lifecycle lock, before assigning an offset or retaining data.
//! Admission rejects closure promptly. `is_closed` must recognize terminal admission,
//! and `terminal_error` may await finalization to supply the typed cause to the core.
//! Retryable recovery remains lane-owned.

use crate::{OffsetId, ZerobusError, ZerobusResult};
use async_trait::async_trait;

/// Diagnostic context for a selected lane's capacity wait.
pub(crate) struct CapacityContext<'a> {
    pub(crate) table_name: &'a str,
    pub(crate) capacity_option: &'static str,
    pub(crate) capacity: usize,
}

#[async_trait]
pub(crate) trait MuxLane: Send + Sync {
    type Batch: Send;
    type Reservation: Send;

    fn capacity_context(&self) -> CapacityContext<'_>;
    fn is_terminal(&self) -> bool;
    fn signal_shutdown(&self) {}
    async fn terminal_cause(&self) -> Option<ZerobusError>;
    async fn reserve_slot(&self) -> ZerobusResult<Self::Reservation>;
    async fn enqueue_admitted<F>(
        &self,
        batch: Self::Batch,
        reservation: Self::Reservation,
        admit: F,
    ) -> ZerobusResult<OffsetId>
    where
        F: FnOnce() -> ZerobusResult<()> + Send;
    async fn flush_lane(&self) -> ZerobusResult<()>;
    async fn wait_for_local_offset(&self, offset: OffsetId) -> ZerobusResult<()>;
    // gRPC needs the shared pre-flush barrier. Arrow flushes within its close supervisor.
    async fn flush_before_close(&self) -> ZerobusResult<()> {
        Ok(())
    }
    // The core records the first error before entering cancellable callback draining.
    // Arrow's result includes its supervisor-owned close-time flush.
    async fn close_after_flush(&mut self) -> Option<ZerobusError>;
    async fn drain_callbacks(&mut self) {}
    async fn unacked_batches(&self) -> ZerobusResult<Vec<Self::Batch>>;
}

#[cfg(feature = "arrow-flight")]
#[async_trait]
impl MuxLane for crate::ZerobusArrowStream {
    type Batch = arrow_array::RecordBatch;
    type Reservation = tokio::sync::OwnedSemaphorePermit;

    fn capacity_context(&self) -> CapacityContext<'_> {
        CapacityContext {
            table_name: crate::ZerobusArrowStream::table_name(self),
            capacity_option: "max_inflight_batches",
            capacity: self.options.max_inflight_batches,
        }
    }
    fn is_terminal(&self) -> bool {
        // Terminal admission precedes retained-batch finalization. Recognize it
        // here, then await the final cause in `terminal_error` before poisoning.
        self.is_ingest_admission_closed()
    }
    async fn terminal_cause(&self) -> Option<ZerobusError> {
        crate::ZerobusArrowStream::terminal_error(self).await
    }
    async fn reserve_slot(&self) -> ZerobusResult<Self::Reservation> {
        crate::ZerobusArrowStream::reserve_capacity(self).await
    }
    async fn enqueue_admitted<F>(
        &self,
        batch: Self::Batch,
        reservation: Self::Reservation,
        admit: F,
    ) -> ZerobusResult<OffsetId>
    where
        F: FnOnce() -> ZerobusResult<()> + Send,
    {
        crate::ZerobusArrowStream::enqueue_reserved_admitted(self, batch, reservation, admit).await
    }
    async fn flush_lane(&self) -> ZerobusResult<()> {
        crate::ZerobusArrowStream::flush(self).await
    }
    async fn wait_for_local_offset(&self, offset: OffsetId) -> ZerobusResult<()> {
        crate::ZerobusArrowStream::wait_for_offset(self, offset).await
    }
    async fn close_after_flush(&mut self) -> Option<ZerobusError> {
        crate::ZerobusArrowStream::close(self).await.err()
    }
    async fn unacked_batches(&self) -> ZerobusResult<Vec<Self::Batch>> {
        crate::ZerobusArrowStream::get_unacked_batches(self).await
    }
}
