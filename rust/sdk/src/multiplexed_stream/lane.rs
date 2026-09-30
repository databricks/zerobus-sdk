//! Private transport contract. `async_trait` keeps this compatible with the core MSRV.
//!
//! Reservations own capacity until admission or drop. Admission must check the mux
//! under the lane's lifecycle lock, before assigning an offset or retaining data.
//! A terminal admission error must await finalization so `is_closed` and the typed
//! terminal cause are visible together. Retryable recovery remains lane-owned.

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
    async fn close_after_flush(&mut self) -> Option<ZerobusError>;
    async fn drain_callbacks(&mut self) {}
    async fn unacked_batches(&self) -> ZerobusResult<Vec<Self::Batch>>;
}
