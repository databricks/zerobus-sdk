//! Private transport contract. `async_trait` keeps this compatible with the core MSRV.
//!
//! Reservations own capacity until admission or drop. Admission must check the mux
//! under the lane's lifecycle lock, before assigning an offset or retaining data.
//! `is_terminal` must recognize terminal admission before the lane finishes
//! finalizing its error. The core calls `terminal_cause` only after that check;
//! it may then wait for the typed cause. Retryable recovery remains lane-owned.

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
    /// Name used in mux errors and lifecycle logs.
    const STREAM_NAME: &'static str;

    /// Payload retained by a lane for recovery.
    type Batch: Send;
    /// Capacity held until admission or drop.
    type Reservation: Send;

    /// Table and configured capacity used in wait diagnostics.
    fn capacity_context(&self) -> CapacityContext<'_>;
    /// Whether this lane has permanently stopped accepting new batches.
    fn is_terminal(&self) -> bool;
    /// Signal background work to stop when the mux is dropped.
    fn signal_shutdown(&self);
    /// Return the finalized cause; call only after `is_terminal()` is true.
    /// May wait for terminal finalization and return `None` after a clean close.
    async fn terminal_cause(&self) -> Option<ZerobusError>;
    /// Reserve one lane slot without consuming an offset.
    async fn reserve_slot(&self) -> ZerobusResult<Self::Reservation>;
    /// Check mux admission under the lane lock, then assign an offset and enqueue.
    async fn enqueue_admitted<F>(
        &self,
        batch: Self::Batch,
        reservation: Self::Reservation,
        admit: F,
    ) -> ZerobusResult<OffsetId>
    where
        F: FnOnce() -> ZerobusResult<()> + Send;
    /// Wait for this lane's queued batches to be acknowledged.
    async fn flush_lane(&self) -> ZerobusResult<()>;
    /// Wait for one lane-local offset.
    async fn wait_for_local_offset(&self, offset: OffsetId) -> ZerobusResult<()>;
    /// Complete any lane flush that must precede concurrent teardown.
    async fn flush_before_close(&self) -> ZerobusResult<()>;
    /// Stop the lane after the flush phase and return its terminal error, if any.
    async fn close_after_flush(&mut self) -> Option<ZerobusError>;
    /// Drain callbacks after the core has recorded the lane-close error.
    async fn drain_callbacks(&mut self);
    /// Return the lane's unacknowledged payloads after close.
    async fn unacked_batches(&self) -> ZerobusResult<Vec<Self::Batch>>;
}
