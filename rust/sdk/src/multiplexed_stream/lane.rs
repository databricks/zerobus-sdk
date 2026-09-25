//! Private transport contract. `async_trait` keeps this compatible with the core MSRV.
//!
//! Reservations own capacity until admission or drop. Admission must check the mux
//! under the lane's lifecycle lock, before assigning an offset or retaining data.
//! A terminal admission error must await finalization so `is_closed` and the typed
//! terminal cause are visible together. Retryable recovery remains lane-owned.

use crate::{EncodedBatch, OffsetId, ZerobusError, ZerobusResult, ZerobusStream};
use async_trait::async_trait;

#[async_trait]
pub(super) trait MuxLane: Send + Sync {
    type Batch: Send;
    type Reservation: Send;

    fn table_name(&self) -> &str;
    fn capacity(&self) -> (&'static str, usize);
    fn is_closed(&self) -> bool;
    fn signal_shutdown(&self) {}
    async fn terminal_error(&self) -> Option<ZerobusError>;
    async fn reserve_capacity(&self) -> ZerobusResult<Self::Reservation>;
    async fn enqueue_reserved_admitted<F>(
        &self,
        batch: Self::Batch,
        reservation: Self::Reservation,
        admit: F,
    ) -> ZerobusResult<OffsetId>
    where
        F: FnOnce() -> ZerobusResult<()> + Send;
    async fn flush(&self) -> ZerobusResult<()>;
    async fn wait_for_offset(&self, offset: OffsetId) -> ZerobusResult<()>;
    // gRPC needs the shared pre-flush barrier. Arrow flushes within its close supervisor.
    async fn flush_before_close(&self) -> ZerobusResult<()> {
        Ok(())
    }
    async fn close_after_flush(&mut self) -> Option<ZerobusError>;
    async fn shutdown_callbacks(&mut self) {}
    async fn get_unacked_batches(&self) -> ZerobusResult<Vec<Self::Batch>>;
}

#[async_trait]
impl MuxLane for ZerobusStream {
    type Batch = EncodedBatch;
    type Reservation = crate::landing_zone::CapacityReservation;

    fn table_name(&self) -> &str {
        &self.table_properties.table_name
    }
    fn capacity(&self) -> (&'static str, usize) {
        ("max_inflight_requests", self.options.max_inflight_requests)
    }
    fn is_closed(&self) -> bool {
        self.is_closed()
    }
    fn signal_shutdown(&self) {
        self.signal_shutdown();
    }
    async fn terminal_error(&self) -> Option<ZerobusError> {
        self.terminal_error().await
    }
    async fn reserve_capacity(&self) -> ZerobusResult<Self::Reservation> {
        self.reserve_capacity().await
    }
    async fn enqueue_reserved_admitted<F>(
        &self,
        batch: Self::Batch,
        reservation: Self::Reservation,
        admit: F,
    ) -> ZerobusResult<OffsetId>
    where
        F: FnOnce() -> ZerobusResult<()> + Send,
    {
        self.enqueue_reserved_admitted(batch, reservation, admit)
            .await
    }
    async fn flush(&self) -> ZerobusResult<()> {
        self.flush().await
    }
    async fn wait_for_offset(&self, offset: OffsetId) -> ZerobusResult<()> {
        self.wait_for_offset(offset).await
    }
    async fn flush_before_close(&self) -> ZerobusResult<()> {
        self.flush().await
    }
    async fn close_after_flush(&mut self) -> Option<ZerobusError> {
        self.close_after_flush().await
    }
    async fn shutdown_callbacks(&mut self) {
        self.shutdown_callbacks().await;
    }
    async fn get_unacked_batches(&self) -> ZerobusResult<Vec<Self::Batch>> {
        self.get_unacked_batches().await
    }
}
