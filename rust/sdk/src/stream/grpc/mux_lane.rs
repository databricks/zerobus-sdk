//! gRPC implementation of the private multiplexed-lane contract.
use std::sync::atomic::Ordering;

use super::ZerobusStream;
use crate::multiplexed_stream::lane::{CapacityContext, MuxLane};
use crate::{EncodedBatch, OffsetId, ZerobusError, ZerobusResult};
use async_trait::async_trait;

#[async_trait]
impl MuxLane for ZerobusStream {
    const STREAM_NAME: &'static str = "MultiplexedStream";

    type Batch = EncodedBatch;
    type Reservation = crate::landing_zone::CapacityReservation;

    fn capacity_context(&self) -> CapacityContext<'_> {
        CapacityContext {
            table_name: &self.table_properties.table_name,
            capacity_option: "max_inflight_requests",
            capacity: self.options.max_inflight_requests,
        }
    }

    fn is_terminal(&self) -> bool {
        ZerobusStream::is_closed(self)
    }

    fn signal_shutdown(&self) {
        self.is_closed.store(true, Ordering::Relaxed);
        self.terminal_token.cancel();
        self.cancellation_token.cancel();
    }

    async fn terminal_cause(&self) -> Option<ZerobusError> {
        self.terminal_token.cancelled().await;
        self.server_error_rx.borrow().clone()
    }

    async fn reserve_slot(&self) -> ZerobusResult<Self::Reservation> {
        ZerobusStream::reserve_capacity(self).await
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
        ZerobusStream::enqueue_reserved_admitted(self, batch, reservation, admit).await
    }

    async fn flush_lane(&self) -> ZerobusResult<()> {
        ZerobusStream::flush(self).await
    }

    async fn wait_for_local_offset(&self, offset: OffsetId) -> ZerobusResult<()> {
        ZerobusStream::wait_for_offset(self, offset).await
    }

    // Keep the gRPC pre-flush barrier and cache its outcome before teardown.
    async fn flush_before_close(&self) -> ZerobusResult<()> {
        ZerobusStream::flush(self).await
    }

    async fn close_after_flush(&mut self) -> Option<ZerobusError> {
        if let Some(result) = &self.supervisor_shutdown_result {
            return result.as_ref().err().cloned();
        }
        let task_error = ZerobusStream::shutdown_supervisor(self).await;
        // The supervisor publishes the final error before cancelling this
        // token. A clean shutdown's transient watch error must not be promoted
        // to a terminal cause.
        let lane_error = if self.terminal_token.is_cancelled() {
            self.server_error_rx.borrow().clone()
        } else {
            None
        };
        let result = lane_error.or(task_error).map_or(Ok(()), Err);
        self.supervisor_shutdown_result = Some(result.clone());
        self.is_closed.store(true, Ordering::Relaxed);
        self.terminal_token.cancel();
        result.err()
    }

    async fn drain_callbacks(&mut self) {
        ZerobusStream::shutdown_callbacks(self).await;
    }

    async fn unacked_batches(&self) -> ZerobusResult<Vec<Self::Batch>> {
        ZerobusStream::get_unacked_batches(self).await
    }
}
