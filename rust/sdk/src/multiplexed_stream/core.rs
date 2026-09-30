//! Shared mux routing and lifecycle. Transport-specific invariants stay in `MuxLane`.
use super::lane::{CapacityContext, MuxLane};
use super::{MessageId, STREAM_BITS};
use crate::{ZerobusError, ZerobusResult};
use futures::future::join_all;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

const CAPACITY_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) struct MuxCore<S: MuxLane> {
    streams: Vec<S>,
    round_robin_counter: AtomicUsize,
    is_closed: AtomicBool,
    closed_token: CancellationToken,
    failure: OnceLock<ZerobusError>,
    // Cache completed pre-close work so cancelled teardown can resume.
    close_flush_result: Option<ZerobusResult<()>>,
}

impl<S: MuxLane> MuxCore<S> {
    pub(super) fn from_streams(streams: Vec<S>) -> Self {
        assert!(
            !streams.is_empty(),
            "MultiplexedStream requires at least one sub-stream"
        );
        assert!(
            streams.len() <= (1 << STREAM_BITS),
            "MultiplexedStream supports at most {} sub-streams",
            1 << STREAM_BITS
        );
        Self {
            streams,
            round_robin_counter: AtomicUsize::new(0),
            is_closed: AtomicBool::new(false),
            closed_token: CancellationToken::new(),
            failure: OnceLock::new(),
            close_flush_result: None,
        }
    }

    #[allow(clippy::result_large_err)]
    pub(super) fn check_closed(&self) -> ZerobusResult<()> {
        if let Some(error) = self.failure.get() {
            return Err(error.clone());
        }
        if self.is_closed_fast() {
            return Err(self.closed_error());
        }
        Ok(())
    }

    fn closed_error(&self) -> ZerobusError {
        self.failure.get().cloned().unwrap_or_else(|| {
            ZerobusError::InvalidStateError("MultiplexedStream is closed".to_string())
        })
    }

    fn is_closed_fast(&self) -> bool {
        self.is_closed.load(Ordering::Relaxed)
    }

    fn first_closed_stream(&self) -> Option<usize> {
        self.streams.iter().position(MuxLane::is_terminal)
    }

    async fn lane_terminal_error(&self, idx: usize, fallback: ZerobusError) -> ZerobusError {
        self.streams[idx].terminal_cause().await.unwrap_or(fallback)
    }

    fn shutdown_on_failure(&self, trigger_index: usize, cause: &ZerobusError) {
        if self.is_closed_fast() || self.failure.set(cause.clone()).is_err() {
            return;
        }
        self.is_closed.store(true, Ordering::Relaxed);
        self.closed_token.cancel();

        error!(
            trigger_stream_index = trigger_index,
            cause = %cause,
            num_streams = self.streams.len(),
            "MultiplexedStream poisoned due to sub-stream failure"
        );
    }

    // Keep the selected lane through backpressure; do not reroute.
    fn pick_substream(&self) -> usize {
        self.round_robin_counter.fetch_add(1, Ordering::Relaxed) % self.streams.len()
    }

    async fn reserve_capacity(&self, stream: &S, idx: usize) -> ZerobusResult<S::Reservation> {
        let started_at = tokio::time::Instant::now();
        let timeout_ms = CAPACITY_WAIT_TIMEOUT.as_millis();
        let CapacityContext {
            table_name,
            capacity_option,
            capacity,
        } = stream.capacity_context();

        self.check_closed()?;

        let wait_for_reservation = async {
            let reservation = stream.reserve_slot();
            tokio::pin!(reservation);

            if let Ok(result) = tokio::time::timeout(Duration::from_secs(1), &mut reservation).await
            {
                return result;
            }
            let waited_ms = started_at.elapsed().as_millis();
            warn!(
                stream_index = idx,
                table_name,
                waited_ms,
                timeout_ms,
                capacity,
                capacity_option,
                "Backpressure: sub-stream at capacity, waiting for drain"
            );
            reservation.await
        };

        let result = tokio::select! {
            result = tokio::time::timeout(CAPACITY_WAIT_TIMEOUT, wait_for_reservation) => result,
            _ = self.closed_token.cancelled() => {
                let waited_ms = started_at.elapsed().as_millis();
                if let Some(failure) = self.failure.get() {
                    return Err(failure.clone());
                }
                warn!(
                    stream_index = idx,
                    table_name,
                    waited_ms,
                    capacity,
                    capacity_option,
                    "Multiplexed capacity wait cancelled by shutdown"
                );
                return Err(ZerobusError::InvalidStateError(
                    format!(
                        "MultiplexedStream closed after {waited_ms} ms while waiting for capacity on sub-stream {idx} for table {table_name} ({capacity_option}: {capacity})"
                    ),
                ));
            }
        };

        match result {
            Ok(Ok(reservation)) => Ok(reservation),
            Ok(Err(e)) => Err(self.handle_lane_error(idx, e).await),
            Err(_) => {
                self.check_closed()?;
                if stream.is_terminal() {
                    return Err(self
                        .handle_lane_error(
                            idx,
                            ZerobusError::ConnectionTimeout(format!(
                                "Timed out waiting for capacity on multiplexed sub-stream {idx}"
                            )),
                        )
                        .await);
                }
                let waited_ms = started_at.elapsed().as_millis();
                warn!(
                    stream_index = idx,
                    table_name,
                    waited_ms,
                    timeout_ms,
                    capacity,
                    capacity_option,
                    "Timed out waiting for multiplexed sub-stream capacity"
                );
                Err(ZerobusError::ConnectionTimeout(format!(
                    "Timed out after {waited_ms} ms waiting for capacity on multiplexed sub-stream {idx} for table {table_name} (configured timeout: {timeout_ms} ms, {capacity_option}: {capacity})"
                )))
            }
        }
    }

    pub(super) fn first(&self) -> &S {
        &self.streams[0]
    }

    pub(super) async fn ingest<F>(&self, prepare: F) -> ZerobusResult<MessageId>
    where
        F: FnOnce(&S) -> ZerobusResult<S::Batch>,
    {
        self.check_closed()?;
        let idx = self.pick_substream();
        let stream = &self.streams[idx];
        let encoded_batch = prepare(stream)?;
        let reservation = self.reserve_capacity(stream, idx).await?;
        let enqueue_result = stream
            .enqueue_admitted(encoded_batch, reservation, || self.check_closed())
            .await;

        match enqueue_result {
            Ok(off) => Ok(MessageId::new(idx, off)),
            Err(e) => Err(self.handle_lane_error(idx, e).await),
        }
    }

    // Payload errors and wait timeouts leave the lane alive; only terminal
    // lane errors poison the mux.
    async fn handle_lane_error(&self, idx: usize, e: ZerobusError) -> ZerobusError {
        if self.streams[idx].is_terminal() {
            let cause = self.lane_terminal_error(idx, e).await;
            self.shutdown_on_failure(idx, &cause);
            cause
        } else {
            warn!(stream_index = idx, error = %e, "Sub-stream operation errored but lane remains alive");
            e
        }
    }

    pub async fn flush(&self) -> ZerobusResult<()> {
        if self.is_closed_fast() && self.failure.get().is_none() {
            return Err(self.closed_error());
        }
        if self.failure.get().is_none() {
            if let Some(idx) = self.first_closed_stream() {
                let error = self
                    .lane_terminal_error(
                        idx,
                        ZerobusError::InvalidStateError(format!(
                            "MultiplexedStream sub-stream {idx} is closed"
                        )),
                    )
                    .await;
                self.shutdown_on_failure(idx, &error);
            }
        }

        let results = join_all(self.streams.iter().map(MuxLane::flush_lane)).await;
        let mut first_error: Option<ZerobusError> = None;
        let mut first_terminal: Option<(usize, ZerobusError)> = None;
        for (i, result) in results.into_iter().enumerate() {
            if let Err(e) = result {
                if self.streams[i].is_terminal() && first_terminal.is_none() {
                    let terminal_error = self.lane_terminal_error(i, e.clone()).await;
                    first_terminal = Some((i, terminal_error));
                }
                if first_error.is_none() {
                    first_error = Some(e);
                } else {
                    warn!(
                        stream_index = i,
                        error = %e,
                        "Additional sub-stream flush error"
                    );
                }
            }
        }
        if let Some((i, error)) = first_terminal {
            self.shutdown_on_failure(i, &error);
        } else if let Some(error) = &first_error {
            warn!(error = %error, "flush errored but sub-streams still alive");
        }
        self.failure
            .get()
            .cloned()
            .or(first_error)
            .map_or(Ok(()), Err)
    }

    pub async fn wait_for_message_id(&self, message_id: MessageId) -> ZerobusResult<()> {
        let idx = message_id.stream_index();
        if idx >= self.streams.len() {
            return Err(ZerobusError::InvalidArgument(format!(
                "Invalid stream index {} in message id",
                idx
            )));
        }
        match self.streams[idx]
            .wait_for_local_offset(message_id.sub_offset())
            .await
        {
            Ok(()) => Ok(()),
            Err(e) => Err(self.handle_lane_error(idx, e).await),
        }
    }

    pub async fn close(&mut self) -> ZerobusResult<()> {
        info!("Closing MultiplexedStream");

        self.is_closed.store(true, Ordering::Relaxed);
        self.closed_token.cancel();

        if self.close_flush_result.is_none() {
            if self.failure.get().is_none() {
                if let Some(idx) = self.first_closed_stream() {
                    if let Some(error) = self.streams[idx].terminal_cause().await {
                        let _ = self.failure.set(error);
                    }
                }
            }
            let mut first_error = None;
            // All lanes stay live through this flush attempt. Cache its result
            // before any supervisor is stopped or callback drain can be cancelled.
            let results = join_all(self.streams.iter().map(MuxLane::flush_before_close)).await;
            for (i, result) in results.into_iter().enumerate() {
                if let Err(error) = result {
                    if self.failure.get().is_none() && self.streams[i].is_terminal() {
                        if let Some(cause) = self.streams[i].terminal_cause().await {
                            let _ = self.failure.set(cause);
                        }
                    }
                    if first_error.is_none() {
                        first_error = Some(error);
                    } else {
                        warn!(stream_index = i, error = %error, "Additional sub-stream flush error during close");
                    }
                }
            }
            self.close_flush_result = Some(first_error.map_or(Ok(()), Err));
        }

        let failure = &self.failure;
        join_all(self.streams.iter_mut().enumerate().map(|(i, stream)| async move {
            if let Some(error) = stream.close_after_flush().await {
                // Publish immediately, not after join_all: another lane's
                // callbacks may still be draining when this close is cancelled.
                if let Err(error) = failure.set(error) {
                    warn!(stream_index = i, error = %error, "Additional terminal lane error during close");
                }
            }
            stream.drain_callbacks().await;
        }))
        .await;

        if let Some(error) = self.failure.get() {
            Err(error.clone())
        } else {
            self.close_flush_result
                .as_ref()
                .expect("close flush completed")
                .clone()
        }
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed_fast()
    }

    pub(super) async fn get_unacked_batches(&mut self) -> ZerobusResult<Vec<S::Batch>> {
        let _ = self.close().await;
        let mut all_batches = Vec::new();
        for stream in &self.streams {
            all_batches.extend(stream.unacked_batches().await?);
        }
        Ok(all_batches)
    }
}

impl<S: MuxLane> Drop for MuxCore<S> {
    fn drop(&mut self) {
        self.is_closed.store(true, Ordering::Relaxed);
        self.closed_token.cancel();
        // Fire cancellation on every sub-stream in parallel so their
        // background tasks can start unwinding concurrently. The Vec drop
        // below then runs each lane's Drop, which aborts any
        // JoinHandles that haven't already exited.
        for stream in &self.streams {
            stream.signal_shutdown();
        }
    }
}
