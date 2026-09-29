//! Fan-out wrapper that distributes ingestion across multiple [`ZerobusStream`]s.
//!
//! A single `ZerobusStream` is throughput-limited by its in-flight window;
//! `MultiplexedStream` routes records round-robin across a fixed set of
//! sub-streams to raise aggregate throughput. When the chosen sub-stream is at
//! capacity the call awaits drain rather than rerouting, so per-sub-stream
//! ordering is preserved.
//!
//! Each ingest returns an opaque [`MessageId`] that packs the sub-stream index
//! and its offset into a single `i64` (6 bits of stream index → up to 64
//! sub-streams). Callers later pass it to
//! [`wait_for_message_id`](MultiplexedStream::wait_for_message_id) without
//! needing to know which sub-stream handled the record.
//!
//! After a mux operation observes an unrecoverable sub-stream error, the mux
//! stops accepting new records. Healthy sub-streams remain alive until the mux
//! is closed or dropped, and any records still buffered can be recovered via
//! [`get_unacked_records`](MultiplexedStream::get_unacked_records) or
//! [`get_unacked_batches`](MultiplexedStream::get_unacked_batches).

use futures::future::join_all;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    AckCallback, DynamicRecord, EncodedBatch, EncodedRecord, MessageDescriptor, OffsetId,
    PreparedInput, ZerobusError, ZerobusResult, ZerobusStream,
};

const CAPACITY_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// Number of bits reserved for the stream index.
/// 6 bits supports up to 64 sub-streams.
const STREAM_BITS: u32 = 6;
pub(crate) const MAX_STREAMS: usize = 1 << STREAM_BITS;
const OFFSET_MASK: i64 = (1i64 << (64 - STREAM_BITS)) - 1;

/// Opaque identifier returned by ingest methods on MultiplexedStream.
/// Encodes the sub-stream index and sub-stream offset in a single i64.
///
/// Unlike a `ZerobusStream` offset, `MessageId` values are not ordered — pass
/// them to [`MultiplexedStream::wait_for_message_id`] to await acknowledgment.
///
/// # Beta
///
/// Multiplexed streams are a Beta API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MessageId(i64);

impl std::fmt::Display for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MessageId(stream={}, offset={})",
            self.stream_index(),
            self.sub_offset()
        )
    }
}

impl MessageId {
    pub(crate) fn new(stream_index: usize, sub_offset: OffsetId) -> Self {
        debug_assert!(stream_index < (1 << STREAM_BITS));
        debug_assert!((0..=OFFSET_MASK).contains(&sub_offset));
        Self(((stream_index as i64) << (64 - STREAM_BITS)) | (sub_offset & OFFSET_MASK))
    }

    /// Returns the sub-stream index this message was sent to.
    pub fn stream_index(&self) -> usize {
        ((self.0 as u64) >> (64 - STREAM_BITS)) as usize
    }

    /// Returns the offset within the sub-stream.
    pub fn sub_offset(&self) -> OffsetId {
        self.0 & OFFSET_MASK
    }

    /// Returns the raw i64 value, e.g. for transport across an FFI boundary.
    pub fn raw(&self) -> i64 {
        self.0
    }

    /// Construct from a raw i64 value previously obtained from [`MessageId::raw`].
    ///
    /// Only round-trip values from `raw()`: a fabricated id pointing at an
    /// offset that was never ingested makes `wait_for_message_id` wait until
    /// the flush timeout (indefinitely if none is configured).
    pub fn from_raw(raw: i64) -> Self {
        Self(raw)
    }
}

struct MultiplexedAckCallbackAdapter {
    stream_index: usize,
    callback: Arc<dyn AckCallback<MessageId>>,
}

impl AckCallback for MultiplexedAckCallbackAdapter {
    fn on_ack(&self, offset_id: OffsetId) {
        self.callback
            .on_ack(MessageId::new(self.stream_index, offset_id));
    }

    fn on_error(&self, offset_id: OffsetId, error_message: &str) {
        self.callback
            .on_error(MessageId::new(self.stream_index, offset_id), error_message);
    }
}

/// Creates the callback installed on one multiplexed sub-stream.
///
/// The adapter captures the sub-stream index and converts each stream-local
/// [`OffsetId`] into the [`MessageId`] exposed by [`MultiplexedStream`].
pub(crate) fn multiplexed_ack_callback(
    stream_index: usize,
    callback: Arc<dyn AckCallback<MessageId>>,
) -> Arc<dyn AckCallback> {
    assert!(
        stream_index < (1 << STREAM_BITS),
        "MultiplexedStream supports at most {} sub-streams",
        1 << STREAM_BITS
    );
    Arc::new(MultiplexedAckCallbackAdapter {
        stream_index,
        callback,
    })
}

/// Distributes ingestion round-robin across a fixed set of [`ZerobusStream`]s.
///
/// A selected sub-stream that is at capacity waits for room rather than
/// rerouting, preserving ordering within each lane. There is no global record,
/// [`MessageId`], acknowledgment, callback, or recovery order across lanes.
///
/// After a mux operation observes a terminal lane failure, the mux rejects new
/// ingestion and preserves that lane's typed error. Healthy lanes remain alive
/// to process already-accepted records until [`close`](Self::close) or drop.
/// [`flush`](Self::flush) waits for every lane's flush attempt, and unacknowledged
/// records can be recovered with [`get_unacked_records`](Self::get_unacked_records)
/// or [`get_unacked_batches`](Self::get_unacked_batches).
///
/// # Beta
///
/// This API is in Beta.
pub struct MultiplexedStream {
    streams: Vec<ZerobusStream>,
    round_robin_counter: AtomicUsize,
    is_closed: AtomicBool,
    closed_token: CancellationToken,
    failure: OnceLock<ZerobusError>,
    /// Completed close-time flush result; retries resume teardown without
    /// flushing a transport whose supervisor may already have stopped.
    close_flush_result: Option<ZerobusResult<()>>,
}

impl MultiplexedStream {
    /// Creates a multiplexed stream over the given sub-streams.
    ///
    /// Ingest waits up to 30 seconds for capacity on its selected sub-stream.
    ///
    /// # Panics
    ///
    /// Panics if `streams` is empty or holds more than 64 sub-streams.
    #[cfg(feature = "testing")]
    pub fn new(streams: Vec<ZerobusStream>) -> Self {
        Self::from_streams(streams)
    }

    pub(crate) fn from_streams(streams: Vec<ZerobusStream>) -> Self {
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

    /// Returns the schema descriptor configured with [`crate::StreamBuilder::dynamic_proto`].
    /// Returns an error if this is not a dynamic-protobuf stream.
    pub fn message_descriptor(&self) -> ZerobusResult<MessageDescriptor> {
        self.streams[0].message_descriptor()
    }

    /// Creates an empty record using this mux's dynamic-protobuf schema.
    pub fn new_record(&self) -> ZerobusResult<DynamicRecord> {
        self.streams[0].new_record()
    }

    #[allow(clippy::result_large_err)]
    fn check_closed(&self) -> ZerobusResult<()> {
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
        self.streams.iter().position(ZerobusStream::is_closed)
    }

    async fn lane_terminal_error(&self, idx: usize, fallback: ZerobusError) -> ZerobusError {
        self.streams[idx].terminal_error().await.unwrap_or(fallback)
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

    // TODO: if the picked sub-stream is at capacity, try the next one before
    // falling back to waiting.
    fn pick_substream(&self) -> usize {
        self.round_robin_counter.fetch_add(1, Ordering::Relaxed) % self.streams.len()
    }

    async fn reserve_capacity(
        &self,
        stream: &ZerobusStream,
        idx: usize,
    ) -> ZerobusResult<crate::landing_zone::CapacityReservation> {
        let started_at = tokio::time::Instant::now();
        let timeout_ms = CAPACITY_WAIT_TIMEOUT.as_millis();
        let table_name = stream.table_properties.table_name.as_str();
        let max_inflight_requests = stream.options.max_inflight_requests;

        self.check_closed()?;

        let wait_for_reservation = async {
            let reservation = stream.reserve_capacity();
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
                max_inflight_requests,
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
                    max_inflight_requests,
                    "Multiplexed capacity wait cancelled by shutdown"
                );
                return Err(ZerobusError::InvalidStateError(
                    format!(
                        "MultiplexedStream closed after {waited_ms} ms while waiting for capacity on sub-stream {idx} for table {table_name} (max_inflight_requests: {max_inflight_requests})"
                    ),
                ));
            }
        };

        match result {
            Ok(Ok(reservation)) => Ok(reservation),
            Ok(Err(e)) => Err(self.handle_lane_error(idx, e).await),
            Err(_) => {
                self.check_closed()?;
                if stream.is_closed() {
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
                    max_inflight_requests,
                    "Timed out waiting for multiplexed sub-stream capacity"
                );
                Err(ZerobusError::ConnectionTimeout(format!(
                    "Timed out after {waited_ms} ms waiting for capacity on multiplexed sub-stream {idx} for table {table_name} (configured timeout: {timeout_ms} ms, max_inflight_requests: {max_inflight_requests})"
                )))
            }
        }
    }

    async fn enqueue_reserved(
        &self,
        stream: &ZerobusStream,
        idx: usize,
        encoded_batch: EncodedBatch,
    ) -> ZerobusResult<MessageId> {
        let reservation = self.reserve_capacity(stream, idx).await?;
        let enqueue_result = stream
            .enqueue_reserved_admitted(encoded_batch, reservation, || self.check_closed())
            .await;

        match enqueue_result {
            Ok(off) => Ok(MessageId::new(idx, off)),
            Err(e) => Err(self.handle_lane_error(idx, e).await),
        }
    }

    // Payload errors and wait timeouts leave the lane alive; only terminal
    // lane errors poison the mux.
    async fn handle_lane_error(&self, idx: usize, e: ZerobusError) -> ZerobusError {
        if self.streams[idx].is_closed() {
            let cause = self.lane_terminal_error(idx, e).await;
            self.shutdown_on_failure(idx, &cause);
            cause
        } else {
            warn!(stream_index = idx, error = %e, "Sub-stream operation errored but lane remains alive");
            e
        }
    }

    /// Ingests a single record into the next sub-stream (round-robin).
    ///
    /// Returns once the record is queued; use
    /// [`wait_for_message_id`](Self::wait_for_message_id) with the returned id
    /// to await server acknowledgment. If the chosen sub-stream is at capacity,
    /// this waits for it to drain rather than rerouting.
    pub async fn ingest_record(
        &self,
        payload: impl Into<PreparedInput>,
    ) -> ZerobusResult<MessageId> {
        self.check_closed()?;
        let idx = self.pick_substream();
        let stream = &self.streams[idx];
        let encoded_batch = stream.prepare_record(payload)?;
        self.enqueue_reserved(stream, idx, encoded_batch).await
    }

    /// Ingests a batch of records into a single sub-stream (round-robin).
    ///
    /// The whole batch lands on one sub-stream so a single returned id covers
    /// it. Returns `None` for an empty batch unless the mux is already closed
    /// or poisoned, in which case it returns an error.
    // TODO: Check if there is a performance advantage in splitting this payload in multiple streams
    pub async fn ingest_records<I, T>(&self, payload: I) -> ZerobusResult<Option<MessageId>>
    where
        I: IntoIterator<Item = T>,
        T: Into<PreparedInput>,
    {
        self.check_closed()?;
        let records: Vec<T> = payload.into_iter().collect();
        if records.is_empty() {
            return Ok(None);
        }
        let idx = self.pick_substream();
        let stream = &self.streams[idx];
        let encoded_batch = stream.prepare_records(records)?;
        self.enqueue_reserved(stream, idx, encoded_batch)
            .await
            .map(Some)
    }

    /// Waits until every record already queued on every sub-stream is
    /// acknowledged by the server.
    ///
    /// If a sub-stream flush fails because that sub-stream reached a terminal
    /// state, the mux is poisoned after every healthy lane has completed its
    /// flush attempt. A stored terminal mux failure takes precedence;
    /// otherwise the first flush error is returned. Additional errors are
    /// logged.
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

        let results = join_all(self.streams.iter().map(ZerobusStream::flush)).await;
        let mut first_error: Option<ZerobusError> = None;
        let mut first_terminal: Option<(usize, ZerobusError)> = None;
        for (i, result) in results.into_iter().enumerate() {
            if let Err(e) = result {
                if self.streams[i].is_closed() && first_terminal.is_none() {
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

    /// Waits for server acknowledgment of the record or batch behind a
    /// [`MessageId`] returned from [`ingest_record`](Self::ingest_record) or
    /// [`ingest_records`](Self::ingest_records).
    ///
    /// Only the lane that owns this message can complete or fail the wait; a
    /// failure on another lane does not make an acknowledged record retryable.
    pub async fn wait_for_message_id(&self, message_id: MessageId) -> ZerobusResult<()> {
        let idx = message_id.stream_index();
        if idx >= self.streams.len() {
            return Err(ZerobusError::InvalidArgument(format!(
                "Invalid stream index {} in message id",
                idx
            )));
        }
        match self.streams[idx]
            .wait_for_offset(message_id.sub_offset())
            .await
        {
            Ok(()) => Ok(()),
            Err(e) => Err(self.handle_lane_error(idx, e).await),
        }
    }

    /// Flushes and closes all sub-streams, releasing their resources.
    ///
    /// A stored or detected terminal lane failure takes precedence; otherwise
    /// the first flush error is returned and additional ones are logged.
    /// On error, use [`get_unacked_records`](Self::get_unacked_records) to
    /// recover records that were never acknowledged.
    /// Retrying a cancelled close resumes teardown once its flush attempt has
    /// completed, retaining both the flush result and terminal lane failures.
    pub async fn close(&mut self) -> ZerobusResult<()> {
        info!("Closing MultiplexedStream");

        self.is_closed.store(true, Ordering::Relaxed);
        self.closed_token.cancel();

        if self.close_flush_result.is_none() {
            if self.failure.get().is_none() {
                if let Some(idx) = self.first_closed_stream() {
                    if let Some(error) = self.streams[idx].terminal_error().await {
                        let _ = self.failure.set(error);
                    }
                }
            }
            let mut first_error = None;
            // All lanes stay live through this flush attempt. Cache its result
            // before any supervisor is stopped or callback drain can be cancelled.
            let results = join_all(self.streams.iter().map(ZerobusStream::flush)).await;
            for (i, result) in results.into_iter().enumerate() {
                if let Err(error) = result {
                    if self.failure.get().is_none() && self.streams[i].is_closed() {
                        if let Some(cause) = self.streams[i].terminal_error().await {
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
            stream.shutdown_callbacks().await;
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

    /// Returns whether the mux is closed — either via [`close`](Self::close)
    /// or because a mux operation observed a sub-stream failure.
    pub fn is_closed(&self) -> bool {
        self.is_closed_fast()
    }

    /// Returns records that were ingested but not acknowledged.
    ///
    /// Closes the mux first to ensure all sub-streams have reached their terminal state,
    /// so results are always complete. Any error from close is swallowed — if records can
    /// still be recovered, they will be returned.
    pub async fn get_unacked_records(
        &mut self,
    ) -> ZerobusResult<impl Iterator<Item = EncodedRecord>> {
        Ok(self.get_unacked_batches().await?.into_iter().flatten())
    }

    /// Returns batches that were ingested but not acknowledged.
    ///
    /// Closes the mux first to ensure all sub-streams have reached their terminal state,
    /// so results are always complete. Any error from close is swallowed — if records can
    /// still be recovered, they will be returned.
    pub async fn get_unacked_batches(&mut self) -> ZerobusResult<Vec<EncodedBatch>> {
        let _ = self.close().await;
        let mut all_batches = Vec::new();
        for stream in &self.streams {
            all_batches.extend(stream.get_unacked_batches().await?);
        }
        Ok(all_batches)
    }
}

impl Drop for MultiplexedStream {
    fn drop(&mut self) {
        self.is_closed.store(true, Ordering::Relaxed);
        self.closed_token.cancel();
        // Fire cancellation on every sub-stream in parallel so their
        // background tasks can start unwinding concurrently. The Vec drop
        // below then runs each `ZerobusStream::Drop`, which aborts any
        // JoinHandles that haven't already exited.
        for stream in &self.streams {
            stream.signal_shutdown();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingMultiplexedCallback {
        acks: Mutex<Vec<MessageId>>,
        errors: Mutex<Vec<(MessageId, String)>>,
    }

    impl AckCallback<MessageId> for RecordingMultiplexedCallback {
        fn on_ack(&self, message_id: MessageId) {
            self.acks.lock().unwrap().push(message_id);
        }

        fn on_error(&self, message_id: MessageId, error_message: &str) {
            self.errors
                .lock()
                .unwrap()
                .push((message_id, error_message.to_string()));
        }
    }

    #[test]
    #[should_panic(expected = "MultiplexedStream requires at least one sub-stream")]
    fn test_constructor_panics_on_empty_streams() {
        MultiplexedStream::from_streams(vec![]);
    }

    #[test]
    fn test_message_id_roundtrip() {
        for stream_idx in 0..64 {
            for sub_offset in [0i64, 1, 100, 1_000_000, i64::MAX >> STREAM_BITS] {
                let id = MessageId::new(stream_idx, sub_offset);
                assert_eq!(id.stream_index(), stream_idx);
                assert_eq!(id.sub_offset(), sub_offset);
            }
        }
    }

    #[test]
    fn test_message_id_zero() {
        let id = MessageId::new(0, 0);
        assert_eq!(id.raw(), 0);
        assert_eq!(id.stream_index(), 0);
        assert_eq!(id.sub_offset(), 0);
    }

    #[test]
    fn test_message_id_different_streams_same_offset() {
        let a = MessageId::new(0, 42);
        let b = MessageId::new(1, 42);
        assert_ne!(a, b);
        assert_eq!(a.sub_offset(), b.sub_offset());
        assert_ne!(a.stream_index(), b.stream_index());
    }

    #[test]
    fn test_multiplexed_ack_callback_routes_substream_ids() {
        let callback = Arc::new(RecordingMultiplexedCallback::default());
        let stream_0 = multiplexed_ack_callback(0, callback.clone());
        let stream_1 = multiplexed_ack_callback(1, callback.clone());

        stream_0.on_ack(42);
        stream_1.on_ack(42);
        stream_1.on_error(43, "test error");

        assert_eq!(
            callback.acks.lock().unwrap().as_slice(),
            &[MessageId::new(0, 42), MessageId::new(1, 42)]
        );
        assert_eq!(
            callback.errors.lock().unwrap().as_slice(),
            &[(MessageId::new(1, 43), "test error".to_string())]
        );
    }

    #[test]
    #[should_panic(expected = "MultiplexedStream supports at most 64 sub-streams")]
    fn test_multiplexed_ack_callback_rejects_invalid_stream_index() {
        let callback = Arc::new(RecordingMultiplexedCallback::default());
        multiplexed_ack_callback(64, callback);
    }
}
