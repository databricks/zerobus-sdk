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

use std::sync::Arc;

mod core;
pub(crate) mod lane;
use core::MuxCore;

use crate::{
    AckCallback, DynamicRecord, EncodedBatch, EncodedRecord, MessageDescriptor, OffsetId,
    PreparedInput, ZerobusResult, ZerobusStream,
};

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
    core: MuxCore<ZerobusStream>,
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
        Self {
            core: MuxCore::from_streams(streams),
        }
    }

    /// Returns the schema descriptor configured with [`crate::StreamBuilder::dynamic_proto`].
    /// Returns an error if this is not a dynamic-protobuf stream.
    pub fn message_descriptor(&self) -> ZerobusResult<MessageDescriptor> {
        self.core.first().message_descriptor()
    }

    /// Creates an empty record using this mux's dynamic-protobuf schema.
    pub fn new_record(&self) -> ZerobusResult<DynamicRecord> {
        self.core.first().new_record()
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
        self.core
            .ingest(|stream| stream.prepare_record(payload))
            .await
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
        self.core.check_closed()?;
        let records: Vec<T> = payload.into_iter().collect();
        if records.is_empty() {
            return Ok(None);
        }
        self.core
            .ingest(|stream| stream.prepare_records(records))
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
        self.core.flush().await
    }

    /// Waits for server acknowledgment of the record or batch behind a
    /// [`MessageId`] returned from [`ingest_record`](Self::ingest_record) or
    /// [`ingest_records`](Self::ingest_records).
    ///
    /// Only the lane that owns this message can complete or fail the wait; a
    /// failure on another lane does not make an acknowledged record retryable.
    pub async fn wait_for_message_id(&self, message_id: MessageId) -> ZerobusResult<()> {
        self.core.wait_for_message_id(message_id).await
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
        self.core.close().await
    }

    /// Returns whether the mux is closed — either via [`close`](Self::close)
    /// or because a mux operation observed a sub-stream failure.
    pub fn is_closed(&self) -> bool {
        self.core.is_closed()
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
        self.core.get_unacked_batches().await
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
