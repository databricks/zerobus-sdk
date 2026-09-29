//! Pluggable stream telemetry.
//!
//! A [`StatsExporter`] receives typed [`StreamStat`] events from a stream (batch
//! sends with byte sizes, acknowledgements, reconnects) and routes them into
//! whatever metrics system the caller uses. Register one on the stream builder
//! with `.stats_exporter(...)`.
//!
//! **Beta**: currently emitted only by Arrow Flight streams (behind the
//! `arrow-flight` feature). The event set is `#[non_exhaustive]` and may grow.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::offset_generator::OffsetId;

/// Byte-size and count stats for an encoded batch.
///
/// Payload only — the offset that identifies the batch is carried separately on
/// [`StreamStat::BatchSent`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchStats {
    /// Rows in this transmission, which may be only an unacknowledged suffix on retry.
    pub records: u64,
    /// Approximate wire payload size: the batch's FlightData data messages
    /// (`data_header + data_body + app_metadata`), after IPC compression when enabled.
    /// Excludes protobuf field overhead, gRPC/HTTP2/TLS framing, the schema message,
    /// and the flight descriptor. Counts payload yielded to the transport, which may
    /// be discarded before reaching the network.
    pub approximate_wire_bytes: u64,
    /// IPC buffer bytes before compression for this transmission, including
    /// dictionary buffers when emitted. Follows the encoder's treatment of
    /// slices and shared buffers. Excludes IPC metadata, alignment padding, and
    /// compression length prefixes; this is not a heap-memory measurement.
    pub uncompressed_bytes: u64,
}

impl BatchStats {
    /// Constructs a `BatchStats`. Provided because the struct is `#[non_exhaustive]`,
    /// so callers in other crates cannot build one with a field literal.
    pub fn new(records: u64, approximate_wire_bytes: u64, uncompressed_bytes: u64) -> Self {
        Self {
            records,
            approximate_wire_bytes,
            uncompressed_bytes,
        }
    }
}

/// A telemetry event emitted by a stream. Owned and `Clone`, so an event can be
/// forwarded across a channel directly (see [`ChannelExporter`]). Not `Copy` or `Eq` —
/// [`Reconnected`](Self::Reconnected) carries a [`ZerobusError`](crate::ZerobusError),
/// which is neither.
///
/// An *offset* identifies one ingest call (one batch), not one row — a batch of
/// 1000 rows has a single offset. Offsets and attempts are scoped to one stream
/// instance; events carry no stream identifier. Exporters may drop events, so a
/// consumer may observe a `BatchAcked` without a corresponding `BatchSent`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum StreamStat {
    /// A complete batch transmission was encoded and yielded to the transport.
    /// Emitted with its final data frame, including retransmissions during recovery,
    /// so a later cancellation or drop cannot discard the event. A transmission
    /// canceled before its final frame does not emit this event.
    ///
    /// If IPC statistics cannot be read, the SDK logs a warning and omits the event
    /// for that transmission. Ingestion and attempt counting continue.
    ///
    /// Each event counts all frames emitted by that completed transmission. A retry
    /// contains the whole batch if no rows were acknowledged, otherwise only the
    /// unacknowledged suffix. Summing events counts completed transmissions and
    /// excludes bytes emitted by earlier incomplete attempts.
    BatchSent {
        /// Offset assigned to the batch by `ingest_batch`.
        offset: OffsetId,
        /// Transmission count for this offset: `0` is the first send; `> 0` is a
        /// retransmit after a reconnect. Batches buffered during recovery keep `0`
        /// for their first send; preparing or queuing a replay without emitting a
        /// data frame does not advance the count.
        ///
        /// The first event for an offset can have `attempt > 0` if earlier attempts
        /// were incomplete or their statistics could not be read. Filtering for
        /// `attempt == 0` can therefore omit a batch entirely; an original-batch size
        /// sample is not guaranteed for every offset.
        attempt: u32,
        /// Byte-size and row-count stats for the batch.
        stats: BatchStats,
    },
    /// A batch was durably acknowledged by the server. Pure durability signal —
    /// byte sizes are reported by [`BatchSent`](Self::BatchSent).
    BatchAcked {
        /// Offset assigned to the batch by `ingest_batch`.
        offset: OffsetId,
    },
    /// An automatic reconnect completed setup, queued replay, and published its sender.
    /// Replay `BatchSent` events may arrive before this event. Initial creation and
    /// manual stream recreation do not emit it.
    Reconnected {
        /// Why the reconnect happened.
        reason: ReconnectReason,
    },
}

/// Why a stream reconnected.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum ReconnectReason {
    /// The failure that triggered the successful reconnect attempt. This can include
    /// an authentication rejection recovered by refreshing credentials.
    TransientFailure(crate::errors::ZerobusError),
    /// A server-requested graceful stream rotation — routine, not an error.
    ServerRotation,
}

/// Sink for stream telemetry. Register with `.stats_exporter(...)` on the builder.
///
/// `record` runs **inline on the stream's IO tasks**, which may call it concurrently.
/// Return immediately: do only atomic updates or a non-blocking `try_send`.
/// Blocking or heavy allocation stalls ingestion or acknowledgment processing.
/// Use [`ChannelExporter`] to hand stats to another task for anything heavier.
pub trait StatsExporter: Send + Sync {
    /// Handle one telemetry event. Must not block or allocate heavily.
    /// An unwinding panic is caught by the SDK, logged, and the event is dropped.
    fn record(&self, stat: StreamStat);
}

/// Delivers one event to `exporter`, isolating a panic in a misbehaving `record`.
/// `record` runs on the stream's IO task, so an unwinding panic there would otherwise
/// tear the stream down; instead it is caught, logged, and the event dropped.
pub(crate) fn dispatch(exporter: &dyn StatsExporter, stat: StreamStat) {
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| exporter.record(stat)));
    if caught.is_err() {
        tracing::warn!("stats exporter panicked in record(); dropping the event");
    }
}

/// Lets an `Arc`-wrapped exporter (e.g. the one from [`channel_exporter`]) be
/// registered directly while the caller keeps a clone to observe it.
impl<T: StatsExporter + ?Sized> StatsExporter for Arc<T> {
    fn record(&self, stat: StreamStat) {
        (**self).record(stat)
    }
}

/// A [`StatsExporter`] that forwards events to a bounded channel, dropping (and
/// counting) events when the channel is full or closed so telemetry never blocks
/// ingestion. Drain the paired receiver from [`channel_exporter`].
pub struct ChannelExporter {
    tx: mpsc::Sender<StreamStat>,
    dropped: AtomicU64,
}

impl ChannelExporter {
    /// Number of events dropped because the channel was full or closed.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl StatsExporter for ChannelExporter {
    fn record(&self, stat: StreamStat) {
        if self.tx.try_send(stat).is_err() {
            // Full or closed: drop. Stats are best-effort; never block ingest.
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Creates a [`ChannelExporter`] with a bounded buffer of `capacity` events and
/// returns it with the receiver to drain concurrently. [`NonZeroUsize`] prevents
/// a zero-capacity channel. The receiver ends after all exporter clones, including
/// those owned by streams and their IO tasks, are dropped and queued events are drained.
///
/// # Panics
///
/// Panics if `capacity` exceeds [`tokio::sync::Semaphore::MAX_PERMITS`].
pub fn channel_exporter(
    capacity: NonZeroUsize,
) -> (Arc<ChannelExporter>, mpsc::Receiver<StreamStat>) {
    let (tx, rx) = mpsc::channel(capacity.get());
    (
        Arc::new(ChannelExporter {
            tx,
            dropped: AtomicU64::new(0),
        }),
        rx,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn channel_exporter_delivers_and_drops_when_full() {
        let (exporter, mut rx) = channel_exporter(std::num::NonZeroUsize::new(1).unwrap());
        exporter.record(StreamStat::BatchSent {
            offset: 7,
            attempt: 0,
            stats: BatchStats {
                records: 3,
                approximate_wire_bytes: 100,
                uncompressed_bytes: 120,
            },
        });
        // Buffer (cap 1) is now full; this one is dropped and counted.
        exporter.record(StreamStat::Reconnected {
            reason: ReconnectReason::ServerRotation,
        });

        // StreamStat is not `Eq` (it can carry a `ZerobusError`), so match on fields.
        match rx.recv().await.unwrap() {
            StreamStat::BatchSent {
                offset,
                attempt,
                stats,
            } => {
                assert_eq!(offset, 7);
                assert_eq!(attempt, 0);
                assert_eq!(
                    stats,
                    BatchStats {
                        records: 3,
                        approximate_wire_bytes: 100,
                        uncompressed_bytes: 120,
                    }
                );
            }
            other => panic!("expected BatchSent, got {other:?}"),
        }
        assert_eq!(exporter.dropped(), 1);
    }

    #[test]
    fn dispatch_isolates_a_panicking_exporter() {
        struct Panicking;
        impl StatsExporter for Panicking {
            fn record(&self, _stat: StreamStat) {
                panic!("boom");
            }
        }
        // dispatch must swallow the panic rather than unwind into the IO task.
        let exporter = Panicking;
        dispatch(&exporter, StreamStat::BatchAcked { offset: 1 });
    }
}
