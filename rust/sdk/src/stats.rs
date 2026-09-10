//! Pluggable stream telemetry.
//!
//! A [`StatsExporter`] receives typed [`StreamStat`] events from a stream (batch
//! sends with byte sizes, acknowledgements, reconnects) and routes them into
//! whatever metrics system the caller uses. Register one on the stream builder
//! with `.stats_exporter(...)`.
//!
//! **Beta**: currently emitted only by Arrow Flight streams (behind the
//! `arrow-flight` feature). The event set is `#[non_exhaustive]` and may grow.

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
    /// Number of rows in the batch.
    pub records: u64,
    /// Approximate on-wire size: the batch's FlightData payload — its data messages
    /// (`data_header + data_body + app_metadata`), after IPC compression when enabled.
    /// This is the Arrow payload only; it excludes gRPC/HTTP2/TLS framing, the one-time
    /// schema message, and the flight descriptor, so it is a lower bound on the actual
    /// network bytes.
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
/// 1000 rows has a single offset. Byte sizes are therefore per batch.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum StreamStat {
    /// A complete batch transmission was encoded and yielded to the transport.
    /// Emitted with its final data frame, including retransmissions during recovery,
    /// so a later cancellation or drop cannot discard the event. A transmission
    /// canceled before its final frame does not emit this event.
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
        /// were incomplete. Filtering for `attempt == 0` can therefore omit a batch
        /// entirely; an original-batch size sample is not guaranteed for every offset.
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
    /// The stream reconnected. `reason` separates a routine server-requested rotation
    /// from recovery after a transient failure.
    Reconnected {
        /// Why the reconnect happened.
        reason: ReconnectReason,
    },
}

/// Why a stream reconnected.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum ReconnectReason {
    /// Recovery after a transient, retryable failure; carries the error that triggered it.
    TransientFailure(crate::errors::ZerobusError),
    /// A server-requested graceful stream rotation — routine, not an error.
    ServerRotation,
}

/// Sink for stream telemetry. Register with `.stats_exporter(...)` on the builder.
///
/// `record` runs **inline inside the stream's IO poll loop** (e.g. tonic's
/// request-body `poll_next`), so it must return immediately: do only atomic updates
/// or a non-blocking `try_send`. Blocking, heavy allocation, or awaiting stalls
/// ingestion for every stream sharing that task. Use [`ChannelExporter`] to hand
/// stats to another task for anything heavier.
pub trait StatsExporter: Send + Sync {
    /// Handle one telemetry event. Must not block, allocate heavily, or panic.
    fn record(&self, stat: StreamStat);
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
/// returns it with the receiver to drain elsewhere.
pub fn channel_exporter(capacity: usize) -> (Arc<ChannelExporter>, mpsc::Receiver<StreamStat>) {
    let (tx, rx) = mpsc::channel(capacity.max(1));
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
        let (exporter, mut rx) = channel_exporter(1);
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
}
