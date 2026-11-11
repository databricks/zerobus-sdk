//! Pull-based per-stream counters returned by `ZerobusStream::stats()` and
//! `ZerobusArrowStream::stats()`.
//!
//! The stream owns one [`StreamStatsState`]. Ingest, acknowledgment, recovery and
//! error paths update it with atomics; the accessor copies it into a
//! [`StreamStats`] snapshot without waiting on any task. Error writes go through
//! [`ServerErrorSender`], so every error the stream publishes is also recorded.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::SystemTime;

use tokio::sync::watch;

use crate::offset_generator::OffsetId;
use crate::ZerobusError;

/// Point-in-time view of a stream's counters.
///
/// Each field is read separately, so a snapshot taken during concurrent ingest or
/// acknowledgment can mix values from slightly different moments. Multiplexed
/// streams do not expose these counters.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct StreamStats {
    /// Records accepted by ingest and not yet acknowledged by the server. Counts
    /// records, not batches. Records that were rejected before reaching the wire
    /// are not counted.
    pub records_in_flight: u64,
    /// Offset of the most recently acknowledged batch, or `None` before the first
    /// acknowledgment. Never decreases.
    pub last_acked_offset: Option<OffsetId>,
    /// Automatic recoveries that completed successfully since the stream was
    /// created. Initial stream creation is not counted.
    pub reconnects: u64,
    /// Most recent error the stream published, or `None` if none has occurred.
    /// Kept after a successful recovery.
    pub last_error: Option<LastStreamError>,
}

/// The most recent error a stream published.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct LastStreamError {
    /// Human-readable error message, the same text as the error's `Display` output.
    pub message: String,
    /// Whether the error is retryable, as reported by `ZerobusError::is_retryable`.
    pub is_retryable: bool,
    /// When the stream recorded the error.
    pub timestamp: SystemTime,
}

/// Shared counters for one stream. `last_acked_offset` holds `-1` until the first
/// acknowledgment; offsets start at 0.
#[derive(Debug)]
pub(crate) struct StreamStatsState {
    records_in_flight: AtomicU64,
    last_acked_offset: AtomicI64,
    reconnects: AtomicU64,
    last_error: RwLock<Option<LastStreamError>>,
}

impl Default for StreamStatsState {
    fn default() -> Self {
        Self {
            records_in_flight: AtomicU64::new(0),
            last_acked_offset: AtomicI64::new(-1),
            reconnects: AtomicU64::new(0),
            last_error: RwLock::new(None),
        }
    }
}

impl StreamStatsState {
    pub(crate) fn record_admitted(&self, records: u64) {
        self.records_in_flight.fetch_add(records, Ordering::Relaxed);
    }

    /// Reverses `record_admitted` for a batch withdrawn before it reached the wire.
    #[cfg_attr(not(feature = "arrow-flight"), allow(dead_code))]
    pub(crate) fn record_withdrawn(&self, records: u64) {
        self.subtract_in_flight(records);
    }

    /// Records newly made durable on the server. Callers pass the delta since their
    /// previous acknowledgment, so partially acknowledged batches are counted exactly.
    pub(crate) fn record_acked_records(&self, records: u64) {
        self.subtract_in_flight(records);
    }

    pub(crate) fn record_acked_offset(&self, offset: OffsetId) {
        self.last_acked_offset.fetch_max(offset, Ordering::Relaxed);
    }

    pub(crate) fn record_reconnect(&self) {
        self.reconnects.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_error(&self, error: &ZerobusError) {
        let entry = LastStreamError {
            message: error.to_string(),
            is_retryable: error.is_retryable(),
            timestamp: SystemTime::now(),
        };
        *self
            .last_error
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Some(entry);
    }

    pub(crate) fn snapshot(&self) -> StreamStats {
        let last_acked = self.last_acked_offset.load(Ordering::Relaxed);
        StreamStats {
            records_in_flight: self.records_in_flight.load(Ordering::Relaxed),
            last_acked_offset: (last_acked >= 0).then_some(last_acked),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            last_error: self
                .last_error
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .clone(),
        }
    }

    // Saturates instead of wrapping, so a bookkeeping mismatch cannot report a huge count.
    fn subtract_in_flight(&self, records: u64) {
        let _ =
            self.records_in_flight
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(current.saturating_sub(records))
                });
    }
}

/// Sender for a stream's server-error watch channel that also records every
/// `Some` value in the stream's [`StreamStatsState`].
#[derive(Clone)]
pub(crate) struct ServerErrorSender {
    tx: watch::Sender<Option<ZerobusError>>,
    stats: Arc<StreamStatsState>,
}

impl ServerErrorSender {
    pub(crate) fn new(
        tx: watch::Sender<Option<ZerobusError>>,
        stats: Arc<StreamStatsState>,
    ) -> Self {
        Self { tx, stats }
    }

    pub(crate) fn send(
        &self,
        value: Option<ZerobusError>,
    ) -> Result<(), watch::error::SendError<Option<ZerobusError>>> {
        if let Some(error) = &value {
            self.stats.record_error(error);
        }
        self.tx.send(value)
    }

    #[cfg_attr(not(feature = "arrow-flight"), allow(dead_code))]
    pub(crate) fn send_replace(&self, value: Option<ZerobusError>) -> Option<ZerobusError> {
        if let Some(error) = &value {
            self.stats.record_error(error);
        }
        self.tx.send_replace(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GrpcStatus;

    fn retryable() -> ZerobusError {
        ZerobusError::StreamClosedError(GrpcStatus::unavailable("transient"))
    }

    #[test]
    fn fresh_state_reports_zeroes_and_no_offset() {
        let stats = StreamStatsState::default().snapshot();
        assert_eq!(stats.records_in_flight, 0);
        assert_eq!(stats.last_acked_offset, None);
        assert_eq!(stats.reconnects, 0);
        assert!(stats.last_error.is_none());
    }

    #[test]
    fn in_flight_counts_records_up_on_admit_and_down_on_ack() {
        let state = StreamStatsState::default();
        state.record_admitted(3);
        state.record_admitted(1);
        assert_eq!(state.snapshot().records_in_flight, 4);
        state.record_acked_records(3);
        state.record_acked_offset(0);
        let stats = state.snapshot();
        assert_eq!(stats.records_in_flight, 1);
        assert_eq!(stats.last_acked_offset, Some(0));
    }

    #[test]
    fn withdrawn_admission_is_not_in_flight() {
        let state = StreamStatsState::default();
        state.record_admitted(5);
        state.record_withdrawn(5);
        assert_eq!(state.snapshot().records_in_flight, 0);
        assert_eq!(state.snapshot().last_acked_offset, None);
    }

    #[test]
    fn in_flight_saturates_at_zero() {
        let state = StreamStatsState::default();
        state.record_admitted(2);
        state.record_acked_records(5);
        assert_eq!(state.snapshot().records_in_flight, 0);
    }

    #[test]
    fn last_acked_offset_is_monotonic() {
        let state = StreamStatsState::default();
        state.record_admitted(3);
        state.record_acked_offset(4);
        state.record_acked_offset(2);
        assert_eq!(state.snapshot().last_acked_offset, Some(4));
    }

    #[test]
    fn reconnects_count_successful_recoveries() {
        let state = StreamStatsState::default();
        state.record_reconnect();
        state.record_reconnect();
        assert_eq!(state.snapshot().reconnects, 2);
    }

    #[test]
    fn last_error_keeps_message_retryability_and_is_replaced() {
        let state = StreamStatsState::default();
        state.record_error(&retryable());
        let first = state.snapshot().last_error.expect("error recorded");
        assert!(first.message.contains("transient"));
        assert!(first.is_retryable);

        state.record_error(&ZerobusError::InvalidArgument("bad".into()));
        let second = state.snapshot().last_error.expect("error recorded");
        assert!(second.message.contains("bad"));
        assert!(!second.is_retryable);
        assert!(second.timestamp >= first.timestamp);
    }

    #[tokio::test]
    async fn server_error_sender_records_some_but_not_none() {
        let stats = Arc::new(StreamStatsState::default());
        let (tx, rx) = watch::channel(None);
        let sender = ServerErrorSender::new(tx, Arc::clone(&stats));

        sender.send(None).unwrap();
        assert!(stats.snapshot().last_error.is_none());

        sender.send(Some(retryable())).unwrap();
        assert!(stats.snapshot().last_error.is_some());
        assert!(rx.borrow().is_some());

        sender.send_replace(None);
        assert!(rx.borrow().is_none());
        // Clearing the watch value keeps the recorded error.
        assert!(stats.snapshot().last_error.is_some());
    }
}
