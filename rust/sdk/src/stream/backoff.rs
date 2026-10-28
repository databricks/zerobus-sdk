//! Recovery backoff shared by the gRPC and Arrow Flight transports.
//!
//! The wait before the next connection attempt grows exponentially with the number
//! of consecutive failures, with jitter, up to a cap. A retry delay that the server
//! attaches to its rejection raises the wait. Only the end of a healthy connection
//! resets the count, so a reconnect that succeeds and then fails again waits longer.

use std::time::Duration;

use rand::Rng;
use tonic_types::StatusExt;

use crate::ZerobusError;

/// A connection must stay up at least this long to reset the consecutive-failure count.
const HEALTHY_CONNECTION_MIN_UPTIME: Duration = Duration::from_secs(30);

/// Growth of the delay per consecutive failure.
const BACKOFF_FACTOR: u64 = 2;

/// Largest jitter, as a share of the delay. Jitter only adds time, so no wait is
/// shorter than the configured backoff.
const MAX_JITTER_SHARE: f64 = 0.5;

/// Counts consecutive failures across reconnects and computes the wait before the
/// next connection attempt.
pub(super) struct RecoveryBackoff {
    base_ms: u64,
    max_ms: u64,
    consecutive_failures: u32,
}

impl RecoveryBackoff {
    pub(super) fn new(base_ms: u64, max_ms: u64) -> Self {
        Self {
            base_ms,
            max_ms,
            consecutive_failures: 0,
        }
    }

    /// Resets the failure count if the connection that just ended was healthy, and
    /// returns whether it was. Uptime alone is not enough: a server that accepts the
    /// stream but rejects its data never acknowledges it. An idle stream gets no
    /// acknowledgements, so a connection also counts if nothing that it sent is still
    /// `pending` acknowledgement at its end.
    pub(super) fn connection_ended(
        &mut self,
        uptime: Duration,
        acknowledged: bool,
        pending: bool,
    ) -> bool {
        let healthy = (acknowledged || !pending) && uptime >= HEALTHY_CONNECTION_MIN_UPTIME;
        if healthy {
            self.consecutive_failures = 0;
        }
        healthy
    }

    /// Adds one consecutive failure and returns the wait before the next attempt.
    /// `error` is the failure whose server retry delay, if any, applies.
    pub(super) fn next_delay(&mut self, error: Option<&ZerobusError>) -> Duration {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        backoff_delay(
            self.base_ms,
            self.max_ms,
            self.consecutive_failures,
            error.and_then(server_retry_delay),
            rand::rng().random_range(0.0..=1.0),
        )
    }
}

/// The `google.rpc.RetryInfo` delay that the server attached to its rejection.
fn server_retry_delay(error: &ZerobusError) -> Option<Duration> {
    match error {
        ZerobusError::CreateStreamError(status) | ZerobusError::StreamClosedError(status) => {
            status.get_details_retry_info()?.retry_delay
        }
        _ => None,
    }
}

/// Returns the wait after the `failures`-th consecutive failure.
///
/// The delay starts at `base_ms` and doubles with each failure, up to the larger of
/// `max_ms` and `base_ms`. `jitter`, from 0 to 1, then adds up to half of the delay, so
/// that clients that fail at the same moment do not all reconnect at the same moment.
/// A server retry delay, up to the same cap, is the minimum wait. Jitter does not apply
/// to it.
fn backoff_delay(
    base_ms: u64,
    max_ms: u64,
    failures: u32,
    server_delay: Option<Duration>,
    jitter: f64,
) -> Duration {
    let cap = Duration::from_millis(max_ms.max(base_ms));
    let growth = BACKOFF_FACTOR.saturating_pow(failures.saturating_sub(1));
    let delay = Duration::from_millis(base_ms.saturating_mul(growth)).min(cap);
    let delay = delay + delay.mul_f64(MAX_JITTER_SHARE * jitter);
    server_delay.map_or(delay, |server_delay| delay.max(server_delay.min(cap)))
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE_MS: u64 = 2_000;
    const MAX_MS: u64 = 30_000;

    #[test]
    fn first_failure_waits_between_base_and_one_and_a_half_times_base() {
        assert_eq!(
            backoff_delay(BASE_MS, MAX_MS, 1, None, 0.0),
            Duration::from_millis(BASE_MS)
        );
        assert_eq!(
            backoff_delay(BASE_MS, MAX_MS, 1, None, 1.0),
            Duration::from_millis(BASE_MS * 3 / 2)
        );
    }

    #[test]
    fn zero_base_does_not_wait() {
        assert_eq!(backoff_delay(0, MAX_MS, 3, None, 1.0), Duration::ZERO);
    }

    #[test]
    fn delay_doubles_with_each_failure() {
        assert_eq!(
            backoff_delay(BASE_MS, MAX_MS, 3, None, 0.0),
            Duration::from_millis(4 * BASE_MS)
        );
    }

    #[test]
    fn large_failure_count_is_capped_without_overflow() {
        assert_eq!(
            backoff_delay(BASE_MS, MAX_MS, u32::MAX, None, 1.0),
            Duration::from_millis(MAX_MS * 3 / 2)
        );
    }

    #[test]
    fn max_below_base_keeps_the_delay_fixed() {
        assert_eq!(
            backoff_delay(BASE_MS, BASE_MS / 2, 3, None, 0.0),
            Duration::from_millis(BASE_MS)
        );
    }

    #[test]
    fn server_retry_delay_is_capped() {
        assert_eq!(
            backoff_delay(BASE_MS, MAX_MS, 1, Some(Duration::from_secs(3_600)), 0.0),
            Duration::from_millis(MAX_MS)
        );
    }

    #[test]
    fn short_server_retry_delay_keeps_the_backoff() {
        assert_eq!(
            backoff_delay(BASE_MS, MAX_MS, 1, Some(Duration::from_secs(1)), 0.0),
            Duration::from_millis(BASE_MS)
        );
    }

    #[test]
    fn server_retry_delay_is_not_jittered() {
        let server_delay = Duration::from_secs(5);
        assert_eq!(
            backoff_delay(BASE_MS, MAX_MS, 1, Some(server_delay), 1.0),
            server_delay
        );
    }

    #[test]
    fn healthy_connection_resets_failure_count() {
        let mut backoff = RecoveryBackoff {
            base_ms: BASE_MS,
            max_ms: MAX_MS,
            consecutive_failures: 3,
        };
        assert!(backoff.connection_ended(HEALTHY_CONNECTION_MIN_UPTIME, true, true));
        assert_eq!(backoff.consecutive_failures, 0);
    }

    #[test]
    fn idle_connection_resets_failure_count() {
        let mut backoff = RecoveryBackoff {
            base_ms: BASE_MS,
            max_ms: MAX_MS,
            consecutive_failures: 3,
        };
        assert!(backoff.connection_ended(HEALTHY_CONNECTION_MIN_UPTIME, false, false));
        assert_eq!(backoff.consecutive_failures, 0);
    }

    #[test]
    fn unacknowledged_connection_keeps_failure_count() {
        let mut backoff = RecoveryBackoff {
            base_ms: BASE_MS,
            max_ms: MAX_MS,
            consecutive_failures: 3,
        };
        assert!(!backoff.connection_ended(HEALTHY_CONNECTION_MIN_UPTIME, false, true));
        assert_eq!(backoff.consecutive_failures, 3);
    }

    #[test]
    fn short_connection_keeps_failure_count() {
        let mut backoff = RecoveryBackoff {
            base_ms: BASE_MS,
            max_ms: MAX_MS,
            consecutive_failures: 3,
        };
        let uptime = HEALTHY_CONNECTION_MIN_UPTIME - Duration::from_millis(1);
        assert!(!backoff.connection_ended(uptime, true, false));
        assert_eq!(backoff.consecutive_failures, 3);
    }
}
