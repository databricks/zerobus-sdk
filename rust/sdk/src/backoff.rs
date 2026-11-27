//! Delay between stream creation and recovery attempts.
//!
//! Clients that fail together, for example when the server rate-limits stream
//! creation, retry together if they all wait the same fixed interval, and then
//! fail together again. Doubling the delay after each failure and adding a random
//! amount to it spreads those retries out over time.

use std::time::Duration;

use rand::Rng;

/// The un-jittered delay stops doubling at this value, unless the configured
/// `recovery_backoff_ms` is already larger.
pub(crate) const MAX_RECOVERY_BACKOFF_MS: u64 = 15_000;

/// Returns how long to wait before retry number `retry_index` (0 for the first retry).
///
/// The un-jittered delay is `base_ms * 2^retry_index`, capped at the larger of
/// `base_ms` and [`MAX_RECOVERY_BACKOFF_MS`]. A uniformly random jitter of up to that
/// delay is added, so the result lies in `[delay, 2 * delay)`. The result is never
/// shorter than `base_ms`, and a `base_ms` of 0 always gives zero.
pub(crate) fn recovery_backoff_delay(base_ms: u64, retry_index: u32) -> Duration {
    let cap_ms = base_ms.max(MAX_RECOVERY_BACKOFF_MS);
    let factor = 1u64.checked_shl(retry_index).unwrap_or(u64::MAX);
    let delay_ms = base_ms.saturating_mul(factor).min(cap_ms);
    let jitter_ms = if delay_ms == 0 {
        0
    } else {
        rand::rng().random_range(0..delay_ms)
    };
    Duration::from_millis(delay_ms.saturating_add(jitter_ms))
}

/// Returns the delays for up to `retries` retries, computed by
/// [`recovery_backoff_delay`] for retry indices starting at `first_retry_index`.
/// Jitter is drawn lazily, as each delay is taken.
pub(crate) fn recovery_backoff(
    base_ms: u64,
    first_retry_index: u32,
    retries: u32,
) -> impl Iterator<Item = Duration> {
    (0..retries).map(move |n| recovery_backoff_delay(base_ms, first_retry_index.saturating_add(n)))
}

/// Returns how long to wait before reconnecting after a server-initiated stream
/// rotation. A rotation is routine, not a failure, so this keeps the average wait
/// at `base_ms`, as before jitter was added, and only spreads it uniformly over
/// `[base_ms / 2, 1.5 * base_ms]` so that streams rotated together don't all
/// reconnect together. A `base_ms` of 0 gives zero.
#[cfg(feature = "arrow-flight")]
pub(crate) fn rotation_backoff_delay(base_ms: u64) -> Duration {
    let jitter_ms = rand::rng().random_range(0..=base_ms);
    Duration::from_millis((base_ms / 2).saturating_add(jitter_ms))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLES: usize = 200;

    fn assert_in_range(base_ms: u64, retry_index: u32, expected_floor_ms: u64) {
        for _ in 0..SAMPLES {
            let delay = recovery_backoff_delay(base_ms, retry_index);
            assert!(
                delay >= Duration::from_millis(expected_floor_ms)
                    && delay < Duration::from_millis(2 * expected_floor_ms),
                "retry {retry_index} with base {base_ms}ms waited {delay:?}, \
                 expected [{expected_floor_ms}ms, {}ms)",
                2 * expected_floor_ms
            );
        }
    }

    #[test]
    fn delay_doubles_per_retry_and_never_drops_below_base() {
        assert_in_range(1_000, 0, 1_000);
        assert_in_range(1_000, 1, 2_000);
        assert_in_range(1_000, 2, 4_000);
        assert_in_range(1_000, 3, 8_000);
    }

    #[test]
    fn delay_stops_doubling_at_cap() {
        assert_in_range(1_000, 4, MAX_RECOVERY_BACKOFF_MS);
        assert_in_range(1_000, 30, MAX_RECOVERY_BACKOFF_MS);
    }

    #[test]
    fn base_above_cap_is_not_reduced() {
        assert_in_range(60_000, 0, 60_000);
        assert_in_range(60_000, 5, 60_000);
    }

    #[test]
    fn zero_base_never_waits() {
        for retry_index in [0, 1, 10, u32::MAX] {
            assert_eq!(recovery_backoff_delay(0, retry_index), Duration::ZERO);
        }
    }

    #[test]
    fn extreme_inputs_saturate_without_panicking() {
        assert_eq!(
            recovery_backoff_delay(u64::MAX, u32::MAX),
            Duration::from_millis(u64::MAX)
        );
        assert_in_range(1, 63, MAX_RECOVERY_BACKOFF_MS);
        assert_in_range(1, 64, MAX_RECOVERY_BACKOFF_MS);
    }

    #[test]
    fn delay_is_jittered() {
        let delays: std::collections::HashSet<_> = (0..SAMPLES)
            .map(|_| recovery_backoff_delay(1_000, 0))
            .collect();
        assert!(
            delays.len() > 1,
            "{SAMPLES} samples drawn from a 1000ms range were all equal"
        );
    }

    #[test]
    fn strategy_yields_one_delay_per_retry() {
        assert_eq!(recovery_backoff(1_000, 0, 0).count(), 0);
        let delays: Vec<_> = recovery_backoff(1_000, 0, 4).collect();
        assert_eq!(delays.len(), 4);
        for (retry_index, delay) in delays.into_iter().enumerate() {
            let floor = Duration::from_millis(1_000 << retry_index);
            assert!(delay >= floor && delay < 2 * floor);
        }
    }

    #[test]
    fn strategy_can_start_at_a_later_retry() {
        let delays: Vec<_> = recovery_backoff(1_000, 1, 3).collect();
        assert_eq!(delays.len(), 3);
        for (n, delay) in delays.into_iter().enumerate() {
            let floor = Duration::from_millis(1_000 << (n + 1));
            assert!(delay >= floor && delay < 2 * floor);
        }
        assert_eq!(recovery_backoff(1_000, u32::MAX, 2).count(), 2);
    }

    #[cfg(feature = "arrow-flight")]
    #[test]
    fn rotation_delay_averages_base_and_is_jittered() {
        let delays: Vec<_> = (0..SAMPLES)
            .map(|_| rotation_backoff_delay(2_000))
            .collect();
        for delay in &delays {
            assert!(
                *delay >= Duration::from_millis(1_000) && *delay <= Duration::from_millis(3_000),
                "rotation waited {delay:?}, expected [1000ms, 3000ms]"
            );
        }
        let distinct: std::collections::HashSet<_> = delays.iter().collect();
        assert!(
            distinct.len() > 1,
            "{SAMPLES} rotation delays were all equal"
        );
        assert_eq!(rotation_backoff_delay(0), Duration::ZERO);
        // Saturates instead of overflowing.
        let _ = rotation_backoff_delay(u64::MAX);
    }
}
