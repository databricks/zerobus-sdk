package stream

import (
	"math"
	"math/rand/v2"
	"time"
)

// maxRecoveryBackoff caps the un-jittered reconnect delay, unless
// Config.RecoveryBackoff is already larger. It matches MAX_RECOVERY_BACKOFF_MS
// in rust/sdk/src/backoff.rs.
const maxRecoveryBackoff = 15 * time.Second

// recoveryBackoffDelay returns how long to wait before retry number retryIndex
// (0 for the first retry). Streams that fail together, for example when the
// server rate-limits stream creation, would retry together after a fixed
// interval; doubling the delay and adding a random amount spreads them out.
//
// The un-jittered delay is base doubled retryIndex times, capped at the larger
// of base and maxRecoveryBackoff. A uniformly random jitter of up to that delay
// is added, so the result lies in [delay, 2*delay). It is never shorter than
// base, and a non-positive base gives zero. This matches
// recovery_backoff_delay in rust/sdk/src/backoff.rs.
func recoveryBackoffDelay(base time.Duration, retryIndex int) time.Duration {
	if base <= 0 {
		return 0
	}
	limit := max(base, maxRecoveryBackoff)
	delay := base
	for i := 0; i < retryIndex && delay < limit; i++ {
		if delay > limit/2 {
			delay = limit
		} else {
			delay *= 2
		}
	}
	jitter := time.Duration(rand.Int64N(int64(delay)))
	if delay > math.MaxInt64-jitter {
		return math.MaxInt64
	}
	return delay + jitter
}
