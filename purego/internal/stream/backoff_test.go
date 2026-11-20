package stream

import (
	"math"
	"testing"
	"time"
)

const backoffSamples = 200

func assertBackoffInRange(t *testing.T, base time.Duration, retryIndex int, floor time.Duration) {
	t.Helper()
	for range backoffSamples {
		got := recoveryBackoffDelay(base, retryIndex)
		if got < floor || got >= 2*floor {
			t.Fatalf("retry %d with base %v waited %v, want [%v, %v)",
				retryIndex, base, got, floor, 2*floor)
		}
	}
}

func TestRecoveryBackoffDoublesAndNeverDropsBelowBase(t *testing.T) {
	assertBackoffInRange(t, time.Second, 0, time.Second)
	assertBackoffInRange(t, time.Second, 1, 2*time.Second)
	assertBackoffInRange(t, time.Second, 2, 4*time.Second)
	assertBackoffInRange(t, time.Second, 3, 8*time.Second)
}

func TestRecoveryBackoffStopsDoublingAtCap(t *testing.T) {
	assertBackoffInRange(t, time.Second, 4, maxRecoveryBackoff)
	assertBackoffInRange(t, time.Second, 1000, maxRecoveryBackoff)
	assertBackoffInRange(t, time.Nanosecond, 64, maxRecoveryBackoff)
}

func TestRecoveryBackoffKeepsBaseAboveCap(t *testing.T) {
	assertBackoffInRange(t, time.Minute, 0, time.Minute)
	assertBackoffInRange(t, time.Minute, 5, time.Minute)
}

func TestRecoveryBackoffNonPositiveBaseNeverWaits(t *testing.T) {
	for _, base := range []time.Duration{0, -time.Second} {
		if got := recoveryBackoffDelay(base, 3); got != 0 {
			t.Fatalf("recoveryBackoffDelay(%v, 3) = %v, want 0", base, got)
		}
	}
}

func TestRecoveryBackoffSaturates(t *testing.T) {
	if got := recoveryBackoffDelay(math.MaxInt64, math.MaxInt); got != math.MaxInt64 {
		t.Fatalf("recoveryBackoffDelay(MaxInt64, MaxInt) = %v, want MaxInt64", got)
	}
}

func TestRecoveryBackoffIsJittered(t *testing.T) {
	seen := map[time.Duration]struct{}{}
	for range backoffSamples {
		seen[recoveryBackoffDelay(time.Second, 0)] = struct{}{}
	}
	if len(seen) < 2 {
		t.Fatalf("%d samples drawn from a 1s range were all equal", backoffSamples)
	}
}
