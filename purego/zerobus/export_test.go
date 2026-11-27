package zerobus

import (
	"context"
	"time"

	"github.com/databricks/zerobus-sdk/purego/internal/transport"
)

// Test-only accessors for unexported helpers and injection points.

// GRPCTarget exposes grpcTarget for endpoint-normalization tests.
func GRPCTarget(endpoint string) (string, error) { return grpcTarget(endpoint) }

// NewMessageID exposes newMessageID for MessageID packing tests.
func NewMessageID(streamIndex int, offset int64) MessageID { return newMessageID(streamIndex, offset) }

// MessageIDStreamIndex exposes the lane index of id.
func MessageIDStreamIndex(id MessageID) int { return id.streamIndex() }

// MessageIDOffset exposes the lane offset of id.
func MessageIDOffset(id MessageID) int64 { return id.offset() }

// MultiplexedLaneAckCallback exposes multiplexedAckCallback, the AckCallback of
// one multiplexed lane.
func MultiplexedLaneAckCallback(streamIndex int, cb MultiplexedAckCallback) AckCallback {
	return multiplexedAckCallback(streamIndex, cb)
}

// NewWithConn builds an SDK around an already-dialed transport connection,
// bypassing New's dialing so tests can point the SDK at an in-memory server.
func NewWithConn(conn *transport.Conn, zerobusEndpoint, ucEndpoint string, opts ...Option) *SDK {
	var cfg sdkConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return newSDK(conn, zerobusEndpoint, ucEndpoint, cfg)
}

// NewWithDial lets connection-mode tests use a local gRPC server while
// exercising the SDK and Stream APIs.
func NewWithDial(dial func() (*transport.Conn, error), zerobusEndpoint, ucEndpoint string, opts ...Option) (*SDK, error) {
	cfg := defaultSDKConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	var conn *transport.Conn
	if !cfg.connectionPerStream {
		var err error
		conn, err = dial()
		if err != nil {
			return nil, err
		}
	}
	sdk := newSDK(conn, zerobusEndpoint, ucEndpoint, cfg)
	sdk.dialConn = dial
	return sdk, nil
}

// OpenStreamCount reports how many streams the SDK still tracks for Close, so
// tests can assert that Stream.Close deregisters itself.
func (s *SDK) OpenStreamCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.streams)
}

// ResolveStreamConfig applies the given options and returns the resolved record
// type, descriptor, and a subset of the core config for assertion.
func ResolveStreamConfig(opts ...StreamOption) (recordType int32, descriptor []byte, maxInflight int, recovery RecoverySetting, waitReady bool) {
	sc := defaultStreamConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&sc)
		}
	}
	return int32(sc.recordType), sc.descriptor, sc.cfg.MaxInflight, sc.cfg.Recovery, sc.waitReady
}

// ResolveStreamTuning returns recovery and batch tuning from options.
func ResolveStreamTuning(opts ...StreamOption) (
	recoveryTimeout, recoveryBackoff, lackOfAckTimeout time.Duration,
	maxBatchRecords int,
	streamPausedMaxWait *time.Duration,
) {
	sc := defaultStreamConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&sc)
		}
	}
	return sc.cfg.RecoveryTimeout, sc.cfg.RecoveryBackoff, sc.cfg.LackOfAckTimeout,
		sc.cfg.MaxBatchRecords, sc.cfg.StreamPausedMaxWait
}

// MultiplexedJitter runs the default wait before a multiplexed lane opens.
func MultiplexedJitter(ctx context.Context, lane int) error {
	return defaultMultiplexedJitter(ctx, lane)
}

// SleepContext exposes sleepContext.
func SleepContext(ctx context.Context, d time.Duration) error { return sleepContext(ctx, d) }

// WithMultiplexedJitter replaces the wait before each lane of a multiplexed
// stream opens.
func WithMultiplexedJitter(wait func(ctx context.Context, lane int) error) StreamOption {
	return func(c *streamConfig) { c.muxJitter = wait }
}

// SetBeforeLaneClose sets a hook that runs right before Close closes each
// lane. The returned function removes it.
func (m *MultiplexedStream) SetBeforeLaneClose(hook func(lane int)) (remove func()) {
	m.hooks.beforeLaneClose.Store(&hook)
	return func() { m.hooks.beforeLaneClose.Store(nil) }
}

// ResolveMultiplexedStreamConfig applies the multiplexed validation and budget
// split, and returns the per-lane MaxInflight.
func ResolveMultiplexedStreamConfig(tableName string, streamCount int, opts ...StreamOption) (laneMaxInflight int, err error) {
	sc, err := multiplexedStreamConfig(tableName, streamCount, opts)
	return sc.cfg.MaxInflight, err
}
