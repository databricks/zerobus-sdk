package zerobus

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/databricks/zerobus-sdk/purego/internal/stream"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// defaultMultiplexedJitter waits before a lane opens. A random delay of 0 to 1
// second spreads the opens of a multiplexed stream. It returns ctx's error if
// ctx ends first.
func defaultMultiplexedJitter(ctx context.Context, _ int) error {
	return sleepContext(ctx, rand.N(time.Second+time.Millisecond).Truncate(time.Millisecond))
}

// multiplexedHooks holds the test hooks of one multiplexed stream. They are
// unset outside tests.
type multiplexedHooks struct {
	// beforeLaneClose runs right before Close closes each lane.
	beforeLaneClose atomic.Pointer[func(lane int)]
}

// MultiplexedStream groups several ordinary streams, called lanes, for one
// table. The lanes send in parallel on their own gRPC streams. WithMaxInflight
// is the budget for the whole multiplexed stream. Create one with
// SDK.CreateMultiplexedStream.
//
// All methods are safe for concurrent use.
//
// Multiplexed streams are a Beta API.
type MultiplexedStream struct {
	lanes []*Stream

	closed atomic.Bool
	// failure holds the first terminal lane error.
	failure atomic.Pointer[multiplexedFailure]

	closeOnce sync.Once
	closeErr  error

	// hooks are the test hooks of this stream.
	hooks multiplexedHooks
}

type multiplexedFailure struct {
	err error
}

// CreateMultiplexedStream opens a multiplexed stream for tableName with
// streamCount lanes. auth is OAuth or Headers.
//
// streamCount must be between 1 and MaxMultiplexedStreams. The lanes split the
// WithMaxInflight budget evenly (MaxInflight / streamCount per lane, the
// remainder is unused), so MaxInflight must be at least streamCount.
// WithAckCallback is rejected; use WithMultiplexedAckCallback.
// WithWaitForReady is always on. Every other option applies to each lane
// unchanged; WithMaxBufferedPayloadBytes is a per-lane limit, so the stream
// can buffer up to streamCount times that value.
//
// It opens the lanes concurrently, each after a random delay of up to 1
// second, and waits until every lane is open, whether or not WithWaitForReady
// is set. ctx bounds the whole creation. If any lane fails to open, it closes
// the lanes that opened and returns the error of the lowest-index lane that
// failed.
//
// Multiplexed streams are a Beta API.
func (s *SDK) CreateMultiplexedStream(
	ctx context.Context,
	tableName string,
	auth Auth,
	streamCount int,
	opts ...StreamOption,
) (*MultiplexedStream, error) {
	const op = "CreateMultiplexedStream"
	sc, err := multiplexedStreamConfig(tableName, streamCount, opts)
	if err != nil {
		return nil, &Error{Op: op, cause: err, retryable: false}
	}
	providers := make([]HeadersProvider, streamCount)
	switch a := auth.(type) {
	case oauthAuth:
		for i := range providers {
			providers[i], err = s.newOAuthProvider(a.clientID, a.clientSecret)
			if err != nil {
				return nil, &Error{Op: op, cause: err, retryable: false}
			}
		}
	case headersAuth:
		if a.provider == nil {
			return nil, &Error{Op: op, cause: fmt.Errorf("headers provider is required"), retryable: false}
		}
		for i := range providers {
			providers[i] = a.provider
		}
	default:
		return nil, &Error{Op: op, cause: fmt.Errorf("auth is required"), retryable: false}
	}
	return s.createMultiplexedStream(ctx, op, tableName, providers, sc)
}

// multiplexedStreamConfig applies opts and checks the multiplexed-only rules.
// The returned config holds the per-lane MaxInflight.
func multiplexedStreamConfig(tableName string, streamCount int, opts []StreamOption) (streamConfig, error) {
	if streamCount < 1 || streamCount > MaxMultiplexedStreams {
		return streamConfig{}, fmt.Errorf("streamCount must be between 1 and %d", MaxMultiplexedStreams)
	}
	sc := streamConfigFromOptions(opts)
	if err := validateStreamArgs(tableName, sc); err != nil {
		return streamConfig{}, err
	}
	if sc.callback != nil {
		return streamConfig{}, fmt.Errorf(
			"WithAckCallback is only valid for ordinary streams; use WithMultiplexedAckCallback")
	}
	maxInflight := positiveOrDefault(sc.cfg.MaxInflight, stream.DefaultMaxInflight)
	if maxInflight < streamCount {
		return streamConfig{}, fmt.Errorf(
			"MaxInflight (%d) must be at least streamCount (%d) for multiplexed streams",
			maxInflight, streamCount)
	}
	sc.cfg.MaxInflight = maxInflight / streamCount
	sc.waitReady = true
	return sc, nil
}

func (s *SDK) createMultiplexedStream(
	ctx context.Context,
	op, tableName string,
	providers []HeadersProvider,
	sc streamConfig,
) (*MultiplexedStream, error) {
	n := len(providers)
	jitter := sc.muxJitter
	if jitter == nil {
		jitter = defaultMultiplexedJitter
	}
	lanes := make([]*Stream, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		laneConfig := sc
		laneConfig.callback = multiplexedAckCallback(i, sc.muxCallback)
		laneConfig.muxCallback = nil
		wg.Add(1)
		go func() {
			defer wg.Done()
			if n > 1 {
				if err := jitter(ctx, i); err != nil {
					errs[i] = wrapErr(op, err)
					return
				}
			}
			lanes[i], errs[i] = s.createStreamConfigured(ctx, op, tableName, providers[i], laneConfig)
		}()
	}
	wg.Wait()

	for _, err := range errs {
		if err == nil {
			continue
		}
		var cleanup sync.WaitGroup
		for _, lane := range lanes {
			if lane != nil {
				cleanup.Add(1)
				go func() {
					defer cleanup.Done()
					_ = lane.Close()
				}()
			}
		}
		cleanup.Wait()
		return nil, err
	}

	return &MultiplexedStream{lanes: lanes}, nil
}

// sleepContext waits for d or until ctx is done.
func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close closes every lane concurrently. Close is idempotent and returns the
// same result to every caller: the first stored lane failure, else the
// lowest-index lane failure, else the first close error. A lane that failed or
// closed before Close, for example through SDK.Close, fails the stream first.
//
// Ack callbacks can still run after Close returns.
func (m *MultiplexedStream) Close() error {
	m.closeOnce.Do(func() {
		if m.failure.Load() == nil {
			for i := range m.lanes {
				if cause := m.laneEndCause(i, nil); cause != nil {
					m.fail(cause)
					break
				}
			}
		}
		m.closed.Store(true)
		errs := make([]error, len(m.lanes))
		var wg sync.WaitGroup
		for i, lane := range m.lanes {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if hook := m.hooks.beforeLaneClose.Load(); hook != nil {
					(*hook)(i)
				}
				errs[i] = lane.Close()
			}()
		}
		wg.Wait()

		if f := m.failure.Load(); f != nil {
			m.closeErr = withOp("Close", f.err)
			return
		}
		for _, lane := range m.lanes {
			if cause := lane.core.TerminalErr(); cause != nil {
				// Store the cause, so later calls return it too.
				m.failure.CompareAndSwap(nil, &multiplexedFailure{err: cause})
				m.closeErr = m.closedErr("Close")
				return
			}
		}
		for _, err := range errs {
			if err != nil {
				m.closeErr = withOp("Close", err)
				return
			}
		}
	})
	return m.closeErr
}

// IsClosed reports whether Close was called or a call saw a lane fail or
// close.
func (m *MultiplexedStream) IsClosed() bool {
	return m.closed.Load()
}

// MessageDescriptor returns the protobuf descriptor configured for this
// stream. It returns nil for JSON streams or unsupported descriptor formats.
func (m *MultiplexedStream) MessageDescriptor() protoreflect.MessageDescriptor {
	return m.lanes[0].MessageDescriptor()
}

// StreamCount returns the number of lanes.
func (m *MultiplexedStream) StreamCount() int {
	return len(m.lanes)
}

// closedErr returns the stored lane failure, or a closed error.
func (m *MultiplexedStream) closedErr(op string) error {
	if f := m.failure.Load(); f != nil {
		return withOp(op, f.err)
	}
	return &Error{Op: op, cause: fmt.Errorf("MultiplexedStream is closed"), retryable: false}
}

// laneEndCause returns the error that ends the lane at idx: its terminal
// cause, or a lane closed error if it is closing or closed without a failure,
// for example through SDK.Close. It returns nil while the lane is open. err is
// the lane's last error, which can show a closing lane before IsClosed does.
func (m *MultiplexedStream) laneEndCause(idx int, err error) error {
	// Read the closed signs first: a failing lane stores its cause before it
	// reports closed, so a lane seen closed here has its cause readable below.
	closed := errors.Is(err, stream.ErrClosed) ||
		errors.Is(err, stream.ErrWatermarkClosed) ||
		m.lanes[idx].IsClosed()
	if cause := m.lanes[idx].core.TerminalErr(); cause != nil {
		return cause
	}
	if closed {
		return &Error{cause: fmt.Errorf("MultiplexedStream lane %d is closed", idx), retryable: false}
	}
	return nil
}

// fail stores cause as the stream's failure, unless the stream is already
// closed, and closes the stream.
func (m *MultiplexedStream) fail(cause error) {
	if !m.closed.Load() {
		m.failure.CompareAndSwap(nil, &multiplexedFailure{err: cause})
	}
	m.closed.Store(true)
}

// withOp returns err with op as its operation. A lane *Error keeps its cause
// and retry class.
func withOp(op string, err error) error {
	if e, ok := err.(*Error); ok {
		return &Error{Op: op, cause: e.cause, retryable: e.retryable}
	}
	return wrapErr(op, err)
}
