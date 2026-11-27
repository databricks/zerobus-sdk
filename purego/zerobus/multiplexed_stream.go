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

// defaultMultiplexedCapacityWait bounds how long one ingest call waits for
// capacity on its lane.
const defaultMultiplexedCapacityWait = 30 * time.Second

// defaultMultiplexedJitter waits before a lane opens. A random delay of 0 to 1
// second spreads the opens of a multiplexed stream. It returns ctx's error if
// ctx ends first.
func defaultMultiplexedJitter(ctx context.Context, _ int) error {
	return sleepContext(ctx, rand.N(time.Second+time.Millisecond).Truncate(time.Millisecond))
}

// multiplexedHooks holds the test hooks of one multiplexed stream. They are
// unset outside tests.
type multiplexedHooks struct {
	// beforeLaneIngest runs right before each lane ingest call.
	beforeLaneIngest atomic.Pointer[func(lane int)]
	// beforeLaneWait runs right before each lane wait in Flush and
	// WaitForMessageID.
	beforeLaneWait atomic.Pointer[func(op string, lane int)]
	// beforeLaneClose runs right before Close closes each lane.
	beforeLaneClose atomic.Pointer[func(lane int)]
}

// errMultiplexedCapacityWait is the context cause when the capacity wait of a
// multiplexed ingest call expires.
var errMultiplexedCapacityWait = errors.New("multiplexed capacity wait expired")

// MultiplexedStream spreads ingest calls round-robin across several ordinary
// streams, called lanes. The lanes send in parallel on their own gRPC streams,
// which raises throughput above one stream. WithMaxInflight is the budget for
// the whole multiplexed stream. Each ingest call returns a MessageID. Create
// one with SDK.CreateMultiplexedStream.
//
// A whole batch goes to one lane. Records keep their order within a lane, but
// there is no order across lanes. When the chosen lane is at capacity
// (WithMaxInflight or WithMaxBufferedPayloadBytes), the call waits for that
// lane; it does not try another lane. The wait lasts at most 30 seconds, then
// the call returns a retryable error. A caller deadline that ends first
// returns the non-retryable context error instead.
//
// A call that sees its lane fail or close returns that lane's error, and the
// stream stores it as its failure. After that, the stream stops accepting
// records, and every later ingest call, Flush, and Close returns the first
// stored failure. It keeps the lane's retry class: once IsClosed reports true,
// Retryable means that a new stream may succeed. Until then, calls on healthy
// lanes still succeed. WaitForMessageID reports only on the lane named by its
// ID, so it can still succeed for a record on a healthy lane. The other lanes
// stay open until Close.
//
// For throughput, queue records in a loop and call Flush once.
//
// All methods are safe for concurrent use.
//
// Multiplexed streams are a Beta API.
type MultiplexedStream struct {
	lanes     []*Stream
	tableName string
	// laneInflight is the MaxInflight of each lane, for error messages.
	laneInflight int
	capacityWait time.Duration
	next         atomic.Uint64

	// closed is set before the lane close contexts are cancelled, so a call
	// that sees one of them done also sees closed.
	closed atomic.Bool
	// laneCloseCtx[i] ends when a lane failure closes the stream. Each lane
	// has its own, so ingest calls on different lanes do not share one context
	// lock.
	laneCloseCtx    []context.Context
	laneCloseCancel []context.CancelFunc
	// ingestsInFlight[i] counts the ingest calls on lane i that have not
	// returned. Close waits until every count is zero.
	ingestsInFlight []inflightCount
	// ingestsDone gets a value when the last ingest call on a lane returns
	// after the stream closed.
	ingestsDone chan struct{}
	// callbackWait bounds how long Close waits for the lane callback workers.
	callbackWait time.Duration
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

// inflightCount is padded to a cache line, so the counts of different lanes
// do not share one.
type inflightCount struct {
	n atomic.Int64
	_ [56]byte
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
	capacityWait := sc.muxCapacityWait
	if capacityWait <= 0 {
		capacityWait = defaultMultiplexedCapacityWait
	}
	callbackWait := sc.cfg.CallbackTeardownTimeout
	if callbackWait <= 0 {
		callbackWait = stream.DefaultCallbackTeardownTimeout
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

	m := &MultiplexedStream{
		lanes:           lanes,
		tableName:       tableName,
		laneInflight:    sc.cfg.MaxInflight,
		capacityWait:    capacityWait,
		callbackWait:    callbackWait,
		laneCloseCtx:    make([]context.Context, n),
		laneCloseCancel: make([]context.CancelFunc, n),
		ingestsInFlight: make([]inflightCount, n),
		ingestsDone:     make(chan struct{}, 1),
	}
	for i := range n {
		m.laneCloseCtx[i], m.laneCloseCancel[i] = context.WithCancel(context.Background())
	}
	return m, nil
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

// IngestRecord queues one record on the next lane and returns its MessageID.
// It blocks only on backpressure, for at most 30 seconds. On error the ID is
// zero and has no meaning.
//
// record is the raw record payload: serialized protobuf bytes for a proto
// stream, or UTF-8 JSON bytes for a JSON stream.
//
// For throughput, queue records in a loop and call Flush once.
func (m *MultiplexedStream) IngestRecord(record []byte) (MessageID, error) {
	return m.IngestRecordContext(context.Background(), record)
}

// IngestRecordContext is IngestRecord with caller context. A deadline can only
// shorten the 30-second capacity wait.
func (m *MultiplexedStream) IngestRecordContext(ctx context.Context, record []byte) (MessageID, error) {
	return m.ingest(ctx, "IngestRecord", func(ctx context.Context, lane *Stream) (int64, error) {
		return lane.core.Ingest(ctx, record)
	})
}

// IngestRecords queues records as one batch on the next lane and returns the
// batch's MessageID. ok is false for an empty batch, which queues nothing. On
// error, id is zero and ok is false; a closed or failed stream returns its
// error also for an empty batch. It blocks only on backpressure, for at most
// 30 seconds.
//
// Prefer this in hot paths to reduce overhead.
func (m *MultiplexedStream) IngestRecords(records [][]byte) (id MessageID, ok bool, err error) {
	return m.IngestRecordsContext(context.Background(), records)
}

// IngestRecordsContext is IngestRecords with caller context. A deadline can
// only shorten the 30-second capacity wait.
func (m *MultiplexedStream) IngestRecordsContext(ctx context.Context, records [][]byte) (id MessageID, ok bool, err error) {
	return m.ingestBatch(ctx, "IngestRecords", records, func(ctx context.Context, lane *Stream) (int64, error) {
		return lane.core.IngestBatch(ctx, records)
	})
}

// IngestJSONRecord queues one JSON payload on the next lane and returns its
// MessageID. Proto streams convert it to protobuf before ingestion. It blocks
// only on backpressure, for at most 30 seconds. On error the ID is zero and
// has no meaning.
func (m *MultiplexedStream) IngestJSONRecord(record []byte) (MessageID, error) {
	return m.IngestJSONRecordContext(context.Background(), record)
}

// IngestJSONRecordContext is IngestJSONRecord with caller context. A deadline
// can only shorten the 30-second capacity wait.
func (m *MultiplexedStream) IngestJSONRecordContext(ctx context.Context, record []byte) (MessageID, error) {
	return m.ingest(ctx, "IngestJSONRecord", func(ctx context.Context, lane *Stream) (int64, error) {
		return lane.IngestJSONOffsetContext(ctx, record)
	})
}

// IngestJSONRecords queues JSON payloads as one batch on the next lane and
// returns the batch's MessageID. ok is false for an empty batch, which queues
// nothing. On error, id is zero and ok is false; a closed or failed stream
// returns its error also for an empty batch. Proto streams convert the
// payloads to protobuf before ingestion. It blocks only on backpressure, for
// at most 30 seconds.
func (m *MultiplexedStream) IngestJSONRecords(records [][]byte) (id MessageID, ok bool, err error) {
	return m.IngestJSONRecordsContext(context.Background(), records)
}

// IngestJSONRecordsContext is IngestJSONRecords with caller context. A
// deadline can only shorten the 30-second capacity wait.
func (m *MultiplexedStream) IngestJSONRecordsContext(ctx context.Context, records [][]byte) (id MessageID, ok bool, err error) {
	return m.ingestBatch(ctx, "IngestJSONRecords", records, func(ctx context.Context, lane *Stream) (int64, error) {
		return lane.IngestJSONRecordsOffsetContext(ctx, records)
	})
}

func (m *MultiplexedStream) ingestBatch(
	ctx context.Context,
	op string,
	records [][]byte,
	call func(context.Context, *Stream) (int64, error),
) (MessageID, bool, error) {
	if err := m.checkClosed(op); err != nil {
		return 0, false, err
	}
	if len(records) == 0 {
		return 0, false, nil
	}
	id, err := m.ingest(ctx, op, call)
	if err != nil {
		return 0, false, err
	}
	return id, true, nil
}

// ingest sends one ingest call to the next lane. The lane call runs under a
// context that the caller, the capacity wait, and a lane failure can each end.
// It checks closed before and after it counts itself as in flight, so Close
// either waits for it or the call sees closed, and calls after Close never
// hold the count up.
func (m *MultiplexedStream) ingest(
	ctx context.Context,
	op string,
	call func(context.Context, *Stream) (int64, error),
) (MessageID, error) {
	if err := m.checkClosed(op); err != nil {
		return 0, err
	}
	idx := int((m.next.Add(1) - 1) % uint64(len(m.lanes)))
	inflight := &m.ingestsInFlight[idx].n
	inflight.Add(1)
	defer m.endIngest(inflight)
	if err := m.checkClosed(op); err != nil {
		return 0, err
	}
	lane := m.lanes[idx]

	laneCtx, cancel := m.laneContext(ctx, idx)
	if hook := m.hooks.beforeLaneIngest.Load(); hook != nil {
		(*hook)(idx)
	}
	offset, err := call(laneCtx, lane)
	cancel()
	if err != nil {
		return 0, m.laneIngestError(ctx, laneCtx, op, idx, err)
	}
	return newMessageID(idx, offset), nil
}

// endIngest ends one in-flight ingest call, and wakes Close if it was the last
// call on its lane after the stream closed.
func (m *MultiplexedStream) endIngest(inflight *atomic.Int64) {
	if inflight.Add(-1) == 0 && m.closed.Load() {
		select {
		case m.ingestsDone <- struct{}{}:
		default:
		}
	}
}

// laneContext derives the context of one ingest call on lane idx. It adds the
// capacity wait only when the caller's deadline is later, and ends when a lane
// failure closes the multiplexed stream.
func (m *MultiplexedStream) laneContext(ctx context.Context, idx int) (context.Context, context.CancelFunc) {
	var cancel context.CancelFunc
	if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > m.capacityWait {
		ctx, cancel = context.WithTimeoutCause(ctx, m.capacityWait, errMultiplexedCapacityWait)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	stop := context.AfterFunc(m.laneCloseCtx[idx], cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

// laneIngestError classifies an error from the lane at idx. If the lane failed
// or closed, it fails the multiplexed stream and returns the lane's error.
func (m *MultiplexedStream) laneIngestError(callerCtx, laneCtx context.Context, op string, idx int, err error) error {
	if cause := m.laneEndCause(idx, err); cause != nil {
		m.fail(cause)
		return withOp(op, cause)
	}
	if callerCtx.Err() == nil {
		// A lane failure cancelled this call.
		if m.laneCloseCtx[idx].Err() != nil {
			return m.closedErr(op)
		}
		if errors.Is(err, context.DeadlineExceeded) &&
			errors.Is(context.Cause(laneCtx), errMultiplexedCapacityWait) {
			return &Error{
				Op: op,
				cause: fmt.Errorf(
					"timed out after %s waiting for capacity on multiplexed lane %d for table %s (MaxInflight per lane: %d)",
					m.capacityWait, idx, m.tableName, m.laneInflight),
				retryable: true,
			}
		}
	}
	return withOp(op, err)
}

// Flush waits until every lane has acknowledged its queued records or failed.
// It flushes all lanes concurrently and waits for all of them. A lane that
// does not finish within its FlushTimeout gives the timeout error, and the
// stream stays open. A failed or closed lane fails the stream, and its error
// wins over other errors; Flush still flushes every lane, then returns the
// first stored failure. A lane that closed without a failure, for example
// through SDK.Close, gives a non-retryable lane closed error. After Close,
// Flush returns the stored failure, else a non-retryable closed error.
func (m *MultiplexedStream) Flush() error {
	return m.FlushContext(context.Background())
}

// FlushContext is Flush with caller context.
func (m *MultiplexedStream) FlushContext(ctx context.Context) error {
	const op = "Flush"
	if m.failure.Load() == nil {
		if m.closed.Load() {
			return m.closedErr(op)
		}
		for i := range m.lanes {
			if cause := m.laneEndCause(i, nil); cause != nil {
				m.fail(cause)
				break
			}
		}
	}

	errs := make([]error, len(m.lanes))
	var wg sync.WaitGroup
	for i, lane := range m.lanes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if hook := m.hooks.beforeLaneWait.Load(); hook != nil {
				(*hook)(op, i)
			}
			errs[i] = lane.core.Flush(ctx)
		}()
	}
	wg.Wait()

	var firstErr error
	laneEnded := false
	for i, err := range errs {
		if err == nil {
			continue
		}
		if cause := m.laneEndCause(i, err); cause != nil {
			m.fail(cause)
			laneEnded = true
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	if f := m.failure.Load(); f != nil {
		return withOp(op, f.err)
	}
	// Close ended a lane while Flush waited.
	if laneEnded {
		return m.closedErr(op)
	}
	return withOp(op, firstErr)
}

// WaitForMessageID waits until the ingest call with this ID is acknowledged,
// or its lane fails. Only the lane named by the ID decides the result. The
// wait is bounded by WithFlushTimeout. An ID whose lane index is out of range
// returns a non-retryable error. If the lane fails or closes before the ID is
// acknowledged, it returns that lane's error, even when another lane failed
// first. A lane that closed without a failure, for example through SDK.Close,
// gives a non-retryable lane closed error. Pass only an ID that this stream
// returned without an error. Prefer Flush for bulk durability checks; use a
// per-record wait only for low-volume strict confirmation flows.
func (m *MultiplexedStream) WaitForMessageID(id MessageID) error {
	return m.WaitForMessageIDContext(context.Background(), id)
}

// WaitForMessageIDContext is WaitForMessageID with caller context.
func (m *MultiplexedStream) WaitForMessageIDContext(ctx context.Context, id MessageID) error {
	const op = "WaitForMessageID"
	idx := id.streamIndex()
	if idx >= len(m.lanes) {
		return &Error{Op: op, cause: fmt.Errorf("invalid lane index %d in message id", idx), retryable: false}
	}
	if hook := m.hooks.beforeLaneWait.Load(); hook != nil {
		(*hook)(op, idx)
	}
	err := m.lanes[idx].core.WaitForOffset(ctx, id.offset())
	if err == nil {
		return nil
	}
	if cause := m.laneEndCause(idx, err); cause != nil {
		m.fail(cause)
		return withOp(op, cause)
	}
	return wrapErr(op, err)
}

// Close stops accepting records and waits for the ingest calls in flight to
// return; each is bounded by its capacity wait. Then it closes every lane
// concurrently. Each lane flushes its queued records within its own
// FlushTimeout before it closes. Close is idempotent and returns the same
// result to every caller: the first stored lane failure, else the lowest-index
// lane failure, else the first close error. A lane that failed or closed before
// Close, for example through SDK.Close, fails the stream first, as in Flush.
//
// When Close returns, all callbacks have run, unless Close waited longer than
// the callback limit (5 s). Do not call Close from a callback.
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
		m.waitIngests()
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
		m.waitCallbacks()

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

// waitIngests waits until no ingest call is in flight.
func (m *MultiplexedStream) waitIngests() {
	for m.ingestInFlight() {
		<-m.ingestsDone
	}
}

func (m *MultiplexedStream) ingestInFlight() bool {
	for i := range m.ingestsInFlight {
		if m.ingestsInFlight[i].n.Load() > 0 {
			return true
		}
	}
	return false
}

// waitCallbacks waits until every lane's callback worker has run its queued
// callbacks, for at most the callback limit in total.
func (m *MultiplexedStream) waitCallbacks() {
	timer := time.NewTimer(m.callbackWait)
	defer timer.Stop()
	for _, lane := range m.lanes {
		select {
		case <-lane.core.CallbacksDone():
		case <-timer.C:
			return
		}
	}
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

func (m *MultiplexedStream) checkClosed(op string) error {
	if f := m.failure.Load(); f != nil {
		return withOp(op, f.err)
	}
	if m.closed.Load() {
		return m.closedErr(op)
	}
	return nil
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
// closed. It closes the stream and wakes the ingest calls blocked on a lane.
func (m *MultiplexedStream) fail(cause error) {
	if !m.closed.Load() {
		m.failure.CompareAndSwap(nil, &multiplexedFailure{err: cause})
	}
	m.closed.Store(true)
	for _, cancel := range m.laneCloseCancel {
		cancel()
	}
}

// withOp returns err with op as its operation. A lane *Error keeps its cause
// and retry class.
func withOp(op string, err error) error {
	if e, ok := err.(*Error); ok {
		return &Error{Op: op, cause: e.cause, retryable: e.retryable}
	}
	return wrapErr(op, err)
}
