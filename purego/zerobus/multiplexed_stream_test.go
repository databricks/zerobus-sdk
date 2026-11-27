package zerobus_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/databricks/zerobus-sdk/purego/internal/stream"
	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
	"github.com/databricks/zerobus-sdk/purego/zerobus"
)

const muxTable = "main.sales.orders"

// Size setup for "lane 0 is full, lane 1 has room": with no acks, a lane
// buffers one bigMuxRecord, or one small record plus one bigMuxRecord, but not
// two bigMuxRecords. A record is charged its raw size plus 544 bytes.
const muxBufferedBytes = 2500

var bigMuxRecord = []byte(`{"p":"` + strings.Repeat("x", 1000) + `"}`)

func muxRecord(i int) []byte { return []byte(fmt.Sprintf(`{"id":%d}`, i)) }

func muxHeaders() zerobus.HeadersProvider {
	return zerobus.NewStaticHeadersProvider(map[string]string{"authorization": "Bearer t"})
}

// noMuxJitter removes the random wait before each lane opens.
var noMuxJitter = zerobus.WithMultiplexedJitter(func(context.Context, int) error { return nil })

func newMuxSDK(t *testing.T, srv *echoServer) *zerobus.SDK {
	t.Helper()
	sdk := zerobus.NewWithConn(dialEcho(t, srv), "https://ws.zerobus.databricks.com", "https://ws.databricks.com")
	t.Cleanup(func() { _ = sdk.Close() })
	return sdk
}

// createMux opens a JSON multiplexed stream with n lanes; opts may override
// the record type.
func createMux(t *testing.T, sdk *zerobus.SDK, n int, opts ...zerobus.StreamOption) *zerobus.MultiplexedStream {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mux, err := sdk.CreateMultiplexedStream(ctx, muxTable, zerobus.Headers(muxHeaders()), n,
		append([]zerobus.StreamOption{zerobus.WithJSON(), noMuxJitter}, opts...)...)
	if err != nil {
		t.Fatalf("CreateMultiplexedStream: %v", err)
	}
	t.Cleanup(func() { _ = mux.Close() })
	return mux
}

func mustIngest(t *testing.T, mux *zerobus.MultiplexedStream, record []byte) zerobus.MessageID {
	t.Helper()
	id, err := mux.IngestRecord(record)
	if err != nil {
		t.Fatalf("IngestRecord(%s): %v", record, err)
	}
	return id
}

// assertPermanentError checks that err is a non-retryable *zerobus.Error and,
// when want is set, that its text is exactly want.
func assertPermanentError(t *testing.T, err error, want string) {
	t.Helper()
	var sdkErr *zerobus.Error
	if !errors.As(err, &sdkErr) {
		t.Fatalf("error %v is not a *zerobus.Error", err)
	}
	if zerobus.Retryable(err) {
		t.Errorf("error %v is retryable, want permanent", err)
	}
	if want != "" && err.Error() != want {
		t.Errorf("error = %q, want %q", err, want)
	}
}

func assertMuxClosed(t *testing.T, err error, op string) {
	t.Helper()
	assertPermanentError(t, err, "zerobus: "+op+": MultiplexedStream is closed")
}

// assertLaneClosed checks that err is the failure of a lane that closed
// without a terminal failure.
func assertLaneClosed(t *testing.T, err error, op string, lane int) {
	t.Helper()
	assertPermanentError(t, err, fmt.Sprintf("zerobus: %s: MultiplexedStream lane %d is closed", op, lane))
}

// assertRejected checks that err is the server's rejection of the record
// that contains marker.
func assertRejected(t *testing.T, err error, marker string) {
	t.Helper()
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Fatalf("error %v: status code = %v, want %v", err, code, codes.InvalidArgument)
	}
	if !strings.Contains(err.Error(), marker) {
		t.Fatalf("error = %v, want the rejection of the record with %q", err, marker)
	}
}

func waitLaneClosed(t *testing.T, mux *zerobus.MultiplexedStream, lane int) {
	t.Helper()
	waitFor(t, mux.Lane(lane).IsClosed, fmt.Sprintf("lane %d to fail", lane))
}

// startIngest runs IngestRecord in a goroutine and returns once the call has
// reached the ingest call of lane and has not returned for a short while, so it
// waits for capacity.
func startIngest(t *testing.T, mux *zerobus.MultiplexedStream, lane int, record []byte) <-chan error {
	t.Helper()
	reached := make(chan struct{})
	var once sync.Once
	t.Cleanup(mux.SetBeforeLaneIngest(func(l int) {
		if l == lane {
			once.Do(func() { close(reached) })
		}
	}))
	done := make(chan error, 1)
	go func() {
		_, err := mux.IngestRecord(record)
		done <- err
	}()
	select {
	case <-reached:
	case err := <-done:
		t.Fatalf("IngestRecord returned before it reached lane %d: %v", lane, err)
	case <-time.After(5 * time.Second):
		t.Fatalf("IngestRecord did not reach lane %d", lane)
	}
	select {
	case err := <-done:
		t.Fatalf("IngestRecord on a full lane %d returned early: %v", lane, err)
	case <-time.After(50 * time.Millisecond):
	}
	return done
}

// releaseHeldAck returns a function that releases srv's held ack. It also
// runs at cleanup, before the stream's Close, so Close never waits for it.
// Call it after createMux.
func releaseHeldAck(t *testing.T, srv *echoServer) func() {
	t.Helper()
	var once sync.Once
	release := func() { once.Do(func() { close(srv.release) }) }
	t.Cleanup(release)
	return release
}

// releaseSoon releases the held ack shortly after the call under test starts.
// The delay only makes the test stronger; it cannot fail correct code.
func releaseSoon(release func()) { time.AfterFunc(20*time.Millisecond, release) }

func newHoldServer(id string) *echoServer {
	return &echoServer{streamID: id, holdMarker: "hold", release: make(chan struct{})}
}

func receive(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not return", what)
		return nil
	}
}

type nopAckCallback struct{}

func (nopAckCallback) OnAck(int64)          {}
func (nopAckCallback) OnError(int64, error) {}

func (c *recordingMuxCallback) ackIDs() []zerobus.MessageID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.acks)
}

func (c *recordingMuxCallback) errorIDs() []zerobus.MessageID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.errIDs)
}

// funcMuxCallback runs onAck for every ack.
type funcMuxCallback struct {
	onAck func(zerobus.MessageID)
}

func (c *funcMuxCallback) OnAck(id zerobus.MessageID)     { c.onAck(id) }
func (*funcMuxCallback) OnError(zerobus.MessageID, error) {}

// syncBuffer is a bytes.Buffer that is safe for concurrent writes.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// assertSameIDs checks that got holds every ID of want exactly once.
func assertSameIDs(t *testing.T, got, want []zerobus.MessageID) {
	t.Helper()
	got, want = slices.Clone(got), slices.Clone(want)
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("callback IDs = %v, want %v", got, want)
	}
}

func TestCreateMultiplexedStreamRejectsInvalidArguments(t *testing.T) {
	uc := &ucTokenServer{}
	ucSrv := httptest.NewServer(uc)
	defer ucSrv.Close()
	creates := make(chan *zerobuspb.CreateIngestStreamRequest, 8)
	sdk := zerobus.NewWithConn(dialEcho(t, &echoServer{streamID: "mux-invalid", creates: creates}),
		"https://ws.zerobus.databricks.com", ucSrv.URL)
	defer sdk.Close()
	const prefix = "zerobus: CreateMultiplexedStream: "

	tests := []struct {
		name        string
		table       string
		nilProvider bool
		nilAuth     bool
		oauth       bool // OAuth auth; clientID is "" unless n is 0
		n           int
		opts        []zerobus.StreamOption
		want        string
	}{
		{name: "zero lanes", n: 0, want: prefix + "streamCount must be between 1 and 64"},
		{name: "too many lanes", n: 65, want: prefix + "streamCount must be between 1 and 64"},
		{name: "OAuth zero lanes", oauth: true, n: 0, want: prefix + "streamCount must be between 1 and 64"},
		{name: "OAuth empty client ID", oauth: true, n: 2, want: prefix + "auth: oauth: clientID is required"},
		{
			name: "MaxInflight below streamCount",
			n:    4,
			opts: []zerobus.StreamOption{zerobus.WithMaxInflight(3)},
			want: prefix + "MaxInflight (3) must be at least streamCount (4) for multiplexed streams",
		},
		{
			name: "ordinary ack callback",
			n:    2,
			opts: []zerobus.StreamOption{zerobus.WithAckCallback(nopAckCallback{})},
			want: prefix + "WithAckCallback is only valid for ordinary streams; use WithMultiplexedAckCallback",
		},
		{name: "empty table name", table: " ", n: 2},
		{name: "nil provider", n: 2, nilProvider: true, want: prefix + "headers provider is required"},
		{name: "nil auth", n: 2, nilAuth: true, want: prefix + "auth is required"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			table := muxTable
			if tc.table != "" {
				table = tc.table
			}
			provider := muxHeaders()
			if tc.nilProvider {
				provider = nil
			}
			opts := append([]zerobus.StreamOption{zerobus.WithJSON()}, tc.opts...)
			auth := zerobus.Headers(provider)
			if tc.oauth {
				clientID := ""
				if tc.n == 0 {
					clientID = "client-id"
				}
				auth = zerobus.OAuth(clientID, "client-secret")
			}
			if tc.nilAuth {
				auth = nil
			}
			mux, err := sdk.CreateMultiplexedStream(context.Background(), table, auth, tc.n, opts...)
			if err == nil {
				_ = mux.Close()
				t.Fatal("create succeeded, want a validation error")
			}
			assertPermanentError(t, err, tc.want)
		})
	}
	// CreateStream shares the OAuth provider build with the OAuth auth of
	// CreateMultiplexedStream.
	st, err := sdk.CreateStream(context.Background(), muxTable, "", "client-secret", zerobus.WithJSON())
	if err == nil {
		_ = st.Close()
		t.Fatal("CreateStream with an empty client ID succeeded")
	}
	assertPermanentError(t, err, "zerobus: CreateStream: auth: oauth: clientID is required")
	if n := sdk.OpenStreamCount(); n != 0 {
		t.Errorf("SDK tracks %d streams, want 0 after rejected creates", n)
	}
	if n := len(creates); n != 0 {
		t.Errorf("server saw %d handshakes, want 0 (validation runs before any lane opens)", n)
	}
	if n := uc.mints.Load(); n != 0 {
		t.Errorf("token mints = %d, want 0 (validation runs before any token request)", n)
	}
}

func TestCreateMultiplexedStreamOpensEveryLane(t *testing.T) {
	sdk := newMuxSDK(t, &echoServer{streamID: "mux-lanes"})
	tests := []struct {
		name string
		n    int
	}{
		{name: "one lane", n: 1},
		{name: "max lanes", n: zerobus.MaxMultiplexedStreams},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recordingMuxCallback{}
			mux := createMux(t, sdk, tc.n, zerobus.WithMultiplexedAckCallback(rec))
			if got := mux.StreamCount(); got != tc.n {
				t.Errorf("StreamCount() = %d, want %d", got, tc.n)
			}
			if got := sdk.OpenStreamCount(); got != tc.n {
				t.Errorf("SDK tracks %d streams, want %d", got, tc.n)
			}
			if mux.IsClosed() {
				t.Error("IsClosed() = true on a new stream")
			}
			if d := mux.MessageDescriptor(); d != nil {
				t.Errorf("MessageDescriptor() = %v on a JSON stream, want nil", d)
			}
			// One record per lane; lanes 32 and up set the sign bit of the ID.
			var ids []zerobus.MessageID
			for i := range tc.n {
				ids = append(ids, mustIngest(t, mux, muxRecord(i)))
			}
			if err := mux.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			last := ids[len(ids)-1]
			if got := zerobus.MessageIDStreamIndex(last); got != tc.n-1 {
				t.Errorf("last ID = %v, want lane %d", last, tc.n-1)
			}
			if err := mux.WaitForMessageID(last); err != nil {
				t.Errorf("WaitForMessageID(%v): %v", last, err)
			}
			for i := range 2 {
				if err := mux.Close(); err != nil {
					t.Fatalf("Close %d: %v", i+1, err)
				}
			}
			if got := sdk.OpenStreamCount(); got != 0 {
				t.Errorf("SDK tracks %d streams after Close, want 0", got)
			}
			if !mux.IsClosed() {
				t.Error("IsClosed() = false after Close")
			}
			waitFor(t, func() bool { return len(rec.ackIDs()) >= len(ids) }, "an ack for every ID")
			assertSameIDs(t, rec.ackIDs(), ids)
		})
	}
}

func TestCreateMultiplexedStreamSharesTokenCache(t *testing.T) {
	uc := &ucTokenServer{}
	ucSrv := httptest.NewServer(uc)
	defer ucSrv.Close()
	sdk := zerobus.NewWithConn(dialEcho(t, &echoServer{streamID: "mux-oauth"}), "https://ws.zerobus.databricks.com", ucSrv.URL)
	defer sdk.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mux, err := sdk.CreateMultiplexedStream(ctx, muxTable, zerobus.OAuth("client-id", "client-secret"), 3, zerobus.WithJSON(), noMuxJitter)
	if err != nil {
		t.Fatalf("CreateMultiplexedStream: %v", err)
	}
	defer mux.Close()
	for i := range 3 {
		mustIngest(t, mux, muxRecord(i))
	}
	if err := mux.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := uc.mints.Load(); got != 1 {
		t.Errorf("token mints = %d, want 1 (lanes share the token cache)", got)
	}
}

func TestResolveMultiplexedStreamConfigSplitsMaxInflight(t *testing.T) {
	tests := []struct {
		name string
		n    int
		opts []zerobus.StreamOption
		want int
	}{
		{name: "remainder unused", n: 3, opts: []zerobus.StreamOption{zerobus.WithMaxInflight(10)}, want: 3},
		{name: "default budget", n: 64, want: stream.DefaultMaxInflight / 64},
		{name: "zero is the default", n: 64, opts: []zerobus.StreamOption{zerobus.WithMaxInflight(0)}, want: stream.DefaultMaxInflight / 64},
		{name: "negative is the default", n: 64, opts: []zerobus.StreamOption{zerobus.WithMaxInflight(-1)}, want: stream.DefaultMaxInflight / 64},
		{name: "one per lane", n: 4, opts: []zerobus.StreamOption{zerobus.WithMaxInflight(4)}, want: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := zerobus.ResolveMultiplexedStreamConfig(muxTable, tc.n,
				append([]zerobus.StreamOption{zerobus.WithJSON()}, tc.opts...)...)
			if err != nil {
				t.Fatalf("ResolveMultiplexedStreamConfig: %v", err)
			}
			if got != tc.want {
				t.Errorf("lane MaxInflight = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestCreateMultiplexedStreamWaitsForEveryLane(t *testing.T) {
	// Two of three lanes get ready.
	creates := make(chan *zerobuspb.CreateIngestStreamRequest, 8)
	sdk := newMuxSDK(t, &echoServer{streamID: "mux-no-ready", noReady: true, readyStreams: 2, creates: creates})

	// No WithWaitForReady: the build waits for every lane anyway.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	mux, err := sdk.CreateMultiplexedStream(ctx, muxTable, zerobus.Headers(muxHeaders()), 3, zerobus.WithJSON(), noMuxJitter)
	if !errors.Is(err, context.DeadlineExceeded) {
		if mux != nil {
			_ = mux.Close()
		}
		t.Fatalf("CreateMultiplexedStream error = %v, want DeadlineExceeded", err)
	}
	if n := len(creates); n != 3 {
		t.Errorf("server saw %d handshakes, want 3", n)
	}
	if n := sdk.OpenStreamCount(); n != 0 {
		t.Errorf("SDK tracks %d streams after a failed create, want 0 (ready lanes are closed)", n)
	}
}

func TestCreateMultiplexedStreamReturnsLowestIndexLaneError(t *testing.T) {
	creates := make(chan *zerobuspb.CreateIngestStreamRequest, 8)
	sdk := newMuxSDK(t, &echoServer{streamID: "mux-lane-error", creates: creates})

	errLane1 := errors.New("lane 1 failed")
	errLane2 := errors.New("lane 2 failed")
	lane2Failed := make(chan struct{})
	jitter := zerobus.WithMultiplexedJitter(func(ctx context.Context, lane int) error {
		switch lane {
		case 1:
			// Fail after lane 2, so the lowest index wins, not the first error.
			select {
			case <-lane2Failed:
			case <-ctx.Done():
				return ctx.Err()
			}
			return errLane1
		case 2:
			defer close(lane2Failed)
			return errLane2
		}
		return nil
	})

	mux, err := sdk.CreateMultiplexedStream(context.Background(), muxTable, zerobus.Headers(muxHeaders()), 3, zerobus.WithJSON(), jitter)
	if err == nil {
		_ = mux.Close()
		t.Fatal("CreateMultiplexedStream succeeded, want the lane 1 error")
	}
	if !errors.Is(err, errLane1) || errors.Is(err, errLane2) {
		t.Fatalf("error = %v, want the lane 1 error", err)
	}
	if n := len(creates); n != 1 {
		t.Errorf("server saw %d handshakes, want 1 (lane 0 opened)", n)
	}
	if n := sdk.OpenStreamCount(); n != 0 {
		t.Errorf("SDK tracks %d streams, want 0 (opened lanes are closed)", n)
	}
}

func TestCreateMultiplexedStreamJitterPerLane(t *testing.T) {
	sdk := newMuxSDK(t, &echoServer{streamID: "mux-jitter"})
	for _, tc := range []struct {
		n    int
		want []int
	}{
		{n: 3, want: []int{0, 1, 2}},
		{n: 1, want: nil},
	} {
		t.Run(fmt.Sprintf("%d lanes", tc.n), func(t *testing.T) {
			var mu sync.Mutex
			var lanes []int
			allStarted := make(chan struct{})
			jitter := zerobus.WithMultiplexedJitter(func(_ context.Context, lane int) error {
				mu.Lock()
				lanes = append(lanes, lane)
				if len(lanes) == tc.n {
					close(allStarted)
				}
				mu.Unlock()
				// Each lane waits until every lane has started, so the lanes
				// must open concurrently.
				select {
				case <-allStarted:
					return nil
				case <-time.After(time.Second):
					return errors.New("lanes did not open concurrently")
				}
			})
			createMux(t, sdk, tc.n, jitter)
			mu.Lock()
			defer mu.Unlock()
			slices.Sort(lanes)
			if !slices.Equal(lanes, tc.want) {
				t.Errorf("jitter ran for lanes %v, want %v", lanes, tc.want)
			}
		})
	}
}

func TestMultiplexedDefaultJitterStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := zerobus.MultiplexedJitter(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("MultiplexedJitter error = %v, want Canceled", err)
	}
	// A wait that ignored ctx would hang until the test times out.
	if err := zerobus.SleepContext(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("SleepContext error = %v, want Canceled", err)
	}
}

// failingCallProvider fails one GetHeaders call with a terminal status.
type failingCallProvider struct {
	calls  atomic.Int32
	failOn int32
}

func (p *failingCallProvider) GetHeaders(context.Context, string) (map[string]string, error) {
	if p.calls.Add(1) == p.failOn {
		return nil, status.Error(codes.InvalidArgument, "lane rejected")
	}
	return map[string]string{"authorization": "Bearer t"}, nil
}

func (*failingCallProvider) Invalidate(context.Context, string) {}

func TestCreateMultiplexedStreamClosesOpenLanesOnLaneFailure(t *testing.T) {
	creates := make(chan *zerobuspb.CreateIngestStreamRequest, 8)
	sdk := newMuxSDK(t, &echoServer{streamID: "mux-open-failure", creates: creates})

	mux, err := sdk.CreateMultiplexedStream(
		context.Background(), muxTable, zerobus.Headers(&failingCallProvider{failOn: 2}), 3, zerobus.WithJSON(), noMuxJitter)
	if err == nil {
		_ = mux.Close()
		t.Fatal("CreateMultiplexedStream succeeded, want the lane open failure")
	}
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Errorf("error %v: status code = %v, want %v", err, code, codes.InvalidArgument)
	}
	if n := len(creates); n != 2 {
		t.Errorf("server saw %d handshakes, want 2 (the lanes that opened)", n)
	}
	if n := sdk.OpenStreamCount(); n != 0 {
		t.Errorf("SDK tracks %d streams, want 0 (opened lanes are closed)", n)
	}
}

func TestMultiplexedStreamRoundRobinIDs(t *testing.T) {
	sdk := newMuxSDK(t, &echoServer{streamID: "mux-round-robin"})
	mux := createMux(t, sdk, 3)

	want := []zerobus.MessageID{
		zerobus.NewMessageID(0, 0), zerobus.NewMessageID(1, 0), zerobus.NewMessageID(2, 0),
		zerobus.NewMessageID(0, 1), zerobus.NewMessageID(1, 1), zerobus.NewMessageID(2, 1),
	}
	for i, w := range want {
		if got := mustIngest(t, mux, muxRecord(i)); got != w {
			t.Errorf("ingest %d: ID = %v, want %v", i, got, w)
		}
	}
	if err := mux.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestMultiplexedStreamBatchGoesToOneLane(t *testing.T) {
	ingests := make(chan *zerobuspb.EphemeralStreamRequest, 8)
	sdk := newMuxSDK(t, &echoServer{streamID: "mux-batch", ingests: ingests})
	mux := createMux(t, sdk, 3)

	id, ok, err := mux.IngestRecords([][]byte{muxRecord(1), muxRecord(2), muxRecord(3)})
	if err != nil || !ok {
		t.Fatalf("IngestRecords = (%v, %v, %v), want an ID", id, ok, err)
	}
	if id != zerobus.NewMessageID(0, 0) {
		t.Errorf("batch ID = %v, want %v", id, zerobus.NewMessageID(0, 0))
	}
	if got := mustIngest(t, mux, muxRecord(4)); zerobus.MessageIDStreamIndex(got) != 1 {
		t.Errorf("ID after the batch = %v, want lane 1", got)
	}
	if err := mux.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	close(ingests)
	var batches, singles int
	for req := range ingests {
		if batch := req.GetIngestRecordBatch(); batch != nil {
			batches++
			if n := len(batch.GetJsonBatch().GetRecords()); n != 3 {
				t.Errorf("batch request has %d records, want 3", n)
			}
		} else {
			singles++
		}
	}
	if batches != 1 || singles != 1 {
		t.Errorf("server got %d batch and %d single requests, want 1 and 1", batches, singles)
	}
}

func TestMultiplexedStreamJSONAndContextIngest(t *testing.T) {
	t.Run("empty batches", func(t *testing.T) {
		mux := createMux(t, newMuxSDK(t, &echoServer{streamID: "mux-empty"}), 2)
		if id, ok, err := mux.IngestRecords(nil); ok || err != nil {
			t.Errorf("IngestRecords(nil) = (%v, %v, %v), want ok false and no error", id, ok, err)
		}
		if id, ok, err := mux.IngestJSONRecords(nil); ok || err != nil {
			t.Errorf("IngestJSONRecords(nil) = (%v, %v, %v), want ok false and no error", id, ok, err)
		}
	})
	t.Run("JSON stream", func(t *testing.T) {
		mux := createMux(t, newMuxSDK(t, &echoServer{streamID: "mux-json"}), 2)
		if id, err := mux.IngestJSONRecord(muxRecord(1)); err != nil || zerobus.MessageIDStreamIndex(id) != 0 {
			t.Fatalf("IngestJSONRecord = (%v, %v), want an ID on lane 0", id, err)
		}
		if id, ok, err := mux.IngestJSONRecords([][]byte{muxRecord(2), muxRecord(3)}); err != nil || !ok || zerobus.MessageIDStreamIndex(id) != 1 {
			t.Fatalf("IngestJSONRecords = (%v, %v, %v), want an ID on lane 1", id, ok, err)
		}
		if err := mux.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	})
	t.Run("proto stream", func(t *testing.T) {
		descriptor := mustMarshalDescriptor(t, &descriptorpb.DescriptorProto{
			Name: proto.String("Order"),
			Field: []*descriptorpb.FieldDescriptorProto{
				descriptorField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64),
			},
		})
		ingests := make(chan *zerobuspb.EphemeralStreamRequest, 8)
		mux := createMux(t, newMuxSDK(t, &echoServer{streamID: "mux-proto", ingests: ingests}), 2,
			zerobus.WithProto(descriptor))
		md := mux.MessageDescriptor()
		if md == nil || md.Name() != "Order" {
			t.Fatalf("MessageDescriptor() = %v, want Order", md)
		}
		// The lane's conversion error keeps its class and gets the mux op.
		_, err := mux.IngestJSONRecord([]byte("not json"))
		var sdkErr *zerobus.Error
		if !errors.As(err, &sdkErr) || sdkErr.Op != "IngestJSONRecord" || zerobus.Retryable(err) {
			t.Fatalf("IngestJSONRecord of invalid JSON: error = %v, want a permanent *zerobus.Error with Op IngestJSONRecord", err)
		}
		if mux.IsClosed() {
			t.Fatal("IsClosed() = true after a record-level error")
		}
		if _, err := mux.IngestJSONRecord([]byte(`{"id":"7"}`)); err != nil {
			t.Fatalf("IngestJSONRecord: %v", err)
		}
		if _, ok, err := mux.IngestJSONRecords([][]byte{[]byte(`{"id":"8"}`)}); err != nil || !ok {
			t.Fatalf("IngestJSONRecords = (%v, %v), want an ID", ok, err)
		}
		if err := mux.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		close(ingests)
		var ids []int64
		decode := func(record []byte) {
			msg := dynamicpb.NewMessage(md)
			if err := proto.Unmarshal(record, msg); err != nil {
				t.Fatalf("server record is not proto: %v", err)
			}
			ids = append(ids, msg.Get(md.Fields().ByName("id")).Int())
		}
		for req := range ingests {
			if single := req.GetIngestRecord(); single != nil {
				if single.GetJsonRecord() != "" {
					t.Fatalf("proto stream sent a JSON record %q", single.GetJsonRecord())
				}
				decode(single.GetProtoEncodedRecord())
			} else if batch := req.GetIngestRecordBatch(); batch != nil {
				if batch.GetJsonBatch() != nil {
					t.Fatalf("proto stream sent a JSON batch %v", batch.GetJsonBatch().GetRecords())
				}
				for _, record := range batch.GetProtoEncodedBatch().GetRecords() {
					decode(record)
				}
			}
		}
		slices.Sort(ids)
		if !slices.Equal(ids, []int64{7, 8}) {
			t.Errorf("proto record ids = %v, want [7 8] (single and batch)", ids)
		}
	})
	t.Run("cancelled context", func(t *testing.T) {
		mux := createMux(t, newMuxSDK(t, &echoServer{streamID: "mux-ctx"}), 2)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		records := [][]byte{muxRecord(1)}
		for name, call := range map[string]func() error{
			"IngestRecordContext": func() error { _, err := mux.IngestRecordContext(ctx, muxRecord(1)); return err },
			"IngestRecordsContext": func() error {
				_, _, err := mux.IngestRecordsContext(ctx, records)
				return err
			},
			"IngestJSONRecordContext": func() error { _, err := mux.IngestJSONRecordContext(ctx, muxRecord(1)); return err },
			"IngestJSONRecordsContext": func() error {
				_, _, err := mux.IngestJSONRecordsContext(ctx, records)
				return err
			},
		} {
			if err := call(); !errors.Is(err, context.Canceled) {
				t.Errorf("%s error = %v, want Canceled", name, err)
			}
		}
	})
}

// fullLaneMux returns a two-lane stream on srv, with acks turned off, whose
// lane 0 is full and whose lane 1 holds record1 and still has room.
func fullLaneMux(t *testing.T, srv *echoServer, record1 []byte, opts ...zerobus.StreamOption) *zerobus.MultiplexedStream {
	t.Helper()
	srv.noAcks = true
	mux := createMux(t, newMuxSDK(t, srv), 2, append([]zerobus.StreamOption{
		zerobus.WithMaxBufferedPayloadBytes(muxBufferedBytes), zerobus.WithFlushTimeout(100 * time.Millisecond)}, opts...)...)
	mustIngest(t, mux, bigMuxRecord) // lane 0 is full
	mustIngest(t, mux, record1)      // lane 1 has room
	return mux
}

func TestMultiplexedIngestCallerContextEndsCapacityWait(t *testing.T) {
	mux := fullLaneMux(t, &echoServer{streamID: "mux-backpressure"}, muxRecord(1))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := mux.IngestRecordContext(ctx, bigMuxRecord) // lane 0
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("IngestRecordContext error = %v, want DeadlineExceeded", err)
	}
	if strings.Contains(err.Error(), "waiting for capacity") {
		t.Errorf("error = %v, want the caller's deadline, not the capacity wait", err)
	}
	if mux.IsClosed() {
		t.Error("IsClosed() = true after a caller timeout")
	}
}

func TestMultiplexedIngestCapacityWaitIsRetryable(t *testing.T) {
	assertCapacityError := func(t *testing.T, mux *zerobus.MultiplexedStream, err error, laneInflight int) {
		t.Helper()
		if !zerobus.Retryable(err) {
			t.Fatalf("error %v is not retryable", err)
		}
		want := fmt.Sprintf("zerobus: IngestRecord: timed out after 50ms waiting for capacity on multiplexed lane 0 "+
			"for table %s (MaxInflight per lane: %d)", muxTable, laneInflight)
		if err.Error() != want {
			t.Errorf("error = %q, want %q", err, want)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("error %v matches DeadlineExceeded, want it distinct from a caller timeout", err)
		}
		if mux.IsClosed() {
			t.Error("IsClosed() = true after a capacity timeout")
		}
	}
	t.Run("buffered bytes", func(t *testing.T) {
		mux := fullLaneMux(t, &echoServer{streamID: "mux-capacity"}, muxRecord(1),
			zerobus.WithMultiplexedCapacityWait(50*time.Millisecond))

		_, err := mux.IngestRecord(bigMuxRecord) // lane 0, no caller deadline
		assertCapacityError(t, mux, err, stream.DefaultMaxInflight/2)
		mustIngest(t, mux, muxRecord(2)) // lane 1
		// A caller deadline later than the capacity wait does not replace it.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err = mux.IngestRecordContext(ctx, bigMuxRecord) // lane 0
		assertCapacityError(t, mux, err, stream.DefaultMaxInflight/2)
	})
	t.Run("per-lane MaxInflight", func(t *testing.T) {
		// Without the split, MaxInflight 4 would admit the fifth record.
		sdk := newMuxSDK(t, &echoServer{streamID: "mux-capacity-inflight", noAcks: true})
		mux := createMux(t, sdk, 2, zerobus.WithMaxInflight(4), zerobus.WithFlushTimeout(100*time.Millisecond),
			zerobus.WithMultiplexedCapacityWait(50*time.Millisecond))
		for i := range 4 {
			mustIngest(t, mux, muxRecord(i)) // two per lane
		}
		_, err := mux.IngestRecord(muxRecord(4)) // lane 0
		assertCapacityError(t, mux, err, 2)
	})
}

// Close does not cancel an ingest call in flight; it waits until the call
// returns.
func TestMultiplexedCloseWaitsForBlockedIngest(t *testing.T) {
	srv := newHoldServer("mux-close-waits")
	mux := createMux(t, newMuxSDK(t, srv), 2, zerobus.WithMaxBufferedPayloadBytes(muxBufferedBytes))
	release := releaseHeldAck(t, srv)
	mustIngest(t, mux, []byte(`{"m":"hold","p":"`+strings.Repeat("x", 1000)+`"}`)) // lane 0 is full
	mustIngest(t, mux, muxRecord(1))                                               // lane 1

	ids := make(chan zerobus.MessageID, 1)
	blocked := make(chan error, 1)
	reached := make(chan struct{})
	var once sync.Once
	t.Cleanup(mux.SetBeforeLaneIngest(func(lane int) {
		if lane == 0 {
			once.Do(func() { close(reached) })
		}
	}))
	go func() {
		id, err := mux.IngestRecord(bigMuxRecord) // lane 0
		ids <- id
		blocked <- err
	}()
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("IngestRecord did not reach lane 0")
	}
	closed := make(chan error, 1)
	go func() { closed <- mux.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned %v while an ingest was in flight", err)
	case err := <-blocked:
		t.Fatalf("blocked IngestRecord returned %v before its lane had room", err)
	case <-time.After(50 * time.Millisecond):
	}

	release()
	if err := receive(t, blocked, "blocked IngestRecord"); err != nil {
		t.Fatalf("blocked IngestRecord: %v, want it admitted after the ack", err)
	}
	id := <-ids
	if err := receive(t, closed, "Close"); err != nil {
		t.Errorf("Close: %v, want nil (the admitted record is acked)", err)
	}
	if err := mux.WaitForMessageID(id); err != nil {
		t.Errorf("WaitForMessageID(%v) of the admitted record: %v", id, err)
	}
}

func TestMultiplexedStreamRejectsCallsAfterClose(t *testing.T) {
	sdk := newMuxSDK(t, &echoServer{streamID: "mux-after-close"})
	mux := createMux(t, sdk, 2)
	id := mustIngest(t, mux, muxRecord(1))
	if err := mux.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// A durable record never fails a wait, also after Close.
	if err := mux.WaitForMessageID(id); err != nil {
		t.Errorf("WaitForMessageID(%v) of an acked record after Close: %v", id, err)
	}

	// Each method only passes context.Background to its Context method, so
	// one row covers both.
	records := [][]byte{muxRecord(2)}
	batch := func(call func() (zerobus.MessageID, bool, error)) func() error {
		return func() error {
			_, ok, err := call()
			if ok {
				t.Error("batch call after Close reported ok")
			}
			return err
		}
	}
	tests := []struct {
		name string
		op   string
		call func() error
	}{
		{"IngestRecord", "IngestRecord", func() error { _, err := mux.IngestRecord(muxRecord(2)); return err }},
		{"IngestRecords", "IngestRecords", batch(func() (zerobus.MessageID, bool, error) { return mux.IngestRecords(records) })},
		{"IngestRecords empty", "IngestRecords", batch(func() (zerobus.MessageID, bool, error) { return mux.IngestRecords(nil) })},
		{"IngestJSONRecord", "IngestJSONRecord", func() error { _, err := mux.IngestJSONRecord(muxRecord(2)); return err }},
		{"IngestJSONRecords", "IngestJSONRecords", batch(func() (zerobus.MessageID, bool, error) { return mux.IngestJSONRecords(records) })},
		{"Flush", "Flush", mux.Flush},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertMuxClosed(t, tc.call(), tc.op)
		})
	}
}

// poisonedMux returns a two-lane stream whose lane 1 the server failed with
// the record "poison-r1", unobserved by the multiplexed stream. Lane 0 holds
// one acked record.
func poisonedMux(t *testing.T, failAfterAck bool) (*zerobus.MultiplexedStream, zerobus.MessageID, zerobus.MessageID) {
	t.Helper()
	sdk := newMuxSDK(t, &echoServer{streamID: "mux-poison", failMarker: "poison", failAfterAck: failAfterAck})
	mux := createMux(t, sdk, 2)
	id0 := mustIngest(t, mux, muxRecord(0))
	id1 := mustIngest(t, mux, []byte(`{"m":"poison-r1"}`))
	waitLaneClosed(t, mux, 1)
	return mux, id0, id1
}

func TestMultiplexedLaneFailurePoisonsOnIngest(t *testing.T) {
	mux, _, _ := poisonedMux(t, false)

	// An ingest checks only its own lane, so lane 0 still accepts one.
	if id := mustIngest(t, mux, muxRecord(2)); zerobus.MessageIDStreamIndex(id) != 0 {
		t.Fatalf("ID = %v, want lane 0", id)
	}
	_, err := mux.IngestRecord(muxRecord(3)) // lane 1
	assertRejected(t, err, "poison-r1")
	_, err = mux.IngestRecord(muxRecord(4)) // lane 0
	assertRejected(t, err, "poison-r1")
	if !mux.IsClosed() {
		t.Error("IsClosed() = false after a lane failure")
	}
	assertRejected(t, mux.Flush(), "poison-r1")
	assertRejected(t, mux.Close(), "poison-r1")
}

func TestMultiplexedLaneFailurePoisonsOnFlush(t *testing.T) {
	t.Run("failed lane record acked", func(t *testing.T) {
		// Only the lane state shows the failure.
		mux, _, id1 := poisonedMux(t, true)

		assertRejected(t, mux.Flush(), "poison-r1")
		_, err := mux.IngestRecord(muxRecord(2))
		assertRejected(t, err, "poison-r1")
		// The failed lane acked the record first, so it is durable.
		if err := mux.WaitForMessageID(id1); err != nil {
			t.Errorf("WaitForMessageID of the acked record on the failed lane: %v", err)
		}
	})
	for _, tc := range []struct {
		name   string
		stored bool
	}{
		{name: "healthy lanes still flushed"},
		{name: "healthy lanes still flushed after the failure is stored", stored: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newHoldServer("mux-poison-flush")
			srv.failMarker = "poison"
			mux := createMux(t, newMuxSDK(t, srv), 2)
			release := releaseHeldAck(t, srv)
			mustIngest(t, mux, []byte(`{"m":"hold"}`))      // lane 0, ack held
			mustIngest(t, mux, []byte(`{"m":"poison-r1"}`)) // lane 1 fails
			if tc.stored {
				waitLaneClosed(t, mux, 1)
				mustIngest(t, mux, muxRecord(2))         // lane 0
				_, err := mux.IngestRecord(muxRecord(3)) // lane 1 stores its failure
				assertRejected(t, err, "poison-r1")
			}

			releaseSoon(release)
			assertRejected(t, mux.Flush(), "poison-r1")
			if !srv.heldAcked.Load() {
				t.Error("Flush returned before lane 0 got its ack")
			}
		})
	}
}

func TestMultiplexedLaneFailureWinsOverTimeout(t *testing.T) {
	// Lane 0 never gets an ack, so its Flush and Close time out; lane 1 fails.
	failedMux := func(t *testing.T, flushTimeout time.Duration) (*zerobus.MultiplexedStream, zerobus.MessageID) {
		t.Helper()
		srv := &echoServer{streamID: "mux-fail-timeout", noAcks: true, failMarker: "poison"}
		mux := createMux(t, newMuxSDK(t, srv), 2, zerobus.WithFlushTimeout(flushTimeout))
		mustIngest(t, mux, muxRecord(0))                       // lane 0, never acked
		id1 := mustIngest(t, mux, []byte(`{"m":"poison-r1"}`)) // lane 1 fails
		return mux, id1
	}
	assertOnlyFailure := func(t *testing.T, err error) {
		t.Helper()
		assertRejected(t, err, "poison-r1")
		if errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("error %v matches DeadlineExceeded, want only the lane failure", err)
		}
	}
	t.Run("Flush", func(t *testing.T) {
		mux, _ := failedMux(t, 500*time.Millisecond)
		assertOnlyFailure(t, mux.Flush())
	})
	t.Run("Close", func(t *testing.T) {
		mux, id1 := failedMux(t, 50*time.Millisecond)
		assertRejected(t, mux.WaitForMessageID(id1), "poison-r1")
		assertOnlyFailure(t, mux.Close())
	})
}

func TestMultiplexedLaneFailurePoisonsOnWaitForMessageID(t *testing.T) {
	srv := newHoldServer("mux-poison-wait")
	srv.failMarker = "poison"
	mux := createMux(t, newMuxSDK(t, srv), 2)
	release := releaseHeldAck(t, srv)
	id0 := mustIngest(t, mux, []byte(`{"m":"hold"}`))      // lane 0, ack held
	id1 := mustIngest(t, mux, []byte(`{"m":"poison-r1"}`)) // lane 1 fails

	assertRejected(t, mux.WaitForMessageID(id1), "poison-r1")
	_, err := mux.IngestRecord(muxRecord(2)) // lane 0
	assertRejected(t, err, "poison-r1")
	// Only the lane named by the ID decides: lane 0 still acks its record.
	releaseSoon(release)
	if err := mux.WaitForMessageID(id0); err != nil {
		t.Errorf("WaitForMessageID on the healthy lane: %v", err)
	}
	if !srv.heldAcked.Load() {
		t.Error("WaitForMessageID returned before lane 0 got its ack")
	}
}

// A call that sees its own lane end returns that lane's result, also when
// another lane's failure is already stored: the lane's cause if it failed, the
// closed error if it closed without a failure. Later calls return the stored
// failure.
func TestMultiplexedCallReturnsOwnLaneResult(t *testing.T) {
	for _, tc := range []struct {
		name    string
		endLane func(t *testing.T, lane *zerobus.Stream)
		check   func(t *testing.T, err error, op string)
	}{
		{
			name: "lane failed",
			endLane: func(t *testing.T, lane *zerobus.Stream) {
				if _, err := lane.IngestRecordOffset([]byte(`{"m":"poison-b"}`)); err != nil {
					t.Fatalf("lane IngestRecordOffset: %v", err)
				}
			},
			check: func(t *testing.T, err error, _ string) { assertRejected(t, err, "poison-b") },
		},
		{
			name:    "lane closed",
			endLane: func(_ *testing.T, lane *zerobus.Stream) { _ = lane.Close() },
			check:   func(t *testing.T, err error, op string) { assertLaneClosed(t, err, op, 1) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := &echoServer{streamID: "mux-own-lane", noAcks: true, failMarker: "poison"}
			mux := createMux(t, newMuxSDK(t, srv), 2, zerobus.WithFlushTimeout(50*time.Millisecond))
			mustIngest(t, mux, muxRecord(0))        // lane 0
			id1 := mustIngest(t, mux, muxRecord(1)) // lane 1, never acked
			mustIngest(t, mux, muxRecord(2))        // lane 0

			// The next ingest goes to lane 1; hold it after its closed check.
			reached, proceed := make(chan struct{}), make(chan struct{})
			t.Cleanup(mux.SetBeforeLaneIngest(func(lane int) {
				if lane == 1 {
					close(reached)
					<-proceed
				}
			}))
			// Release the held ingest also when the test fails, so the
			// cleanup Close does not wait for it.
			var once sync.Once
			release := func() { once.Do(func() { close(proceed) }) }
			t.Cleanup(release)
			done := make(chan error, 1)
			go func() {
				_, err := mux.IngestRecord(muxRecord(3))
				done <- err
			}()
			select {
			case <-reached:
			case <-time.After(5 * time.Second):
				t.Fatal("IngestRecord did not reach lane 1")
			}

			// Lane 0 fails first, and the stream stores its failure.
			if _, err := mux.Lane(0).IngestRecordOffset([]byte(`{"m":"poison-a"}`)); err != nil {
				t.Fatalf("lane 0 IngestRecordOffset: %v", err)
			}
			waitLaneClosed(t, mux, 0)
			_, err := mux.IngestRecord(muxRecord(4)) // lane 0
			assertRejected(t, err, "poison-a")
			tc.endLane(t, mux.Lane(1))
			waitLaneClosed(t, mux, 1)

			release()
			tc.check(t, receive(t, done, "IngestRecord on lane 1"), "IngestRecord")
			tc.check(t, mux.WaitForMessageID(id1), "WaitForMessageID")
			_, err = mux.IngestRecord(muxRecord(5))
			assertRejected(t, err, "poison-a")
		})
	}
}

func TestMultiplexedPoisonWakesBlockedIngest(t *testing.T) {
	mux := fullLaneMux(t, &echoServer{streamID: "mux-poison-wakes", failMarker: "poison"}, []byte(`{"m":"poison-r1"}`))
	waitLaneClosed(t, mux, 1)

	blocked := startIngest(t, mux, 0, bigMuxRecord)
	// An ingest on lane 1 is the first call to see the failure.
	_, err := mux.IngestRecord(muxRecord(2))
	assertRejected(t, err, "poison-r1")
	err = receive(t, blocked, "blocked IngestRecord")
	assertRejected(t, err, "poison-r1")
	if errors.Is(err, context.Canceled) {
		t.Errorf("error %v matches Canceled, want the lane failure", err)
	}
	if strings.Contains(err.Error(), "MultiplexedStream is closed") {
		t.Errorf("error = %v, want the lane failure, not the closed error", err)
	}
}

func TestMultiplexedLaneFailureWakesIngestsBlockedOnIt(t *testing.T) {
	srv := newHoldServer("mux-lane-fail-wakes")
	srv.failMarker = "poison"
	srv.failAfterAck = true
	mux := createMux(t, newMuxSDK(t, srv), 1, zerobus.WithMaxInflight(1))
	release := releaseHeldAck(t, srv)
	mustIngest(t, mux, []byte(`{"m":"hold-poison"}`)) // held; after its ack the lane fails

	const waiters = 3
	var reached atomic.Int32
	t.Cleanup(mux.SetBeforeLaneIngest(func(int) { reached.Add(1) }))
	type result struct {
		id  zerobus.MessageID
		err error
	}
	results := make(chan result, waiters)
	for i := range waiters {
		go func() {
			id, err := mux.IngestRecord(muxRecord(i))
			results <- result{id, err}
		}()
	}
	waitFor(t, func() bool { return reached.Load() == waiters }, "every ingest to reach the full lane")
	select {
	case r := <-results:
		t.Fatalf("IngestRecord on a full lane returned early: (%v, %v)", r.id, r.err)
	case <-time.After(50 * time.Millisecond):
	}

	release()
	// The ack frees one slot before the lane fails, so one ingest may succeed.
	var successes int
	for range waiters {
		var r result
		select {
		case r = <-results:
		case <-time.After(5 * time.Second):
			t.Fatal("blocked IngestRecord did not return")
		}
		if r.err == nil {
			successes++
			if r.id != zerobus.NewMessageID(0, 1) {
				t.Errorf("admitted ingest ID = %v, want lane 0 offset 1", r.id)
			}
			continue
		}
		assertRejected(t, r.err, "hold-poison")
		if strings.Contains(r.err.Error(), "MultiplexedStream is closed") {
			t.Errorf("error = %v, want the lane failure, not the closed error", r.err)
		}
	}
	if successes > 1 {
		t.Errorf("%d blocked ingests succeeded, want at most 1", successes)
	}
	if !mux.IsClosed() {
		t.Error("IsClosed() = false after the lane failed")
	}
	_, err := mux.IngestRecord(muxRecord(9))
	assertRejected(t, err, "hold-poison")
}

func TestMultiplexedNonTerminalLaneErrorDoesNotPoison(t *testing.T) {
	sdk := newMuxSDK(t, &echoServer{streamID: "mux-oversized"})
	mux := createMux(t, sdk, 2, zerobus.WithMaxPayloadBytes(64))

	_, err := mux.IngestRecord(bigMuxRecord) // lane 0
	if !errors.Is(err, stream.ErrPayloadTooLarge) {
		t.Fatalf("IngestRecord of an oversized record: error = %v, want ErrPayloadTooLarge", err)
	}
	if id := mustIngest(t, mux, muxRecord(1)); zerobus.MessageIDStreamIndex(id) != 1 {
		t.Errorf("next ID = %v, want lane 1", id)
	}
	if id := mustIngest(t, mux, muxRecord(2)); zerobus.MessageIDStreamIndex(id) != 0 {
		t.Errorf("ID after that = %v, want lane 0", id)
	}
	if mux.IsClosed() {
		t.Error("IsClosed() = true after a record-level error")
	}
}

func TestMultiplexedLaneClosingClosesStream(t *testing.T) {
	sdk := newMuxSDK(t, &echoServer{streamID: "mux-lane-closing", noAcks: true})
	mux := createMux(t, sdk, 1, zerobus.WithFlushTimeout(500*time.Millisecond))
	mustIngest(t, mux, muxRecord(0))

	// The lane stays closing for its flush timeout, since nothing is acked.
	closed := make(chan error, 1)
	go func() { closed <- mux.Lane(0).Close() }()
	var err error
	waitFor(t, func() bool {
		_, err = mux.IngestRecord(muxRecord(1))
		return err != nil
	}, "an ingest to fail while lane 0 closes")
	if mux.Lane(0).IsClosed() {
		t.Fatal("lane 0 closed before the ingest failed; the closing state was not tested")
	}
	assertLaneClosed(t, err, "IngestRecord", 0)
	if !mux.IsClosed() {
		t.Error("IsClosed() = false after a lane began to close")
	}
	receive(t, closed, "lane Close")
}

func TestMultiplexedSDKCloseClosesStream(t *testing.T) {
	t.Run("idle stream", func(t *testing.T) {
		sdk := newMuxSDK(t, &echoServer{streamID: "mux-sdk-close"})
		mux := createMux(t, sdk, 2)
		mustIngest(t, mux, muxRecord(1))
		if err := mux.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		if err := sdk.Close(); err != nil {
			t.Fatalf("SDK.Close: %v", err)
		}

		if mux.IsClosed() {
			t.Error("IsClosed() = true before any call saw the closed lanes")
		}
		// Flush is the first call to see the closed lanes; it stores the
		// first closed lane as the failure.
		assertLaneClosed(t, mux.Flush(), "Flush", 0)
		if !mux.IsClosed() {
			t.Fatal("IsClosed() = false after SDK.Close")
		}
		_, err := mux.IngestRecord(muxRecord(2))
		assertLaneClosed(t, err, "IngestRecord", 0)
		assertLaneClosed(t, mux.Close(), "Close", 0)
	})
	t.Run("Close first", func(t *testing.T) {
		sdk := newMuxSDK(t, &echoServer{streamID: "mux-sdk-close-first"})
		mux := createMux(t, sdk, 2)
		mustIngest(t, mux, muxRecord(1))
		if err := mux.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		if err := sdk.Close(); err != nil {
			t.Fatalf("SDK.Close: %v", err)
		}
		// No call has seen the closed lanes; Close finds and stores lane 0.
		assertLaneClosed(t, mux.Close(), "Close", 0)
		_, err := mux.IngestRecord(muxRecord(2))
		assertLaneClosed(t, err, "IngestRecord", 0)
	})
	// A call that already waits on a lane when SDK.Close closes it gets the
	// lane closed error, not the lane's retryable error.
	for _, tc := range []struct {
		op   string
		lane int
		call func(*zerobus.MultiplexedStream, []zerobus.MessageID) error
	}{
		{op: "Flush", lane: -1, call: func(mux *zerobus.MultiplexedStream, _ []zerobus.MessageID) error {
			return mux.Flush()
		}},
		{op: "WaitForMessageID", lane: 1, call: func(mux *zerobus.MultiplexedStream, ids []zerobus.MessageID) error {
			return mux.WaitForMessageID(ids[1])
		}},
	} {
		t.Run(tc.op+" waiting", func(t *testing.T) {
			sdk := newMuxSDK(t, &echoServer{streamID: "mux-sdk-close-wait", noAcks: true})
			mux := createMux(t, sdk, 3, zerobus.WithFlushTimeout(30*time.Second))
			var ids []zerobus.MessageID
			for i := range 3 {
				ids = append(ids, mustIngest(t, mux, muxRecord(i)))
			}
			waiting := make(chan int, zerobus.MaxMultiplexedStreams)
			t.Cleanup(mux.SetBeforeLaneWait(func(op string, lane int) {
				if op == tc.op {
					waiting <- lane
				}
			}))

			done := make(chan error, 1)
			go func() { done <- tc.call(mux, ids) }()
			lanes := 1
			if tc.lane < 0 {
				lanes = mux.StreamCount()
			}
			for range lanes {
				select {
				case lane := <-waiting:
					if tc.lane >= 0 && lane != tc.lane {
						t.Fatalf("%s waits on lane %d, want lane %d", tc.op, lane, tc.lane)
					}
				case err := <-done:
					t.Fatalf("%s returned before it waited on every lane: %v", tc.op, err)
				case <-time.After(5 * time.Second):
					t.Fatalf("%s did not wait on its lanes", tc.op)
				}
			}
			if err := sdk.Close(); err != nil {
				t.Fatalf("SDK.Close: %v", err)
			}
			// Flush stores the lowest-index lane; a wait stores its own lane.
			lane := max(tc.lane, 0)
			assertLaneClosed(t, receive(t, done, tc.op), tc.op, lane)
			if !mux.IsClosed() {
				t.Error("IsClosed() = false after SDK.Close")
			}
			assertLaneClosed(t, mux.Close(), "Close", lane)
		})
	}
}

func TestMultiplexedFlush(t *testing.T) {
	t.Run("waits for every lane", func(t *testing.T) {
		srv := newHoldServer("mux-flush")
		mux := createMux(t, newMuxSDK(t, srv), 3)
		release := releaseHeldAck(t, srv)
		mustIngest(t, mux, muxRecord(0))
		mustIngest(t, mux, muxRecord(1))
		mustIngest(t, mux, []byte(`{"m":"hold"}`)) // lane 2, ack held
		releaseSoon(release)
		if err := mux.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		if !srv.heldAcked.Load() {
			t.Error("Flush returned before lane 2 got its ack")
		}
	})
	t.Run("WaitForMessageID waits for its own offset", func(t *testing.T) {
		srv := newHoldServer("mux-wait-offset")
		mux := createMux(t, newMuxSDK(t, srv), 2)
		release := releaseHeldAck(t, srv)
		id0 := mustIngest(t, mux, muxRecord(0)) // lane 0, offset 0
		mustIngest(t, mux, muxRecord(1))        // lane 1
		id2 := mustIngest(t, mux, []byte(`{"m":"hold"}`))
		if id2 != zerobus.NewMessageID(0, 1) {
			t.Fatalf("held record ID = %v, want lane 0 offset 1", id2)
		}
		if err := mux.WaitForMessageID(id0); err != nil {
			t.Fatalf("WaitForMessageID(%v): %v", id0, err)
		}

		// Offset 0 of lane 0 is acked, but the wait on offset 1 still waits for
		// its own ack.
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		if err := mux.WaitForMessageIDContext(ctx, id2); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("WaitForMessageIDContext(%v) with a held ack = %v, want DeadlineExceeded", id2, err)
		}
		if mux.IsClosed() {
			t.Error("IsClosed() = true after a WaitForMessageID timeout")
		}
		mustIngest(t, mux, muxRecord(3))
		release()
		if err := mux.WaitForMessageID(id2); err != nil {
			t.Errorf("WaitForMessageID(%v) after the ack: %v", id2, err)
		}
	})
	t.Run("timeout does not close", func(t *testing.T) {
		mux := createMux(t, newMuxSDK(t, &echoServer{streamID: "mux-flush-timeout", noAcks: true}), 2,
			zerobus.WithFlushTimeout(100*time.Millisecond))
		mustIngest(t, mux, muxRecord(1))
		err := mux.Flush()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Flush error = %v with no acks, want DeadlineExceeded", err)
		}
		if strings.Contains(err.Error(), "MultiplexedStream is closed") {
			t.Errorf("Flush error = %v, want the timeout, not the closed error", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := mux.FlushContext(ctx); !errors.Is(err, context.Canceled) {
			t.Errorf("FlushContext error = %v with a cancelled context, want Canceled", err)
		}
		if mux.IsClosed() {
			t.Error("IsClosed() = true after a Flush timeout")
		}
		mustIngest(t, mux, muxRecord(2))
	})
	t.Run("Close while Flush waits", func(t *testing.T) {
		srv := newHoldServer("mux-flush-close")
		mux := createMux(t, newMuxSDK(t, srv), 2, zerobus.WithFlushTimeout(50*time.Millisecond))
		releaseHeldAck(t, srv)
		mustIngest(t, mux, []byte(`{"m":"hold"}`)) // lane 0, ack held
		// Close runs after Flush has passed its closed check, and ends lane 0
		// before Flush waits on it.
		var once sync.Once
		t.Cleanup(mux.SetBeforeLaneWait(func(op string, lane int) {
			if op == "Flush" && lane == 0 {
				once.Do(func() { _ = mux.Close() })
			}
		}))
		assertMuxClosed(t, mux.Flush(), "Flush")
	})
}

func TestMultiplexedWaitForMessageIDRejectsUnknownLane(t *testing.T) {
	mux := createMux(t, newMuxSDK(t, &echoServer{streamID: "mux-bad-id"}), 2)
	for _, tc := range []struct {
		id   zerobus.MessageID
		lane int
	}{
		{id: zerobus.NewMessageID(5, 0), lane: 5},
		{id: zerobus.MessageID(-1), lane: 63},
	} {
		assertPermanentError(t, mux.WaitForMessageID(tc.id),
			fmt.Sprintf("zerobus: WaitForMessageID: invalid lane index %d in message id", tc.lane))
	}
}

// A clean double Close: TestCreateMultiplexedStreamOpensEveryLane.
func TestMultiplexedCloseReturnsOneErrorToEveryCaller(t *testing.T) {
	mux := createMux(t, newMuxSDK(t, &echoServer{streamID: "mux-close-error", noAcks: true}), 2,
		zerobus.WithFlushTimeout(50*time.Millisecond))
	mustIngest(t, mux, muxRecord(1))
	mustIngest(t, mux, muxRecord(2))

	errs := make(chan error, 2)
	for range 2 {
		go func() { errs <- mux.Close() }()
	}
	first, second := receive(t, errs, "Close"), receive(t, errs, "Close")
	third := mux.Close()
	if first == nil {
		t.Fatal("Close = nil with unacked records, want the flush timeout")
	}
	if second != first || third != first {
		t.Errorf("Close errors = %v, %v, %v, want one shared error", first, second, third)
	}
}

func TestMultiplexedCloseWaitsForRunningCallback(t *testing.T) {
	entered, unblock := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(unblock) })
	t.Cleanup(release)
	var once sync.Once
	cb := &funcMuxCallback{onAck: func(zerobus.MessageID) {
		once.Do(func() { close(entered) })
		<-unblock
	}}
	mux := createMux(t, newMuxSDK(t, &echoServer{streamID: "mux-callback-wait"}), 2, zerobus.WithMultiplexedAckCallback(cb))
	mustIngest(t, mux, muxRecord(0))
	if err := mux.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("OnAck did not run")
	}

	closed := make(chan error, 1)
	go func() { closed <- mux.Close() }()
	// Once every lane is closed, Close can only be waiting for the callback.
	for i := range mux.StreamCount() {
		waitLaneClosed(t, mux, i)
	}
	select {
	case err := <-closed:
		t.Fatalf("Close returned %v while OnAck still ran", err)
	case <-time.After(50 * time.Millisecond):
	}
	release()
	if err := receive(t, closed, "Close"); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestMultiplexedCloseWaitForCallbacksIsBounded(t *testing.T) {
	entered, unblock := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { close(unblock) })
	var once sync.Once
	cb := &funcMuxCallback{onAck: func(zerobus.MessageID) {
		once.Do(func() { close(entered) })
		<-unblock
	}}
	mux := createMux(t, newMuxSDK(t, &echoServer{streamID: "mux-callback-bound"}), 2,
		zerobus.WithMultiplexedAckCallback(cb), zerobus.WithCallbackTeardownTimeout(50*time.Millisecond))
	mustIngest(t, mux, muxRecord(0))
	if err := mux.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("OnAck did not run")
	}

	closed := make(chan error, 1)
	go func() { closed <- mux.Close() }()
	// The default limit is 5 s; the 50 ms limit must end the wait well before.
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close waited for a blocked callback past its limit")
	}
}

func TestMultiplexedCloseRacesIngest(t *testing.T) {
	rec := &recordingMuxCallback{}
	sdk := newMuxSDK(t, &echoServer{streamID: "mux-close-race"})
	mux := createMux(t, sdk, 4, zerobus.WithMultiplexedAckCallback(rec))

	const workers = 8
	start := make(chan struct{})
	var firstIngest, done sync.WaitGroup
	firstIngest.Add(workers)
	ids := make([][]zerobus.MessageID, workers)
	lastErrs := make([]error, workers)
	for w := range workers {
		done.Add(1)
		go func() {
			defer done.Done()
			<-start
			for {
				id, err := mux.IngestRecord(muxRecord(w))
				if len(ids[w]) == 0 {
					firstIngest.Done()
				}
				if err != nil {
					lastErrs[w] = err
					return
				}
				ids[w] = append(ids[w], id)
			}
		}()
	}
	close(start)
	firstIngest.Wait()
	if err := mux.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	done.Wait()

	var all []zerobus.MessageID
	for w := range workers {
		if len(ids[w]) == 0 {
			t.Fatalf("worker %d ingested nothing before Close: %v", w, lastErrs[w])
		}
		assertMuxClosed(t, lastErrs[w], "IngestRecord")
		all = append(all, ids[w]...)
	}
	// Close returns after every lane's callbacks ran.
	assertSameIDs(t, rec.ackIDs(), all)
}

func TestMultiplexedCloseClosesLanesConcurrently(t *testing.T) {
	const n = 3
	mux := createMux(t, newMuxSDK(t, &echoServer{streamID: "mux-close-concurrent"}), n)
	for i := range n {
		mustIngest(t, mux, muxRecord(i))
	}
	// Each lane waits until every lane has started to close.
	var started atomic.Int32
	allStarted := make(chan struct{})
	t.Cleanup(mux.SetBeforeLaneClose(func(lane int) {
		if started.Add(1) == n {
			close(allStarted)
		}
		select {
		case <-allStarted:
		case <-time.After(time.Second):
			t.Errorf("lane %d: the lanes did not close concurrently", lane)
		}
	}))
	if err := mux.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// Lane 0 fails with "tag-a" and lane 1 with "tag-b"; no call has seen either
// failure when the observer under test runs.
func TestMultiplexedLowestIndexLaneFailureWins(t *testing.T) {
	bothLanesFailed := func(t *testing.T, failAfterAck bool) *zerobus.MultiplexedStream {
		t.Helper()
		sdk := newMuxSDK(t, &echoServer{streamID: "mux-lowest", failMarker: "tag-", failAfterAck: failAfterAck})
		mux := createMux(t, sdk, 2)
		mustIngest(t, mux, []byte(`{"m":"tag-a"}`))
		mustIngest(t, mux, []byte(`{"m":"tag-b"}`))
		waitLaneClosed(t, mux, 0)
		waitLaneClosed(t, mux, 1)
		return mux
	}
	t.Run("Flush", func(t *testing.T) {
		mux := bothLanesFailed(t, false)
		if mux.IsClosed() {
			t.Error("IsClosed() = true before any call saw a lane failure")
		}
		assertRejected(t, mux.Flush(), "tag-a")
		if !mux.IsClosed() {
			t.Error("IsClosed() = false after Flush saw the lane failures")
		}
		_, err := mux.IngestRecord(muxRecord(1))
		assertRejected(t, err, "tag-a")
	})
	t.Run("Close", func(t *testing.T) {
		mux := bothLanesFailed(t, true)
		assertRejected(t, mux.Close(), "tag-a")
		// Close stores the cause it found.
		_, err := mux.IngestRecord(muxRecord(1))
		assertRejected(t, err, "tag-a")
		assertRejected(t, mux.Flush(), "tag-a")
		assertRejected(t, mux.Close(), "tag-a")
	})
}

func TestMultiplexedAckCallbackReportsErrorIDs(t *testing.T) {
	rec := &recordingMuxCallback{}
	mux := createMux(t, newMuxSDK(t, &echoServer{streamID: "mux-ack-errors", noAcks: true}), 2,
		zerobus.WithMultiplexedAckCallback(rec), zerobus.WithFlushTimeout(50*time.Millisecond))

	var ids []zerobus.MessageID
	for i := range 4 {
		ids = append(ids, mustIngest(t, mux, muxRecord(i)))
	}
	_ = mux.Close()
	assertSameIDs(t, rec.errorIDs(), ids)
}

func TestMultiplexedAckCallbackPanicIsLogged(t *testing.T) {
	var logs syncBuffer
	prevOut, prevFlags := log.Writer(), log.Flags()
	log.SetOutput(&logs)
	t.Cleanup(func() {
		log.SetOutput(prevOut)
		log.SetFlags(prevFlags)
	})
	rec := &recordingMuxCallback{}
	var once sync.Once
	cb := &funcMuxCallback{onAck: func(id zerobus.MessageID) {
		once.Do(func() { panic("mux callback failure") })
		rec.OnAck(id)
	}}
	mux := createMux(t, newMuxSDK(t, &echoServer{streamID: "mux-ack-panic"}), 2, zerobus.WithMultiplexedAckCallback(cb))

	var ids []zerobus.MessageID
	for i := range 4 {
		ids = append(ids, mustIngest(t, mux, muxRecord(i)))
	}
	if err := mux.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := logs.String(); !strings.Contains(got, "zerobus: MultiplexedAckCallback.OnAck(message ID ") ||
		!strings.Contains(got, "panicked: mux callback failure") {
		t.Errorf("log = %q, want the recovered panic", got)
	}
	if got := rec.ackIDs(); len(got) != len(ids)-1 {
		t.Errorf("OnAck after the panic ran for %v, want the other %d of %v", got, len(ids)-1, ids)
	}
}

func TestMultiplexedAckCallbackCanCallStream(t *testing.T) {
	var mux *zerobus.MultiplexedStream
	type result struct {
		ingest, flush, wait error
	}
	results := make(chan result, 1)
	var once sync.Once
	cb := &funcMuxCallback{onAck: func(zerobus.MessageID) {
		once.Do(func() {
			var r result
			var id zerobus.MessageID
			id, r.ingest = mux.IngestRecord(muxRecord(1))
			r.flush = mux.Flush()
			if r.ingest == nil {
				r.wait = mux.WaitForMessageID(id)
			}
			results <- r
		})
	}}
	mux = createMux(t, newMuxSDK(t, &echoServer{streamID: "mux-ack-calls"}), 2, zerobus.WithMultiplexedAckCallback(cb))
	mustIngest(t, mux, muxRecord(0))

	select {
	case r := <-results:
		if r.ingest != nil || r.flush != nil || r.wait != nil {
			t.Errorf("calls from OnAck: IngestRecord %v, Flush %v, WaitForMessageID %v, want all nil", r.ingest, r.flush, r.wait)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("calls from OnAck did not return")
	}
	if err := mux.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// GetUnackedRecords closes the stream first and ignores the close error.
func TestMultiplexedGetUnackedClosesStream(t *testing.T) {
	t.Run("open", func(t *testing.T) {
		mux := createMux(t, newMuxSDK(t, &echoServer{streamID: "mux-unacked-open", noAcks: true}), 2,
			zerobus.WithFlushTimeout(50*time.Millisecond))
		r0, r1 := muxRecord(0), muxRecord(1)
		mustIngest(t, mux, r0)
		mustIngest(t, mux, r1)

		records, err := mux.GetUnackedRecords()
		if err != nil {
			t.Fatalf("GetUnackedRecords: %v", err)
		}
		if want := [][]byte{r0, r1}; !slices.EqualFunc(records, want, bytes.Equal) {
			t.Errorf("GetUnackedRecords = %q, want %q", records, want)
		}
		if !mux.IsClosed() {
			t.Error("IsClosed() = false after GetUnackedRecords")
		}
		_, err = mux.IngestRecord(muxRecord(2))
		assertMuxClosed(t, err, "IngestRecord")
		batches, err := mux.GetUnackedBatches()
		if err != nil {
			t.Fatalf("GetUnackedBatches: %v", err)
		}
		if want := [][][]byte{{r0}, {r1}}; !slices.EqualFunc(batches, want, func(x, y [][]byte) bool {
			return slices.EqualFunc(x, y, bytes.Equal)
		}) {
			t.Errorf("GetUnackedBatches = %q, want %q", batches, want)
		}
		// Close returns the result of the close that GetUnackedRecords ran.
		if err := mux.Close(); err == nil {
			t.Error("Close = nil with unacked records, want the flush timeout")
		}
	})
	t.Run("lane failed", func(t *testing.T) {
		srv := &echoServer{streamID: "mux-unacked-failed", noAcks: true, failMarker: "poison"}
		mux := createMux(t, newMuxSDK(t, srv), 2, zerobus.WithFlushTimeout(50*time.Millisecond))
		stranded, poisoned := []byte(`{"m":"stranded"}`), []byte(`{"m":"poison-r1"}`)
		mustIngest(t, mux, stranded) // lane 0, never acked
		mustIngest(t, mux, poisoned) // lane 1 fails
		waitLaneClosed(t, mux, 1)

		records, err := mux.GetUnackedRecords()
		if err != nil {
			t.Fatalf("GetUnackedRecords: %v", err)
		}
		if want := [][]byte{stranded, poisoned}; !slices.EqualFunc(records, want, bytes.Equal) {
			t.Errorf("GetUnackedRecords = %q, want %q (lane 0 first)", records, want)
		}
		assertRejected(t, mux.Close(), "poison-r1")
	})
}

func TestMultiplexedGetUnackedAfterClose(t *testing.T) {
	mux := createMux(t, newMuxSDK(t, &echoServer{streamID: "mux-unacked", noAcks: true}), 2,
		zerobus.WithFlushTimeout(50*time.Millisecond))
	r := [][]byte{muxRecord(0), muxRecord(1), muxRecord(2), muxRecord(3)}
	b := [][]byte{muxRecord(10), muxRecord(11)}
	var ids []zerobus.MessageID
	for _, record := range r {
		ids = append(ids, mustIngest(t, mux, record))
	}
	if id, ok, err := mux.IngestRecords(b); err != nil || !ok || zerobus.MessageIDStreamIndex(id) != 0 {
		t.Fatalf("IngestRecords = (%v, %v, %v), want an ID on lane 0", id, ok, err)
	}
	_ = mux.Close()
	// Close abandoned the record, so the wait fails with the lane closed error.
	assertLaneClosed(t, mux.WaitForMessageID(ids[0]), "WaitForMessageID", 0)

	records, err := mux.GetUnackedRecords()
	if err != nil {
		t.Fatalf("GetUnackedRecords: %v", err)
	}
	wantRecords := [][]byte{r[0], r[2], b[0], b[1], r[1], r[3]}
	if !slices.EqualFunc(records, wantRecords, bytes.Equal) {
		t.Errorf("GetUnackedRecords = %q, want %q (lane 0 first)", records, wantRecords)
	}
	batches, err := mux.GetUnackedBatches()
	if err != nil {
		t.Fatalf("GetUnackedBatches: %v", err)
	}
	wantBatches := [][][]byte{{r[0]}, {r[2]}, b, {r[1]}, {r[3]}}
	if !slices.EqualFunc(batches, wantBatches, func(x, y [][]byte) bool {
		return slices.EqualFunc(x, y, bytes.Equal)
	}) {
		t.Errorf("GetUnackedBatches = %q, want %q (lane 0 first)", batches, wantBatches)
	}
}
