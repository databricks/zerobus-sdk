package zerobus_test

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/databricks/zerobus-sdk/purego/internal/stream"
	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
	"github.com/databricks/zerobus-sdk/purego/zerobus"
)

const muxTable = "main.sales.orders"

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

type nopAckCallback struct{}

func (nopAckCallback) OnAck(int64)          {}
func (nopAckCallback) OnError(int64, error) {}

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
			mux := createMux(t, sdk, tc.n)
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
		})
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

func TestMultiplexedCloseClosesLanesConcurrently(t *testing.T) {
	const n = 3
	mux := createMux(t, newMuxSDK(t, &echoServer{streamID: "mux-close-concurrent"}), n)
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
