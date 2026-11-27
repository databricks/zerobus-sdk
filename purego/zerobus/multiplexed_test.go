package zerobus_test

import (
	"context"
	"errors"
	"math"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/databricks/zerobus-sdk/purego/zerobus"
)

const maxMessageIDOffset = int64(1)<<58 - 1

func TestMessageIDPacking(t *testing.T) {
	if zerobus.MaxMultiplexedStreams != 64 {
		t.Fatalf("MaxMultiplexedStreams = %d, want 64", zerobus.MaxMultiplexedStreams)
	}
	tests := []struct {
		name        string
		streamIndex int
		offset      int64
		wantRaw     int64
	}{
		{name: "lane 0 offset 0", streamIndex: 0, offset: 0, wantRaw: 0},
		{name: "lane 0 max offset", streamIndex: 0, offset: maxMessageIDOffset, wantRaw: maxMessageIDOffset},
		{name: "middle lane", streamIndex: 1, offset: 5, wantRaw: 1<<58 | 5},
		{name: "lane 32 sets the sign bit", streamIndex: 32, offset: 0, wantRaw: math.MinInt64},
		{name: "lane 32 with offset", streamIndex: 32, offset: 7, wantRaw: math.MinInt64 + 7},
		{name: "lane 63", streamIndex: 63, offset: 0, wantRaw: math.MinInt64 + 31<<58},
		{name: "max ID is -1", streamIndex: 63, offset: maxMessageIDOffset, wantRaw: -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id := zerobus.NewMessageID(tc.streamIndex, tc.offset)
			if int64(id) != tc.wantRaw {
				t.Fatalf("NewMessageID(%d, %d) = %d, want %d", tc.streamIndex, tc.offset, int64(id), tc.wantRaw)
			}
			if got := zerobus.MessageIDStreamIndex(id); got != tc.streamIndex {
				t.Errorf("lane index = %d, want %d", got, tc.streamIndex)
			}
			if got := zerobus.MessageIDOffset(id); got != tc.offset {
				t.Errorf("lane offset = %d, want %d", got, tc.offset)
			}
			raw := zerobus.MessageID(tc.wantRaw)
			if got := zerobus.NewMessageID(zerobus.MessageIDStreamIndex(raw), zerobus.MessageIDOffset(raw)); got != raw {
				t.Errorf("round trip of raw %d = %d", tc.wantRaw, int64(got))
			}
		})
	}

	// Offset bits above the low 58 may alias, but must never change the lane.
	id := zerobus.NewMessageID(2, 1<<58)
	if got := zerobus.MessageIDStreamIndex(id); got != 2 {
		t.Errorf("lane index with an oversized offset = %d, want 2", got)
	}
	if got := zerobus.MessageIDOffset(id); got != 0 {
		t.Errorf("lane offset with an oversized offset = %d, want 0", got)
	}
}

// recordingMuxCallback records every MultiplexedAckCallback call.
type recordingMuxCallback struct {
	mu     sync.Mutex
	acks   []zerobus.MessageID
	errIDs []zerobus.MessageID
	errs   []error
}

func (c *recordingMuxCallback) OnAck(id zerobus.MessageID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.acks = append(c.acks, id)
}

func (c *recordingMuxCallback) OnError(id zerobus.MessageID, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errIDs = append(c.errIDs, id)
	c.errs = append(c.errs, err)
}

func TestMultiplexedLaneAckCallbackMapsOffsets(t *testing.T) {
	rec := &recordingMuxCallback{}
	sentinel := errors.New("lane failed")

	lane3 := zerobus.MultiplexedLaneAckCallback(3, rec)
	lane3.OnAck(7)
	lane3.OnError(9, sentinel)
	zerobus.MultiplexedLaneAckCallback(63, rec).OnAck(maxMessageIDOffset)
	zerobus.MultiplexedLaneAckCallback(0, rec).OnAck(0)

	wantAcks := []zerobus.MessageID{zerobus.NewMessageID(3, 7), zerobus.MessageID(-1), zerobus.MessageID(0)}
	if len(rec.acks) != len(wantAcks) {
		t.Fatalf("OnAck IDs = %v, want %v", rec.acks, wantAcks)
	}
	for i, want := range wantAcks {
		if rec.acks[i] != want {
			t.Errorf("OnAck ID[%d] = %v, want %v", i, rec.acks[i], want)
		}
	}
	if len(rec.errIDs) != 1 || rec.errIDs[0] != zerobus.NewMessageID(3, 9) {
		t.Fatalf("OnError IDs = %v, want [%v]", rec.errIDs, zerobus.NewMessageID(3, 9))
	}
	if rec.errs[0] != sentinel {
		t.Errorf("OnError error = %v, want the lane error passed through unchanged", rec.errs[0])
	}
}

func TestCreateStreamRejectsMultiplexedAckCallbackBeforeTokenRequest(t *testing.T) {
	uc := &ucTokenServer{}
	ucSrv := httptest.NewServer(uc)
	defer ucSrv.Close()

	conn := dialEcho(t, &echoServer{streamID: "stream-mux-callback-oauth"})
	sdk := zerobus.NewWithConn(conn, "https://ws.zerobus.databricks.com", ucSrv.URL)
	defer sdk.Close()

	st, err := sdk.CreateStream(context.Background(), "main.sales.orders", "client-id", "client-secret",
		zerobus.WithJSON(), zerobus.WithMultiplexedAckCallback(&recordingMuxCallback{}))
	if err == nil {
		_ = st.Close()
		t.Fatal("CreateStream succeeded, want the multiplexed callback rejected")
	}
	assertMuxCallbackRejected(t, err, "CreateStream")
	if got := uc.mints.Load(); got != 0 {
		t.Errorf("token mints = %d, want 0 (rejection runs before any network call)", got)
	}
}

func assertMuxCallbackRejected(t *testing.T, err error, op string) {
	t.Helper()
	var sdkErr *zerobus.Error
	if !errors.As(err, &sdkErr) {
		t.Fatalf("error %v is not a *zerobus.Error", err)
	}
	if zerobus.Retryable(err) {
		t.Errorf("error %v is retryable, want permanent", err)
	}
	want := "zerobus: " + op + ": WithMultiplexedAckCallback is only valid with CreateMultiplexedStream"
	if got := err.Error(); got != want {
		t.Errorf("error = %q, want %q", got, want)
	}
}

func TestCreateStreamNilMultiplexedAckCallbackIsUnset(t *testing.T) {
	conn := dialEcho(t, &echoServer{streamID: "stream-mux-callback-nil"})
	sdk := zerobus.NewWithConn(conn, "https://ws.zerobus.databricks.com", "https://ws.databricks.com")
	defer sdk.Close()
	provider := zerobus.NewStaticHeadersProvider(map[string]string{"authorization": "Bearer t"})

	// A later nil option clears an earlier callback.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := sdk.CreateStreamWithProvider(ctx, "main.sales.orders", provider, zerobus.WithJSON(),
		zerobus.WithMultiplexedAckCallback(&recordingMuxCallback{}), zerobus.WithMultiplexedAckCallback(nil))
	if err != nil {
		t.Fatalf("CreateStreamWithProvider: %v", err)
	}
	if n := sdk.OpenStreamCount(); n != 1 {
		t.Errorf("SDK tracks %d streams, want 1", n)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
