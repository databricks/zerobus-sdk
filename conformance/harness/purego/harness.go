// TestHarness service: maps each contract RPC onto the pure-Go Zerobus SDK
// (purego/zerobus).
//
// Supported slice: protocol=GRPC with format=PROTO or JSON. Every other combination
// returns UNIMPLEMENTED with a TODO (see checkSliceSupported).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/databricks/zerobus-sdk/conformance/harness/purego/internal/pb"
	"github.com/databricks/zerobus-sdk/purego/zerobus"
)

// createStreamTimeout bounds stream open. CreateStream waits for first-open
// (WithWaitForReady), so a dead endpoint fails here rather than on Flush.
const createStreamTimeout = 30 * time.Second

// streamEntry is one open SDK stream plus what its records are built against.
type streamEntry struct {
	sdk    *zerobus.SDK
	stream *zerobus.Stream
	// desc is the proto message descriptor; nil for JSON streams.
	desc   protoreflect.MessageDescriptor
	format pb.RecordFormat
	// mu serializes per-stream RPCs (ingest / await / flush / close). The map lock
	// in harnessService is never held across an SDK call.
	mu sync.Mutex
}

type harnessService struct {
	pb.UnimplementedTestHarnessServer

	mu      sync.Mutex // guards streams only
	streams map[string]*streamEntry
	nextID  atomic.Uint64

	shutdownOnce sync.Once
	shutdownCh   chan struct{} // closed by the Shutdown RPC
}

func newHarnessService() *harnessService {
	return &harnessService{
		streams:    make(map[string]*streamEntry),
		shutdownCh: make(chan struct{}),
	}
}

func (h *harnessService) lookup(streamID string) (*streamEntry, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	e, ok := h.streams[streamID]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown stream_id %q", streamID)
	}
	return e, nil
}

// closeAll terminates any streams the orchestrator did not Close. Called only after
// the gRPC server has stopped, so no RPC can race it. It uses SDK.Close (no flush),
// so shutdown does not block on acks.
func (h *harnessService) closeAll() {
	h.mu.Lock()
	entries := make([]*streamEntry, 0, len(h.streams))
	for id, e := range h.streams {
		delete(h.streams, id)
		entries = append(entries, e)
	}
	h.mu.Unlock()
	for _, e := range entries {
		if err := e.sdk.Close(); err != nil {
			slog.Warn("closing leftover stream at shutdown", "err", err)
		}
	}
}

// checkSliceSupported rejects every protocol/format combination this slice does not
// implement. Each TODO names the work that remains.
func checkSliceSupported(protocol pb.Protocol, format pb.RecordFormat) error {
	switch {
	case protocol == pb.Protocol_GRPC && format == pb.RecordFormat_PROTO:
		return nil
	case protocol == pb.Protocol_GRPC && format == pb.RecordFormat_JSON:
		return nil
	case protocol == pb.Protocol_GRPC && format == pb.RecordFormat_AVRO:
		return status.Error(codes.Unimplemented,
			"TODO(conformance): GRPC + AVRO not implemented in the purego harness yet (purego has no Avro support)")
	case protocol == pb.Protocol_GRPC && format == pb.RecordFormat_ARROW:
		return status.Error(codes.Unimplemented,
			"TODO(conformance): GRPC + ARROW is not a valid pairing; ARROW rides ARROW_FLIGHT")
	case protocol == pb.Protocol_ARROW_FLIGHT:
		return status.Error(codes.Unimplemented,
			"TODO(conformance): ARROW_FLIGHT protocol not implemented in the purego harness yet (purego has no Arrow Flight support)")
	default:
		return status.Error(codes.InvalidArgument, "protocol and format must be specified")
	}
}

// toStatus maps an SDK error onto a gRPC status. A stream that is already closed or
// failed is FAILED_PRECONDITION; anything else is INTERNAL with the SDK message.
func toStatus(err error, closed bool) error {
	if err == nil {
		return nil
	}
	if closed {
		return status.Errorf(codes.FailedPrecondition, "stream closed: %v", err)
	}
	return status.Error(codes.Internal, err.Error())
}

// noAuthHeadersProvider sends no auth metadata. The mock checks no credentials.
// (purego's NewStaticHeadersProvider rejects an empty map, so it cannot be used.)
type noAuthHeadersProvider struct{}

func (noAuthHeadersProvider) GetHeaders(context.Context, string) (map[string]string, error) {
	return map[string]string{}, nil
}

func (noAuthHeadersProvider) Invalidate(context.Context, string) {}

func (h *harnessService) CreateStream(ctx context.Context, req *pb.CreateStreamRequest) (*pb.CreateStreamResponse, error) {
	if err := checkSliceSupported(req.GetProtocol(), req.GetFormat()); err != nil {
		return nil, err
	}

	var desc protoreflect.MessageDescriptor
	streamOpts := []zerobus.StreamOption{zerobus.WithWaitForReady()}
	switch req.GetFormat() {
	case pb.RecordFormat_PROTO:
		d, err := descriptorFromBytes(req.GetDescriptorProtoBytes())
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		desc = d
		streamOpts = append(streamOpts, zerobus.WithProto(req.GetDescriptorProtoBytes()))
	case pb.RecordFormat_JSON:
		streamOpts = append(streamOpts, zerobus.WithJSON())
	}

	slog.Info("CreateStream", "endpoint", req.GetServerEndpoint(), "table", req.GetTablePath(), "format", req.GetFormat())

	// Plaintext (the mock is loopback, no TLS) and no auth. The SDK is built per
	// stream, as in the Rust harness, so each stream owns its connection.
	// ucEndpoint is unused on the custom-provider path.
	sdk, err := zerobus.New(req.GetServerEndpoint(), "",
		zerobus.WithInsecureTransportForTesting())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	openCtx, cancel := context.WithTimeout(ctx, createStreamTimeout)
	defer cancel()
	st, err := sdk.CreateStreamWithProvider(openCtx, req.GetTablePath(), noAuthHeadersProvider{}, streamOpts...)
	if err != nil {
		_ = sdk.Close()
		return nil, status.Error(codes.Internal, err.Error())
	}

	streamID := fmt.Sprintf("stream-%d", h.nextID.Add(1)-1)
	h.mu.Lock()
	h.streams[streamID] = &streamEntry{sdk: sdk, stream: st, desc: desc, format: req.GetFormat()}
	h.mu.Unlock()

	return &pb.CreateStreamResponse{StreamId: streamID}, nil
}

func (h *harnessService) IngestRecord(ctx context.Context, req *pb.IngestRecordRequest) (*pb.IngestRecordResponse, error) {
	e, err := h.lookup(req.GetStreamId())
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	var payload []byte
	if e.format == pb.RecordFormat_JSON {
		payload, err = jsonRecordPayload(req.GetRecordJson())
	} else {
		payload, err = encodeProtoRecord(e.desc, req.GetRecordJson())
	}
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Queue only: returns once the SDK has admitted the record, not once it is
	// acknowledged. Waiting here would cost one round-trip per record.
	offset, err := e.stream.IngestRecordOffsetContext(ctx, payload)
	if err != nil {
		return nil, toStatus(err, e.stream.IsClosed())
	}
	return &pb.IngestRecordResponse{Offset: offset}, nil
}

func (h *harnessService) AwaitWriteCompleted(ctx context.Context, req *pb.AwaitRequest) (*pb.AwaitResponse, error) {
	e, err := h.lookup(req.GetStreamId())
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	// WaitForOffset has no timeout of its own, so timeout_ms is applied here.
	// A value <= 0 waits with no deadline (the SDK's FlushTimeout still applies).
	wctx := ctx
	if ms := req.GetTimeoutMs(); ms > 0 {
		var cancel context.CancelFunc
		wctx, cancel = context.WithTimeout(ctx, time.Duration(ms)*time.Millisecond)
		defer cancel()
	}

	// Per the contract, a failed wait is reported in the body, not as a gRPC error.
	// Empty error_message means success.
	if err := e.stream.WaitForOffsetContext(wctx, req.GetOffset()); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return &pb.AwaitResponse{ErrorMessage: fmt.Sprintf(
				"timed out after %d ms waiting for offset %d", req.GetTimeoutMs(), req.GetOffset())}, nil
		}
		return &pb.AwaitResponse{ErrorMessage: err.Error()}, nil
	}
	return &pb.AwaitResponse{}, nil
}

func (h *harnessService) Flush(ctx context.Context, req *pb.FlushRequest) (*pb.Empty, error) {
	e, err := h.lookup(req.GetStreamId())
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.stream.FlushContext(ctx); err != nil {
		return nil, toStatus(err, e.stream.IsClosed())
	}
	return &pb.Empty{}, nil
}

func (h *harnessService) Close(ctx context.Context, req *pb.CloseRequest) (*pb.Empty, error) {
	// Remove first so new RPCs on this id see NOT_FOUND. The entry is dropped even if
	// the SDK close fails.
	h.mu.Lock()
	e, ok := h.streams[req.GetStreamId()]
	delete(h.streams, req.GetStreamId())
	h.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown stream_id %q", req.GetStreamId())
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	// stream.Close flushes queued records and then tears the stream down. The SDK
	// is closed regardless, so its connection is released even if the flush failed.
	closeErr := e.stream.Close()
	_ = e.sdk.Close()
	if closeErr != nil {
		return nil, toStatus(closeErr, false)
	}
	slog.Info("stream closed", "stream_id", req.GetStreamId())
	return &pb.Empty{}, nil
}

func (h *harnessService) Shutdown(context.Context, *pb.Empty) (*pb.Empty, error) {
	slog.Info("Shutdown RPC received")
	// Only signal here. main calls GracefulStop after this response is written;
	// calling GracefulStop from inside a handler would wait on itself.
	h.shutdownOnce.Do(func() { close(h.shutdownCh) })
	return &pb.Empty{}, nil
}
