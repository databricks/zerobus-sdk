// TestHarness service: maps each contract RPC onto the pure-Go Zerobus SDK
// (purego/zerobus). Each RPC is a pass-through to the SDK call of the same meaning;
// the harness adds only record conversion and stream_id bookkeeping.
//
// Error model (per the contract): an error the SDK under test returns is reported in
// the response's SdkError with a nil gRPC error. A gRPC status is reserved for
// harness-internal faults (unknown stream_id, unsupported protocol/format, records or
// schemas the harness cannot convert, an empty batch, internal bugs).
//
// Deadlines: the orchestrator's RPC deadline is not forwarded to the SDK. Every SDK
// call runs under sdkCtx, so only the SDK's own ack/flush timeouts bound it.
//
// Supported slice: protocol=GRPC with format=PROTO or JSON. Every other combination
// returns UNIMPLEMENTED with a TODO (see checkSliceSupported).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/databricks/zerobus-sdk/conformance/harness/purego/internal/pb"
	"github.com/databricks/zerobus-sdk/purego/zerobus"
)

// streamEntry is one SDK stream plus what its records are built against. Entries stay
// registered after Close, so later RPCs on the id reach the SDK and get its own error.
type streamEntry struct {
	sdk    *zerobus.SDK
	stream *zerobus.Stream
	// desc is the proto message descriptor; nil for JSON streams.
	desc   protoreflect.MessageDescriptor
	format pb.RecordFormat
}

type harnessService struct {
	pb.UnimplementedTestHarnessServer

	// mu guards the streams map only. It is never held across an SDK call, so the
	// SDK's own goroutine safety is what governs concurrent RPCs on one stream.
	mu      sync.Mutex
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

// lookup returns the entry for a stream_id the harness created. Only an id it never
// created is NOT_FOUND; a closed stream is still found.
func (h *harnessService) lookup(streamID string) (*streamEntry, error) {
	h.mu.Lock()
	e, ok := h.streams[streamID]
	h.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown stream_id %q", streamID)
	}
	return e, nil
}

// closeAll terminates any streams the orchestrator did not Close. Called only after
// the gRPC server has stopped, so no RPC can race it. It uses SDK.Close (no flush),
// so shutdown does not block on acks. SDK.Close is idempotent, so streams already
// closed by the orchestrator are harmless to close again.
func (h *harnessService) closeAll() {
	h.mu.Lock()
	entries := make([]*streamEntry, 0, len(h.streams))
	for _, e := range h.streams {
		entries = append(entries, e)
	}
	h.mu.Unlock()
	for _, e := range entries {
		if err := e.sdk.Close(); err != nil {
			slog.Warn("closing leftover stream at shutdown", "err", err)
		}
	}
}

// sdkCtx is the context for every SDK call. It has no deadline or cancellation from
// the orchestrator, so the SDK's own timeouts alone bound each call.
func sdkCtx() context.Context {
	return context.Background()
}

// checkSliceSupported rejects every protocol/format combination this slice does not
// implement. Each TODO names the work that remains.
func checkSliceSupported(protocol pb.Protocol, format pb.RecordFormat) error {
	switch {
	case protocol == pb.Protocol_PROTOCOL_GRPC && format == pb.RecordFormat_RECORD_FORMAT_PROTO:
		return nil
	case protocol == pb.Protocol_PROTOCOL_GRPC && format == pb.RecordFormat_RECORD_FORMAT_JSON:
		return nil
	case protocol == pb.Protocol_PROTOCOL_GRPC && format == pb.RecordFormat_RECORD_FORMAT_AVRO:
		return status.Error(codes.Unimplemented,
			"TODO(conformance): GRPC + AVRO not implemented in the purego harness yet (purego has no Avro support)")
	case protocol == pb.Protocol_PROTOCOL_GRPC && format == pb.RecordFormat_RECORD_FORMAT_ARROW:
		return status.Error(codes.Unimplemented,
			"TODO(conformance): GRPC + ARROW is not a valid pairing; ARROW rides ARROW_FLIGHT")
	case protocol == pb.Protocol_PROTOCOL_ARROW_FLIGHT:
		return status.Error(codes.Unimplemented,
			"TODO(conformance): ARROW_FLIGHT protocol not implemented in the purego harness yet (purego has no Arrow Flight support)")
	default:
		return status.Error(codes.InvalidArgument, "protocol and format must be specified")
	}
}

// sdkError converts an error returned by the SDK under test into the contract's
// SdkError. Retryable is the SDK's own classification (zerobus.Retryable). A nil error
// yields nil, meaning success.
func sdkError(err error) *pb.SdkError {
	if err == nil {
		return nil
	}
	return &pb.SdkError{Message: err.Error(), Retryable: zerobus.Retryable(err)}
}

// encodePayload turns one canonical record_json into the SDK's native payload: the
// record bytes unchanged for a JSON stream, serialized protobuf for a proto stream. A
// failure here is a harness-side conversion error (the record does not match the
// schema), so the caller reports it as a gRPC status, not an SdkError.
func (e *streamEntry) encodePayload(recordJSON string) ([]byte, error) {
	if e.format == pb.RecordFormat_RECORD_FORMAT_JSON {
		return jsonRecordPayload(recordJSON), nil
	}
	return encodeProtoRecord(e.desc, recordJSON)
}

// noAuthHeadersProvider sends no auth metadata. The mock checks no credentials.
// (purego's NewStaticHeadersProvider rejects an empty map, so it cannot be used.)
type noAuthHeadersProvider struct{}

func (noAuthHeadersProvider) GetHeaders(context.Context, string) (map[string]string, error) {
	return map[string]string{}, nil
}

func (noAuthHeadersProvider) Invalidate(context.Context, string) {}

// abandon releases an SDK whose stream will not be handed to the caller. The response
// carries a single error, so the primary error is the one the caller sees; a failure
// to release the SDK is logged rather than dropped.
func abandon(sdk *zerobus.SDK, st *zerobus.Stream) {
	if st != nil {
		if err := st.Close(); err != nil {
			slog.Warn("closing abandoned stream", "err", err)
		}
	}
	if err := sdk.Close(); err != nil {
		slog.Warn("closing abandoned SDK", "err", err)
	}
}

func (h *harnessService) CreateStream(_ context.Context, req *pb.CreateStreamRequest) (*pb.CreateStreamResponse, error) {
	if err := checkSliceSupported(req.GetProtocol(), req.GetFormat()); err != nil {
		return nil, err
	}

	var streamOpts []zerobus.StreamOption
	switch req.GetFormat() {
	case pb.RecordFormat_RECORD_FORMAT_PROTO:
		streamOpts = append(streamOpts, zerobus.WithProto(req.GetDescriptorProtoBytes()))
	case pb.RecordFormat_RECORD_FORMAT_JSON:
		streamOpts = append(streamOpts, zerobus.WithJSON())
	}

	slog.Info("CreateStream", "endpoint", req.GetServerEndpoint(), "table", req.GetTablePath(), "format", req.GetFormat())

	// Plaintext (the mock is loopback, no TLS) and no auth. The SDK is built per
	// stream, as in the Rust harness, so each stream owns its connection.
	// ucEndpoint is unused on the custom-provider path.
	sdk, err := zerobus.New(req.GetServerEndpoint(), "",
		zerobus.WithInsecureTransportForTesting())
	if err != nil {
		return &pb.CreateStreamResponse{Error: sdkError(err)}, nil
	}

	st, err := sdk.CreateStreamWithProvider(sdkCtx(), req.GetTablePath(), noAuthHeadersProvider{}, streamOpts...)
	if err != nil {
		abandon(sdk, nil)
		return &pb.CreateStreamResponse{Error: sdkError(err)}, nil
	}

	// The SDK takes a proto descriptor as opaque bytes and does not parse it for proto
	// streams, so the harness parses it here to encode records. This runs after the SDK
	// has accepted the stream; a descriptor the harness cannot parse is a fixture fault.
	var desc protoreflect.MessageDescriptor
	if req.GetFormat() == pb.RecordFormat_RECORD_FORMAT_PROTO {
		d, err := descriptorFromBytes(req.GetDescriptorProtoBytes())
		if err != nil {
			abandon(sdk, st)
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		desc = d
	}

	streamID := fmt.Sprintf("stream-%d", h.nextID.Add(1)-1)
	h.mu.Lock()
	h.streams[streamID] = &streamEntry{sdk: sdk, stream: st, desc: desc, format: req.GetFormat()}
	h.mu.Unlock()

	return &pb.CreateStreamResponse{StreamId: streamID}, nil
}

func (h *harnessService) IngestRecord(_ context.Context, req *pb.IngestRecordRequest) (*pb.IngestRecordResponse, error) {
	e, err := h.lookup(req.GetStreamId())
	if err != nil {
		return nil, err
	}

	payload, err := e.encodePayload(req.GetRecordJson())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	offset, err := e.stream.IngestRecordOffsetContext(sdkCtx(), payload)
	if err != nil {
		return &pb.IngestRecordResponse{Error: sdkError(err)}, nil
	}
	return &pb.IngestRecordResponse{Offset: offset}, nil
}

// IngestRecords converts every record, then makes one call to the SDK's batch ingest,
// which returns the single offset for the whole batch.
func (h *harnessService) IngestRecords(_ context.Context, req *pb.IngestRecordsRequest) (*pb.IngestRecordsResponse, error) {
	e, err := h.lookup(req.GetStreamId())
	if err != nil {
		return nil, err
	}

	// The SDK answers an empty batch with offset -1 and no error. That sentinel is not
	// an offset, so the harness rejects the request rather than returning it.
	if len(req.GetRecordJson()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "record_json must not be empty")
	}

	payloads := make([][]byte, 0, len(req.GetRecordJson()))
	for i, rj := range req.GetRecordJson() {
		p, err := e.encodePayload(rj)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "record %d: %v", i, err)
		}
		payloads = append(payloads, p)
	}

	offset, err := e.stream.IngestRecordsOffsetContext(sdkCtx(), payloads)
	if err != nil {
		return &pb.IngestRecordsResponse{Error: sdkError(err)}, nil
	}
	return &pb.IngestRecordsResponse{Offset: offset}, nil
}

// AwaitWriteCompleted waits for the offset via the SDK. The harness imposes no
// deadline: the wait is bounded by the SDK's own ack/flush timeout.
func (h *harnessService) AwaitWriteCompleted(_ context.Context, req *pb.AwaitRequest) (*pb.AwaitResponse, error) {
	e, err := h.lookup(req.GetStreamId())
	if err != nil {
		return nil, err
	}
	return &pb.AwaitResponse{Error: sdkError(e.stream.WaitForOffsetContext(sdkCtx(), req.GetOffset()))}, nil
}

func (h *harnessService) Flush(_ context.Context, req *pb.FlushRequest) (*pb.ActionResponse, error) {
	e, err := h.lookup(req.GetStreamId())
	if err != nil {
		return nil, err
	}
	return &pb.ActionResponse{Error: sdkError(e.stream.FlushContext(sdkCtx()))}, nil
}

func (h *harnessService) Close(_ context.Context, req *pb.CloseRequest) (*pb.ActionResponse, error) {
	e, err := h.lookup(req.GetStreamId())
	if err != nil {
		return nil, err
	}

	// stream.Close flushes queued records and then tears the stream down. The SDK
	// handle is closed afterwards to release its connection. Close is idempotent on
	// both, and the entry stays registered so later RPCs reach the SDK.
	streamErr := e.stream.Close()
	sdkErr := e.sdk.Close()
	if streamErr != nil && sdkErr != nil {
		slog.Warn("closing SDK after stream close failed", "err", sdkErr)
	}
	slog.Info("stream closed", "stream_id", req.GetStreamId())

	closeErr := streamErr
	if closeErr == nil {
		closeErr = sdkErr
	}
	return &pb.ActionResponse{Error: sdkError(closeErr)}, nil
}

func (h *harnessService) Shutdown(context.Context, *pb.Empty) (*pb.Empty, error) {
	slog.Info("Shutdown RPC received")
	// Only signal here. main calls GracefulStop after this response is written;
	// calling GracefulStop from inside a handler would wait on itself.
	h.shutdownOnce.Do(func() { close(h.shutdownCh) })
	return &pb.Empty{}, nil
}
