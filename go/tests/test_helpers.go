package tests

import (
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestHeadersProvider implements a simple headers provider for testing
type TestHeadersProvider struct{}

func (p *TestHeadersProvider) GetHeaders() (map[string]string, error) {
	headers := make(map[string]string)
	headers["authorization"] = "Bearer test_token"
	headers["x-databricks-zerobus-table-name"] = "test_table"
	return headers, nil
}

// TestCallback tracks acknowledgments and errors for testing
type TestCallback struct {
	acks   []int64
	errors []ErrorRecord
	mu     sync.Mutex
}

type ErrorRecord struct {
	OffsetID     int64
	ErrorMessage string
}

func NewTestCallback() *TestCallback {
	return &TestCallback{
		acks:   make([]int64, 0),
		errors: make([]ErrorRecord, 0),
	}
}

func (c *TestCallback) OnAck(offsetID int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.acks = append(c.acks, offsetID)
}

func (c *TestCallback) OnError(offsetID int64, errorMessage string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errors = append(c.errors, ErrorRecord{
		OffsetID:     offsetID,
		ErrorMessage: errorMessage,
	})
}

func (c *TestCallback) GetAcks() []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]int64, len(c.acks))
	copy(result, c.acks)
	return result
}

func (c *TestCallback) GetErrors() []ErrorRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]ErrorRecord, len(c.errors))
	copy(result, c.errors)
	return result
}

// CreateTestDescriptorProto creates a simple test protobuf descriptor
// This returns the serialized FileDescriptorSet bytes
func CreateTestDescriptorProto() []byte {
	// This is a minimal proto descriptor for testing
	// In practice, you'd generate this from your actual proto files
	// For now, we'll return a simple descriptor that matches a basic message structure
	// Message TestMessage { int64 id = 1; string message = 2; }

	// This is a pre-serialized FileDescriptorSet for the test message
	// Generated from: message TestMessage { int64 id = 1; string message = 2; }
	descriptor := []byte{
		0x0a, 0x45, 0x0a, 0x0b, 0x74, 0x65, 0x73, 0x74, 0x2e, 0x70, 0x72, 0x6f,
		0x74, 0x6f, 0x22, 0x36, 0x0a, 0x0b, 0x54, 0x65, 0x73, 0x74, 0x4d, 0x65,
		0x73, 0x73, 0x61, 0x67, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18,
		0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x02, 0x69, 0x64, 0x12, 0x17, 0x0a,
		0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x18, 0x02, 0x20, 0x01,
		0x28, 0x09, 0x52, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x62,
		0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
	}

	return descriptor
}

// Helper function to create mock responses for common test patterns
func CreateStreamResponse(streamID string, delayMs int64) MockResponse {
	return MockResponse{
		Type:     MockResponseCreateStream,
		StreamID: streamID,
		DelayMs:  delayMs,
	}
}

func RecordAckResponse(offset int64, delayMs int64) MockResponse {
	return MockResponse{
		Type:          MockResponseRecordAck,
		AckUpToOffset: offset,
		DelayMs:       delayMs,
	}
}

func ErrorResponse(code codes.Code, message string, delayMs int64) MockResponse {
	return MockResponse{
		Type:       MockResponseError,
		GrpcStatus: status.New(code, message),
		DelayMs:    delayMs,
	}
}
