package zerobus

import "unsafe"

// RecordAck represents an acknowledgment for an ingested record.
// Call Await() to wait for server acknowledgment of the record.
//
// Deprecated: This API is maintained for backwards compatibility.
// Use IngestRecordOffset() instead for a simpler API that returns the offset directly.
type RecordAck struct {
	streamPtr unsafe.Pointer
	offset    int64
	err       error
}

// Await waits for the server to acknowledge the record at this offset.
// This method blocks until the server confirms the record has been durably written.
//
// Deprecated: This API is maintained for backwards compatibility.
// Use IngestRecordOffset() followed by stream.WaitForOffset() for more explicit control.
//
// Example:
//
//	ack, _ := stream.IngestRecord(data)
//	offset, err := ack.Await()  // Blocks until server acknowledges
func (a *RecordAck) Await() (int64, error) {
	if a.err != nil {
		return -1, a.err
	}

	// Wait for server acknowledgment
	err := streamWaitForOffset(a.streamPtr, a.offset)
	if err != nil {
		return -1, err
	}

	return a.offset, nil
}

// Offset returns the offset for the ingested record without waiting for acknowledgment.
// The offset is available immediately after the record is queued.
func (a *RecordAck) Offset() (int64, error) {
	return a.offset, a.err
}
