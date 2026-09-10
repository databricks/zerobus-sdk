//go:build avro

package zerobus

import (
	"context"
	"fmt"

	"github.com/hamba/avro/v2"
)

// hambaObjectEncoder encodes AvroRecord fields against a parsed writer schema.
type hambaObjectEncoder struct{ schema avro.Schema }

func (e hambaObjectEncoder) encode(fields map[string]any) ([]byte, error) {
	b, err := avro.Marshal(e.schema, fields)
	if err != nil {
		return nil, fmt.Errorf("avro encode: %w", err)
	}
	return b, nil
}

func init() {
	newAvroObjectEncoder = func(schemaJSON string) (avroObjectEncoder, error) {
		s, err := avro.Parse(schemaJSON)
		if err != nil {
			return nil, fmt.Errorf("parse avro schema: %w", err)
		}
		return hambaObjectEncoder{schema: s}, nil
	}
}

// IngestAvroRecordOffset encodes one AvroRecord against the stream's writer
// schema and queues it, returning its logical offset (-1 on error).
//
// Requires the avro build tag. For pre-encoded datums use IngestRecordOffset.
// For throughput, queue records in a loop and call Flush once.
func (s *Stream) IngestAvroRecordOffset(record AvroRecord) (int64, error) {
	return s.IngestAvroRecordOffsetContext(context.Background(), record)
}

// IngestAvroRecordOffsetContext is IngestAvroRecordOffset with caller context.
func (s *Stream) IngestAvroRecordOffsetContext(ctx context.Context, record AvroRecord) (int64, error) {
	b, err := s.encodeObjectRecord(record)
	if err != nil {
		// A client-side encode failure is permanent, not transient.
		return -1, &Error{Op: "IngestAvroRecordOffset", cause: err, retryable: false}
	}
	off, err := s.core.Ingest(ctx, b)
	return off, wrapErr("IngestAvroRecordOffset", err)
}

// IngestAvroRecordsOffset encodes AvroRecords and queues them as one batch.
// Empty batch returns -1 with nil error; failures return -1.
func (s *Stream) IngestAvroRecordsOffset(records []AvroRecord) (int64, error) {
	return s.IngestAvroRecordsOffsetContext(context.Background(), records)
}

// IngestAvroRecordsOffsetContext is IngestAvroRecordsOffset with caller context.
func (s *Stream) IngestAvroRecordsOffsetContext(ctx context.Context, records []AvroRecord) (int64, error) {
	b := make([][]byte, len(records))
	for i, r := range records {
		bs, err := s.encodeObjectRecord(r)
		if err != nil {
			// A client-side encode failure is permanent, not transient.
			return -1, &Error{Op: "IngestAvroRecordsOffset", cause: fmt.Errorf("record %d: %w", i, err), retryable: false}
		}
		b[i] = bs
	}
	off, err := s.core.IngestBatch(ctx, b)
	return off, wrapErr("IngestAvroRecordsOffset", err)
}

// encodeObjectRecord encodes an AvroRecord to a raw datum via the stream's
// object encoder, built at stream creation.
func (s *Stream) encodeObjectRecord(rec AvroRecord) ([]byte, error) {
	if s.avroEnc == nil {
		return nil, fmt.Errorf("stream is not an Avro stream")
	}
	return s.avroEnc.encode(map[string]any(rec))
}
