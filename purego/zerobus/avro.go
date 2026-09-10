//go:build avro

package zerobus

import "github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"

// WithAvro selects Avro encoding; schemaJSON is the writer schema, validated at
// stream creation. Ingest objects via IngestAvroRecordOffset or pre-encoded
// datums via IngestRecordOffset. Feature in development; requires the avro tag.
func WithAvro(schemaJSON string) StreamOption {
	return func(c *streamConfig) {
		c.recordType = zerobuspb.RecordType_AVRO
		c.descriptor = nil
		c.avroSchema = schemaJSON
	}
}
