//go:build avro

package zerobus

import (
	"fmt"
	"runtime"
	"unsafe"

	"github.com/hamba/avro/v2"
)

// AvroTableProperties contains information about the target table for Avro ingestion
type AvroTableProperties struct {
	// Fully qualified table name (catalog.schema.table)
	TableName string

	// Avro writer schema as a JSON string (required)
	SchemaJSON string
}

// AvroRecord is a map of field names to values for Avro object encoding.
type AvroRecord map[string]any

// Union wraps a value for Avro union branch tagging.
func Union(branch string, v any) map[string]any {
	return map[string]any{branch: v}
}

// CreateAvroStream creates a new Avro stream for ingesting pre-encoded Avro records or objects.
// This is a Beta feature gated by the `avro` build tag. The Zerobus service Avro support is pending.
//
// Parameters:
//   - tableProps: Table properties including name and Avro schema JSON
//   - clientID: OAuth 2.0 client ID
//   - clientSecret: OAuth 2.0 client secret
//   - options: Stream configuration options (nil for defaults)
//
// Returns an error if:
//   - Invalid table name format
//   - Invalid Avro schema JSON
//   - Authentication fails
//   - Network connectivity issues
//
// Example:
//
//	schemaJSON := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}`
//	stream, err := sdk.CreateAvroStream(
//	    AvroTableProperties{
//	        TableName: "catalog.schema.table",
//	        SchemaJSON: schemaJSON,
//	    },
//	    clientID,
//	    clientSecret,
//	    nil,
//	)
func (s *ZerobusSdk) CreateAvroStream(
	tableProps AvroTableProperties,
	clientID string,
	clientSecret string,
	options *StreamConfigurationOptions,
) (*ZerobusStream, error) {
	if s.ptr == nil {
		return nil, &ZerobusError{Message: "SDK has been freed", IsRetryable: false}
	}

	// Parse schema for object encoding
	schema, err := avro.Parse(tableProps.SchemaJSON)
	if err != nil {
		return nil, &ZerobusError{Message: "parse avro schema: " + err.Error(), IsRetryable: false}
	}

	ptr, err := sdkCreateAvroStream(
		s.ptr,
		tableProps.TableName,
		tableProps.SchemaJSON,
		clientID,
		clientSecret,
		options,
	)
	if err != nil {
		return nil, err
	}

	stream := &ZerobusStream{
		ptr:        ptr,
		avroSchema: unsafe.Pointer(&schema),
	}

	// Set up finalizer for automatic cleanup
	runtime.SetFinalizer(stream, func(st *ZerobusStream) {
		st.Close()
	})

	return stream, nil
}

// CreateAvroStreamWithHeadersProvider creates a new Avro stream using a custom headers provider.
// This is a Beta feature gated by the `avro` build tag. The Zerobus service Avro support is pending.
//
// Parameters:
//   - tableProps: Table properties including name and Avro schema JSON
//   - headersProvider: Custom implementation of HeadersProvider interface
//   - options: Stream configuration options (nil for defaults)
//
// Returns an error if:
//   - Invalid table name format
//   - Invalid Avro schema JSON
//   - Headers provider returns an error
//   - Network connectivity issues
//
// Example:
//
//	provider := &CustomHeadersProvider{}
//	schemaJSON := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}`
//	stream, err := sdk.CreateAvroStreamWithHeadersProvider(
//	    AvroTableProperties{
//	        TableName: "catalog.schema.table",
//	        SchemaJSON: schemaJSON,
//	    },
//	    provider,
//	    nil,
//	)
func (s *ZerobusSdk) CreateAvroStreamWithHeadersProvider(
	tableProps AvroTableProperties,
	headersProvider HeadersProvider,
	options *StreamConfigurationOptions,
) (*ZerobusStream, error) {
	if s.ptr == nil {
		return nil, &ZerobusError{Message: "SDK has been freed", IsRetryable: false}
	}

	// Parse schema for object encoding
	schema, err := avro.Parse(tableProps.SchemaJSON)
	if err != nil {
		return nil, &ZerobusError{Message: "parse avro schema: " + err.Error(), IsRetryable: false}
	}

	ptr, err := sdkCreateAvroStreamWithHeadersProvider(
		s.ptr,
		tableProps.TableName,
		tableProps.SchemaJSON,
		headersProvider,
		options,
	)
	if err != nil {
		return nil, err
	}

	stream := &ZerobusStream{
		ptr:        ptr,
		avroSchema: unsafe.Pointer(&schema),
	}

	// Set up finalizer for automatic cleanup
	runtime.SetFinalizer(stream, func(st *ZerobusStream) {
		st.Close()
	})

	return stream, nil
}

// IngestAvroBytesOffset ingests a pre-encoded Avro datum and returns the offset.
// This is a Beta API gated by the `avro` build tag.
//
// Avro records must be pre-encoded as binary Avro datums before passing to this method.
// This method returns as soon as the record is queued; the SDK sends it and tracks its
// acknowledgment in the background.
//
// The idiomatic flow is to ingest in a loop and call Flush() to confirm durability.
//
// Parameters:
//   - data: Pre-encoded Avro datum as []byte
//
// Returns:
//   - int64: The offset of the ingested record
//   - error: Any error that occurred during ingestion
//
// Example:
//
//	// Assuming you have pre-encoded Avro data
//	avroData := encodeAvroRecord(record, schemaJSON)
//	offset, err := stream.IngestAvroBytesOffset(avroData)
//	if err != nil {
//	    log.Fatal(err)
//	}
func (st *ZerobusStream) IngestAvroBytesOffset(data []byte) (int64, error) {
	if st.ptr == nil {
		return -1, &ZerobusError{Message: "Stream has been closed", IsRetryable: false}
	}

	return streamIngestAvroRecord(st.ptr, data)
}

// IngestAvroBytesBatchOffset ingests a batch of pre-encoded Avro datums and returns one offset for the batch.
// This is a Beta API gated by the `avro` build tag.
//
// All records in the batch must be pre-encoded Avro datums.
// This method returns as soon as the batch is queued; the server round-trip happens in the background.
//
// Parameters:
//   - records: Slice of pre-encoded Avro datums, each as []byte
//
// Returns:
//   - int64: One offset that represents the entire batch
//   - error: Any error that occurred during ingestion
//
// If the batch is empty, returns -1 with no error.
//
// Example:
//
//	records := [][]byte{
//	    encodeAvroRecord(record1, schemaJSON),
//	    encodeAvroRecord(record2, schemaJSON),
//	    encodeAvroRecord(record3, schemaJSON),
//	}
//	offset, err := stream.IngestAvroBytesBatchOffset(records)
//	if err != nil {
//	    log.Fatal(err)
//	}
func (st *ZerobusStream) IngestAvroBytesBatchOffset(records [][]byte) (int64, error) {
	if st.ptr == nil {
		return -1, &ZerobusError{Message: "Stream has been closed", IsRetryable: false}
	}

	if len(records) == 0 {
		return -1, nil
	}

	return streamIngestAvroRecords(st.ptr, records)
}

// IngestAvroBytesNowait ingests a pre-encoded Avro datum without waiting (fire-and-forget).
// This is a Beta API gated by the `avro` build tag.
//
// The function returns immediately after spawning a background task to queue the record.
// Ingestion errors from the background task are silently ignored.
//
// The stream must remain open until all background tasks have completed.
//
// Parameters:
//   - data: Pre-encoded Avro datum as []byte
//
// Returns an error only for argument validation failures (e.g. nil stream, empty data).
//
// Example:
//
//	err := stream.IngestAvroBytesNowait(avroData)
//	if err != nil {
//	    log.Fatal(err)
//	}
func (st *ZerobusStream) IngestAvroBytesNowait(data []byte) error {
	if st.ptr == nil {
		return &ZerobusError{Message: "Stream has been closed", IsRetryable: false}
	}

	return streamIngestAvroRecordNowait(st.ptr, data)
}

// IngestAvroBytesBatchNowait ingests a batch of pre-encoded Avro datums without waiting (fire-and-forget).
// This is a Beta API gated by the `avro` build tag.
//
// Returns immediately after the records are handed off; ingestion errors from the background task
// are silently ignored.
//
// The stream must remain open until all background tasks have completed.
//
// Parameters:
//   - records: Slice of pre-encoded Avro datums, each as []byte
//
// Returns an error only for argument validation failures (nil stream, etc).
//
// Example:
//
//	err := stream.IngestAvroBytesBatchNowait([][]byte{data1, data2, data3})
//	if err != nil {
//	    log.Fatal(err)
//	}
func (st *ZerobusStream) IngestAvroBytesBatchNowait(records [][]byte) error {
	if st.ptr == nil {
		return &ZerobusError{Message: "Stream has been closed", IsRetryable: false}
	}

	if len(records) == 0 {
		return nil
	}

	return streamIngestAvroRecordsNowait(st.ptr, records)
}

// IngestAvroRecordOffset encodes an AvroRecord to binary Avro and ingests it.
// This is a Beta API gated by the `avro` build tag.
//
// Encodes the record against the stream's writer schema using native Avro encoding.
// This method returns as soon as the record is queued; the SDK sends it and tracks its
// acknowledgment in the background.
//
// The idiomatic flow is to ingest in a loop and call Flush() to confirm durability.
//
// Parameters:
//   - record: AvroRecord (map[string]any) with field names matching the schema
//
// Returns:
//   - int64: The offset of the ingested record
//   - error: Encode error or any error that occurred during ingestion
//
// Example:
//
//	record := AvroRecord{
//	    "id": int64(1),
//	    "name": "Alice",
//	}
//	offset, err := stream.IngestAvroRecordOffset(record)
//	if err != nil {
//	    log.Fatal(err)
//	}
func (st *ZerobusStream) IngestAvroRecordOffset(record AvroRecord) (int64, error) {
	if st.ptr == nil {
		return -1, &ZerobusError{Message: "Stream has been closed", IsRetryable: false}
	}

	data, err := st.encodeAvroRecord(record)
	if err != nil {
		return -1, err
	}

	return streamIngestAvroRecord(st.ptr, data)
}

// IngestAvroRecordsOffset encodes AvroRecords to binary Avro and ingests them as a batch.
// This is a Beta API gated by the `avro` build tag.
//
// Each record is encoded against the stream's writer schema.
// This method returns as soon as the batch is queued; the server round-trip happens in the background.
//
// Parameters:
//   - records: Slice of AvroRecord (each map[string]any)
//
// Returns:
//   - int64: One offset that represents the entire batch
//   - error: Encode error or any error that occurred during ingestion
//
// If the batch is empty, returns -1 with no error.
//
// Example:
//
//	records := []AvroRecord{
//	    {"id": int64(1), "name": "Alice"},
//	    {"id": int64(2), "name": "Bob"},
//	}
//	offset, err := stream.IngestAvroRecordsOffset(records)
//	if err != nil {
//	    log.Fatal(err)
//	}
func (st *ZerobusStream) IngestAvroRecordsOffset(records []AvroRecord) (int64, error) {
	if st.ptr == nil {
		return -1, &ZerobusError{Message: "Stream has been closed", IsRetryable: false}
	}

	if len(records) == 0 {
		return -1, nil
	}

	encoded := make([][]byte, len(records))
	for i, rec := range records {
		data, err := st.encodeAvroRecord(rec)
		if err != nil {
			return -1, &ZerobusError{Message: fmt.Sprintf("encode record %d: %v", i, err), IsRetryable: false}
		}
		encoded[i] = data
	}

	return streamIngestAvroRecords(st.ptr, encoded)
}

// IngestAvroRecordNowait encodes an AvroRecord and ingests it without waiting (fire-and-forget).
// This is a Beta API gated by the `avro` build tag.
//
// The function returns immediately after spawning a background task to queue the record.
// Ingestion errors from the background task are silently ignored.
//
// The stream must remain open until all background tasks have completed.
//
// Parameters:
//   - record: AvroRecord (map[string]any) with field names matching the schema
//
// Returns an error only for argument validation or encoding failures.
//
// Example:
//
//	err := stream.IngestAvroRecordNowait(AvroRecord{"id": int64(1), "name": "Alice"})
//	if err != nil {
//	    log.Fatal(err)
//	}
func (st *ZerobusStream) IngestAvroRecordNowait(record AvroRecord) error {
	if st.ptr == nil {
		return &ZerobusError{Message: "Stream has been closed", IsRetryable: false}
	}

	data, err := st.encodeAvroRecord(record)
	if err != nil {
		return err
	}

	return streamIngestAvroRecordNowait(st.ptr, data)
}

// IngestAvroRecordsNowait encodes AvroRecords and ingests them without waiting (fire-and-forget).
// This is a Beta API gated by the `avro` build tag.
//
// Returns immediately after the records are handed off; ingestion errors from the background task
// are silently ignored.
//
// The stream must remain open until all background tasks have completed.
//
// Parameters:
//   - records: Slice of AvroRecord (each map[string]any)
//
// Returns an error only for argument validation or encoding failures.
//
// Example:
//
//	records := []AvroRecord{
//	    {"id": int64(1), "name": "Alice"},
//	    {"id": int64(2), "name": "Bob"},
//	}
//	err := stream.IngestAvroRecordsNowait(records)
//	if err != nil {
//	    log.Fatal(err)
//	}
func (st *ZerobusStream) IngestAvroRecordsNowait(records []AvroRecord) error {
	if st.ptr == nil {
		return &ZerobusError{Message: "Stream has been closed", IsRetryable: false}
	}

	if len(records) == 0 {
		return nil
	}

	encoded := make([][]byte, len(records))
	for i, rec := range records {
		data, err := st.encodeAvroRecord(rec)
		if err != nil {
			return &ZerobusError{Message: fmt.Sprintf("encode record %d: %v", i, err), IsRetryable: false}
		}
		encoded[i] = data
	}

	return streamIngestAvroRecordsNowait(st.ptr, encoded)
}

// encodeAvroRecord encodes an AvroRecord using the stream's schema.
func (st *ZerobusStream) encodeAvroRecord(record AvroRecord) ([]byte, error) {
	if st.avroSchema == nil {
		return nil, &ZerobusError{Message: "stream is not an Avro stream", IsRetryable: false}
	}
	schema := (*avro.Schema)(st.avroSchema)
	return avro.Marshal(*schema, map[string]any(record))
}
