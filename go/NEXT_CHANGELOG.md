# NEXT CHANGELOG

## Release v1.7.0

### New Features and Improvements

- Add Avro record format support (Beta, gated by `avro` build tag) — enables ingestion of Avro records as Go objects (marshaled to JSON) or pre-encoded bytes. Requires Zerobus server Avro support (pending). Use `go build -tags avro` to enable.
  - Object-based ingestion: `IngestAvroRecordOffset(record)`, `IngestAvroRecordsOffset(records)` for automatic JSON encoding
  - Pre-encoded bytes: `IngestAvroBytesOffset(data)`, `IngestAvroBytesBatchOffset(records)` for pre-encoded Avro datums
  - Fire-and-forget variants: `IngestAvroRecordNowait()`, `IngestAvroRecordsNowait()`, `IngestAvroBytesNowait()`, `IngestAvroBytesBatchNowait()`

### Deprecations

### Bug Fixes

### Documentation

- Update Avro example to demonstrate object-based record ingestion with loop-then-flush pattern

### Internal Changes

### API Changes

- Renamed pre-encoded bytes methods for clarity: `IngestAvroRecordOffset` → `IngestAvroBytesOffset`, `IngestAvroRecordsOffset` → `IngestAvroBytesBatchOffset`, `IngestAvroRecordNowait` → `IngestAvroBytesNowait`, `IngestAvroRecordsNowait` → `IngestAvroBytesBatchNowait`
