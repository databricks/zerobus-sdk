# NEXT CHANGELOG

## Release v0.4.0

### New Features and Improvements
- Add Avro record format support (Beta) via new CMake option `ZEROBUS_ENABLE_AVRO`. When enabled, `Sdk::create_avro_stream()` and `Stream::ingest_avro_record()`/`ingest_avro_records()` become available with two overload paths:
  - **Pre-encoded bytes** (existing): `ingest_avro_record(const uint8_t*, size_t)` and `ingest_avro_record(const std::vector<uint8_t>&)` for caller-encoded Avro datums.
  - **JSON record objects** (new): `ingest_avro_record(const std::string& json)` and `ingest_avro_records(const std::vector<std::string>& jsons)` for JSON-encoded records; the Rust core encodes them against the stream's writer schema.
- Add Avro examples under `cpp/examples/avro/`: single-record (`single.cpp`) and batch (`batch.cpp`) ingestion patterns.

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
