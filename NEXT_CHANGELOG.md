# NEXT CHANGELOG

## Release v0.2.0

### New Features and Improvements

- **Batch Ingestion API**: Added `ingest_records()` method for ingesting multiple records at once
  - All-or-nothing semantics: entire batch succeeds or fails as a unit
  - Ingesting an empty batch is a no-op.
- **JSON Serialization Support**: Added support for JSON record serialization alongside Protocol Buffers (default)
  - No protobuf schema compilation required
- Added `HeadersProvider`, a trait for flexible authentication strategies
- Implemented `OAuthHeadersProvider` for OAuth 2.0 Client Credentials flow (default authentication method used by `create_stream()`)

### Bug Fixes

### Documentation

- Added JSON and protobuf serialization examples for batch ingestion
- Enhanced API Reference with batch ingestion documentation
- Added JSON and protobuf serialization examples
- Updated README's.
- Enhanced API Reference with JSON mode documentation
- Added Azure workspace and endpoint URL examples

### Internal Changes

- [**BREAKING**] Changed backpressure mechanism to track in-flight requests instead of in-flight records

### API Changes

- [**BREAKING**] changed `max_inflight_records` to `max_inflight_requests` in `StreamConfigurationOptions` as we now track in-flight requests
- [**BREAKING**] `get_unacked_records()` method now returns `impl Iterator<Item = EncodedRecord>` instead of `Vec<Vec<u8>>` - flattens all batches into individual records
- Added `get_unacked_batches()` method to `ZerobusStream` that returns `Vec<EncodedBatch>` to preserve batch structure - records ingested together remain grouped
- Added `ingest_records()` method to `ZerobusStream` for bulk record ingestion
- `recreate_stream` method in `ZerobusSdk` now accepts a reference to a stream, instead of taking ownership of it.
- `TableProperties` struct now has `descriptor_proto` field as optional (**breaking change**).
- Added `HeadersProvider` trait for custom header strategies
- Added `OAuthHeadersProvider` struct for OAuth 2.0 authentication with Databricks OIDC endpoint
- Added `create_stream_with_headers_provider` method to `ZerobusSdk` for custom authentication header providers
