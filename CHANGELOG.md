# Version changelog

## Release v0.4.0

### New Features and Improvements

- **Acknowledgment Callbacks**: Added callback support for receiving notifications when records are acknowledged
  - New `AckCallback` trait with `on_ack()` and `on_error()` methods
  - Configurable via `ack_callback` field in `StreamConfigurationOptions` and `ArrowStreamConfigurationOptions`

- Added support for `TINYINT/BYTE`, `TIMESTAMP_NTZ`, and `VARIANT` data types in the proto generation tool

- **Alternative Ingestion API with Direct Offset Return**: Added `ingest_record_offset()` and `ingest_records_offset()` methods
  - Return `OffsetId` (logical offset) directly as an integer (after queuing) instead of wrapping it in a Future
  - Can be used with new `wait_for_offset()` method to block on acknowledgment when needed
  - Allows decoupling record ingestion from acknowledgment tracking
  - Useful for scenarios where you want to collect offsets and wait on them selectively

### Deprecations

- **Deprecated `ingest_record()` and `ingest_records()` methods**: Use `ingest_record_offset()` and `ingest_records_offset()` instead
  - The new methods return offsets directly (after queuing) without Future wrapping for a cleaner API
  - Use with `wait_for_offset()` to explicitly wait for acknowledgments when needed
  - Old methods will continue to work but may be removed in a future version

### Bug Fixes

- Improved error propagation in `wait_for_offset()` and `flush()`: errors from the server are now detected and returned immediately instead of waiting for timeout, providing faster feedback and more accurate error messages

- Improved error classification in OAuth token retrieval: 5xx server errors and network failures are now retryable, while 4xx client errors (invalid credentials, etc.) are non-retryable

### Documentation

### Internal Changes

- Refactored `wait_for_offset_internal` to remove unnecessary double loop
- Optimized gRPC channel reuse: `ZerobusSdk` now reuses a single gRPC channel across multiple stream creations instead of creating a new channel for each stream, improving connection efficiency and reducing resource overhead
- Enhanced background tasks with `is_closed` checks and proper error broadcasting to the shared error channel, ensuring timely shutdown and accurate error reporting

- Added `user-agent` header to all gRPC requests for SDK version tracking

- Refactored `flush()` and `wait_for_offset()` to share common waiting logic via `wait_for_offset_internal()`, reducing code duplication and ensuring consistent behavior

- Improved graceful close mechanism: when server signals stream closure, SDK now continues processing acknowledgments for in-flight records while pausing new record transmission until timeout.

### API Changes

- [**BREAKING**] Added `callback_max_wait_time_ms` to `StreamConfigurationOptions` to limit how long callbacks may run after stream closure (`None` = infinite, `Some(x)` = `x` ms).
- Added `ack_callback: Option<Arc<dyn AckCallback>>` field to `StreamConfigurationOptions` and `ArrowStreamConfigurationOptions` for acknowledgment callbacks
- Added `AckCallback` trait with `on_ack(offset_id)` and `on_error(offset_id, error_message)` methods

- Added Arrow IPC compression support via `ipc_compression: Option<CompressionType>` in `ArrowStreamConfigurationOptions` (supports `LZ4_FRAME` and `ZSTD`, default: `None`)
- **[BREAKING]** Changed `ZerobusArrowStream::ingest_batch()` to return `OffsetId` directly instead of `Future<Output = OffsetId>`. Use `wait_for_offset(offset)` to explicitly wait for acknowledgment
- Added `ZerobusArrowStream::wait_for_offset()` method to wait for acknowledgment of a specific offset
- Added `is_closed` check at the beginning of `flush()` for both `ZerobusStream` and `ZerobusArrowStream`

- Added `ingest_record_offset()` method to `ZerobusStream` for direct offset return without Future wrapping
- Added `ingest_records_offset()` method to `ZerobusStream` for batch ingestion with direct offset return
- Added `wait_for_offset()` method to `ZerobusStream` to wait for acknowledgment of a specific offset

- [**BREAKING**] Added `stream_paused_max_wait_time_ms` to `StreamConfigurationOptions` to configure maximum wait time during graceful stream close (`None` = wait for full server duration, `Some(0)` = immediate recovery, `Some(x)` = wait up to min(x, server_duration) milliseconds)

## Release v0.3.0

### New Features and Improvements

- **Arrow Flight Ingestion (Experimental)**: Added experimental Arrow Flight support for high-throughput Apache Arrow record batch ingestion
  - Opt-in feature: enable with `features = ["arrow-flight"]` in Cargo.toml
  - Transmits Arrow RecordBatches in native IPC format (no format conversion required)
  - Same recovery and retry semantics as gRPC streams
  - **Note**: This feature is currently experimental and unsupported

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

## Release v0.1.1

- Added comprehensive API documentation and fixed Cargo.toml metadata for crates.io publication

## Release v0.1.0

Initial release of the Databricks Zerobus Ingest SDK for Rust.

### API Changes

- Added `ZerobusSdk` struct for creating ingestion streams.
- Added `ZerobusStream` struct for managing the stateful gRPC stream.
- The `ingest_record` method returns a future that resolves to the record's acknowledgment offset.
- Added `TableProperties` for configuring the target table schema and name.
- Added `StreamConfigurationOptions` for fine-tuning stream behavior like recovery and timeouts.
- Added `ZerobusError` enum for detailed error handling, including a `is_retryable()` method.
- The SDK is built on `tokio` and is fully asynchronous.
