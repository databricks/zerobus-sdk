# NEXT CHANGELOG

## Release v0.5.0

### New Features and Improvements

- **Builder Pattern for SDK Initialization**: Added `ZerobusSdk::builder()` for fluent SDK configuration
  - `.endpoint()` - Set the Zerobus endpoint (scheme is optional, defaults work with or without `https://`)
  - `.unity_catalog_url()` - Set the Unity Catalog URL (optional when using custom headers providers)
  - `.tls_config()` - Provide a custom `TlsConfig` implementation (defaults to `SecureTlsConfig`)
- **Configurable TLS via `TlsConfig` trait**: TLS is now configured through a strategy pattern
  - `SecureTlsConfig` (default) - Production TLS with system CA certificates
  - `NoTlsConfig` - No-op TLS for testing with plaintext `http://` endpoints (requires `testing` feature)
  - Implement `TlsConfig` trait for custom certificate handling
- **SDK Identifier Header**: Renamed `user-agent` header to `x-zerobus-sdk` for clearer SDK identification in gRPC metadata
- **Type Widening for Record Ingestion**: Added wrapper types for record ingestion
  - **`ProtoMessage<T>`**: SDK handles encoding - pass any `prost::Message` directly
  - **`JsonValue<T>`**: SDK handles serialization - pass any `serde::Serialize` type directly
  - **`ProtoBytes`**: Client handles encoding - explicit wrapper for pre-encoded protobuf bytes
  - **`JsonString`**: Client handles serialization - explicit wrapper for pre-serialized JSON strings
  - **Backward compatible**: existing code using `Vec<u8>` and `String` continues to work
  - Works with both single record and batch ingestion methods

### Deprecations

- **`ZerobusSdk::new()`**: Use `ZerobusSdk::builder()` instead
- **`ZerobusSdk.use_tls` field**: TLS is now controlled via the `TlsConfig` trait passed to the builder


### Bug Fixes

- **[Experimental] Record-based acknowledgment tracking for Arrow Flight streams**: Added cumulative record counting to support proper ack tracking and correct recovery when batches are auto-chunked.

### Documentation

- Reorganized examples directory structure: `json/single`, `json/batch`, `proto/single`, `proto/batch`
- Added separate README files for JSON and Protocol Buffers examples with comprehensive documentation
- Updated all examples to demonstrate three data-passing approaches: auto-encoding/serializing wrappers, pre-encoded/serialized wrappers, and backward-compatible raw types

### Internal Changes


### API Changes

- **Added `ZerobusSdkBuilder`** for fluent SDK configuration (replaces `ZerobusSdk::new()`)
- **Added `TlsConfig` trait** with `SecureTlsConfig` (default) and `NoTlsConfig` (behind `testing` feature)
- **Renamed header** from `user-agent` to `x-zerobus-sdk` in gRPC metadata
- **Added type widening wrapper types** (backward compatible):
  - Added `ProtoMessage<T: prost::Message>` - SDK handles encoding for protobuf messages
  - Added `JsonValue<T: serde::Serialize>` - SDK handles serialization for JSON objects
  - Added `ProtoBytes` - for pre-encoded protobuf bytes (client handles encoding)
  - Added `JsonString` - for pre-serialized JSON strings (client handles serialization)
  - All new types implement `Into<EncodedRecord>` for seamless integration
  - Existing `Vec<u8>` and `String` types continue to work (backward compatible)
