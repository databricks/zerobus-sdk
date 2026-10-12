# NEXT CHANGELOG

## Release v2.12.0

### Major Changes

### New Features and Improvements

- Added the feature-gated `PersistentStream` API for creating durable ingestion
  streams and resuming them by `stream_id` from the last committed offset.
- Arrow Flight streams now compress IPC payloads with ZSTD by default.
  `ArrowStreamConfigurationOptions::ipc_compression` defaults to
  `Some(CompressionType::ZSTD)` instead of `None`. To keep sending uncompressed
  payloads, call `.ipc_compression(None)` on the stream builder.

### Bug Fixes

### Documentation

### Internal Changes

- Added the unsupported `internal-proto` feature, which exposes the generated
  gRPC types to the repository's mock gRPC server.

- Extracted a private transport-generic mux core and lane contract. The gRPC
  mux keeps its existing behavior, while each lane owns its close and recovery
  paths.

### Breaking Changes

- The generated protobuf/gRPC types are no longer public:
  `databricks_zerobus_ingest_sdk::databricks::zerobus` has been removed
  ([#822](https://github.com/databricks/zerobus-sdk/issues/822)), so later
  additions to the wire protocol aren't breaking changes.
  `StreamConfigurationOptions::record_type` is now the SDK-owned `RecordType`
  (`Proto`, `Json`, and `Avro` with the `avro` feature), which is
  `#[non_exhaustive]` and has no `Unspecified` variant. Import
  `databricks_zerobus_ingest_sdk::RecordType` instead. The stream builder API
  and the other-language SDKs are unaffected.

### Deprecations

### API Changes

- Added `RecordType`.
