# NEXT CHANGELOG

## Release v3.0.0

### Major Changes

### New Features and Improvements

- Arrow Flight streams with IPC compression now pad IPC buffers to a multiple
  of 8 bytes instead of 64. With compression, 64-byte alignment gives no
  benefit and only adds padding bytes to the payload. Uncompressed payloads
  keep 64-byte alignment.

### Bug Fixes

### Documentation

### Internal Changes

- Added a stateful persistent-stream mock and integration coverage for create,
  flush, resume offset continuity, and unknown stream IDs.

- Added the unsupported `internal-proto` feature, which exposes the generated
  gRPC types to the repository's mock gRPC server.

### Breaking Changes

- Bumped the Arrow dependencies (`arrow-array`, `arrow-schema`, `arrow-ipc`, and
  the vendored `arrow-flight` fork) from `59.2` to `60.0`. The vendored
  `arrow-flight` crate was re-synced to upstream `60.0.0` with the slice-aware
  batch-split fix (arrow-rs#9388 / #5352) re-applied. Because the SDK re-exports
  Arrow types through its public API, consumers using the `arrow-flight`
  feature that exchange those types with the SDK must upgrade their own Arrow
  dependency to `60.x`; Arrow `59` and `60` types cannot be mixed in the same
  build. Arrow `60` requires Rust 1.88 or newer. Consumers on the default build
  (without the `arrow-flight` feature) are unaffected.

- Added `#[non_exhaustive]` to the `EncodedRecord`, `EncodedBatch`, and
  `EncodedBatchIter` enums, and to the doc-hidden `PreparedInput`. Enabling the
  `avro` feature anywhere in a build, or adding a record format in a later
  release, no longer breaks exhaustive matches in other crates
  ([#909](https://github.com/databricks/zerobus-sdk/issues/909)).

  Migration: add a `_ => { … }` arm to any `match` on these enums, such as one
  over the records returned by `get_unacked_records()` or
  `get_unacked_batches()`.

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
