# NEXT CHANGELOG

## Release v3.0.0

### Major Changes

### New Features and Improvements

- Arrow Flight streams with IPC compression now pad IPC buffers to a multiple
  of 8 bytes instead of 64. With compression, 64-byte alignment gives no
  benefit and only adds padding bytes to the payload. Uncompressed payloads
  keep 64-byte alignment.

- Added `StreamBuilder::compression` and `ArrowCompression` (`IpcZstd` by
  default, `IpcLz4`, `GrpcZstd`, or `None`) to choose Arrow Flight payload
  compression with one setting. `GrpcZstd` compresses each whole gRPC request
  message, including the IPC header and alignment padding that IPC compression
  leaves raw, which sends fewer bytes for wide or sparse schemas. Its buffers
  are sent without IPC compression and with 8-byte alignment. The default
  leaves the wire format unchanged.

### Bug Fixes

### Documentation

### Internal Changes

- Added a stateful persistent-stream mock and integration coverage for create,
  flush, resume offset continuity, and unknown stream IDs.

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

### Deprecations

- Deprecated `StreamBuilder::ipc_compression` and the
  `ArrowStreamConfigurationOptions::ipc_compression` field in favor of
  `StreamBuilder::compression` and the `compression` field. They keep working:
  of the two setters, the last call wins, and the deprecated field applies while
  `compression` is `None`.

### API Changes
