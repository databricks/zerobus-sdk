# NEXT CHANGELOG

## Release v3.0.0

### Major Changes

### New Features and Improvements

- Arrow Flight streams with IPC compression now pad IPC buffers to a multiple
  of 8 bytes instead of 64. With compression, 64-byte alignment gives no
  benefit and only adds padding bytes to the payload. Uncompressed payloads
  keep 64-byte alignment.
- Zeroparser's `MessageRegistry` is now aligned to 128 bytes. When threads
  share one registry and clone its `Arc` for every record, the reference
  counts were on the same cache line as the registry fields that the parser
  reads, which made parsing several times slower on many threads.

### Bug Fixes

### Documentation

- Documented persistent streams (behind the `eos` feature) in the Rust README,
  with a usage section, API reference entries, and a runnable JSON example that
  creates a durable stream, ingests in a loop with one `flush()`, and resumes by
  `stream_id`.

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

- `schema::UcColumn` and `schema::UcTableSchema` are now `#[non_exhaustive]`,
  so they can no longer be built with struct literals outside the SDK. Use
  `UcColumn::new(name, type_name)` with the `with_type_text`, `with_type_json`,
  `with_nullable`, and `with_position` setters, and
  `UcTableSchema::new(catalog_name, schema_name, name, columns)`. Fields stay
  public for reading, and deserializing from the Unity Catalog API response is
  unchanged. New Unity Catalog fields can now be added in minor releases.
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

- Added `UcColumn::new`, `UcColumn::with_type_text`, `UcColumn::with_type_json`,
  `UcColumn::with_nullable`, `UcColumn::with_position`, and
  `UcTableSchema::new`.
- Added `RecordType`.
