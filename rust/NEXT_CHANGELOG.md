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

- Extracted a private transport-generic mux core and lane contract. The gRPC
  mux keeps its existing behavior, while each lane owns its close and recovery
  paths.

### Breaking Changes

- `schema::UcColumn` and `schema::UcTableSchema` are now `#[non_exhaustive]`,
  so new Unity Catalog metadata fields can be added without a major release.
  Build them with `UcColumn::new(name, type_name)` and the `with_type_text`,
  `with_type_json`, `with_nullable`, and `with_position` setters, or with
  `UcTableSchema::new(catalog_name, schema_name, name, columns)`, instead of
  struct literals. `UcColumn::new` uses the same defaults as deserialization.
  The fields stay public for reading, and deserializing from the Unity Catalog
  API response is unchanged.

### Deprecations

### API Changes

- Added `UcColumn::new`, its `with_*` setters, and `UcTableSchema::new`.
