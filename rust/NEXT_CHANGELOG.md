# NEXT CHANGELOG

## Release v3.1.0

### Major Changes

### New Features and Improvements
- Added `StreamBuilder::compression` and `ArrowCompression` (`IpcZstd`,
  `IpcLz4`, `GrpcZstd`, or `Uncompressed`) to choose Arrow Flight payload
  compression with one setting. `GrpcZstd` compresses each whole gRPC request
  message, including the IPC header and alignment padding that IPC compression
  leaves raw, which sends fewer bytes for wide or sparse schemas. Its buffers
  are sent without IPC compression and with 8-byte alignment, and it can use
  more client CPU than `IpcZstd`. With it, `BatchStats::approximate_wire_bytes`
  counts bytes after gRPC compression. The default leaves the wire format
  unchanged.
- Added `schema::fetch_uc_table_schema` and
  `schema::fetch_uc_table_schema_with_client`, which fetch a table's schema from
  the Unity Catalog REST API (`GET /api/2.1/unity-catalog/tables/{table}`) given
  a bearer token and return a `UcTableSchema`. The `_with_client` variant takes a
  caller-supplied `reqwest::Client` (for a proxy, custom TLS roots, or timeouts);
  the other wraps it with a default client. Combine the result with the existing
  `descriptor_from_uc_schema` (dynamic protobuf) or `arrow_schema_from_uc_schema`
  (Arrow Flight) to resolve a table's schema at runtime without pre-generating a
  `.proto` file.

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

- Deprecated `StreamBuilder::ipc_compression` and the
  `ArrowStreamConfigurationOptions::ipc_compression` field in favor of
  `StreamBuilder::compression` and the `compression` field. They keep working:
  of the two setters, the last call wins, and the deprecated field applies while
  `compression` is `ArrowCompression::FallbackToLegacy`, its default. That
  variant is deprecated too and will be removed with the field.

### API Changes
- Added the `ZerobusError::SchemaFetchError { message, retryable }` variant,
  returned by `schema::fetch_uc_table_schema`. The enum is `#[non_exhaustive]`,
  so this is an additive change.
