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
