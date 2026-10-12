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

- The declared minimum supported Rust version is now 1.85 (was 1.70). The
  default build already needed Rust 1.85 through its dependencies, so no build
  that worked before stops working; `rust-version` now says so, and CI checks
  it. Optional features can need a newer toolchain (for example `avro`).

### Deprecations

### API Changes
