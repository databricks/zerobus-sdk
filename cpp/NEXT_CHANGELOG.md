# NEXT CHANGELOG

## Release v0.5.0

### New Features and Improvements

- Arrow Flight streams now compress IPC payloads with ZSTD by default.
  `ArrowStreamOptions::ipc_compression` defaults to `IpcCompression::Zstd` instead of
  `IpcCompression::NoCompression`. To keep sending uncompressed payloads, set it to
  `IpcCompression::NoCompression`.

### Bug Fixes

### Documentation

- Documented the GCP workspace URL and Zerobus endpoint formats. The formats for
  AWS, Azure, and GCP now live in one [connection URL
  table](https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id),
  and this SDK's README, examples, and API docs link to it instead of repeating
  cloud-specific URLs.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
