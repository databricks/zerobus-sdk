# NEXT CHANGELOG

## Release v0.5.0

### New Features and Improvements

- Arrow Flight streams now compress IPC payloads with ZSTD by default.
  `ArrowStreamOptions::ipc_compression` defaults to `IpcCompression::Zstd` instead of
  `IpcCompression::NoCompression`. To keep sending uncompressed payloads, set it to
  `IpcCompression::NoCompression`.
- Added a stream builder (Beta). `Sdk::stream_builder()` returns a `StreamBuilder` with `table()`,
  `oauth()` or `headers_provider()`, `json()` or `compiled_proto()`, a setter per `StreamOptions`
  field, `validate()` and `build()`. Unset options keep the `StreamOptions` defaults, so `build()`
  opens the same stream as `create_stream`. Unlike `create_stream`, it rejects
  `max_inflight_requests` 0.

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

- Added `zerobus::StreamBuilder` and `Sdk::stream_builder()` (Beta).
