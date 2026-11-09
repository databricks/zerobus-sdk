# NEXT CHANGELOG

## Release v1.9.0

### New Features and Improvements

- Arrow Flight streams now compress IPC payloads with ZSTD by default.
  `DefaultArrowStreamConfigurationOptions()` sets `IPCCompression` to `IPCCompressionZstd`, and the
  zero value `IPCCompressionDefault` and nil options also mean ZSTD instead of no compression. To
  keep sending uncompressed payloads, set `IPCCompression` to `IPCCompressionNone`.

### Deprecations

### Bug Fixes

### Documentation

- Documented the GCP workspace URL and Zerobus endpoint formats. The formats for
  AWS, Azure, and GCP now live in one [connection URL
  table](https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id),
  and this SDK's README, examples, and API docs link to it instead of repeating
  cloud-specific URLs.

### Internal Changes

### API Changes
