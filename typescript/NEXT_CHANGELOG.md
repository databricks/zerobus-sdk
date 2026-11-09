# NEXT CHANGELOG

## Release v1.6.0

### Major Changes

### New Features and Improvements

- Arrow Flight streams now compress IPC payloads with ZSTD by default. Leaving
  `ArrowStreamConfigurationOptions.ipcCompression` unset now means `IpcCompressionType.Zstd`
  instead of no compression. To keep sending uncompressed payloads, set it to the new
  `IpcCompressionType.None`.

### Bug Fixes

### Documentation

- Documented the GCP workspace URL and Zerobus endpoint formats. The formats for
  AWS, Azure, and GCP now live in one [connection URL
  table](https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id),
  and this SDK's README, examples, and API docs link to it instead of repeating
  cloud-specific URLs.
- The examples exit with an error naming the missing environment variable instead of
  falling back to placeholder connection values.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

- `IpcCompressionType` gains `None` (`-1`), which disables IPC compression.
