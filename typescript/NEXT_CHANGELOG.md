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

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

- `IpcCompressionType` gains `None` (`-1`), which disables IPC compression.
