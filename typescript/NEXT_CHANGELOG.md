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

- Stream creation and recovery retries now back off exponentially with jitter
  instead of waiting a fixed `recoveryBackoffMs` between attempts (from the Rust core).
  The wait still starts at `recoveryBackoffMs` and is never shorter than it. It
  doubles after each consecutive failed attempt, up to 15 seconds (or
  `recoveryBackoffMs` if larger), and a random jitter of up to the same amount is
  added. With the defaults (2 s, 4 retries), a stream that keeps failing gives
  up after about 29–58 s of waiting instead of 8 s. Clients rejected at the same
  time, for example by the server's stream-creation rate limit, no longer all
  retry at the same moment. No API change; a backoff of 0 still retries
  immediately.

### Deprecations

### API Changes

- `IpcCompressionType` gains `None` (`-1`), which disables IPC compression.
