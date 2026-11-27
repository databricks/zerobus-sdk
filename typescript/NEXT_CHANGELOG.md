# NEXT CHANGELOG

## Release v1.6.0

### Major Changes

### New Features and Improvements

- Arrow Flight streams now compress IPC payloads with ZSTD by default. Leaving
  `ArrowStreamConfigurationOptions.ipcCompression` unset now means `IpcCompressionType.Zstd`
  instead of no compression. To keep sending uncompressed payloads, set it to the new
  `IpcCompressionType.None`.

- Behavior change (no API change): stream creation and recovery retries now back off
  exponentially with jitter instead of waiting a fixed `recoveryBackoffMs` between
  attempts (from the Rust core). The wait starts at `recoveryBackoffMs` and is never
  shorter than it. It doubles after each consecutive failed attempt, up to 15 s (or
  `recoveryBackoffMs` if larger), and a random jitter of up to the same amount is
  added, so a single wait can reach 30 s. With the defaults (2 s, 4 retries),
  retries wait 2–4 s, 4–8 s, 8–16 s, then 15–30 s, so a stream that keeps failing
  gives up after about 29–58 s of waiting instead of 8 s. Recovery of a gRPC stream
  after a failure now also waits before its first reconnect attempt (2–4 s by
  default), so streams the server drops at the same time don't all reconnect at
  once; a gRPC stream that keeps failing during recovery gives up after about
  44–88 s instead of 8 s. A server-initiated Arrow Flight stream rotation still
  waits `recoveryBackoffMs` on average (a random wait between half and 1.5 times
  it), and a gRPC close signal still reconnects immediately. Clients rejected at the
  same time, for example by the server's stream-creation rate limit, no longer all
  retry at the same moment. A backoff of 0 still retries immediately.

  To keep a total retry window close to the previous one, lower `recoveryRetries`.
  With the default 2 s base, 2 retries wait 6–12 s in total (14–28 s for gRPC
  recovery after a failure).

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
