# NEXT CHANGELOG

## Release v1.9.0

### New Features and Improvements

- Arrow Flight streams now compress IPC payloads with ZSTD by default.
  `DefaultArrowStreamConfigurationOptions()` sets `IPCCompression` to `IPCCompressionZstd`, and the
  zero value `IPCCompressionDefault` and nil options also mean ZSTD instead of no compression. To
  keep sending uncompressed payloads, set `IPCCompression` to `IPCCompressionNone`.

- Behavior change (no API change): stream creation and recovery retries now back off
  exponentially with jitter instead of waiting a fixed `RecoveryBackoffMs` between
  attempts (from the Rust core). The wait starts at `RecoveryBackoffMs` and is never
  shorter than it. It doubles after each consecutive failed attempt, up to 15 s (or
  `RecoveryBackoffMs` if larger), and a random jitter of up to the same amount is
  added, so a single wait can reach 30 s. With the defaults (2 s, 4 retries),
  retries wait 2–4 s, 4–8 s, 8–16 s, then 15–30 s, so a stream that keeps failing
  gives up after about 29–58 s of waiting instead of 8 s. Recovery of a gRPC stream
  after a failure now also waits before its first reconnect attempt (2–4 s by
  default), so streams the server drops at the same time don't all reconnect at
  once; a gRPC stream that keeps failing during recovery gives up after about
  44–88 s instead of 8 s. A server-initiated Arrow Flight stream rotation still
  waits `RecoveryBackoffMs` on average (a random wait between half and 1.5 times
  it), and a gRPC close signal still reconnects immediately. Clients rejected at the
  same time, for example by the server's stream-creation rate limit, no longer all
  retry at the same moment.

  To keep a total retry window close to the previous one, lower `RecoveryRetries`.
  With the default 2 s base, 2 retries wait 6–12 s in total (14–28 s for gRPC
  recovery after a failure).

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
