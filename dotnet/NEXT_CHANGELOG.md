# NEXT CHANGELOG

## Release v0.2.0

### New Features and Improvements

- JSON and protobuf streams now use a dedicated gRPC connection by default.
  Call `ZerobusSdk.CreateBuilder().ConnectionPerStream(false)` to share one
  HTTP/2 connection across streams.

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
  44–88 s instead of 8 s. A server close signal still reconnects immediately.
  Clients rejected at the same time, for example by the server's stream-creation
  rate limit, no longer all retry at the same moment. A backoff of 0 still retries
  immediately.

  To keep a total retry window close to the previous one, lower `RecoveryRetries`.
  With the default 2 s base, 2 retries wait 6–12 s in total (14–28 s for gRPC
  recovery after a failure).

### Deprecations

### Bug Fixes

- Fixed a use-after-free in which a custom `IHeadersProvider` could be freed
  while the Rust core was still inside a `GetHeaders()` call into it during
  connection recovery. Provider ownership is now handed to the FFI via the new
  `free_user_data` destroy callback, which releases the provider's `GCHandle`
  only after any in-flight `GetHeaders()` has returned; the stream no longer
  frees the handle on dispose. This applies to both the synchronous
  (`CreateStreamWithHeadersProvider`) and asynchronous
  (`CreateStreamWithHeadersProviderAsync`) creation paths. Tracks the FFI
  signature change to `zerobus_sdk_create_stream_with_headers_provider` and
  `zerobus_sdk_create_stream_with_headers_provider_async`. No public API change.

### Documentation

- Corrected installation and source-build prerequisites, separated JSON and
  protobuf stream examples, and added a runnable generated-message example.
- Documented the asynchronous ingestion model across the README, XML docs, and
  examples: ingest records in a loop, then call `Flush()` once instead of waiting
  for each record. `WaitForOffset()` is now presented as a targeted wait for a
  specific offset.
- Documented that `GetUnackedRecords()` can fail while the stream is still
  active after a flush timeout, and stopped reporting success after ingest
  failures. Pointed CONTRIBUTING at `src/Zerobus/Native/`.
- Documented the GCP workspace URL and Zerobus endpoint formats. The formats for
  AWS, Azure, and GCP now live in one [connection URL
  table](https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id),
  and this SDK's README and API docs link to it instead of repeating cloud-specific
  URLs.

### Internal Changes

- Made the .NET release workflow build-only, consistent with the other SDKs. It now packs the NuGet package and uploads it as an artifact; publishing and the GitHub Release happen downstream.
- Pin the full NuGet restore graph with `packages.lock.json` and fail CI restore when the lock files are stale (`RestoreLockedMode`).

### API Changes
