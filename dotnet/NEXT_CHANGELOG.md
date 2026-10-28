# NEXT CHANGELOG

## Release v0.2.0

### New Features and Improvements

- JSON and protobuf streams now use a dedicated gRPC connection by default.
  Call `ZerobusSdk.CreateBuilder().ConnectionPerStream(false)` to share one
  HTTP/2 connection across streams.
- Stream recovery now waits with exponential backoff and jitter.
  `RecoveryBackoffMs` is now the base delay: the delay doubles with each
  consecutive failure, up to 30 seconds. The SDK waits the delay plus a random
  jitter of up to half of the delay, so no wait is shorter than before. A retry
  delay that the server sends with a rejection is the minimum wait, up to 30
  seconds; the jitter does not apply to it. `RecoveryRetries` still applies to
  each recovery; only the backoff exponent continues across successful
  reconnects. It resets when a connection ends that stayed up for at least 30
  seconds and either received an acknowledgement or had no sent records or
  batches left unacknowledged. JSON and Protocol Buffer streams now also wait
  before the first reconnect after a connection error, and after a server close
  signal on a connection that was not healthy. Stream creation uses the same
  backoff, so with the default options, creating a stream against an
  unavailable server waits 30 to 45 seconds in total before it fails, instead
  of about 8 seconds. With the default options, a recovery that keeps failing
  now waits at least 30 seconds before the stream fails (at least 60 seconds
  after a connection error), instead of about 8 seconds.

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

### Internal Changes

- Made the .NET release workflow build-only, consistent with the other SDKs. It now packs the NuGet package and uploads it as an artifact; publishing and the GitHub Release happen downstream.
- Pin the full NuGet restore graph with `packages.lock.json` and fail CI restore when the lock files are stale (`RestoreLockedMode`).

### API Changes
