# NEXT CHANGELOG

## Release v0.2.0

### New Features and Improvements

- JSON and protobuf streams now use a dedicated gRPC connection by default.
  Call `ZerobusSdk.CreateBuilder().ConnectionPerStream(false)` to share one
  HTTP/2 connection across streams.

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
- Documented that `GetUnackedRecords()` works only after the stream closes or
  fails, for example after a flush timeout, and stopped reporting success after
  ingest failures. Pointed CONTRIBUTING at `src/Zerobus/Native/`.

### Internal Changes

- Made the .NET release workflow build-only, consistent with the other SDKs. It now packs the NuGet package and uploads it as an artifact; publishing and the GitHub Release happen downstream.
- Pin the full NuGet restore graph with `packages.lock.json` and fail CI restore when the lock files are stale (`RestoreLockedMode`).

### Breaking Changes

- A `Flush()` or `WaitForOffset()` timeout now closes the stream. This change
  comes from the Rust core (v3.0.0). The stream stops recovery and does not send
  its unacknowledged records again. `Close()`, `CloseAsync()`, `Dispose()`, and
  `DisposeAsync()` on an already-closed stream do not report the stored error,
  so use the error from the failed call. Then call `RecreateStream()`, which
  resends the unacknowledged records on the new stream. Or call `CreateStream()`
  and resend the records from `GetUnackedRecords()`. Do not retry `Flush()` or
  `Close()` on the same stream. The server can already have accepted batches that
  were sent but not acknowledged, so those batches can be sent twice.

### API Changes
