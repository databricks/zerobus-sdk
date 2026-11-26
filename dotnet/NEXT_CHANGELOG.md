# NEXT CHANGELOG

## Release v0.2.0

### New Features and Improvements

- JSON and protobuf streams now use a dedicated gRPC connection by default.
  Call `ZerobusSdk.CreateBuilder().ConnectionPerStream(false)` to share one
  HTTP/2 connection across streams.
- Added `ZerobusSdk.CreateStreamBuilder()`, a fluent builder that is now the
  recommended way to create a stream. Set the table, authentication
  (`OAuth()` or `HeadersProvider()`), and configuration overrides, then select
  the format with `Json()` or `Proto(descriptorProto)` and call `Build()` or
  `BuildAsync()`.

### Deprecations

- Deprecated `CreateStream`, `CreateJsonStream`, `CreateProtoStream`, and their
  `WithHeadersProvider` and `Async` variants in favor of
  `CreateStreamBuilder()`. They will be removed in the next major release. For
  example, replace `sdk.CreateJsonStream(table, clientId, clientSecret, options)`
  with `sdk.CreateStreamBuilder().Table(table).OAuth(clientId, clientSecret).Json().Build()`,
  moving each `StreamConfigurationOptions` value to the builder setter of the
  same name.

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
- `CreateJsonStream*` and `CreateProtoStream*` now throw `ObjectDisposedException`
  after the SDK is disposed, like `CreateStream*`.

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

- New public API: `ZerobusSdk.CreateStreamBuilder()`, `StreamBuilder`,
  `JsonStreamBuilder`, and `ProtoStreamBuilder`.
- Marked the `CreateStream*`, `CreateJsonStream*`, and `CreateProtoStream*`
  methods `[Obsolete]`.
