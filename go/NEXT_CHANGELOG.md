# NEXT CHANGELOG

## Release v1.9.0

### New Features and Improvements

- Arrow Flight streams now compress IPC payloads with ZSTD by default.
  `DefaultArrowStreamConfigurationOptions()` sets `IPCCompression` to `IPCCompressionZstd`, and the
  zero value `IPCCompressionDefault` and nil options also mean ZSTD instead of no compression. To
  keep sending uncompressed payloads, set `IPCCompression` to `IPCCompressionNone`.
- Added external-IdP token federation, for example with Microsoft Entra ID, so a
  stream can authenticate with an IdP token instead of a Databricks OAuth client
  secret. Build a `FederatedToken` with `NewFederatedToken` from an
  `IdpTokenSupplier`, optionally with `WithClientID` for workload identity
  federation, and pass it to `CreateStreamWithFederatedToken` or
  `CreateArrowStreamWithFederatedToken`. The SDK exchanges the IdP token for a
  Zerobus-scoped Databricks token (RFC 8693), caches it, and refreshes it before
  it expires ([#1069](https://github.com/databricks/zerobus-sdk/issues/1069)).

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

- Added `IdpTokenSupplier`, `FederatedToken`, `FederatedTokenOption`,
  `NewFederatedToken`, `WithClientID`,
  `ZerobusSdk.CreateStreamWithFederatedToken`, and
  `ZerobusSdk.CreateArrowStreamWithFederatedToken`.
