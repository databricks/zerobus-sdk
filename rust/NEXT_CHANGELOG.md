# NEXT CHANGELOG

## Release v3.3.0

### Major Changes

### New Features and Improvements

- Added personal access token (PAT) authentication (Private Preview) behind the new
  opt-in `pat-auth` feature. PAT authentication for Zerobus requires server-side
  enablement and is not available for Arrow Flight streams yet. Call
  `.personal_access_token(token)` on `StreamBuilder` or `PersistentStreamBuilder` to
  authenticate with a Databricks PAT created with the `zerobus` API scope. The SDK
  sends the token as given and never refreshes it, and never includes it in `Debug`
  output. An authentication rejection while creating the stream fails immediately
  instead of spending the one-shot credential-refresh retry.

### Bug Fixes

- `FederatedTokenProvider` (and `StreamBuilder::federated_auth`) now fails with a
  non-retryable `ZerobusError::InvalidArgument` when the IdP token supplier
  returns an empty or whitespace-only token. Before, the SDK sent an empty
  `subject_token` to the token endpoint.

### Documentation

### Internal Changes

- Added end-to-end tests for external-IdP token federation through
  `StreamBuilder::federated_auth`. They run against the mock token endpoint and
  mock gRPC server and cover the exchange request in both modes, token caching
  and isolation, refresh, auth rejection, and token-endpoint errors. The mock
  token endpoint now records each request, and the mock gRPC server records each
  stream's `authorization` and table-name headers.

### Breaking Changes

### Deprecations

### API Changes

- Added `StreamBuilder::personal_access_token` and
  `PersistentStreamBuilder::personal_access_token` (behind `pat-auth`; the persistent
  builder also needs `eos`). An empty token is rejected with
  `ZerobusError::InvalidArgument`; as with the other authentication setters, the last
  call wins (Private Preview).
- Added `PatHeadersProvider` (behind `pat-auth`), a `HeadersProvider` that sends a
  personal access token (Private Preview).
- Added `HeadersProvider::can_refresh`, a defaulted method returning `true`. A provider
  with a fixed credential returns `false` to skip the one-shot auth retry during stream
  creation; `PatHeadersProvider` does. Existing implementations are unaffected.
