# NEXT CHANGELOG

## Release v3.3.0

### Major Changes

### New Features and Improvements

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
