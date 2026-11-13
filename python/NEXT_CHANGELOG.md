# NEXT CHANGELOG

## Release v1.12.0

### Major Changes

### New Features and Improvements

### Bug Fixes

- A `FederatedToken` whose `idp_token_supplier` returns an empty or
  whitespace-only string now fails `create_stream` with `NonRetriableException`.
  Before, the SDK sent an empty `subject_token` to the token endpoint.

### Documentation

- Documented the GCP workspace URL and Zerobus endpoint formats. The formats for
  AWS, Azure, and GCP now live in one [connection URL
  table](https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id),
  and this SDK's README, examples, proto generator, and API docs link to it instead
  of repeating cloud-specific URLs.
- The examples exit with an error naming the missing environment variable instead of
  falling back to placeholder values.

### Internal Changes

- Added end-to-end tests for `FederatedToken` that call `create_stream` against a
  mock token endpoint, covering the exchange request in both modes, token caching
  and isolation, refresh, supplier errors, and token-endpoint errors, on the sync
  and async SDKs.

### Breaking Changes

### Deprecations

### API Changes
