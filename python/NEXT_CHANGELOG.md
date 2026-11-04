# NEXT CHANGELOG

## Release v1.12.0

### Major Changes

### New Features and Improvements

### Bug Fixes

### Documentation

- Documented the GCP workspace URL and Zerobus endpoint formats. The formats for
  AWS, Azure, and GCP now live in one [connection URL
  table](https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id),
  and this SDK's README, examples, proto generator, and API docs link to it instead
  of repeating cloud-specific URLs.
- The examples exit with an error naming the missing environment variable instead of
  falling back to placeholder values.

### Internal Changes

- Added testing-only `no_tls` (sync `ZerobusSdk`) and `no_auth` (`create_stream`)
  paths for the Layer-1 conformance test harness, gated behind the `testing` feature
  (`maturin build --features testing`). They disable TLS and auth and must never be
  used against a real endpoint; they are absent from released wheels.

### Breaking Changes

### Deprecations

### API Changes
