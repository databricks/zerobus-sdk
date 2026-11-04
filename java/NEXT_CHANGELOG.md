# NEXT CHANGELOG

## Release v1.9.0

### Major Changes

### New Features and Improvements

### Bug Fixes

### Documentation

- Documented the GCP workspace URL and Zerobus endpoint formats. The formats for
  AWS, Azure, and GCP now live in one [connection URL
  table](https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id),
  and this SDK's README, examples, proto generator docs, and API docs link to it
  instead of repeating cloud-specific URLs.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

- Added `ZerobusSdk.forTestingNoTls` (Beta, testing only) for the Layer-1 conformance
  harness. It connects over plaintext to an `http://` endpoint. The native no-TLS path is
  gated behind the JNI `testing` feature; without it the method throws `ZerobusException`.
  Never use it against a real endpoint.
