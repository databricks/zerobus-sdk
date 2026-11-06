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

- Added the testing-only `ZerobusSdk.forTestingNoTls` entry point for the Layer-1
  conformance test harness. The native no-TLS path is gated behind the JNI `testing`
  feature; without a testing-enabled native library the method throws, and it must
  never be used against a real endpoint.

### Breaking Changes

### Deprecations

### API Changes
