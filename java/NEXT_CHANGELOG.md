# NEXT CHANGELOG

## Release v1.9.0

### Major Changes

### New Features and Improvements

- Applications on the protobuf-java 3.25 runtime can use the SDK. The SDK now depends on
  `protobuf-java` 3.25.9 (previously 4.33.0), and its generated service classes are built with
  protoc 3.25.9. The SDK is tested with protobuf-java 3.25.9 and 4.36.2; 3.24 and earlier 3.x
  releases are not supported. Applications that don't declare `protobuf-java` themselves now get
  3.25.9 transitively instead of 4.33.0, so declare `protobuf-java` 4.x in your build to keep 4.x.

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
