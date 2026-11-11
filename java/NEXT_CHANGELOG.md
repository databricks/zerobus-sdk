# NEXT CHANGELOG

## Release v1.9.0

### Major Changes

### New Features and Improvements

- The SDK now also works with the protobuf-java 3.25 runtime. Applications that declare
  `protobuf-java` 3.25.x themselves can use the regular JAR. The default runtime is unchanged.

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
