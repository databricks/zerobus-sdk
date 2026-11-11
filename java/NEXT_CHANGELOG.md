# NEXT CHANGELOG

## Release v1.9.0

### Major Changes

### New Features and Improvements

- Native core log events (connection failures, recovery attempts, token refreshes, server
  errors) are now forwarded to SLF4J under `com.databricks.zerobus.core`, followed by the Rust
  module path. Core events are filtered by the effective level of that logger, so set it to DEBUG
  to see core debug output. See the Logging section of the README.

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
