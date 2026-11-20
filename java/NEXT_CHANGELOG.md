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

- The JAR now includes the generated `InsertRecordsRequest` and
  `InsertRecordsResponse` classes from the shared service proto. They are wire
  types, not a supported API.
