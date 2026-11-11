# NEXT CHANGELOG

## Release v3.3.0

### Major Changes

### New Features and Improvements

### Bug Fixes

- Arrow Flight streams no longer log a warning on every connection when a
  `HeadersProvider` sends `x-databricks-zerobus-table-name` with the stream's own
  table, as the README tells custom providers to do. The warning now appears only
  when the provider's table differs from the stream's. The stream's table is
  still the one sent.

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
