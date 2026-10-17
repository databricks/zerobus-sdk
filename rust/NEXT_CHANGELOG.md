# NEXT CHANGELOG

## Release v2.13.0

### Major Changes

### New Features and Improvements

- On platforms where vendored `protoc` is not available, fallback to trying `protoc` on `PATH` or `PROTOC` environment variable override.

### Bug Fixes

### Documentation

### Internal Changes

- Added a stateful persistent-stream mock and integration coverage for create,
  flush, resume offset continuity, and unknown stream IDs.

### Breaking Changes

### Deprecations

### API Changes
