# NEXT CHANGELOG

## Release v3.2.0

### Major Changes

### New Features and Improvements

### Bug Fixes

### Documentation

### Internal Changes

- The SDK build script now regenerates the gRPC code when
  `zerobus_service.proto` changes in builds with the `zeroparser` feature
  (such as `cargo test --workspace` and `--all-features` builds). It used to
  keep the stale generated code until the next clean build.

### Breaking Changes

### Deprecations

### API Changes
