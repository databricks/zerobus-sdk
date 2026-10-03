# NEXT CHANGELOG

## Release v2.12.0

### Major Changes

### New Features and Improvements

- On platforms where vendored `protoc` is not available, fallback to trying `protoc` on `PATH` or `PROTOC` environment variable override.

### Bug Fixes

### Documentation

### Internal Changes

- Extracted a private transport-generic mux core and lane contract. The gRPC
  mux keeps its existing behavior, while each lane owns its close and recovery
  paths.

### Breaking Changes

### Deprecations

### API Changes
