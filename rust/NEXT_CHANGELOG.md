# NEXT CHANGELOG

## Release v2.13.0

### Major Changes

### New Features and Improvements

- Added `MessageRegistry::from_descriptors` (behind the `zeroparser` feature) for building a registry from a parse root plus sibling/imported top-level messages, each keyed by its package-qualified FQN. This lets the zeroparser decode messages whose root fields reference imported well-known types (e.g. `google.protobuf.Timestamp`, `Duration`, and the wrapper types) that `from_descriptor` alone does not register. `others` takes borrowed `&[(&str, &DescriptorProto)]` pairs.

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
