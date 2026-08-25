# NEXT CHANGELOG

## Release v2.11.0

### Major Changes

### New Features and Improvements

- Added Avro support to `StreamBuilder::multiplexed(n)` (Beta) when the `avro`
  feature is enabled; every lane shares the configured writer schema.

### Bug Fixes

### Documentation

- Added a multiplexed Avro example using loop-then-flush ingestion.

### Internal Changes

- Added the feature-gated persistent gRPC transport, durable wire offsets,
  resume-watermark reconciliation after a lost acknowledgment, and validation
  for setup responses, acknowledgment bounds, and offset overflow.

### Breaking Changes

### Deprecations

### API Changes
