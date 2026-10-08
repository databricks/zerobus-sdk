# NEXT CHANGELOG

## Release v2.12.0

### Major Changes

### New Features and Improvements

- Added the feature-gated `PersistentStream` API for creating durable ingestion
  streams and resuming them by `stream_id` from the last committed offset.
- Fewer duplicate records after a server close signal and after `close()` on
  JSON and Protocol Buffer gRPC streams. When records are still unacknowledged
  after the graceful-close wait, the SDK now half-closes the stream and applies
  the acknowledgments the server sends for up to 500 ms. With
  `stream_paused_max_wait_time_ms(Some(0))`, recovery can now take up to 500 ms
  longer when records are unacknowledged and the server has not ended the
  stream.

### Bug Fixes

### Documentation

### Internal Changes

- Extracted a private transport-generic mux core and lane contract. The gRPC
  mux keeps its existing behavior, while each lane owns its close and recovery
  paths.

### Breaking Changes

### Deprecations

### API Changes
