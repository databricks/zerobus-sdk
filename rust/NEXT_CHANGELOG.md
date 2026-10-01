# NEXT CHANGELOG

## Release v2.12.0

### Major Changes

### New Features and Improvements

- Added Rust-only multiplexed Arrow Flight streams via
  `.arrow(schema).multiplexed(n).build_arrow()`. Whole batches route round-robin
  with a mux-wide `max_inflight_batches` budget, lane-local recovery, and the
  existing mux poisoning and message-ID semantics.

### Bug Fixes

- Arrow `ingest_batch` calls blocked on `max_inflight_batches` now fail as soon as
  `close()` starts instead of waiting for close to finish.

### Documentation

- Documented Arrow mux lifecycle, capacity, partial-ack recovery, and shared
  lane-local telemetry; added the `arrow_multiplexed` example.

### Internal Changes

- Extracted a private transport-generic mux core and lane contract. The gRPC
  mux keeps its existing behavior, while each lane owns its close and recovery
  paths.

### Breaking Changes

### Deprecations

### API Changes

- Added `MultiplexedArrowStream` and `MultiplexedStreamBuilder::build_arrow()`
  behind the `arrow-flight` feature, with batch and IPC ingestion, message waits,
  flush, close, and unacknowledged-batch retrieval.
