# NEXT CHANGELOG

## Release v2.12.0

### Major Changes

### New Features and Improvements

- Added the feature-gated `PersistentStream` API for creating durable ingestion
  streams and resuming them by `stream_id` from the last committed offset.
- Arrow Flight streams now compress IPC payloads with ZSTD by default.
  `ArrowStreamConfigurationOptions::ipc_compression` defaults to
  `Some(CompressionType::ZSTD)` instead of `None`. To keep sending uncompressed
  payloads, call `.ipc_compression(None)` on the stream builder.

- Added Rust-only multiplexed Arrow Flight streams via
  `.arrow(schema).multiplexed(n).build_arrow()`. Whole batches route round-robin
  with a mux-wide `max_inflight_batches` budget, lane-local recovery, and the
  existing mux poisoning and message-ID semantics.

- Fewer duplicate records after a server close signal and after `close()` on
  JSON and Protocol Buffer gRPC streams. When records are still unacknowledged
  after the graceful-close wait, the SDK now half-closes the stream and applies
  the acknowledgments the server sends for up to 500 ms. With
  `stream_paused_max_wait_time_ms(Some(0))`, recovery can now take up to 500 ms
  longer when records are unacknowledged and the server has not ended the
  stream.

### Bug Fixes

- Arrow `ingest_batch` calls blocked on `max_inflight_batches` now fail as soon as
  `close()` starts instead of waiting for close to finish.

### Documentation

- Documented Arrow mux lifecycle, capacity, partial-ack recovery, and telemetry
  restrictions; added the `arrow_multiplexed` example.

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
