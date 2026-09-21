# NEXT CHANGELOG

## Release v2.10.0

### Major Changes

### New Features and Improvements

- Added `StreamBuilder::multiplexed(n)` (Beta) for JSON and protobuf streams, with
  concurrent construction, a shared in-flight budget, and a separate
  `multiplexed_ack_callback` for `MessageId` notifications. Existing ordinary
  `ack_callback` usage is unchanged. Multiplexed Avro streams are also supported
  when the `avro` feature is enabled; every lane shares the configured writer schema.
- Multi-lane multiplexed construction opens sub-streams concurrently with
  bounded random startup jitter and cleans up successful opens if construction
  fails or is cancelled. A single lane opens immediately.
- Multiplexed streams divide the mux-wide `max_inflight_requests` budget evenly
  across sub-streams.

### Bug Fixes

- Cancelled gRPC stream construction now cancels and aborts supervisor,
  callback, sender, and receiver tasks before ownership is returned. This
  applies to ordinary and multiplexed stream builds.

### Documentation

- Added multiplexed-stream guidance and a complete compiled-protobuf example
  with queued ingestion, periodic flushing, `MessageId` callbacks, and close.
- Added a multiplexed Avro example using loop-then-flush ingestion.

### Internal Changes

- Made gRPC graceful-close deadline tests deterministic with a paused clock and
  acknowledgment barriers, avoiding false failures from Windows scheduling delays.

### Breaking Changes

### Deprecations

### API Changes

- Exported `MultiplexedStream`, `MultiplexedStreamBuilder`, and `MessageId` with
  default features.
- Added `StreamBuilder::multiplexed_ack_callback` for `MessageId` callbacks
  while preserving `ack_callback` for ordinary `OffsetId` callbacks; each
  terminal mode rejects the other mode's callback.
- Added `MultiplexedStream::new_record()` for dynamic-protobuf records.
