# NEXT CHANGELOG

## Release v0.2.1

### New Features and Improvements

- **Alternative Ingestion API with Direct Offset Return**: Added `ingest_record_v2()` and `ingest_records_v2()` methods
  - Return `OffsetId` (logical offset) directly as an integer instead of wrapping it in a Future
  - Can be used with new `wait_for_offset()` method to block on acknowledgment when needed
  - Allows decoupling record ingestion from acknowledgment tracking
  - Useful for scenarios where you want to collect offsets and wait on them selectively

### Bug Fixes

### Documentation

### Internal Changes

- Refactored `flush()` and `wait_for_offset()` to share common waiting logic via `wait_for_offset_internal()`, reducing code duplication and ensuring consistent behavior

- Improved graceful close mechanism: when server signals stream closure, SDK now continues processing acknowledgments for in-flight records while pausing new record transmission until timeout.

### API Changes

- Added `ingest_record_v2()` method to `ZerobusStream` for immediate offset return without Future wrapping
- Added `ingest_records_v2()` method to `ZerobusStream` for batch ingestion with immediate offset return
- Added `wait_for_offset()` method to `ZerobusStream` to wait for acknowledgment of a specific offset

- [**BREAKING**] Added `stream_paused_max_wait_time_ms` to `StreamConfigurationOptions` to configure maximum wait time during graceful stream close (`None` = wait for full server duration, `Some(0)` = immediate recovery, `Some(x)` = wait up to min(x, server_duration) milliseconds)