# NEXT CHANGELOG

## Release v0.4.0

### New Features and Improvements

- Added support for `TINYINT/BYTE`, `TIMESTAMP_NTZ`, and `VARIANT` data types in the proto generation tool

- **Alternative Ingestion API with Direct Offset Return**: Added `ingest_record_offset()` and `ingest_records_offset()` methods
  - Return `OffsetId` (logical offset) directly as an integer (after queuing) instead of wrapping it in a Future
  - Can be used with new `wait_for_offset()` method to block on acknowledgment when needed
  - Allows decoupling record ingestion from acknowledgment tracking
  - Useful for scenarios where you want to collect offsets and wait on them selectively

### Deprecations

- **Deprecated `ingest_record()` and `ingest_records()` methods**: Use `ingest_record_offset()` and `ingest_records_offset()` instead
  - The new methods return offsets directly (after queuing) without Future wrapping for a cleaner API
  - Use with `wait_for_offset()` to explicitly wait for acknowledgments when needed
  - Old methods will continue to work but may be removed in a future version

### Bug Fixes

### Documentation

### Internal Changes

- Refactored `flush()` and `wait_for_offset()` to share common waiting logic via `wait_for_offset_internal()`, reducing code duplication and ensuring consistent behavior

- Improved graceful close mechanism: when server signals stream closure, SDK now continues processing acknowledgments for in-flight records while pausing new record transmission until timeout.

### API Changes

- Added Arrow IPC compression support by adding `ipc_compression` to `ArrowStreamConfigurationOptions` (`None` (default), `Lz4`, `Zstd`)
- Changed `ZerobusArrowStream::ingest_batch()` to return `OffsetId` directly instead of `Future<Output = OffsetId>`
- Added `ZerobusArrowStream::wait_for_offset()` method to wait for acknowledgment of a specific offset
- Added `is_closed` check at the beginning of `flush()` for both `ZerobusStream` and `ZerobusArrowStream`

- Added `ingest_record_offset()` method to `ZerobusStream` for direct offset return without Future wrapping
- Added `ingest_records_offset()` method to `ZerobusStream` for batch ingestion with direct offset return
- Added `wait_for_offset()` method to `ZerobusStream` to wait for acknowledgment of a specific offset

- [**BREAKING**] Added `stream_paused_max_wait_time_ms` to `StreamConfigurationOptions` to configure maximum wait time during graceful stream close (`None` = wait for full server duration, `Some(0)` = immediate recovery, `Some(x)` = wait up to min(x, server_duration) milliseconds)