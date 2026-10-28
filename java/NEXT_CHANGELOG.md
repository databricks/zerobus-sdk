# NEXT CHANGELOG

## Release v1.8.0

### Major Changes

### New Features and Improvements

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

- A `flush()` or `waitForOffset()` timeout now closes the stream. This change comes from the
  Rust SDK 3.0.0 core. Recovery stops, and the SDK does not send the pending records again.
  On a failed record stream, the first `close()` now throws the error that made it fail.
  Later calls return without an error, because the wrapper has already released the stream.
  If an `AckCallback` is set, a timeout calls its `onError()` for each unacknowledged
  offset.
  Migration: call `recreateStream()` (or `recreateArrowStream()` for Arrow streams) on the
  closed stream. It resends the unacknowledged batches. Do not retry `flush()`,
  `waitForOffset()`, or `close()` on the same stream. An ingest call that fails because the
  stream is closing or closed did not admit its record. Send that record on the new stream.
  The server can already have accepted batches that were sent but not acknowledged, so a
  resend can duplicate those batches.

### Deprecations

### API Changes
