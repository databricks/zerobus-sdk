# NEXT CHANGELOG

## Release v1.5.0

### Major Changes

### New Features and Improvements

### Bug Fixes

- Updated the native addon's transitive `rustls-webpki` to 0.103.16 and
  `webpki-roots` to 1.0.11, which stop trusting the compromised eMudhra (emSign)
  CAs.

### Documentation

### Internal Changes

### Breaking Changes

- A `flush()` or `waitForOffset()` timeout now closes the stream. This change comes from the
  Rust SDK 3.0.0 core. Recovery stops, and the SDK does not send the pending records again.
  On a failed record stream, the first `close()` now rejects with the error that made it
  fail. Later calls resolve, because the wrapper has already released the stream.
  Migration: call `recreateStream()` (or `recreateArrowStream()` for Arrow streams) on the
  closed stream, before `close()`. It resends the unacknowledged batches from
  `getUnackedBatches()`. Do not retry `flush()`, `waitForOffset()`, or `close()` on the
  same stream. An ingest call that fails because the stream is closing or closed did not
  admit its record. Send that record on the new stream. The server can already have
  accepted batches that were sent but not acknowledged, so a resend can duplicate those
  batches.

### Deprecations

### API Changes
