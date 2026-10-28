# NEXT CHANGELOG

## Release v1.8.0

### New Features and Improvements

### Deprecations

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

- A `Flush()` or `WaitForOffset()` timeout now closes the stream. This change
  comes from the Rust core (v3.0.0). The stream stops recovery and does not send
  its pending records again. The first `Close()` on that stream returns the
  stored error and sends nothing. Later `Close()` calls on the same value return
  `nil`. A late acknowledgment can make the call succeed, but the stream still
  closes. `Close()` on a `ZerobusStream` that failed for any other reason now
  also returns the stored error, instead of `nil`.

  Migration: call `GetUnackedRecords()` (or `GetUnackedBatches()` for Arrow)
  before `Close()`. Create a new stream and resend those records there. Do not
  retry `Flush()` or `Close()` on the same stream. The server can already have
  accepted batches that were sent but not acknowledged, so resending can
  duplicate those batches.

### API Changes
