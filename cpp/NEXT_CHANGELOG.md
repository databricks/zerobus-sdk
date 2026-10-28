# NEXT CHANGELOG

## Release v0.5.0

### New Features and Improvements

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

- A `flush()` or `wait_for_offset()` timeout now closes the stream. This change
  comes from the Rust core (v3.0.0). The stream stops recovery and does not send
  its unacknowledged records again. The first `close()` on that stream throws
  the stored error and sends nothing. A retried `close()` throws the same error.
  A late acknowledgment can make the call succeed, but the stream still closes.
  `close()` on a `Stream` that failed for any other reason now also throws the
  stored error.
  If an `ack_callback` is set, a timeout calls its `on_error()` for each
  unacknowledged offset.

  Migration: after `close()` throws, call `get_unacked_records()` (or
  `get_unacked_batches()` for `ArrowStream`). Create a new stream and resend
  those records there. Do not retry `flush()` or `close()` on the same stream.
  The server can already have accepted batches that were sent but not
  acknowledged, so resending can duplicate those batches.

### Deprecations

### API Changes
