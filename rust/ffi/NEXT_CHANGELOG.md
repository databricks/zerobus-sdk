# NEXT CHANGELOG

## Release v1.10.0

### Major Changes

### New Features and Improvements

### Bug Fixes

- The library is now built with `rustls-webpki` 0.103.16 and `webpki-roots`
  1.0.11, which stop trusting the compromised eMudhra (emSign) CAs.
- The `CRecordArray` returned by `encoded_records_to_c_array` is now allocated at
  exactly `len` records. It could previously have spare capacity, and
  `zerobus_free_record_array` then passed the wrong size when freeing it.
  Released builds were not affected: the library always uses the platform's
  system allocator, which frees the whole block and ignores the size on every
  supported platform. The fix makes the code meet Rust's allocation contract,
  so it stays correct if a size-aware allocator is ever used.

### Documentation

### Internal Changes

- `zerobus_stream_get_unacked_records` and its async variant now report an error
  through `CResult` for a record type the C API cannot represent, since the Rust
  SDK's record enums are now `#[non_exhaustive]`
  ([#909](https://github.com/databricks/zerobus-sdk/issues/909)). No record type
  reaches this path today. No ABI change; `zerobus.h` is unchanged.

### Behavior Changes

### Breaking Changes

- Behavior from the Rust SDK v3.0.0 core: a `zerobus_stream_flush`,
  `zerobus_stream_wait_for_offset`, `zerobus_arrow_stream_flush`, or
  `zerobus_arrow_stream_wait_for_offset` timeout (also through
  `zerobus_stream_flush_async` and `zerobus_stream_wait_for_offset_async`) now
  closes the stream. Recovery stops, and the stream never sends its unacknowledged
  records again. The timeout error can arrive up to 1 s after `flush_timeout_ms`,
  because the stream first drains its connection. A late acknowledgement of the
  target during that drain makes the call succeed, but the stream stays closed.
  A retried flush on the same stream fails unless every record was acknowledged,
  and `zerobus_stream_close` or `zerobus_stream_close_async` on a
  stream that failed or timed out now returns the error that made the stream
  terminal, instead of success. After an Arrow timeout,
  `zerobus_arrow_stream_close` returns the stored timeout error instead of
  flushing again.
  `CResult.is_retryable` stays `true` for these errors. No ABI change; `zerobus.h`
  is unchanged.

  Migration: after such an error, close the stream, get the records with
  `zerobus_stream_get_unacked_records` or
  `zerobus_arrow_stream_get_unacked_batches`, and resend them on a new stream. Do
  not retry flush or close on the same stream. The server can already have
  accepted batches that were sent but not acknowledged, so a resend can duplicate
  those batches.

### Deprecations

### API Changes
