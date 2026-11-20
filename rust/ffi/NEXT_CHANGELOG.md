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

- Arrow Flight streams now compress IPC payloads with ZSTD by default.
  `zerobus_arrow_get_default_config()` returns `ipc_compression = 1` (ZSTD) instead of `-1`
  (no compression), and passing `options = NULL` to `zerobus_sdk_create_arrow_stream` or
  `zerobus_sdk_create_arrow_stream_with_headers_provider` also uses ZSTD. Set
  `ipc_compression = -1` to keep sending uncompressed payloads.
- Stream creation and recovery retries now back off exponentially with jitter
  instead of waiting a fixed `recovery_backoff_ms` between attempts (from the Rust core).
  The wait still starts at `recovery_backoff_ms` and is never shorter than it. It
  doubles after each consecutive failed attempt, up to 15 seconds (or
  `recovery_backoff_ms` if larger), and a random jitter of up to the same amount is
  added. With the defaults (2 s, 4 retries), a stream that keeps failing gives
  up after about 29–58 s of waiting instead of 8 s. Clients rejected at the same
  time, for example by the server's stream-creation rate limit, no longer all
  retry at the same moment. No API change; a backoff of 0 still retries
  immediately.

### Breaking Changes

### Deprecations

### API Changes
