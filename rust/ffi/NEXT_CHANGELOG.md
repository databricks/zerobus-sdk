# NEXT CHANGELOG

## Release v1.10.0

### Major Changes

### New Features and Improvements

- Behavior change (no API change): stream creation and recovery retries now back off
  exponentially with jitter instead of waiting a fixed `recovery_backoff_ms` between
  attempts (from the Rust core). The wait starts at `recovery_backoff_ms` and is
  never shorter than it. It doubles after each consecutive failed attempt, up to
  15 s (or `recovery_backoff_ms` if larger), and a random jitter of up to the same
  amount is added, so a single wait can reach 30 s. With the defaults (2 s, 4
  retries), retries wait 2–4 s, 4–8 s, 8–16 s, then 15–30 s, so a stream that keeps
  failing gives up after about 29–58 s of waiting instead of 8 s. Recovery of a gRPC
  stream after a failure now also waits before its first reconnect attempt (2–4 s by
  default), so streams the server drops at the same time don't all reconnect at
  once; a gRPC stream that keeps failing during recovery gives up after about
  44–88 s instead of 8 s. A server-initiated Arrow Flight stream rotation still
  waits `recovery_backoff_ms` on average (a random wait between half and 1.5 times
  it), and a gRPC close signal still reconnects immediately. Clients rejected at the
  same time, for example by the server's stream-creation rate limit, no longer all
  retry at the same moment. A backoff of 0 still retries immediately.

  To keep a total retry window close to the previous one, lower `recovery_retries`.
  With the default 2 s base, 2 retries wait 6–12 s in total (14–28 s for gRPC
  recovery after a failure).

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

### Breaking Changes

### Deprecations

### API Changes
