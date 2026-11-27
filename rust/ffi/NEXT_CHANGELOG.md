# NEXT CHANGELOG

## Release v1.10.0

### Major Changes

### New Features and Improvements

- Add multiplexed gRPC streams (Beta). `zerobus_sdk_create_multiplexed_stream` and `zerobus_sdk_create_multiplexed_stream_with_headers_provider` open `stream_count` sub-streams and return a `CZerobusMultiplexedStream*`. Ack callbacks receive message IDs and are passed as create parameters, with `free_ack_user_data` releasing `ack_user_data` after the last callback. Every multiplexed call except `is_closed` and `free` also has an `_async` variant that reports through a completion callback (`CreateMultiplexedStreamAsyncCallback` for the creates, `MessageIdAsyncCallback` for the ingests). The `zerobus_multiplexed_stream_*` functions ingest proto/JSON records (single and batch), wait for message IDs, flush, close, and recover unacknowledged records. Blocking multiplexed calls made from inside an ack callback fail with a non-retryable error; use the `_async` calls.

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

- Add the `CZerobusMultiplexedStream` opaque type, the `CreateMultiplexedStreamAsyncCallback` and `MessageIdAsyncCallback` types, the `zerobus_sdk_create_multiplexed_stream` and `zerobus_sdk_create_multiplexed_stream_with_headers_provider` functions with their `_async` variants, and the `zerobus_multiplexed_stream_*` functions to `zerobus.h`. Purely additive.
