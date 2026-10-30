# NEXT CHANGELOG

## Release v1.10.0

### Major Changes

### New Features and Improvements

- JSON and protobuf gRPC connections now use HTTP/2 keepalive (30 second PING
  interval, 10 second timeout) and TCP keepalive (30 second idle time) by
  default, picked up from the Rust SDK.

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

### Deprecations

### API Changes

- Added `zerobus_sdk_builder_http2_keepalive(builder, interval_ms, timeout_ms)`
  and `zerobus_sdk_builder_tcp_keepalive(builder, idle_ms)`. `0` disables
  (for `timeout_ms`, keeps the default). Additive; existing symbols are
  unchanged.
