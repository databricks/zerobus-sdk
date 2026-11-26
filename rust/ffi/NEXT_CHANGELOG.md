# NEXT CHANGELOG

## Release v1.10.0

### Major Changes

### New Features and Improvements

- Added external-IdP token federation, for example with Microsoft Entra ID.
  `zerobus_sdk_create_stream_with_federated_token` and
  `zerobus_sdk_create_arrow_stream_with_federated_token` take an
  `IdpTokenSupplierCallback` that returns the current IdP token (or an error and
  whether it is retryable) as a `CIdpToken`. The SDK exchanges the token for a
  Zerobus-scoped Databricks token (RFC 8693), caches it, and refreshes it before
  it expires. A null `client_id` selects account-level federation; a non-null
  one selects workload identity federation as that service principal.
  `cache_identity` partitions the token cache per logical identity. Ownership of
  `user_data` and `free_user_data` follows the headers-provider entry points
  ([#1069](https://github.com/databricks/zerobus-sdk/issues/1069)).

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

- Added `CIdpToken`, `IdpTokenSupplierCallback`,
  `zerobus_sdk_create_stream_with_federated_token`, and
  `zerobus_sdk_create_arrow_stream_with_federated_token`. The change to
  `zerobus.h` is additive.
