# NEXT CHANGELOG

## Release v1.10.0

### Major Changes

### New Features and Improvements

- Add the Avro record format (Beta) behind the `avro` feature, exposed in the C
  header under `#if defined(ZEROBUS_AVRO)`: `zerobus_sdk_create_avro_stream`
  (+ `_async` / `_with_headers_provider` / `_with_headers_provider_async`), which
  takes the writer schema JSON at creation, and `zerobus_stream_ingest_avro_record`
  / `_records` (+ `_async` / `_nowait`), which ingest a pre-encoded Avro datum
  (single or batch). Additive — existing functions and `CStreamConfigurationOptions`
  are unchanged, and the header is byte-identical with and without the feature.
  Ephemeral streams only; server support is pending.
- The C ABI carries pre-encoded datums only; it holds no Avro encoder. Wrappers
  that offer a record-object API encode the object to a datum natively (in their
  own language) and pass the bytes through `zerobus_stream_ingest_avro_record`.

### Bug Fixes

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
