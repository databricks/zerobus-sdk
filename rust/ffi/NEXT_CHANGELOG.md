# NEXT CHANGELOG

## Release v1.9.0

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

### Documentation

### Internal Changes

### Behavior Changes

### Breaking Changes

### Deprecations

### API Changes
