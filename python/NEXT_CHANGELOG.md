# NEXT CHANGELOG

## Release v1.9.0

### Major Changes

### New Features and Improvements
- Add Avro record format support (build-gated via `avro` feature). Records can be passed as dicts (encoded via fastavro) or pre-encoded bytes. Use `TableProperties(..., avro_schema=json_schema)` to create Avro streams.
- `TableProperties` exposes a read-only `record_format` property (`"proto"`, `"json"`, or `"avro"`).

### Bug Fixes
- Records are now coerced strictly by the stream's declared format instead of being inferred from each payload's type. A payload that doesn't match the stream — e.g. a `str` on a proto stream — now raises `TypeError` rather than being silently mis-encoded.

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
- `RecordType` gains `UNSPECIFIED` (the default for `StreamConfigurationOptions.record_type`) and `AVRO`. When set to a value other than `UNSPECIFIED`, `record_type` is validated against the stream's schema and must agree with it.
