# NEXT CHANGELOG

## Release v1.9.0

### Major Changes

### New Features and Improvements
- Add Avro record format support. Create an Avro stream with `TableProperties(..., avro_schema=json_schema)`; records can be passed as dicts (encoded via fastavro — install the `avro` extra) or pre-encoded bytes (no extra needed).
- `TableProperties` exposes a read-only `record_format` property (`"proto"`, `"json"`, or `"avro"`).

### Bug Fixes
- Records are now coerced strictly by the stream's declared format instead of being inferred from each payload's type. A payload that doesn't match the stream — e.g. a `str` on a proto stream — now raises `TypeError` rather than being silently mis-encoded.

### Documentation

### Internal Changes
- Type stubs mark `TableProperties.table_name`, `avro_schema`, and `record_format` as read-only properties. Assignment already raised `AttributeError` at runtime.

### Breaking Changes
- `StreamConfigurationOptions.record_type` was previously ignored; it is now validated against the format inferred from `TableProperties`. An explicit `record_type` that disagrees — e.g. `RecordType.PROTO` on a table with no descriptor (a JSON stream), or `RecordType.JSON` on a descriptor table — now raises `ValueError` at stream creation. Migration: omit `record_type` (it defaults to `RecordType.UNSPECIFIED`), or set it to match `TableProperties.record_format`.

### Deprecations

### API Changes
- `RecordType` gains `UNSPECIFIED` (the default for `StreamConfigurationOptions.record_type`) and `AVRO`. When set to a value other than `UNSPECIFIED`, `record_type` is validated against the stream's schema and must agree with it.
