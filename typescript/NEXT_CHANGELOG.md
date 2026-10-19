# NEXT CHANGELOG

## Release v1.5.0

### Major Changes

### New Features and Improvements

- Add the Avro record format (Beta). Set `avroSchema` (the writer schema as a JSON
  string) on `TableProperties`, then ingest either a record object — encoded against
  the schema with `avsc` — or pre-encoded Avro bytes (a `Buffer`), through the existing
  `ingestRecordOffset` / `ingestRecordsOffset`. `avsc` is an optional dependency
  (`npm install avsc`); the pre-encoded-`Buffer` path needs no dependency. Avro is
  compiled into the published binaries by default; a from-source `npm run build` omits
  it (`npm run build:avro` / `build:all` enable it). Feature in development.

### Bug Fixes

### Documentation

- Document the Avro record format: README "Avro" section and `examples/avro/`.

### Internal Changes

- The public entry point is now a thin JavaScript wrapper (`wrapper.js`) over the native
  NAPI binding; it performs Avro object encoding and forwards all other calls unchanged.

### Breaking Changes

### Deprecations

### API Changes

- `TableProperties` gains an optional `avroSchema` field (JSON string); mutually exclusive
  with `descriptorProto`.
