# NEXT CHANGELOG

## Release v1.5.0

### Major Changes

### New Features and Improvements

- Add the Avro record format (Beta). Set `avroSchema` (the writer schema as a JSON
  string) on `TableProperties`, then ingest either a record object — encoded against
  the schema with `avsc` — or pre-encoded Avro bytes (a `Buffer`), through the existing
  `ingestRecordOffset` / `ingestRecordsOffset`. `avsc` is an optional dependency
  (`npm install avsc`); the pre-encoded-`Buffer` path needs no dependency. The published
  (CI-built) release binaries include Avro; a from-source `npm run build` omits it
  (`npm run build:avro` / `build:all` enable it). Feature in development.

### Bug Fixes

- Updated the native addon's transitive `rustls-webpki` to 0.103.16 and
  `webpki-roots` to 1.0.11, which stop trusting the compromised eMudhra (emSign)
  CAs.

### Documentation

- Document the Avro record format: README "Avro" section and `examples/avro/`.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

- The package entry point is now `wrapper.js` / `wrapper.d.ts`, a thin JavaScript wrapper over
  the native binding that encodes Avro record objects and forwards other calls unchanged.
- `TableProperties` gains an optional `avroSchema` field (JSON string); mutually exclusive
  with `descriptorProto`.
