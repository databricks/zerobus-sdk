# NEXT CHANGELOG

## Release v1.8.0

### Major Changes

### New Features and Improvements

- Add Avro record format (Beta) via `streamBuilder().avro(schemaJson)`, producing a
  `ZerobusAvroStream`. Ingest `GenericRecord` objects (encoded against the writer schema) or
  pre-encoded Avro datums (`byte[]`), one at a time or in batches. Requires Java 11+ and the
  `org.apache.avro:avro` dependency (supplied by the caller, like `arrow-vector`); the rest of the
  SDK still runs on Java 8. See the README for the Java value each Avro type expects.

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

- New public API: `RecordFormat` enum; `EncodedBatch.getFormat()` and the
  `EncodedBatch(List<byte[]>, RecordFormat)` constructor; and `ZerobusSdk.recreateStream(ZerobusAvroStream)`.
