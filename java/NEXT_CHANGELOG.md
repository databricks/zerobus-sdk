# NEXT CHANGELOG

## Release v1.8.0

### Major Changes

### New Features and Improvements

- Add Avro record format (Beta) via `streamBuilder().avro(schemaJson)`, producing a
  `ZerobusAvroStream`. Ingest `GenericRecord` objects (encoded against the writer schema) or
  pre-encoded Avro datums (`byte[]`), one at a time or in batches. Ingesting record objects requires
  Java 11+ and the `org.apache.avro:avro` dependency (supplied by the caller, like `arrow-vector`);
  the pre-encoded `byte[]` path and the rest of the SDK run on Java 8. A record's schema must match
  the writer schema (field order and namespace matter; metadata is ignored), and encode failures are
  non-retriable. See the README for the Java value each Avro type expects.

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

- `EncodedBatch(List<byte[]>, boolean)` — use `EncodedBatch(List<byte[]>, RecordFormat)` instead; the
  boolean cannot represent the Avro format (it maps `false` to Protocol Buffers).

### API Changes

- New public API: `RecordFormat` enum; `EncodedBatch.getFormat()` and the
  `EncodedBatch(List<byte[]>, RecordFormat)` constructor; and `ZerobusSdk.recreateStream(ZerobusAvroStream)`.
