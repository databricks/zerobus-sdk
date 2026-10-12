# NEXT CHANGELOG

## Release v2.12.0

### Major Changes

### New Features and Improvements

- Added the feature-gated `PersistentStream` API for creating durable ingestion
  streams and resuming them by `stream_id` from the last committed offset.
- Arrow Flight streams now compress IPC payloads with ZSTD by default.
  `ArrowStreamConfigurationOptions::ipc_compression` defaults to
  `Some(CompressionType::ZSTD)` instead of `None`. To keep sending uncompressed
  payloads, call `.ipc_compression(None)` on the stream builder.

### Bug Fixes

### Documentation

### Internal Changes

- Extracted a private transport-generic mux core and lane contract. The gRPC
  mux keeps its existing behavior, while each lane owns its close and recovery
  paths.

### Breaking Changes

- `EncodedRecord`, `EncodedBatch`, and `EncodedBatchIter` are now
  `#[non_exhaustive]`. Their `Avro` variants exist whenever any crate in the
  build enables the `avro` feature, so an exhaustive `match` could stop
  compiling because of an unrelated dependency. Add a `_ =>` arm to matches on
  these types; future record formats can then ship without a major release.
- `EncodedBatch` variants now hold the SDK-owned `BatchRecords<T>` instead of
  `smallvec::SmallVec<[T; 1]>`, and `EncodedBatchIter` variants hold
  `BatchRecordsIntoIter<T>` instead of `smallvec::IntoIter<[T; 1]>`, so
  `smallvec` is no longer part of the public API. `BatchRecords` derefs to
  `[T]` and implements `IntoIterator`, `FromIterator`, and `From<Vec<T>>`;
  replace `smallvec![..]` or `SmallVec::from_vec(v)` with `v.into()`. A
  single-record batch is still stored inline.

### Deprecations

### API Changes

- Added `BatchRecords` and `BatchRecordsIntoIter`.
