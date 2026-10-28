# NEXT CHANGELOG

## Release v3.0.0

### Major Changes

### New Features and Improvements

- Arrow Flight streams with IPC compression now pad IPC buffers to a multiple
  of 8 bytes instead of 64. With compression, 64-byte alignment gives no
  benefit and only adds padding bytes to the payload. Uncompressed payloads
  keep 64-byte alignment.
- Zeroparser's `MessageRegistry` is now aligned to 128 bytes. When threads
  share one registry and clone its `Arc` for every record, the reference
  counts were on the same cache line as the registry fields that the parser
  reads, which made parsing several times slower on many threads.
- Stream recovery now waits with exponential backoff and jitter instead of a
  fixed `recovery_backoff_ms` delay. After the n-th consecutive failure, the
  delay is `recovery_backoff_ms * 2^(n-1)`, capped at the new
  `recovery_backoff_max_ms` (default 30 seconds). The SDK waits the delay plus
  a random jitter of up to half of the delay, so no wait is shorter than
  before. A retry delay that the server attaches to its rejection
  (`google.rpc.RetryInfo`) is now the minimum wait, up to the same cap; the
  jitter does not apply to it.
  `recovery_retries` still applies to each recovery; only the backoff exponent
  continues across successful reconnects. It resets when a connection ends
  that stayed up for at least 30 seconds and either received an
  acknowledgement or had no sent records or batches left unacknowledged. This
  applies to JSON, Protocol Buffer, and Arrow Flight streams, and to initial
  stream creation. JSON and Protocol Buffer streams now also wait before the
  first reconnect after a connection error, and after a server close signal on
  a connection that was not healthy. With the defaults, `build()` or
  `build_arrow()` against an unavailable server now waits 30 to 45 seconds in
  total before it returns the error, instead of about 8 seconds. With the
  defaults, a recovery that keeps failing now waits at least 30 seconds before
  the stream fails (at least 60 seconds for JSON and Protocol Buffer streams
  after a connection error), instead of about 8 seconds. If `close()` on a
  JSON or Protocol Buffer stream finishes its flush while the stream waits for
  a backoff, the stream now stops at once instead of after the 1 s shutdown
  timeout.

### Bug Fixes

- The crate now requires `rustls-webpki` 0.103.16 or later and `webpki-roots`
  1.0.11 or later. These releases stop trusting the compromised eMudhra (emSign)
  CAs: rustls-webpki rejects chains through the emSign G3 issuing CAs, and the
  bundled root set no longer includes the emSign G3, C3 and C1 roots.
  Upgrading the SDK is enough to pick up the fix. The crate does not use either
  dependency directly; it declares them only to set the minimum versions.

### Documentation

- Documented persistent streams (behind the `eos` feature) in the Rust README,
  with a usage section, API reference entries, and a runnable JSON example that
  creates a durable stream, ingests in a loop with one `flush()`, and resumes by
  `stream_id`.

### Internal Changes

- Added a stateful persistent-stream mock and integration coverage for create,
  flush, resume offset continuity, and unknown stream IDs.
- Removed the `tokio-retry` dependency, because the wait now depends on the
  error. `tonic-types` is now a regular dependency instead of part of the
  `arrow-flight` feature.

### Breaking Changes

- Bumped the Arrow dependencies (`arrow-array`, `arrow-schema`, `arrow-ipc`, and
  the vendored `arrow-flight` fork) from `59.2` to `60.0`. The vendored
  `arrow-flight` crate was re-synced to upstream `60.0.0` with the slice-aware
  batch-split fix (arrow-rs#9388 / #5352) re-applied. Because the SDK re-exports
  Arrow types through its public API, consumers using the `arrow-flight`
  feature that exchange those types with the SDK must upgrade their own Arrow
  dependency to `60.x`; Arrow `59` and `60` types cannot be mixed in the same
  build. Arrow `60` requires Rust 1.88 or newer. Consumers on the default build
  (without the `arrow-flight` feature) are unaffected.

- Added `#[non_exhaustive]` to the `EncodedRecord`, `EncodedBatch`, and
  `EncodedBatchIter` enums, and to the doc-hidden `PreparedInput`. Enabling the
  `avro` feature anywhere in a build, or adding a record format in a later
  release, no longer breaks exhaustive matches in other crates
  ([#909](https://github.com/databricks/zerobus-sdk/issues/909)).

  Migration: add a `_ => { … }` arm to any `match` on these enums, such as one
  over the records returned by `get_unacked_records()` or
  `get_unacked_batches()`.

- `schema::UcColumn` and `schema::UcTableSchema` are now `#[non_exhaustive]`,
  so they can no longer be built with struct literals outside the SDK. Use
  `UcColumn::new(name, type_name)` with the `with_type_text`, `with_type_json`,
  `with_nullable`, and `with_position` setters, and
  `UcTableSchema::new(catalog_name, schema_name, name, columns)`. Fields stay
  public for reading, and deserializing from the Unity Catalog API response is
  unchanged. New Unity Catalog fields can now be added in minor releases.

### Deprecations

### API Changes

- Added `UcColumn::new`, `UcColumn::with_type_text`, `UcColumn::with_type_json`,
  `UcColumn::with_nullable`, `UcColumn::with_position`, and
  `UcTableSchema::new`.
- Added the `recovery_backoff_max_ms` option to `StreamConfigurationOptions`
  and `ArrowStreamConfigurationOptions`, the `recovery_backoff_max_ms` setters
  on `StreamBuilder` and `PersistentStreamBuilder`, and
  `stream_options::defaults::RECOVERY_BACKOFF_MAX_MS`.
