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

- Added `StreamBuilder::max_message_size_bytes` to split Arrow Flight batches by
  encoded size instead of by row count. Each message is kept within the limit
  (measured before gRPC compression), so a batch is sent in as few messages as the
  limit allows. The default is unchanged. A single row larger than the limit is
  sent as its own message over the limit, because rows are not split.

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

- The mock gRPC server used by the repository's tests moved to an unpublished
  `test-support` crate that generates its own server bindings, and the SDK no
  longer generates gRPC server code.

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

- Removed `tonic` types from the public API. `tonic` is pre-1.0, and each 0.x
  minor release is a breaking change, so a `tonic` upgrade no longer breaks the
  SDK API. Two parts of the API named `tonic` types:

  - `ZerobusError::CreateStreamError` and `ZerobusError::StreamClosedError` now
    carry a `GrpcStatus` instead of a `tonic::Status`. `GrpcStatus::code()`
    returns a `GrpcCode` (`#[non_exhaustive]`, 17 gRPC status codes) and
    `GrpcStatus::message()` returns the message. The `Display` output is the
    same as the output of `tonic::Status` today, so error strings do not change.
    Retry and authentication classification do not change. The status no longer
    exposes `tonic::Status` details and metadata.

    Migration: compare `status.code()` with `GrpcCode` and not with
    `tonic::Code`. For example, replace `tonic::Code::Unauthenticated` with
    `GrpcCode::Unauthenticated`.

  - `TlsConfig::configure_endpoint(&self, Endpoint) -> ZerobusResult<Endpoint>`
    is now `TlsConfig::tls_settings(&self) -> ZerobusResult<TlsSettings>`.
    `TlsSettings` describes the TLS setup of the channel: the system CA
    certificates (the default), more CA certificates, a client identity for
    mutual TLS, a domain-name override, and plaintext for tests. The SDK
    applies the settings to the endpoint and calls `tls_settings()` for every
    new channel, so an implementation can still rotate certificates.
    `SecureTlsConfig` and `NoTlsConfig` keep their behavior.

    Migration: return a `TlsSettings` from `tls_settings()`. For example,
    replace `ClientTlsConfig::new().ca_certificate(Certificate::from_pem(pem))`
    with `TlsSettings::new().without_native_roots().with_ca_certificate_pem(pem)`.
    `ClientTlsConfig::new()` trusts no system CA certificates, and
    `TlsSettings::new()` does, so `without_native_roots()` keeps the old
    behavior. A `TlsConfig` can no longer change settings of the gRPC endpoint
    that are not TLS settings.
    You no longer need a `tonic` dependency to implement `TlsConfig`.

    The SDK now checks the TLS settings when it applies them. With
    `without_native_roots()`, it rejects a CA entry that holds no PEM
    certificate, because the channel would otherwise trust no server. It also
    reports a CA or a client identity that it cannot use as
    `ZerobusError::InvalidArgument`, which is not retryable, so recovery does
    not retry the mistake. `FailedToEstablishTlsConnectionError` is still the
    error for other TLS setup failures.

- `schema::UcColumn` and `schema::UcTableSchema` are now `#[non_exhaustive]`,
  so they can no longer be built with struct literals outside the SDK. Use
  `UcColumn::new(name, type_name)` with the `with_type_text`, `with_type_json`,
  `with_nullable`, and `with_position` setters, and
  `UcTableSchema::new(catalog_name, schema_name, name, columns)`. Fields stay
  public for reading, and deserializing from the Unity Catalog API response is
  unchanged. New Unity Catalog fields can now be added in minor releases.
- The generated protobuf/gRPC types are no longer public:
  `databricks_zerobus_ingest_sdk::databricks::zerobus` has been removed
  ([#822](https://github.com/databricks/zerobus-sdk/issues/822)), so later
  additions to the wire protocol aren't breaking changes.
  `StreamConfigurationOptions::record_type` is now the SDK-owned `RecordType`
  (`Proto`, `Json`, and `Avro` with the `avro` feature), which is
  `#[non_exhaustive]` and has no `Unspecified` variant. Import
  `databricks_zerobus_ingest_sdk::RecordType` instead. The stream builder API
  and the other-language SDKs are unaffected.

### Deprecations

### API Changes

- Added `GrpcStatus` and `GrpcCode`. Use them to inspect
  `ZerobusError::CreateStreamError` and `ZerobusError::StreamClosedError`.
- Added `TlsSettings`. It implements `TlsConfig`, so you can pass it to
  `ZerobusSdkBuilder::tls_config` in an `Arc`. Its `Debug` output does not print
  key or certificate material.
- Added `UcColumn::new`, `UcColumn::with_type_text`, `UcColumn::with_type_json`,
  `UcColumn::with_nullable`, `UcColumn::with_position`, and
  `UcTableSchema::new`.
- Added `RecordType`.
