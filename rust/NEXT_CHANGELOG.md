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

### Bug Fixes

### Documentation

- Documented persistent streams (behind the `eos` feature) in the Rust README,
  with a usage section, API reference entries, and a runnable JSON example that
  creates a durable stream, ingests in a loop with one `flush()`, and resumes by
  `stream_id`.

### Internal Changes

- Added a stateful persistent-stream mock and integration coverage for create,
  flush, resume offset continuity, and unknown stream IDs.

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
  release is a breaking change, so a `tonic` upgrade no longer breaks the SDK
  API. Two parts of the API named `tonic` types:

  - `ZerobusError::CreateStreamError` and `ZerobusError::StreamClosedError` now
    carry a `GrpcStatus` instead of a `tonic::Status`. `GrpcStatus::code()`
    returns a `GrpcCode` (`#[non_exhaustive]`, 17 gRPC status codes) and
    `GrpcStatus::message()` returns the message. The `Display` output is the
    same as the output of `tonic::Status`, so error strings do not change.
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

### Deprecations

### API Changes

- Added `GrpcStatus` and `GrpcCode`. Use them to inspect
  `ZerobusError::CreateStreamError` and `ZerobusError::StreamClosedError`.
- Added `TlsSettings`. It implements `TlsConfig`, so you can pass it to
  `ZerobusSdkBuilder::tls_config` in an `Arc`. Its `Debug` output does not print
  key or certificate material.
