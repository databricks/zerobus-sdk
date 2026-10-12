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

- `tonic` types are no longer part of the public API, so future `tonic`
  upgrades aren't breaking changes. `ZerobusError::CreateStreamError` and
  `ZerobusError::StreamClosedError` now carry the SDK-owned `GrpcStatus`
  instead of `tonic::Status`; compare `status.code()` against `GrpcCode`
  instead of `tonic::Code`. Error messages are unchanged.
- `TlsConfig::configure_endpoint(&self, tonic::transport::Endpoint)` is
  replaced by `TlsConfig::tls_settings(&self) -> ZerobusResult<TlsSettings>`.
  `SecureTlsConfig` and `NoTlsConfig` work as before. Custom implementations
  should return a `TlsSettings`, or pass a `TlsSettings` directly since it
  implements `TlsConfig`; it supports extra CA certificates, a client identity
  for mutual TLS, and a domain-name override. Custom implementations can no
  longer change non-TLS settings on the gRPC endpoint.

### Deprecations

### API Changes

- Added `GrpcStatus`, `GrpcCode`, and `TlsSettings`.
