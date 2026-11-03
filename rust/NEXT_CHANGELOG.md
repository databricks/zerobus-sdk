# NEXT CHANGELOG

## Release v3.1.0

### Major Changes

### New Features and Improvements
- Added `StreamBuilder::compression` and `ArrowCompression` (`IpcZstd`,
  `IpcLz4`, `GrpcZstd`, or `Uncompressed`) to choose Arrow Flight payload
  compression with one setting. `GrpcZstd` compresses each whole gRPC request
  message, including the IPC header and alignment padding that IPC compression
  leaves raw, which sends fewer bytes for wide or sparse schemas. Its buffers
  are sent without IPC compression and with 8-byte alignment, and it can use
  more client CPU than `IpcZstd`. With it, `BatchStats::approximate_wire_bytes`
  counts bytes after gRPC compression. The default leaves the wire format
  unchanged.
- Added `TlsSettings::with_ca_certificate_file` and
  `TlsSettings::with_client_identity_files` to load CA certificates and the
  mutual TLS client identity from PEM files, so rotated certificates take
  effect without a restart or a custom `TlsConfig`. A missing, unreadable,
  non-regular or oversized file gives a non-retryable
  `ZerobusError::InvalidArgument` with the path. Unusable content read from a
  file, such as a certificate that doesn't match the key, gives a retryable
  `ZerobusError::ChannelCreationError`.
- The SDK now calls `TlsConfig::tls_settings()` before it reuses a gRPC channel
  for a new stream or a recovery attempt, and opens a new channel when the
  settings changed. Rotated certificates now reach a shared channel
  (`connection_per_stream` off) and gRPC recovery, which used to keep the
  certificates the channel was opened with. If the settings can't be read or
  applied at that point, the SDK keeps the current channel and logs a warning.
  A gRPC stream now opens its first channel inside the recovery retry loop, as
  Arrow Flight streams already did, so a retryable error there is retried when
  recovery is enabled.

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

- Deprecated `StreamBuilder::ipc_compression` and the
  `ArrowStreamConfigurationOptions::ipc_compression` field in favor of
  `StreamBuilder::compression` and the `compression` field. They keep working:
  of the two setters, the last call wins, and the deprecated field applies while
  `compression` is `ArrowCompression::FallbackToLegacy`, its default. That
  variant is deprecated too and will be removed with the field.

### API Changes
