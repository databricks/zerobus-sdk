# NEXT CHANGELOG

## Release v1.9.0

### Major Changes

### New Features and Improvements

- Add `ZerobusSdk.builder()` returning a `ZerobusSdkBuilder`, which sets the endpoints, application
  name, connection mode, and TLS options. The existing `ZerobusSdk` constructors still work and
  keep their defaults.
- Add `TlsOptions` to configure TLS: trust only the CA certificates you add (`trustSystemRoots(false)`),
  add CA certificates as PEM text or as files, present a client certificate for mutual TLS as PEM
  text or as files, and override the server domain name. Certificate files are read on each new
  connection, so rotated files take effect without a restart. Previously the SDK always used the
  operating system's CA certificates.
- `HeadersProvider` header names ending in `-bin` are now accepted. The value must be the standard
  base64 encoding of the bytes, and the SDK sends the decoded bytes as binary gRPC metadata.

### Bug Fixes

### Documentation

- Documented the GCP workspace URL and Zerobus endpoint formats. The formats for
  AWS, Azure, and GCP now live in one [connection URL
  table](https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id),
  and this SDK's README, examples, proto generator docs, and API docs link to it
  instead of repeating cloud-specific URLs.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

- New public API: `ZerobusSdk.builder()` returning `ZerobusSdkBuilder`; `TlsOptions` and
  `TlsOptions.Builder` (created with `TlsOptions.builder()`). `ZerobusSdkBuilder.tlsOptions(...)` takes
  a `TlsOptions`.
