# NEXT CHANGELOG

## Release v1.11.0

### Major Changes

### New Features and Improvements

- Arrow Flight streams now compress IPC payloads with ZSTD by default.
  `ArrowStreamConfigurationOptions.ipc_compression` defaults to
  `IPCCompression.ZSTD` instead of `IPCCompression.NONE`. To keep sending
  uncompressed payloads, pass `ipc_compression=IPCCompression.NONE`.

### Bug Fixes

- Updated the native extension's transitive `rustls-webpki` to 0.103.16 and
  `webpki-roots` to 1.0.11, which stop trusting the compromised eMudhra (emSign)
  CAs.
- The package now requires `certifi>=2026.10.19`. `generate_proto` sends OAuth
  client credentials through `requests`, which trusts the installed `certifi`
  bundle. This version drops the emSign ECC Root CA - G3 root.

### Documentation

### Internal Changes

- Updated `certifi` to 2026.10.19 and, on Python 3.10+, `pip` to 26.2.2 in
  `requirements-dev.lock`; both drop the emSign ECC Root CA - G3 root.

### Breaking Changes

### Deprecations

### API Changes
