# NEXT CHANGELOG

## Release v1.11.0

### Major Changes

### New Features and Improvements

- Arrow Flight streams now compress IPC payloads with ZSTD by default.
  `ArrowStreamConfigurationOptions.ipc_compression` defaults to
  `IPCCompression.ZSTD` instead of `IPCCompression.NONE`. To keep sending
  uncompressed payloads, pass `ipc_compression=IPCCompression.NONE`.
- Stream recovery now waits with exponential backoff and jitter.
  `recovery_backoff_ms` is now the base delay: the delay doubles with each
  consecutive failure, up to 30 seconds. The SDK waits the delay plus a random
  jitter of up to half of the delay, so no wait is shorter than before. A retry
  delay that the server sends with a rejection is the minimum wait, up to 30
  seconds; the jitter does not apply to it. `recovery_retries` still applies to
  each recovery; only the backoff exponent continues across successful
  reconnects. It resets when a connection ends that stayed up for at least 30
  seconds and either received an acknowledgement or had no sent records or
  batches left unacknowledged. JSON and Protocol Buffer streams now also wait
  before the first reconnect after a connection error, and after a server close
  signal on a connection that was not healthy. Stream creation uses the same
  backoff, so with the default options, creating a stream against an
  unavailable server waits 30 to 45 seconds in total before it fails, instead
  of about 8 seconds. With the default options, a recovery that keeps failing
  now waits at least 30 seconds before the stream fails (at least 60 seconds
  for JSON and Protocol Buffer streams after a connection error), instead of
  about 8 seconds.

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
