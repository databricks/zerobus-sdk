# NEXT CHANGELOG

## Release v3.3.0

### Major Changes

### New Features and Improvements

- Added personal access token (PAT) authentication (Private Preview) behind the new
  opt-in `pat-auth` feature. PAT authentication for Zerobus requires server-side
  enablement and is not available for Arrow Flight streams yet. Call
  `.personal_access_token(token)` on `StreamBuilder` or `PersistentStreamBuilder` to
  authenticate with a Databricks PAT created with the `zerobus` API scope. The SDK
  sends the token as given and never refreshes it, and never includes it in `Debug`
  output. An authentication rejection while creating the stream fails immediately
  instead of spending the one-shot credential-refresh retry.

### Bug Fixes

- The SDK now reads TLS certificate files on a blocking thread instead of an
  async worker, and a gRPC channel source reads them before it takes the lock
  that streams sharing it wait on. A slow certificate volume used to stall
  other tasks on the worker and hold up stream creation and recovery for every
  stream on a shared channel.
- When the TLS settings of a gRPC channel change, the SDK now keeps the channel
  it replaced until a call on the new channel reaches the server, and goes back
  to it if the new channel fails first, for example because the server rejects
  a rotated client certificate or a rotated CA doesn't trust the server. It
  tries the new channel again when the settings change again or when the
  replaced channel fails too. The new channel used to replace a working one
  right away, so every later stream and recovery attempt on that source failed
  until the settings changed again.

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

- Added `StreamBuilder::personal_access_token` and
  `PersistentStreamBuilder::personal_access_token` (behind `pat-auth`; the persistent
  builder also needs `eos`). An empty token is rejected with
  `ZerobusError::InvalidArgument`; as with the other authentication setters, the last
  call wins (Private Preview).
- Added `PatHeadersProvider` (behind `pat-auth`), a `HeadersProvider` that sends a
  personal access token (Private Preview).
- Added `HeadersProvider::can_refresh`, a defaulted method returning `true`. A provider
  with a fixed credential returns `false` to skip the one-shot auth retry during stream
  creation; `PatHeadersProvider` does. Existing implementations are unaffected.
