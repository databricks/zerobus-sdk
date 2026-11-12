# NEXT CHANGELOG

## Release v3.3.0

### Major Changes

### New Features and Improvements

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
