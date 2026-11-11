# NEXT CHANGELOG

## Release v3.3.0

### Major Changes

### New Features and Improvements

### Bug Fixes

- A `TlsSettings::with_domain_name` value that isn't a DNS name or an IP
  address, such as one with a scheme or a port, now makes stream creation fail
  at once with a non-retryable `ZerobusError::InvalidArgument` that names the
  value. It used to give a retryable `FailedToEstablishTlsConnectionError` with
  no reason, or a retryable `ChannelCreationError` when the settings also loaded
  files, so stream creation retried it until the recovery retries ran out.

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
