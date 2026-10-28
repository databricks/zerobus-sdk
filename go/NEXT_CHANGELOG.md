# NEXT CHANGELOG

## Release v1.8.0

### New Features and Improvements

- Stream recovery now waits with exponential backoff and jitter.
  `RecoveryBackoffMs` is now the base delay: the delay doubles with each
  consecutive failure, up to 30 seconds. The SDK waits the delay plus a random
  jitter of up to half of the delay, so no wait is shorter than before. A retry
  delay that the server sends with a rejection is the minimum wait, up to 30
  seconds; the jitter does not apply to it. `RecoveryRetries` still applies to
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

### Deprecations

### Bug Fixes

### Documentation

### Internal Changes

### API Changes
