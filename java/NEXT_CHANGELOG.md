# NEXT CHANGELOG

## Release v1.9.0

### Major Changes

### New Features and Improvements

### Bug Fixes

- Fix `AckCallback` notifications being silently dropped after a callback throws. An exception from `onAck` or `onError` was left pending on the SDK thread that ran the callback, so every later callback on that thread, including callbacks of other streams, failed without reaching Java. The exception is now cleared and logged at ERROR level through SLF4J with its stack trace, and later callbacks are delivered.

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
