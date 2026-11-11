# NEXT CHANGELOG

## Release v3.3.0

### Major Changes

### New Features and Improvements

### Bug Fixes
- Fixed gRPC streams failing with `Server ack timeout` although the server
  acknowledged the record in time. The timer restarted on every receiver loop
  iteration, so idle time before a send counted against the next record's
  deadline. `server_lack_of_ack_timeout_ms` now counts from when the oldest
  unacknowledged record was sent, or from the last server message if that is
  later. Idle time with nothing in flight no longer counts. Arrow Flight streams
  already worked this way.

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
