# NEXT CHANGELOG

## Release v3.3.0

### Major Changes

### New Features and Improvements

- Added `stats()` to `ZerobusStream`, `ZerobusArrowStream` and `PersistentStream`. It returns a
  `StreamStats` snapshot of records in flight, the last acknowledged offset, the number of
  successful automatic reconnects, and the most recent error (`LastStreamError`). The accessor
  reads atomic counters and does not wait on the stream's tasks. Multiplexed streams do not
  expose these counters. `stats_reader()` returns a `StreamStatsReader`, a cloneable handle that
  reads the same counters without borrowing the stream, for callers that cannot hold the stream.

### Bug Fixes

### Documentation

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
