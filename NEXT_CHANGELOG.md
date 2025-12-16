# NEXT CHANGELOG

## Release v0.2.1

### New Features and Improvements

### Bug Fixes

### Documentation

### Internal Changes

- Improved graceful close mechanism: when server signals stream closure, SDK now continues processing acknowledgments for in-flight records while pausing new record transmission until timeout.

### API Changes

- [**BREAKING**] Added `stream_paused_max_wait_time_ms` to `StreamConfigurationOptions` to configure maximum wait time during graceful stream close (`None` = wait for full server duration, `Some(0)` = immediate recovery, `Some(x)` = wait up to min(x, server_duration) milliseconds)