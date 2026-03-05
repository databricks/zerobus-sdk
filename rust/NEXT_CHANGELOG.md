# NEXT CHANGELOG

## Release v1.0.0

### New Features and Improvements
- Added HTTP proxy support via standard environment variables (`grpc_proxy`, `https_proxy`, `http_proxy`), following gRPC core conventions. Proxied connections use HTTP CONNECT tunneling with end-to-end TLS. Supports `no_grpc_proxy` / `no_proxy` for bypass rules.

### Deprecations

### Bug Fixes
- Fixed a rare race condition in `wait_for_offset_internal` where the actual server error (e.g., `InvalidArgument`) was lost and replaced by a generic `StreamClosedError`. This occurred when `error_rx.changed()` fired but `is_closed` had not yet been set by the supervisor, causing the error to be missed on the next loop iteration.

### Documentation

### Internal Changes

### API Changes
