# NEXT CHANGELOG

## Release v0.3.0

### New Features and Improvements


### Deprecations


### Bug Fixes
**IMPORTANT**: Fixed memory safety issue where Go garbage collector could move data while Rust FFI was reading it, causing crashes          
    - Implemented proper memory pinning using `runtime.Pinner` in all FFI functions that pass Go slices to Rust
    - Updated `streamIngestProtoRecords`, `streamIngestProtoRecord`, `streamIngestJSONRecords`, `sdkCreateStream`, and
  `sdkCreateStreamWithHeadersProvider`
    - Uses `unsafe.SliceData()` for safe pointer conversion (requires Go 1.20+)
    - Pins data before passing to Rust, ensuring pointers remain valid during FFI calls

### Documentation


### Internal Changes


### API Changes

