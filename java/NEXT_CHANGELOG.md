# NEXT CHANGELOG

## Release v1.9.0

### Major Changes

### New Features and Improvements

- Added `getStats()` to the stream classes (`ZerobusProtoStream`, `ZerobusJsonStream`, `ZerobusAvroStream`, `ZerobusArrowStream` and the deprecated `ZerobusStream`). It returns an immutable `StreamStats` snapshot with the records in flight, the last acknowledged offset, the number of successful reconnects, and the most recent error. It reads in-memory counters and does not wait for ingest or acknowledgment work. See the Stream Statistics section of the README.

### Bug Fixes

### Documentation

- Documented the GCP workspace URL and Zerobus endpoint formats. The formats for
  AWS, Azure, and GCP now live in one [connection URL
  table](https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id),
  and this SDK's README, examples, proto generator docs, and API docs link to it
  instead of repeating cloud-specific URLs.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
