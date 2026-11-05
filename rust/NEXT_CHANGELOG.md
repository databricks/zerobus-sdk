# NEXT CHANGELOG

## Release v3.2.0

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

- Documented the GCP workspace URL and Zerobus endpoint formats. The formats for
  AWS, Azure, and GCP now live in one [connection URL
  table](https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id),
  and this SDK's README, examples, schema generator, and API docs link to it instead
  of repeating cloud-specific URLs.
- The examples read their connection settings from the `ZEROBUS_SERVER_ENDPOINT`,
  `DATABRICKS_WORKSPACE_URL`, `ZEROBUS_TABLE_NAME`, `DATABRICKS_CLIENT_ID`, and
  `DATABRICKS_CLIENT_SECRET` environment variables instead of constants in the
  source.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes
