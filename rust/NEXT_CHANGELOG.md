# NEXT CHANGELOG

## Release v3.2.0

### Major Changes

### New Features and Improvements

- Added `ZerobusSdkBuilder::endpoint_override` (Arrow Flight): dial an alternate address while keeping the endpoint as `:authority` and TLS server name.

### Bug Fixes

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
