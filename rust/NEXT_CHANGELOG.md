# NEXT CHANGELOG

## Release v3.2.0

### Major Changes

### New Features and Improvements

- `HeadersProvider` can now send binary gRPC metadata on both gRPC and Arrow Flight
  streams. Header names ending in `-bin` take a standard base64 value, with or
  without padding. The SDK decodes it and sends the bytes as sensitive binary
  metadata. Previously, these headers failed to send.

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

- Moved the table, authentication, record format, and shared retry settings of
  `StreamBuilder` into a private builder core that other client builders can
  reuse. Stream behavior is unchanged.

### Breaking Changes

### Deprecations

### API Changes
