# NEXT CHANGELOG

## Release v1.9.0

### Major Changes

### New Features and Improvements

- `ZerobusSdk.getToken()` returns the table-scoped OAuth token the SDK uses for its
  streams, with its expiry, to authenticate Zerobus' other [API
  protocols](https://docs.databricks.com/aws/en/ingestion/zerobus-api-protocols)
  (REST, OpenTelemetry, MQTT, Kafka). Every call mints a new token.

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

- Add static `ZerobusSdk.getToken(String workspaceUrl, String tableName, String clientId,
  String clientSecret, String workspaceId)` and the `OAuthToken` class
  (`getAccessToken()`, `getExpiresAt()`), whose `toString()` redacts the access token.
