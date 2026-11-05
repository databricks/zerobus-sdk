# NEXT CHANGELOG

## Release v1.9.0

### New Features and Improvements

- `GetToken` returns the table-scoped OAuth token the SDK uses for its streams,
  with its expiry, to authenticate Zerobus' other [API
  protocols](https://docs.databricks.com/aws/en/ingestion/zerobus-api-protocols)
  (REST, OpenTelemetry, MQTT, Kafka). Every call mints a new token.

### Deprecations

### Bug Fixes

### Documentation

- Documented the GCP workspace URL and Zerobus endpoint formats. The formats for
  AWS, Azure, and GCP now live in one [connection URL
  table](https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id),
  and this SDK's README, examples, and API docs link to it instead of repeating
  cloud-specific URLs.

### Internal Changes

### API Changes

- Add `GetToken(workspaceURL, tableName, clientID, clientSecret, workspaceID)` and
  the `OAuthToken` type (`AccessToken`, `ExpiresAt`), whose `String` and `GoString`
  redact the access token.
