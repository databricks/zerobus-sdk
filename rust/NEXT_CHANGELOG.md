# NEXT CHANGELOG

## Release v3.2.0

### Major Changes

### New Features and Improvements

- `DefaultTokenFactory::get_token_with_expiry` returns the table-scoped OAuth token
  the SDK uses for its streams, with its expiry, to authenticate Zerobus' other [API
  protocols](https://docs.databricks.com/aws/en/ingestion/zerobus-api-protocols)
  (REST, OpenTelemetry, MQTT, Kafka). Every call mints a new token.

### Bug Fixes

- `DefaultTokenFactory::get_token` now fails with a retryable `TokenFetchError` if the
  token endpoint doesn't respond within 30 seconds; it could previously wait forever.
  Stream creation is unaffected, since its recovery timeout already bounds the mint.

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

- Add `DefaultTokenFactory::get_token_with_expiry` and the `OAuthToken` struct
  (`access_token`, `expires_at`), whose `Debug` output redacts the access token.
