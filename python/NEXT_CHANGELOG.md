# NEXT CHANGELOG

## Release v1.12.0

### Major Changes

### New Features and Improvements

- `get_token()` returns the table-scoped OAuth token the SDK uses for its streams,
  with its expiry, to authenticate Zerobus' other [API
  protocols](https://docs.databricks.com/aws/en/ingestion/zerobus-api-protocols)
  (REST, OpenTelemetry, MQTT, Kafka). Every call mints a new token.

### Bug Fixes

### Documentation

- Documented the GCP workspace URL and Zerobus endpoint formats. The formats for
  AWS, Azure, and GCP now live in one [connection URL
  table](https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id),
  and this SDK's README, examples, proto generator, and API docs link to it instead
  of repeating cloud-specific URLs.
- The examples exit with an error naming the missing environment variable instead of
  falling back to placeholder values.

### Internal Changes

### Breaking Changes

### Deprecations

### API Changes

- Add `get_token(workspace_url, table_name, client_id, client_secret, workspace_id)`
  to `zerobus` and `zerobus.sdk.sync`, its async counterpart `zerobus.sdk.aio.get_token`,
  and the `OAuthToken` dataclass (`access_token`, `expires_at`), whose `repr` omits
  the access token.
