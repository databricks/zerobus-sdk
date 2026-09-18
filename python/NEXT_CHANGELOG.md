# NEXT CHANGELOG

## Release v1.9.0

### Major Changes

### New Features and Improvements
- Add Avro record format support. Create an Avro stream with `TableProperties(..., avro_schema=json_schema)`; records can be passed as dicts (encoded via fastavro — install the `avro` extra) or pre-encoded bytes (no extra needed).
- `TableProperties` exposes a read-only `record_format` property (`"proto"`, `"json"`, or `"avro"`).

- JSON and protobuf streams now use a dedicated gRPC connection by default.
  Pass `connection_per_stream=False` to the synchronous or asynchronous
  `ZerobusSdk` constructor to share one HTTP/2 connection across streams.

- Added `FederatedToken` for external-IdP (for example Entra ID) authentication.
  Pass `auth=FederatedToken(idp_token_supplier=..., databricks_client_id=...)`
  to `create_stream` and the SDK exchanges the external IdP token for a
  Zerobus-scoped Databricks token (RFC 8693 token exchange), caching and
  refreshing it. Supports account-level federation (omit `databricks_client_id`,
  identity synced via Automatic Identity Management) and workload identity
  federation (set `databricks_client_id` to the service principal, no secret).
  The `idp_token_supplier` callback may be synchronous or asynchronous. A
  transient failure in the callback (it raised) surfaces as a retryable
  `ZerobusException`, matching OAuth mint failures; caller misuse (a non-string
  return, or an async callback on the sync SDK) surfaces as a non-retryable
  `NonRetriableException`. Under account-level federation the shared token cache
  is partitioned by `FederatedToken` identity, so two different identities used
  from one `ZerobusSdk` do not collide and serve each other's token, while
  reusing the same `FederatedToken` across streams keeps its cached token shared
  — no partition key to pass. Existing `client_id`/`client_secret` and
  `headers_provider` calls are unchanged.
- Added an optional `HeadersProvider.invalidate()` hook (a no-op on the base class)
  and forwarded it through the Python bridge, so a custom provider can drop cached
  auth state when the server rejects a token.

### Bug Fixes
- Records are now coerced strictly by the stream's declared format instead of being inferred from each payload's type. A payload that doesn't match the stream — e.g. a `str` on a proto stream — now raises `TypeError` rather than being silently mis-encoded.

### Documentation

### Internal Changes

### Breaking Changes
- `StreamConfigurationOptions.record_type` was previously ignored; it is now validated against the format inferred from `TableProperties`. An explicit `record_type` that disagrees — e.g. `RecordType.PROTO` on a table with no descriptor (a JSON stream), or `RecordType.JSON` on a descriptor table — now raises `ValueError` at stream creation. Migration: omit `record_type` (it defaults to `RecordType.UNSPECIFIED`), or set it to match `TableProperties.record_format`.

### Deprecations

### API Changes
- `RecordType` gains `UNSPECIFIED` (the default for `StreamConfigurationOptions.record_type`) and `AVRO`. When set to a value other than `UNSPECIFIED`, `record_type` is validated against the stream's schema and must agree with it.
