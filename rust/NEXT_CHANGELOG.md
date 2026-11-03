# NEXT CHANGELOG

## Release v3.2.0

### Major Changes

### New Features and Improvements

- `HeadersProvider` can now send binary gRPC metadata on both gRPC and Arrow Flight
  streams. Header names ending in `-bin` take a standard base64 value, with or
  without padding. The SDK decodes it and sends the bytes as sensitive binary
  metadata. Previously, these headers failed to send.
- Added a unary ingestion client for sparse traffic. Each
  `UnaryClient::ingest_records()` call sends one batch in one `InsertRecords`
  request and returns once the batch is durable, without opening a stream.
  Build one with `ZerobusSdk::unary_client_builder()`. JSON only: `.json()`
  sends the batch as a JSON array, `.json_lines()` as JSON Lines. The client
  checks that every record is exactly one JSON object and names the index of
  a bad record, enforces `max_ingest_payload_bytes` on the whole
  request body, retries retryable errors per the recovery settings, and bounds
  each attempt by `server_lack_of_ack_timeout_ms`. All unary clients of one SDK
  share one connection, separate from streams and unaffected by
  `connection_per_stream`. It sends HTTP/2 keepalive pings while a call is in
  flight, so a connection that dropped silently between calls is replaced.
  For continuous ingestion, keep using streams.

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
- Documented the unary client in the Rust README, with a usage section, API
  reference entries, and a runnable example (`examples/json/unary.rs`).

### Internal Changes

- Moved the table, authentication, record format, and shared retry settings of
  `StreamBuilder` into a private builder core that other client builders can
  reuse. Stream behavior is unchanged.
- Added the `InsertRecords` RPC to the SDK's copy of the service proto and
  regenerated the purego and purec bindings.
- Added integration tests for the unary client against the mock server, which
  now scripts `InsertRecords` replies and records each call. They cover the
  request body and headers, client-side validation, retries and their backoff,
  authentication and headers provider failures, timeouts, connection sharing,
  reconnects, HTTP/2 keepalive through a black-holing proxy, and concurrent
  calls from many tasks and OS threads.

### Breaking Changes

### Deprecations

### API Changes

- Added `ZerobusSdk::unary_client_builder()`, `UnaryClientBuilder`, and
  `UnaryClient`.
- Added `ZerobusError::UnaryIngestError(GrpcStatus)`, returned when an
  `InsertRecords` call fails with a gRPC status. `ZerobusError` is
  `#[non_exhaustive]`, so this is not a breaking change.
