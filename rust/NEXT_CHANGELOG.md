# NEXT CHANGELOG

## Release v3.3.0

### Major Changes

### New Features and Improvements

- Added personal access token (PAT) authentication (Private Preview) behind the new
  opt-in `pat-auth` feature. PAT authentication for Zerobus requires server-side
  enablement and is not available for Arrow Flight streams yet. Call
  `.personal_access_token(token)` on `StreamBuilder` or `PersistentStreamBuilder` to
  authenticate with a Databricks PAT created with the `zerobus` API scope. The SDK
  sends the token as given and never refreshes it, and never includes it in `Debug`
  output. An authentication rejection while creating the stream fails immediately
  instead of spending the one-shot credential-refresh retry.
- Added a unary ingestion client for sparse traffic. Each
  `UnaryClient::ingest_records()` call sends one batch in one `InsertRecords`
  request and returns once the batch is durable, without opening a stream. Build
  one with `ZerobusSdk::unary_client_builder()`. JSON only: `.json()` sends the
  batch as a JSON array. The client checks that every record is exactly one JSON
  object and names the index of a bad record, enforces `max_ingest_payload_bytes`
  on the whole request body, retries retryable errors per the recovery settings,
  and bounds each attempt by `server_lack_of_ack_timeout_ms`. All unary clients of
  one SDK share one connection, separate from streams and unaffected by
  `connection_per_stream`. It sends HTTP/2 keepalive pings while a call is in
  flight, so a connection that dropped silently between calls is replaced. Like
  streams, it picks up rotated TLS certificates on the next call. For continuous
  ingestion, keep using streams.

### Bug Fixes

### Documentation

- Documented the unary client in the Rust README, with a usage section, API
  reference entries, and a runnable example (`examples/json/unary.rs`).

### Internal Changes

- Added the `InsertRecords` RPC to the service proto.
- The SDK build script now regenerates the gRPC code when
  `zerobus_service.proto` changes in builds with the `zeroparser` feature
  (such as `cargo test --workspace` and `--all-features` builds), and the test
  mock server's build script regenerates its code on any change to that proto.
  Both used to keep the stale generated code until the next clean build.

### Breaking Changes

### Deprecations

### API Changes

- Added `StreamBuilder::personal_access_token` and
  `PersistentStreamBuilder::personal_access_token` (behind `pat-auth`; the persistent
  builder also needs `eos`). An empty token is rejected with
  `ZerobusError::InvalidArgument`; as with the other authentication setters, the last
  call wins (Private Preview).
- Added `PatHeadersProvider` (behind `pat-auth`), a `HeadersProvider` that sends a
  personal access token (Private Preview).
- Added `HeadersProvider::can_refresh`, a defaulted method returning `true`. A provider
  with a fixed credential returns `false` to skip the one-shot auth retry during stream
  creation or a unary call; `PatHeadersProvider` does. Existing implementations are
  unaffected.
- Added `ZerobusSdk::unary_client_builder()`, `UnaryClientBuilder`, and
  `UnaryClient`.
- Added `ZerobusError::UnaryIngestError(GrpcStatus)`, returned when an
  `InsertRecords` call fails with a gRPC status. `ZerobusError` is
  `#[non_exhaustive]`, so this is not a breaking change.

