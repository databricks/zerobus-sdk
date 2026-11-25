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
- Added a unary ingestion client for sparse traffic:
  `ZerobusSdk::unary_client_builder()` builds a `UnaryClient` whose
  `ingest_records()` sends one batch per `InsertRecords` call and returns once it
  is durable. JSON only. Retries are on by default, so a batch can be written
  twice after a timeout; `.recovery(false)` sends each batch at most once.
- Added `stats()` to `ZerobusStream`, `ZerobusArrowStream` and `PersistentStream`. It returns a
  `StreamStats` snapshot of records in flight, the last acknowledged offset, the number of
  successful automatic reconnects, and the most recent error (`LastStreamError`). The accessor
  reads atomic counters and does not wait on the stream's tasks. Multiplexed streams do not
  expose these counters. `stats_reader()` returns a `StreamStatsReader`, a cloneable handle that
  reads the same counters without borrowing the stream, for callers that cannot hold the stream.
  `LastStreamError` carries the typed `ZerobusError` in its `error` field, alongside the message.

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
- Added `ZerobusSdk::unary_client_builder()`, `UnaryClientBuilder`, `UnaryClient`,
  and `UnaryIngestResult`.
- Added `ZerobusError::UnaryIngestError(GrpcStatus)`, returned when an
  `InsertRecords` call fails with a gRPC status. `ZerobusError` is
  `#[non_exhaustive]`, so this is not a breaking change.

