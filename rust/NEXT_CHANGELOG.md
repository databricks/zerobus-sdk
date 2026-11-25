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

### Bug Fixes

- Arrow Flight streams no longer log a warning on every connection when a
  `HeadersProvider` sends `x-databricks-zerobus-table-name` with the stream's own
  table, as the README tells custom providers to do. The warning now appears only
  when the provider's table differs from the stream's. The stream's table is
  still the one sent.

### Documentation

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
  creation; `PatHeadersProvider` does. Existing implementations are unaffected.
