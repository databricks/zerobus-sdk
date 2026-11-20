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

### Documentation

### Internal Changes

### Breaking Changes

- Stream creation and recovery retries now back off exponentially with jitter
  instead of waiting a fixed `recovery_backoff_ms` between attempts. The wait
  still starts at `recovery_backoff_ms` and is never shorter than it. It doubles
  after each consecutive failed attempt, up to 15 seconds (or
  `recovery_backoff_ms` if larger), and a random jitter of up to the same amount
  is added. With the defaults (2 s, 4 retries), retries wait 2–4 s, 4–8 s,
  8–16 s, then 15–30 s, so a stream that keeps failing gives up after about
  29–58 s of waiting instead of 8 s. This covers initial creation on both
  transports, gRPC stream re-creation during recovery (including persistent
  streams), and Arrow Flight reconnects, which includes the reconnect after a
  server-initiated stream rotation. Clients rejected at the same time, for
  example by the server's stream-creation rate limit, no longer all retry at
  the same moment. No API change; `recovery_backoff_ms(0)` still retries
  immediately.
- Multiplexed streams (Beta): when `recovery_retries` is not set, each
  sub-stream now uses twice the default (8 instead of 4), both for its initial
  open and for later recoveries. A single sub-stream that runs out of retries
  fails the whole multiplexed stream, so with the per-stream default a mux
  failed much more often than an ordinary stream, and more often the more
  sub-streams it had. An explicit `recovery_retries(n)` still applies to each
  sub-stream as given.

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
