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

- Behavior change (no API change): stream creation and recovery retries now back off
  exponentially with jitter instead of waiting a fixed `recovery_backoff_ms` between
  attempts. The wait starts at `recovery_backoff_ms` and is never shorter than it.
  It doubles after each consecutive failed attempt, up to 15 s (or
  `recovery_backoff_ms` if larger), and a random jitter of up to the same amount is
  added, so a single wait can reach 30 s. With the defaults (2 s, 4 retries),
  retries wait 2–4 s, 4–8 s, 8–16 s, then 15–30 s, so a stream that keeps failing
  gives up after about 29–58 s of waiting instead of 8 s. Recovery of a gRPC stream
  after a failure now also waits before its first reconnect attempt (2–4 s by
  default), so streams the server drops at the same time don't all reconnect at
  once; a gRPC stream that keeps failing during recovery gives up after about
  44–88 s instead of 8 s. A server-initiated Arrow Flight stream rotation still
  waits `recovery_backoff_ms` on average (a random wait between half and 1.5 times
  it), and a gRPC close signal still reconnects immediately. Clients rejected at the
  same time, for example by the server's stream-creation rate limit, no longer all
  retry at the same moment. A backoff of 0 still retries immediately.

  To keep a total retry window close to the previous one, lower `recovery_retries`.
  With the default 2 s base, 2 retries wait 6–12 s in total (14–28 s for gRPC
  recovery after a failure).

### Bug Fixes

### Documentation

### Internal Changes

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
