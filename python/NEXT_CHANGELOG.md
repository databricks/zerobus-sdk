# NEXT CHANGELOG

## Release v1.12.0

### Major Changes

### New Features and Improvements

- Added personal access token (PAT) authentication (Private Preview). PAT
  authentication for Zerobus requires server-side enablement.
  Pass `auth=PersonalAccessToken(token)` to `create_stream()` (sync and async) to
  authenticate with a Databricks PAT created with the `zerobus` API scope. The token stays in the Rust
  core, so no Python callback runs per request, and it is never shown in `repr()`. It
  is compiled into the wheel; no extra is needed. See `examples/sync_example_pat.py`.

- Stream creation and recovery retries now back off exponentially with jitter
  instead of waiting a fixed `recovery_backoff_ms` between attempts (from the Rust core).
  The wait still starts at `recovery_backoff_ms` and is never shorter than it. It
  doubles after each consecutive failed attempt, up to 15 seconds (or
  `recovery_backoff_ms` if larger), and a random jitter of up to the same amount is
  added. With the defaults (2 s, 4 retries), a stream that keeps failing gives
  up after about 29–58 s of waiting instead of 8 s. Clients rejected at the same
  time, for example by the server's stream-creation rate limit, no longer all
  retry at the same moment. No API change; a backoff of 0 still retries
  immediately.

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

- Added `PersonalAccessToken`, exported from `zerobus` and `zerobus.sdk.shared`. It
  raises `ValueError` for an empty token (Private Preview).
- `create_stream(auth=...)` now accepts a `PersonalAccessToken` as well as a
  `FederatedToken`. As before, `auth=` cannot be combined with
  `client_id`/`client_secret` (Private Preview).
