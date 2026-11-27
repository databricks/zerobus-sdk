# NEXT CHANGELOG

## Release v1.12.0

### Major Changes

### New Features and Improvements

- Added personal access token (PAT) authentication (Private Preview). PAT
  authentication for Zerobus requires server-side enablement.
  Pass `auth=PersonalAccessToken(token)` to `create_stream()` (sync and async), or call
  `personal_access_token(token)` on the persistent stream builder, to authenticate with
  a Databricks PAT created with the `zerobus` API scope. The token stays in the Rust
  core, so no Python callback runs per request, and it is never shown in `repr()`. It
  is compiled into the wheel; no extra is needed. See `examples/sync_example_pat.py`.
- Added multiplexed streams (Beta). `create_multiplexed_stream(table_properties,
  stream_count, *, auth=, options=)` on the sync and async `ZerobusSdk` opens
  `stream_count` (1 to 64) sub-streams to one table and distributes records round-robin
  across them, to raise throughput when one stream is the bottleneck; there is no global
  order across sub-streams. `auth=` takes `OAuth`, a headers provider, a
  `FederatedToken`, or a `PersonalAccessToken`. Ingest calls return an opaque `int` message
  ID (not ordered, may be negative) for `wait_for_message_id()`, and `flush()` confirms
  every sub-stream. The `max_inflight_records` budget is divided across sub-streams and must
  be at least `stream_count`. Sync multiplexed streams are thread-safe. See
  `examples/sync_example_multiplexed.py` and `examples/async_example_multiplexed.py`.

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
- Added `PersistentStreamBuilder.personal_access_token(token)`, taking the token string or
  a `PersonalAccessToken`. It replaces an earlier `oauth()` or `headers_provider()` call,
  and they replace it (Private Preview).
- Added `ZerobusSdk.create_multiplexed_stream()`, `MultiplexedStream` (in `zerobus.sdk.sync`
  and `zerobus.sdk.aio`), `MultiplexedStreamOptions` (a `StreamConfigurationOptions` whose
  `ack_callback` receives message IDs), and `OAuth`, exported from `zerobus` and
  `zerobus.sdk.shared` (Beta). A plain `StreamConfigurationOptions` with `ack_callback` set
  raises `ValueError` for a multiplexed stream.
- `StreamConfigurationOptions` can now be subclassed in Python; a subclass with its own
  `__init__` passes the options on with `super().__init__(**kwargs)`.
