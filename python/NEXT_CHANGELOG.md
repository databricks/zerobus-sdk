# NEXT CHANGELOG

## Release v1.12.0

### Major Changes

### New Features and Improvements

- Added `ZerobusSdk.stream_builder()` to the sync and async SDKs, mirroring the Rust SDK's
  `stream_builder()`. Set the table, authentication (OAuth or a custom headers provider),
  record format (`json()`, `compiled_proto(...)`, or `avro(...)`), and stream settings, then
  call `build()` for a `ZerobusStream`. `create_stream()` is unchanged; use it for
  external-IdP federation (`auth=`), which the builder does not support yet.
- Added multiplexed streams (Beta). Call `multiplexed(n)` (1 to 64) on the stream builder,
  optionally `ack_callback(...)`, then `build()`. A multiplexed stream opens `n` sub-streams
  to one table and distributes records round-robin across them, to raise throughput when one
  stream is the bottleneck; there is no global order across sub-streams. `ingest_record()`
  and `ingest_records()` return an opaque `int` message ID (not ordered, may be negative) for
  `wait_for_message_id()`; `flush()` confirms every sub-stream. The `max_inflight_requests`
  budget is divided across sub-streams and must be at least `n`. The `AckCallback` set with
  `multiplexed(n).ack_callback(...)` receives message IDs; `ack_callback()` on the stream
  builder itself is rejected for multiplexed streams.

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

- The persistent stream builder and the new stream builder share their common setters through
  a new internal base class. The persistent builder's behavior and error messages are
  unchanged.

### Breaking Changes

### Deprecations

### API Changes

- Added `StreamBuilder`, `MultiplexedStreamBuilder`, and `MultiplexedStream` to
  `zerobus.sdk.sync` and `zerobus.sdk.aio`, and `ZerobusSdk.stream_builder()`.
  `StreamBuilder.multiplexed(n)` returns a `MultiplexedStreamBuilder`. `MultiplexedStream` has
  `ingest_record()`, `ingest_records()`, `wait_for_message_id()`, `flush()`, `close()`,
  `get_unacked_records()`, `get_unacked_batches()`, and the property `is_closed`.
