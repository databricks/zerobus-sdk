# NEXT CHANGELOG

## Release v0.2.0

### New Features and Improvements

- Give each PureGo stream a dedicated gRPC connection by default, matching the
  other SDKs. Pass `WithConnectionPerStream(false)` to keep the previous shared
  connection behavior for many low-throughput streams.
- Tear the connection down gracefully when the server requests a stream pause
  (`CloseStreamSignal`), as a clean `Close` already did: the client half-closes
  the request stream and drains remaining acknowledgments before reconnecting,
  so the server observes an orderly `END_STREAM` instead of an abrupt cancel. A
  request still being sent is allowed to finish, so an acknowledgment already
  received for it makes the record durable instead of replaying the record on
  the new connection. Teardown stays bounded by the drain budget.
- `ColumnsFromDescriptor` returns the set of column names a serialized protobuf
  descriptor declares, so callers can inspect a table's columns without parsing
  the descriptor themselves. It accepts the bytes returned by
  `FetchProtoDescriptorFromUC` or passed to `WithProto`.

- Behavior change (no API change): stream creation and reconnect retries now back
  off exponentially with jitter instead of waiting a fixed `WithRecoveryBackoff`
  interval, matching the Rust core and the SDKs built on it. The wait starts at the
  configured backoff and is never shorter than it. It doubles after each consecutive
  failed attempt, up to 15 s (or the configured backoff if larger), and a random
  jitter of up to the same amount is added, so a single wait can reach 30 s. With
  the defaults (2 s, 4 retries), retries wait 2–4 s, 4–8 s, 8–16 s, then 15–30 s, so
  a stream that keeps failing to open gives up after about 29–58 s of waiting
  instead of 8 s. Streams rejected at the same time, for example by the server's
  stream-creation rate limit, no longer all retry at the same moment.

  Behavior change: once an established stream is lost, recovery now makes up to
  `WithRecoveryRetries` + 1 reconnect attempts instead of `WithRecoveryRetries`, as
  the Rust core's gRPC streams do: losing the stream no longer spends a retry. With
  the defaults, a stream whose reconnects keep failing gives up after 5 attempts and
  about 44–88 s of waiting, instead of 4 attempts and 8 s. A failed first open is
  still retried `WithRecoveryRetries` times.

  To keep a total retry window close to the previous one, lower
  `WithRecoveryRetries`. With the default 2 s base, 2 retries wait 6–12 s in total
  when the first open fails, and 14–28 s during recovery.

### Bug Fixes

### Documentation

- Flush recovery no longer treats every flush error as terminal. The JSON single
  example retrieves unacknowledged records on flush failure before teardown and
  replays them on a fresh stream. The JSON batch example demonstrates that a
  batch produces a single ack callback event and waits for that callback before
  exit.
- Corrected the README's dynamic JSON rules: a payload carrying a field the
  descriptor does not declare fails conversion; unknown fields are not ignored.
- Documented the GCP workspace URL and Zerobus endpoint formats. The formats for
  AWS, Azure, and GCP now live in one [connection URL
  table](https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id),
  and this SDK's README, examples, and API docs link to it instead of repeating
  cloud-specific URLs.

### Internal Changes

- Generalize the stream core's durability model so one implementation serves both
  the atomic proto and JSON protocols and the record-count protocol the Arrow
  Flight path needs: acknowledgments are tracked as cumulative durability units,
  a partially acknowledged item replays only its unacknowledged suffix,
  submission receipts report how much of a multi-frame send reached the server,
  and encoder, ack-model, and opener seams let a protocol instantiate the core
  over its own payload type. Proto and JSON behavior is unchanged, and nothing is
  exposed through a public API yet.
- Add `internal/arrowproto`, the Arrow IPC payload for the upcoming Arrow
  ingestion path. A payload is a canonical self-contained IPC stream materialized
  when it is built, so the core can hold it across a reconnect without pinning the
  caller's `RecordBatch`, and it can be sliced by row so a partially acknowledged
  batch replays only its unacknowledged suffix. Admission sizes a batch from the
  rows it covers rather than the whole buffers it points at, because a slice
  shares its parent's buffers and would otherwise be charged for the entire
  parent. Nested children are sized from the same row window, since slicing
  rebases only the top-level node and leaves a struct's fields and a list's
  values spanning the whole parent, and a batch's custom metadata is charged
  separately because it is written into the IPC message header rather than into
  any column buffer. Nothing is exposed through a public API yet.
- Add the `github.com/apache/arrow-go/v18` dependency. arrow-go requires
  `google.golang.org/grpc` v1.82.0, which raises this module's grpc minimum from
  v1.81.1.

### Breaking Changes

### Deprecations

### API Changes
