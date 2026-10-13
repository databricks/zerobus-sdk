# Rust defaults mapped to the C POC

This comparison uses the checked-out Rust JSON/protobuf gRPC implementation, not
Arrow Flight. The dependency versions in `rust/Cargo.lock` are tonic 0.14.5,
hyper 1.8.1, h2 0.4.13, and Tokio 1.52.2. The active
[`bench-config.json`](bench-config.json) uses the closest available config-only
mapping. It is **not an identical implementation or a measured winning preset**.

No SDK code, threads, reconnection, or token-cache behavior was changed. Host,
table, input, warmup, measurement duration, and latency sampling were preserved.

## Configuration mapping

| Aspect | Rust JSON/protobuf path | C benchmark preset | Exact match? |
|---|---|---|---|
| Single-record ingestion | One request per `ingest_record_offset`; sender does not merge calls into protocol batches | `batch: single` | Yes, at the request-boundary level. Tonic still coalesces encoded messages for transport. |
| Payload lifetime | Retain unacknowledged payloads in landing zone until durable ack | `retention: acked` | Same release event; C does not replay them. |
| Request limit | 1,000,000 **combined queued and sent-but-unacked ingest calls**, enforced by one semaphore | `max-inflight: 1000000`, `queue-capacity: 1048576` | Approximation: C bounds pending admission and outstanding wire requests independently. Its ring requires a power of two. |
| Outbound request channel | Separate bounded Tokio MPSC channel, capacity 2,048 | No corresponding intermediate channel | Not configurable in C. Setting the C admission ring to 2,048 would not reproduce the two-stage Rust pipeline. |
| Queue synchronization | Mutex-protected `VecDeque`s, Tokio semaphore and notifications | `queue: mutex` | Similar synchronization choice, different implementation and admission accounting. |
| Byte budget | No separate aggregate payload-byte budget on this path | Existing `buffer-bytes: 1073741824` (1 GiB) retained | C-only safety bound, not a Rust default. |
| Per-call payload cap | 10 MiB minus 64 KiB = 10,420,224 record bytes | `batch-bytes: 10420224` | Conservative approximation: C caps encoded message bytes including envelope, Rust caps the sum of record bytes. This is a maximum, not a batching target. |
| Transport coalescing | Tonic yields encoded bytes at 32 KiB or when its source is pending/finished; codec starts at 8 KiB | `write-coalesce-bytes: 32768` | Similar target, different layer: C coalesces HTTP/2 output after framing. |
| Admission wait | Wait for a landing-zone permit or terminal shutdown; no per-call timer | `admission-timeout-ms: 300000` | Deliberate finite POC approximation, **not** a Rust admission default. Removes the previous one-second deadline while keeping a safety bound. |
| Flush | Default timeout 300,000 ms | `drain-timeout: 300` seconds | Same duration for each warmup/measured drain. |
| No-ack watchdog | Default 60,000 ms; can trigger recovery | No equivalent | Admission timeout is not an ack watchdog. |
| Connection layout | One connection per stream by default | One stream, one connection | Matches this single-stream experiment. |
| Execution | Separate sender/receiver async tasks, plus supervisor, scheduled on the caller's Tokio runtime | One pthread I/O owner; `idle: sleep` | Not equivalent. Tokio task count is not the C `io-threads` setting. |
| TCP_NODELAY | Enabled by tonic default | Already enabled by C | No config change needed. |
| Recovery/token management | Recovery and token cache enabled by default | Remain absent | Intentional POC scope. |

`batch-records: 8192` and `batch-delay-us: 0` were preserved; neither causes
aggregation in `single` mode. Latency sampling stays at `sample-every: 1024` for
comparability with earlier C results; this is benchmark instrumentation, not a
Rust default.

Setting the C queue to 1,000,000 would silently round its effective capacity down
to 524,288. The preset instead requests 1,048,576 explicitly. **Do not interpret
the two C limits as a shared million-request cap.** At the current 1 KiB record
size, the retained 1 GiB payload budget independently bounds live payload blocks
to about 1,048,576 records. With 4 KiB records, it bounds them to about 262,144.
The payload budget counts exact reserved bytes, without size-class rounding.
Descriptor arrays, queue entries, allocator overhead, TLS state, and stacks are extra
memory; total RSS can exceed 1 GiB. Rust's request-count budget also allows
substantial memory growth, especially with explicit batches.

## Internal structure

```text
Rust:
caller -> landing zone (queued + observed, shared capacity permit)
       -> sender task -> MPSC(2048) -> tonic encoder -> hyper/h2 -> TLS
reply  -> receiver task -> remove observed requests -> release permits/payloads

C POC:
caller -> bounded admission ring -> I/O owner -> nghttp2 -> OpenSSL/TLS
reply  -> same I/O owner -> advance ack watermark -> release retained payloads
```

Rust's landing zone uses `VecDeque::push_back` / `pop_front`; it does not shift
an array on each pop. Observing an item moves it to the observed queue without
releasing its capacity permit. The receiver releases permits on cumulative
acknowledgments. The outbound MPSC channel limits the sender's lead over tonic,
but is not the public ingestion-capacity limit.

Tonic's encoder can put multiple independently framed gRPC messages in a single
buffer. That is transport coalescing, not an `IngestRecordBatchRequest`. Rust's
explicit `ingest_records_offset` API sends a caller-selected batch. Consequently,
changing the C config to `auto` or treating 32 KiB as its protocol batch size
would not reproduce Rust's single-record default path.

The C owner both sends and reads a connection. More `io-threads` only distributes
**different connections** across owners; it cannot split one connection into
sender and receiver threads. The benchmark requires
`producers >= streams >= connections >= io-threads`. Preserve 1/1/1/1 for this
comparison. Separate sender/receiver execution and a task pool remain separate
future architecture experiments.

## HTTP/2 details that configuration does not copy

The Rust SDK constructs a tonic endpoint without explicit window overrides.
For the locked dependency versions, hyper's defaults are 2 MiB stream and 5 MiB
connection receive windows, 16 KiB maximum inbound frames, a 1 MiB per-stream
send-buffer limit, and adaptive receive windows disabled. The C POC advertises
16 MiB stream and connection receive windows and does not expose those settings
in the benchmark config.

These advertised receive windows govern data arriving **at the client**, mostly
acknowledgments here. They are not the server-advertised send windows that limit
client-to-server ingestion. Copying smaller client receive windows is not a
justified throughput fix. Likewise, hyper's 1 MiB send-buffer limit is not the
same thing as the C `write-coalesce-bytes` threshold.

## What the existing measurements tell us

| Existing result | Protocol batching | Timed ack MB/s | Process CPU cores | Drain seconds |
|---|---|---:|---:|---:|
| `20260923T140219859288761-4022388.json` | Automatic, 1 MiB cap | 8.858750 | 0.019822 | 0.619435 |
| `20260923T150330340799803-28882.json` | Single | 8.747548 | 0.031826 | 3.939172 |

These are separate single observations, not controlled repeated comparisons and
not Rust measurements. Nevertheless, nearly equal throughput with low CPU use
does not suggest a CPU-saturated encoder. The single-record producer spent about
11.89 of its 12 measured seconds waiting for admission, while its queue stayed
near full. This is evidence of downstream backpressure, not proof of whether
the limiting factor is socket/TLS/HTTP2 progress, network, or service behavior.

A larger backlog may only increase memory consumption and end-of-run draining.
At the observed rate, draining roughly 1 GiB would take around two minutes; there
is a drain after warmup as well as after measurement. Compare both timed and
cohort MB/s, plus drain time, rather than interpreting a faster producer loop as
better durable throughput. No live run of this preset has been performed by the
agent.

## Source references

- [Rust stream defaults](../rust/sdk/src/stream_configuration.rs) and
  [shared constants](../rust/sdk/src/stream_options.rs).
- [Landing zone](../rust/sdk/src/landing_zone.rs),
  [admission](../rust/sdk/src/stream/grpc/ingest.rs),
  [sender](../rust/sdk/src/stream/grpc/sender.rs),
  [receiver](../rust/sdk/src/stream/grpc/receiver.rs), and
  [outbound channel](../rust/sdk/src/stream/grpc/connection.rs).
- [SDK connection default](../rust/sdk/src/builder/sdk_builder.rs) and
  [endpoint construction](../rust/sdk/src/sdk.rs).
- Dependency source under Cargo's registry cache: tonic 0.14.5
  `src/codec/{mod,encode}.rs`, `src/transport/channel/endpoint.rs`, and
  hyper 1.8.1 `src/proto/h2/client.rs`.
- [C admission, batching, and transport](../purec-perf/src/sdk.c) and
  [payload byte-budget helpers](../purec-perf/src/pool.c).
