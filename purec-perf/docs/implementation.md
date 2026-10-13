# Pure C POC implementation guide

This document is maintained with the implementation. The public boundary is
`include/zbperf.h`; the benchmark is an independent installed-library consumer.
The POC studies a JSON `EphemeralStream` over nghttp2/OpenSSL. It does not retry,
replay, mint tokens, or refresh them. A failure terminates the affected stream
or connection and makes pending waits fail.

## Source map

| Files | Responsibility |
|---|---|
| `include/zbperf.h` | Public opaque handles, configurations, operations, and statistics. |
| `src/sdk.c` | Admission, descriptor lifetime, workers, connection state, batching, acknowledgments, flush. |
| `src/queue.[ch]` | Fixed-capacity mutex and atomic MPSC descriptor rings. |
| `src/pool.[ch]` | Atomic reservation and release of exact payload bytes. |
| `src/proto.[ch]` | Minimal JSON protobuf envelopes and response decoding. |
| `CMakeLists.txt`, `cmake/`, `Makefile` | Standalone build, sanitizer options, installed CMake package. |
| `examples/ingest.c` | Complete pipelined public-API application. |
| `tests/test_*.c` | Queue, byte budget, wire format, and deterministic SDK state regressions. |
| `tests/integration*`, `tests/benchmark_integration.py` | Local protocol/TLS and external-consumer accounting fixtures. |
| `../purec-perf-bench/benchmark.c` | Independent public-API load driver. |
| `../purec-perf-bench/mint_token.sh` | One-shot SP token exchange outside the SDK and measured benchmark. |
| `../purec-perf-bench/run_matrix.py`, `tests/` | Interleaved experiment orchestration and benchmark tests. |

## Objects and execution ownership

```text
Application producer threads
           |
           | copy and publish (concurrent zbp_ingest / zbp_ingest_batch)
           v
Stream's bounded request queue
           |
           | one consumer
           v
Connection's assigned I/O worker
  | batching + wire offsets + protobuf framing
  | nghttp2 callbacks -> TLS -> socket
  | socket -> TLS -> nghttp2 -> response parser
  v
Outstanding-request FIFO -> cumulative acknowledgment -> progress / reclamation
```

A runtime owns a fixed number of pthread I/O workers. Connections are assigned
round-robin. One worker services several connections when there are fewer
workers than connections; one worker per connection permits independent I/O
progress. Benchmark producers are application threads, not SDK threads.

Each connection has one socket, one SSL object in TLS mode, and one nghttp2
session. A session can contain multiple logical gRPC streams. Workers alone
access active SSL/nghttp2 state. Connection and stream lists are append-only:
creators publish fully initialized handles with a release compare/exchange,
and workers acquire the list head. No structural mutex is held across I/O.
Creating handles may allocate, resolve DNS, and wait
for handshakes; these operations are excluded from timed throughput.

Handles remain alive until runtime destruction. Statistics can be read after
stream close. Destruction requires that application API calls have stopped;
it joins workers before freeing data they can reference. This keeps reference
counting off the normal per-record path.

## Public operations

| Operation | Meaning |
|---|---|
| `zbp_runtime_create` | Configure I/O worker count, idle policy, output coalescing. |
| `zbp_connection_open` | Open a physical connection with fixed endpoint and credentials. |
| `zbp_stream_open` | Complete JSON stream creation on that connection. |
| `zbp_ingest` | Copy and publish one JSON record. |
| `zbp_ingest_batch` | Copy and publish an explicit protocol batch atomically. |
| `zbp_flush` | Publish a barrier and wait for durability of preceding work. |
| `zbp_stream_get_stats` | Read progress without waiting for durability. |
| `zbp_stream_close` | Stop admission and drain after producers have stopped. |
| `zbp_stream_abort` | Cancel pending work and wake callers, retaining the handle. |
| `zbp_runtime_destroy` | Stop workers and free all owned resources. |

Record bytes need only be valid for the duration of an ingest call. The SDK
does not validate JSON syntax or transform records. Length, bounds, arithmetic,
and protocol framing checks remain necessary. All-or-nothing batch admission
means an unsuccessful call does not admit a prefix of its records.

Configuration initializers define the baseline: one sleeping I/O owner, 64 KiB
write coalescing, mutex queues, single-record wire requests, and release-sent
payloads. Each stream defaults to 4,096 queue slots, 4,096 outstanding requests,
64 MiB payload capacity, 1 MiB messages, and a 1,024-record automatic-batch cap.
Accumulation delay and latency sampling default to zero. Connection and stream
admission timeouts default to 10 seconds. All capacities are per stream in the
SDK; the benchmark divides aggregate budgets explicitly.

`ZBP_INVALID` rejects malformed arguments/configurations; `ZBP_LIMIT` rejects
requests outside capacity/encoding bounds; `ZBP_NOMEM` reports setup or payload
allocation failure. Exhausting the payload byte budget follows the bounded
admission wait path. Allocation failure rolls back the byte reservation and
descriptor before returning `ZBP_NOMEM`. `ZBP_TIMEOUT` from ingest admits nothing; from
flush it does not retract previously admitted records. A later flush can still
succeed. `ZBP_CLOSED` ends admission; `ZBP_IO`, `ZBP_PROTOCOL`, and `ZBP_AUTH`
represent terminal transport, protocol, or authentication/trust failures.
Detailed server messages and production recovery policy are intentionally absent.

Flush is a stream-queue barrier, not a sample of a racing statistics counter.
An ingest completed before flush starts precedes the barrier. A concurrently
overlapping ingest may fall on either side. A barrier seals any automatic batch
before it. Its target is the last preceding wire offset; an empty barrier needs
no new wire message. A timed-out flush cannot leave a reference to caller stack
memory in the queue. Barriers use pooled descriptors with two references, one
for the waiter and one for the worker; recycling requires both to release.
One deadline covers admission and waiting for the ack. Head barriers are also
consumed outside nghttp2 DATA callbacks, so an exhausted send window cannot
strand a barrier whose preceding requests have already been acknowledged.
Local-only barriers also progress during TLS retry backpressure on another
stream sharing the connection; a partially encoded request must finish before
its following barrier can be consumed.
After draining barriers, request selection checks the head again: a concurrent
flush can publish a new barrier between the drain and the next peek.

## Request queues and allocation

The queue holds descriptors; it does not concatenate payload bytes. Both queue
variants have a fixed power-of-two capacity:

- The mutex ring takes a short lock for descriptor insertion/removal. Payload
  preparation and copying happen outside that lock.
- The atomic MPSC ring uses monotonic positions and a sequence number per slot.
  A producer reserves a slot, writes it, then publishes with release ordering.
  The consumer acquires the sequence before reading and releases the slot for
  reuse only after consumption. A reserved but unpublished slot is not ready.

Allocation must finish before an irreversible queue reservation. Failed
admission returns all acquired storage. Reuse of queue indices never shifts
entries, and payload cursor advancement never copies the remaining payload.
Producer and consumer indices are separated to reduce cache-line contention.

Payload storage uses one exact-size `malloc` allocation for each admitted
single record or explicit batch. Before allocation, a compare/exchange loop
reserves its byte count against the stream's `buffer_bytes` limit. The existing
atomic `buffered_bytes` counter is the sole occupancy source. A failed budget
reservation enters the existing bounded admission wait; a failed `malloc`
returns its byte reservation and descriptor immediately. Payloads are freed
before their byte reservations are released, so another producer cannot reuse
capacity while the old allocation is still live.

There are no payload size classes, slabs, eviction scans, or payload-pool mutex.
Removing the scan prevents admission near a full buffer from walking all live
slabs under a mutex. The system allocator may retain free memory internally;
the SDK's budget limits live payload bytes rather than the process's RSS.

Descriptors are allocated once: `queue_capacity + max_inflight + 2` per stream.
Their atomic free-list head combines a 32-bit index with a 32-bit generation
counter; as with other finite tags, this is not an unbounded ABA proof under
arbitrarily long thread suspension and generation wraparound.
Stream allocations honor the queue's 64-byte alignment. Payload and queue
capacity remain bounded; retries do not grow storage indefinitely.
`buffered_bytes` counts exact bytes reserved during admission and retained
through the selected release point. Single records use their JSON byte count;
explicit batches also include repeated-string protobuf prefixes already stored
with their payload. Envelope headers are stored separately. The byte budget
excludes descriptors, nghttp2 buffers, and allocator overhead. Process RSS also
includes the input corpus and stacks. `peak_buffered_bytes` records successful
allocations' observed reserved occupancy. The atomic budget needs only relaxed
ordering because queue publication and descriptor recycling separately order
access to payload memory.
The mutex ring and atomic ring share the same storage strategy so measurements
do not conflate queue synchronization with allocation changes.

There is intentionally no thread-local producer registration or hidden
producer-specific stream lifetime. Such complexity is outside the selected
stream-only API comparison. `pool.c` remains in the source list for external
consumers that compile these sources directly; it now contains only byte-budget
helpers.

## Batching, offsets, and encoding

The wire contract is `databricks.zerobus.Zerobus/EphemeralStream`. The first
request is `CreateIngestStreamRequest` with a table name and JSON record type.
Record requests are sent only after stream creation succeeds.

Single mode sends one `IngestRecordRequest` per single-record API call. Explicit
batches use `IngestRecordBatchRequest` with repeated JSON strings. Auto mode
combines available single-record calls into bounded protocol batches. Explicit
batches retain their caller-selected boundary.

The initial automatic-batching cap is 1 MiB encoded message size or 1,024
records. Zero accumulation delay sends currently available work without waiting
to fill a batch. A configured delay supplies an additional benchmark dimension;
flush always seals preceding work promptly.

Wire offsets belong to requests, not individual records. Only the I/O owner
assigns them, in send order, starting from zero. Each outstanding request stores
its number of records and payload bytes. When the server acknowledges offset
N, all sent requests through N become durable and contribute their totals to
public progress. Duplicate cumulative acks do not double-count. An impossible
future ack must not invent durability.

Encoding uses small protobuf/gRPC headers plus payload spans. There is no
temporary allocation containing a second copy of the entire encoded request.
nghttp2's normal DATA callback copies spans into its frame buffer. Partial
callback consumption advances span cursors. This is a minimum-intermediate-copy
design, not a claim of zero-copy TLS or network transmission.

## Buffer lifetime experiment

```text
admitted -> queued -> consumed by transport -> acknowledged
                         |                       |
release-sent mode:     payload recycled       metadata retired
retain-until-ack:      payload retained       payload + metadata retired
```

The queue slot can be reused after the owner consumes the descriptor. A bounded
outstanding FIFO maintains acknowledgment accounting independently. In
release-sent mode, payload storage can be recycled once the transport owns any
remaining copy; it need not remain alive until socket transmission or ack. In
retain-until-ack mode the original storage remains attached to the outstanding
request. No additional payload copy is introduced for retention.

Both modes retain identical accounting metadata and enforce outstanding
capacity. The retained mode is a measurement of a future landing zone's cost;
there is no replay path and no durability guarantee after a terminal failure.

## I/O, flow control, and waits

Workers use nonblocking sockets and readiness polling. Producer notifications
are coalesced through an eventfd. Workers recheck work after arming sleep so a
publication concurrent with sleeping cannot be lost. Blocked application
threads use condition variables and predicate loops with monotonic deadlines.
Admission waiters register one of two reasons under the stream's wait mutex:
allocation (a descriptor or payload byte reservation) or queue capacity. A caller
unregisters after allocation succeeds before it can register for queue space;
there is no migration between reasons while registered. Flush and setup waiters
use the aggregate waiter count. The counts let each capacity notification check
whether anyone is waiting for the resource that actually changed.

Outgoing DATA callbacks collect queue-slot and payload releases into a pending
mask owned by the stream's I/O worker. Queue consumption wakes queue waiters; freeing
release-sent payloads also wakes allocation waiters. Retain-until-ack payloads
do not produce allocation notifications during DATA encoding. A cumulative ACK
retires all covered requests, then adds the allocation reason only when it
released retained payloads/descriptors. Outstanding-metadata capacity is owned
by the I/O worker and is not an admission wait reason.

The connection owner clears and flushes every stream's pending mask once at the
end of its bounded service turn, after reading and writing. Multiple DATA and
ACK callbacks in that turn therefore produce at most one ordinary capacity
broadcast per stream. A wrapper performs this flush on every return, including
TLS retry and transport-error paths, before the owner can block on readiness.
The per-direction budget limits I/O bytes per turn, while ACK-retirement work
depends on the acknowledged backlog. There is no strict wall-clock notification
bound or wakeup timer. Producers never access the pending mask.

Barrier completion, errors, cancellation, cleanup, and readiness changes still
notify all registered waiters immediately. A barrier's last reference and
producer-side rollback notify allocation waiters immediately after returning
their descriptor or bytes. Both admission reasons share a condition variable,
so a necessary broadcast can also wake callers waiting for the other reason;
they recheck their predicates and wait again.

Sequentially consistent fences pair registration with notification. A waiter
increments the aggregate and matching reason counters, fences, and then
rechecks its resource predicate before sleeping. A notifier releases resources,
fences, and only then checks the matching counter. If the waiter fence precedes
the notifier fence, the counter check observes registration (or a later
unregistration). In the opposite order, the predicate recheck observes the
resource release or a later update. Thus relaxed payload-byte accounting cannot
combine with a stale waiter count to lose the only wakeup. The same fence pair
covers the aggregate readiness/flush/status predicates. The wait mutex makes
the final predicate check and condition-variable sleep atomic with respect to
a broadcaster acquiring that mutex.

Each worker services reads as well as writes under load. Stream and connection
HTTP/2 windows limit transmission; increasing our receive window does not
increase the peer's receive window. Never busy-send against an exhausted
window while ignoring inbound WINDOW_UPDATE or acknowledgment frames.

Output coalescing is independent of protocol batching. A reusable buffer can
combine nghttp2 output before a socket/TLS call; direct mode preserves the
uncoalesced path for comparison. Unsent bytes remain immutable across partial
writes. A pending TLS write retains its exact contents and retry length. A
write returning WANT_WRITE still permits reads, so acknowledgments and control
frames progress when both directions are backpressured. No new outbound work
replaces a pending write; direct mode relies on nghttp2 retaining its buffer,
while coalesced mode retains the output-buffer slice. Write WANT_READ and read
WANT_WRITE are retried as the operation requiring handshake progress. TLS
internal pending plaintext is drained without waiting for another socket edge.
Each service turn budgets up to 1 MiB per direction for connection fairness.
`ppoll` preserves microsecond batching deadlines. SDK workers block SIGPIPE
locally; the application process's signal disposition is unchanged.

The response parser maintains gRPC header/payload cursors across HTTP/2 DATA
frames. It handles fragmented and multiple responses without shifting the
unconsumed buffer. Each stream allows at most 1 MiB for a response. Each
connection has a persistent 64 KiB input buffer; enabled output coalescing uses
at least 32 KiB, even when the requested threshold is smaller. Larger HTTP/2
frames are accepted through partial send-callback consumption.

HTTP failures, unsuccessful gRPC trailers, stream reset, peer close before full
acknowledgment, and TLS/socket failure become terminal statuses. No fallback to
plaintext occurs. Explicit plaintext testing is restricted to loopback.

## Benchmark boundary and measurements

`purec-perf-bench/` has its own CMake project. It can be copied anywhere and
uses `find_package(zbperf CONFIG REQUIRED)` to link an installed SDK. It does not
include private SDK headers or source, and the SDK does not launch benchmarks,
load datasets, acquire tokens, or generate records.

The adjacent standalone `mint_token.sh` helper performs a one-shot OAuth
client-credentials exchange before a run, using SP credentials from the
environment. It requests the workspace's Zerobus resource and table-scoped
Unity Catalog privileges over verified HTTPS. It is not called by the SDK or
benchmark executable and adds no token cache, refresh logic, or runtime
dependency to the C library. Only a captured token crosses into the SDK.

Statistics are optional observations of the SDK's ordinary work. Counters are
atomic; a multi-field snapshot is not a transaction. The SDK defaults to no
latency sampling (`sample_every=0`); the benchmark defaults to 1024. Each
selected admission carries its ingest-entry timestamp. An
explicit batch has one timestamp, and an automatic batch retains the earliest
selected admission timestamp among its members. Thus the histogram has at
most one sample per wire request, not one sample per record. It includes
allocation/copy, admission waits, local queueing, and acknowledgment latency.
Histogram buckets are log2 microseconds. Disable sampling for a throughput
cross-check. Connection statistics count transport calls/bytes and stalls.

`zbp_connection_get_diagnostics()` separately exposes peer-advertised initial
stream credit and maximum frame size, current connection credit and the minimum
credit among active streams, lifetime peak observed credit, and the count plus
minimum/maximum stream WINDOW_UPDATE increments. Window snapshots
are taken after peer SETTINGS/WINDOW_UPDATE and after each send turn. These are
send limits; our own 16 MiB receive window governs incoming acknowledgments.
Counters distinguish socket/TLS write backpressure (`write_would_block`), TLS
write retries requiring reads (`write_want_read`), and the 1 MiB fairness budget
(`send_budget_yields`). `flow_control_ns` counts observed time with queued or
partially sent payload and exhausted peer credit, including an ongoing stall.
Available peer credit ends the timed stall before the next send turn, and newly
exhausted credit starts another. A versioned atomic observation keeps the elapsed
counter monotonic across stall start/end while producers read statistics.
Other observations are non-transactional, not a tracing timeline or proof that
every nanosecond in the interval was idle. Reading them adds no wire traffic.
The separate API leaves the original public statistics structure unchanged.

The benchmark preloads a realistic JSONL corpus, starts producers together,
drains warmup work, measures a fixed interval, and performs a final bounded
flush. It reports timed acknowledged rate separately from completed-cohort
rate including the final drain. Admission is never presented as durability.
Memory/queue peaks that cannot be reset are labeled lifetime peaks.

Staging comparisons use the same data, aggregate memory budget, CPU placement,
and request limits. Three interleaved screening trials and five finalist trials
help expose noise. Server saturation can hide client improvements; document
such results as inconclusive. No table or dataset is selected implicitly.

## Deferred execution models

### Separate sender and receiver execution

Possible boundaries are separate protocol/queue tasks above a single transport
owner, or physical read/write execution below a synchronized connection state
machine. Splitting above transport is the simpler first experiment: a sender
prepares work, the transport owner handles nghttp2/OpenSSL, and acknowledgment
processing may be handed to a receiver task.

Do not concurrently invoke an nghttp2 session or SSL object without an explicit
ownership protocol. TLS reads can require writes, and TLS writes can require
reads. HTTP/2 input also changes outbound flow-control state. Any physical
split must coordinate those dependencies, partial-write lifetimes, errors, and
shutdown. Extra queues, cache transfers, and scheduling may cost more than the
parallel work saves. Measure it when profiles show one owner saturated by
independent encoding/ack work, not merely because two directions exist.

### Task pool instead of dedicated workers

A later executor could schedule bounded ingestion, send-readiness,
receive-readiness, and acknowledgment tasks on a pthread pool. A connection
still needs serialized ownership (a strand, lease, or equivalent), even when
different pool threads run its successive tasks. Readiness events should
schedule work instead of parking a pool thread on a socket or ack.

Define task budgets, fair rescheduling, cancellation, and ownership transfer
before implementing this. Avoid scheduling the same connection concurrently;
bound pending tasks and do not move backpressure into an unbounded executor
queue. Preserve publication order, flush barriers, and immutable TLS retry
buffers through handoffs. Runtime shutdown must cancel/join tasks before any
stream or connection can be freed.

The useful experiment is many connections with uneven activity, where dedicated
workers underutilize cores. Compare runnable-task overhead, connection cache
locality, fairness, throughput, and latency with the fixed-worker baseline.
Neither deferred model is assumed to improve performance.

## Verification record

Unit and integration results, sanitizer availability, exact commands, and
observed limitations are recorded in `findings.md` as they are run. Performance
results require an explicitly identified staging table and realistic corpus;
local protocol fixtures establish correctness, not a staging throughput claim.
