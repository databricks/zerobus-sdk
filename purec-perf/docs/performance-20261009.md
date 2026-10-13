# Pool removal and admission wakeup optimization — October 9, 2026

The payload slab pool is gone. Payloads now use exact-size `malloc`/`free`
allocations bounded by an atomic byte reservation. The descriptor free list
remains. Admission notifications were also reduced: producers wake for relevant
capacity changes, with ordinary notifications combined across an I/O service
turn. The benchmark hot path was not modified.

These changes remove the severe multi-producer client bottleneck. They do not
establish substantially higher durable throughput than Rust against the real
local Shinkansen server: that workload reaches a server-side plateau around
100 MB/s. Receipt-only comparisons expose the client headroom separately.

The full experiment history, telemetry, artifact hashes, intermediate controls,
and limitations are in the [benchmark report](/home/danilo.trninic/benchmarks/docs/C_SDK_OPTIMIZATION_20261009.md).
The [implementation guide](implementation.md#request-queues-and-allocation)
describes ownership and synchronization in detail.

## What changed

| Files | Change |
|---|---|
| `src/pool.c`, `src/pool.h` | Replace slab scanning, eviction, and the pool mutex with atomic exact-byte reservation/release helpers. The filenames remain for consumers compiling the source list directly. |
| `src/sdk.c` | Allocate/free payloads; roll back failed admission; distinguish allocation and queue waiters; collect ordinary DATA/ACK capacity notifications and flush them on every connection-service return. |
| `tests/test_pool.c` | Exact budgets, reuse, arithmetic limits, concurrent reservations. |
| `tests/test_state.c` | Allocation failure and lifetime, resource-specific producer wakeups, wait-counter cleanup, multi-callback coalescing, TLS retry/error return paths. |
| `include/zbperf.h` | Clarify payload-budget statistics; no public layout or signature change. |
| `README.md`, `docs/implementation.md`, `NEXT_CHANGELOG.md` | Document the allocation and notification behavior, constraints, and performance handoff. |
| `../purec-perf-bench/RUST_DEFAULTS.md` | Correct the historical slab-pool description. |

The external suite has a separate read-only server sampler, experiment configs,
and reports under `~/benchmarks/`. No benchmark, OAuth, or profiling dependency
was added to the SDK. Rust, the wire format, transport settings, and SDK defaults
are unchanged. There is still no reconnect, token cache, or replay path.

## Why these changes helped

The earlier pool profile attributed 65.64% of sampled client CPU to
`zbp_pool_get`. Near capacity, allocation scanned live slabs under a mutex;
four producers amplified that cost. Removing the pool raised real-service
four-producer throughput from 41.54 to 92.45 MB/s in the first controlled
experiment, without changing the mutex queue.

That exposed a second bottleneck: excessive producer wakeups. Freeing each
payload could acquire the wait mutex and wake competing producers repeatedly.
ACK-retained producers could also wake on DATA encoding even though their byte
budget was still full. Coalescing reclamation, separating wait reasons, and
deferring ordinary notifications until the end of an I/O service turn removed
most of this work. In the four-producer receipt experiment, the final step cut
voluntary context switches from 2.72 million to 182 thousand while more than
doubling throughput compared with the preceding implementation.

The final step was not a universal win: the short explicit-batch control was
about 4% slower than the preceding stage. The report retains that result.

### Preserved guarantees and caveats

- Ingest still copies input before returning; successful flush still waits for
  ACKs covering all preceding work. Capacity limits and admission timeouts remain.
- The byte limit now counts exact live payload reservations, including stored
  explicit-batch length prefixes, rather than rounded size-class capacity.
  Allocator overhead, allocator-retained free memory, descriptors, transport
  buffers, and the application's corpus are excluded. It is not an RSS cap.
- Payload `malloc` failure returns `ZBP_NOMEM` after rolling back the reservation
  and descriptor. Budget exhaustion uses bounded admission waiting.
- Ordinary notifications are deferred only within a service turn, including
  flushing on TLS retry/error returns before polling. Errors, cancellation,
  barriers, and producer rollback still notify immediately. ACK reclamation can
  cover a large backlog, so the I/O byte budget is not a wall-clock wakeup bound.
- Paired C11 fences protect waiter registration/predicate recheck against
  release/notification. The shared condition variable and its mutex remain on
  the blocked path; this is not an entirely lock-free SDK.

## Repeated results

All 36 frozen confirmation trials passed. Values below are medians of three
repetitions with shuffled case order. Real-service measurements lasted 60
seconds after five seconds of warmup; receipt-only measurements lasted 30
seconds after five seconds of warmup. The full report preserves every trial,
min–max ranges, artifact hashes, and accounting checks.

Real local Shinkansen, single-record calls and C ACK retention:

| Producers | C MB/s | Rust MB/s | C CPU µs/record | Rust CPU µs/record |
|---|---:|---:|---:|---:|
| 1 | 93.76 | 95.07 | 2.627 | 3.973 |
| 4 | 93.38 | 95.87 | 3.428 | 4.851 |

C's CPU cost is 33.9% lower with one producer and 29.3% lower with four, but
throughput is 1.4% and 2.6% lower respectively. Release-sent C medians were
95.27 and 94.63 MB/s; that also does not establish a throughput advantage.
Compared with the earlier old-C four-producer control (41.54 MB/s), the final
93.38 MB/s is a 2.25× improvement. That comparison spans separate experiments,
not paired repeated trials of the old binary.

Receipt-only TLS receiver, C atomic queue and ACK retention:

| Profile | C MB/s | Rust MB/s | C / Rust | C CPU µs/record | Rust CPU µs/record |
|---|---:|---:|---:|---:|---:|
| Single records, 1 producer | 854.31 | 563.28 | 1.52× | 3.561 | 4.210 |
| Single records, 4 producers | 903.21 | 431.70 | 2.09× | 3.996 | 5.494 |
| Explicit batches up to 512 records / 1 MiB, 1 producer | 1245.63 | 864.09 | 1.44× | 2.176 | 3.898 |

Rust uses the stronger worker count found in screening: one for singles and
two for explicit batches. This is the fastest Rust configuration tested for
each profile, not an exhaustive tuning claim. Both clients have the same CPU
allowance, but not identical execution models: C's pthread producers and I/O
owner can use more than one core; the selected single-worker Rust runtime uses
about one. CPU per record includes the cost of that concurrency.

The first four-producer Rust receipt trial fell to 142.71 MB/s during the run;
the other two reached 438.63 and 431.70 MB/s. The slow trial remains included
in the median and range. Its configuration and hashes matched, its client
remained CPU-busy, and receiver activity declined. No stack profile identified
the cause, and process-level samples cannot exclude cache/SMT interference.
C's three repeats were stable at 900.60–903.72 MB/s. Even its slowest repeat
exceeded twice the fastest Rust repeat, so the headline gain does not rely on
the slow outlier. Receiver headroom is not certified; these are results for
the full benchmark/receiver setup, not isolated SDK maxima.

Measurements use completed-cohort logical JSON bytes, including producer stop
and final ACK drain, in decimal MB/s. They do not score admission alone. Client
CPU includes the whole independent benchmark process, not just SDK functions.
All profiles have one stream and connection; four producers share that stream.
Receipt-only ACKs do not imply durability or table visibility.

## Verification

The final implementation passed these checks before confirmation measurements:

| Check | Result |
|---|---|
| `make check` and Release unit suite | Build clean; 4/4 unit tests |
| Full Release protocol/TLS integration | 41/41 |
| ASan + UBSan | 4/4 units and full 41/41 integration |
| ThreadSanitizer | Queue, byte-budget, state: 3/3; no warnings or suppressions |
| Independent code review and finding analysis | Converged; no remaining code or documentation findings |

The final code review's documentation warning concerned an overstated bound on
notification delay; it was corrected. A handoff-review nit about which file
documented allocation failure was also corrected. No retained false positives
or reference inaccuracies were reported. `NEXT_CHANGELOG.md` is updated. These
checks do not constitute an exhaustive proof of all thread schedules. No
final-code TSAN TLS integration claim is made; see [historical findings](findings.md) for earlier
OpenSSL qualifications.

## Run it again

From `~/benchmarks`, with your existing local Shinkansen stack running:

```sh
python3.8 bench.py plan configs/c-optimization-20261009/real-current.json
zsh -c 'source ~/.zshrc >/dev/null 2>&1; python3.8 scripts/run_guarded.py run configs/c-optimization-20261009/real-current.json --poll-seconds 1'
```

This single-case config builds the current C source, links the independent
benchmark, mints the token, and runs against the existing ClickBench table.
It uses one producer, an atomic queue, ACK retention, 65,536 queue/in-flight
slots, a 64 MiB byte budget, one sleeping I/O owner, and 64 KiB write coalescing.
The optimized configuration is explicit: SDK defaults were not changed.

The old suite targets pin the old C executable. Rebuilding the SDK does not
update them. `c-current` builds current source; `c-final` pins the measured final
implementation; `c-optimized` intentionally remains the stage-three intermediate.
See the [config guide](/home/danilo.trninic/benchmarks/configs/c-optimization-20261009/README.md)
for the complete comparison suite, frozen artifacts, and receiver-only cases.

Further durable-throughput work needs profiling the busy server-side stream
processing task. Client headroom alone cannot remove that bottleneck. No server
algorithm, storage policy, or durability setting was changed in these experiments.
