# Benchmark options and experiment tracker

Use this file as an editable notebook. Record results below, then mark a preferred
choice for each category. The category ledger has no winners yet.

[PERFORMANCE_FINDINGS.md](PERFORMANCE_FINDINGS.md) records the 2026-09-23
single-stream staging investigation: eleven runs, with a peer HTTP/2
stream-credit bottleneck and 8.86 MB/s sustained acknowledged throughput.

Config keys below go in `baseline` or a finalist's `options`, without `--`.
Numeric values are JSON numbers, modes are strings, and `test-plaintext` is a
boolean. A finalist inherits the baseline and overrides only its listed keys.
Producer counts are selected by the runner's `--producers`, not by finalists.

For the current Rust-aligned preset, see [RUST_DEFAULTS.md](RUST_DEFAULTS.md).
It distinguishes copied defaults from C-only approximations and records the
remaining queue, transport, and scheduling differences. Defaults in the tables
below still describe the C executable itself, not that preset.

## Record sets

| Corpus | Records | Bytes per JSON record | File size, including LF | Contents |
|---|---:|---:|---:|---|
| [records-1k.jsonl](examples/records-1k.jsonl) | 1 | 1,024 (1 KiB) | 1,025 | `id`, `name`, `level`, `msg`, `region`; `msg` is all `x` |
| [records-4k.jsonl](examples/records-4k.jsonl) | 1 | 4,096 (4 KiB) | 4,097 | Same fields, longer `msg` |

Sizes include JSON keys, punctuation, and string quotes, but exclude the trailing
newline and transport framing. The records match the schema of
`shinkansen.default.danilo_fluentbit_poc`. Each file is valid JSONL with one
object, not a JSON array. Set `input` to the file path. The benchmark preloads the
file and cycles through it; repeated ingestions append duplicate rows.

Run each experiment separately with each corpus to see how record size changes
the tradeoffs. These synthetic corpora isolate size effects; they do not model
real-world content variability or server-side compression behavior.

## Discrete choices

Defaults here are the benchmark executable's defaults, not recommendations or
measured winners. Suggested numeric sweeps below are starting points.

| Category / key | Available choices | Default | What changes / caveat |
|---|---|---|---|
| Queue / `queue` | `mutex`, `atomic` | `mutex` | Admission queue synchronization. Atomic mode does not make the entire SDK lock-free. |
| Batching / `batch` | `single`, `auto`, `explicit` | `single` | One record per call/message; individual calls combined by SDK; or producer calls the batch API. |
| Retention / `retention` | `sent`, `acked` | `sent` | Reclaim payload after transport consumption, or retain until durable ack. Both modes track acks. `acked` estimates the memory cost of a future landing zone; it does not add replay/reconnect. |
| Idle policy / `idle` | `sleep`, `spin`, `busy` | `sleep` | Sleep while idle; bounded spin then sleep; or continuously poll. Compare CPU cost alongside throughput. |
| Transport buffering / `write-coalesce-bytes` | `0` (direct) or positive byte count | `65536` | Combines transport writes, independently of protocol batching; buffer is per connection. |

The first four rows give 2 × 3 × 2 × 3 = **36 discrete combinations** before
topology, coalescing, or numeric parameters. Adding the three predefined
coalescing settings gives 108 per topology/producer count/corpus. Enumerating
every numeric combination would be impractical; the tables specify the available
dimensions, and the stage table provides a manageable initial test set.

## Concurrency and topology

| Key | Default | Suggested sweep | Meaning / constraint |
|---|---:|---|---|
| `producers` | 1 | 1, 2, 4, 8, 16 | Application ingestion threads. Matrix CLI `--producers` overrides the config value; maximum 65,536. |
| `streams` | 1 | 1 or producer count | Ingest streams; producers map to streams round-robin. |
| `connections` | 1 | 1 or stream count | TLS/HTTP2 connections; streams map to connections round-robin. |
| `io-threads` | 1 | 1 or connection count | SDK I/O owners; connections map to owners round-robin; maximum 1,024. |

Require `producers >= streams >= connections >= io-threads >= 1`.
For a producer count `P`, these named combinations isolate placement and ownership:

| Combination | Producers | Streams | Connections | I/O threads | Built-in stage |
|---|---:|---:|---:|---:|---|
| Shared stream | P | 1 | 1 | 1 | `topology` |
| Shared connection | P | P | 1 | 1 | `topology`, P > 1 |
| Separate connections, shared owner | P | P | P | 1 | `topology`, P > 1; `ownership` |
| Separate connections, dedicated owners | P | P | P | P | `ownership`, P > 1 |

Intermediate valid counts are also supported through custom finalists. These are
dedicated pthread I/O owners, not a task pool. Split send/receive threads and
task-pool scheduling remain future experiments, not current flags.

## Numeric tuning

| Key | Default | Suggested values | Scope / notes |
|---|---:|---|---|
| `batch-records` | 1024 | 64, 256, 1024 | Record cap for `auto` and `explicit`; positive. Bytes may limit batches first. |
| `batch-bytes` | 1048576 | 65536, 262144, 1048576 | Encoded batch cap in bytes. Must fit at least one record plus envelope overhead; explicit batching reserves conservative envelope space. |
| `batch-delay-us` | 0 | 0, 10, 50, 100 | Automatic accumulation window, in microseconds; only affects `auto`. Zero drains available work without intentional waiting. |
| `write-coalesce-bytes` | 65536 | 0, 65536, 262144 | Per-connection output buffering; SDK maximum 16 MiB. Independent of batch size. |
| `spin-us` | 50 | 0, 10, 50, 100 | Bounded spin duration; relevant only for `idle: spin`. |
| `buffer-bytes` | 67108864 | 16777216, 67108864, 268435456 | Aggregate payload-storage budget across streams. Allocator rounding counts against it; allow room for entire explicit batches. |
| `queue-capacity` | 4096 | 1024, 4096, 16384 | Aggregate admission request slots, divided equally; each stream rounds down to a power of two, minimum 2. An explicit batch occupies one slot, not one per record. |
| `max-inflight` | 4096 | 1024, 4096, 16384 | Aggregate outstanding wire-request metadata slots, divided equally; at least 1 per stream. |
| `sample-every` | 1024 | 0, 1024, 8192 | Admission-to-ack latency sampling interval; 0 disables latency samples. Hold fixed unless measuring sampling overhead. |
| `admission-timeout-ms` | 1000 | 1000, 10000 | Maximum bounded-admission wait, positive milliseconds. Timeout fails the run; it is not an achieved throughput result. |

Keep **aggregate** payload/queue/in-flight budgets equal for topology comparisons.
Read effective budgets from results: division and power-of-two rounding can
reduce them. Per-connection output buffers, descriptors, TLS state, corpus
storage, and thread stacks are additional memory, not part of `buffer-bytes`.
Queue entries before automatic batching and wire-request entries after batching
are not interchangeable units.

SDK limits additionally require at least 256 payload-buffer bytes per stream,
`batch-bytes` in 64–4,294,967,231, and `batch-records` in 1–4,294,967,295. Queue
capacity per stream is at most 1,073,741,824 (a power of two); in-flight capacity
is at most 2,147,483,645 per stream. These are representational bounds, not usable
memory-budget recommendations. `batch-delay-us`, `spin-us`, `sample-every`, and
`admission-timeout-ms` must fit an unsigned 32-bit value. Service limits can be lower than SDK
limits; the byte caps must accommodate actual records and their envelopes.

## Workload, timing, and connection settings

| Key | Default | Meaning |
|---|---|---|
| `host` | Required | Ingest endpoint hostname, without URL scheme. |
| `port` | Required | Port 1–65535, normally 443 for TLS. |
| `table` | Required | Existing `catalog.schema.table`; schema must match records. Wrapper mints for `baseline.table`. |
| `input` | Required | Nonempty regular JSONL file; preloaded before measurement. Relative paths use the caller's working directory. |
| `token-env` | `ZB_TOKEN` | Name of environment variable containing the token, never the token itself. Use `ZB_TOKEN` with the wrapper. |
| `ca-file` | System trust store | Optional CA file for TLS verification. |
| `test-plaintext` | `false` | Local fixtures only (`localhost`, `127.0.0.1`, `::1`), not staging. Use the benchmark directly for fixture runs. |
| `warmup` | 10 | Warmup admission seconds, then a full drain; zero is allowed. |
| `duration` | 60 | Timed admission seconds, positive; fractional values allowed. |
| `drain-timeout` | 60 | Positive seconds for flushing all streams, once after warmup and once after measurement. |

Warmup also ingests real records. Wall-clock runtime includes startup, warmup,
both drains, and the measured window. The SDK does not refresh tokens, reconnect,
or retry a failed run. Mint a fresh token between invocations and keep each matrix
within its roughly one-hour token lifetime. The wrapper uses the token helper's
workspace defaults; override `ZB_WORKSPACE_URL` and `ZB_WORKSPACE_ID` together for
another workspace (the ingest `host` does not select the OAuth workspace).

## Built-in comparison stages

All unlisted options inherit the baseline. `P` is the current producer count.
The runner does not automatically carry a winner into the next stage.

| Stage | Variant name (before `_pP`) | Overrides |
|---|---|---|
| `queues` | `mutex` | `queue: mutex` |
| `queues` | `atomic` | `queue: atomic` |
| `batching` | `single` | `batch: single`, `batch-delay-us: 0` |
| `batching` | `explicit` | `batch: explicit`, `batch-delay-us: 0` |
| `batching` | `auto_1m_now` | `batch: auto`, `batch-bytes: 1048576`, `batch-delay-us: 0` |
| `batching` | `auto_64k_now` | `batch: auto`, `batch-bytes: 65536`, `batch-delay-us: 0` |
| `batching` | `auto_1m_50us` | `batch: auto`, `batch-bytes: 1048576`, `batch-delay-us: 50` |
| `coalescing` | `direct` | `write-coalesce-bytes: 0` |
| `coalescing` | `coalesce_64k` | `write-coalesce-bytes: 65536` |
| `coalescing` | `coalesce_256k` | `write-coalesce-bytes: 262144` |
| `topology` | `shared_stream` | `streams: 1`, `connections: 1`, `io-threads: 1` |
| `topology` | `shared_connection` | `streams: P`, `connections: 1`, `io-threads: 1` (P > 1) |
| `topology` | `separate_connections` | `streams: P`, `connections: P`, `io-threads: 1` (P > 1) |
| `ownership` | `shared_owner` | `streams: P`, `connections: P`, `io-threads: 1` |
| `ownership` | `dedicated_owners` | `streams: P`, `connections: P`, `io-threads: P` (P > 1) |
| `retention` | `sent` | `retention: sent` |
| `retention` | `acked` | `retention: acked` |
| `idle` | `sleep` | `idle: sleep` |
| `idle` | `spin` | `idle: spin` (inherits `spin-us`) |
| `idle` | `busy` | `idle: busy` |
| `finalists` | Your entry's `name` | Your entry's `options`; may combine controls, but not override `producers` |

For arbitrary sweeps, add multiple finalists with distinct names. Example:

```json
"finalists": [
  {"name": "baseline", "options": {}},
  {"name": "atomic-auto", "options": {"queue": "atomic", "batch": "auto"}},
  {"name": "atomic-auto-256k", "options": {"queue": "atomic", "batch": "auto", "batch-bytes": 262144}}
]
```

This is a fragment to insert alongside `baseline`, not a complete config.
Test interactions among category winners here; the individual winners may not
form the best combination.

## Runner controls

These are runner CLI flags, not keys under `baseline`.

| Control | `run.sh` default | Direct `run_matrix.py` default / behavior |
|---|---|---|
| Config | First positional argument | Required `--config PATH` |
| Benchmark binary | Built in this benchmark's `build/` | Required `--bench PATH` |
| `--stage` | `finalists` | Required; one stage per invocation |
| `--producers` | `1` | `1,2,4,8,16`; comma-separated distinct positive counts |
| `--repeats` | `1` | 3 for screening, 5 for `finalists` |
| `--seed` | 20260922 | Same; reproducible shuffled order within each repetition |
| `--output` | Unique `.json` file under benchmark `results/` | Required for live runs; must not already exist; indented array of runs |
| `--dry-run` | Off | Print scheduled commands; no build, token, result file, or ingestion |

The wrapper's one-repetition default is convenient for a smoke run. Pass
`--repeats 3` or more when comparing performance. Live runs execute sequentially,
with a new process for each variant/repetition. Total runs = sum of variants at
each requested producer count × repetitions; topology and ownership have fewer
variants for P = 1. No full Cartesian product is generated automatically.

## Results ledger

Copy one row per variant/producer count/corpus; use the matrix summary's median
over successful repeats. Link the saved config and raw result JSON so an entry
can be reproduced. Use `pending`, `measured`, `preferred`, `inconclusive`, or
`failed` in Status. Mark preference only within the same workload and baseline.

| ID | Category / variant | Config / baseline version | Corpus | P / streams / connections / I/O | Successful / failed repeats | Median ack MB/s | Median cohort ack MB/s | Median CPU ns/acked record | Ack-rate relative range | Results file | Status / notes |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Q1 | queues / mutex | — | 1 KiB | 1 / 1 / 1 / 1 | — | — | — | — | — | — | pending |
| Q2 | queues / atomic | Same as Q1 | 1 KiB | 1 / 1 / 1 / 1 | — | — | — | — | — | — | pending |
| — | — | — | — | — | — | — | — | — | — | — | pending |

Raw metrics for the four numeric result columns are, respectively,
`acked_mb_per_second`, `cohort_acked_mb_per_second`,
`cohort_cpu_ns_per_acked_record`, and
`acked_mb_per_second.relative_range` (from the matrix summary).
MB/s uses 1 MB = 1,000,000 bytes, not MiB. Raw byte counters remain in bytes;
records/s is still available in results and summaries. Saved `.json` files are
indented arrays, with one entry for each run. Existing JSONL result files retain
their original format and units; record input files also remain JSONL.
Higher ack throughput is better; lower CPU ns per ack is better. Cohort throughput
includes producer stop and final drain. Relative range is `(max - min) / median`,
not a confidence interval. Also inspect raw latency histogram percentiles, memory,
and backpressure counters when these matter; latency buckets are upper bounds.

## Preferred choices by category

Edit these after measurements; keep the evidence ID and workload qualification.
There need not be one universal winner. For a tie or a service-limited result,
write `inconclusive` instead of choosing based on noise.

| Category | Preferred option / combination | Evidence IDs | Workload / producer counts | Reason / tradeoff |
|---|---|---|---|---|
| Queue | Not measured | — | — | — |
| Batching mode and limits | Not measured | — | — | — |
| Write coalescing | Not measured | — | — | — |
| Stream / connection topology | Not measured | — | — | — |
| I/O ownership | Not measured | — | — | — |
| Payload retention | Not measured | — | — | — |
| Idle / spin policy | Not measured | — | — | — |
| Buffer / queue / in-flight budgets | Not measured | — | — | — |
| Combined finalist | Not measured | — | — | — |

Keep compiler/build flags, SDK version, CPU affinity, host load, corpus, table,
and aggregate budgets fixed within a comparison. Check failures first: failed
runs are excluded from medians and must not disappear from the decision. Avoid
ranking clients from admission rate alone, and do not interpret a server/network
bottleneck as proof that two SDK configurations perform equally.
