# Single-stream throughput investigation — 2026-09-23

## Outcome

**100 MB/s was not reached.** Eleven successful staging runs identify peer-side
HTTP/2 stream credit as the immediate limit on this host/network path. The
60-second confirmation run delivered **8.860 MB/s acknowledged**, with
**98.82% of the measured interval waiting for stream credit** and only
**0.014 CPU cores** used by the entire benchmark process.

Changing client queue sizes, batching, or I/O thread ownership cannot enlarge
the peer's advertised send window. No speculative transport rewrite was made.
The SDK now exposes the diagnostics that establish this limitation, and the
standalone benchmark records them through the public API.

## Conditions and results

Every run used one producer, one stream, one connection, and one I/O owner;
TLS verification remained enabled. The destination was
`shinkansen.default.danilo_fluentbit_poc` at the configured us-west-2 staging
endpoint. The input was the existing synthetic 1 KiB JSONL record, except for
the explicitly labeled 4 KiB test. There was no compression. All rates below
are acknowledged payload bytes, in decimal MB/s, not producer admission rates.

The controlled runs used a 32 MiB payload budget, 32,768 queue/in-flight slots,
acked retention, and a 1 MiB encoded batch cap. Explicit batches hold up to
1,014 of the 1 KiB records because the benchmark reserves protobuf overhead.
The user's `bench-config.json` was left unchanged.

| Experiment | Measured window | Acknowledged MB/s | CPU cores |
|---|---:|---:|---:|
| Original binary, single-record calls | 10 s | 8.726 | 0.045 |
| Original binary, explicit batches | 10 s | 8.826 | 0.018 |
| Original binary, automatic batches | 10 s | 8.841 | 0.027 |
| Initial diagnostics, single-record calls | 10 s | 8.652 | 0.047 |
| Initial diagnostics, explicit batches | 10 s | 8.826 | 0.015 |
| Initial diagnostics, automatic batches | 10 s | 8.658 | 0.029 |
| Direct writes, explicit batches | 10 s | 8.722 | 0.015 |
| 4 KiB records, explicit batches | 10 s | 8.356 | 0.015 |
| Repeated explicit-batch control | 10 s | 7.995 | 0.012 |
| Busy polling, explicit batches | 10 s | 8.826 | 1.002 |
| Sustained explicit batches | 60 s | **8.860** | **0.014** |

These are separate observations, not a claim that small differences establish
a configuration winner. In particular, the busy-poll result overlaps previous
sleeping runs while consuming an entire core. No configuration approached
100 MB/s.

All measured cohorts fully drained: admitted and acknowledged record/byte
counts matched. The measured cohorts wrote 1,770,198,016 payload bytes in total,
plus warmup data, to the existing staging table. No table, grants, server
configuration, or host kernel settings were changed.

Raw results and reproduction configs:

- [Original binary results](results/investigation-baseline.json) and
  [initial diagnostic results](results/investigation-diagnostics.json), using
  [investigation.json](investigation.json).
- [Control results](results/investigation-controls.json), using
  [investigation-controls.json](investigation-controls.json).
- [60-second result](results/investigation-sustained.json), using
  [investigation-sustained.json](investigation-sustained.json).
- [TCP socket snapshot](results/investigation-tcp.txt).

The original executable remains in `build-investigation-baseline/` for further
A/B checks. Result files preserve exact arguments and effective configuration.

## Evidence for the bottleneck

The final 60-second run reported:

| Observation | Value | Meaning |
|---|---:|---|
| Peer initial stream window | 2,097,152 bytes | At most 2 MiB of outstanding HTTP/2 DATA credit per stream |
| Observed peak connection credit | 33,554,382 bytes | Connection credit is much larger than stream credit |
| Connection credit at cutoff | 16,842,806 bytes | Connection credit was still available |
| Stream credit at cutoff | 0 bytes | Stream credit was exhausted |
| Stream credit waiting | 59.292 / 60.000 seconds | About 98.82% of the measured interval |
| Stream replenishment size | 1,048,617–1,064,910 bytes | About 1 MiB per WINDOW_UPDATE |
| Stream replenishments including drain | 537 | Credit returned periodically while the queue remained loaded |
| TLS/socket write retry count | 0 | The transport was not blocked on socket write readiness |
| Send-budget yields including drain | 1 | The 1 MiB worker fairness budget was not the recurring gate |

TCP snapshots during runs show approximately 119–121 ms RTT. The saved snapshot
shows 119.355 ms. Roughly 1 MiB of credit per 120 ms corresponds to
**8.74 MB/s**, consistent with measured throughput. This is an explanatory
approximation, not a timestamped proof of every credit-update interval.

Even an ideal full-window-per-RTT rate at a 2 MiB window and 120 ms RTT is only
about **17.5 MB/s**, well below the requested 100 MB/s. The observed half-window
replenishment behavior delivers less. The C client's own 16 MiB receive window
controls traffic in the opposite direction (acks); increasing it cannot change
this peer-advertised ingestion credit.

Direct writes and busy polling retain the same peer-credit limitation. The
former changes buffer handling, and the latter removes scheduler sleeping from
the hot loop. Neither removes the throughput ceiling. Larger input records
also do not materially improve the byte rate.

This establishes where the client is blocked. It does **not** identify which
server/proxy hop owns the 2 MiB policy, or rule out additional downstream
limits after this one is removed. The checked-out server source is not proof
of the deployed configuration; live advertised credit is the relevant evidence.

## What needs to change for 100 MB/s

The next useful experiment needs an environment change, not more client queue
combinations:

1. Run this same one-stream benchmark from a host close to the staging endpoint
   in us-west-2. With roughly 1 MiB replenishments, an RTT below about 10 ms
   removes this particular arithmetic ceiling. It does not guarantee the goal.
2. Alternatively, have the service/proxy owner raise the effective **receive
   window for each incoming stream**. At 120 ms, 100 MB/s needs at least 12 MB
   of in-flight data; a 32 MiB stream window is a reasonable experiment allowing
   for half-window refill thresholds. Connection credit and TCP socket buffers
   must also be sufficient. Changing only the backend will not help if an
   intervening HTTP/2 terminator keeps its 2 MiB window.

Both alternatives require an explicitly selected host or coordination with the
service owner. Neither was performed here. All runs retained one uncompressed
stream, rather than treating multiple streams as a single-stream result.

From `purec-perf-bench/`, on the selected host:

```sh
./run.sh investigation-sustained.json
```

That command rebuilds both projects, mints a token for the configured table,
and saves a new result. It appends real records to staging. Keep the same corpus,
batch mode, acknowledged-throughput metric, and single-stream topology when
comparing the next result.

## Diagnostics and validation

`zbp_connection_get_diagnostics()` is an additive public API; the old config and
statistics structures retain their layouts. It reports peer credit and observed
waiting through atomics without changing TLS, HTTP/2 settings, retry policy,
queue design, or thread ownership. The benchmark links against the installed
SDK and uses no private headers.

`measurement.transport_diagnostics` contains counter deltas for measured
admission plus drain, explicit in-window flow-control seconds, lifetime peaks
and update-size extrema, and credit snapshots at cutoff. Across connections,
durations/counts sum, peaks take the maximum, and peer/current-credit limits
take the minimum. Zero (no update observed) is excluded from the minimum
WINDOW_UPDATE size. For multiple connections, summed waiting time can exceed
wall-clock time; it is not a process idle percentage.

The initial diagnostic result file used a coarse stall timer that included
the short sending intervals between credit refills. The control and sustained
files use the refined timer, stopping when credit arrives and restarting after
it is exhausted. Use those files for the waiting-time percentages above.

Validation includes four C test targets, the full local HTTP/2/TLS integration
matrix, the standalone benchmark's CLI/matrix/wrapper suites, and all four
local benchmark accounting layouts. New checks cover peer settings and credit
updates, concurrent clock snapshots, actual pending/idle flow-control predicates,
and diagnostic aggregation in the benchmark output. None of these local tests
is presented as a measurement of real service throughput.
