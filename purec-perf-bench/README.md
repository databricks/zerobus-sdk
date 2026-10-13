# Independent pure C performance benchmark

This is a standalone consumer of the installed `zbperf` public C API. Copy this directory anywhere and build against an SDK installation; neither SDK source files nor private headers are used. Linux, C11, pthreads, CMake 3.16+, and Python 3 (matrix runner/tests) are required.

## One-command runs from this checkout

`run.sh` builds and installs the sibling `purec-perf` SDK, builds the benchmark,
mints a token, and runs your JSON matrix config. It also needs Bash, `curl`, `jq`,
and exported `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` credentials.
The script uses Release builds, disables SDK sanitizers, and enables
`compile_commands.json` in both projects' `build/` directories for IDEs.
It pins the benchmark to this SDK installation, even if an older package was
previously selected in CMake.

For one configuration, save this as `bench-config.json` and replace `HOST` and
`/absolute/path/to/checkout`:

```json
{
  "baseline": {
    "host": "HOST",
    "port": 443,
    "table": "shinkansen.default.danilo_fluentbit_poc",
    "input": "/absolute/path/to/checkout/purec-perf-bench/examples/records-1k.jsonl",
    "batch": "auto",
    "warmup": 2,
    "duration": 10,
    "drain-timeout": 60
  },
  "finalists": [{"name": "baseline", "options": {}}]
}
```

```sh
./purec-perf-bench/run.sh bench-config.json --dry-run
./purec-perf-bench/run.sh bench-config.json
# Override any matrix-run controls as needed:
./purec-perf-bench/run.sh bench-config.json --stage queues --producers 1,4 --repeats 3
```

Defaults are `--stage finalists --producers 1 --repeats 1`, with a unique UTC
timestamped `.json` output under `purec-perf-bench/results/`. Pass `--output FILE` to
choose another new file; existing files are never overwritten. Other matrix
flags are forwarded, except `--bench` and `--config`, which this wrapper owns.
The producer count always comes from the wrapper/matrix flags, not the baseline.
`--dry-run` only prints the planned commands: no builds, token minting, result
files, or ingestion. `--help` does not build or mint either.

The token's table is read from `baseline.table` in your config. Every selected
variant must keep that table and the baseline host/port, use `ZB_TOKEN` (the
default), and use TLS. This avoids sending the single table-scoped token to an
overridden endpoint or trying to use it for another table. Run separate configs
for different targets. Workspace defaults and overrides are those of
`mint_token.sh` below; choose a workspace matching the configured ingest host.
The token stays in the script's subprocess, not your parent terminal.

The script works from any working directory. Relative config, input, CA, and
explicit output paths are relative to the caller's working directory, not to
the config file; use absolute input paths for portable configs. Live runs append
real rows continuously, including during warmup. Tokens are not refreshed.

[EXPERIMENTS.md](EXPERIMENTS.md) lists the knobs, comparison stages, and an editable
results ledger. The one-record [1 KiB](examples/records-1k.jsonl) and
[4 KiB](examples/records-4k.jsonl) JSONL inputs use `x`-filled messages and match
the table above. Each JSON record is exactly 1024 or 4096 UTF-8 bytes, excluding
its trailing newline. These are synthetic size probes, not realistic corpora.

[PERFORMANCE_FINDINGS.md](PERFORMANCE_FINDINGS.md) records eleven live
single-stream runs and the measured peer HTTP/2 credit bottleneck. The sustained
run reached 8.86 MB/s, not 100 MB/s. `investigation-sustained.json` reproduces
that bounded-memory experiment without changing your `bench-config.json`.

The checked-in `bench-config.json` now approximates the Rust JSON/protobuf
single-record defaults using the C controls. [RUST_DEFAULTS.md](RUST_DEFAULTS.md)
explains the source defaults, queue/task structure, and non-equivalent settings.
Its million-scale request limits and acked payload retention can use over 1 GiB
of memory and lengthen both drains substantially; they are not proven faster.
Run that config from this directory (`./run.sh bench-config.json`) because its
`input` is `examples/records-1k.jsonl`.

## Build and installation

Build and install the SDK first, using the actual checkout path:

```sh
cmake -S /path/to/checkout/purec-perf -B /tmp/zbperf-sdk-build \
  -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/tmp/zbperf-prefix
cmake --build /tmp/zbperf-sdk-build -j
cmake --install /tmp/zbperf-sdk-build
```

Then configure this project from any location:

```sh
cmake -S /anywhere/purec-perf-bench -B /tmp/zbperf-bench-build \
  -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=/tmp/zbperf-prefix \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
cmake --build /tmp/zbperf-bench-build -j
cmake -E chdir /tmp/zbperf-bench-build ctest --output-on-failure
```

The only SDK dependency is `find_package(zbperf CONFIG REQUIRED)` and the installed `zbperf::zbperf` target. `BUILD_TESTING=OFF` removes the configure-time Python requirement. CMake resolves the SDK's transitive link dependencies. Save the SDK's configure/build log alongside results: the benchmark records its compiler/build flags and SDK/dependency versions exported by the package, but cannot inspect how an arbitrary installed SDK was compiled.

## Inputs and execution

Supply the endpoint host **without a URL scheme**, port, an existing table, and a compatible JSONL file explicitly. This project has no staging table or dataset default. Confirm the intended existing benchmark table and its schema before running staging ingestion. The benchmark executable never creates tables, obtains tokens, or alters grants. The separate token helper below can obtain a token before a run.

The input file must be a nonempty regular JSONL file. Blank lines are skipped and CRLF is supported. Every nonempty line is treated as one immutable record. NUL bytes are rejected; full JSON/schema validation is left to the service. All bytes and record views are loaded before opening connections or timing. Records cycle indefinitely, starting at a different corpus offset for each producer. The output contains corpus size, record count, and an FNV-1a fingerprint, never record contents. The fingerprint identifies repeated runs, and is not a cryptographic integrity check.

Obtain a fresh bearer token outside this program and export it in `ZB_TOKEN` (or select a different existing environment variable with `--token-env NAME`). Never put a token on the command line or in a matrix config. Tokens are not refreshed; keep complete runs within their lifetime. TLS verifies the host against the system trust store; use `--ca-file` for a specific CA. `--test-plaintext` enables h2c only for `localhost`, `127.0.0.1`, or `::1`.

```sh
/tmp/zbperf-bench-build/zbperf-bench \
  --host HOST --port 443 --table CATALOG.SCHEMA.TABLE --input /data/records.jsonl \
  --producers 4 --streams 1 --connections 1 --io-threads 1 \
  --queue mutex --batch auto --retention sent \
  --warmup 10 --duration 60 --drain-timeout 60
```

One JSON object is printed to stdout; diagnostics go to stderr. Failures exit nonzero and include `ok:false`. `--dry-run` loads and checks the corpus and configuration, prints the resolved configuration, and exits without reading a token or opening connections. `--help` lists all switches. Decimal second durations support short local tests.

Topology is explicit: stream `i` uses connection `i % connections`; producer `i` uses stream `i % streams`; the SDK assigns connections across the selected I/O owners. Require `producers >= streams >= connections >= io_threads >= 1` to avoid unused resources. Every producer runs one ingestion loop. No producer waits for a record's acknowledgment.

### Getting a token from your service principal

With your SP credentials already in `DATABRICKS_CLIENT_ID` and
`DATABRICKS_CLIENT_SECRET`, source the shell helper:

```sh
source /path/to/purec-perf-bench/mint_token.sh CATALOG.SCHEMA.TABLE
```

It uses `curl` and `jq`, defaults to the dogfood staging workspace, and exports
`ZB_TOKEN` in your current shell without displaying it. Set `ZB_WORKSPACE_URL`
and `ZB_WORKSPACE_ID` beforehand to override the workspace. Source it rather
than executing it so the exported variable remains available. It disables shell
tracing to avoid exposing the token. Tokens normally expire after one hour;
source it again to mint a fresh one. No ingestion or grant changes are performed.

## Controls

| Option | Default | Meaning |
|---|---:|---|
| `--queue` | `mutex` | `mutex` or `atomic` admission queue |
| `--batch` | `single` | Individual calls/messages, SDK `auto`, or caller `explicit` batches |
| `--batch-records` | 1024 | Automatic or explicit batch record cap |
| `--batch-bytes` | 1048576 | Encoded message cap; explicit batches reserve conservative envelope space |
| `--batch-delay-us` | 0 | SDK automatic accumulation window |
| `--retention` | `sent` | Reclaim payload after nghttp2 consumption or `acked` durability |
| `--idle` | `sleep` | `sleep`, bounded `spin`, or `busy` polling |
| `--spin-us` | 50 | Bounded-spin duration |
| `--write-coalesce-bytes` | 65536 | SDK output buffer size; 0 requests direct writes |
| `--buffer-bytes` | 67108864 | Aggregate payload-storage budget across all streams |
| `--queue-capacity` | 4096 | Aggregate request slots; each stream gets the largest power of two fitting its equal share |
| `--max-inflight` | 4096 | Aggregate outstanding-request metadata entries, divided equally |
| `--sample-every` | 1024 | SDK latency sample interval; 0 disables samples |
| `--admission-timeout-ms` | 1000 | Maximum bounded-admission wait; a timeout fails the run |
| `--warmup` | 10 | Warmup admission duration, followed by a full drain |
| `--duration` | 60 | Timed admission window |
| `--drain-timeout` | 60 | One shared deadline for flushing every stream in each phase |

The JSON config reports requested, effective aggregate, and per-stream budgets. Payload/in-flight shares round down; queue shares round down to a power of two (minimum 2). For default power-of-two producer sweeps, equal aggregate budgets remain equal across topologies. Explicit batches occupy one descriptor and retain their own request boundary. Their producer reuses a preallocated record-view array; there is no hot-loop allocation or JSON generation in the benchmark. Use sufficient payload budget for one whole explicit batch plus the SDK's allocation rounding. Admission or memory-limit failures invalidate a run rather than being counted as successful work.

## Timing and result interpretation

Both phases create producers and wait until they are ready behind a condition-variable barrier. Warmup completes and flushes before the measured baseline is captured. The main thread releases producers, measures with `CLOCK_MONOTONIC`, samples occupancy approximately every 100 ms, and sets a stop flag at the deadline. There is no clock syscall, log, formatting, or allocation per record in the benchmark loop. An ingestion call already underway at cutoff can finish while stopping; its data belongs to the final drained cohort.

After the cutoff snapshot, producers join and each stream flushes under one shared drain deadline. An acknowledgment failure, admission failure, interrupt, or incomplete final drain fails the run. SDK snapshots contain individually atomic counters; they are not transactional and the cutoff across streams has a small readout skew. Final cohort counters are stable after all producers stop and flushes finish.

| Metric | Interpretation |
|---|---|
| `acked_records_per_second`, `acked_mb_per_second` | Acks observed between the post-warmup baseline and timed cutoff, divided by the actual window duration; payload throughput in MB/s |
| `admitted_records_per_second` | Records admitted by the cutoff; this is queueing throughput, not durability throughput |
| `cohort_acked_records_per_second`, `cohort_acked_mb_per_second` | Every measured-phase record durably acknowledged, including work completing after cutoff, divided by admission plus stop plus drain time; payload throughput in MB/s |
| `producer_stop_seconds` | Elapsed time until all producers finish, including an in-progress admission call |
| `cpu_cores_in_window` | Process user+system CPU time divided by wall time; includes all SDK and benchmark threads |
| `cohort_cpu_ns_per_acked_record` | Process CPU for admission, stop, and drain, divided by cohort durable records |
| `process_peak_rss_kib_lifetime` | Linux process lifetime high-water RSS; includes corpus, startup, warmup, and stacks |
| `stream_peak_*_sum_lifetime` | Sum of per-stream lifetime maxima; those maxima need not occur simultaneously |
| `sampled_*` | Aggregate occupancy sampled during the measured window; short spikes can be missed |
| `wire_requests`, backpressure, transport counters | Post-warmup deltas covering measured admission and final drain |
| `transport_diagnostics` | Peer send-credit limits, window updates, observed flow-control waiting, and write retries; see [field semantics](PERFORMANCE_FINDINGS.md#diagnostics-and-validation) |
| `latency` | SDK sampled admission-to-ack latency for the measured cohort, excluding warmup samples |

Throughput uses decimal **MB/s: 1 MB = 1,000,000 bytes**. Raw byte counters and
byte-sized configuration limits remain in bytes, and records/s remains available.
Benchmark report `schema_version: 2` replaces the old `*_bytes_per_second` rate
fields with `*_mb_per_second`. The matrix also converts rates from older
schema-1 benchmark binaries when collecting new results; existing files on disk
are not modified.

Latency percentiles report the upper edge of a log2 microsecond histogram bucket, not an exact percentile. The last bucket saturates; its upper edge is reported as `null`. `max_us_lifetime` is clearly labeled because a cumulative maximum cannot have warmup subtracted. A zero sample count means latency estimates are unavailable, not that latency was zero. A selected admission carries its ingest-entry timestamp, including allocation/copy and admission waits. An explicit batch has one timestamp; an automatic batch keeps the earliest selected timestamp among its members. Histograms therefore contain at most one sample per wire request, not per record; consider this difference when comparing batching modes.

Record CPU placement with `taskset` if needed; the report includes the inherited allowed CPU set. For comparable runs, hold the corpus, table, aggregate memory budgets, host placement, CPU mask, build flags, dependency versions, and competing workloads constant. A local mock measures client behavior but does not establish staging throughput. Service-side saturation, table quotas, network caps, or rising backpressure can make a client comparison inconclusive. Never select a winner from one run or from admission throughput alone.

## Staged matrix runner

`run_matrix.py` uses only the executable and its public JSON output. Each stage varies a small set of related controls. Repetitions are interleaved in a reproducibly shuffled order; it does not run a full Cartesian product or automatically promote winners.

Prepare a JSON file using the actual existing host/table and absolute corpus path:

```json
{
  "baseline": {
    "host": "HOST",
    "port": 443,
    "table": "CATALOG.SCHEMA.TABLE",
    "input": "/data/records.jsonl",
    "queue": "mutex",
    "batch": "single",
    "streams": 1,
    "connections": 1,
    "io-threads": 1,
    "buffer-bytes": 67108864,
    "queue-capacity": 4096,
    "max-inflight": 4096,
    "warmup": 10,
    "duration": 60,
    "drain-timeout": 60
  }
}
```

```sh
python3 /anywhere/purec-perf-bench/run_matrix.py \
  --bench /tmp/zbperf-bench-build/zbperf-bench --config /data/experiment.json \
  --stage queues --dry-run

python3 /anywhere/purec-perf-bench/run_matrix.py \
  --bench /tmp/zbperf-bench-build/zbperf-bench --config /data/experiment.json \
  --stage queues --output /data/queue-results.json
```

Dry run only reads config and prints exact argv arrays; it creates no result files and runs no subprocesses. Execution first invokes benchmark dry-run for every variant before ingestion, then creates a **new** result file (existing files are never overwritten). The file is an indented JSON array, including for a single run, and each completed or failed run is added and flushed immediately. It remains readable between runs. Summary JSON on stdout includes medians, minima/maxima, relative range, and failure counts; raw results retain timestamps, effective config, seed, and exit status.

Saved results are JSON, not JSONL: readers should load the whole array (for
example, `jq '.[0].result.measurement' results/RUN.json`) rather than parsing one
run per line. Record input files remain JSONL. The standalone C executable still
prints a compact JSON object; the matrix handles formatting outside the measured
workload.

Use stages in order, updating the baseline manually based on measurements:

1. `queues`: mutex/atomic with producer counts 1, 2, 4, 8, 16.
2. `batching`: individual, explicit, automatic 1 MiB/64 KiB, and a 50 us accumulation experiment.
3. `coalescing`: direct transport writes versus 64 KiB/256 KiB output buffers, holding protocol batching fixed.
4. `topology`: shared stream, streams sharing one connection, separate connections; all use one I/O owner to isolate placement.
5. `ownership`: separate connections, shared owner versus one owner per connection.
6. `retention`, then `idle`: compare each policy on the strongest selected baseline.
7. `finalists`: add `"finalists": [{"name": "candidate-a", "options": {"queue": "atomic", "batch": "auto"}}, ...]` to the config; selected combinations are tested together.

Screening defaults to three repetitions; finalists to five. `--producers 4,8` selects a smaller sweep; select counts compatible with the baseline's stream count. `--repeats` and `--seed` override defaults. Finalist options inherit the baseline and may override any public switch except `producers`. The entire matrix may outlive a one-hour token; split stages into shorter invocations and mint fresh tokens between them. Automatic token refresh is deliberately absent.

## Verification

CTest checks configuration validation, JSON escaping, corpus parsing, missing-token handling, bounded aggregate capacities, matrix scheduling, topology constraints, secret-option rejection, and exclusion of failed runs from summary statistics. These checks do not contact a workspace. SDK integration tests exercise the public API against local protocol/TLS fixtures; run short benchmark smoke tests against those fixtures to verify the executable's acknowledgment accounting. Sanitizer builds belong in correctness validation, never performance comparisons.
