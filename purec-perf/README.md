# Pure C ingestion performance POC

An experimental C11 Zerobus client for studying concurrent ingestion, batching,
buffer lifetime, and connection placement. It uses pthreads, C atomics, nghttp2,
and OpenSSL. It is independent of the production `purec/` SDK and does not
provide reconnection, replay, OAuth acquisition, or token refresh.

The implementation guide is [docs/implementation.md](docs/implementation.md).
It describes ownership, synchronization, and the actual data path. Experimental
results and their limitations belong in [docs/findings.md](docs/findings.md).
The [October 9 optimization report](docs/performance-20261009.md) covers pool
removal, producer wakeup changes, C/Rust comparisons, and current-source reruns.

## Build and install

Linux, a C11 compiler, CMake, pkg-config, OpenSSL development headers, nghttp2
development headers, and pthreads are required. On Ubuntu, the library packages
are `libssl-dev` and `libnghttp2-dev`.

```sh
cmake -S purec-perf -B purec-perf/build -DCMAKE_BUILD_TYPE=Release
cmake --build purec-perf/build -j
cmake -E chdir purec-perf/build ctest --output-on-failure
cmake --install purec-perf/build --prefix /tmp/zbperf-install
```

The install exports `zbperf::zbperf` from `find_package(zbperf CONFIG REQUIRED)`
and installs `<zbperf.h>`. Public types contain no pthread, atomic, nghttp2, or
OpenSSL types. C++ callers can include the public header.

The [benchmark](../purec-perf-bench/README.md) is a separate project. It uses only
the installed package and public API. Copy that project elsewhere and pass the
installation prefix through `CMAKE_PREFIX_PATH`; it needs no SDK source tree.

## Ingestion

Create a runtime, connection, and stream, then queue records and flush once:

```c
#include <zbperf.h>
#include <stdlib.h>

/* After opening a stream, checking each returned zbp_status: */
static zbp_status ingest_records(zbp_stream *stream,
                                const zbp_record *records, size_t count) {
    for (size_t i = 0; i < count; ++i) {
        zbp_status status = zbp_ingest(stream, records[i].data, records[i].size);
        if (status != ZBP_OK) return status;
    }
    return zbp_flush(stream, 60000);
}
```

Use `zbp_ingest_batch()` when the caller already has a batch. Input bytes are
copied before successful return. Ingest and flush are safe for concurrent calls
on a stream. No producer registration is necessary. Admission may wait for
bounded queue, memory, or outstanding-request capacity; it does not wait for
each individual record's acknowledgment.

Payloads use exact-size `malloc`/`free` allocations within each stream's
`buffer_bytes` budget. An atomic byte reservation enforces that bound before
allocation; there is no payload slab pool or pool mutex. `buffered_bytes` counts
these reserved bytes, including JSON length prefixes for explicit batches.
Allocator overhead and non-payload storage are outside this budget. Exhausting
the byte budget applies admission backpressure; allocator failure returns
`ZBP_NOMEM` with no records admitted.

`zbp_flush()` inserts a barrier and waits for acknowledgment of preceding work.
`zbp_stream_get_stats()` provides nonblocking progress snapshots. Stop and join
producer threads before `zbp_stream_close()`. Abort on an unsuccessful run, stop
all API users, and call `zbp_runtime_destroy()` to free its connections and
streams. Close preserves handles for final statistics; destruction frees them.

`zbp_connection_get_diagnostics()` provides peer send-window limits, observed
flow-control waiting, transport retry counts, and I/O fairness-budget yields for
throughput investigations. It is read-only and does not change protocol behavior.

Supply a fresh bare token through `zbp_connection_config.token`, normally from
an environment variable in the application. TLS verifies the certificate and
host by default. `ca_file` adds an explicit trust source for test environments.
`test_plaintext` is an explicit loopback-only h2c mode, never an automatic TLS
fallback. Tokens are neither printed nor included in benchmark output.

To obtain that token using your existing service principal, use the standalone
[token helper](../purec-perf-bench/README.md#getting-a-token-from-your-service-principal).
It exchanges environment-provided SP credentials for a table-scoped OAuth token
before you start the application; OAuth acquisition remains outside the SDK.

The API is experimental and versioned separately from published SDKs. Options,
limits, error semantics, and deferred alternatives are documented in the
implementation guide.

`examples/ingest.c` is a complete small application. Its `zbp_ingest_example`
target accepts `HOST PORT TABLE 'JSON_RECORD' [COUNT]`; supply `ZB_TOKEN` in the
environment and a JSON record matching the existing table. It demonstrates
automatic batching and the queue-then-flush pattern.

For the staging table `shinkansen.default.danilo_fluentbit_poc`, run
`./purec-perf/examples/run_ingest.sh` from the checkout root. With your SP
credentials already exported, it builds the example, mints a token, and sends
one built-in JSON record (`id`, `name`, `level`, `msg`, `region`). Pass a count,
for example `./purec-perf/examples/run_ingest.sh 1000`, to send that many copies.
This writes records to the table; it does not create or alter it.

## Additional verification

Build separate trees with `-DZBPERF_SANITIZE=address,undefined` and
`-DZBPERF_SANITIZE=thread`; never use sanitizer binaries for performance results.
The integration fixture requires Python 3 with `h2>=4,<5` and the `openssl`
command. It uses loopback and temporary self-signed certificates; it does not
need staging credentials.

```sh
python3 purec-perf/tests/integration.py \
  --client purec-perf/build/zbp_integration_client
```

The standalone benchmark and staging setup require an explicitly chosen table
and JSONL dataset. No table is created, altered, or selected implicitly.
