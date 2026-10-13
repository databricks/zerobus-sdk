# Validation and performance findings

For the October 9 pool-removal and wakeup changes, see the
[optimization report](performance-20261009.md). The dated findings below are
preserved as historical observations, not the latest validation status.

## Scope and environment

Local correctness validation was performed on 2026-09-22, on Linux x86_64 with
GCC 10.5.0, CMake 3.28.3, nghttp2 1.40.0, OpenSSL 1.1.1f, and Python 3.6.15.
Release builds use strict `-Wall -Wextra -Wpedantic -Werror`; sanitizer builds
use Debug. The example application is built with the SDK.

The only source additions are `purec-perf/` and `purec-perf-bench/`. Published
SDK APIs, the production C SDK, and the old POC are unchanged. No commit, push,
table creation, grant update, or staging ingestion was performed.

## Checks

| Check | Result |
|---|---|
| Release CTest | Queue, protobuf, and pool passed; final state-test recheck was killed before `main` (qualification below). |
| ASan + UBSan unit tests | 4/4 passed, including leak detection. |
| TSan unit tests | 4/4 passed without suppressions. |
| Release protocol/TLS integration | Full 41/41 matrix passed. |
| ASan + UBSan protocol/TLS integration | Reduced 23/23 matrix passed. |
| TSan protocol/TLS integration | Reduced 23/23 matrix passed with the external-library qualification below. |
| Independent benchmark build | Source copied outside checkout, linked only to installed SDK. |
| Benchmark CLI and matrix tests | 2/2 CTest suites passed. |
| Benchmark acknowledgment accounting | Four stream/connection/owner layouts passed against a local peer. |

The integration fixture independently decodes protobuf/gRPC envelopes and
checks table metadata, contiguous request offsets, record contents, and
admission/acknowledgment totals. Eight producers share four streams; caller
input is overwritten immediately after admission to check copied ownership.
The matrix covers both queues, all batching modes, both retention policies,
shared/separate connections and owners, direct/coalesced output, and idle modes.
Additional cases cover fragmented replies, small HTTP/2 windows, duplicated and
impossible acks, delayed acks after flush timeout, abort under backpressure,
reset, bad trailers, truncated responses, and certificate rejection.

The `full_duplex` case combines large requests, padded valid acknowledgments,
enlarged HTTP/2 windows, and constrained socket buffers. The `stalled_flush`
case consumes the entire stream send window, acknowledges the request without
restoring that window, and requires subsequent flushes to complete.
`test_state.c` additionally injects exact queue/TLS interleavings without
production test hooks. Its SSL calls are scripted; its nghttp2 session is real.
Actual OpenSSL interoperability is covered by the separate TLS fixture.

The four standalone benchmark accounting layouts are mutex/single with one
stream, atomic/auto with two streams on one connection, atomic/explicit with
two connections on one owner, and mutex/auto with two connections/two owners.
Each checks that the fixture's record total equals warmup plus completed
measured cohort, and that measured admitted and acknowledged totals match.

### Release state-test qualification

The Release state test passed during development, but final revalidation
consistently receives SIGKILL (exit 137) during dynamic-loader startup, before
test execution. A fresh byte-identical Release build behaves the same way,
including with `LD_LIBRARY_PATH` unset. `strace` places the kill before `main`;
RSS was approximately 3.9 MiB, available host memory approximately 69 GiB, and
cgroup OOM-kill/failure counters were zero. The cause remains unexplained.
No source changes or security-setting changes were made to evade the kill.
Therefore the final Release CTest suite is **not** reported fully passing.
The same final state-test source passes ASan/UBSan and TSan, and the Release
SDK passes all actual socket/TLS integration cases. Recheck this test on another
host before treating the Release unit suite as clean.

### TSan qualification

An unsuppressed TLS integration run reported a lock-order cycle entirely within
the host's uninstrumented OpenSSL `CRYPTO_THREAD_*` implementation. No SDK lock
was present in that reported cycle. Its underlying cause is not established;
this is not an unqualified claim that the report is a false positive.

The optional `tests/tsan-openssl.supp` suppresses deadlock reports matching
`CRYPTO_THREAD_write_lock`. This leaves data-race detection enabled and SDK-only
lock cycles unsuppressed; it can hide any deadlock report containing that
external frame. It is not a production setting. Unit tests need no suppression.
Repeat TLS validation without it when using a different/instrumented OpenSSL.

### Fixes established by validation and review

- Corrected aligned allocation for cache-line-aligned stream state and slab
  payload storage after UBSan checks.
- Kept flush ordering in queue barriers instead of racing progress snapshots.
- Fixed a concurrent publication race where a barrier arriving after an empty
  drain could be selected as a payload. Deterministic tests cover both queues
  and immediate/deferred acknowledgment completion.
- Preserved immutable TLS retries while allowing receive progress under write
  WANT_WRITE. Tests cover both output modes and WANT_READ/WANT_WRITE transitions.
- Allowed already-satisfied barriers to progress while another stream's TLS
  operation remains blocked; tests cover both queues and all retry directions.
- Drained head barriers independently of HTTP/2 DATA callbacks and send windows.
- Handled remote END_STREAM with incomplete responses, worker-local SIGPIPE,
  buffered TLS receive progress, and partial consumption of large output frames.

The SDK development skill's review/analysis loop confirmed two blockers in
cycle 1 and one in cycle 2; all three were fixed and given regression coverage.
Review converged in cycle 3 with zero findings. No warnings, nits, or retained
false positives were reported. No reference-document inaccuracies required
changes. `NEXT_CHANGELOG.md`, README, the example, and API comments are updated.

## Reproduction

From the checkout root, with the fixture requirements installed:

```sh
cmake -S purec-perf -B /tmp/zbperf-sdk-build -DCMAKE_BUILD_TYPE=Release
cmake --build /tmp/zbperf-sdk-build -j4
cmake -E chdir /tmp/zbperf-sdk-build ctest --output-on-failure
python3 purec-perf/tests/integration.py \
  --client /tmp/zbperf-sdk-build/zbp_integration_client

cmake -S purec-perf -B /tmp/zbperf-asan -DCMAKE_BUILD_TYPE=Debug \
  -DZBPERF_SANITIZE=address,undefined
cmake --build /tmp/zbperf-asan -j4
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1 \
  cmake -E chdir /tmp/zbperf-asan ctest --output-on-failure
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1 \
  python3 purec-perf/tests/integration.py \
  --client /tmp/zbperf-asan/zbp_integration_client --quick

cmake -S purec-perf -B /tmp/zbperf-tsan -DCMAKE_BUILD_TYPE=Debug \
  -DZBPERF_SANITIZE=thread
cmake --build /tmp/zbperf-tsan -j4
TSAN_OPTIONS=halt_on_error=1 \
  cmake -E chdir /tmp/zbperf-tsan ctest --output-on-failure
TSAN_OPTIONS=halt_on_error=1 python3 purec-perf/tests/integration.py \
  --client /tmp/zbperf-tsan/zbp_integration_client --quick
```

If the documented OpenSSL report occurs, rerun the last command with
`TSAN_OPTIONS=halt_on_error=1:suppressions=/absolute/checkout/purec-perf/tests/tsan-openssl.supp`
and retain that qualification alongside the result. The host's CTest 3.16
ignores `--test-dir`, so commands deliberately set its working directory.

Actual validation artifacts reside under `/tmp/zbperf-validation.wkizkj/`:
`asan`, `tsan`, `install`, `external-benchmark`, and `bench-build`. PyPI DNS was
unavailable; the fixture used checked-out h2 v4.1.0, hpack v4.0.0, and hyperframe
v6.0.1 via
`PYTHONPATH=/tmp/zbperf-validation.wkizkj/h2/src:/tmp/zbperf-validation.wkizkj/hpack/src:/tmp/zbperf-validation.wkizkj/hyperframe/src`.
These dependencies are temporary validation inputs, not vendored SDK code.

Build the external consumer using the two READMEs, then validate its accounting:

```sh
python3 purec-perf/tests/benchmark_integration.py \
  --bench /path/to/external-benchmark-build/zbperf-bench
```

## Performance status and next experiment

The staging benchmark table and realistic JSONL corpus remain unspecified.
No staging ingestion has been performed and there is no measured staging
performance winner yet. Local fixture throughput is not representative of
service throughput.

Potential table names found in existing Shinkansen load-test scripts are
`shinkansen.default.click_bench_managed_2` and
`shinkansen.default.click_house_bench_managed_2`. These are discovery leads only,
not confirmed targets or permission to write to those tables.

Once the target and compatible JSONL data are confirmed, run the independent
runner's staged queue, batching, coalescing, topology, ownership, retention,
idle, and finalist comparisons. Keep aggregate budgets and CPU placement fixed,
save raw results/build metadata, and acquire fresh tokens outside the SDK.
Use acknowledged rates plus CPU cost and drain time to choose the next design
experiment. The split sender/receiver and task-pool alternatives are documented
in `implementation.md`; neither is implemented or presumed faster.
