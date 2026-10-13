# Unreleased

### Internal Changes

- Coalesce normal capacity notifications across all DATA and ACK callbacks in
  one connection service turn, flushing pending notifications on every return
  path before readiness polling. Immediate barrier/error notifications remain.
- Distinguish allocation and queue-space admission waits, so DATA encoding does
  not wake producers waiting for ACK-retained byte capacity. Pair waiter
  registration and notification with fences to prevent missed wakeups under
  the C11 memory model.
- Coalesce admission-capacity notifications within each outgoing DATA callback
  and cumulative acknowledgment, reducing producer wakeups during reclamation.
  Barrier completion, errors, and cancellation still notify immediately.
- Replace the payload slab pool and its mutex/eviction scans with exact-size
  allocations bounded by atomic byte reservations. `buffered_bytes` now counts
  exact reserved payload bytes instead of rounded size-class capacity. Payload
  allocation failure returns `ZBP_NOMEM` immediately after admission rollback.

### New Features

- Read-only peer HTTP/2 credit, flow-control time, socket/TLS retry, and send-budget
  diagnostics through a separate API, preserving existing statistics layouts.
- Experimental C11 ingestion SDK with concurrent copied-input APIs, flush and
  acknowledgment progress, bounded queues, protocol batching, and configurable
  payload retention, connection placement, and I/O scheduling.
- Installable CMake package and independent benchmark consumer.
- Small shell helper to mint and export a table-scoped SP token before a run.
- Example shell script to build, mint a staging token, and ingest sample JSON.
- Config-driven benchmark wrapper builds both projects with compilation databases,
  mints a token scoped to the configured table, and saves uniquely named results.
- One-record 1 KiB and 4 KiB JSONL inputs for synthetic size comparisons.

### Benchmark Output Changes

- Report peer HTTP/2 credit and replenishment sizes, observed flow-control
  waiting, and transport retries in `measurement.transport_diagnostics`.
- Save matrix results as indented JSON arrays (`.json`) instead of one-line
  JSONL entries. Each completed run is flushed; existing result files are untouched.
- Report throughput in decimal MB/s (1 MB = 1,000,000 bytes). Experimental
  benchmark report schema 2 uses `acked_mb_per_second` and
  `cohort_acked_mb_per_second` instead of byte-per-second fields; record rates and
  raw byte counters are unchanged. The matrix converts older binary reports.

### Documentation

- Pool-removal and admission-wakeup investigation with separate real-service
  and receipt-only C/Rust comparisons, validation, and current-source reruns.
- Eleven-run single-stream staging investigation, retained result files and
  reproduction configs, and the external peer-credit limit on the 100 MB/s goal.
- Implementation and synchronization guide, benchmark methodology, and deferred
  split-I/O and task-pool experiments.
- Benchmark controls and comparison-stage catalog with editable result and
  preferred-option tables.
- Rust JSON/protobuf defaults and pipeline comparison, with a config-only C
  benchmark approximation and explicit capacity, timeout, and memory caveats.
