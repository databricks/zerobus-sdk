# Zerobus SDK — Contributor Guide

For agents **working on** the SDK. To **use** an SDK, see `README.md` and the SDK's own `README.md`. The one client-code rule is in the last section.

## Architecture you can't read from the tree

One Rust core (`rust/sdk/`), exposed through several bindings; two SDKs do **not** use it.

- **Rust-backed:**
  - C ABI / cbindgen (`rust/ffi/`, `zerobus.h`) → **Go (cgo), C++, .NET**
  - JNI (`rust/jni/`) → **Java**; PyO3 → **Python**; NAPI-RS → **TypeScript**
- **Independent** (not the Rust core): **Pure Go** (`purego/`, over grpc-go), **Pure C** (`purec/`, nghttp2/OpenSSL/libprotobuf-c). Rust-core *code* doesn't reach them, but the shared proto does.

## Commands

Run from the SDK directory.

| SDK        | Build             | Test            | Lint                 | Format               |
|------------|-------------------|-----------------|----------------------|----------------------|
| Rust       | `make build`      | `make test`     | `make lint`          | `make fmt`           |
| Python     | `make build-rust` | `make test`     | `make lint`          | `make fmt`           |
| TypeScript | `npm run build`   | `npm test`      | `cargo clippy`       | `cargo fmt`          |
| Java       | `mvn compile`     | `mvn test`      | `mvn spotless:check` | `mvn spotless:apply` |
| Go (cgo)   | `make build`      | `make test`     | `make lint`          | `make fmt`           |
| C++        | `make build`      | `make test`     | `make lint`          | `make fmt`           |
| Pure Go    | `go build ./...`  | `go test ./...` | `go vet ./...`       | `gofmt -w .`         |
| Pure C     | `make build`      | `make test`     | `make lint`          | `make fmt`           |
| .NET       | `make build`      | `make test`     | `make lint`          | `make fmt`           |

## Proto and cross-SDK contract

- Edit the shared proto (`rust/sdk/zerobus_service.proto`) once; regenerate every binding; pass the CI drift gate.
- Map record-type enums by name — numbering differs per SDK (TS is 0-based).
- Keep the schema's consumer list current.
- Rust core is the reference for defaults, units, and errors; flag deviations.

## Versioning and change classification

- Breaking changes (incl. removing/renaming public API) only in a major; deprecate first with a removal date.
- File each change in the right `NEXT_CHANGELOG.md` section.
- A silent default/behavior change is breaking — add a migration note.
- A toolchain/MSRV floor bump is breaking.
- Name the first version per SDK for a GA/graduation change.
- SDKs version independently; a core bump needs a wrapper bump only if the wrapper's API changes.

## Feature gating (new features)

- Gate a new feature when it earns it — heavy deps / raised MSRV, or server-gated (PrPr/PuPr) so opt-in is explicit. Use the SDK's native gate (Cargo feature with `default = []`, Go build tag, build flag).
- Wire the gate through every build path (Makefile, CI, release: cargo and zigbuild); a missed path ships broken.
- A Cargo feature isn't pip-selectable — Python compiles it into the default wheel (effectively always on); verify the released wheel.
- Document any toolchain floor the gate raises, at the gate.

## Public API and stability

- No third-party or generated types in a public API; own your types, keep conversions internal.
- Make public enums non-exhaustive; don't derive `Copy` on a public event.
- Use an options object or builder, not positional optional args.
- Put config on the right type (writer schema on `TableProperties`, not per-call).
- Mark unstable API Beta in code and README.

## Config and input validation

- Reject a value that would panic the core; name the minimum.
- Don't leak an internal constraint (e.g. ring power-of-two) into the public API.
- A core guard is unreachable if the wrapper iterates first — guard both (single and batch).
- Check how a wrapper's default / `0` / `-1` config value maps across the boundary to Rust — a wrapper default can silently become a different Rust value (`0` read as a real limit, a zero-valued enum as "unset").

## Errors and retry

- A deterministic/config error is non-retryable.
- Keep a terminal-status allowlist; don't treat unknown statuses as transient.
- Don't reuse the auth-rejection code for a TLS/cert failure.
- Keep the typed cause; don't synthesize over it.
- Pin error messages with a test — wrappers surface them.

## Concurrency and lifecycle

- Cancellation can leave the stream half-dead — set the closed flag before the first await; check it on every send path.
- Re-check durability before failing a wait, or a retry duplicates a durable record.
- A flush/close waits for all children; don't short-circuit on first error.
- Don't let flush and close each wait the full timeout (they compound).
- Close in order: half-close, drain to EOF, close.
- A nowait API needs a completion point, or don't ship it.

## Resources and ownership

- Close the native handle on every error path.
- Do fallible managed work before the native create; on failure, close the handle.
- Drop a reused buffer/encoder after a failure.
- A diagnostic read snapshots; it doesn't consume data a retry needs.
- Transfer callback ownership so a slow callback can't outlive its owner.

## Cross-FFI internals (Rust-backed SDKs)

- Rust owns handle memory; wrappers free/close/destroy, and free error strings via `zerobus_free_error_message()`.
- `zerobus.h` is cbindgen-generated; CI diffs it against the default build. A non-additive change breaks Go/C++/.NET — treat it as a public-API change.
- Go uses `runtime.Pinner` (keep it) and a `cgo.Handle` registry (a leaked handle leaks a Go object).
- The FFI layer runs its own tokio runtime.
- Prefer batch over single-record calls; each crossing serializes. Descriptors and Arrow IPC schemas cross as byte arrays (one-time at stream creation); Arrow Flight IPC is near-zero-copy, so don't re-encode.

## Observability

- Record the event before waking the caller, and emit at completion (not the next poll), so cancel/EOF doesn't drop it.
- The exporter callback runs in the request poll — don't block, allocate, or panic; mark a retransmit with an attempt.

## Client-side encoding

- Check field order, and nested/union/array fields.
- Convert decimal/date/timestamp/uuid/long correctly; test each type and a non-UTC `TZ`.
- Don't copy the payload twice across the binding.

## Dependencies, build, and CI

- Pin an exact version for a temporary workaround; re-run codegen to prove committed output matches.
- Build examples in CI; out-of-workspace code rots (e.g. the excluded `rust/examples/avro`).
- `push.yml` is the path-filtered gate; a `rust/**` change also runs wrapper tests against local Rust source (non-blocking).

## Docs, changelog, and release

- A user-facing change updates `NEXT_CHANGELOG.md` (template sections vary per SDK), README, `examples/`, and API docs (doc comments / Python `.pyi` stubs / JSDoc / Javadoc / godoc).
- Call breaking changes out under `### Breaking Changes` (where the template has it) with migration steps; deprecations need a replacement and timeline.
- Examples must run as written; fix a defect rather than documenting it.
- Lead every example, doc comment, and test with loop-then-`flush()`, never a per-record wait.
- Releases are per-SDK, tag-triggered — use the `zerobus-sdk-release:*` skills; finalizing moves `NEXT_CHANGELOG.md` into `CHANGELOG.md` and resets it with the next header.
- Native libs release independently: Go needs an `ffi/v*`, Java a `jni/v*`, before picking up core changes; C++ builds the FFI from source and bundles the archive.

Release details — workflows are build-only `workflow_dispatch`; the skills create registries, tags, and GitHub Releases downstream (only Pure Go's workflow checks its tag). Pure C isn't released yet.

| SDK        | Version source                                           | Registry                        | Tag             |
|------------|----------------------------------------------------------|---------------------------------|-----------------|
| Rust       | `rust/sdk/Cargo.toml`                                    | crates.io                       | `rust/v*`       |
| Python     | Cargo.toml via maturin (`python/rust/`)                  | PyPI                            | `python/v*`     |
| TypeScript | `typescript/package.json`                                | npm                             | `typescript/v*` |
| Java       | `java/pom.xml`                                           | Maven Central                   | `java/v*`       |
| Go (cgo)   | `go/version.go`                                          | pkg.go.dev (`.../go`)           | `go/v*`         |
| Pure Go    | `purego/zerobus/sdk.go`                                  | pkg.go.dev (`.../purego`)       | `purego/v*`     |
| C++        | `cpp/include/zerobus/version.hpp` + `cpp/CMakeLists.txt` | Source / CMake + GitHub Release | `cpp/v*`        |
| .NET       | `dotnet/src/Zerobus/Zerobus.csproj`                      | NuGet                           | `dotnet/v*`     |
| FFI        | `rust/ffi/Cargo.toml`                                    | GitHub Release                  | `ffi/v*`        |
| JNI        | `rust/jni/Cargo.toml`                                    | GitHub Release (feeds Java)     | `jni/v*`        |

FFI, JNI, C++, and Go ship native archives on a GitHub Release, created by the publish step outside these workflows.

## Tests

- Cover error and native paths.
- Keep a mock faithful to the server; inject codes the SDK classifies as intended.

## Review and PR process

- Carry a fix to every sibling path, including other open PRs.

## Commit conventions

- Subject: SDK scope (`[Rust]`, `[All]`), imperative, under 50 chars. DCO required (`git commit -s`); see `CONTRIBUTING.md`.

---

## For agents generating client code (usage)

- **Never wait for an ack after every `ingest`** — it forces one server round-trip per record and collapses throughput. `ingest` only queues; ingest in a loop, then `flush()` once (periodically for unbounded streams, or register an ack callback). Reserve per-record `wait_for_offset` for low-volume must-be-durable cases. See `README.md` → "Acknowledgments and throughput".
- Prefer the batch API in hot paths — it amortizes the per-call FFI crossing.
- Methods: ingest `ingest_record_offset` / `ingestRecordOffset` / `IngestRecordOffset`; wait `wait_for_offset` / `waitForOffset` / `WaitForOffset`; batch `ingest_records_offset` / `ingestRecordsOffset` / `IngestRecordsOffset`.
- Thread-safety differs: Go goroutines and Python-async/TS are safe; Java is not; Python sync is single-threaded — check the SDK's README before sharing a stream.
- Java has no finalizer — use `close()` / try-with-resources or leak JNI resources. Python/TS free on GC (refcounting), which may not flush — prefer explicit `close()`.
