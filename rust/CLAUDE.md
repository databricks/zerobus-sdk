# Rust SDK — Contributor Guide (core)

The core SDK; every Rust-backed SDK binds it:

- Go, C++, .NET — via the C FFI (`rust/ffi/`).
- Java — via JNI (`rust/jni/`).
- Python, TypeScript — via PyO3 / NAPI-RS.

Root `CLAUDE.md` has the shared rules; this is the Rust-core delta. The JSON/protobuf engine is in `sdk/src/stream/grpc/`; Arrow Flight is in `sdk/src/stream/arrow/` (feature `arrow-flight`).

## Build and test

From `rust/`: `make build` / `build-release`, `build-ffi` (rebuilds `zerobus.h`), `build-jni`, `lint`, `fmt`, `check`. Not obvious:

- **`make test` is a feature matrix.** Workspace minus SDK+JNI, then SDK `--all-features`, then SDK `--no-default-features`, then `zerobus-jni --all-features`. Plain `cargo test --workspace` skips `avro` and no-default-features.
- **`examples/avro` isn't in CI** (workspace-excluded). Check it with `cd examples/avro && cargo test`.
- **A `tests/src/` file runs only if listed as `[[test]]`** in `tests/Cargo.toml`; otherwise it builds only when a registered target `mod`s it.

## FFI and JNI rules

- **Regenerate the header after FFI changes.** `ffi/build.rs` writes `zerobus.h`. After editing `rust/ffi/src`, run `cargo build -p zerobus-ffi` and commit it. A non-additive change breaks Go/C++/.NET — treat it as public-API.
- **Don't let rustfmt reorder `ffi/src/lib.rs`.** The comments between `mod` lines block alphabetizing; that order is the header's declaration order.
- **Keep the JNI ABI version in sync.** Bump `NATIVE_ABI_VERSION` (`jni/src/sdk.rs`) and `EXPECTED_NATIVE_ABI` (`NativeLoader.java`) together on any native-signature change; a mismatch is rejected at load.
- **Leave `panic = "unwind"` (root `[profile.release]`).** `panic = "abort"` turns the FFI `catch_unwind` guards into process aborts, and Cargo ignores member-crate profiles.
- **No ambient runtime** — the FFI and JNI crates each run their own tokio runtime.

## Public API and breaking changes

A public-surface change reaches: `pub` items → Rust consumers; FFI signatures/structs → Go/C++/.NET; JNI methods → Java; PyO3 (`python/rust/src/lib.rs`) and NAPI (`typescript/src/lib.rs`) wrap the SDK directly, so refactors can break them via changed paths/types.

- **New trait methods need a default** — users implement `HeadersProvider`, `AckCallback`, `TlsConfig` externally.
- **Additive-safe:** new `pub` items, new `#[non_exhaustive]` enum variants / struct fields, feature-gated modules. Don't derive `Copy` on a public event.
- **Deprecate before removing:** `#[deprecated(since, note)]`, keep ≥1 minor, remove next major.

(Root owns the general semver rule.)

## Feature flags

Off by default unless noted; these constraints aren't visible in `Cargo.toml`:

- `arrow-flight` — Arrow Flight ingestion.
- `avro` — Avro (Beta). Ephemeral streams only; raises MSRV (apache-avro).
- `eos` — persistent streams (in dev). JSON + compiled protobuf only; no Arrow, Avro, dynamic protobuf, or multiplexing.
- `pat-auth` — personal access token auth (Beta, private preview; needs server-side enablement). No new deps or MSRV change. Gates `StreamBuilder::personal_access_token`, `PatHeadersProvider`, and, with `eos`, `PersistentStreamBuilder::personal_access_token`.
- `zeroparser` — zero-copy protobuf parser; opt-in (`sdk/src/zeroparser/`).
- `internal-arrow-c-data` — wrapper-only C Data importer; unstable, don't depend on it.
- `webpki-version-floor` — **on by default.** A `rustls-webpki`/`webpki-roots` floor distrusting compromised CAs (crates not imported). Bindings must keep it on; a `default-features = false` build must restore the distrust itself.
- `test-hooks` — test-only; not in shipped artifacts (but a whole-workspace `cargo build` enables it via the tests crate).
- `testing` — test utils; the FFI crate enables it, so testing-gated items reach native artifacts.

**MSRV:** declared `1.70` is stale/unenforced — the default graph needs ~1.83, and `arrow-flight`/`avro` need 1.88. Not CI-gated; check before changing.

## Errors, lifecycle, performance

- **Classify every new error** — `is_retryable()` (`errors.rs`) flows through the FFI (`CResult.is_retryable`) to all wrappers.
- **Drop doesn't flush** — `Drop` aborts tasks and discards unacked data. `close()`/`flush()` first; check any wrapper/FFI path that drops a live stream.
- **Ingest in a loop, then `flush()` once.** Never `wait_for_offset()` per record (a round-trip each). The watermark is monotonic, so waiting on the last offset confirms the run; or use an `ack_callback`. (Root has the cross-SDK rule.)

## Changelog, docs, release

- **User-facing change:** update `rust/NEXT_CHANGELOG.md`, `rust/README.md`, `rust/examples/`, and `///` docs (they render on docs.rs). (Root owns the general rules.)
- **Releases:** version in `rust/sdk/Cargo.toml`. Go and C++ rebuild the FFI from source (no `ffi/v*` wait; that release is for external C/FFI consumers); Java consumes the JNI build (`release-jni`). Coordinate when a core change touches FFI signatures or wrapper behavior.

## Arrow Flight

See `sdk/src/stream/arrow/README.md` for recovery/ACK invariants — notably: don't reorder the `biased` select arms without a deterministic race test, and use `test-hooks` barriers / paused time (not sleeps) for timing tests.
