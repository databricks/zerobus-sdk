# TypeScript SDK

Node.js wrapper around the Rust core via NAPI-RS.

## Client code patterns

When writing or reviewing client code that uses this SDK, prefer the idiomatic
ingest-then-flush flow. `ingestRecordOffset()` (and `ingestRecordsOffset()`) resolves as
soon as the record is queued — the SDK sends it and tracks its acknowledgment in the
background. Ingest in a loop, then call `flush()` once to confirm durability (once for a
bounded batch, or periodically for a long-running stream).

Each ingest returns the record's offset, and `waitForOffset(offset)` confirms a specific
record when one must be acknowledged before continuing (acks are ordered, so the last
offset confirms the whole run); prefer `flush()` for bulk. Ordinary streams have no ack
callbacks — their async confirmation tools are `flush()` and `waitForOffset()`.

The one thing to avoid is calling `waitForOffset()` after every record in a tight loop,
since that limits throughput to one record per round-trip.

Multiplexed streams (Beta, `streamBuilder()...multiplexed(n)`) differ: ingest returns an
unordered message ID, so `waitForMessageId(id)` confirms only that record and the last ID
does not confirm the run — use `flush()`. They are the only streams with an ack callback
(`multiplexed(n).ackCallback({ onAck, onError })`); keep it trivial in examples.

## Structure

```
typescript/
├── src/lib.rs        # NAPI-RS bindings (Rust ↔ Node.js bridge)
├── src/multiplexed.rs # NAPI-RS bindings for multiplexed streams (Beta)
├── index.js/.d.ts    # NAPI-generated native binding (gitignored build output)
├── wrapper.js/.d.ts  # Public entry point: thin JS wrapper over the native binding; encodes
│                     #   Avro record objects (via the optional `avsc` dependency)
├── stream_builder.js # Internal: streamBuilder() builders and the mux ack-callback dispatcher
├── avro.js           # Internal: Avro record-object encoding
├── Cargo.toml        # Rust crate config for NAPI-RS
├── package.json      # npm package config (main → wrapper.js)
├── test/             # Node.js test runner tests
├── examples/         # Usage examples
├── schemas/          # Proto schema files for examples
└── utils/            # Helper utilities
```

The native addon compiles to a platform-specific `.node` binary. NAPI-RS generates TypeScript
type definitions automatically. The package entry point is `wrapper.js` (not the generated
`index.js`): it encodes Avro record objects to bytes, adds `streamBuilder()`, and makes a
multiplexed stream's `close()` wait until its ack callbacks have run; everything else is
forwarded unchanged. `wrapper.d.ts` is hand-written: update it with every wrapper change.
The `avro` and `arrow-flight` Cargo features are off by default for a from-source `npm run build`,
but CI and release builds enable both (`npm run build:all`), so published binaries include them.

## Build commands

Run from `typescript/`:

- `npm install` — Install dependencies
- `npm run build` — Release build
- `npm run build:debug` — Debug build (faster iteration)
- `npm run build:arrow` / `npm run build:avro` — Same builds with Arrow Flight / Avro
- `npm run build:all` — Release build with both Arrow Flight and Avro (what CI and release use)
- `npm test` — Run all tests
- `npm run test:unit` / `npm run test:integration` — Targeted test runs
- `cargo fmt --all` and `cargo clippy --all-targets --all-features` — Lint/format Rust code

## FFI boundary: NAPI-RS

NAPI-RS bridges Rust types to JavaScript with automatic type conversions. Key considerations:

- **Memory**: NAPI external objects are ref-counted by Node.js GC. Cleanup triggers Rust `Drop`. No explicit free needed, but `close()` should be called to flush.
- **JS callbacks** (headers provider, mux ack callback) cross as threadsafe functions. A JS throw inside one is a fatal Node exception, so the JS side must catch everything (`stream_builder.js` does for ack callbacks). The mux ack TSFN is unref'd while the stream is open so it never keeps Node alive, and re-ref'd during close (`src/multiplexed.rs`).
- **Async**: Rust futures become JavaScript Promises. The tokio runtime runs on a separate thread pool — Node.js event loop is never blocked.
- **Payload conversion**: The binding coerces payloads by the stream's record type:
  - Proto / JSON streams: `Buffer` → proto bytes, `string` → JSON, objects with `.encode()` →
    protobuf message (calls encode, passes bytes), plain objects → JSON.stringify'd.
  - Avro streams: `Buffer` → Avro datum. Record objects are encoded to bytes JS-side (by the
    wrapper, via `avsc`) before they reach the native binding — the native layer only sees bytes.
- **Error mapping**: `ZerobusError` struct with `message` and `isRetryable` fields, thrown as JS exceptions.

## Breaking change rules

The public API is what TypeScript consumers see — the generated `.d.ts` types and exported classes:

- Removing or renaming exported classes/methods is breaking.
- Changing a method signature (parameter types, return type) is breaking.
- The NAPI-RS binding layer (`src/lib.rs`) is internal; refactoring is safe as long as the JS-facing API is preserved.
- Deprecate by adding JSDoc `@deprecated` annotations and console warnings before removal.

## Performance notes

- NAPI-RS has low overhead for Buffer payloads (shared memory, no copy).
- String payloads (JSON) require UTF-8 validation at the boundary.
- The automatic `JSON.stringify` path for plain objects adds serialization cost — encourage users to pass pre-serialized strings or Buffers for hot paths.
- Batch ingestion amortizes the JS→Rust crossing overhead.

## Changelog and documentation

- Every PR must update `typescript/NEXT_CHANGELOG.md` under the appropriate section if it changes user-facing behavior.
- Update `typescript/README.md` if the change affects usage, setup, or API surface.
- Add or update examples in `typescript/examples/` for new or modified APIs.
- NAPI-RS auto-generates `.d.ts` type definitions — verify the generated types after API changes.

## Release

- Version source: `typescript/package.json` (`"version": "x.y.z"`).
- Tag: `typescript/v<semver>` → triggers `release-typescript.yml` → builds Linux and Windows `.node` binaries with Arrow Flight and Avro (`npm run build:all`) → publishes to npm.
- macOS `.node` binaries are supplied via the `macos-binaries-release` workflow input. They must be built with `npm run build:all`; `npm run build` omits both features.
- On version bump PR: update version in `package.json`, move `NEXT_CHANGELOG.md` contents to `CHANGELOG.md`, reset `NEXT_CHANGELOG.md`.

## Platform targets

Pre-built binaries for: Linux x86_64, Linux aarch64, macOS x86_64, macOS arm64, Windows x86_64.
Node.js >= 16 required.
