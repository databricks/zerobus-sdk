# Zerobus SDK — Layer-1 Conformance Testing Infrastructure

**Status:** design.

## 1. Purpose & scope

One shared, behavioral conformance suite that drives **every open-source Zerobus SDK**
(Rust, Python, TypeScript, Java, Go — **not** Scala) against a purpose-built mock server
that speaks the real Zerobus wire protocol over real TCP. Because each SDK runs as a real
process over a real socket, the suite catches what in-process unit mocks cannot: wire-shape
correctness, TLS/streaming/backpressure, recovery, and **cross-SDK consistency** — the
headline value: a fix or feature can't land in one SDK and be silently forgotten in another.

This is **Layer 1** only (structured, human-authored, deterministic scenarios). Layer 2
(property/chaos) and Layer 3 (happy-path smoke vs a real server) are out of scope.

**Build model:** everything is built from **this checkout / HEAD** (local-first). This is
the same "build from HEAD" behavior the existing `cross-sdk-*` CI jobs use. Running against a
pinned released SDK version is a later, local-only extension.

## 2. Processes & planes (overview)

Three independent processes. **No process links against another's code.** Two *control*
planes (both owned by the orchestrator) plus one *data* plane (the thing under test).

```
            ┌───────────────────────────────────────────────────────────┐
            │  Orchestrator (Python, asyncio)  —  the only "brain"        │
            │  expands the matrix, reads scenarios, drives both sides,    │
            │  runs assertions, collects results                          │
            └───────┬───────────────────────────────────────┬───────────┘
    TestHarness gRPC │ (control)                   (control) │ MockAdmin gRPC
                     ▼                                        ▼
            ┌─────────────────┐   real Zerobus gRPC   ┌───────────────────┐
            │  Harness (per   │   OR Arrow Flight     │  Mock server      │
            │  SDK subprocess)│   ===================▶│  (Rust)           │
            │  links the real │   DATA plane — the    │  Zerobus + Flight │
            │  SDK; translates│   protocol under test │  data planes;     │
            │ TestHarness RPCs│                       │  MockAdmin;        │
            │  into SDK calls │                       │  captures + injects│
            └─────────────────┘                       └───────────────────┘
```

- **Orchestrator** (Python): parses CLI, expands `(sdk, transport, format, scenario)` into a
  run list, builds/launches the mock + harness subprocesses, parses each one's endpoint from
  stdout, opens the two control channels, executes each scenario's steps, asserts, tears down.
- **Harness** (one thin binary per SDK): a gRPC server exposing the language-neutral
  `TestHarness` protocol; it maps each RPC onto the real SDK's API. The **only** code written
  per SDK. It is *stateless with respect to fixtures* — it only ever sees what the
  orchestrator sends over `TestHarness`; it never reads `conformance/schemas/`.
- **Mock server** (Rust): hosts the Zerobus data plane, the Arrow Flight data plane, and the
  `MockAdmin` control plane on one auto-assigned `127.0.0.1` port.

The **data plane** (harness's SDK → mock) is the real protocol and *is the thing being
tested*. The two **control planes** are how the orchestrator drives and inspects.

## 3. Mock server — single source of truth

The repo already contains two mature mock implementations used by the Rust integration tests:
`rust/tests/src/mock_grpc.rs` (`impl Zerobus`: ephemeral/persistent/retire) and
`rust/tests/src/mock_arrow_flight.rs` (`impl FlightService`: `do_put`). They already carry
fault injection, ack-gating, counters, TLS, and reset.

**Decision: the mock lives *once*, here, and existing tests import it.** `conformance/mock_server/`
becomes a Rust crate that is **both a library and a binary**:

- the **library** exposes the service impls + helpers (`MockZerobus`, `MockFlight`,
  `MockAdmin`, counters, injection, `start_server`);
- the **binary** launches all three services and prints `MOCK_ENDPOINT=…` for the orchestrator.

`rust/tests` takes a dev-dependency on this crate; `mock_grpc.rs` / `mock_arrow_flight.rs`
are replaced by imports from `conformance/mock_server`. One mock, one place to change when the
protocol moves — which also removes the "mock drift" risk, since there is no second copy to
fall out of sync.

### Two transports = two gRPC services, one endpoint

The SDK ingests over one of two **distinct** gRPC services, and the mock serves both:

| | Zerobus (native) | Arrow Flight |
|---|---|---|
| service | `databricks.zerobus.Zerobus` | `arrow.flight.protocol.FlightService` |
| ingest RPC | `EphemeralStream` (bidi) | `DoPut` |
| framing | encoded record bytes (proto/json/avro) | Arrow IPC schema + RecordBatches |
| ack | cumulative `durability_ack_up_to_offset` | per-batch `PutResult` |

`MockAdmin` is not part of the protocol — it is the orchestrator's out-of-band backdoor
(counters + captured bytes today; fault injection next). It never carries ingest data.

## 4. Control & data contracts (overview)

### TestHarness (orchestrator → harness)

Language-neutral verbs the harness maps onto the SDK:

- `CreateStream(protocol, schema_artifact, table, options) → stream_id`
  — `schema_artifact` is the per-format schema the SDK needs: `descriptor_proto_bytes`
    (proto), `avro_schema_json` (avro), `arrow_schema_ipc` (arrow); JSON needs none.
- `IngestRecord(stream_id, record) → offset` — **`record` is a *logical* record** (canonical
  protobuf-JSON), **not pre-encoded bytes**, so the SDK's own encoder is exercised.
- `AwaitWriteCompleted(stream_id, offset, timeout)` · `Flush(stream_id)` · `Close(stream_id)`
  · `Shutdown()`.

The harness converts the logical record to the SDK's native object with the language's own
proto/avro/arrow library (a one-liner per format) and lets the SDK encode.

### MockAdmin (orchestrator → mock)

- Counters: `GetCallCount`, `GetCapturedMessageCount`, `GetOnCompletedCallCount`,
  `GetCancelCallCount`.
- Captured wire data: `GetCapturedRecordBytes(stream, offset)`, `GetAckOffset(stream)`.
- `Reset`, `Shutdown`.
- *Fault injection (next):* `Inject{Crash,Hang,SlowAck,StatusError}` — the real L1 payload,
  tracked separately.

### Data plane (harness SDK → mock)

The actual Zerobus gRPC / Arrow Flight traffic. The mock records every message both ways so
scenarios can assert on exactly what the SDK put on the wire.

## 5. Scenarios — three gates + steps

A scenario is a **static, ordered list of steps**, authored in **Python** (chosen over YAML /
JSON / Starlark / Rust: only an embedded language carries symbolic refs and predicate
assertions without pushing a bespoke interpreter into the orchestrator). A step is either an
**action** (invoke on the SDK harness or the mock) or an **assertion** (read a MockAdmin
observation or SDK result and check it).

### 5.1 Applicability — three independent gates

Every run is a point in `(sdk, transport, format)`. "Doesn't run here" has three distinct
causes; keeping them separate is what makes applicability simple:

1. **Validity** (protocol truth, SDK-independent). `proto`/`json`/`avro` are gRPC record
   encodings; `arrow` rides Arrow Flight. So `grpc → {proto,json,avro}`,
   `arrow_flight → {arrow}`. Combinations like `(*, arrow_flight, avro)` are **INVALID** and
   are never enumerated.
2. **Capability** (per-SDK, drifts). e.g. `avro ∈ {rust, python}`; `proto`/`json` ∈ all. A
   *valid* tuple an SDK doesn't support (e.g. `go` + `avro`) is **UNSUPPORTED** — excluded
   but reported. *Source today:* a static table in the orchestrator. (A harness
   `GetCapabilities` self-report + CI drift-diff is a deferred refinement.)
3. **Applicability** (the scenario's own gate). A declarative selector over the tuple, as a
   **decorator argument** so the matrix is machine-enumerable without executing bodies. An
   unspecified dimension is a wildcard; **the format is usually *implied by the schema***
   (using an avro-only type auto-restricts to avro — no hand-written selector).

Consequence: an author declares *what a scenario needs*; validity + capability decide *where
it lands*. You never enumerate the "doesn't apply" side.

### 5.2 Steps: actions & assertions

- **Actions** — `sdk.create_stream`, `sdk.ingest`, `sdk.flush`, `sdk.close`,
  `sdk.await_write`; `mock.reset`, `mock.inject(...)` (future).
- **Assertions** — `mock.call_count`, `mock.captured_message_count`, `mock.on_completed_count`,
  `mock.cancel_count`, `mock.ack_offset(stream)`, `mock.captured_record_count(stream)`,
  `mock.captured_record_bytes(stream, offset)`; later `sdk.state(stream)` for recovery.

**Performance rule (applies to every scenario):** ingest in a loop, then `flush()` once —
**never** wait for an acknowledgment after each `ingest`. `ingest` returns an offset handle to
wait on *later*, not a signal to wait now.

### 5.3 Authoring form

A thin Python **builder**: the scenario function records steps; action calls return **real
ref objects** (`r.offset`) used by later assertions — no stringly-typed keys, and no
`__eq__`/`__bool__` "trap" magic. Staticness is guaranteed *architecturally* (each scenario is
evaluated once, **combo-blind** — the orchestrator applies the `(sdk, transport, format)` at
execution, so there is no combo to branch on). A small **load-time validator** checks ref
integrity, golden existence, and gate validity.

### 5.4 Reporting states

No silent pass — every run reaches an explicit terminal state:
**PASS / FAIL / ERROR** (infra/crash ≠ assertion miss) **/ N-A** (valid+supported, scenario
doesn't target it) **/ UNSUPPORTED** (gate 2) **/ INVALID** (gate 1). No `XFAIL` — a known
cross-SDK divergence simply shows as **FAIL on the diverging SDK** (the consistency signal
falls out of per-tuple results). Guard-rail: a scenario that reaches **0** PASS/FAIL across
the whole matrix is an error (dead / mistyped gate).

Error assertions key on a **normalized error code / category, never the wire message** (the
five SDKs phrase errors differently).

## 6. Schemas & per-format record generation

Principle: **split data from code.** Language-agnostic data (schemas, record values, golden
bytes) lives once on the orchestrator side; the only per-language code is the native-object
materialization inside each harness.

### 6.1 Schemas — `conformance/schemas/<name>/`

Top-level shared asset, referenced by name. Hand-authored per-format sources plus generated
artifacts:

```
conformance/schemas/simple_record/
  schema.proto            # authored
  schema.avsc             # authored (avro)
  schema.arrow.json       # authored (only if a scenario needs arrow)
  build/                  # generated, gitignored: FileDescriptorSet (.desc), etc.
```

A schema's **supported formats** derive from the types it uses (the common core —
`bool,int64,double,string,bytes,struct,list` — is expressible everywhere; format-exclusive
types such as avro `union`/`fixed`/`decimal` restrict a scenario to that format). The
orchestrator loads the schema and ships the right artifact bytes in `CreateStream`.

### 6.2 Record values

The logical record is a plain dict authored **in the scenario** (`{"id": 1, "message": "hello"}`),
written once and identical across formats. It is shipped to the harness as JSON on
`IngestRecord` **and** reused by the orchestrator to produce the expected golden bytes — one
artifact, both roles.

### 6.3 Encoding & byte-exact goldens (non-circularity)

- **Encode on the SDK, capture raw bytes in the mock, compare in the orchestrator.** The mock
  stores raw wire bytes only — it never decodes (decoding in the Rust mock would be circular
  for the Rust SDK and bloats the shared mock).
- **Assertion = byte-exact against a committed golden.** All five tested SDKs encode through
  the *same Rust core* (Go=cgo, TS=NAPI, Python=PyO3, Java=JNI, Rust=native), so they emit
  identical bytes by construction — byte-exact can't produce spurious cross-SDK diffs.
- **Expected bytes come from an independent encoder, not the SDK under test.** *First slice:*
  the orchestrator derives them at runtime from the logical dict via independent Python
  encoders (`protobuf`/`fastavro`) in `data/`. *Hardening (deferred):* generate once,
  validate, and **freeze** as committed `conformance/schemas/<name>/golden/<record-id>.<format>.bin`
  (DAT-style). Either way a mismatch is the signal you want: someone changed the encoding —
  confirm it's intended, regenerate.
- The **encoded-bytes scenarios are gated `format ∈ {proto, json, avro}`** — Arrow is excluded
  (its IPC alignment/compression make byte-exact version-sensitive), so no Arrow decision is
  needed here.

## 7. Starter scenarios (first three)

### 7.1 `happy_path_ingest` — universal `(*, *, *)`

```python
@scenario()                                  # no gate → runs everywhere valid+supported
def happy_path_ingest(s):
    st = s.sdk.create_stream(schema="simple_record")
    last = None
    for i in range(5):
        last = s.sdk.ingest(st, {"id": i, "message": f"m{i}"})   # queue only — never wait here
    s.sdk.flush(st)                                              # wait once for all acks
    s.sdk.close(st)
    s.expect(s.mock.ack_offset(st), equals=last.offset)
    s.expect(s.mock.captured_record_count(st), equals=5)
    s.expect(s.mock.on_completed_count(st), equals=1)
    s.expect(s.mock.cancel_count(st), equals=0)
```

### 7.2 `encoded_bytes_proto` — gated `format=proto`

```python
@scenario(applies=formats("proto"))
def encoded_bytes_proto(s):
    st = s.sdk.create_stream(schema="simple_record")
    r  = s.sdk.ingest(st, {"id": 1, "message": "hello"})
    s.sdk.flush(st)
    s.sdk.close(st)
    s.expect(s.mock.captured_record_bytes(st, r.offset),
             equals=golden("simple_record", {"id": 1, "message": "hello"}, "proto"))
```

### 7.3 `encoded_bytes_avro` — gated `format=avro` (auto-restricts to rust|python via capability)

```python
@scenario(applies=formats("avro"))
def encoded_bytes_avro(s):
    st = s.sdk.create_stream(schema="simple_record")
    r  = s.sdk.ingest(st, {"id": 1, "message": "hello"})
    s.sdk.flush(st)
    s.sdk.close(st)
    s.expect(s.mock.captured_record_bytes(st, r.offset),
             equals=golden("simple_record", {"id": 1, "message": "hello"}, "avro"))
```

Same logical record, same schema; the format dimension + its golden are what differ. Scenario
2 runs for every SDK (all support proto); scenario 3 lands only on `(rust|python, grpc, avro)`
and is reported N-A / UNSUPPORTED elsewhere.

## 8. Orchestrator module layout

Deliberately small — the essential job is: expand the matrix → per run launch mock + harness,
open two channels, walk the step list issuing RPCs and checking assertions, collect results,
tear down. Everything else is support.

```
conformance/orchestrator/src/zerobus_test_runner/
  cli.py          # arg parse → runner
  runner.py       # matrix expansion + per-run orchestration + result collection
  process.py      # build + locate + launch subprocess + endpoint-parse + teardown
  clients.py      # TestHarness + MockAdmin gRPC stubs
  scenario.py     # the builder DSL + step executor + load-time validator
  data/           # schema loading, record values/fixtures, golden encoders
  _generated/     # committed contract-proto stubs (no runtime codegen)
```

Cut from the POC: the YAML/Python dual path (Python only), the SDK-version matrix
(HEAD-only), and **runtime proto codegen** (commit the stubs). `process.py` and `data/` each
merge three POC modules that were really one concern.

## 9. Directory layout

```
conformance/
  DESIGN.md
  test                       # ./conformance/test — bash wrapper on the CLI
  orchestrator/              # Python (see §8)
  mock_server/               # Rust crate: library (imported by rust/tests) + binary
    src/{lib,main,mock_zerobus,mock_flight,mock_admin}.rs
    proto/mock_admin.proto
  harness/
    proto/test_harness.proto # the control contract, shared by all harnesses
    <lang>/
      src/harness.*          # TestHarness service impl
      src/records.*          # per-language logical-record → native-object materialization
  schemas/<name>/            # shared schema sources + generated artifacts + golden/
  scenarios/*.py             # Python scenarios (record values authored here)
```

## 10. Deferred (not in the first cut)

- Fault-injection MockAdmin RPCs (crash / hang / slow-ack / status-error) and the behavioral
  scenarios that use them — the real L1 depth, next after the first three land.
- Harness `GetCapabilities` self-report + CI drift-diff (static table for now).
- A lowered **JSON IR** for scenarios (on-failure protocol traces, CI caching, and eventual
  L2 chaos generation) — the in-memory step list is already that IR; serialize only when
  needed.
- Pinned released-version testing (local-only), CI wiring, Arrow byte-exact strategy.

## 11. Dependency fidelity — test what ships (principle)

The suite must exercise **exactly the implementation that ships**. When we vendor or patch a
dependency, the patch *is* the behavior customers get — so the tests must use the patched
copy, not the pristine upstream one. Testing against upstream when we ship a patch is false
confidence. This applies to **any** dependency, any language (not just `arrow-flight`).

- **SDK-under-test side — automatic.** Every harness is built from this checkout and uses the
  SDK's own dependency resolution, so it already gets whatever the SDK vendors/patches. No
  action needed; it is self-correcting.
- **The gap is our *separate* build units** that resolve dependencies independently of the
  SDK: the conformance Rust crates (`mock_server`, the rust harness) and the golden encoder.
  Left alone these pull pristine copies from the public registry — the silent-wrong-copy trap.
- **Fix (Rust): share the workspace's dependency resolution.** Make the conformance Rust
  crates members of the Rust workspace and declare the shared deps as `{ workspace = true }`.
  This repo pins vendored crates in `[workspace.dependencies]` (e.g. `arrow-flight = { path =
  "third_party/arrow-flight", version = "60.0" }`), so a member that opts in with `.workspace =
  true` gets the vendored/patched copy automatically — reusing the workspace's own pin list
  (the authoritative record of *what we patched*), not a brittle scan of `third_party/`. Note:
  workspace *membership alone* doesn't redirect a crates.io dep — the member must reference the
  dep as `workspace = true` (or the workspace must use `[patch.crates-io]`, which *does* apply
  to all members automatically). Either way the goal is one shared, authoritative pin.
- **Fix (golden side): frozen committed golden bytes.** A committed `.bin` uses no live crate
  at test time, so the compare side is immune to dependency versions entirely (and it also
  removes the python-avro fastavro-vs-fastavro circularity). See §6.3.
- **Automatic by default; explicit loud override; not a routine dimension.** Forgetting to use
  the patched copy yields a silently-green run against code we don't ship — the same cardinal
  sin as a silent-pass gate — so the default must be automatic (reuse each ecosystem's patch
  mechanism). Allow a deliberate upstream run only as a named, *reported* mode (e.g.
  `--deps upstream`) for investigating a patch's effect or forward-compat; never a silent
  default. Do **not** make `(local | upstream)` a standing matrix axis — it doubles the matrix
  to test a configuration we don't ship. Keep it a build config that defaults to shipping.
