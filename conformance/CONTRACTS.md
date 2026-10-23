# Conformance infra — shared contracts (FROZEN for the first slice)

These are the interfaces between the orthogonal slices. Every slice conforms to
them; do not change them unilaterally. See `DESIGN.md` for the why.

## Processes & handshake

- **Mock server** (`conformance/mock_server/`): on startup, binds `127.0.0.1:0`,
  prints exactly one line `MOCK_ENDPOINT=127.0.0.1:<port>` to **stdout**, flushes,
  then serves. Hosts the Zerobus gRPC data plane + the `MockAdmin` service on that
  one port. (Arrow Flight data plane is a later addition — stub/omit for now.)
- **Harness** (`conformance/harness/<lang>/`): on startup, binds `127.0.0.1:0`,
  prints exactly one line `HARNESS_ENDPOINT=127.0.0.1:<port>` to **stdout**, flushes,
  then serves the `TestHarness` service.
- Both: logs go to **stderr**; only the endpoint line goes to stdout. Both exit
  cleanly on the `Shutdown` RPC and on SIGINT/SIGTERM.

## Protos (the two control contracts)

- `conformance/harness/proto/test_harness.proto` — `TestHarness` (orchestrator → harness).
- `conformance/mock_server/proto/mock_admin.proto` — `MockAdmin` (orchestrator → mock).
- The Zerobus data-plane proto is the existing `rust/sdk/zerobus_service.proto`
  (`databricks.zerobus.Zerobus`). The mock implements it; the SDK speaks it.

## Record JSON (logical record)

`IngestRecord.record_json` is a JSON **object** mapping field name → value, e.g.
`{"id": 1, "message": "hello"}`. Canonical protobuf-JSON conventions: int64 as a
JSON number within 2^53 (string beyond), bytes as base64. The harness converts this
+ the schema artifact into the SDK's native record and lets the SDK encode. The
orchestrator uses the *same* logical dict to look up the golden bytes.

## Capability table (static, in the orchestrator — first slice)

```
sdk         transports        formats
rust        grpc, arrow_flight proto, json, avro, arrow
python      grpc, arrow_flight proto, json, avro, arrow
go          grpc               proto, json
typescript  grpc               proto, json
java        grpc               proto, json
```
Validity: `grpc → {proto,json,avro}`, `arrow_flight → {arrow}`. First slice only
wires **rust / grpc / proto** end-to-end; the rest are table entries for the matrix.

## Python builder DSL surface (scenarios ⇄ orchestrator)

Scenario files call this; the orchestrator (`scenario.py`) implements it. Calls
RECORD steps (they do not execute); refs resolve at run time.

```python
@scenario(applies=None)            # decorator; registers the scenario fn. applies = a Gate or None (=(*,*,*))
def name(s): ...

formats(*names) -> Gate            # gate on the format dimension, e.g. formats("proto")
golden(schema: str, record: dict, fmt: str) -> GoldenRef  # resolves committed golden bytes

# inside a scenario, `s` is the recorder:
s.sdk.create_stream(schema: str, table: str = "cat.schema.table") -> StreamRef
s.sdk.ingest(stream: StreamRef, record: dict) -> RecordRef     # RecordRef.offset is a symbolic ref
s.sdk.flush(stream: StreamRef)
s.sdk.close(stream: StreamRef)
s.sdk.await_write(rec: RecordRef, timeout_ms: int = 10000)

s.mock.call_count() -> Target
s.mock.captured_message_count() -> Target
s.mock.on_completed_count(stream: StreamRef = None) -> Target
s.mock.cancel_count(stream: StreamRef = None) -> Target
s.mock.captured_record_count(stream: StreamRef) -> Target
s.mock.ack_offset(stream: StreamRef) -> Target
s.mock.captured_record_bytes(stream: StreamRef, offset) -> Target

s.expect(target: Target, equals=<value|Ref|GoldenRef>)
```

Reporting states: PASS / FAIL / ERROR / N-A / UNSUPPORTED / INVALID (no xfail).

## CLI

```
./conformance/test run --sdk <rust|…> --scenario <name> [--transport grpc] [--format proto]
```
Omitted `--transport`/`--format` → expand across all valid+supported+applicable
combos. The wrapper puts `orchestrator/src` on PYTHONPATH and runs
`python -m zerobus_test_runner`.

## First-slice simplifications (remove later)

- Mock counters are **global per mock instance**; one stream per scenario; the
  orchestrator calls `MockAdmin.Reset` before each run. `StreamQuery.stream_id` is
  reserved/ignored for now.
- Only `rust / grpc / proto` is wired end-to-end. `json`/`avro`/`arrow` and the
  other four SDKs: leave clear `TODO`s, don't fake them.
- No fault injection yet. No `GetCapabilities` RPC (use the static table).
- Contract-proto stubs are committed under each consumer (`orchestrator/.../_generated/`),
  generated once — no runtime codegen.
