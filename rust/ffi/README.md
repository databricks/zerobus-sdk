# Zerobus C FFI

C Foreign Function Interface bindings for the Zerobus Rust SDK.

## Building

```bash
# Build both static and dynamic libraries
cargo build -p zerobus-ffi --release

# Output:
# target/release/libzerobus_ffi.a    (static library)
# target/release/libzerobus_ffi.so   (Linux dynamic library)
# target/release/libzerobus_ffi.dylib (macOS dynamic library)
# target/release/zerobus_ffi.dll     (Windows dynamic library)
```

## Cross-compilation

```bash
# Linux ARM64
rustup target add aarch64-unknown-linux-gnu
cargo build -p zerobus-ffi --release --target aarch64-unknown-linux-gnu

# Linux musl (Alpine), x86_64 and ARM64 — cross-compiled with cargo-zigbuild.
# Built with -C target-feature=-crt-static so the libs link musl dynamically,
# which is correct for dynamically-linked Alpine hosts.
cargo install cargo-zigbuild
rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl
RUSTFLAGS="-C target-feature=-crt-static" \
  cargo zigbuild -p zerobus-ffi --release --target x86_64-unknown-linux-musl
RUSTFLAGS="-C target-feature=-crt-static" \
  cargo zigbuild -p zerobus-ffi --release --target aarch64-unknown-linux-musl

# macOS ARM64 (Apple Silicon)
rustup target add aarch64-apple-darwin
cargo build -p zerobus-ffi --release --target aarch64-apple-darwin

# Windows
rustup target add x86_64-pc-windows-gnu
cargo build -p zerobus-ffi --release --target x86_64-pc-windows-gnu
```

## Usage

### Go (CGO with static library)

```go
/*
#cgo LDFLAGS: -L${SRCDIR}/lib -lzerobus_ffi -ldl -lpthread -lm
#include "zerobus.h"
*/
import "C"
```

### C# (P/Invoke with dynamic library)

```csharp
[DllImport("zerobus_ffi", CallingConvention = CallingConvention.Cdecl)]
private static extern IntPtr zerobus_sdk_new(string endpoint, string ucUrl, ref CResult result);
```

### C++

```cpp
#include "zerobus.h"

// Link with -lzerobus_ffi
```

### SDK connection ownership

Each JSON/protobuf stream uses a dedicated gRPC connection by default. To
restore shared HTTP/2 multiplexing, configure the builder before consuming it:

```c
CZerobusSdkBuilder *builder = zerobus_sdk_builder_new();
zerobus_sdk_builder_connection_per_stream(builder, false);
```

Arrow Flight streams already use dedicated connections and are unaffected.

HTTP/2 multiplexes logical streams over one TCP connection. On high-throughput
workloads over the public internet, packet loss and TCP retransmissions can
cause head-of-line blocking across every stream on that connection, reducing
aggregate throughput. Dedicated connections isolate that loss. For many
smaller, low-throughput streams, shared multiplexing is recommended to reduce
connection overhead.

### Async stream creation callback

For callers that do not want to block a thread in the synchronous stream
creation functions, use `zerobus_sdk_create_stream_async` or
`zerobus_sdk_create_stream_with_headers_provider_async`. Each returns once the
request has been validated and queued on the Rust runtime, then invokes a
one-shot `CreateStreamAsyncCallback` with either the created
`CZerobusStream*` or a null stream plus a failure `CResult`.

The callback receives a `const CResult*` that is valid only for the duration of
the call, so copy `error_message` if you need to keep it. All string,
descriptor, and config inputs are copied before the async create call returns,
but the `CZerobusSdk*` itself must remain valid until the callback fires.

### Multiplexed streams (Beta)

When one stream is the throughput bottleneck and global ordering is not
required, a multiplexed stream opens several sub-streams to the same table and
routes records round-robin across them. Ordering is preserved only within each
sub-stream. Ingest calls return an opaque message ID instead of an offset:

```c
CResult r = {0};
CStreamConfigurationOptions opts = zerobus_get_default_config();
CZerobusMultiplexedStream *mux = zerobus_sdk_create_multiplexed_stream(
    sdk, "catalog.schema.table", descriptor, descriptor_len,
    client_id, client_secret, /* stream_count */ 4, &opts,
    /* ack_on_ack, ack_on_error, ack_user_data, free_ack_user_data */
    NULL, NULL, NULL, NULL, &r);
if (mux == NULL) {
    zerobus_free_error_message(r.error_message);
    return;
}

size_t i = 0;
while (i < n) {
    /* Returns once queued; do not wait for each record. */
    r = (CResult){0};
    zerobus_multiplexed_stream_ingest_proto_record(mux, rec[i], rec_len[i], &r);
    if (r.success) { i++; continue; }
    /* rec[i] was not queued. */
    bool retryable = r.is_retryable;
    zerobus_free_error_message(r.error_message);
    if (zerobus_multiplexed_stream_is_closed(mux)) break;
    if (!retryable) { reject_record(i++); continue; }  /* the record is invalid */
    backoff();  /* retryable while the mux is open: try rec[i] again */
}

r = (CResult){0};
bool ok = zerobus_multiplexed_stream_flush(mux, &r);  /* wait once for every ack */
if (!ok) zerobus_free_error_message(r.error_message);
r = (CResult){0};
ok = zerobus_multiplexed_stream_close(mux, &r) && ok;
if (!ok) zerobus_free_error_message(r.error_message);
if (!ok || i < n) {
    r = (CResult){0};
    CRecordArray unacked = zerobus_multiplexed_stream_get_unacked_records(mux, &r);
    /* Re-send unacked.records, then rec[i..n), on a new stream. Each
       record's data has data_len bytes and no NUL: copy a JSON record
       (is_json) into a NUL-terminated buffer before a JSON ingest. */
    zerobus_free_record_array(unacked);
    zerobus_free_error_message(r.error_message);
}
zerobus_multiplexed_stream_free(mux);
```

- Message IDs are not ordered and may be negative. Check `r.success` rather
  than the sign of the return value, and pass IDs only to
  `zerobus_multiplexed_stream_wait_for_message_id` on the same stream. An
  empty batch returns -2 with `r.success` set; -2 is not a message ID. On a
  closed mux it fails with -1.
- A NULL record pointer or array is valid only with a length or count of 0.
- A failed ingest call queued nothing, and its records will not come back from
  `zerobus_multiplexed_stream_get_unacked_records`. Retry it only when
  `r.is_retryable` is set and the mux is still open. A non-retryable error on
  an open mux means the record itself is invalid; once the mux is closed,
  re-send it on a new stream.
- A batch call sends the whole batch to one sub-stream. To use every
  sub-stream, send at least `stream_count` batches.
- For JSON, pass options with `record_type = 2` and a NULL descriptor. NULL
  options select protobuf.
- Every `zerobus_multiplexed_stream_*` call except `_free` may run
  concurrently on one handle. `_free` must not overlap any other call.
- Ack callbacks are create parameters (`ack_on_ack`, `ack_on_error`,
  `ack_user_data`, `free_ack_user_data`); the `ack_on_*` fields of
  `CStreamConfigurationOptions` must stay NULL, and its `ack_user_data` is
  ignored. They receive opaque message IDs (not ordered, may be negative), and
  callbacks from different sub-streams may run concurrently, so `ack_user_data`
  must be thread-safe. The FFI owns `ack_user_data` and calls
  `free_ack_user_data` once, from any thread, after the last callback. That can
  be after the create returns, even a failed one, and after
  `zerobus_multiplexed_stream_free` returns. With a NULL `free_ack_user_data`,
  keep `ack_user_data` valid for the rest of the process. Callbacks must not
  call `zerobus_multiplexed_stream_*` functions or block on a thread that does.
- With `zerobus_sdk_create_multiplexed_stream_with_headers_provider`, one
  headers callback serves every sub-stream. The FFI serializes calls into it,
  so a slow callback delays every sub-stream's open; keep it fast. Like ack
  callbacks, it must not call `zerobus_multiplexed_stream_*` functions.
- If a sub-stream fails, the mux stops accepting records once a call observes
  the failure. Recover anything unacknowledged with
  `zerobus_multiplexed_stream_get_unacked_records`, and send them, plus any
  record whose ingest call failed, on a new stream. Every call returns the
  same records, so re-send them once, from one caller.
- Every call except `is_closed` and `free` has an `_async` variant that
  returns at once and reports through a completion callback on a runtime
  thread (`zerobus_sdk_create_multiplexed_stream_async`,
  `zerobus_multiplexed_stream_ingest_proto_record_async`, ...). A callback can
  run before its call returns. Its `result` is valid only during the call: do not free its
  `error_message` (the FFI frees it), and copy it to keep it. A callback may
  call the `_async` functions, `is_closed` and `free`, but no blocking call.
  Call `free` from a callback only after the `_async` call that started it
  has returned.
- Flush and close wait for every `_async` ingest accepted before them to be
  queued, but not for its completion callback; a failed `_async` ingest is not
  in `get_unacked_records`, so keep each record until its callback reports
  success. `_async` ingests have no back-pressure, so limit how many are
  outstanding: one that waits for capacity longer than 30 seconds fails in
  its callback. They can be queued in a different order than called. The
  multiplexed stream has no `_nowait` ingest; use the `_async` calls instead.
- Arrow Flight multiplexing is not yet exposed through the C FFI.

### Dynamic protobuf from a Unity Catalog schema (pure C)

Build a protobuf descriptor and encode records straight from Unity Catalog
table metadata — no pre-generated `.proto` file and no second Rust crate:

```c
CResult r = {0};

/* init: fetch GET /api/2.1/unity-catalog/tables/{name} and pass its JSON body */
CZerobusProtoSchema *schema = zerobus_proto_schema_from_uc_json(uc_table_json, &r);
if (schema == NULL) {
    zerobus_free_error_message(r.error_message);
    return;
}

uintptr_t dlen = 0;
const uint8_t *desc = zerobus_proto_schema_descriptor_bytes(schema, &dlen);
if (desc == NULL) {
    zerobus_proto_schema_free(schema);
    return;
}

r = (CResult){0};
CZerobusStream *stream = zerobus_sdk_create_stream(
    sdk, table_name, desc, dlen, client_id, client_secret, &opts, &r);
if (stream == NULL) {
    zerobus_free_error_message(r.error_message);
    zerobus_proto_schema_free(schema);
    return;
}

uint8_t *buf = NULL;
uintptr_t len = 0;
r = (CResult){0};
if (!zerobus_proto_schema_encode_json(schema, record_json, &buf, &len, &r)) {
    zerobus_free_error_message(r.error_message);
    zerobus_stream_free(stream);
    zerobus_proto_schema_free(schema);
    return;
}

const uint8_t *records[] = { buf };
const uintptr_t record_lens[] = { len };
r = (CResult){0};
if (zerobus_stream_ingest_proto_records(stream, records, record_lens, 1, &r) < 0) {
    zerobus_free_error_message(r.error_message);
    zerobus_free_proto_bytes(buf, len);
    zerobus_stream_free(stream);
    zerobus_proto_schema_free(schema);
    return;
}
zerobus_free_proto_bytes(buf, len);

int failed = 0;
r = (CResult){0};
if (!zerobus_stream_flush(stream, &r)) {
    zerobus_free_error_message(r.error_message);
    failed = 1;
}
r = (CResult){0};
if (!zerobus_stream_close(stream, &r)) {
    zerobus_free_error_message(r.error_message);
    failed = 1;
}
zerobus_stream_free(stream);
zerobus_sdk_free(sdk);
zerobus_proto_schema_free(schema);
if (failed) {
    return;
}
```

Encoding contract: record object keys are matched to column names; unknown keys
are ignored (upstream records often carry extra non-column metadata). Records are
encoded through protobuf's JSON mapping, so a few column types need their JSON
value shaped accordingly:

| Unity Catalog type            | Proto type      | JSON value to supply                                             |
|-------------------------------|-----------------|------------------------------------------------------------------|
| `STRING`                      | `string`        | string                                                           |
| `INT`/`INTEGER`, `SHORT`/`SMALLINT` | `int32`   | number                                                           |
| `LONG`/`BIGINT`               | `int64`         | number **or string** — use a **string** above 2^53 (see below)   |
| `FLOAT`, `DOUBLE`             | `float`/`double`| number                                                           |
| `BOOLEAN`                     | `bool`          | boolean                                                          |
| `BINARY`                      | `bytes`         | **base64-encoded string** (not a JSON array of byte values)      |
| `DATE`                        | `int32`         | integer — days since the Unix epoch (not an ISO-8601 string)     |
| `TIMESTAMP`                   | `int64`         | integer — microseconds since the Unix epoch (not an ISO-8601 string) |
| `VARIANT`                     | `string`        | **JSON-encoded string** — a string whose contents are the variant's JSON (objects, arrays, or primitives) |
| `ARRAY<T>`                    | `repeated T`    | JSON array of `T` values                                         |
| `MAP<K,V>`                    | `map<K,V>`      | JSON object (`K` must be an integral, bool, or string type)      |
| `STRUCT<...>`                 | nested `message`| JSON object                                                      |

This table mirrors the supported set in the [Zerobus type-support
reference](https://docs.databricks.com/aws/en/ingestion/zerobus-limits#type-support).
Beyond that reference, the encoder additionally accepts `BYTE`/`TINYINT` (→
`int32`), `TIMESTAMP_NTZ` (→ `int64` micros, same wire shape as `TIMESTAMP`),
and `DECIMAL` (→ `string`, e.g. `"123.45"`, to preserve precision/scale).
Complex columns (`ARRAY`/`MAP`/`STRUCT`) require the column's `type_json` from
the Unity Catalog REST response to be present in the input.

Two precision pitfalls worth calling out:

- **64-bit integers above 2^53** (large `BIGINT` values, and `TIMESTAMP` micros
  past year 2255) lose precision when emitted as a JSON number by most encoders.
  Pass them as JSON **strings** (the canonical protobuf-JSON form for 64-bit
  ints), which round-trip exactly.
- **`DATE`/`TIMESTAMP*` unit mismatches** are silent: writing milliseconds where
  microseconds are expected shifts every row by 10³.

Non-nullable scalar and struct columns become proto2 `required` fields; a record
missing one is rejected rather than encoded. Presence is checked recursively, so
a required field missing inside a `STRUCT`, inside an `ARRAY<STRUCT>` element, or
inside a `MAP` value is also rejected (the error names the full path, e.g.
`addr.zip`, `items[2].id`, `props[home].zip`). Non-nullable `ARRAY`/`MAP` columns
map to `repeated`, which has no presence, so an omitted one encodes as empty
rather than being rejected.

A handle may be shared by concurrent readers: worker threads may call
`zerobus_proto_schema_encode_json` and `zerobus_proto_schema_descriptor_bytes`
on the same handle concurrently. `zerobus_proto_schema_free` must be called
exactly once, after all in-flight calls on the handle have returned — it must
not race any other use of the handle.

## API Reference

See `zerobus.h` for the complete C API documentation.
