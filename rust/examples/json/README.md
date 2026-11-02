# JSON Examples

This directory contains examples demonstrating JSON-based data ingestion into Databricks Delta tables using the Zerobus Rust SDK.

## Table of Contents

- [Overview](#overview)
- [Three Ways to Pass Data](#three-ways-to-pass-data)
- [Single-Record Example](#single-record-example)
  - [Running the Example](#running-the-example)
  - [Code Highlights](#code-highlights)
- [Batch Example](#batch-example)
  - [Running the Example](#running-the-example-1)
  - [Code Highlights](#code-highlights-1)
- [Persistent Stream Example](#persistent-stream-example)
- [Unary Example](#unary-example)
- [Adapting for Your Custom Table](#adapting-for-your-custom-table)

## Overview

JSON examples are recommended for getting started - they're simpler and don't require schema generation.

**Features:**
- No schema generation required
- Easy to understand and modify
- Great for quick prototyping

**Available examples:**
- **`single.rs`** - Ingest records one at a time using `ingest_record_offset()`
- **`batch.rs`** - Ingest multiple records at once using `ingest_records_offset()`
- **`persistent.rs`** - Create a durable stream or resume it by `stream_id`
- **`unary.rs`** - Send a batch in one call without a stream, for sparse traffic

## Three Ways to Pass Data

The SDK supports three approaches for passing JSON data:

| Approach | Type | Description |
|----------|------|-------------|
| **Auto-serializing** | `JsonValue(struct)` | Pass any serializable struct; SDK handles JSON conversion |
| **Pre-serialized** | `JsonString(string)` | Pass pre-built JSON strings with explicit wrapper |
| **Backward-compatible** | `String` | Pass raw strings directly (no wrapper needed) |

**When to use each:**
- **`JsonValue`** - When you have Rust structs and want compile-time type safety
- **`JsonString`** - When you have pre-built JSON strings and want explicit type clarity
- **Raw `String`** - For backward compatibility with existing code; works the same as `JsonString`

## Single-Record Example

### Running the Example

1. Export the environment variables listed in [Prerequisites](../README.md#3-configure-credentials)

2. Run the example:
   ```bash
   cargo run -p rust-examples-json --example json_single
   ```

**Expected output:**
```
=== Offset-based API (Recommended) ===
[Auto-serializing] Record queued with offset ID: 0
[Pre-serialized] Record queued with offset ID: 1
[Backward-compatible] Record queued with offset ID: 2
All records acknowledged
Stream closed successfully
```

### Code Highlights

The example demonstrates all three data-passing approaches. Each `ingest_record_offset()`
returns as soon as the record is queued; we ingest all of them and `flush()` once at the end
rather than waiting per record (waiting per record forces a server round-trip each time):

```rust
use databricks_zerobus_ingest_sdk::{JsonValue, JsonString};

// 1. Auto-serializing: pass struct directly
let order = Order { id: 1, customer_name: "Alice".to_string(), /* ... */ };
let _offset = stream.ingest_record_offset(JsonValue(order)).await?;

// 2. Pre-serialized: pass JSON string with wrapper
let json = r#"{
    "id": 2,
    "customer_name": "Bob"
}"#.to_string();
let _offset = stream.ingest_record_offset(JsonString(json)).await?;

// 3. Backward-compatible: pass raw string (no wrapper)
let json = r#"{
    "id": 3,
    "customer_name": "Carol"
}"#.to_string();
let _offset = stream.ingest_record_offset(json).await?;

// Confirm all queued records at once.
stream.flush().await?;
```

**Building a JSON stream:**
```rust
let stream = sdk
    .stream_builder()
    .table(&config.table_name)
    .oauth(&config.client_id, &config.client_secret)
    .json()
    .build()
    .await?;
```

## Batch Example

### Running the Example

1. Export the environment variables listed in [Prerequisites](../README.md#3-configure-credentials)

2. Run the example:
   ```bash
   cargo run -p rust-examples-json --example json_batch
   ```

**Expected output:**
```
=== Offset-based API (Recommended) ===
[Auto-serializing] Batch of 3 records queued with offset ID: 0
[Pre-serialized] Batch of 3 records queued with offset ID: 1
[Backward-compatible] Batch of 3 records queued with offset ID: 2
All offset-API batches acknowledged
Stream closed successfully
```

### Code Highlights

```rust
use databricks_zerobus_ingest_sdk::{JsonValue, JsonString};

let batch1: Vec<JsonValue<Order>> = vec![
    JsonValue(Order { id: 1, /* ... */ }),
    JsonValue(Order { id: 2, /* ... */ }),
    JsonValue(Order { id: 3, /* ... */ }),
];
stream.ingest_records_offset(batch1).await?;

let batch2: Vec<JsonString> = vec![
    JsonString(r#"{ "id": 4 }"#.to_string()),
    JsonString(r#"{ "id": 5 }"#.to_string()),
    JsonString(r#"{ "id": 6 }"#.to_string()),
];
stream.ingest_records_offset(batch2).await?;

let batch3: Vec<String> = vec![
    r#"{ "id": 7 }"#.to_string(),
    r#"{ "id": 8 }"#.to_string(),
    r#"{ "id": 9 }"#.to_string(),
];
stream.ingest_records_offset(batch3).await?;

stream.flush().await?;
```

**Batch semantics:**
- **All-or-nothing**: The entire batch succeeds or fails as a unit
- **Single acknowledgment**: One offset ID for the whole batch
- **Empty batches**: Returns `None` (no-op)

## Persistent Stream Example

Persistent streams retain their identity and committed offset on the server,
allowing a later process to resume ingestion without restarting offsets.

On the first run, leave `ZEROBUS_PERSISTENT_STREAM_ID` unset:

```bash
cargo run -p rust-examples-json --example json_persistent
```

The example prints the newly assigned stream ID. Save it, then resume on a
later run:

```bash
ZEROBUS_PERSISTENT_STREAM_ID=<stream-id> \
  cargo run -p rust-examples-json --example json_persistent
```

The example queues records in a loop and calls `flush()` once afterward. Each run
writes new records. The record ids continue after the last committed offset: the
first run writes ids 1 and 2, and the next run writes ids 3 and 4. Its final
`close()` ends only the current connection; the durable stream remains available
for another resume.

## Unary Example

A unary client sends each batch in one call and returns once the batch is
durable. It opens no stream, so it suits sparse traffic, such as a few batches
per minute. For continuous ingestion, use a stream as in `batch.rs`.

```bash
cargo run -p rust-examples-json --example json_unary
```

**Expected output:**
```
Batch of 3 records is durable
```

```rust
let client = sdk
    .unary_client_builder()
    .table(TABLE_NAME)
    .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
    .json()
    .build()
    .await?;

// One call for the whole batch; nothing to flush or close afterwards.
client.ingest_records(batch).await?;
```

Build the client once and reuse it. Send the records you have in one
`ingest_records()` call; never one record per call in a loop. See
[Unary Ingestion](../../README.md#unary-ingestion-sparse-traffic) for the full
behavior, including JSON Lines.

## Adapting for Your Custom Table

JSON examples require no schema generation. Simply modify the struct and JSON to match your table:

**1. Update the struct (for `JsonValue` approach):**

```rust
#[derive(Serialize)]
struct YourData {
    your_field_1: String,
    your_field_2: i32,
    your_field_3: bool,
}
```

**2. Update the JSON strings (for `JsonString` or raw approach):**

```rust
let json = r#"{
    "your_field_1": "value",
    "your_field_2": 123,
    "your_field_3": true
}"#.to_string();
```

**3. Configure the environment variables** listed in the
[examples prerequisites](../README.md#3-configure-credentials).
