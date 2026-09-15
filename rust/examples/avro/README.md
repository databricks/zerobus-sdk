# Avro Examples

This directory contains examples demonstrating Avro-based data ingestion into Databricks Delta tables using the Zerobus Rust SDK.

The `avro` feature flag must be enabled.

## Table of Contents

- [Overview](#overview)
- [Two Ways to Pass Data](#two-ways-to-pass-data)
- [Single-Record Example](#single-record-example)
  - [Running the Example](#running-the-example)
  - [Code Highlights](#code-highlights)
- [Batch Example](#batch-example)
  - [Running the Example](#running-the-example-1)
  - [Code Highlights](#code-highlights-1)
- [Adapting for Your Custom Table](#adapting-for-your-custom-table)

## Overview

Avro is a third record format option alongside JSON and Protocol Buffers: a compact, row-oriented binary encoding driven by a writer schema (JSON) that you declare once at stream creation. It is a good fit when your data already has an Avro schema or your pipeline is Avro-native (Kafka, Spark, Hadoop).

**Features:**
- Compact row-oriented binary encoding driven by an Avro writer schema
- Ingest a record object (`AvroRecord`) the stream encodes against the schema, or pre-encoded datums (`AvroBytes`)

**Available examples:**
- **`single.rs`** - Ingest records one at a time using `ingest_record_offset()`
- **`batch.rs`** - Ingest multiple records at once using `ingest_records_offset()`

> **Beta / feature flag.** Avro is behind the `avro` Cargo feature (the examples' `Cargo.toml` enables it for you). It is **Beta**: ephemeral streams only, and the feature is in development. The `avro` feature requires Rust 1.85 (via `apache-avro`).

## Two Ways to Pass Data

The SDK supports two approaches for passing Avro data:

| Approach | Type | Description |
|----------|------|-------------|
| **Record object** | `AvroRecord(AvroValue)` | Build the record as an `AvroValue`; the stream encodes it against the writer schema |
| **Pre-encoded** | `AvroBytes(bytes)` | Pass a raw Avro datum you encoded yourself |

**When to use each:**
- **`AvroRecord`** - The default. Build an `AvroValue` (`apache_avro`'s value type, re-exported by the SDK) and the stream resolves + encodes it against the writer schema. It covers **every** Avro type — records, arrays, maps, unions, enums, `fixed`, `decimal`, and logical types.
- **`AvroBytes`** - When you already hold an Avro-encoded datum (e.g. produced upstream) and want to pass the raw bytes through unchanged.

This example crate is excluded from the Cargo workspace (so the `avro` feature doesn't leak into the other crates), so run the examples from this directory with `--example`, not with `-p`.

## Single-Record Example

### Running the Example

1. Configure credentials in `single.rs` (see [Prerequisites](../README.md#prerequisites))

2. Run the example:
   ```bash
   cargo run --example avro_single
   ```

**Expected output:**
```
[Record object] Record queued with offset ID: 0
[Pre-encoded] Record queued with offset ID: 1
All records acknowledged
Stream closed successfully
```

### Code Highlights

The example demonstrates both data-passing approaches. Each `ingest_record_offset()`
returns as soon as the record is queued; we ingest all of them and `flush()` once at the end
rather than waiting per record (waiting per record forces a server round-trip each time):

```rust
use databricks_zerobus_ingest_sdk::{AvroBytes, AvroRecord, AvroValue};

// The writer schema, declared once at stream creation.
let mut stream = sdk
    .stream_builder()
    .table(TABLE_NAME)
    .oauth(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
    .avro(AVRO_SCHEMA)
    .build()
    .await?;

// 1. Record object: build an AvroValue; the stream encodes it against the writer schema.
let record = AvroValue::Record(vec![
    ("id".to_string(), AvroValue::Long(1)),
    ("customer_name".to_string(), AvroValue::String("Alice".to_string())),
]);
let _offset = stream.ingest_record_offset(AvroRecord(record)).await?;

// 2. Pre-encoded: a raw Avro datum you encoded yourself.
let _offset = stream.ingest_record_offset(AvroBytes(datum)).await?;

// Confirm all queued records at once.
stream.flush().await?;
```

## Batch Example

### Running the Example

1. Configure credentials in `batch.rs` (see [Prerequisites](../README.md#prerequisites))

2. Run the example:
   ```bash
   cargo run --example avro_batch
   ```

**Expected output:**
```
[Record objects] Batch queued with offset ID: 0
[Pre-encoded] Batch queued with offset ID: 1
All offset-API batches acknowledged
Stream closed successfully
```

### Code Highlights

```rust
use databricks_zerobus_ingest_sdk::{AvroBytes, AvroRecord, AvroValue};

// Record objects: a batch of AvroValues the stream encodes against the schema.
let records: Vec<AvroRecord> = vec![
    AvroRecord(AvroValue::Record(vec![
        ("id".to_string(), AvroValue::Long(1)),
        ("customer_name".to_string(), AvroValue::String("Alice".to_string())),
    ])),
    AvroRecord(AvroValue::Record(vec![
        ("id".to_string(), AvroValue::Long(2)),
        ("customer_name".to_string(), AvroValue::String("Bob".to_string())),
    ])),
];
stream.ingest_records_offset(records).await?;

// Pre-encoded: a batch of raw Avro datums.
let pre_encoded_batch: Vec<AvroBytes> = vec![AvroBytes(datum_1), AvroBytes(datum_2)];
stream.ingest_records_offset(pre_encoded_batch).await?;

stream.flush().await?;
```

**Batch semantics:**
- **All-or-nothing**: The entire batch succeeds or fails as a unit
- **Single acknowledgment**: One offset ID for the whole batch
- **Empty batches**: Returns `None` (no-op)

## Adapting for Your Custom Table

To ingest into your own table, change the Avro writer schema and the record fields to match its columns.

**1. Update the writer schema** (`AVRO_SCHEMA` in `single.rs` / `batch.rs`) to match your table's columns:

```json
{
  "type": "record",
  "name": "YourRecord",
  "fields": [
    {"name": "your_field_1", "type": "string"},
    {"name": "your_field_2", "type": "long"},
    {"name": "your_field_3", "type": "boolean"}
  ]
}
```

**2. Update the `AvroValue`** (for the `AvroRecord` approach) so its fields and value types match the schema:

```rust
let record = AvroValue::Record(vec![
    ("your_field_1".to_string(), AvroValue::String("value".to_string())),
    ("your_field_2".to_string(), AvroValue::Long(123)),
    ("your_field_3".to_string(), AvroValue::Boolean(true)),
]);
```

**3. Update table name and credentials** in the constants at the top of `single.rs` or `batch.rs`.
