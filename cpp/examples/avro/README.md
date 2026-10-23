# Avro Examples (Beta)

This directory contains examples demonstrating Avro record-object ingestion into
Databricks Delta tables using the Zerobus C++ SDK.

## Overview

Avro examples show how to ingest records encoded as JSON objects against the
table's Avro writer schema. The Avro format offers schema validation at ingestion
time and works well for tables with strict schemas. The server maps JSON fields
to the table's columns using the writer schema.

**Features:**
- Schema validation via Avro
- Encode records as JSON objects
- Single and batch ingestion

**Available examples:**
- **`single.cpp`** — ingest records one at a time using `ingest_avro_record()`
- **`batch.cpp`** — ingest multiple records at once using `ingest_avro_records()`

Both open an Avro stream by setting `StreamOptions::record_type` to
`RecordType::Avro`.

## Prerequisites

1. Create a Delta table with a known schema (or use an existing one).
2. Export connection settings (see [Main Examples README](../README.md#prerequisites)):
   ```bash
   export ZEROBUS_SERVER_ENDPOINT="https://<your-shard-id>.zerobus.<region>.cloud.databricks.com"
   export DATABRICKS_WORKSPACE_URL="https://<your-workspace>.cloud.databricks.com"
   export ZEROBUS_TABLE_NAME="catalog.schema.table_name"
   export DATABRICKS_CLIENT_ID="<your_client_id>"
   export DATABRICKS_CLIENT_SECRET="<your_client_secret>"
   ```

## Single-Record Example

### Running

```bash
./build/examples/avro_single
```

**Expected output:**
```
Record 1 queued with offset ID: 0
Record 2 queued with offset ID: 1
Stream closed successfully.
```

### Code Highlights

Each `ingest_avro_record()` queues the record; the example ingests multiple
records and calls `flush()` ONCE at the end:

```cpp
// Queue records
stream.ingest_avro_record(R"({"id": 1, "name": "Alice"})");
stream.ingest_avro_record(R"({"id": 2, "name": "Bob"})");

// Flush once to confirm all records durable
stream.flush();
```

**Creating an Avro stream:**
```cpp
zerobus::TableProperties props;
props.table_name = table_name;

zerobus::StreamOptions options;
options.record_type = zerobus::RecordType::Avro;

zerobus::Stream stream =
    sdk.create_stream(props, client_id, client_secret, options);
```

## Batch Example

### Running

```bash
./build/examples/avro_batch
```

**Expected output:**
```
Batch of 3 records queued with offset ID: 0
Stream closed successfully.
```

### Code Highlights

`ingest_avro_records()` hands a batch of JSON records to the SDK in one FFI
crossing, amortizing the per-call overhead:

```cpp
std::vector<std::string> batch = {
    R"({"id": 1, "name": "Alice"})",
    R"({"id": 2, "name": "Bob"})",
    R"({"id": 3, "name": "Carol"})"
};

std::int64_t offset = stream.ingest_avro_records(batch);
stream.flush();  // confirm the batch
```

## JSON Format

Each record must be a valid UTF-8 JSON object whose fields match the table's
schema. Unknown keys are ignored:

```cpp
R"({"id": 123, "name": "Alice", "created_at": 1620000000000000})"
```

## Notes

- Avro support is **Beta** and requires the C++ SDK to be built with
  `-DZEROBUS_ENABLE_AVRO=ON`.
- Records are encoded against the stream's writer schema and validated by the server.
- For high throughput, prefer batch ingestion (`ingest_avro_records`) over
  per-record calls (`ingest_avro_record`).
