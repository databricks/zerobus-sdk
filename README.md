# Zerobus Rust SDK

A high-performance Rust client for streaming data ingestion into Databricks Delta tables using the Zerobus service.

## Disclaimer

[Public Preview](https://docs.databricks.com/release-notes/release-types.html): This SDK is supported for production use cases and is available to all customers. Databricks is actively working on stabilizing the Zerobus Ingest SDK for Rust. Minor version updates may include backwards-incompatible changes.

We are keen to hear feedback from you on this SDK. Please [file issues](https://github.com/databricks/zerobus-sdk-rs/issues), and we will address them.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Repository Structure](#repository-structure)
- [How It Works](#how-it-works)
- [Usage Guide](#usage-guide)
  - [1. Generate Protocol Buffer Schema](#1-generate-protocol-buffer-schema)
  - [2. Initialize the SDK](#2-initialize-the-sdk)
  - [3. Configure Authentication](#3-configure-authentication)
  - [4. Create a Stream](#4-create-a-stream)
  - [5. Ingest Data](#5-ingest-data)
  - [6. Handle Acknowledgments](#6-handle-acknowledgments)
  - [7. Close the Stream](#7-close-the-stream)
- [Configuration Options](#configuration-options)
- [Error Handling](#error-handling)
- [Examples](#examples)
- [Best Practices](#best-practices)
- [API Reference](#api-reference)
- [Building from Source](#building-from-source)
- [Community and Contributing](#community-and-contributing)
- [License](#license)

## Overview

The Zerobus Rust SDK provides a robust, async-first interface for ingesting large volumes of data into Databricks Delta tables. It abstracts the complexity of the Zerobus service and handles authentication, retries, stream recovery, and acknowledgment tracking automatically.

**What is Zerobus?** Zerobus is a high-throughput streaming service for direct data ingestion into Databricks Delta tables, optimized for real-time data pipelines and high-volume workloads.

## Features

- **Async/Await Support** - Built on Tokio for efficient concurrent I/O operations
- **Automatic OAuth 2.0 Authentication** - Seamless token management with Unity Catalog
- **Built-in Recovery** - Automatic retry and reconnection for transient failures
- **High Throughput** - Configurable inflight record limits for optimal performance
- **Batch Ingestion** - Ingest multiple records at once with all-or-nothing semantics for maximum throughput
- **Flexible Serialization** - Support for both JSON (simple) and Protocol Buffers (type-safe) data formats
- **Type Safety** - Protocol Buffers ensure schema validation at compile time
- **Schema Generation** - CLI tool to generate protobuf schemas from Unity Catalog tables
- **Flexible Configuration** - Fine-tune timeouts, retries, and recovery behavior
- **Graceful Stream Management** - Proper flushing and acknowledgment tracking

## Installation

Add the SDK to your `Cargo.toml`:

```bash
cargo add databricks-zerobus-ingest-sdk
cargo add prost prost-types
cargo add tokio --features macros,rt-multi-thread
```
**Why these dependencies?**
- **`databricks-zerobus-ingest-sdk`** - The SDK itself
- **`prost`** and **`prost-types`** - Required for encoding your data to Protocol Buffers and loading schema descriptors
- **`tokio`** - Async runtime required for running async functions (the SDK is fully async)

> **What's in the crates.io package?** The published crate contains only the core Zerobus ingestion SDK. Tools for schema generation (`tools/generate_files`) and working examples (`examples/`) are only available in the [GitHub repository](https://github.com/databricks/zerobus-sdk-rs). You'll need to clone the repo to generate protobuf schemas from your Unity Catalog tables.

### For Local Development

Clone the repository and use a path dependency:

```bash
git clone https://github.com/databricks/zerobus-sdk-rs.git
cd your_project
```

Then in your `Cargo.toml`:

```toml
[dependencies]
databricks-zerobus-ingest-sdk = { path = "../zerobus-sdk-rs/sdk" }
prost = "0.13.3"
prost-types = "0.13.3"
tokio = { version = "1.42.0", features = ["macros", "rt-multi-thread"] }
```

## Quick Start

The SDK supports two serialization formats and two ingestion methods:

**Serialization:**
- **JSON** (Recommended for getting started): Simpler approach using JSON strings, no schema generation required
- **Protocol Buffers** (Recommended for production): Type-safe approach with schema validation at compile time

**Ingestion Methods:**
- **Single-record** (`ingest_record_offset`): Ingest records one at a time with per-record acknowledgment
- **Batch** (`ingest_records_offset`): Ingest multiple records at once with all-or-nothing semantics for higher throughput

> **Note:** The older `ingest_record()` and `ingest_records()` methods are deprecated as of v0.4.0. Use the `_offset` variants instead.

See [`examples/README.md`](examples/README.md) for detailed setup instructions and examples for all combinations.

## Repository Structure

```
zerobus_rust_sdk/
├── sdk/                                # Core SDK library
│   ├── src/
│   │   ├── lib.rs                      # Main SDK and stream implementation
│   │   ├── default_token_factory.rs    # OAuth 2.0 token handling
│   │   ├── errors.rs                   # Error types and retryable logic
│   │   ├── headers_provider.rs         # Trait for custom authentication headers
│   │   ├── stream_configuration.rs     # Stream options
│   │   ├── landing_zone.rs             # Inflight record buffer
│   │   └── offset_generator.rs         # Logical offset tracking
│   ├── zerobus_service.proto           # gRPC protocol definition
│   ├── build.rs                        # Build script for protobuf compilation
│   └── Cargo.toml
│
├── tools/
│   └── generate_files/                 # Schema generation CLI tool
│       ├── src/
│       │   ├── main.rs                 # CLI entry point
│       │   └── generate.rs             # Unity Catalog -> Proto conversion
│       ├── README.md                   # Tool documentation
│       └── Cargo.toml
│
├── examples/
│   ├── README.md                       # Examples documentation
│   ├── json/
│   │   ├── README.md                   # JSON examples documentation
│   │   ├── single/                     # JSON single-record example
│   │   │   ├── src/main.rs
│   │   │   └── Cargo.toml
│   │   └── batch/                      # JSON batch ingestion example
│   │       ├── src/main.rs
│   │       └── Cargo.toml
│   └── proto/
│       ├── README.md                   # Protocol Buffers examples documentation
│       ├── single/                     # Protocol Buffers single-record example
│       │   ├── src/main.rs
│       │   ├── output/                 # Generated schema files
│       │   └── Cargo.toml
│       └── batch/                      # Protocol Buffers batch ingestion example
│           ├── src/main.rs
│           ├── output/                 # Generated schema files
│           └── Cargo.toml
│
├── tests/                              # Integration tests crate
│   ├── src/
│   │   ├── mock_grpc.rs                # Mock Zerobus gRPC server
│   │   └── rust_tests.rs               # Test suite
│   ├── build.rs
│   └── Cargo.toml
│
├── Cargo.toml                          # Workspace configuration
└── README.md                           # This file
```

### Key Components

- **`sdk/`** - The main library crate containing all SDK functionality
- **`tools/`** - CLI tool for generating Protocol Buffer schemas from Unity Catalog tables
- **`examples/`** - Complete working examples demonstrating SDK usage
- **Workspace** - Root `Cargo.toml` defines a Cargo workspace for unified builds

## How It Works

### Architecture Overview

```
+-----------------+
|    Your App     |
+-----------------+
        | 1. create_stream()
        v
+-----------------+
|   ZerobusSdk    |
| - Manages TLS   |
| - Creates       |
|   channels      |
+-----------------+
        | 2. Opens bidirectional gRPC stream
        v
+--------------------------------------+
|            ZerobusStream             |
| +----------------------------------+ |
| |           Supervisor             | | Manages lifecycle, recovery
| +----------------------------------+ |
|                  |                   |
|      +-----------+-----------+       |
|      v                       v       |
| +----------+          +----------+   | 
| |  Sender  |          | Receiver |   | Parallel tasks
| |  Task    |          |  Task    |   |
| +----------+          +----------+   |
|      ^                       |       |
|      |                       v       |
| +----------------------------------+ |
| |          Landing Zone            | | Inflight buffer
| +----------------------------------+ |
+--------------------------------------+
            | 3. gRPC stream
            v
+-----------------------+
|      Databricks       |
|    Zerobus Service    |
+-----------------------+
```

### Data Flow

1. **Ingestion** - Your app calls `stream.ingest_record(data)` or `stream.ingest_records(batch)`
2. **Buffering** - Records are placed in the landing zone with logical offsets
3. **Sending** - Sender task sends records over gRPC with physical offsets
4. **Acknowledgment** - Receiver task gets server ack and resolves the future
5. **Recovery** - If connection fails, supervisor reconnects and resends unacked records

### Authentication Flow

The SDK uses OAuth 2.0 client credentials flow:

1. SDK constructs authorization request with Unity Catalog privileges
2. Sends request to `{uc_endpoint}/oidc/v1/token` with client credentials
3. Token includes scoped permissions for the specific table
4. Token is attached to gRPC metadata as Bearer token
5. Fresh tokens are fetched automatically on each connection

### Custom Authentication

For advanced use cases, you can implement the `HeadersProvider` trait to supply your own authentication headers. This is useful for integrating with a different OAuth provider, using a centralized token caching service, or implementing alternative authentication mechanisms.

> **Note:** The headers you provide must still conform to the authentication protocol expected by the Zerobus service. The default implementation, `OAuthHeadersProvider`, serves as the reference for the required headers (`authorization` and `x-databricks-zerobus-table-name`). This feature provides flexibility in *how* you source your credentials, not in changing the authentication protocol itself.

**Example:**

```rust
use databricks_zerobus_ingest_sdk::*;
use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;

struct MyCustomAuthProvider;

#[async_trait]
impl HeadersProvider for MyCustomAuthProvider {
    async fn get_headers(&self) -> ZerobusResult<HashMap<&'static str, String>> {
        let mut headers = HashMap::new();
        // Custom logic to fetch and cache a token would go here.
        headers.insert("authorization", "Bearer <your-token>".to_string());
        headers.insert("x-databricks-zerobus-table-name", "<your-table-name>".to_string());
        Ok(headers)
    }
}

async fn example(sdk: ZerobusSdk, table_properties: TableProperties) -> ZerobusResult<()> {
    let custom_provider = Arc::new(MyCustomAuthProvider {});
    let stream = sdk.create_stream_with_headers_provider(
        table_properties,
        custom_provider,
        None,
    ).await?;
    Ok(())
}
```

## Usage Guide

The SDK supports two approaches for data serialization:

1. **JSON** - Simpler approach that uses JSON strings. No schema generation required, making it ideal for quick prototyping. See [`examples/README.md`](examples/README.md) for a complete example.
2. **Protocol Buffers** - Type-safe approach with schema validation at compile time. Recommended for production use cases. This guide focuses on the Protocol Buffers approach.

For JSON-based ingestion, you can skip the schema generation step and directly pass JSON strings to `ingest_record()`.

### 1. Generate Protocol Buffer Schema (Protocol Buffers approach only)

> **Important Note**: The schema generation tool and examples are **only available in the GitHub repository**. The crate published on [crates.io](https://crates.io/crates/databricks-zerobus-ingest-sdk) contains only the core Zerobus ingestion SDK logic. To generate protobuf schemas or see working examples, clone the repository:
> 
> ```bash
> git clone https://github.com/databricks/zerobus-sdk-rs.git
> cd zerobus-sdk-rs
> ```

Use the included tool to generate schema files from your Unity Catalog table:

```bash
cd tools/generate_files

# For AWS
cargo run -- \
  --uc-endpoint "https://<your-workspace>.cloud.databricks.com" \
  --client-id "your-client-id" \
  --client-secret "your-client-secret" \
  --table "catalog.schema.table" \
  --output-dir "../../output"

# For Azure
cargo run -- \
  --uc-endpoint "https://<your-workspace>.azuredatabricks.net" \
  --client-id "your-client-id" \
  --client-secret "your-client-secret" \
  --table "catalog.schema.table" \
  --output-dir "../../output"
```

This generates three files:
- `{table}.proto` - Protocol Buffer schema definition
- `{table}.rs` - Rust structs with serialization code
- `{table}.descriptor` - Binary descriptor for runtime validation

See [`tools/generate_files/README.md`](tools/generate_files/README.md) for supported data types and limitations.

See [`examples/README.md`](examples/README.md) for more information on how to get OAuth credentials.

### 2. Initialize the SDK

Create an SDK instance with your Databricks workspace endpoints:

```rust
// For AWS
let sdk = ZerobusSdk::new(
    "https://<your-shard-id>.zerobus.<region>.cloud.databricks.com".to_string(),  // Zerobus endpoint
    "https://<your-workspace>.cloud.databricks.com".to_string(),     // Unity Catalog endpoint
)?;

// For Azure
let sdk = ZerobusSdk::new(
    "https://<your-shard-id>.zerobus.<region>.azuredatabricks.net".to_string(),  // Zerobus endpoint
    "https://<your-workspace>.azuredatabricks.net".to_string(),     // Unity Catalog endpoint
)?;
```

**Note:** The workspace ID is automatically extracted from the Zerobus endpoint when `ZerobusSdk::new()` is called.

### 3. Configure Authentication

The SDK handles authentication automatically. You just need to provide:
- **Client ID** - Your OAuth client ID
- **Client Secret** - Your OAuth client secret
- **Unity Catalog Endpoint** - Passed to SDK constructor
- **Table Name** - Included in table properties

```rust
let client_id = "your-client-id".to_string();
let client_secret = "your-client-secret".to_string();
```

See [`examples/README.md`](examples/README.md) for more information on how to get these credentials.

### 4. Create a Stream

Configure table properties and stream options:

```rust
use std::fs;
use prost::Message;
use prost_types::{FileDescriptorSet, DescriptorProto};

// Load descriptor from generated files
fn load_descriptor(path: &str, file: &str, msg: &str) -> DescriptorProto {
    let bytes = fs::read(path).expect("Failed to read descriptor");
    let file_set = FileDescriptorSet::decode(bytes.as_ref()).unwrap();

    let file_desc = file_set.file.into_iter()
        .find(|f| f.name.as_deref() == Some(file))
        .unwrap();

    file_desc.message_type.into_iter()
        .find(|m| m.name.as_deref() == Some(msg))
        .unwrap()
}

let descriptor_proto = load_descriptor(
    "output/orders.descriptor",
    "orders.proto",
    "table_Orders",
);

let table_properties = TableProperties {
    table_name: "catalog.schema.orders".to_string(),
    descriptor_proto,
};

let options = StreamConfigurationOptions {
    max_inflight_requests: 10000,
    recovery: true,
    recovery_timeout_ms: 15000,
    recovery_backoff_ms: 2000,
    recovery_retries: 4,
    ..Default::default()
};

let mut stream = sdk.create_stream(
    table_properties,
    client_id,
    client_secret,
    Some(options),
).await?;
```

### 5. Ingest Data

The SDK provides flexible ways to ingest data. You can pass records at different levels of abstraction:

- **Auto-serialization** (`ProtoMessage`, `JsonValue`) - Pass your structs directly; the SDK handles encoding/serialization
- **Pre-serialization** (`ProtoBytes`, `JsonString`) - Pass pre-encoded/serialized data with explicit type wrappers
- **Backward-compatible** (raw `Vec<u8>`, `String`) - Pass raw bytes or strings directly without wrappers (works the same as pre-serialization)

#### Single Record Ingestion

Ingest records one at a time.

##### Recommended API: Offset-Based Acknowledgment

**Protocol Buffers:**

```rust
use databricks_zerobus_ingest_sdk::{ProtoMessage, ProtoBytes};
use prost::Message;

let record = YourMessage {
    field1: Some("value".to_string()),
    field2: Some(42),
};

// Auto-encoding: SDK handles encoding.
let offset = stream.ingest_record_offset(ProtoMessage(record.clone())).await?;
stream.wait_for_offset(offset).await?;

// Pre-encoded: explicit wrapper for pre-encoded bytes.
let offset = stream.ingest_record_offset(ProtoBytes(record.encode_to_vec())).await?;
stream.wait_for_offset(offset).await?;

// Backward-compatible: raw Vec<u8> works without wrapper.
let offset = stream.ingest_record_offset(record.encode_to_vec()).await?;
stream.wait_for_offset(offset).await?;
```

**JSON:**

```rust
use databricks_zerobus_ingest_sdk::{JsonValue, JsonString};
use serde::Serialize;

#[derive(Serialize)]
struct MyData { id: i32, name: String }

let data = MyData { id: 1, name: "Alice".to_string() };

// Auto-serializing: SDK handles serialization.
let offset = stream.ingest_record_offset(JsonValue(data)).await?;
stream.wait_for_offset(offset).await?;

// Pre-serialized: explicit wrapper for pre-serialized JSON.
let json = r#"{
    "id": 1,
    "name": "Alice"
}"#.to_string();
let offset = stream.ingest_record_offset(JsonString(json)).await?;
stream.wait_for_offset(offset).await?;

// Backward-compatible: raw String works without wrapper.
let json = r#"{
    "id": 1,
    "name": "Alice"
}"#.to_string();
let offset = stream.ingest_record_offset(json).await?;
stream.wait_for_offset(offset).await?;
```

##### Deprecated API: Future-Based Acknowledgment

**Protocol Buffers:**

```rust
use databricks_zerobus_ingest_sdk::{ProtoMessage, ProtoBytes};
use prost::Message;

let record = YourMessage {
    field1: Some("value".to_string()),
    field2: Some(42),
};

// Auto-encoding: SDK handles encoding.
let ack_future = stream.ingest_record(ProtoMessage(record.clone())).await?;
let offset = ack_future.await?;

// Pre-encoded: explicit wrapper for pre-encoded bytes.
let ack_future = stream.ingest_record(ProtoBytes(record.encode_to_vec())).await?;
let offset = ack_future.await?;

// Backward-compatible: raw Vec<u8> works without wrapper.
let ack_future = stream.ingest_record(record.encode_to_vec()).await?;
let offset = ack_future.await?;
```

**JSON:**

```rust
use databricks_zerobus_ingest_sdk::{JsonValue, JsonString};
use serde::Serialize;

#[derive(Serialize)]
struct MyData { id: i32, name: String }

let data = MyData { id: 1, name: "Alice".to_string() };

// Auto-serializing: SDK handles serialization.
let ack_future = stream.ingest_record(JsonValue(data)).await?;
let offset = ack_future.await?;

// Pre-serialized: explicit wrapper for pre-serialized JSON.
let json = r#"{
    "id": 1,
    "name": "Alice"
}"#.to_string();
let ack_future = stream.ingest_record(JsonString(json)).await?;
let offset = ack_future.await?;

// Backward-compatible: raw String works without wrapper.
let json = r#"{
    "id": 1,
    "name": "Alice"
}"#.to_string();
let ack_future = stream.ingest_record(json).await?;
let offset = ack_future.await?;
```

**Recommended:** Use `ingest_record_offset()` for new code. It provides a cleaner API by returning the offset directly (after queuing), allowing you to use `wait_for_offset()` when you explicitly need to wait for acknowledgment

#### Batch Ingestion

Ingest multiple records at once for higher throughput with all or nothing semantics.

##### Recommended API: Offset-Based Acknowledgment

**Protocol Buffers:**

```rust
use databricks_zerobus_ingest_sdk::{ProtoMessage, ProtoBytes};

// Auto-encoding: SDK handles encoding.
let records: Vec<ProtoMessage<YourMessage>> = vec![
    ProtoMessage(YourMessage { id: Some(1), /* ... */ }),
    ProtoMessage(YourMessage { id: Some(2), /* ... */ }),
    ProtoMessage(YourMessage { id: Some(3), /* ... */ }),
];

// This await queues the batch for sending and returns the offset directly.
// Returns Some(offset) for non-empty batches, None for empty batches.
if let Some(offset_id) = stream.ingest_records_offset(records).await? {
    stream.wait_for_offset(offset_id).await?;
}

// Pre-encoded: explicit wrapper for pre-encoded bytes.
let records: Vec<ProtoBytes> = vec![
    ProtoBytes(YourMessage { id: Some(1), /* ... */ }.encode_to_vec()),
    ProtoBytes(YourMessage { id: Some(2), /* ... */ }.encode_to_vec()),
    ProtoBytes(YourMessage { id: Some(3), /* ... */ }.encode_to_vec()),
];

if let Some(offset_id) = stream.ingest_records_offset(records).await? {
    stream.wait_for_offset(offset_id).await?;
}

// Backward-compatible: raw Vec<u8> works without wrapper.
let records: Vec<Vec<u8>> = vec![
    YourMessage { id: Some(1), /* ... */ }.encode_to_vec(),
    YourMessage { id: Some(2), /* ... */ }.encode_to_vec(),
    YourMessage { id: Some(3), /* ... */ }.encode_to_vec(),
];

if let Some(offset_id) = stream.ingest_records_offset(records).await? {
    stream.wait_for_offset(offset_id).await?;
}
```

**JSON:**

```rust
use databricks_zerobus_ingest_sdk::{JsonValue, JsonString};
use serde::Serialize;

#[derive(Serialize)]
struct MyData { id: i32, name: String }

// Auto-serializing: SDK handles serialization.
let records: Vec<JsonValue<MyData>> = vec![
    JsonValue(MyData { id: 1, name: "Alice".to_string() }),
    JsonValue(MyData { id: 2, name: "Bob".to_string() }),
    JsonValue(MyData { id: 3, name: "Carol".to_string() }),
];

if let Some(offset_id) = stream.ingest_records_offset(records).await? {
    stream.wait_for_offset(offset_id).await?;
}

// Pre-serialized: explicit wrapper for pre-serialized JSON.
let records: Vec<JsonString> = vec![
    JsonString(r#"{
        "id": 1,
        "name": "Alice"
    }"#.to_string()),
    JsonString(r#"{
        "id": 2,
        "name": "Bob"
    }"#.to_string()),
    JsonString(r#"{
        "id": 3,
        "name": "Carol"
    }"#.to_string()),
];

if let Some(offset_id) = stream.ingest_records_offset(records).await? {
    stream.wait_for_offset(offset_id).await?;
}

// Backward-compatible: raw String works without wrapper.
let records: Vec<String> = vec![
    r#"{
        "id": 1,
        "name": "Alice"
    }"#.to_string(),
    r#"{
        "id": 2,
        "name": "Bob"
    }"#.to_string(),
    r#"{
        "id": 3,
        "name": "Carol"
    }"#.to_string(),
];

if let Some(offset_id) = stream.ingest_records_offset(records).await? {
    stream.wait_for_offset(offset_id).await?;
}
```

##### Deprecated API: Future-Based Acknowledgment

**Protocol Buffers:**

```rust
use databricks_zerobus_ingest_sdk::{ProtoMessage, ProtoBytes};

// Auto-encoding: SDK handles encoding.
let records: Vec<ProtoMessage<YourMessage>> = vec![
    ProtoMessage(YourMessage { id: Some(1), /* ... */ }),
    ProtoMessage(YourMessage { id: Some(2), /* ... */ }),
];

if let Some(ack_future) = stream.ingest_records(records).await? {
    let offset_id = ack_future.await?;
}

// Pre-encoded: explicit wrapper for pre-encoded bytes.
let records: Vec<ProtoBytes> = vec![
    ProtoBytes(YourMessage { id: Some(1), /* ... */ }.encode_to_vec()),
    ProtoBytes(YourMessage { id: Some(2), /* ... */ }.encode_to_vec()),
];

if let Some(ack_future) = stream.ingest_records(records).await? {
    let offset_id = ack_future.await?;
}

// Backward-compatible: raw Vec<u8> works without wrapper.
let records: Vec<Vec<u8>> = vec![
    YourMessage { id: Some(1), /* ... */ }.encode_to_vec(),
    YourMessage { id: Some(2), /* ... */ }.encode_to_vec(),
];

if let Some(ack_future) = stream.ingest_records(records).await? {
    let offset_id = ack_future.await?;
}
```

**JSON:**

```rust
use databricks_zerobus_ingest_sdk::{JsonValue, JsonString};
use serde::Serialize;

#[derive(Serialize)]
struct MyData { id: i32, name: String }

// Auto-serializing: SDK handles serialization.
let records: Vec<JsonValue<MyData>> = vec![
    JsonValue(MyData { id: 1, name: "Alice".to_string() }),
    JsonValue(MyData { id: 2, name: "Bob".to_string() }),
];

if let Some(ack_future) = stream.ingest_records(records).await? {
    let offset_id = ack_future.await?;
}

// Pre-serialized: explicit wrapper for pre-serialized JSON.
let records: Vec<JsonString> = vec![
    JsonString(r#"{
        "id": 1,
        "name": "Alice"
    }"#.to_string()),
    JsonString(r#"{
        "id": 2,
        "name": "Bob"
    }"#.to_string()),
];

if let Some(ack_future) = stream.ingest_records(records).await? {
    let offset_id = ack_future.await?;
}

// Backward-compatible: raw String works without wrapper.
let records: Vec<String> = vec![
    r#"{
        "id": 1,
        "name": "Alice"
    }"#.to_string(),
    r#"{
        "id": 2,
        "name": "Bob"
    }"#.to_string(),
];

if let Some(ack_future) = stream.ingest_records(records).await? {
    let offset_id = ack_future.await?;
}
```

**Batch API Semantics:**
- **All-or-nothing**: The entire batch succeeds or fails as a unit. If any record in the batch fails, the entire batch is rejected.
- **Atomic acknowledgment**: You receive a single acknowledgment for the entire batch, not individual records.
- **Better throughput**: Reduces network overhead by sending multiple records in a single request.
- **Empty batches**: Ingesting an empty batch is a no-op. The future resolves to `None`.
- **Preserved on failure**: Batches are preserved as units when retrieving via `get_unacked_batches()` or when reingested via `recreate_stream()`. Note that `get_unacked_records()` flattens batches into individual records.

**High throughput patterns:**

With `ingest_record_offset()` and `ingest_records_offset()`, you can ingest many records without immediately waiting for acknowledgments. Use `flush()` to periodically wait for all pending acknowledgments:

```rust
use databricks_zerobus_ingest_sdk::ProtoMessage;

// Example with single-record ingestion.
for i in 0..100_000 {
    let record = YourMessage {
        id: Some(i),
        timestamp: Some(chrono::Utc::now().timestamp()),
        data: Some(format!("record-{}", i)),
    };

    // This await only waits for the record to be queued.
    let _offset = stream.ingest_record_offset(ProtoMessage(record)).await?;

    // Periodically flush and wait for acks to avoid unbounded memory growth.
    if (i + 1) % 10_000 == 0 {
        stream.flush().await?;
    }
}

// Flush and wait for remaining acknowledgments.
stream.flush().await?;

// Same pattern works for batch ingestion.
let batches: Vec<Vec<ProtoMessage<YourMessage>>> = vec![/* batch1 */, /* batch2 */, /* batch3 */];
for batch in batches {
    let _offset = stream.ingest_records_offset(batch).await?;
}

stream.flush().await?;
```

**Parallelizing with multiple streams:**

Since each stream uses a single gRPC connection, opening multiple threads on the same stream doesn't improve throughput. For true parallelization, open multiple streams (e.g., partition your data):

```rust
use tokio::task::JoinSet;
use databricks_zerobus_ingest_sdk::ProtoMessage;

let mut tasks = JoinSet::new();

// Partition data across multiple streams for parallel ingestion.
for partition in 0..4 {
    let sdk_clone = sdk.clone();
    let table_properties = table_properties.clone();
    let client_id = client_id.clone();
    let client_secret = client_secret.clone();

    tasks.spawn(async move {
        let mut stream = sdk_clone.create_stream(
            table_properties,
            client_id,
            client_secret,
            None,
        ).await?;

        // Ingest partition data, using single-record or batch ingestion.
        for i in (partition * 25_000)..((partition + 1) * 25_000) {
            let record = YourMessage { id: Some(i), /* ... */ };
            let _offset = stream.ingest_record_offset(ProtoMessage(record)).await?;
        }

        // Close implicitly waits for all acknowledgments from the server.
        stream.close().await?;
        Ok::<_, ZerobusError>(())
    });
}

// Wait for all streams to complete.
while let Some(result) = tasks.join_next().await {
    result??;
}
```

### 6. Handle Acknowledgments

The recommended `ingest_record_offset()` and `ingest_records_offset()` methods return offsets directly (after queuing):
- `ingest_record_offset()` returns `OffsetId` (the logical offset)
- `ingest_records_offset()` returns `Option<OffsetId>` (None if the batch is empty)

```rust
// Ingest and get offset, after queuing the record.
let offset_id = stream.ingest_record_offset(data).await?;
println!("Record sent with offset Id: {}", offset_id);

// Wait for acknowledgment when needed.
stream.wait_for_offset(offset_id).await?;
println!("Record committed at offset: {}", offset_id);

// For batches, the method returns Option<OffsetId>.
// None if the batch is empty.
let batch = vec![data1, data2, data3];
if let Some(offset_id) = stream.ingest_records_offset(batch).await? {
    println!("Batch sent with last offset: {}", offset_id);
    stream.wait_for_offset(offset_id).await?;
    println!("Batch committed");
} else {
    println!("Empty batch, no records ingested");
}

// High-throughput: collect offsets and wait selectively.
let mut offsets = Vec::new();
for i in 0..1000 {
    let offset = stream.ingest_record_offset(record).await?;
    offsets.push(offset);
}
// Wait for specific offsets as needed.
for offset in offsets {
    stream.wait_for_offset(offset).await?;
}

// Or use flush() to wait for all pending acknowledgments at once.
stream.flush().await?;
```

### 7. Close the Stream

Always close streams to ensure data is flushed:

```rust
// Close gracefully (flushes automatically)
stream.close().await?;
```

If the stream fails, retrieve unacknowledged records:

```rust
match stream.close().await {
    Err(_) => {
        // Option 1: Get individual records (flattened)
        let unacked = stream.get_unacked_records().await?;
        let total_records = unacked.count();
        println!("Failed to ack {} records", total_records);
        
        // Option 2: Get records grouped by batch (preserves batch structure)
        let unacked_batches = stream.get_unacked_batches().await?;
        let total_records: usize = unacked_batches.iter().map(|batch| batch.get_record_count()).sum();
        println!("Failed to ack {} records in {} batches", total_records, unacked_batches.len());
        
        // Retry with a new stream
    }
    Ok(_) => println!("Stream closed successfully"),
}
```

## Configuration Options

### StreamConfigurationOptions

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_inflight_requests` | `usize` | 1,000,000 | Maximum unacknowledged requests in flight |
| `recovery` | `bool` | true | Enable automatic stream recovery on failure |
| `recovery_timeout_ms` | `u64` | 15,000 | Timeout for recovery operations (ms) |
| `recovery_backoff_ms` | `u64` | 2,000 | Delay between recovery retry attempts (ms) |
| `recovery_retries` | `u32` | 4 | Maximum number of recovery attempts |
| `server_lack_of_ack_timeout_ms` | `u64` | 60,000 | Timeout waiting for server acks (ms) |
| `flush_timeout_ms` | `u64` | 300,000 | Timeout for flush operations (ms) |
| `record_type` | `RecordType` | `RecordType::Proto` | Record serialization format (Proto or Json) |
| `stream_paused_max_wait_time_ms` | `Option<u64>` | `None` | Max time to wait during graceful close (`None` = full server duration, `Some(0)` = immediate, `Some(x)` = min(x, server_duration)) |

**Example:**

```rust
let options = StreamConfigurationOptions {
    max_inflight_requests: 50000,
    recovery: true,
    recovery_timeout_ms: 20000,
    recovery_retries: 5,
    flush_timeout_ms: 600000,
    ..Default::default()
};
```

## Error Handling

The SDK categorizes errors as **retryable** or **non-retryable**:

### Retryable Errors
Auto-recovered if `recovery` is enabled:
- Network failures
- Connection timeouts
- Temporary server errors
- Stream closed by server

### Non-Retryable Errors
Require manual intervention:
- `InvalidUCTokenError` - Invalid OAuth credentials
- `InvalidTableName` - Table doesn't exist or invalid format
- `InvalidArgument` - Invalid parameters or schema mismatch
- `Code::Unauthenticated` - Authentication failure
- `Code::PermissionDenied` - Insufficient table permissions
- `ChannelCreationError` - Failed to establish TLS connection

**Check if an error is retryable:**

```rust
match stream.ingest_record(payload).await {
    Ok(ack) => {
        let offset = ack.await?;
    }
    Err(e) if e.is_retryable() => {
        eprintln!("Retryable error, SDK will auto-recover: {}", e);
    }
    Err(e) => {
        eprintln!("Fatal error, manual intervention needed: {}", e);
        return Err(e.into());
    }
}
```

## Examples

### Complete Working Examples

The `examples/` directory contains four working examples covering different serialization formats and ingestion patterns:

| Example | Serialization | Ingestion | Package |
|---------|--------------|-----------|---------|
| `json/single/` | JSON | Single-record | `example_json_single` |
| `json/batch/` | JSON | Batch | `example_json_batch` |
| `proto/single/` | Protocol Buffers | Single-record | `example_proto_single` |
| `proto/batch/` | Protocol Buffers | Batch | `example_proto_batch` |


Check [`examples/README.md`](examples/README.md) for setup instructions and detailed comparisons.

### Stream Recovery

```rust
let sdk = ZerobusSdk::new(endpoint, uc_endpoint);

let mut stream = sdk.create_stream(
    table_properties.clone(),
    client_id.clone(),
    client_secret.clone(),
    Some(options),
).await?;

// Ingest data...
match stream.close().await {
    Err(_) => {
        // Stream failed, recreate with unacked records
        stream = sdk.recreate_stream(stream).await?;
    }
    Ok(_) => println!("Closed successfully"),
}
```

## Tests

Integration tests live in the `tests/` crate and run against a lightweight mock Zerobus gRPC server.

- Mock server: `tests/src/mock_grpc.rs`
- Test suite: `tests/src/rust_tests.rs`

Run tests with logs:

```bash
cargo test -p tests -- --nocapture
```

## Best Practices

1. **Reuse SDK Instances** - Create one `ZerobusSdk` per application and reuse for multiple streams
2. **Always Close Streams** - Use `stream.close().await?` to ensure all data is flushed
3. **Choose the Right Ingestion Method**:
   - Use `ingest_records_offset()` for high throughput batch ingestion
   - Use `ingest_record_offset()` when processing records individually
   - Both return offsets directly; use `wait_for_offset()` to explicitly wait for acknowledgments
   - The older `ingest_record()` and `ingest_records()` methods are deprecated
4. **Tune Inflight Limits** - Adjust `max_inflight_requests` based on memory and throughput needs
5. **Enable Recovery** - Always set `recovery: true` in production environments
6. **Handle Ack Futures** - Use `tokio::spawn` for fire-and-forget or batch-wait for verification
7. **Monitor Errors** - Log and alert on non-retryable errors
8. **Validate Schemas** - Use the schema generation tool to ensure type safety (for Protocol Buffers)
9. **Secure Credentials** - Never hardcode secrets; use environment variables or secret managers
10. **Test Recovery** - Simulate failures to verify your error handling logic

## API Reference

### `ZerobusSdk`

Main entry point for the SDK.

**Constructor:**
```rust
pub fn new(zerobus_endpoint: String, unity_catalog_url: String) -> ZerobusResult<Self>
```

**Methods:**
```rust
pub async fn create_stream(
    &self,
    table_properties: TableProperties,
    client_id: String,
    client_secret: String,
    options: Option<StreamConfigurationOptions>,
) -> ZerobusResult<ZerobusStream>
```

```rust
pub async fn recreate_stream(
    &self,
    stream: ZerobusStream
) -> ZerobusResult<ZerobusStream>
```
Recreates a failed stream, preserving and re-ingesting unacknowledged records.

```rust
pub async fn create_stream_with_headers_provider(
    &self,
    table_properties: TableProperties,
    headers_provider: Arc<dyn HeadersProvider>,
    options: Option<StreamConfigurationOptions>,
) -> ZerobusResult<ZerobusStream>
```
Creates a stream with a custom headers provider for advanced authentication.

### `ZerobusStream`

Represents an active ingestion stream.

**Methods:**
```rust
pub async fn ingest_record_offset(
    &self,
    payload: impl Into<EncodedRecord>
) -> ZerobusResult<OffsetId>
```
Ingests a single encoded record (Protocol Buffers or JSON). The await queues the record for sending and returns the logical offset ID directly. Use `wait_for_offset()` to explicitly wait for server acknowledgment of this offset.

```rust
pub async fn ingest_records_offset(
    &self,
    payloads: Vec<impl Into<EncodedRecord>>
) -> ZerobusResult<Option<OffsetId>>
```
Ingests multiple encoded records as a batch with all-or-nothing semantics. The entire batch either succeeds or fails as a unit. The await queues the batch for sending and returns the logical offset ID directly (or `None` for empty batches). Use `wait_for_offset()` to explicitly wait for server acknowledgment.

```rust
pub async fn ingest_record(
    &self,
    payload: Vec<u8>
) -> ZerobusResult<impl Future<Output = ZerobusResult<OffsetId>>>>
```
**Deprecated:** Use `ingest_record_offset()` instead. Returns a future that resolves to the offset ID.

```rust
pub async fn ingest_records(
    &self,
    payloads: Vec<Vec<u8>>
) -> ZerobusResult<impl Future<Output = ZerobusResult<Option<OffsetId>>>>
```
**Deprecated:** Use `ingest_records_offset()` instead. Returns a future that resolves to `Some(offset_id)` for non-empty batches, or `None` if the batch is empty.

```rust
pub async fn wait_for_offset(&self, offset_id: OffsetId) -> ZerobusResult<()>
```
Waits for acknowledgment of a specific logical offset. Use this method with offsets returned from `ingest_record_offset()` or `ingest_records_offset()` to explicitly wait for server acknowledgment.

```rust
pub async fn flush(&self) -> ZerobusResult<()>
```
Flushes all pending records and waits for acknowledgment.

```rust
pub async fn close(&mut self) -> ZerobusResult<()>
```
Flushes and closes the stream gracefully.

```rust
pub async fn get_unacked_records(&self) -> ZerobusResult<impl Iterator<Item = EncodedRecord>>
```
Returns an iterator over all unacknowledged records as individual `EncodedRecord` items. This flattens batches into individual records. Only call after stream failure.

```rust
pub async fn get_unacked_batches(&self) -> ZerobusResult<Vec<EncodedBatch>>
```
Returns unacknowledged records grouped by batch, preserving the original batch structure. Records ingested together remain grouped:
- Each `ingest_record()` call creates a batch containing one record
- Each `ingest_records()` call creates a batch containing multiple records

Only call after stream failure.

### `TableProperties`

Configuration for the target table.

**Fields:**
```rust
pub struct TableProperties {
    pub table_name: String,
    pub descriptor_proto: Option<prost_types::DescriptorProto>,
}
```

- `table_name` - Full table name (e.g., "catalog.schema.table")
- `descriptor_proto` - Optional Protocol buffer descriptor loaded from generated files (required for Proto record type, None for JSON)

### `StreamConfigurationOptions`

Stream behavior configuration.

**Fields:**
```rust
pub struct StreamConfigurationOptions {
    pub max_inflight_requests: usize,
    pub recovery: bool,
    pub recovery_timeout_ms: u64,
    pub recovery_backoff_ms: u64,
    pub recovery_retries: u32,
    pub server_lack_of_ack_timeout_ms: u64,
    pub flush_timeout_ms: u64,
    pub record_type: RecordType,
    pub stream_paused_max_wait_time_ms: Option<u64>,
}
```

See [Configuration Options](#configuration-options) for details.

### `ZerobusError`

Error type for all SDK operations.

**Methods:**
```rust
pub fn is_retryable(&self) -> bool
```
Returns `true` if the error can be automatically recovered by the SDK.

## Building from Source

For contributors or those who want to build and test the SDK:

```bash
git clone https://github.com/YOUR_USERNAME/zerobus_rust_sdk.git
cd zerobus_rust_sdk
cargo build --workspace
```

**Build specific components:**

```bash
# Build only SDK
cargo build -p databricks-zerobus-ingest-sdk

# Build only schema tool
cargo build -p generate_files

# Build and run JSON single-record example
cargo run -p example_json_single

# Build and run JSON batch example
cargo run -p example_json_batch

# Build and run Protocol Buffers single-record example
cargo run -p example_proto_single

# Build and run Protocol Buffers batch example
cargo run -p example_proto_batch
```

## Community and Contributing

This is an open source project. We welcome contributions, feedback, and bug reports.

- **[Contributing Guide](CONTRIBUTING.md)**: Learn how to contribute, including our development process and coding style.
- **[Changelog](CHANGELOG.md)**: See the history of changes in the SDK.
- **[Security Policy](SECURITY.md)**: Read about our security process and how to report vulnerabilities.
- **[Developer Certificate of Origin (DCO)](DCO)**: Understand the agreement for contributions.
- **[Open Source Attributions](NOTICE)**: See a list of the open source libraries we use.

## License

This SDK is licensed under the Databricks License. See the [LICENSE](LICENSE) file for the full license text. The license is also available online at [https://www.databricks.com/legal/db-license](https://www.databricks.com/legal/db-license).

## Requirements

- **Rust** 1.70 or higher (2021 edition)
- **Databricks** workspace with Zerobus access enabled
- **OAuth 2.0** client credentials (client ID and secret)
- **Unity Catalog** endpoint access
- **TLS** - Uses native OS certificate store


---

For issues, questions, or contributions, please visit the [GitHub repository](https://github.com/databricks/zerobus-sdk-rs).
