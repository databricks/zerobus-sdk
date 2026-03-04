# Zerobus Go SDK Examples

This directory contains examples demonstrating how to use the Zerobus Go SDK to ingest data into Databricks Delta tables.

## Available Examples

Examples are organized by data format (JSON vs Protocol Buffers) and ingestion pattern (single vs batch):

### JSON Examples
**Recommended for getting started** - Simpler examples that use JSON for data serialization.

#### Single Record Ingestion
- Location: `examples/json/single/main.go`
- Ingests records one at a time using `IngestRecordOffset()`
- No schema generation required
- Direct JSON string ingestion
- Great for quick prototyping

#### Batch Ingestion
- Location: `examples/json/batch/main.go`
- Ingests multiple records at once using `IngestRecordsOffset()`
- Optimized for high-throughput scenarios
- Shows batch offset handling and waiting

### Protocol Buffers Examples
More advanced examples that use Protocol Buffers for type-safe data serialization.

#### Single Record Ingestion
- Location: `examples/proto/single/main.go`
- Ingests records one at a time with protobuf
- Type-safe record creation
- Better for production use cases

#### Batch Ingestion
- Location: `examples/proto/batch/main.go`
- Ingests multiple protobuf records at once
- Most efficient for high-volume scenarios
- Combines type safety with batch performance

## Common Features

All examples demonstrate the complete ingestion workflow:

1. **Creating a stream** - Establish a connection to the Zerobus server
2. **Sending records** - Queue records for ingestion and receive offsets
3. **Getting offsets** - Each record gets a logical sequence number (offset)
4. **Waiting for acknowledgments** - Confirm the server has durably written specific records
5. **Error handling** - Detect and handle failures during ingestion
6. **Closing the stream** - Gracefully shut down and ensure all data is persisted

---

## Prerequisites

### 1. Create a Databricks Table

First, create a table in your Databricks workspace using the following SQL:

```sql
CREATE TABLE catalog.schema.air_quality (
  device_name STRING,
  temp INT,
  humidity BIGINT
);
```

Replace `catalog.schema.air_quality` with your actual catalog, schema, and table name.

**Note:** This schema matches the examples. You can modify it for your use case, but make sure to update the example code accordingly.

### 2. Set Up OAuth Service Principal

You'll need a Databricks service principal with OAuth credentials:

1. In your Databricks workspace, go to **Settings** → **Identity and Access**
2. Create a service principal or use an existing one
3. Generate OAuth credentials (client ID and secret)
4. Grant the service principal the following permissions on your table:
   - `SELECT` - Read table schema
   - `MODIFY` - Write data to the table
   - `USE CATALOG` and `USE SCHEMA` - Access catalog and schema

### 3. Configure Credentials

All examples require the same environment variables. You'll need to set these before running:

```bash
export ZEROBUS_SERVER_ENDPOINT="https://workspace-id.zerobus.region.cloud.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"
export ZEROBUS_TABLE_NAME="catalog.schema.air_quality"
```

**How to get these values:**

- **ZEROBUS_SERVER_ENDPOINT** - Zerobus ingestion endpoint for your workspace
  - **AWS**: `https://<workspace-id>.zerobus.<region>.cloud.databricks.com`
  - **Azure**: `https://<workspace-id>.zerobus.<region>.azuredatabricks.net`
  - Ask your Databricks account team for the correct endpoint

- **DATABRICKS_WORKSPACE_URL** - Your Databricks workspace URL (Unity Catalog endpoint)
  - This is the URL you use to access your Databricks workspace
  - **AWS**: `https://<workspace>.cloud.databricks.com`
  - **Azure**: `https://<workspace>.azuredatabricks.net`

- **DATABRICKS_CLIENT_ID** - OAuth 2.0 client ID from your service principal
  - Found in Settings → Identity and Access → Service Principals
  - Example: `a1b2c3d4-e5f6-7890-abcd-ef1234567890`

- **DATABRICKS_CLIENT_SECRET** - OAuth 2.0 client secret from your service principal
  - Generated when creating OAuth credentials for the service principal
  - This is only shown once, so save it securely

- **ZEROBUS_TABLE_NAME** - Full table name in format `catalog.schema.table`
  - Example: `main.ingestion.air_quality`

---

## Running JSON Examples

JSON examples are simpler and don't require schema generation.

### Single Record Example

```bash
# Set credentials (same for all examples)
export ZEROBUS_SERVER_ENDPOINT="https://workspace-id.zerobus.region.cloud.databricks.com"
export DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"
export ZEROBUS_TABLE_NAME="catalog.schema.air_quality"

# Run the example
cd examples/json/single
go run main.go
```

Expected output shows 5 records ingested sequentially with offsets 0-4.

### Batch Example

```bash
# Set credentials (if not already set)
# Run the batch example
cd examples/json/batch
go run main.go
```

Expected output shows a batch of 5 records ingested at once, returning the last offset.

---

## Running Protocol Buffers Examples

Protocol Buffers examples provide type safety and better performance but require schema generation.

### Step 1: Install Protocol Buffer Compiler

```bash
# Ubuntu/Debian
sudo apt-get install protobuf-compiler

# macOS
brew install protobuf

# Install Go plugin
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
```

### Step 2: Generate Go Code (if needed)

The proto code is already generated, but if you need to regenerate:

```bash
cd examples/proto
./generate_proto.sh
```

This creates `pb/air_quality.pb.go` with generated Go structs.

### Step 3: Run Examples

**Single Record Example:**
```bash
# Set credentials
cd examples/proto/single
go run main.go
```

**Batch Example:**
```bash
cd examples/proto/batch
go run main.go
```

---

## Key Differences Between Examples

### Single vs Batch
- **Single**: Call `IngestRecordOffset()` for each record individually
- **Batch**: Call `IngestRecordsOffset()` with a slice of records for better throughput

### JSON vs Protocol Buffers
| Feature | JSON | Protocol Buffers |
|---------|------|------------------|
| Setup | No schema generation needed | Requires protoc and code generation |
| Type Safety | Runtime validation only | Compile-time type checking |
| Performance | Text-based encoding | Efficient binary encoding |
| Best For | Prototyping, simple use cases | Production, high-throughput |

---

## Code Pattern Comparison

### Single Record Ingestion

**JSON:**
```go
jsonRecord := `{"device_name": "sensor-001", "temp": 20, "humidity": 60}`
offset, err := stream.IngestRecordOffset(jsonRecord)
```

**Protocol Buffers:**
```go
message := &pb.AirQuality{
    DeviceName: proto.String("sensor-001"),
    Temp:       proto.Int32(20),
    Humidity:   proto.Int64(60),
}
data, _ := proto.Marshal(message)
offset, err := stream.IngestRecordOffset(data)
```

### Batch Ingestion

**JSON:**
```go
records := []interface{}{
    `{"device_name": "sensor-001", "temp": 20, "humidity": 60}`,
    `{"device_name": "sensor-002", "temp": 21, "humidity": 61}`,
}
batchOffset, err := stream.IngestRecordsOffset(records)
```

**Protocol Buffers:**
```go
var records []interface{}
for i := 0; i < 5; i++ {
    message := &pb.AirQuality{...}
    data, _ := proto.Marshal(message)
    records = append(records, data)
}
batchOffset, err := stream.IngestRecordsOffset(records)
```

---

## Understanding Offsets and Acknowledgments

The SDK uses a two-phase process for ingestion:

### Phase 1: Send Records

When you call `IngestRecordOffset()` or `IngestRecordsOffset()`:
1. Your record is queued for transmission
2. An offset (sequence number) is assigned
3. The offset is returned
4. The SDK transmits the record to the server in the background

You don't wait for the server yet - you can continue sending more records or doing other work.

### Phase 2: Wait for Acknowledgment (Blocking)

When you call `WaitForOffset(offset)`:
1. **Blocks** until the server confirms that record is durably written
2. Returns `nil` on success
3. Returns an error if the record fails

This is optional - you only wait when you need confirmation.

**Single record example:**
```go
// Phase 1: Send and get offset (returns immediately)
offset, _ := stream.IngestRecordOffset(data)
log.Printf("Record queued with offset %d", offset)

// Do other work here if needed...

// Phase 2: Wait for server confirmation (blocks until confirmed)
if err := stream.WaitForOffset(offset); err != nil {
    log.Printf("Record failed: %v", err)
} else {
    log.Println("Server confirmed record is durable")
}
```

**Batch example:**
```go
// Phase 1: Send batch (returns immediately with one offset)
batchOffset, _ := stream.IngestRecordsOffset(records)
log.Printf("Batch queued with offset: %d", batchOffset)

// Do other work here if needed...

// Phase 2: Wait for confirmation (blocks until entire batch is confirmed)
if err := stream.WaitForOffset(batchOffset); err != nil {
    log.Printf("Batch failed: %v", err)
} else {
    log.Println("Entire batch confirmed by server")
}
```

---

## Retrieving Unacknowledged Records After Failure

The `GetUnackedRecords()` method allows you to retrieve records that weren't acknowledged after a stream closes or fails. This is useful for implementing retry logic.

**IMPORTANT:** `GetUnackedRecords()` can only be called after the stream has closed or failed. Calling it on an active stream will return an error.

```go
// Send records
for i := 0; i < 100; i++ {
    stream.IngestRecordOffset(data)
}

// Try to close the stream
if err := stream.Close(); err != nil {
    // Stream failed - check for unacknowledged records
    unacked, err := stream.GetUnackedRecords()
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("%d records failed to be acknowledged", len(unacked))

    // Retry with a new stream
    newStream, _ := sdk.CreateStream(tableProps, clientID, clientSecret, options)
    for _, record := range unacked {
        newStream.IngestRecordOffset(record)
    }
    newStream.Close()
}
```

This is useful for implementing custom retry logic and ensuring no data is lost after stream failures.

---

## Adapting for Your Custom Table

### For JSON Examples

Simply modify the JSON string to match your table's schema:

```go
jsonRecord := `{
    "your_field_1": "value1",
    "your_field_2": 123,
    "your_field_3": true
}`
```

### For Protocol Buffers Examples

Create a `.proto` file matching your table schema and regenerate code:

```protobuf
syntax = "proto2";
package examples;
option go_package = "zerobus-examples/pb";

message MyTable {
    optional string field1 = 1;
    optional int32 field2 = 2;
}
```

Then generate: `protoc --go_out=. --go_opt=paths=source_relative my_table.proto`

---

## Performance Tips

- Use **batch ingestion** for better throughput when ingesting many records
- Create **multiple streams** for parallel ingestion across goroutines
- Use **Protocol Buffers** for production scenarios with high volume
- Start with **JSON single** for quick prototyping

---

## Next Steps

- Try modifying examples for your table schema
- Experiment with different batch sizes
- Test concurrent ingestion with multiple goroutines
- Implement error handling and retry logic for production

## Additional Resources

- [Main SDK Documentation](../README.md)
- [API Reference](../README.md#api-reference)
- [Protocol Buffers Documentation](https://protobuf.dev/)
