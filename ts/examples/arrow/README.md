# Arrow Flight Examples

**Experimental/Unsupported**: Arrow Flight support is experimental and not yet supported for production use. The API may change in future releases.

## Enabling Arrow Support

Arrow Flight support is **disabled by default**. To enable it, you must build the SDK with the `arrow-flight` feature:

```bash
# Build with Arrow support
npm run build:arrow

# Or for debug builds
npm run build:debug:arrow
```

This directory contains examples demonstrating how to use Arrow Flight for high-performance columnar data ingestion.

## Overview

Arrow Flight provides efficient columnar data transfer using the Apache Arrow format. It's ideal for:
- Analytics workloads with large datasets
- Interoperability with data science tools (pandas, PyArrow, etc.)
- High-throughput columnar data ingestion

## Examples

### Single Record Example (`single.ts`)

Demonstrates ingesting individual Arrow IPC buffers:

```bash
npm run example:arrow:single
```

**Key patterns:**
- Creating Arrow tables with `tableFromArrays()`
- Serializing to IPC format with `tableToIPC(table, 'stream')`
- Using `ingestBatch()` for single records
- High-throughput: send many, wait for last offset

### Batch Example (`batch.ts`)

Demonstrates efficient batch ingestion with multiple rows per Arrow batch:

```bash
npm run example:arrow:batch
```

**Key patterns:**
- Batching multiple rows in a single Arrow table (most efficient)
- Large batch ingestion (100+ rows per batch)
- Multiple batches with single acknowledgment wait

## Prerequisites

1. Install apache-arrow:
   ```bash
   npm install apache-arrow
   ```

2. Set environment variables (see main README)

## Schema Definition

Arrow Flight streams require an explicit schema definition using `ArrowTableProperties`:

```typescript
import { ArrowTableProperties, ArrowDataType } from '@databricks/zerobus-ingest-sdk';

const tableProperties: ArrowTableProperties = {
    tableName: 'catalog.schema.table',
    schemaFields: [
        { name: 'device_name', dataType: ArrowDataType.Utf8, nullable: false },
        { name: 'temp', dataType: ArrowDataType.Int32, nullable: false },
        { name: 'humidity', dataType: ArrowDataType.Int64, nullable: false }
    ]
};
```

### Available Data Types

| ArrowDataType | Description |
|---------------|-------------|
| `Boolean` | Boolean values |
| `Int8`, `Int16`, `Int32`, `Int64` | Signed integers |
| `UInt8`, `UInt16`, `UInt32`, `UInt64` | Unsigned integers |
| `Float32`, `Float64` | Floating point |
| `Utf8`, `LargeUtf8` | Strings |
| `Binary`, `LargeBinary` | Binary data |
| `Date32`, `Date64` | Dates |
| `TimestampMicros`, `TimestampNanos` | Timestamps |

## Creating Arrow Data

Use the `apache-arrow` package to create Arrow tables. **Important:** You must use explicit vector types to match the server schema:

```typescript
import {
    tableToIPC,
    makeVector,
    LargeUtf8,
    Int32,
    Int64,
    makeTable,
} from 'apache-arrow';

// Create vectors with explicit types (must match table schema)
const deviceNameVector = makeVector({ type: new LargeUtf8(), values: ['sensor-1', 'sensor-2'] });
const tempVector = makeVector({ type: new Int32(), values: [22, 23] });
const humidityVector = makeVector({ type: new Int64(), values: [BigInt(65), BigInt(67)] });

const table = makeTable({
    device_name: deviceNameVector,
    temp: tempVector,
    humidity: humidityVector,
});

// Serialize to IPC stream format
const ipcBuffer = tableToIPC(table, 'stream');

// Ingest into Arrow stream
const offset = await arrowStream.ingestBatch(Buffer.from(ipcBuffer));
await arrowStream.waitForOffset(offset);
```

**Note:** Do not use `tableFromArrays()` as it infers types that may not match the server schema (e.g., strings become Dictionary types, numbers become Float64).

## Performance Tips

1. **Batch rows together**: Arrow is columnar, so batching many rows in a single table is more efficient than individual records.

2. **Use appropriate batch sizes**: For most workloads, 1,000-10,000 rows per batch provides good throughput.

3. **Enable compression**: Use `ipcCompression` option for network-bound workloads:
   ```typescript
   const options: ArrowStreamConfigurationOptions = {
       ipcCompression: IpcCompressionType.Lz4Frame, // or Zstd
   };
   ```

4. **Wait for last offset**: When ingesting multiple batches, wait only for the last offset.

## Error Handling

```typescript
try {
    const offset = await arrowStream.ingestBatch(ipcBuffer);
    await arrowStream.waitForOffset(offset);
} catch (error) {
    // Get unacknowledged batches for retry
    const unacked = await arrowStream.getUnackedBatches();

    // Recreate stream and re-ingest
    const newStream = await sdk.recreateArrowStream(arrowStream);
    for (const batch of unacked) {
        await newStream.ingestBatch(batch);
    }
}
```
