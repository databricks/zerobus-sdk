# Avro Examples (Beta)

Avro-based ingestion into Databricks Delta tables with the Zerobus TypeScript SDK.

> **Beta.** The feature is in development. Avro is compiled into
> the published binaries by default. Object encoding uses the optional [`avsc`](https://www.npmjs.com/package/avsc)
> dependency — `npm install avsc`. The pre-encoded-`Buffer` path needs no dependency.

## Overview

Declare the Avro **writer schema** (JSON) once, on `TableProperties.avroSchema`. This selects
the Avro record format. Then ingest through the usual `ingestRecordOffset` /
`ingestRecordsOffset`, passing either:

| Approach | Type | Description |
|----------|------|-------------|
| **Record object** | `object` | The SDK encodes it against the writer schema with `avsc` |
| **Pre-encoded** | `Buffer` / `Uint8Array` | A raw Avro datum you encoded yourself, passed through unchanged |

```typescript
import { ZerobusSdk, TableProperties, RecordType } from '@databricks/zerobus-ingest-sdk';

const schema = JSON.stringify({
  type: 'record', name: 'Order',
  fields: [{ name: 'id', type: 'long' }, { name: 'customer_name', type: 'string' }],
});

const tableProperties: TableProperties = { tableName: 'catalog.schema.orders', avroSchema: schema };
const stream = await sdk.createStream(tableProperties, clientId, clientSecret);

await stream.ingestRecordOffset({ id: 1n, customer_name: 'Alice' }); // queue only — do NOT wait here
await stream.flush();                                                // wait once for all pending acks
```

## Value conventions

- **`long` / `timestamp` precision:** `long` (int64) fields take a JavaScript **BigInt**
  (e.g. `1n`) so the full 64-bit range is preserved — a plain `number` silently loses
  precision past 2^53. `date` / `timestamp-millis` / `timestamp-micros` logical-type fields
  accept a JavaScript `Date`.
- **Unions:** `["null", T]` takes `null` or a `T` value directly. In `["int", "long"]`, a
  `number` selects `int` and a `BigInt` selects `long`. A union of two same-JS-type branches
  (e.g. `["float", "double"]`) takes the wrapped form `{ double: 1.5 }`.
- **`decimal`:** pass a `number`/`BigInt` (scaled per the schema) or the raw unscaled bytes.
- **`fixed` / `bytes`:** pass a `Buffer` of the exact length.

## Examples

| Example | Method | Run |
|---------|--------|-----|
| `single.ts` | Single-record | `npm run example:avro:single` |
| `batch.ts` | Batch | `npm run example:avro:batch` |

Set `ZEROBUS_SERVER_ENDPOINT`, `DATABRICKS_WORKSPACE_URL`, `ZEROBUS_TABLE_NAME`,
`DATABRICKS_CLIENT_ID`, and `DATABRICKS_CLIENT_SECRET` first (see the top-level
[examples README](../README.md#prerequisites)).
