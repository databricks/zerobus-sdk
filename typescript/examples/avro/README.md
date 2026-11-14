# Avro Examples (Beta)

Avro-based ingestion into Databricks Delta tables with the Zerobus TypeScript SDK.

> **Beta.** The feature is in development. The published (CI-built) release binaries include
> Avro; a from-source build enables it with `npm run build:avro`. Object encoding uses the
> optional [`avsc`](https://www.npmjs.com/package/avsc) dependency — `npm install avsc`. The
> pre-encoded-`Buffer` path needs no dependency.

## Overview

Declare the Avro **writer schema** (JSON) once, on `TableProperties.avroSchema`. This selects
the Avro record format. Then ingest through the usual `ingestRecordOffset` /
`ingestRecordsOffset`, passing either:

| Approach | Type | Description |
|----------|------|-------------|
| **Record object** | `object` | The SDK encodes it against the writer schema with `avsc` |
| **Pre-encoded** | `Buffer` / `Uint8Array` | A raw Avro datum you encoded yourself, passed through unchanged |

Pre-encoded bytes must be **bare Avro binary datums** written with the writer schema.
Container files, single-object encoding, and Confluent-framed bytes (e.g. from a schema
registry) are not accepted.

```typescript
import { ZerobusSdk, TableProperties } from '@databricks/zerobus-ingest-sdk';

const schema = JSON.stringify({
  type: 'record', name: 'Order',
  fields: [{ name: 'id', type: 'long' }, { name: 'customer_name', type: 'string' }],
});

const sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);
const tableProperties: TableProperties = { tableName: 'catalog.schema.orders', avroSchema: schema };
const stream = await sdk.createStream(tableProperties, clientId, clientSecret);

await stream.ingestRecordOffset({ id: 1n, customer_name: 'Alice' }); // queue only — do NOT wait here
await stream.flush();                                                // wait once for all pending acks
```

## Value conventions

`all_types.ts` has a runnable field of every type below.

| Avro type | JavaScript value |
|-----------|------------------|
| `boolean` | `boolean` |
| `int` | `number` |
| `long` | `BigInt` (e.g. `1n`); a safe-integer `number` is also accepted |
| `float` / `double` | `number` |
| `string` | `string` |
| `bytes` | `Buffer` of any length |
| `fixed(N)` | `Buffer` of exactly N bytes |
| `enum` | the symbol `string` |
| `array<T>` | array of `T` values |
| `map<T>` | plain object keyed by `string`, values `T` |
| `record` | plain object keyed by field name |
| `["null", T]` | `null` or a `T` value |
| `["int", "long"]` | a `number` selects `int`, a `BigInt` selects `long` |
| `decimal` | decimal `string` (exact, e.g. `'1.23'`), `number`, `BigInt` (whole value — `5n` is `5.00`), or unscaled bytes |
| `uuid` | UUID `string` (packed to 16 bytes on `fixed(16)`) |
| `date` | `'YYYY-MM-DD'` string, or a `Date` read as its UTC calendar day |
| `time-millis` / `time-micros` | `number` of ms / µs since midnight |
| `timestamp-millis` / `-micros` | `Date`, or epoch ms / µs as a `number` or `BigInt` |
| `local-timestamp-millis` / `-micros` | `Date`, read by its UTC value |

Notes:

- **`long` precision:** a `BigInt` preserves the full 64-bit range; a plain `number` silently
  loses precision past 2^53.
- **`timestamp-micros`:** pass a `Date` or a `BigInt` to reach the full range (e.g. year 9999);
  a plain `number` is exact only to about year 2255 (the 2^53 µs limit).
- **`local-timestamp`:** a `Date` is read by its UTC value, not a local wall-clock.
- **Numeric unions:** a `long` beside another numeric branch (`["int","long"]`,
  `["long","double"]`) takes a `BigInt`, so a `number` is unambiguously the other branch. A
  union of two non-`long` numeric branches (e.g. `["float","double"]`) is still ambiguous and
  takes the wrapped form `{ double: 1.5 }`.
- **`decimal` / `uuid` / `date`:** extra fractional digits, precision overflow, a malformed
  UUID, and an impossible calendar day are all rejected.

## Examples

| Example | Method | Run |
|---------|--------|-----|
| `single.ts` | Single-record | `npm run example:avro:single` |
| `batch.ts` | Batch | `npm run example:avro:batch` |
| `all_types.ts` | Every type | `npm run example:avro:types` |

Set `ZEROBUS_SERVER_ENDPOINT`, `DATABRICKS_WORKSPACE_URL`, `ZEROBUS_TABLE_NAME`,
`DATABRICKS_CLIENT_ID`, and `DATABRICKS_CLIENT_SECRET` first (see the top-level
[examples README](../README.md#prerequisites)).
