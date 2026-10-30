/**
 * Avro Type Reference Example.
 *
 * One record with a field for each Avro type in the value-conventions table, showing the
 * JavaScript value to pass. Run with `npm run example:avro:types`. The comment on each field
 * is the contract; see examples/avro/README.md for the full value conventions.
 */

import { ZerobusSdk, StreamConfigurationOptions, TableProperties, RecordType } from '../../wrapper';

const SERVER_ENDPOINT = process.env.ZEROBUS_SERVER_ENDPOINT || 'https://your-workspace-id.zerobus.region.cloud.databricks.com';
const DATABRICKS_WORKSPACE_URL = process.env.DATABRICKS_WORKSPACE_URL || 'https://your-workspace.cloud.databricks.com';
const TABLE_NAME = process.env.ZEROBUS_TABLE_NAME || 'catalog.schema.table';
const CLIENT_ID = process.env.DATABRICKS_CLIENT_ID || 'your-oauth-client-id';
const CLIENT_SECRET = process.env.DATABRICKS_CLIENT_SECRET || 'your-oauth-client-secret';

// Writer schema with every supported Avro type.
const AVRO_SCHEMA = JSON.stringify({
    type: 'record',
    name: 'AllTypes',
    fields: [
        // Primitives.
        { name: 'f_boolean', type: 'boolean' },
        { name: 'f_int', type: 'int' },
        { name: 'f_long', type: 'long' }, // int64 — BigInt (a plain number loses precision past 2^53)
        { name: 'f_float', type: 'float' },
        { name: 'f_double', type: 'double' },
        { name: 'f_string', type: 'string' },
        { name: 'f_bytes', type: 'bytes' },

        // Complex.
        { name: 'f_enum', type: { type: 'enum', name: 'Suit', symbols: ['HEARTS', 'DIAMONDS', 'CLUBS', 'SPADES'] } },
        { name: 'f_array', type: { type: 'array', items: 'long' } },
        { name: 'f_map', type: { type: 'map', values: 'long' } },
        { name: 'f_fixed', type: { type: 'fixed', name: 'Hash4', size: 4 } },
        { name: 'f_record', type: { type: 'record', name: 'Point', fields: [{ name: 'x', type: 'int' }, { name: 'y', type: 'int' }] } },

        // Unions.
        { name: 'f_nullable', type: ['null', 'long'] }, // nullable BIGINT: null, a number, or a BigInt
        { name: 'f_int_or_long', type: ['int', 'long'] }, // a number -> int, a BigInt -> long

        // Logical types.
        { name: 'f_decimal', type: { type: 'bytes', logicalType: 'decimal', precision: 18, scale: 2 } },
        { name: 'f_uuid', type: { type: 'fixed', name: 'Uuid', size: 16, logicalType: 'uuid' } },
        { name: 'f_date', type: { type: 'int', logicalType: 'date' } },
        { name: 'f_time_millis', type: { type: 'int', logicalType: 'time-millis' } },
        { name: 'f_time_micros', type: { type: 'long', logicalType: 'time-micros' } },
        { name: 'f_timestamp_millis', type: { type: 'long', logicalType: 'timestamp-millis' } },
        { name: 'f_timestamp_micros', type: { type: 'long', logicalType: 'timestamp-micros' } },
        { name: 'f_local_ts_millis', type: { type: 'long', logicalType: 'local-timestamp-millis' } },
        { name: 'f_local_ts_micros', type: { type: 'long', logicalType: 'local-timestamp-micros' } },
    ],
});

// One record with the JS value for every field above.
const RECORD = {
    f_boolean: true,
    f_int: 42,
    f_long: 9223372036854775807n, // BigInt preserves the full 64-bit range
    f_float: 1.5,
    f_double: 2.718281828,
    f_string: 'hello',
    f_bytes: Buffer.from([0xde, 0xad, 0xbe, 0xef]), // bytes: a Buffer of any length

    f_enum: 'HEARTS', // enum: a symbol string
    f_array: [1n, 2n, 3n], // array<long>: an array of the item's JS value
    f_map: { requests: 10n, errors: 1n }, // map<long>: a plain object keyed by string
    f_fixed: Buffer.from([1, 2, 3, 4]), // fixed(N): a Buffer of exactly N bytes
    f_record: { x: 1, y: 2 }, // nested record: a plain object

    f_nullable: null, // null, or 5 / 5n
    f_int_or_long: 7, // a number picks int; pass 7n to pick long

    f_decimal: '1234.56', // decimal: a string (exact), a number, a BigInt (whole value), or unscaled bytes
    f_uuid: '123e4567-e89b-12d3-a456-426614174000', // uuid: a string, packed to 16 bytes on fixed(16)
    f_date: '2025-01-01', // date: a 'YYYY-MM-DD' string, or a Date read as its UTC calendar day
    f_time_millis: 9 * 3600000, // time-of-day: milliseconds since midnight (a number)
    f_time_micros: 9 * 3600000000, // time-of-day: microseconds since midnight (a number)
    f_timestamp_millis: new Date('2025-01-01T09:00:00Z'), // a Date, or epoch-millis as number/BigInt
    f_timestamp_micros: new Date('9999-12-31T23:59:59Z'), // a Date (full range), or epoch-micros as a BigInt
    f_local_ts_millis: new Date('2025-01-01T09:00:00Z'), // a Date, read by its UTC value
    f_local_ts_micros: new Date('2025-01-01T09:00:00Z'), // a Date, read by its UTC value
};

async function main() {
    console.log('Avro Type Reference Example');
    console.log('='.repeat(60));

    const sdk = new ZerobusSdk(SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL);
    const tableProperties: TableProperties = { tableName: TABLE_NAME, avroSchema: AVRO_SCHEMA };
    const options: StreamConfigurationOptions = { recordType: RecordType.Avro, recovery: true };

    const stream = await sdk.createStream(tableProperties, CLIENT_ID, CLIENT_SECRET, options);
    console.log('Stream created');

    try {
        const offset = await stream.ingestRecordOffset(RECORD); // queue only — do NOT wait here
        console.log(`Record queued with offset ID: ${offset}`);

        await stream.flush(); // wait once for the pending ack
        console.log('Record acknowledged');

        await stream.close();
        console.log('Stream closed successfully');
    } catch (error) {
        // Close on failure, but don't let a close error mask the original.
        await stream.close().catch(() => {});
        throw error;
    }
}

main().catch((error) => {
    console.error('Fatal error:', error);
    process.exit(1);
});
