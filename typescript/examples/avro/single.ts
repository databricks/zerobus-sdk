/**
 * Avro Single-Record Ingestion Example.
 *
 * Declare the writer schema on TableProperties, then ingest a record object (encoded
 * by the SDK via the optional `avsc` dependency) or a pre-encoded Avro Buffer.
 * `long` / BIGINT fields take a BigInt (e.g. `1n`) to preserve the full 64-bit range.
 */

import { ZerobusSdk, StreamConfigurationOptions, TableProperties, RecordType } from '../../wrapper';

const SERVER_ENDPOINT = process.env.ZEROBUS_SERVER_ENDPOINT || 'https://your-workspace-id.zerobus.region.cloud.databricks.com';
const DATABRICKS_WORKSPACE_URL = process.env.DATABRICKS_WORKSPACE_URL || 'https://your-workspace.cloud.databricks.com';
const TABLE_NAME = process.env.ZEROBUS_TABLE_NAME || 'catalog.schema.table';
const CLIENT_ID = process.env.DATABRICKS_CLIENT_ID || 'your-oauth-client-id';
const CLIENT_SECRET = process.env.DATABRICKS_CLIENT_SECRET || 'your-oauth-client-secret';

// The Avro writer schema (JSON). Declared once at stream creation.
const AVRO_SCHEMA = JSON.stringify({
    type: 'record',
    name: 'Order',
    fields: [
        { name: 'id', type: 'long' },
        { name: 'customer_name', type: 'string' },
    ],
});

async function main() {
    console.log('Avro Single-Record Ingestion Example');
    console.log('='.repeat(60));

    const sdk = new ZerobusSdk(SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL);

    // avroSchema selects the Avro format; recordType is optional and defaults to it.
    const tableProperties: TableProperties = {
        tableName: TABLE_NAME,
        avroSchema: AVRO_SCHEMA,
    };
    const options: StreamConfigurationOptions = {
        recordType: RecordType.Avro,
        recovery: true,
    };

    const stream = await sdk.createStream(tableProperties, CLIENT_ID, CLIENT_SECRET, options);
    console.log('Stream created');

    try {
        // 1. Record object: the SDK encodes it against the writer schema.
        const offset1 = await stream.ingestRecordOffset({ id: 1n, customer_name: 'Alice' });
        console.log(`[Record object] queued with offset ID: ${offset1}`);

        // 2. Pre-encoded: encode a datum yourself and pass the raw bytes.
        const avro = require('avsc');
        const type = avro.Type.forSchema(JSON.parse(AVRO_SCHEMA));
        const datum: Buffer = type.toBuffer({ id: 2, customer_name: 'Bob' });
        const offset2 = await stream.ingestRecordOffset(datum);
        console.log(`[Pre-encoded] queued with offset ID: ${offset2}`);

        // Confirm everything queued so far with a single flush().
        await stream.flush();
        console.log('All records acknowledged');

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
