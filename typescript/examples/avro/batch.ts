/**
 * Avro Batch Ingestion Example.
 *
 * Ingest a batch of record objects or pre-encoded Avro Buffers with
 * ingestRecordsOffset() (all-or-nothing, one acknowledgment). Object encoding uses
 * the optional `avsc` dependency; `long` / BIGINT fields take a BigInt (e.g. `1n`).
 */

import { ZerobusSdk, StreamConfigurationOptions, TableProperties, RecordType } from '../../wrapper';

const SERVER_ENDPOINT = process.env.ZEROBUS_SERVER_ENDPOINT || 'https://your-workspace-id.zerobus.region.cloud.databricks.com';
const DATABRICKS_WORKSPACE_URL = process.env.DATABRICKS_WORKSPACE_URL || 'https://your-workspace.cloud.databricks.com';
const TABLE_NAME = process.env.ZEROBUS_TABLE_NAME || 'catalog.schema.table';
const CLIENT_ID = process.env.DATABRICKS_CLIENT_ID || 'your-oauth-client-id';
const CLIENT_SECRET = process.env.DATABRICKS_CLIENT_SECRET || 'your-oauth-client-secret';

const AVRO_SCHEMA = JSON.stringify({
    type: 'record',
    name: 'Order',
    fields: [
        { name: 'id', type: 'long' },
        { name: 'customer_name', type: 'string' },
    ],
});

async function main() {
    console.log('Avro Batch Ingestion Example');
    console.log('='.repeat(60));

    const sdk = new ZerobusSdk(SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL);
    const tableProperties: TableProperties = { tableName: TABLE_NAME, avroSchema: AVRO_SCHEMA };
    const options: StreamConfigurationOptions = { recordType: RecordType.Avro, recovery: true };

    const stream = await sdk.createStream(tableProperties, CLIENT_ID, CLIENT_SECRET, options);
    console.log('Stream created');

    try {
        // Record objects: the SDK encodes each against the writer schema.
        const records = [
            { id: 1n, customer_name: 'Alice' },
            { id: 2n, customer_name: 'Bob' },
        ];
        const offset1 = await stream.ingestRecordsOffset(records);
        console.log(`[Record objects] batch queued with offset ID: ${offset1}`);

        // Pre-encoded: a batch of raw Avro datums (Buffers).
        const avro = require('avsc');
        const type = avro.Type.forSchema(JSON.parse(AVRO_SCHEMA));
        const preEncoded: Buffer[] = [
            type.toBuffer({ id: 3, customer_name: 'Cy' }),
            type.toBuffer({ id: 4, customer_name: 'Dana' }),
        ];
        const offset2 = await stream.ingestRecordsOffset(preEncoded);
        console.log(`[Pre-encoded] batch queued with offset ID: ${offset2}`);

        await stream.flush();
        console.log('All batches acknowledged');

        await stream.close();
        console.log('Stream closed successfully');
    } catch (error) {
        await stream.close();
        throw error;
    }
}

main().catch((error) => {
    console.error('Fatal error:', error);
    process.exit(1);
});
