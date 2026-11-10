/**
 * Protocol Buffers Multiplexed Stream Example (Beta)
 *
 * This example demonstrates:
 * - Opening a multiplexed stream with sdk.streamBuilder()...multiplexed(n)
 * - Ingesting records in a loop and confirming them with a single flush()
 * - Counting acknowledgments with an ack callback
 */

import * as path from 'path';
import * as fs from 'fs';
import { ZerobusSdk } from '../../wrapper';
import { loadDescriptorProto } from '../../utils/descriptor';
import * as airQuality from '../generated/air_quality';
import { requiredEnv } from '../config';

const SERVER_ENDPOINT = requiredEnv('ZEROBUS_SERVER_ENDPOINT');
const WORKSPACE_URL = requiredEnv('DATABRICKS_WORKSPACE_URL');
const TABLE_NAME = requiredEnv('ZEROBUS_TABLE_NAME');
const CLIENT_ID = requiredEnv('DATABRICKS_CLIENT_ID');
const CLIENT_SECRET = requiredEnv('DATABRICKS_CLIENT_SECRET');

const STREAM_COUNT = 4;
const NUM_RECORDS = 1000;

async function main() {
    console.log('Protocol Buffers Multiplexed Stream Example (Beta)');
    console.log('='.repeat(60));

    const descriptorPath = path.join(__dirname, '..', '..', 'schemas', 'air_quality_descriptor.pb');
    if (!fs.existsSync(descriptorPath)) {
        console.error('Error: Protobuf descriptor not found at:', descriptorPath);
        console.log('\nGenerate it using: npm run build:proto');
        process.exit(1);
    }
    const descriptorBase64 = loadDescriptorProto({
        descriptorPath,
        protoFileName: 'air_quality.proto',
        messageName: 'AirQuality'
    });

    const sdk = new ZerobusSdk(SERVER_ENDPOINT, WORKSPACE_URL);

    // Keep the callback trivial: it runs on the event loop once per record or batch.
    let acked = 0;
    let failed = 0;
    const stream = await sdk.streamBuilder()
        .table(TABLE_NAME)
        .oauth(CLIENT_ID, CLIENT_SECRET)
        .compiledProto(descriptorBase64)
        .multiplexed(STREAM_COUNT)
        .ackCallback({
            onAck() { acked++; },
            onError(messageId, message) {
                failed++;
                console.error(`Record ${messageId} failed: ${message}`);
            }
        })
        .build();
    console.log(`Multiplexed stream opened with ${STREAM_COUNT} sub-streams`);

    const AirQuality = airQuality.examples.AirQuality;

    try {
        // Ingest in a loop: each call resolves as soon as the record is queued.
        for (let i = 0; i < NUM_RECORDS; i++) {
            await stream.ingestRecord(AirQuality.create({
                deviceName: `sensor-${(i % 10).toString().padStart(3, '0')}`,
                temp: 20 + (i % 15),
                humidity: 50 + (i % 40)
            }));
        }
        // One flush confirms every record ingested so far, on every sub-stream.
        await stream.flush();
        console.log(`Ingested and flushed ${NUM_RECORDS} records`);
    } catch (error) {
        // Close anyway, but report the ingest error rather than a close error it may cause.
        await stream.close().catch((closeError) => console.error('close() failed:', closeError));
        throw error;
    }
    // close() resolves once every ack callback has run.
    await stream.close();
    console.log(`Stream closed: ${acked} acknowledged, ${failed} failed`);
}

main().catch((error) => {
    console.error('Fatal error:', error);
    process.exit(1);
});
