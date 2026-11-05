/**
 * Protocol Buffers Batch Ingestion Example
 *
 * This example demonstrates:
 * - Creating a stream with Protocol Buffers record type
 * - Loading and using protobuf descriptors
 * - Ingesting batches of records with ingestRecordsOffset() (recommended)
 * - Using waitForOffset() for selective acknowledgment
 * - Deprecated ingestRecords() API for comparison
 * - All-or-nothing batch semantics
 */

import * as path from 'path';
import * as fs from 'fs';
import { ZerobusSdk, StreamConfigurationOptions, TableProperties, RecordType } from '../../index';
import { loadDescriptorProto } from '../../utils/descriptor';
import * as airQuality from '../generated/air_quality';
import { requiredEnv } from '../config';

const SERVER_ENDPOINT = requiredEnv('ZEROBUS_SERVER_ENDPOINT');
const WORKSPACE_URL = requiredEnv('DATABRICKS_WORKSPACE_URL');
const TABLE_NAME = requiredEnv('ZEROBUS_TABLE_NAME');
const CLIENT_ID = requiredEnv('DATABRICKS_CLIENT_ID');
const CLIENT_SECRET = requiredEnv('DATABRICKS_CLIENT_SECRET');

async function main() {
    console.log('Protocol Buffers Batch Ingestion Example');
    console.log('='.repeat(60));

    // Load protobuf descriptor
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
    console.log('Protobuf descriptor loaded');

    // Initialize SDK
    const sdk = new ZerobusSdk(SERVER_ENDPOINT, WORKSPACE_URL);

    // Configure table properties with descriptor (required for Protocol Buffers)
    const tableProperties: TableProperties = {
        tableName: TABLE_NAME,
        descriptorProto: descriptorBase64
    };

    // Configure stream with Protocol Buffers record type
    const options: StreamConfigurationOptions = {
        recordType: RecordType.Proto,
        maxInflightRequests: 100,
        recovery: true
    };

    // Create stream
    const stream = await sdk.createStream(
        tableProperties,
        CLIENT_ID,
        CLIENT_SECRET,
        options
    );
    console.log('Stream created');

    const AirQuality = airQuality.examples.AirQuality;

    try {
        await ingestWithOffsetApi();
        await ingestWithFutureApi();

        await stream.close();
        console.log('Stream closed successfully');
    } catch (error) {
        await stream.close();
        throw error;
    }

    /**
     * Recommended API: returns offset directly after queuing.
     */
    async function ingestWithOffsetApi() {
        console.log('\n=== Offset-based API (Recommended) ===');

        // 1. Auto-encoding: array of Message objects - SDK handles encoding
        const batch1 = [
            AirQuality.create({ deviceName: 'sensor-001', temp: 22, humidity: 65 }),
            AirQuality.create({ deviceName: 'sensor-002', temp: 23, humidity: 67 }),
            AirQuality.create({ deviceName: 'sensor-003', temp: 24, humidity: 69 })
        ];

        const offset1 = await stream.ingestRecordsOffset(batch1);
        if (offset1 !== null) {
            console.log(`[Auto-encoding] Batch of 3 records queued with offset ID: ${offset1}`);
        }

        // 2. Pre-encoded: array of Buffers
        const batch2 = [
            AirQuality.create({ deviceName: 'sensor-004', temp: 25, humidity: 71 }),
            AirQuality.create({ deviceName: 'sensor-005', temp: 26, humidity: 73 }),
            AirQuality.create({ deviceName: 'sensor-006', temp: 27, humidity: 75 })
        ].map(record => Buffer.from(AirQuality.encode(record).finish()));

        const offset2 = await stream.ingestRecordsOffset(batch2);
        if (offset2 !== null) {
            console.log(`[Pre-encoded] Batch of 3 records queued with offset ID: ${offset2}`);
        }

        // 3. Large batch example
        console.log('\n[Large batch] Queueing batch of 100 records...');
        const largeBatch = Array.from({ length: 100 }, (_, i) =>
            AirQuality.create({
                deviceName: `sensor-${i.toString().padStart(3, '0')}`,
                temp: 20 + (i % 15),
                humidity: 50 + (i % 40)
            })
        );

        const offset3 = await stream.ingestRecordsOffset(largeBatch);
        if (offset3 !== null) {
            console.log(`[Large batch] 100 records queued with offset ID: ${offset3}`);
        }

        await stream.flush();
        console.log('All offset-API batches acknowledged');

        // 4. Empty batch returns null
        const emptyOffset = await stream.ingestRecordsOffset([]);
        console.log(`[Empty batch] Returns: ${emptyOffset}`);
    }

    /**
     * Deprecated API: returns future that resolves to offset.
     */
    async function ingestWithFutureApi() {
        console.log('\n=== Future-based API (Deprecated) ===');

        // 1. Auto-encoding: array of Message objects
        const batch1 = [
            AirQuality.create({ deviceName: 'sensor-legacy-001', temp: 28, humidity: 77 }),
            AirQuality.create({ deviceName: 'sensor-legacy-002', temp: 29, humidity: 79 }),
            AirQuality.create({ deviceName: 'sensor-legacy-003', temp: 30, humidity: 81 })
        ];

        const offset1 = await stream.ingestRecords(batch1);
        if (offset1 !== null) {
            console.log(`[Auto-encoding] Batch acknowledged with offset ID: ${offset1}`);
        }

        // 2. Pre-encoded: array of Buffers
        const batch2 = [
            AirQuality.create({ deviceName: 'sensor-legacy-004', temp: 31, humidity: 83 }),
            AirQuality.create({ deviceName: 'sensor-legacy-005', temp: 32, humidity: 85 }),
            AirQuality.create({ deviceName: 'sensor-legacy-006', temp: 33, humidity: 87 })
        ].map(record => Buffer.from(AirQuality.encode(record).finish()));

        const offset2 = await stream.ingestRecords(batch2);
        if (offset2 !== null) {
            console.log(`[Pre-encoded] Batch acknowledged with offset ID: ${offset2}`);
        }
    }
}

// Run the example
main().catch((error) => {
    console.error('Fatal error:', error);
    process.exit(1);
});
