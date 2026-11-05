/**
 * JSON Batch Ingestion Example
 *
 * This example demonstrates:
 * - Creating a stream with JSON record type
 * - Ingesting batches of records with ingestRecordsOffset() (recommended)
 * - Using waitForOffset() for selective acknowledgment
 * - Deprecated ingestRecords() API for comparison
 * - All-or-nothing batch semantics
 */

import { ZerobusSdk, StreamConfigurationOptions, TableProperties, RecordType } from '../../index';
import { requiredEnv } from '../config';

const SERVER_ENDPOINT = requiredEnv('ZEROBUS_SERVER_ENDPOINT');
const WORKSPACE_URL = requiredEnv('DATABRICKS_WORKSPACE_URL');
const TABLE_NAME = requiredEnv('ZEROBUS_TABLE_NAME');
const CLIENT_ID = requiredEnv('DATABRICKS_CLIENT_ID');
const CLIENT_SECRET = requiredEnv('DATABRICKS_CLIENT_SECRET');

/**
 * AirQuality record structure matching the table schema:
 * - device_name: STRING
 * - temp: INT
 * - humidity: BIGINT
 */
interface AirQuality {
    device_name: string;
    temp: number;
    humidity: number;
}

async function main() {
    console.log('JSON Batch Ingestion Example');
    console.log('='.repeat(60));

    // Initialize SDK
    const sdk = new ZerobusSdk(SERVER_ENDPOINT, WORKSPACE_URL);

    // Configure table properties (no descriptor needed for JSON)
    const tableProperties: TableProperties = {
        tableName: TABLE_NAME
    };

    // Configure stream with JSON record type
    const options: StreamConfigurationOptions = {
        recordType: RecordType.Json,
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

        // 1. Auto-serializing: array of objects - SDK handles JSON.stringify()
        const batch1: AirQuality[] = [
            { device_name: 'sensor-001', temp: 22, humidity: 65 },
            { device_name: 'sensor-002', temp: 23, humidity: 67 },
            { device_name: 'sensor-003', temp: 24, humidity: 69 }
        ];

        const offset1 = await stream.ingestRecordsOffset(batch1);
        if (offset1 !== null) {
            console.log(`[Auto-serializing] Batch of 3 records queued with offset ID: ${offset1}`);
        }

        // 2. Pre-serialized: array of JSON strings
        const batch2: string[] = [
            JSON.stringify({ device_name: 'sensor-004', temp: 25, humidity: 71 }),
            JSON.stringify({ device_name: 'sensor-005', temp: 26, humidity: 73 }),
            JSON.stringify({ device_name: 'sensor-006', temp: 27, humidity: 75 })
        ];

        const offset2 = await stream.ingestRecordsOffset(batch2);
        if (offset2 !== null) {
            console.log(`[Pre-serialized] Batch of 3 records queued with offset ID: ${offset2}`);
        }

        // 3. Large batch example
        console.log('\n[Large batch] Queueing batch of 100 records...');
        const largeBatch: AirQuality[] = Array.from({ length: 100 }, (_, i) => ({
            device_name: `sensor-${i.toString().padStart(3, '0')}`,
            temp: 20 + (i % 15),
            humidity: 50 + (i % 40)
        }));

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

        // 1. Auto-serializing: array of objects
        const batch1: AirQuality[] = [
            { device_name: 'sensor-legacy-001', temp: 28, humidity: 77 },
            { device_name: 'sensor-legacy-002', temp: 29, humidity: 79 },
            { device_name: 'sensor-legacy-003', temp: 30, humidity: 81 }
        ];

        const offset1 = await stream.ingestRecords(batch1);
        if (offset1 !== null) {
            console.log(`[Auto-serializing] Batch acknowledged with offset ID: ${offset1}`);
        }

        // 2. Pre-serialized: array of JSON strings
        const batch2: string[] = [
            JSON.stringify({ device_name: 'sensor-legacy-004', temp: 31, humidity: 83 }),
            JSON.stringify({ device_name: 'sensor-legacy-005', temp: 32, humidity: 85 }),
            JSON.stringify({ device_name: 'sensor-legacy-006', temp: 33, humidity: 87 })
        ];

        const offset2 = await stream.ingestRecords(batch2);
        if (offset2 !== null) {
            console.log(`[Pre-serialized] Batch acknowledged with offset ID: ${offset2}`);
        }
    }
}

// Run the example
main().catch((error) => {
    console.error('Fatal error:', error);
    process.exit(1);
});
