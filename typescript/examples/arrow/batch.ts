/**
 * Arrow Flight Batch Ingestion Example
 *
 * **Experimental/Unsupported**: Arrow Flight support is experimental and not yet
 * supported for production use. The API may change in future releases.
 *
 * This example demonstrates:
 * - Creating an Arrow Flight stream with schema definition
 * - Ingesting Arrow batches containing multiple rows (most efficient)
 * - Using waitForOffset() for selective acknowledgment
 * - Large batch ingestion for high-throughput scenarios
 *
 * Arrow Flight provides high-performance columnar data transfer and is great for
 * analytics workloads and interoperability with data science tools.
 */

import {
    tableFromArrays,
    tableToIPC,
    makeVector,
    LargeUtf8,
    Int32,
    Int64,
    Schema,
    Field,
    makeTable,
} from 'apache-arrow';
import { ZerobusSdk, ArrowStreamConfigurationOptions, ArrowTableProperties, ArrowDataType } from '../../index';

// Configuration - set via environment variables or modify these defaults
const SERVER_ENDPOINT = process.env.ZEROBUS_SERVER_ENDPOINT || 'https://your-workspace-id.zerobus.region.cloud.databricks.com';
const DATABRICKS_WORKSPACE_URL = process.env.DATABRICKS_WORKSPACE_URL || 'https://your-workspace.cloud.databricks.com';
const TABLE_NAME = process.env.ZEROBUS_TABLE_NAME || 'catalog.schema.table';
const CLIENT_ID = process.env.DATABRICKS_CLIENT_ID || 'your-oauth-client-id';
const CLIENT_SECRET = process.env.DATABRICKS_CLIENT_SECRET || 'your-oauth-client-secret';

// Define the Arrow schema explicitly to match table schema
const arrowSchema = new Schema([
    new Field('device_name', new LargeUtf8(), true),
    new Field('temp', new Int32(), true),
    new Field('humidity', new Int64(), true),
]);

/**
 * Creates an Arrow IPC buffer from multiple records.
 * This is the most efficient way to use Arrow - batching many rows together.
 */
function createArrowBatch(records: Array<{ deviceName: string; temp: number; humidity: bigint }>): Buffer {
    // Create vectors with explicit types
    const deviceNameVector = makeVector({ type: new LargeUtf8(), values: records.map(r => r.deviceName) });
    const tempVector = makeVector({ type: new Int32(), values: records.map(r => r.temp) });
    const humidityVector = makeVector({ type: new Int64(), values: records.map(r => r.humidity) });

    const table = makeTable({
        device_name: deviceNameVector,
        temp: tempVector,
        humidity: humidityVector,
    });

    // Serialize to Arrow IPC stream format
    const ipcBytes = tableToIPC(table, 'stream');
    return Buffer.from(ipcBytes);
}

async function main() {
    console.log('Arrow Flight Batch Ingestion Example');
    console.log('='.repeat(60));
    console.log('NOTE: Arrow Flight support is experimental/unsupported');
    console.log('='.repeat(60));

    // Validate configuration
    if (CLIENT_ID === 'your-oauth-client-id' || CLIENT_SECRET === 'your-oauth-client-secret') {
        console.error('Error: Please set DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET environment variables');
        process.exit(1);
    }

    if (SERVER_ENDPOINT === 'https://your-workspace-id.zerobus.region.cloud.databricks.com') {
        console.error('Error: Please set ZEROBUS_SERVER_ENDPOINT environment variable');
        process.exit(1);
    }

    if (TABLE_NAME === 'catalog.schema.table') {
        console.error('Error: Please set ZEROBUS_TABLE_NAME environment variable');
        process.exit(1);
    }

    // Initialize SDK
    const sdk = new ZerobusSdk(SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL);

    // Configure Arrow table properties with schema
    // Note: Schema must match the table's Arrow schema (use LargeUtf8 for STRING columns)
    const tableProperties: ArrowTableProperties = {
        tableName: TABLE_NAME,
        schemaFields: [
            { name: 'device_name', dataType: ArrowDataType.LargeUtf8, nullable: true },
            { name: 'temp', dataType: ArrowDataType.Int32, nullable: true },
            { name: 'humidity', dataType: ArrowDataType.Int64, nullable: true }
        ]
    };

    // Configure Arrow stream
    const options: ArrowStreamConfigurationOptions = {
        maxInflightBatches: 100,
        recovery: true
    };

    // Create Arrow stream
    const stream = await sdk.createArrowStream(
        tableProperties,
        CLIENT_ID,
        CLIENT_SECRET,
        options
    );
    console.log('Arrow stream created');

    try {
        // 1. Small batch - 3 records in one Arrow batch (most efficient)
        const records1 = [
            { deviceName: 'sensor-001', temp: 22, humidity: BigInt(65) },
            { deviceName: 'sensor-002', temp: 23, humidity: BigInt(67) },
            { deviceName: 'sensor-003', temp: 24, humidity: BigInt(69) }
        ];
        const batch1 = createArrowBatch(records1);

        const offset1 = await stream.ingestBatch(batch1);
        console.log(`[Arrow batch] 3 rows sent with offset ID: ${offset1}`);
        await stream.waitForOffset(offset1);
        console.log(`[Arrow batch] Acknowledged with offset ID: ${offset1}`);

        // 2. Another small batch
        const records2 = [
            { deviceName: 'sensor-004', temp: 25, humidity: BigInt(71) },
            { deviceName: 'sensor-005', temp: 26, humidity: BigInt(73) },
            { deviceName: 'sensor-006', temp: 27, humidity: BigInt(75) }
        ];
        const batch2 = createArrowBatch(records2);

        const offset2 = await stream.ingestBatch(batch2);
        console.log(`[Arrow batch] 3 rows sent with offset ID: ${offset2}`);
        await stream.waitForOffset(offset2);
        console.log(`[Arrow batch] Acknowledged with offset ID: ${offset2}`);

        // 3. Large batch example - 100 records in a single Arrow batch
        console.log('\n[Large Arrow batch] Sending batch of 100 records...');
        const largeRecords = Array.from({ length: 100 }, (_, i) => ({
            deviceName: `sensor-${i.toString().padStart(3, '0')}`,
            temp: 20 + (i % 15),
            humidity: BigInt(50 + (i % 40))
        }));
        const largeBatch = createArrowBatch(largeRecords);

        const offset3 = await stream.ingestBatch(largeBatch);
        await stream.waitForOffset(offset3);
        console.log(`[Large Arrow batch] 100 records acknowledged with offset ID: ${offset3}`);

        // 4. High-throughput pattern: multiple batches, wait for last
        console.log('\n[High-throughput] Sending 10 batches of 10 records each...');
        let lastOffset: bigint = BigInt(0);
        for (let batchNum = 0; batchNum < 10; batchNum++) {
            const batchRecords = Array.from({ length: 10 }, (_, i) => ({
                deviceName: `batch${batchNum}-sensor-${i}`,
                temp: 20 + i,
                humidity: BigInt(50 + i * 2)
            }));
            const batch = createArrowBatch(batchRecords);
            lastOffset = await stream.ingestBatch(batch);
        }
        await stream.waitForOffset(lastOffset);
        console.log(`[High-throughput] All 100 records (10 batches) acknowledged (last offset: ${lastOffset})`);

        await stream.close();
        console.log('Arrow stream closed successfully');
    } catch (error) {
        await stream.close();
        throw error;
    }
}

// Run the example
main().catch((error) => {
    console.error('Fatal error:', error);
    process.exit(1);
});
