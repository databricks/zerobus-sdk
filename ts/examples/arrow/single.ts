/**
 * Arrow Flight Single-Record Ingestion Example
 *
 * **Experimental/Unsupported**: Arrow Flight support is experimental and not yet
 * supported for production use. The API may change in future releases.
 *
 * This example demonstrates:
 * - Creating an Arrow Flight stream with schema definition
 * - Ingesting individual Arrow IPC buffers with ingestBatch()
 * - Using waitForOffset() for selective acknowledgment
 * - Efficient columnar data ingestion for analytics workloads
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
const SERVER_ENDPOINT = process.env.ZEROBUS_SERVER_ENDPOINT || 'your-workspace-id.zerobus.region.cloud.databricks.com';
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
 * Creates an Arrow IPC buffer from a single record.
 * Arrow is columnar, so even single records are stored as mini-batches.
 */
function createArrowRecord(deviceName: string, temp: number, humidity: bigint): Buffer {
    // Create vectors with explicit types
    const deviceNameVector = makeVector({ type: new LargeUtf8(), values: [deviceName] });
    const tempVector = makeVector({ type: new Int32(), values: [temp] });
    const humidityVector = makeVector({ type: new Int64(), values: [humidity] });

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
    console.log('Arrow Flight Single-Record Ingestion Example');
    console.log('='.repeat(60));
    console.log('NOTE: Arrow Flight support is experimental/unsupported');
    console.log('='.repeat(60));

    // Validate configuration
    if (CLIENT_ID === 'your-oauth-client-id' || CLIENT_SECRET === 'your-oauth-client-secret') {
        console.error('Error: Please set DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET environment variables');
        process.exit(1);
    }

    if (SERVER_ENDPOINT === 'your-workspace-id.zerobus.region.cloud.databricks.com') {
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
        // Single record as Arrow IPC buffer
        const record1 = createArrowRecord('sensor-001', 22, BigInt(65));

        const offset1 = await stream.ingestBatch(record1);
        console.log(`[Arrow IPC] Record sent with offset ID: ${offset1}`);
        await stream.waitForOffset(offset1);
        console.log(`[Arrow IPC] Record acknowledged with offset ID: ${offset1}`);

        // Another record
        const record2 = createArrowRecord('sensor-002', 24, BigInt(70));

        const offset2 = await stream.ingestBatch(record2);
        console.log(`[Arrow IPC] Record sent with offset ID: ${offset2}`);
        await stream.waitForOffset(offset2);
        console.log(`[Arrow IPC] Record acknowledged with offset ID: ${offset2}`);

        // High-throughput pattern: send many, wait once
        console.log('\n[High-throughput] Sending 10 Arrow records...');
        let lastOffset: bigint = BigInt(0);
        for (let i = 0; i < 10; i++) {
            const record = createArrowRecord(
                `sensor-${i.toString().padStart(3, '0')}`,
                20 + i,
                BigInt(50 + i * 2)
            );
            lastOffset = await stream.ingestBatch(record);
        }
        await stream.waitForOffset(lastOffset);
        console.log(`[High-throughput] All 10 records acknowledged (last offset: ${lastOffset})`);

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
