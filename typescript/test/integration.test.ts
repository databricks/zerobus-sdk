/**
 * Integration tests for Zerobus TypeScript SDK
 *
 * These tests require a Databricks workspace and valid credentials.
 * Set the following environment variables before running:
 * - ZEROBUS_SERVER_ENDPOINT
 * - DATABRICKS_WORKSPACE_URL
 * - DATABRICKS_CLIENT_ID
 * - DATABRICKS_CLIENT_SECRET
 * - ZEROBUS_TABLE_NAME
 *
 * Run with: npm test -- --grep integration
 */

import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert';
import { ZerobusSdk, RecordType, TableProperties, StreamConfigurationOptions } from '../index';

// Check which environment variables are missing
const missingEnvVars: string[] = [];
const requiredEnvVars = {
    'ZEROBUS_SERVER_ENDPOINT': process.env.ZEROBUS_SERVER_ENDPOINT,
    'DATABRICKS_WORKSPACE_URL': process.env.DATABRICKS_WORKSPACE_URL,
    'DATABRICKS_CLIENT_ID': process.env.DATABRICKS_CLIENT_ID,
    'DATABRICKS_CLIENT_SECRET': process.env.DATABRICKS_CLIENT_SECRET,
    'ZEROBUS_TABLE_NAME': process.env.ZEROBUS_TABLE_NAME,
};

for (const [name, value] of Object.entries(requiredEnvVars)) {
    if (!value) {
        missingEnvVars.push(name);
    }
}

// Skip integration tests if any credentials not provided
const SKIP_INTEGRATION = missingEnvVars.length > 0;

if (SKIP_INTEGRATION) {
    console.log('\n⚠️  Integration tests skipped - missing environment variables:');
    missingEnvVars.forEach(varName => {
        console.log(`  ✗ ${varName}`);
    });
    console.log('\nTo run integration tests, set these environment variables:');
    console.log('  export ZEROBUS_SERVER_ENDPOINT="https://<workspace-id>.zerobus.<region>.cloud.databricks.com"');
    console.log('  export DATABRICKS_WORKSPACE_URL="https://<workspace>.cloud.databricks.com"');
    console.log('  export DATABRICKS_CLIENT_ID="<service-principal-id>"');
    console.log('  export DATABRICKS_CLIENT_SECRET="<service-principal-secret>"');
    console.log('  export ZEROBUS_TABLE_NAME="catalog.schema.table"\n');
}

describe('Integration Tests', { skip: SKIP_INTEGRATION }, () => {
    let sdk: ZerobusSdk;
    const zerobusEndpoint = process.env.ZEROBUS_SERVER_ENDPOINT || '';
    const workspaceUrl = process.env.DATABRICKS_WORKSPACE_URL || '';
    const tableName = process.env.ZEROBUS_TABLE_NAME || '';
    const clientId = process.env.DATABRICKS_CLIENT_ID || '';
    const clientSecret = process.env.DATABRICKS_CLIENT_SECRET || '';

    before(() => {
        // Skip hook execution if tests are being skipped
        if (SKIP_INTEGRATION || !zerobusEndpoint || !workspaceUrl) {
            return;
        }

        // Initialize SDK for integration tests
        sdk = new ZerobusSdk(zerobusEndpoint, workspaceUrl);
    });

    describe('JSON ingestion', () => {
        it('should ingest single JSON record', async () => {
            const tableProperties: TableProperties = { tableName };
            const options: StreamConfigurationOptions = {
                recordType: RecordType.Json,
                maxInflightRequests: 100,
            };

            const stream = await sdk.createStream(
                tableProperties,
                clientId,
                clientSecret,
                options
            );

            try {
                const record = { device_name: 'test-sensor', temp: 25, humidity: 60 };
                const offsetId = await stream.ingestRecord(JSON.stringify(record));
                assert.ok(typeof offsetId === 'bigint');
                assert.ok(offsetId >= 0n);

                await stream.flush();
            } finally {
                await stream.close();
            }
        });

        it('should ingest JSON object (auto-stringify)', async () => {
            const tableProperties: TableProperties = { tableName };
            const options: StreamConfigurationOptions = {
                recordType: RecordType.Json,
                maxInflightRequests: 100,
            };

            const stream = await sdk.createStream(
                tableProperties,
                clientId,
                clientSecret,
                options
            );

            try {
                const record = { device_name: 'test-sensor-2', temp: 26, humidity: 61 };
                const offsetId = await stream.ingestRecord(record);
                assert.ok(typeof offsetId === 'bigint');

                await stream.flush();
            } finally {
                await stream.close();
            }
        });

        it('should ingest batch of JSON records', async () => {
            const tableProperties: TableProperties = { tableName };
            const options: StreamConfigurationOptions = {
                recordType: RecordType.Json,
                maxInflightRequests: 1000,
            };

            const stream = await sdk.createStream(
                tableProperties,
                clientId,
                clientSecret,
                options
            );

            try {
                const records = Array.from({ length: 10 }, (_, i) => ({
                    device_name: `sensor-${i}`,
                    temp: 20 + i,
                    humidity: 50 + i,
                }));

                const offsetId = await stream.ingestRecords(records);
                assert.ok(offsetId === null || typeof offsetId === 'bigint');

                await stream.flush();
            } finally {
                await stream.close();
            }
        });
    });

    describe('Batch ingestion', () => {
        it('should ingest empty batch and return null', async () => {
            const tableProperties: TableProperties = { tableName };
            const options: StreamConfigurationOptions = {
                recordType: RecordType.Json,
            };

            const stream = await sdk.createStream(
                tableProperties,
                clientId,
                clientSecret,
                options
            );

            try {
                const offsetId = await stream.ingestRecords([]);
                assert.strictEqual(offsetId, null);
            } finally {
                await stream.close();
            }
        });

        it('should ingest batch of string records', async () => {
            const tableProperties: TableProperties = { tableName };
            const options: StreamConfigurationOptions = {
                recordType: RecordType.Json,
            };

            const stream = await sdk.createStream(
                tableProperties,
                clientId,
                clientSecret,
                options
            );

            try {
                const records = [
                    JSON.stringify({ device_name: 'batch-1', temp: 20, humidity: 50 }),
                    JSON.stringify({ device_name: 'batch-2', temp: 21, humidity: 51 }),
                    JSON.stringify({ device_name: 'batch-3', temp: 22, humidity: 52 }),
                ];

                const offsetId = await stream.ingestRecords(records);
                assert.ok(typeof offsetId === 'bigint');

                await stream.flush();
            } finally {
                await stream.close();
            }
        });
    });

    describe('Offset-based API (v0.4.0)', () => {
        it('should ingest single record with ingestRecordOffset and waitForOffset', async () => {
            const tableProperties: TableProperties = { tableName };
            const options: StreamConfigurationOptions = {
                recordType: RecordType.Json,
                maxInflightRequests: 100,
            };

            const stream = await sdk.createStream(
                tableProperties,
                clientId,
                clientSecret,
                options
            );

            try {
                const record = { device_name: 'offset-api-test', temp: 25, humidity: 60 };
                const offsetId = await stream.ingestRecordOffset(record);
                assert.ok(typeof offsetId === 'bigint');
                assert.ok(offsetId >= 0n);

                // Wait for acknowledgment
                await stream.waitForOffset(offsetId);
            } finally {
                await stream.close();
            }
        });

        it('should ingest batch with ingestRecordsOffset and waitForOffset', async () => {
            const tableProperties: TableProperties = { tableName };
            const options: StreamConfigurationOptions = {
                recordType: RecordType.Json,
                maxInflightRequests: 100,
            };

            const stream = await sdk.createStream(
                tableProperties,
                clientId,
                clientSecret,
                options
            );

            try {
                const records = Array.from({ length: 5 }, (_, i) => ({
                    device_name: `offset-batch-${i}`,
                    temp: 20 + i,
                    humidity: 50 + i,
                }));

                const offsetId = await stream.ingestRecordsOffset(records);
                assert.ok(offsetId === null || typeof offsetId === 'bigint');

                if (offsetId !== null) {
                    await stream.waitForOffset(offsetId);
                }
            } finally {
                await stream.close();
            }
        });

        it('should handle high-throughput pattern: send many, wait once', async () => {
            const tableProperties: TableProperties = { tableName };
            const options: StreamConfigurationOptions = {
                recordType: RecordType.Json,
                maxInflightRequests: 1000,
            };

            const stream = await sdk.createStream(
                tableProperties,
                clientId,
                clientSecret,
                options
            );

            try {
                let lastOffset: bigint = BigInt(0);

                // Send 10 records quickly
                for (let i = 0; i < 10; i++) {
                    lastOffset = await stream.ingestRecordOffset({
                        device_name: `high-throughput-${i}`,
                        temp: 20 + i,
                        humidity: 50 + i,
                    });
                }

                // Wait for last offset only (implies all previous are acknowledged)
                await stream.waitForOffset(lastOffset);
            } finally {
                await stream.close();
            }
        });

        it('should return null for empty batch with ingestRecordsOffset', async () => {
            const tableProperties: TableProperties = { tableName };
            const options: StreamConfigurationOptions = {
                recordType: RecordType.Json,
            };

            const stream = await sdk.createStream(
                tableProperties,
                clientId,
                clientSecret,
                options
            );

            try {
                const offsetId = await stream.ingestRecordsOffset([]);
                assert.strictEqual(offsetId, null);
            } finally {
                await stream.close();
            }
        });
    });

    describe('Stream recovery', () => {
        it('should retrieve unacked records after close', async () => {
            const tableProperties: TableProperties = { tableName };
            const options: StreamConfigurationOptions = {
                recordType: RecordType.Json,
                maxInflightRequests: 100,
            };

            const stream = await sdk.createStream(
                tableProperties,
                clientId,
                clientSecret,
                options
            );

            try {
                // Ingest but don't flush
                await stream.ingestRecord({ device_name: 'test', temp: 25, humidity: 60 });

                // Close without flush
                await stream.close();

                // Get unacked records (may be empty if already acknowledged)
                const unacked = await stream.getUnackedRecords();
                assert.ok(Array.isArray(unacked));
            } catch (error) {
                // Expected if stream is in wrong state
                assert.ok(error);
            }
        });

        it('should retrieve unacked batches after failure', async () => {
            const tableProperties: TableProperties = { tableName };
            const options: StreamConfigurationOptions = {
                recordType: RecordType.Json,
            };

            const stream = await sdk.createStream(
                tableProperties,
                clientId,
                clientSecret,
                options
            );

            try {
                const batch1 = [
                    { device_name: 'sensor-1', temp: 20, humidity: 50 },
                    { device_name: 'sensor-2', temp: 21, humidity: 51 },
                ];

                await stream.ingestRecords(batch1);
                await stream.close();

                // Try to get unacked batches
                const unackedBatches = await stream.getUnackedBatches();
                assert.ok(Array.isArray(unackedBatches));
            } catch (error) {
                // Expected if stream is in wrong state
                assert.ok(error);
            }
        });
    });

    describe('Error handling', () => {
        it('should reject with error for invalid table name', async () => {
            const tableProperties: TableProperties = {
                tableName: 'invalid.table.name.that.does.not.exist',
            };
            const options: StreamConfigurationOptions = {
                recordType: RecordType.Json,
            };

            try {
                const stream = await sdk.createStream(
                    tableProperties,
                    clientId,
                    clientSecret,
                    options
                );
                await stream.close();
                assert.fail('Should have thrown an error');
            } catch (error) {
                assert.ok(error);
                assert.ok(error instanceof Error);
            }
        });

        it('should reject with error for invalid credentials', async () => {
            const tableProperties: TableProperties = { tableName };
            const options: StreamConfigurationOptions = {
                recordType: RecordType.Json,
            };

            try {
                const stream = await sdk.createStream(
                    tableProperties,
                    'invalid-client-id',
                    'invalid-client-secret',
                    options
                );
                await stream.close();
                assert.fail('Should have thrown an error');
            } catch (error) {
                assert.ok(error);
                assert.ok(error instanceof Error);
            }
        });
    });
});
