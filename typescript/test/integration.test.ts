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
 * The table has the columns device_name STRING, temp INT and humidity BIGINT.
 *
 * Build with every feature first (the Avro test needs it), then run:
 *   npm run build:debug:all && npm run test:integration
 */

import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert';
import { execFileSync } from 'node:child_process';
import * as path from 'node:path';
import { ZerobusSdk, RecordType, TableProperties, StreamConfigurationOptions } from '../index';

const descriptor = require('protobufjs/ext/descriptor');
const protobuf = require('protobufjs');
const wrapper = require('../wrapper');
// AvroEncoder lives in the internal avro.js module; require it directly for tests.
const { AvroEncoder } = require('../avro');

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
    console.log('  See https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id');
    console.log('  export ZEROBUS_SERVER_ENDPOINT="<your-zerobus-server-endpoint>"');
    console.log('  export DATABRICKS_WORKSPACE_URL="<your-databricks-workspace-url>"');
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

    describe('Multiplexed streams', () => {
        // Rows from this run carry a unique device_name prefix, so they can be checked in the
        // table afterwards (printed at the end).
        const RUN_PREFIX = `ts-mux-${Date.now()}-${process.pid}`;
        const expectedRows: Record<string, number> = {};
        let muxSdk: any;

        before(() => {
            muxSdk = new wrapper.ZerobusSdk(zerobusEndpoint, workspaceUrl);
        });

        after(() => {
            console.log(`\nMultiplexed stream rows written with prefix ${RUN_PREFIX}:`, expectedRows);
            console.log(
                `Verify with: SELECT regexp_extract(device_name, '^${RUN_PREFIX}-([a-z0-9]+)-', 1) AS test, count(*) ` +
                    `FROM ${tableName} WHERE device_name LIKE '${RUN_PREFIX}-%' GROUP BY 1 ORDER BY 1`
            );
        });

        const row = (test: string, i: number) => ({ device_name: `${RUN_PREFIX}-${test}-${i}`, temp: i, humidity: i });
        const wrote = (test: string, rows: number) => { expectedRows[test] = (expectedRows[test] ?? 0) + rows; };
        // Opens a JSON stream unless the options and table properties say otherwise.
        const open = (streamCount: number, options: object = {}, tableProperties: TableProperties = { tableName }) =>
            muxSdk.createMultiplexedStream(tableProperties, { oauth: { clientId, clientSecret } }, streamCount,
                { recordType: RecordType.Json, ...options });

        function recordingCallback() {
            const recorded = { acked: [] as bigint[], duplicates: [] as bigint[], errors: [] as Array<[bigint, string]> };
            const seen = new Set<bigint>();
            const callback = {
                onAck(messageId: bigint) {
                    if (seen.has(messageId)) recorded.duplicates.push(messageId);
                    seen.add(messageId);
                    recorded.acked.push(messageId);
                },
                onError(messageId: bigint, message: string) {
                    recorded.errors.push([messageId, message]);
                },
            };
            return { recorded, callback };
        }

        // A stream that opens by mistake is closed before the test fails.
        async function assertOpenRejects(opening: Promise<any>, expected: RegExp) {
            let stream: any;
            try {
                stream = await opening;
            } catch (error) {
                assert.ok(error instanceof Error);
                assert.match(error.message, expected);
                return;
            }
            await stream.close();
            assert.fail(`createMultiplexedStream() should have rejected with ${expected}`);
        }

        const OPEN_FAILED = /^Failed to create multiplexed stream: /;

        it('acks every message ID of a JSON stream once before close() resolves', async () => {
            const { recorded, callback } = recordingCallback();
            const stream = await open(4, { ackCallback: callback });
            const ids: bigint[] = [];
            try {
                for (let i = 0; i < 100; i++) {
                    ids.push(await stream.ingestRecord(JSON.stringify(row('i1', i))));
                }
                for (let i = 100; i < 110; i++) {
                    ids.push(await stream.ingestRecord(row('i1', i)));
                }
                for (let b = 0; b < 5; b++) {
                    const batch = Array.from({ length: 10 }, (_, j) => JSON.stringify(row('i1', 110 + b * 10 + j)));
                    ids.push(await stream.ingestRecords(batch));
                }
                assert.strictEqual(await stream.ingestRecords([]), null);
                await stream.flush();
            } finally {
                await stream.close();
            }
            wrote('i1', 160);
            // close() resolves only after every callback has run, so no polling here.
            assert.strictEqual(new Set(ids).size, 115);
            assert.deepStrictEqual(recorded.duplicates, []);
            assert.deepStrictEqual(recorded.errors, []);
            assert.deepStrictEqual(new Set(recorded.acked), new Set(ids));
        });

        it('ingests protobuf Buffers and message objects and acks them in getUnackedRecords()', async () => {
            const root = new protobuf.Root().loadSync(path.join(__dirname, '..', 'schemas', 'air_quality.proto'), { keepCase: true });
            const AirQuality = root.lookupType('examples.AirQuality');
            const descriptorBase64 = Buffer.from(
                descriptor.DescriptorProto.encode(AirQuality.toDescriptor('proto2')).finish()
            ).toString('base64');
            const { recorded, callback } = recordingCallback();
            const stream = await open(2, { recordType: RecordType.Proto, ackCallback: callback }, { tableName, descriptorProto: descriptorBase64 });
            try {
                for (let i = 0; i < 50; i++) {
                    await stream.ingestRecord(Buffer.from(AirQuality.encode(AirQuality.create(row('i2', i))).finish()));
                }
                for (let i = 50; i < 60; i++) {
                    await stream.ingestRecord(AirQuality.create(row('i2', i)));
                }
                await stream.flush();
                // getUnackedRecords() closes the stream first and runs every callback.
                assert.deepStrictEqual(await stream.getUnackedRecords(), []);
                assert.strictEqual(stream.isClosed, true);
            } catch (error) {
                await stream.close().catch(() => {});
                throw error;
            }
            wrote('i2', 60);
            assert.strictEqual(recorded.acked.length, 60);
            assert.deepStrictEqual(recorded.errors, []);
        });

        it('ingests Avro record objects, a pre-encoded record and a batch', async () => {
            const schema = JSON.stringify({
                type: 'record',
                name: 'AirQuality',
                fields: [
                    { name: 'device_name', type: 'string' },
                    { name: 'temp', type: 'int' },
                    { name: 'humidity', type: 'long' },
                ],
            });
            const avroRow = (i: number) => ({ ...row('i3', i), humidity: BigInt(i) });
            const stream = await open(2, { recordType: RecordType.Avro }, { tableName, avroSchema: schema });
            try {
                for (let i = 0; i < 50; i++) {
                    await stream.ingestRecord(avroRow(i));
                }
                await stream.ingestRecord(new AvroEncoder(schema).encode(avroRow(50)));
                await stream.ingestRecords(Array.from({ length: 10 }, (_, j) => avroRow(51 + j)));
                await stream.flush();
                // getUnackedBatches() closes the stream first.
                assert.deepStrictEqual(await stream.getUnackedBatches(), []);
            } catch (error) {
                await stream.close().catch(() => {});
                throw error;
            }
            wrote('i3', 61);
        });

        it('round-trips negative message IDs from lanes 32 and up', async () => {
            const { recorded, callback } = recordingCallback();
            const stream = await open(40, { maxInflightRequests: 400, ackCallback: callback });
            const ids: bigint[] = [];
            try {
                for (let i = 0; i < 80; i++) {
                    ids.push(await stream.ingestRecord(JSON.stringify(row('i4', i))));
                }
                const negativeIds = ids.filter(id => id < 0n);
                assert.ok(negativeIds.length > 0, 'lanes 32-63 have negative message IDs');

                // Message IDs are bigint, and an ID outside the i64 range is rejected instead of
                // being truncated across the binding.
                assert.throws(() => stream.waitForMessageId(5 as any), { code: 'BigintExpected' });
                assert.throws(() => stream.waitForMessageId(2n ** 63n), /messageId exceeds i64 range/);
                assert.throws(() => stream.waitForMessageId(-(2n ** 63n) - 1n), /messageId exceeds i64 range/);

                // Deliberate per-ID waits (a low-volume check, not the ingest pattern): a negative
                // ID from lanes 32 and up is accepted and resolves.
                for (const id of negativeIds) {
                    await stream.waitForMessageId(id);
                }
                await stream.flush();
            } finally {
                await stream.close();
            }
            wrote('i4', 80);
            assert.deepStrictEqual(recorded.errors, []);
            const acked = new Set(recorded.acked);
            for (const id of ids.filter(messageId => messageId < 0n)) {
                assert.ok(acked.has(id), `negative message ID ${id} was acked`);
            }
        });

        it('rejects every operation except close() after close()', async () => {
            const stream = await open(2);
            let id: bigint;
            try {
                assert.strictEqual(stream.isClosed, false);
                assert.throws(() => stream.ingestRecord(42), /Payload must be a Buffer, string/);
                id = await stream.ingestRecord(JSON.stringify(row('i6', 0)));
                await stream.flush();
            } finally {
                await stream.close();
            }
            wrote('i6', 1);
            assert.strictEqual(stream.isClosed, true);
            await assert.rejects(stream.ingestRecord(JSON.stringify(row('i6', 1))), /Stream has been closed/);
            await assert.rejects(stream.ingestRecords([JSON.stringify(row('i6', 2))]), /Stream has been closed/);
            await assert.rejects(stream.ingestRecords([]), /Stream has been closed/);
            await assert.rejects(stream.flush(), /Stream has been closed/);
            await assert.rejects(stream.waitForMessageId(id!), /Stream has been closed/);
            await assert.rejects(stream.getUnackedRecords(), /Stream has been closed/);
            await assert.rejects(stream.getUnackedBatches(), /Stream has been closed/);
            await stream.close();
        });

        it('acks concurrent ingests and settles every ingest started before close()', { timeout: 60_000 }, async () => {
            const { recorded, callback } = recordingCallback();
            const stream = await open(4, { ackCallback: callback });
            let first: bigint[] = [];
            let second: PromiseSettledResult<bigint>[] = [];
            try {
                // Started without awaiting each one, so the ingests run concurrently.
                const pending: Promise<bigint>[] = Array.from({ length: 100 }, (_, i) =>
                    stream.ingestRecord(JSON.stringify(row('i8', i))));
                const batch = Array.from({ length: 5 }, (_, j) => JSON.stringify(row('i8', 100 + j)));
                pending.push(stream.ingestRecords(batch));
                first = await Promise.all(pending);
                assert.strictEqual(new Set(first).size, 101);
                await stream.flush();

                // More ingests, then close() at once: each one either lands or is refused.
                const late = Array.from({ length: 20 }, (_, i) => stream.ingestRecord(JSON.stringify(row('i8', 105 + i))));
                const closing = stream.close();
                second = await Promise.allSettled(late);
                await closing;
            } catch (error) {
                await stream.close().catch(() => {});
                throw error;
            }
            const landed: bigint[] = [];
            for (const result of second) {
                if (result.status === 'fulfilled') {
                    landed.push(result.value);
                } else {
                    assert.match((result.reason as Error).message, /Stream has been closed/);
                }
            }
            wrote('i8', 105 + landed.length);
            assert.deepStrictEqual(recorded.errors, []);
            assert.deepStrictEqual(new Set(recorded.acked), new Set([...first, ...landed]));
        });

        it('reports a record the server rejects through onError and returns it unacked', async () => {
            const { recorded, callback } = recordingCallback();
            const stream = await open(1, { recovery: false, ackCallback: callback });
            try {
                // temp is an INT column, so the server rejects this record.
                const record = JSON.stringify({ device_name: `${RUN_PREFIX}-v1-0`, temp: 'not a number', humidity: 1 });
                const id = await stream.ingestRecord(record);
                await assert.rejects(stream.flush(), { message: /^Failed to flush stream: / });
                // close() settles only after the drained marker, so every callback has run here.
                await assert.rejects(stream.close(), { message: /^Failed to close stream: / });
                assert.deepStrictEqual(recorded.errors.map(([errorId]) => errorId), [id]);
                assert.ok(recorded.errors[0][1].length > 0, 'onError gets a message');
                assert.deepStrictEqual(recorded.acked, []);
                assert.strictEqual(stream.isClosed, true);
                await assert.rejects(stream.ingestRecord(record), { message: /^Failed to ingest record: / });
                await assert.rejects(stream.ingestRecords([]), { message: /^Failed to ingest batch: / });
                // A failed close keeps the stream, so its unacked records can be read back.
                assert.deepStrictEqual(await stream.getUnackedRecords(), [Buffer.from(record)]);
                assert.deepStrictEqual(await stream.getUnackedBatches(), [[Buffer.from(record)]]);
            } catch (error) {
                await stream.close().catch(() => {});
                throw error;
            }
        });

        it('lets Node exit with a stream that was never closed', { timeout: 90_000 }, () => {
            const script = `
                const { ZerobusSdk, RecordType } = require(${JSON.stringify(path.resolve(__dirname, '..', 'wrapper.js'))});
                const sdk = new ZerobusSdk(process.env.ZEROBUS_SERVER_ENDPOINT, process.env.DATABRICKS_WORKSPACE_URL);
                // Fails unless the flush below completes.
                process.exitCode = 1;
                (async () => {
                    const stream = await sdk.createMultiplexedStream(
                        { tableName: process.env.ZEROBUS_TABLE_NAME },
                        { oauth: { clientId: process.env.DATABRICKS_CLIENT_ID, clientSecret: process.env.DATABRICKS_CLIENT_SECRET } },
                        2, { recordType: RecordType.Json, ackCallback: { onAck() {} } });
                    for (let i = 0; i < 5; i++) {
                        await stream.ingestRecord(JSON.stringify({ device_name: ${JSON.stringify(`${RUN_PREFIX}-i10-`)} + i, temp: i, humidity: i }));
                    }
                    await stream.flush();
                    process.exitCode = 0;
                })().catch((error) => { console.error(error); });
            `;
            // Throws if the child exits non-zero or is still running after the timeout.
            execFileSync(process.execPath, ['-e', script], { env: process.env, timeout: 60_000, stdio: 'pipe' });
            wrote('i10', 5);
        });

        it('rejects invalid OAuth credentials', async () => {
            await assertOpenRejects(
                muxSdk.createMultiplexedStream({ tableName }, { oauth: { clientId: 'invalid', clientSecret: 'invalid' } }, 2,
                    { recordType: RecordType.Json }),
                OPEN_FAILED
            );
        });

        it('uses the headers provider to open the stream', async () => {
            let calls = 0;
            const headersProvider = {
                getHeadersCallback: () => {
                    calls++;
                    return [['authorization', 'Bearer invalid'], ['x-databricks-zerobus-table-name', tableName]];
                },
            };
            await assertOpenRejects(
                muxSdk.createMultiplexedStream({ tableName }, { headersProvider }, 2,
                    { recordType: RecordType.Json, recovery: false, recoveryRetries: 0 }),
                OPEN_FAILED
            );
            assert.strictEqual(calls, 1);
        });
    });
});
