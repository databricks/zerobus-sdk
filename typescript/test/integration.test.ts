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

    // The native createStream (../index) throws these synchronously; the wrapper's async
    // createStream turns the same errors into rejections.
    describe('native createStream format resolution', () => {
        const validDescriptor = Buffer.from(
            descriptor.DescriptorProto.encode(
                descriptor.DescriptorProto.create({
                    name: 'AirQuality',
                    field: [{ name: 'device_name', number: 1, label: 1, type: 9 }],
                })
            ).finish()
        ).toString('base64');
        const avroSchema = JSON.stringify({
            type: 'record',
            name: 'AirQuality',
            fields: [{ name: 'device_name', type: 'string' }],
        });
        const create = (tableProperties: Omit<TableProperties, 'tableName'>, recordType?: RecordType) =>
            sdk.createStream({ tableName, ...tableProperties }, clientId, clientSecret, { recordType });

        it('throws on a descriptor that is not base64', () => {
            assert.throws(() => create({ descriptorProto: '%%%' }, RecordType.Proto), /Failed to decode descriptor/);
        });

        it('throws on base64 that is not a DescriptorProto', () => {
            const garbage = Buffer.from([0xff, 0xff, 0xff]).toString('base64');
            assert.throws(() => create({ descriptorProto: garbage }, RecordType.Proto), /Failed to parse descriptor proto/);
        });

        it('throws when descriptorProto and avroSchema are both set', () => {
            assert.throws(() => create({ descriptorProto: validDescriptor, avroSchema }), /mutually exclusive/);
        });

        it('throws when avroSchema is set with a non-Avro recordType', () => {
            assert.throws(
                () => create({ avroSchema }, RecordType.Json),
                /avroSchema is set but recordType is not RecordType.Avro/
            );
        });

        it('throws when recordType is Avro without an avroSchema', () => {
            assert.throws(() => create({}, RecordType.Avro), /no avroSchema was provided/);
        });

        it('decodes the descriptor before checking exclusivity with avroSchema', () => {
            assert.throws(() => create({ descriptorProto: '%%%', avroSchema }), /Failed to decode descriptor/);
        });

        it('rejects the default Proto record type without a descriptor', async () => {
            await assert.rejects(create({}), /requires descriptor_proto/);
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
        const builder = () => muxSdk.streamBuilder().table(tableName).oauth(clientId, clientSecret);
        // The lane (sub-stream index) is in the top 6 bits of a message ID.
        const laneOf = (messageId: bigint) => Number(BigInt.asUintN(64, messageId) >> 58n);
        const drainEventLoop = async () => {
            for (let i = 0; i < 5; i++) {
                await new Promise(resolve => setImmediate(resolve));
            }
        };

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
        async function assertBuildRejects(streamBuilder: any, expected: RegExp) {
            let stream: any;
            try {
                stream = await streamBuilder.build();
            } catch (error) {
                assert.match((error as Error).message, expected);
                return;
            }
            await stream.close();
            assert.fail(`build() should have rejected with ${expected}`);
        }

        // Core error texts (rust/sdk/src/errors.rs, default_token_factory.rs, grpc_status.rs):
        // a 400/401 from the OAuth token endpoint is InvalidUCTokenError, and a stream the server
        // refuses is CreateStreamError with the Unauthenticated or PermissionDenied description.
        const TOKEN_REJECTED = /^Failed to create multiplexed stream: Specified UC token is in invalid format: Client error \(40[01]\)/;
        const STREAM_AUTH_REJECTED = new RegExp(
            "^Failed to create multiplexed stream: Failed to create stream: code: " +
                "'(The request does not have valid authentication credentials|" +
                "The caller does not have permission to execute the specified operation)'"
        );

        it('acks every message ID of a JSON stream once before close() resolves', async () => {
            const { recorded, callback } = recordingCallback();
            const stream = await builder().json().multiplexed(4).ackCallback(callback).build();
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
            assert.deepStrictEqual(new Set(ids.map(laneOf)), new Set([0, 1, 2, 3]));
        });

        it('ingests protobuf Buffers and message objects without an ack callback', async () => {
            const root = new protobuf.Root().loadSync(path.join(__dirname, '..', 'schemas', 'air_quality.proto'), { keepCase: true });
            const AirQuality = root.lookupType('examples.AirQuality');
            const descriptorBase64 = Buffer.from(
                descriptor.DescriptorProto.encode(AirQuality.toDescriptor('proto2')).finish()
            ).toString('base64');
            const stream = await builder().compiledProto(descriptorBase64).multiplexed(2).build();
            try {
                for (let i = 0; i < 50; i++) {
                    await stream.ingestRecord(Buffer.from(AirQuality.encode(AirQuality.create(row('i2', i))).finish()));
                }
                for (let i = 50; i < 60; i++) {
                    await stream.ingestRecord(AirQuality.create(row('i2', i)));
                }
                await stream.flush();
                // getUnackedRecords() closes the stream first.
                assert.deepStrictEqual(await stream.getUnackedRecords(), []);
                assert.strictEqual(stream.isClosed, true);
            } catch (error) {
                await stream.close().catch(() => {});
                throw error;
            }
            wrote('i2', 60);
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
            const stream = await builder().avro(schema).multiplexed(2).build();
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
            const stream = await builder().json().maxInflightRequests(400).multiplexed(40).ackCallback(callback).build();
            const ids: bigint[] = [];
            try {
                for (let i = 0; i < 80; i++) {
                    ids.push(await stream.ingestRecord(JSON.stringify(row('i4', i))));
                }
                assert.deepStrictEqual(new Set(ids.map(laneOf)), new Set(Array.from({ length: 40 }, (_, lane) => lane)));
                assert.ok(ids.some(id => id < 0n), 'lanes 32-63 have negative message IDs');

                // Message IDs are bigint, and an ID outside the i64 range is rejected instead of
                // being truncated across the binding.
                assert.throws(() => stream.waitForMessageId(5 as any), { code: 'BigintExpected' });
                assert.throws(() => stream.waitForMessageId(2n ** 63n), /messageId exceeds i64 range/);
                assert.throws(() => stream.waitForMessageId(-(2n ** 63n) - 1n), /messageId exceeds i64 range/);

                // Deliberate per-ID waits, one per lane (a low-volume check, not the ingest pattern):
                // a negative ID from lanes 32 and up is accepted and resolves.
                const firstIdPerLane = new Map<number, bigint>();
                for (const id of ids) {
                    if (!firstIdPerLane.has(laneOf(id))) firstIdPerLane.set(laneOf(id), id);
                }
                for (const id of firstIdPerLane.values()) {
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
            const stream = await builder().json().multiplexed(2).build();
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
            await assert.rejects(stream.flush(), /Stream has been closed/);
            await assert.rejects(stream.waitForMessageId(id!), /Stream has been closed/);
            await assert.rejects(stream.getUnackedRecords(), /Stream has been closed/);
            await assert.rejects(stream.getUnackedBatches(), /Stream has been closed/);
            await stream.close();
        });

        it('closes the stream and runs every callback in getUnackedRecords()', async () => {
            const { recorded, callback } = recordingCallback();
            const stream = await builder().json().multiplexed(2).ackCallback(callback).build();
            try {
                for (let i = 0; i < 20; i++) {
                    await stream.ingestRecord(JSON.stringify(row('i7', i)));
                }
                await stream.flush();
                // getUnackedRecords() closes the stream first.
                assert.deepStrictEqual(await stream.getUnackedRecords(), []);
            } catch (error) {
                await stream.close().catch(() => {});
                throw error;
            }
            wrote('i7', 20);
            assert.strictEqual(stream.isClosed, true);
            assert.strictEqual(recorded.acked.length, 20);
        });

        it('keeps ingesting while a waitForMessageId() call is pending', { timeout: 90_000 }, async () => {
            // A wide margin: ingest and flush must finish long before the wait times out.
            const stream = await builder().json().flushTimeoutMs(30_000).multiplexed(1).build();
            try {
                // A fabricated lane-0 ID that is never acked: the wait lasts until the flush timeout.
                const waiting = stream.waitForMessageId(1_000_000n);
                let waitSettled = false;
                waiting.then(() => { waitSettled = true; }, () => { waitSettled = true; });
                for (let i = 0; i < 10; i++) {
                    await stream.ingestRecord(JSON.stringify(row('i8', i)));
                }
                await stream.flush();
                wrote('i8', 10);
                assert.strictEqual(waitSettled, false, 'ingest and flush finished while the wait was pending');
                await assert.rejects(waiting, /Failed to wait for message ID: /);
            } finally {
                // A timed-out wait leaves the lane open, so close() succeeds.
                await stream.close();
            }
        });

        it('survives ack callbacks that throw or reject', async () => {
            // Not t.mock: Node 16's node:test has none (see the unit tests' stubConsoleError).
            const originalConsoleError = console.error;
            let consoleErrorCalls = 0;
            console.error = (...args: unknown[]) => {
                if (args[0] === 'Zerobus multiplexed stream ack callback failed:') {
                    consoleErrorCalls++;
                } else {
                    originalConsoleError(...args);
                }
            };
            const unhandled: unknown[] = [];
            const onUnhandled = (reason: unknown) => { unhandled.push(reason); };
            process.on('unhandledRejection', onUnhandled);
            try {
                let acks = 0;
                let asyncAcks = 0;
                const callbacks = [
                    { onAck() { acks++; throw new Error('ack handler failed'); } },
                    { async onAck() { asyncAcks++; throw new Error('async ack handler failed'); } },
                ];
                for (const callback of callbacks) {
                    const stream = await builder().json().multiplexed(2).ackCallback(callback).build();
                    try {
                        for (let i = 0; i < 20; i++) {
                            await stream.ingestRecord(JSON.stringify(row('i9', i)));
                        }
                        await stream.flush();
                    } finally {
                        await stream.close();
                    }
                }
                wrote('i9', 40);
                await drainEventLoop();
                assert.strictEqual(acks, 20);
                assert.strictEqual(asyncAcks, 20);
                // Every throw and every rejection was logged, none escaped.
                assert.strictEqual(consoleErrorCalls, 40);
                assert.deepStrictEqual(unhandled, []);
            } finally {
                process.off('unhandledRejection', onUnhandled);
                console.error = originalConsoleError;
            }
        });

        it('lets Node exit with a stream that was never closed', { timeout: 90_000 }, () => {
            const script = `
                const { ZerobusSdk } = require(${JSON.stringify(path.resolve(__dirname, '..', 'wrapper.js'))});
                const sdk = new ZerobusSdk(process.env.ZEROBUS_SERVER_ENDPOINT, process.env.DATABRICKS_WORKSPACE_URL);
                (async () => {
                    const stream = await sdk.streamBuilder()
                        .table(process.env.ZEROBUS_TABLE_NAME)
                        .oauth(process.env.DATABRICKS_CLIENT_ID, process.env.DATABRICKS_CLIENT_SECRET)
                        .json().multiplexed(2).ackCallback({ onAck() {} }).build();
                    for (let i = 0; i < 5; i++) {
                        await stream.ingestRecord(JSON.stringify({ device_name: ${JSON.stringify(`${RUN_PREFIX}-i10-`)} + i, temp: i, humidity: i }));
                    }
                    await stream.flush();
                })().catch((error) => { console.error(error); process.exitCode = 1; });
            `;
            // Throws if the child exits non-zero or is still running after the timeout.
            execFileSync(process.execPath, ['-e', script], { env: process.env, timeout: 60_000, stdio: 'pipe' });
            wrote('i10', 5);
        });

        it('rejects invalid OAuth credentials', async () => {
            await assertBuildRejects(
                muxSdk.streamBuilder().table(tableName).oauth('invalid', 'invalid').json().multiplexed(2),
                TOKEN_REJECTED
            );
        });

        it('opens an ordinary stream with streamBuilder().build()', async () => {
            const stream = await builder().json().build();
            assert.ok(stream instanceof wrapper.ZerobusStream);
            try {
                for (let i = 0; i < 10; i++) {
                    await stream.ingestRecordOffset(JSON.stringify(row('i12', i)));
                }
                await stream.flush();
            } finally {
                await stream.close();
            }
            wrote('i12', 10);
        });

        it('checks the in-flight budget before opening any sub-stream', async () => {
            await assertBuildRejects(
                builder().json().maxInflightRequests(3).multiplexed(4),
                /Failed to create multiplexed stream: .*max_inflight_requests \(3\) .*stream_count \(4\)/
            );
        });

        it('passes a stream count of 64 through to the core', async () => {
            await assertBuildRejects(builder().json().maxInflightRequests(63).multiplexed(64), /\(63\) .*\(64\)/);
        });

        it('resolves the descriptor with the same code as createStream', async () => {
            await assertBuildRejects(builder().compiledProto('%%%').multiplexed(2), /Failed to decode descriptor/);
        });

        it('uses the headers provider to open the stream', async () => {
            let calls = 0;
            const headersProvider = {
                getHeadersCallback: () => {
                    calls++;
                    return [['authorization', 'Bearer invalid'], ['x-databricks-zerobus-table-name', tableName]];
                },
            };
            await assertBuildRejects(
                muxSdk.streamBuilder().table(tableName).headersProvider(headersProvider).json()
                    .recovery(false).recoveryRetries(0).multiplexed(2),
                STREAM_AUTH_REJECTED
            );
            assert.ok(calls >= 1);
        });

        it('lets Node exit and runs no callback after a failed build', { timeout: 90_000 }, async () => {
            const script = `
                const { ZerobusSdk } = require(${JSON.stringify(path.resolve(__dirname, '..', 'wrapper.js'))});
                const sdk = new ZerobusSdk(process.env.ZEROBUS_SERVER_ENDPOINT, process.env.DATABRICKS_WORKSPACE_URL);
                sdk.streamBuilder()
                    .table(process.env.ZEROBUS_TABLE_NAME)
                    .oauth(process.env.DATABRICKS_CLIENT_ID, process.env.DATABRICKS_CLIENT_SECRET)
                    .json().maxInflightRequests(3).multiplexed(4).ackCallback({ onAck() {} }).build()
                    .then(
                        (stream) => { process.exitCode = 1; return stream.close(); },
                        (error) => { if (!/max_inflight_requests/.test(error.message)) process.exitCode = 1; }
                    );
            `;
            // Throws if the child exits non-zero or is still running after the timeout.
            execFileSync(process.execPath, ['-e', script], { env: process.env, timeout: 60_000, stdio: 'pipe' });

            const { recorded, callback } = recordingCallback();
            await assertBuildRejects(
                builder().json().maxInflightRequests(3).multiplexed(4).ackCallback(callback),
                /max_inflight_requests/
            );
            await drainEventLoop();
            assert.deepStrictEqual(recorded.acked, []);
            assert.deepStrictEqual(recorded.errors, []);
        });

        it('rejects stream counts that would wrap or truncate to a valid count', async () => {
            const options: StreamConfigurationOptions = { recordType: RecordType.Json };
            for (const streamCount of [-1, 1.5, NaN, Infinity, 2 ** 32, 2 ** 32 + 2]) {
                await assert.rejects(
                    (sdk as any)._createMultiplexedStream({ tableName }, clientId, clientSecret, streamCount, options),
                    /Failed to create multiplexed stream: .*between 1 and 64/,
                    String(streamCount)
                );
            }
        });
    });
});
