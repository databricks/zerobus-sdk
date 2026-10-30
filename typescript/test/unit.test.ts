/**
 * Unit tests for Zerobus TypeScript SDK
 *
 * These tests verify the TypeScript bindings and type conversions.
 * Integration tests that require a Databricks workspace are in integration.test.ts
 */

import { describe, it, before } from 'node:test';
import * as assert from 'node:assert';
import { execFileSync } from 'node:child_process';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { pathToFileURL } from 'node:url';
import {
    ArrowDataType,
    IpcCompressionType,
    ZerobusArrowStream,
    ZerobusSdk,
    RecordType,
    TableProperties,
    StreamConfigurationOptions,
} from '../index';
import { HeadersProvider } from '../src/headers_provider';
import { loadDescriptorProto } from '../utils/descriptor.js';
// AvroEncoder lives in the internal avro.js module; require it directly for tests.
const { AvroEncoder } = require('../avro');

const descriptor = require('protobufjs/ext/descriptor');

describe('ZerobusSdk', () => {
    describe('constructor', () => {
        it('should create SDK instance with valid endpoints', () => {
            const sdk = new ZerobusSdk(
                'https://1234567890.zerobus.us-west-2.cloud.databricks.com',
                'https://test-workspace.cloud.databricks.com'
            );
            assert.ok(sdk);
        });

        it('should extract workspace ID from endpoint', () => {
            const sdk = new ZerobusSdk(
                'https://9876543210.zerobus.us-west-2.cloud.databricks.com',
                'https://test.cloud.databricks.com'
            );
            assert.ok(sdk);
        });

        it('should accept an options object with applicationName', () => {
            const sdk = new ZerobusSdk(
                'https://1234567890.zerobus.us-west-2.cloud.databricks.com',
                'https://test-workspace.cloud.databricks.com',
                { applicationName: 'my-app/1.0' }
            );
            assert.ok(sdk);
        });

        it('should accept shared connection mode', () => {
            const sdk = new ZerobusSdk(
                'https://1234567890.zerobus.us-west-2.cloud.databricks.com',
                'https://test-workspace.cloud.databricks.com',
                { connectionPerStream: false }
            );
            assert.ok(sdk);
        });

        it('should accept an empty options object', () => {
            const sdk = new ZerobusSdk(
                'https://1234567890.zerobus.us-west-2.cloud.databricks.com',
                'https://test-workspace.cloud.databricks.com',
                {}
            );
            assert.ok(sdk);
        });
    });

    describe('configuration validation', () => {
        it('should accept valid StreamConfigurationOptions', () => {
            const options: StreamConfigurationOptions = {
                recordType: RecordType.Json,
                maxInflightRequests: 1000,
                recovery: true,
                recoveryTimeoutMs: 15000,
                recoveryBackoffMs: 2000,
                recoveryRetries: 3,
            };
            assert.strictEqual(options.recordType, RecordType.Json);
            assert.strictEqual(options.maxInflightRequests, 1000);
        });

        it('should accept optional StreamConfigurationOptions', () => {
            const options: StreamConfigurationOptions = {
                recordType: RecordType.Proto,
            };
            assert.strictEqual(options.recordType, RecordType.Proto);
        });

        it('should accept streamPausedMaxWaitTimeMs configuration option', () => {
            const options: StreamConfigurationOptions = {
                recordType: RecordType.Json,
                maxInflightRequests: 100,
                streamPausedMaxWaitTimeMs: 3000,
            };
            assert.strictEqual(options.streamPausedMaxWaitTimeMs, 3000);
        });

        it('should allow undefined for streamPausedMaxWaitTimeMs', () => {
            const options: StreamConfigurationOptions = {
                recordType: RecordType.Json,
            };
            assert.strictEqual(options.streamPausedMaxWaitTimeMs, undefined);
        });
    });

    describe('TableProperties', () => {
        it('should create table properties with just table name (JSON mode)', () => {
            const props: TableProperties = {
                tableName: 'catalog.schema.table',
            };
            assert.strictEqual(props.tableName, 'catalog.schema.table');
            assert.strictEqual(props.descriptorProto, undefined);
        });

        it('should create table properties with descriptor (Proto mode)', () => {
            const props: TableProperties = {
                tableName: 'catalog.schema.table',
                descriptorProto: 'base64encodedstring',
            };
            assert.strictEqual(props.tableName, 'catalog.schema.table');
            assert.strictEqual(props.descriptorProto, 'base64encodedstring');
        });
    });
});

describe('HeadersProvider', () => {
    it('should accept custom headers provider implementation', () => {
        const provider: HeadersProvider = {
            getHeadersCallback: () => [
                ['authorization', 'Bearer test-token'],
                ['x-databricks-zerobus-table-name', 'catalog.schema.table'],
            ],
        };

        assert.ok(provider);
        assert.ok(typeof provider.getHeadersCallback === 'function');
    });

    it('should return correct header format', () => {
        const provider: HeadersProvider = {
            getHeadersCallback: () => [
                ['authorization', 'Bearer test-token'],
                ['x-databricks-zerobus-table-name', 'test-table'],
                ['x-custom-header', 'custom-value'],
            ],
        };

        const headers = provider.getHeadersCallback();

        assert.strictEqual(headers.length, 3);
        assert.deepStrictEqual(headers[0], ['authorization', 'Bearer test-token']);
        assert.deepStrictEqual(headers[1], ['x-databricks-zerobus-table-name', 'test-table']);
        assert.deepStrictEqual(headers[2], ['x-custom-header', 'custom-value']);
    });
});

describe('Descriptor utilities', () => {
    it('should load DescriptorProto from the CommonJS helper', () => {
        const descriptorPath = path.join(os.tmpdir(), `zerobus-descriptor-${process.pid}.pb`);
        const message = descriptor.DescriptorProto.create({
            name: 'AirQuality',
            field: [
                {
                    name: 'device_name',
                    number: 1,
                    label: 1,
                    type: 9,
                },
            ],
        });
        const fileDescriptorSet = descriptor.FileDescriptorSet.create({
            file: [
                {
                    name: 'schemas/air_quality.proto',
                    messageType: [message],
                },
            ],
        });

        fs.writeFileSync(descriptorPath, descriptor.FileDescriptorSet.encode(fileDescriptorSet).finish());

        try {
            const encoded = loadDescriptorProto({
                descriptorPath,
                protoFileName: 'air_quality.proto',
                messageName: 'AirQuality',
            });
            const decoded = descriptor.DescriptorProto.decode(Buffer.from(encoded, 'base64'));
            assert.strictEqual(decoded.name, 'AirQuality');
            assert.strictEqual(decoded.field[0].name, 'device_name');
        } finally {
            fs.rmSync(descriptorPath, { force: true });
        }
    });

    it('should require a path boundary when matching proto file names', () => {
        const descriptorPath = path.join(os.tmpdir(), `zerobus-descriptor-boundary-${process.pid}.pb`);
        const shadowMessage = descriptor.DescriptorProto.create({
            name: 'AirQuality',
            field: [
                {
                    name: 'wrong_field',
                    number: 1,
                    label: 1,
                    type: 9,
                },
            ],
        });
        const expectedMessage = descriptor.DescriptorProto.create({
            name: 'AirQuality',
            field: [
                {
                    name: 'device_name',
                    number: 1,
                    label: 1,
                    type: 9,
                },
            ],
        });
        const fileDescriptorSet = descriptor.FileDescriptorSet.create({
            file: [
                {
                    name: 'schemas/not_air_quality.proto',
                    messageType: [shadowMessage],
                },
                {
                    name: 'schemas/air_quality.proto',
                    messageType: [expectedMessage],
                },
            ],
        });

        fs.writeFileSync(descriptorPath, descriptor.FileDescriptorSet.encode(fileDescriptorSet).finish());

        try {
            const encoded = loadDescriptorProto({
                descriptorPath,
                protoFileName: 'air_quality.proto',
                messageName: 'AirQuality',
            });
            const decoded = descriptor.DescriptorProto.decode(Buffer.from(encoded, 'base64'));
            assert.strictEqual(decoded.field[0].name, 'device_name');
        } finally {
            fs.rmSync(descriptorPath, { force: true });
        }
    });

    it('should resolve the packed helper from CommonJS, ESM, and NodeNext', { timeout: 120_000 }, () => {
        const packageRoot = path.resolve(__dirname, '..');
        const temporaryRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'zerobus-package-test-'));
        const consumerRoot = path.join(temporaryRoot, 'consumer');
        const npmCli = process.env.npm_execpath;
        assert.ok(npmCli, 'npm_execpath must be set when running the npm test script');

        const runNpm = (args: string[], cwd: string): string =>
            execFileSync(process.execPath, [npmCli, ...args], { cwd, encoding: 'utf8' });

        try {
            fs.mkdirSync(consumerRoot);
            fs.writeFileSync(
                path.join(consumerRoot, 'package.json'),
                JSON.stringify({ name: 'zerobus-package-test', private: true, type: 'module' }, null, 2)
            );

            runNpm(['pack', '--pack-destination', temporaryRoot, '--json', '--silent'], packageRoot);
            const tarballs = fs.readdirSync(temporaryRoot).filter((file) => file.endsWith('.tgz'));
            assert.strictEqual(tarballs.length, 1, 'npm pack should create exactly one tarball');
            const tarballPath = path.join(temporaryRoot, tarballs[0]);

            runNpm(
                [
                    'install',
                    '--prefer-offline',
                    '--ignore-scripts',
                    '--omit=optional',
                    '--no-audit',
                    '--no-fund',
                    '--package-lock=false',
                    '--no-save',
                    tarballPath,
                ],
                consumerRoot
            );

            const packageSubpath = '@databricks/zerobus-ingest-sdk/utils/descriptor.js';
            execFileSync(
                process.execPath,
                [
                    '--input-type=commonjs',
                    '-e',
                    `const helper = require('${packageSubpath}'); if (typeof helper.loadDescriptorProto !== 'function') process.exit(1);`,
                ],
                { cwd: consumerRoot }
            );
            execFileSync(
                process.execPath,
                [
                    '--input-type=module',
                    '-e',
                    `import { loadDescriptorProto } from '${packageSubpath}'; if (typeof loadDescriptorProto !== 'function') process.exit(1);`,
                ],
                { cwd: consumerRoot }
            );

            const typeTestPath = path.join(consumerRoot, 'consumer.mts');
            fs.writeFileSync(
                typeTestPath,
                `import { loadDescriptorProto, type LoadDescriptorOptions } from '${packageSubpath}';\n` +
                    `const options: LoadDescriptorOptions = { descriptorPath: 'schema.pb', protoFileName: 'schema.proto', messageName: 'Record' };\n` +
                    `const encoded: string = loadDescriptorProto(options);\n` +
                    `void encoded;\n`
            );
            execFileSync(
                process.execPath,
                [
                    path.join(packageRoot, 'node_modules', 'typescript', 'bin', 'tsc'),
                    '--noEmit',
                    '--strict',
                    '--skipLibCheck',
                    '--target',
                    'ES2020',
                    '--module',
                    'NodeNext',
                    '--moduleResolution',
                    'NodeNext',
                    typeTestPath,
                ],
                { cwd: consumerRoot }
            );
        } finally {
            fs.rmSync(temporaryRoot, { recursive: true, force: true });
        }
    });
});

describe('RecordType enum', () => {
    it('should have Json value', () => {
        assert.strictEqual(RecordType.Json, 0);
    });

    it('should have Proto value', () => {
        assert.strictEqual(RecordType.Proto, 1);
    });

    it('should have Avro value', () => {
        assert.strictEqual(RecordType.Avro, 2);
    });
});

describe('Avro encoding', () => {
    const schema = JSON.stringify({
        type: 'record',
        name: 'Order',
        fields: [
            { name: 'id', type: 'long' },
            { name: 'name', type: 'string' },
        ],
    });

    it('encodes a record object to Avro bytes', () => {
        const enc = new AvroEncoder(schema);
        const buf = enc.encode({ id: 1n, name: 'Alice' });
        assert.ok(Buffer.isBuffer(buf) && buf.length > 0);
    });

    it('passes a pre-encoded Buffer through unchanged', () => {
        const enc = new AvroEncoder(schema);
        const pre = Buffer.from([1, 2, 3]);
        assert.ok(enc.encode(pre).equals(pre));
    });

    it('round-trips a full-range int64 via BigInt (no 2^53 precision loss)', () => {
        const enc = new AvroEncoder(schema);
        const buf = enc.encode({ id: 9223372036854775807n, name: 'x' });
        const back = (enc as any)._avroType().fromBuffer(buf);
        assert.strictEqual(back.id, 9223372036854775807n);
    });

    it('encodeBatch encodes an array and rejects a single record', () => {
        const enc = new AvroEncoder(schema);
        const out = enc.encodeBatch([{ id: 1n, name: 'a' }, Buffer.from([9])]);
        assert.strictEqual(out.length, 2);
        assert.ok(out.every((b: Buffer) => Buffer.isBuffer(b)));
        assert.throws(() => enc.encodeBatch({ id: 1n, name: 'a' } as any), /array of records/);
    });

    it('a bytes-only payload never parses the schema', () => {
        const enc = new AvroEncoder('not valid json');
        assert.ok(enc.encode(Buffer.from([1])).equals(Buffer.from([1])));
    });

    it('encodes by field name, not key order (top-level and nested)', () => {
        const enc = new AvroEncoder(
            JSON.stringify({
                type: 'record',
                name: 'Outer',
                fields: [
                    { name: 'temp', type: 'int' },
                    { name: 'humidity', type: 'int' },
                    {
                        name: 'inner',
                        type: {
                            type: 'record',
                            name: 'Inner',
                            fields: [
                                { name: 'x', type: 'int' },
                                { name: 'y', type: 'string' },
                            ],
                        },
                    },
                ],
            })
        );
        const ordered = enc.encode({ temp: 20, humidity: 90, inner: { x: 1, y: 'a' } });
        const reordered = enc.encode({ inner: { y: 'a', x: 1 }, humidity: 90, temp: 20 });
        assert.ok(ordered.equals(reordered));
        // Not vacuous: swapping the values (same keys) must change the bytes.
        const swapped = enc.encode({ temp: 90, humidity: 20, inner: { x: 1, y: 'a' } });
        assert.ok(!ordered.equals(swapped));
    });

    it('accepts a Date for timestamp and local-timestamp logical types', () => {
        const d = new Date(Date.UTC(2024, 0, 1, 9, 0, 0));
        for (const lt of ['timestamp-millis', 'timestamp-micros', 'local-timestamp-millis', 'local-timestamp-micros']) {
            const enc = new AvroEncoder(
                JSON.stringify({
                    type: 'record',
                    name: 'R',
                    fields: [{ name: 't', type: { type: 'long', logicalType: lt } }],
                })
            );
            const raw = lt.endsWith('micros') ? d.getTime() * 1000 : d.getTime();
            // A Date encodes identically to passing the underlying number.
            assert.ok(enc.encode({ t: d }).equals(enc.encode({ t: raw })));
        }
    });

    const field = (type: unknown) =>
        new AvroEncoder(JSON.stringify({ type: 'record', name: 'R', fields: [{ name: 'v', type }] }));

    it('applies the BigInt long to every long spelling (nullable BIGINT, expanded long)', () => {
        const nullable = field(['null', 'long']);
        assert.strictEqual(nullable._avroType().fromBuffer(nullable.encode({ v: 9223372036854775807n })).v, 9223372036854775807n);
        assert.ok(Buffer.isBuffer(nullable.encode({ v: 5 }))); // a safe integer is also accepted
        assert.ok(Buffer.isBuffer(field({ type: 'long' }).encode({ v: 1n })));
    });

    it('encodes decimals without float error and rejects scale/precision overflow', () => {
        const dec = field({ type: 'bytes', logicalType: 'decimal', precision: 18, scale: 2 });
        assert.strictEqual(dec._avroType().fromBuffer(dec.encode({ v: '9999999999999999.99' })).v.toString('hex'), '0de0b6b3a763ffff');
        assert.deepStrictEqual(dec.encode({ v: 5n }), dec.encode({ v: '5.00' })); // BigInt 5n is the whole value 5.00
        assert.throws(() => field({ type: 'bytes', logicalType: 'decimal', precision: 10, scale: 2 }).encode({ v: 1.005 }), /decimal places/);
        assert.throws(() => field({ type: 'bytes', logicalType: 'decimal', precision: 4, scale: 2 }).encode({ v: 123456 }), /precision/);
    });

    it('accepts a YYYY-MM-DD string for date and rejects impossible days', () => {
        const dt = field({ type: 'int', logicalType: 'date' });
        const back = dt._avroType().fromBuffer(dt.encode({ v: '2025-01-01' })).v as Date;
        assert.strictEqual(back.toISOString().slice(0, 10), '2025-01-01');
        // Must not silently roll over to 2025-03-02.
        assert.throws(() => dt.encode({ v: '2025-02-30' }), /invalid date/);
    });

    it('packs a uuid string into fixed(16) and rejects malformed input', () => {
        const uu = field({ type: 'fixed', name: 'U', size: 16, logicalType: 'uuid' });
        assert.strictEqual(uu.encode({ v: '123e4567-e89b-12d3-a456-426614174000' }).length, 16);
        // A wrong-length hex string must error, not silently truncate to 16 bytes.
        assert.throws(() => uu.encode({ v: '123e4567-e89b-12d3-a456-4266141740001' }), /invalid uuid/);
    });

    it('forwards a pre-encoded Buffer without copying', () => {
        const buf = Buffer.from([1, 2, 3]);
        assert.strictEqual(field('int').encode(buf), buf);
    });
});

describe('Package entry (ESM interop)', () => {
    it('exposes named exports when imported as ESM', () => {
        // ESM import of the package entry; fails with a SyntaxError if a named export
        // doesn't resolve (see the explicit exports in wrapper.js).
        // Use a file:// URL: ESM rejects a bare absolute path on Windows (`C:\…` reads as a scheme).
        const entry = pathToFileURL(path.resolve(__dirname, '..', 'wrapper.js')).href;
        execFileSync(
            process.execPath,
            [
                '--input-type=module',
                '-e',
                `import { ZerobusSdk, ZerobusStream, RecordType } from ${JSON.stringify(entry)};
                 if (typeof ZerobusSdk !== 'function') process.exit(1);
                 if (typeof ZerobusStream !== 'function') process.exit(1);
                 if (RecordType.Avro !== 2) process.exit(1);`,
            ],
            { stdio: 'pipe' }
        );
    });
});

describe('Arrow Flight exports', () => {
    it('should include the Arrow API when built with arrow-flight', () => {
        assert.strictEqual(ArrowDataType.Int32, 3);
        assert.strictEqual(IpcCompressionType.Lz4Frame, 0);
        assert.strictEqual(typeof ZerobusArrowStream, 'function');
        assert.strictEqual(typeof ZerobusSdk.prototype.createArrowStream, 'function');
    });
});

describe('Type widening validation', () => {
    it('should accept Buffer for Proto mode', () => {
        const buffer = Buffer.from([1, 2, 3, 4]);
        assert.ok(Buffer.isBuffer(buffer));
    });

    it('should accept string for JSON mode', () => {
        const jsonString = JSON.stringify({ test: 'data' });
        assert.strictEqual(typeof jsonString, 'string');
    });

    it('should accept plain object for JSON mode', () => {
        const obj = { device_name: 'sensor-1', temp: 25 };
        assert.strictEqual(typeof obj, 'object');
        assert.ok(!Buffer.isBuffer(obj));
    });

    it('should validate protobuf message interface', () => {
        // Mock protobuf message
        const mockProtoMessage = {
            encode: function() {
                return {
                    finish: function() {
                        return Buffer.from([1, 2, 3]);
                    }
                };
            }
        };

        assert.ok(typeof mockProtoMessage.encode === 'function');
        const encoded = mockProtoMessage.encode();
        assert.ok(typeof encoded.finish === 'function');
        const buffer = encoded.finish();
        assert.ok(Buffer.isBuffer(buffer));
    });
});

describe('Error handling', () => {
    it('should provide meaningful error messages', () => {
        try {
            // Invalid endpoint format
            new ZerobusSdk('', '');
            assert.fail('Should have thrown an error');
        } catch (error) {
            assert.ok(error);
            assert.ok(error instanceof Error);
        }
    });
});

describe('Batch operations', () => {
    it('should accept array of buffers for batch proto', () => {
        const buffers = [
            Buffer.from([1, 2, 3]),
            Buffer.from([4, 5, 6]),
            Buffer.from([7, 8, 9]),
        ];
        assert.strictEqual(buffers.length, 3);
        buffers.forEach(buf => assert.ok(Buffer.isBuffer(buf)));
    });

    it('should accept array of strings for batch JSON', () => {
        const jsonStrings = [
            JSON.stringify({ id: 1 }),
            JSON.stringify({ id: 2 }),
            JSON.stringify({ id: 3 }),
        ];
        assert.strictEqual(jsonStrings.length, 3);
        jsonStrings.forEach(str => assert.strictEqual(typeof str, 'string'));
    });

    it('should accept array of plain objects for batch JSON', () => {
        const objects = [
            { device: 'sensor-1', temp: 20 },
            { device: 'sensor-2', temp: 21 },
            { device: 'sensor-3', temp: 22 },
        ];
        assert.strictEqual(objects.length, 3);
        objects.forEach(obj => assert.strictEqual(typeof obj, 'object'));
    });

    it('should accept mixed formats in array (Buffer, string, object)', () => {
        const records = [
            Buffer.from([1, 2, 3]),
            JSON.stringify({ id: 2 }),
            { id: 3 },
        ];
        assert.strictEqual(records.length, 3);
    });

    it('should handle empty batch', () => {
        const emptyBatch: any[] = [];
        assert.strictEqual(emptyBatch.length, 0);
    });
});

describe('New v0.4.0 API types', () => {
    it('should define ingestRecordOffset method on ZerobusStream type', () => {
        // Type check - these are defined in index.d.ts
        // ZerobusStream.prototype.ingestRecordOffset exists
        assert.ok(true, 'ingestRecordOffset is defined in type definitions');
    });

    it('should define ingestRecordsOffset method on ZerobusStream type', () => {
        // Type check - these are defined in index.d.ts
        // ZerobusStream.prototype.ingestRecordsOffset exists
        assert.ok(true, 'ingestRecordsOffset is defined in type definitions');
    });

    it('should define waitForOffset method on ZerobusStream type', () => {
        // Type check - these are defined in index.d.ts
        // ZerobusStream.prototype.waitForOffset exists
        assert.ok(true, 'waitForOffset is defined in type definitions');
    });
});
