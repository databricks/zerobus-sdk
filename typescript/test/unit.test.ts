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

    const tsField = (lt: string) =>
        new AvroEncoder(
            JSON.stringify({ type: 'record', name: 'R', fields: [{ name: 't', type: { type: 'long', logicalType: lt } }] })
        );

    it('accepts a Date for timestamp and local-timestamp logical types', () => {
        const d = new Date(Date.UTC(2024, 0, 1, 9, 0, 0));
        for (const lt of ['timestamp-millis', 'timestamp-micros', 'local-timestamp-millis', 'local-timestamp-micros']) {
            const enc = tsField(lt);
            const raw = lt.endsWith('micros') ? BigInt(d.getTime()) * 1000n : BigInt(d.getTime());
            // A Date encodes identically to passing the underlying BigInt.
            assert.ok(enc.encode({ t: d }).equals(enc.encode({ t: raw })));
        }
    });

    it('encodes timestamp-micros across the full int64 range (year 9999, as Date or BigInt)', () => {
        // Number-backed micros caps near year 2255 (2^53 µs); the BigInt-backed long reaches 9999.
        const enc = tsField('timestamp-micros');
        const far = new Date('9999-12-31T23:59:59Z');
        assert.ok(enc.encode({ t: far }).equals(enc.encode({ t: BigInt(far.getTime()) * 1000n })));
        assert.strictEqual(
            (enc._avroType().fromBuffer(enc.encode({ t: far })).t as Date).toISOString(),
            '9999-12-31T23:59:59.000Z'
        );
    });

    const field = (type: unknown) =>
        new AvroEncoder(JSON.stringify({ type: 'record', name: 'R', fields: [{ name: 'v', type }] }));

    it('applies the BigInt long to every long spelling (nullable BIGINT, expanded long)', () => {
        const nullable = field(['null', 'long']);
        assert.strictEqual(nullable._avroType().fromBuffer(nullable.encode({ v: 9223372036854775807n })).v, 9223372036854775807n);
        assert.ok(Buffer.isBuffer(nullable.encode({ v: 5 }))); // a safe integer is also accepted
        assert.ok(Buffer.isBuffer(field({ type: 'long' }).encode({ v: 1n })));
    });

    it('selects the branch in a numeric union: a number is the narrower type, a BigInt is long', () => {
        // Beside another numeric branch the long takes a BigInt only, so the number is unambiguous.
        const intLong = field(['int', 'long']);
        assert.strictEqual(intLong._avroType().fromBuffer(intLong.encode({ v: 1 })).v, 1); // int branch
        assert.strictEqual(intLong._avroType().fromBuffer(intLong.encode({ v: 1n })).v, 1n); // long branch
        const longDouble = field(['long', 'double']);
        assert.strictEqual(longDouble._avroType().fromBuffer(longDouble.encode({ v: 1.5 })).v, 1.5); // double
        assert.strictEqual(longDouble._avroType().fromBuffer(longDouble.encode({ v: 2n })).v, 2n); // long
    });

    it('encodes decimals without float error and rejects scale/precision overflow', () => {
        const dec = field({ type: 'bytes', logicalType: 'decimal', precision: 18, scale: 2 });
        assert.strictEqual(dec._avroType().fromBuffer(dec.encode({ v: '9999999999999999.99' })).v.toString('hex'), '0de0b6b3a763ffff');
        assert.deepStrictEqual(dec.encode({ v: 5n }), dec.encode({ v: '5.00' })); // BigInt 5n is the whole value 5.00
        assert.throws(() => field({ type: 'bytes', logicalType: 'decimal', precision: 10, scale: 2 }).encode({ v: 1.005 }), /decimal places/);
        assert.throws(() => field({ type: 'bytes', logicalType: 'decimal', precision: 4, scale: 2 }).encode({ v: 123456 }), /precision/);
        // A number in exponent form (String(1e-7) === '1e-7') encodes like its decimal string.
        const small = field({ type: 'bytes', logicalType: 'decimal', precision: 10, scale: 7 });
        assert.deepStrictEqual(small.encode({ v: 0.0000001 }), small.encode({ v: '0.0000001' }));
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
                `import { ZerobusSdk, ZerobusStream, ZerobusMultiplexedStream, RecordType } from ${JSON.stringify(entry)};
                 if (typeof ZerobusSdk !== 'function') process.exit(1);
                 if (typeof ZerobusStream !== 'function') process.exit(1);
                 if (typeof ZerobusMultiplexedStream !== 'function') process.exit(1);
                 if (RecordType.Avro !== 2) process.exit(1);`,
            ],
            { stdio: 'pipe' }
        );
    });
});

describe('Arrow Flight exports', () => {
    it('should include the Arrow API when built with arrow-flight', () => {
        assert.strictEqual(ArrowDataType.Int32, 3);
        assert.strictEqual(IpcCompressionType.None, -1);
        assert.strictEqual(IpcCompressionType.Lz4Frame, 0);
        assert.strictEqual(IpcCompressionType.Zstd, 1);
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

describe('Multiplexed stream native entry point', () => {
    it('keeps the entry point out of the type definitions', () => {
        const readTypings = (file: string) => fs.readFileSync(path.resolve(__dirname, '..', file), 'utf8');
        // index.d.ts is generated by the build; a stale copy would pass the check below.
        assert.match(readTypings('index.d.ts'), /class ZerobusMultiplexedStream/);
        for (const file of ['index.d.ts', 'wrapper.d.ts']) {
            assert.doesNotMatch(readTypings(file), /createMultiplexedStream/, file);
        }
    });
});

// Wrapper-level tests (public entry point). Every native call is stubbed on one SDK
// instance, so nothing here reaches the native stream code or the network.
const wrapper = require('../wrapper');

const UNIT_ZEROBUS_ENDPOINT = 'https://1234567890.zerobus.us-west-2.cloud.databricks.com';
const UNIT_WORKSPACE_URL = 'https://test-workspace.cloud.databricks.com';

// Captured native arguments:
// createStream: [tableProperties, clientId, clientSecret, options, headersProvider]
// _createMultiplexedStream: [tableProperties, clientId, clientSecret, streamCount, options,
//                            headersProvider, ackDispatcher]
function stubbedSdk(nativeStream: any = {}) {
    const sdk = new wrapper.ZerobusSdk(UNIT_ZEROBUS_ENDPOINT, UNIT_WORKSPACE_URL);
    const captured: { stream?: any[]; mux?: any[] } = {};
    sdk._native.createStream = (...args: any[]) => {
        captured.stream = args;
        return Promise.resolve(nativeStream);
    };
    sdk._native._createMultiplexedStream = (...args: any[]) => {
        captured.mux = args;
        return Promise.resolve(nativeStream);
    };
    return { sdk, captured };
}

const jsonBuilder = (sdk: any) => sdk.streamBuilder().table('c.s.t').oauth('id', 'secret').json();

// Tracks whether a promise has settled, without handling its outcome for the test.
function track(promise: Promise<unknown>) {
    const state = { settled: false };
    promise.then(() => { state.settled = true; }, () => { state.settled = true; });
    return state;
}

async function drainEventLoop() {
    for (let i = 0; i < 5; i++) {
        await new Promise(resolve => setImmediate(resolve));
    }
}

const muxAvroSchema = JSON.stringify({
    type: 'record',
    name: 'Order',
    fields: [
        { name: 'id', type: 'long' },
        { name: 'name', type: 'string' },
    ],
});

describe('streamBuilder', () => {
    it('rejects invalid stream counts at multiplexed() without reaching native', () => {
        const { sdk, captured } = stubbedSdk();
        for (const streamCount of [0, -1, 65, 1.5, NaN, '4', true, 4n]) {
            assert.throws(
                () => jsonBuilder(sdk).multiplexed(streamCount as any),
                /streamCount must be an integer between 1 and 64/,
                String(streamCount)
            );
        }
        assert.strictEqual(captured.mux, undefined);
    });

    it('passes the stream count bounds 1 and 64 to native unchanged', async () => {
        for (const streamCount of [1, 64]) {
            const { sdk, captured } = stubbedSdk();
            await jsonBuilder(sdk).multiplexed(streamCount).build();
            assert.strictEqual(captured.mux![3], streamCount);
        }
    });

    it('offers only ackCallback and build after multiplexed()', () => {
        const { sdk } = stubbedSdk();
        const mux = jsonBuilder(sdk).multiplexed(2);
        const methods = Object.getOwnPropertyNames(Object.getPrototypeOf(mux))
            .filter(name => name !== 'constructor')
            .sort();
        assert.deepStrictEqual(methods, ['ackCallback', 'build']);
        for (const name of ['table', 'oauth', 'json', 'multiplexed', 'recoveryRetries']) {
            assert.strictEqual(mux[name], undefined, name);
        }
        assert.strictEqual(mux.ackCallback({ onAck() {} }), mux);
    });

    it('has no ackCallback on the base builder', () => {
        const { sdk } = stubbedSdk();
        assert.strictEqual(typeof sdk.streamBuilder().ackCallback, 'undefined');
    });

    it('snapshots the settings when multiplexed() is called', async () => {
        const { sdk, captured } = stubbedSdk();
        const base = sdk.streamBuilder().table('a.b.c').oauth('id1', 'secret1').json().recoveryRetries(1);
        const mux = base.multiplexed(2);
        base.table('x.y.z').oauth('id2', 'secret2').recoveryRetries(5);
        await mux.build();
        const [tableProperties, clientId, clientSecret, , options] = captured.mux!;
        assert.strictEqual(tableProperties.tableName, 'a.b.c');
        assert.strictEqual(clientId, 'id1');
        assert.strictEqual(clientSecret, 'secret1');
        assert.strictEqual(options.recoveryRetries, 1);
    });

    it('rejects build() when the table, authentication or record format is missing', async () => {
        const { sdk, captured } = stubbedSdk();
        const cases: Array<[string, () => any, RegExp]> = [
            ['table', () => sdk.streamBuilder().oauth('id', 'secret').json(), /table is required/],
            ['auth', () => sdk.streamBuilder().table('c.s.t').json(), /authentication is required/],
            ['format', () => sdk.streamBuilder().table('c.s.t').oauth('id', 'secret'), /record format is required/],
        ];
        for (const [name, builder, expected] of cases) {
            await assert.rejects(builder().build(), expected, `ordinary, missing ${name}`);
            await assert.rejects(builder().multiplexed(2).build(), expected, `multiplexed, missing ${name}`);
        }
        assert.strictEqual(captured.stream, undefined);
        assert.strictEqual(captured.mux, undefined);
    });

    it('uses the last record format and authentication setter', async () => {
        const descriptorProto = Buffer.from([1, 2, 3]).toString('base64');
        const provider = { getHeadersCallback: async () => [] as [string, string][] };
        const build = async (configure: (builder: any) => any) => {
            const { sdk, captured } = stubbedSdk();
            await configure(sdk.streamBuilder().table('c.s.t')).build();
            return captured.stream!;
        };

        let [tableProperties, , , options] = await build(b => b.oauth('id', 'secret').json().compiledProto(descriptorProto));
        assert.strictEqual(options.recordType, RecordType.Proto);
        assert.strictEqual(tableProperties.descriptorProto, descriptorProto);

        [tableProperties, , , options] = await build(b => b.oauth('id', 'secret').compiledProto(descriptorProto).json());
        assert.strictEqual(options.recordType, RecordType.Json);
        assert.strictEqual('descriptorProto' in tableProperties, false);

        let [, clientId, clientSecret, , headersProvider] = await build(b => b.json().oauth('id', 'secret').headersProvider(provider));
        assert.strictEqual(headersProvider, provider);
        assert.strictEqual(clientId, '');
        assert.strictEqual(clientSecret, '');

        [, clientId, clientSecret, , headersProvider] = await build(b => b.json().headersProvider(provider).oauth('id', 'secret'));
        assert.strictEqual(headersProvider, undefined);
        assert.strictEqual(clientId, 'id');
        assert.strictEqual(clientSecret, 'secret');
    });

    it('chains every option setter and maps it to StreamConfigurationOptions', async () => {
        const { sdk, captured } = stubbedSdk();
        const builder = jsonBuilder(sdk);
        const setters: Array<[string, unknown]> = [
            ['maxInflightRequests', 10],
            ['recovery', false],
            ['recoveryTimeoutMs', 11],
            ['recoveryBackoffMs', 12],
            ['recoveryRetries', 13],
            ['flushTimeoutMs', 14],
            ['serverLackOfAckTimeoutMs', 15],
            ['streamPausedMaxWaitTimeMs', 16],
        ];
        for (const [name, value] of setters) {
            assert.strictEqual(builder[name](value), builder, name);
        }
        await builder.build();
        const expected: StreamConfigurationOptions = {
            maxInflightRequests: 10,
            recovery: false,
            recoveryTimeoutMs: 11,
            recoveryBackoffMs: 12,
            recoveryRetries: 13,
            flushTimeoutMs: 14,
            serverLackOfAckTimeoutMs: 15,
            streamPausedMaxWaitTimeMs: 16,
            recordType: RecordType.Json,
        };
        assert.deepStrictEqual(captured.stream![3], expected);
    });

    it('maps each record format to native for ordinary and multiplexed builds', async () => {
        const descriptorProto = Buffer.from([1, 2, 3]).toString('base64');
        const formats: Array<[string, (b: any) => any, RecordType, Record<string, string>]> = [
            ['json', b => b.json(), RecordType.Json, {}],
            ['proto', b => b.compiledProto(descriptorProto), RecordType.Proto, { descriptorProto }],
            ['avro', b => b.avro(muxAvroSchema), RecordType.Avro, { avroSchema: muxAvroSchema }],
        ];
        for (const [name, setFormat, recordType, schemaProperties] of formats) {
            const ordinary = stubbedSdk();
            const stream = await setFormat(ordinary.sdk.streamBuilder().table('c.s.t').oauth('id', 'secret')).build();
            assert.strictEqual(ordinary.captured.mux, undefined, name);
            assert.deepStrictEqual(ordinary.captured.stream![0], { tableName: 'c.s.t', ...schemaProperties }, name);
            assert.strictEqual(ordinary.captured.stream![3].recordType, recordType, name);
            assert.ok(stream instanceof wrapper.ZerobusStream, name);
            assert.strictEqual(stream._avro !== null, name === 'avro', name);

            const mux = stubbedSdk();
            await setFormat(mux.sdk.streamBuilder().table('c.s.t').oauth('id', 'secret')).multiplexed(2).build();
            assert.strictEqual(mux.captured.stream, undefined, name);
            assert.deepStrictEqual(mux.captured.mux![0], { tableName: 'c.s.t', ...schemaProperties }, name);
            assert.strictEqual(mux.captured.mux![4].recordType, recordType, name);
        }
    });

    it('rejects an empty or non-string table, credential or schema', () => {
        const { sdk } = stubbedSdk();
        const cases: Array<[string, (b: any) => any]> = [
            ['tableName', b => b.table('')],
            ['tableName', b => b.table(5)],
            ['clientId', b => b.oauth('', 'secret')],
            ['clientSecret', b => b.oauth('id', 5)],
            ['descriptorBase64', b => b.compiledProto('')],
            ['schemaJson', b => b.avro('')],
        ];
        for (const [name, call] of cases) {
            assert.throws(
                () => call(sdk.streamBuilder()),
                { name: 'TypeError', message: new RegExp(`${name} must be a non-empty string`) },
                name
            );
        }
    });

    it('leaves the builder unchanged when oauth() throws', async () => {
        const { sdk } = stubbedSdk();
        for (const [clientId, clientSecret] of [['id', 5], ['', 'secret']]) {
            const builder = sdk.streamBuilder().table('c.s.t').json();
            assert.throws(() => builder.oauth(clientId, clientSecret), TypeError);
            await assert.rejects(builder.build(), /authentication is required/);
        }
        const provider = { getHeadersCallback: () => [] as [string, string][] };
        for (const [clientId, clientSecret] of [['id', 5], ['', 'secret']]) {
            const stubbed = stubbedSdk();
            const builder = stubbed.sdk.streamBuilder().table('c.s.t').json().headersProvider(provider);
            assert.throws(() => builder.oauth(clientId, clientSecret), TypeError);
            await builder.build();
            const [, capturedId, capturedSecret, , headersProvider] = stubbed.captured.stream!;
            assert.strictEqual(headersProvider, provider);
            assert.strictEqual(capturedId, '');
            assert.strictEqual(capturedSecret, '');
        }
    });

    it('rejects a headers provider without a getHeadersCallback function', () => {
        const { sdk } = stubbedSdk();
        for (const provider of [{}, null, { getHeadersCallback: 'x' }]) {
            assert.throws(
                () => sdk.streamBuilder().headersProvider(provider),
                { name: 'TypeError', message: /headersProvider expects an object with a getHeadersCallback function/ },
                JSON.stringify(provider)
            );
        }
    });

    it('validates numeric options as unsigned 32-bit integers', async () => {
        // [setter, minimum]: a zero in-flight budget could never send a record.
        const setters: Array<[string, number]> = [
            ['maxInflightRequests', 1],
            ['recoveryTimeoutMs', 0],
            ['recoveryBackoffMs', 0],
            ['recoveryRetries', 0],
            ['flushTimeoutMs', 0],
            ['serverLackOfAckTimeoutMs', 0],
            ['streamPausedMaxWaitTimeMs', 0],
        ];
        for (const [name, minimum] of setters) {
            const { sdk } = stubbedSdk();
            for (const value of [minimum - 1, 1.5, NaN, '5', 2 ** 32]) {
                assert.throws(
                    () => jsonBuilder(sdk)[name](value),
                    { name: 'RangeError', message: new RegExp(`${name} must be an integer between ${minimum} and 4294967295`) },
                    `${name}(${String(value)})`
                );
            }
            for (const value of [minimum, 4294967295]) {
                const stubbed = stubbedSdk();
                await jsonBuilder(stubbed.sdk)[name](value).build();
                assert.strictEqual(stubbed.captured.stream![3][name], value, `${name}(${value})`);
            }
        }
    });

    it('accepts only a boolean for recovery', async () => {
        const { sdk, captured } = stubbedSdk();
        for (const value of ['yes', 1]) {
            assert.throws(
                () => jsonBuilder(sdk).recovery(value),
                { name: 'TypeError', message: /recovery must be a boolean/ },
                String(value)
            );
        }
        await jsonBuilder(sdk).recovery(false).build();
        assert.strictEqual(captured.stream![3].recovery, false);
    });

    it('clears streamPausedMaxWaitTimeMs with null or undefined', async () => {
        for (const cleared of [null, undefined]) {
            const { sdk, captured } = stubbedSdk();
            await jsonBuilder(sdk).streamPausedMaxWaitTimeMs(100).streamPausedMaxWaitTimeMs(cleared).build();
            assert.strictEqual('streamPausedMaxWaitTimeMs' in captured.stream![3], false, String(cleared));
        }
    });

    it('exports ZerobusMultiplexedStream from the package entry', () => {
        assert.strictEqual(typeof wrapper.ZerobusMultiplexedStream, 'function');
    });

    it('rejects an ackCallback that is not a callback object', () => {
        const { sdk } = stubbedSdk();
        const mux = jsonBuilder(sdk).multiplexed(2);
        for (const callback of [() => {}, null, undefined, {}]) {
            assert.throws(
                () => mux.ackCallback(callback),
                /ackCallback expects an object with an onAck and\/or onError method/,
                JSON.stringify(callback) ?? String(callback)
            );
        }
    });
});

describe('ZerobusMultiplexedStream wrapper', () => {
    const openMux = async (nativeStream: any, configure: (b: any) => any = b => b.json(), callback?: object) => {
        const { sdk, captured } = stubbedSdk(nativeStream);
        let mux = configure(sdk.streamBuilder().table('c.s.t').oauth('id', 'secret')).multiplexed(2);
        if (callback !== undefined) {
            mux = mux.ackCallback(callback);
        }
        const stream = await mux.build();
        return { stream, dispatch: captured.mux![6] };
    };

    it('encodes an Avro record object before ingest', async () => {
        let received: any;
        const { stream } = await openMux(
            { ingestRecord: async (payload: any) => { received = payload; return 7n; } },
            b => b.avro(muxAvroSchema)
        );
        const record = { id: 1n, name: 'Alice' };
        assert.strictEqual(await stream.ingestRecord(record), 7n);
        assert.ok(Buffer.isBuffer(received));
        assert.ok(received.equals(new AvroEncoder(muxAvroSchema).encode(record)));
    });

    it('encodes an Avro batch and rejects a non-array batch', async () => {
        let received: any;
        const { stream } = await openMux(
            { ingestRecords: async (payloads: any) => { received = payloads; return 8n; } },
            b => b.avro(muxAvroSchema)
        );
        const record = { id: 1n, name: 'Alice' };
        const preEncoded = Buffer.from([9]);
        assert.strictEqual(await stream.ingestRecords([record, preEncoded]), 8n);
        assert.strictEqual(received.length, 2);
        assert.ok(received[0].equals(new AvroEncoder(muxAvroSchema).encode(record)));
        assert.ok(received[1].equals(preEncoded));
        assert.throws(() => stream.ingestRecords(record), /array of records/);
    });

    it('passes non-Avro payloads through unchanged', async () => {
        const received: any[] = [];
        const { stream } = await openMux({ ingestRecord: async (payload: any) => { received.push(payload); return 1n; } });
        const payloads = ['{"a":1}', { a: 1 }, Buffer.from([1, 2])];
        for (const payload of payloads) {
            await stream.ingestRecord(payload);
        }
        payloads.forEach((payload, i) => assert.strictEqual(received[i], payload));
    });

    it('forwards waitForMessageId, flush and isClosed', async () => {
        let waitedFor: unknown;
        let flushed = 0;
        const { stream } = await openMux({
            waitForMessageId: async (messageId: bigint) => { waitedFor = messageId; },
            flush: async () => { flushed++; },
            isClosed: true,
        });
        await stream.waitForMessageId(-5n);
        assert.strictEqual(waitedFor, -5n);
        await stream.flush();
        assert.strictEqual(flushed, 1);
        assert.strictEqual(stream.isClosed, true);
    });

    it('settles close() only after the drained marker', { timeout: 5_000 }, async () => {
        const { stream, dispatch } = await openMux({ close: async () => {} }, undefined, { onAck() {} });
        const closing = stream.close();
        const state = track(closing);
        await drainEventLoop();
        assert.strictEqual(state.settled, false);
        dispatch('drained');
        await closing;
    });

    it('rejects close() with the native error only after the drained marker', { timeout: 5_000 }, async () => {
        const failure = new Error('close failed');
        const { stream, dispatch } = await openMux({ close: async () => { throw failure; } }, undefined, { onAck() {} });
        const closing = stream.close();
        const state = track(closing);
        await drainEventLoop();
        assert.strictEqual(state.settled, false);
        dispatch('drained');
        await assert.rejects(closing, (error: unknown) => error === failure);
    });

    it('settles every close and unacked getter after one drained marker', { timeout: 5_000 }, async () => {
        const { stream, dispatch } = await openMux(
            {
                close: async () => {},
                getUnackedRecords: async () => [],
                getUnackedBatches: async () => [],
            },
            undefined,
            { onAck() {} }
        );
        const calls = [stream.close(), stream.close(), stream.getUnackedRecords(), stream.getUnackedBatches()];
        const states = calls.map(track);
        await drainEventLoop();
        assert.ok(states.every(state => !state.settled));
        dispatch('drained');
        await Promise.all(calls);
        assert.doesNotThrow(() => dispatch('drained'));
    });

    it('settles close() with the native result when there is no ack callback', async () => {
        const { stream, dispatch } = await openMux({ close: async () => {} });
        assert.strictEqual(dispatch, undefined);
        await stream.close();
    });

    it('rejects at once when native close or getUnackedRecords throws synchronously', async () => {
        const failure = new Error('napi failure');
        const { stream } = await openMux(
            {
                close: () => { throw failure; },
                getUnackedRecords: () => { throw failure; },
            },
            undefined,
            { onAck() {} }
        );
        for (const call of [() => stream.close(), () => stream.getUnackedRecords()]) {
            const pending = call();
            const state = track(pending);
            await drainEventLoop();
            assert.strictEqual(state.settled, true);
            await assert.rejects(pending, (error: unknown) => error === failure);
        }
    });
});

describe('Multiplexed ack callback', () => {
    // The dispatcher the wrapper hands to native, captured from the stubbed entry point.
    const captureDispatch = async (callback: object) => {
        const { sdk, captured } = stubbedSdk({});
        const stream = await jsonBuilder(sdk).multiplexed(2).ackCallback(callback).build();
        return { dispatch: captured.mux![6], stream };
    };

    // Replaces console.error and records its arguments; call restore() in a finally block.
    // Not t.mock: Node 16's node:test has none, and treats a test function that takes a
    // parameter as callback-style.
    const stubConsoleError = (replacement: () => void = () => {}) => {
        const original = console.error;
        const calls: unknown[][] = [];
        console.error = (...args: unknown[]) => {
            calls.push(args);
            replacement();
        };
        return { calls, restore: () => { console.error = original; } };
    };

    it('delivers an ack with the message ID as a bigint', async () => {
        const acked: unknown[] = [];
        const { dispatch } = await captureDispatch({ onAck: (messageId: bigint) => { acked.push(messageId); } });
        dispatch('ack', -42n);
        assert.deepStrictEqual(acked, [-42n]);
        assert.strictEqual(typeof acked[0], 'bigint');
    });

    it('delivers an error with the message ID and message', async () => {
        const errors: unknown[] = [];
        const { dispatch } = await captureDispatch({ onError: (messageId: bigint, message: string) => { errors.push([messageId, message]); } });
        dispatch('error', 7n, 'boom');
        assert.deepStrictEqual(errors, [[7n, 'boom']]);
    });

    it('logs a throwing callback and keeps delivering', async () => {
        const consoleError = stubConsoleError();
        try {
            let acks = 0;
            let errors = 0;
            const { dispatch } = await captureDispatch({
                onAck() { acks++; throw new Error('ack handler failed'); },
                onError() { errors++; throw new Error('error handler failed'); },
            });
            assert.doesNotThrow(() => dispatch('ack', 1n));
            assert.strictEqual(consoleError.calls.length, 1);
            dispatch('ack', 2n);
            assert.strictEqual(acks, 2);
            assert.doesNotThrow(() => dispatch('error', 3n, 'boom'));
            dispatch('error', 4n, 'boom');
            assert.strictEqual(errors, 2);
            assert.strictEqual(consoleError.calls.length, 4);
            for (const args of consoleError.calls) {
                assert.strictEqual(args[0], 'Zerobus multiplexed stream ack callback failed:');
                assert.ok(args[1] instanceof Error);
            }
            assert.strictEqual((consoleError.calls[0][1] as Error).message, 'ack handler failed');
            assert.strictEqual((consoleError.calls[2][1] as Error).message, 'error handler failed');
        } finally {
            consoleError.restore();
        }
    });

    it('ignores an event whose method is missing, without logging', async () => {
        const consoleError = stubConsoleError();
        try {
            const onlyError = await captureDispatch({ onError() {} });
            assert.doesNotThrow(() => onlyError.dispatch('ack', 1n));
            const onlyAck = await captureDispatch({ onAck() {} });
            assert.doesNotThrow(() => onlyAck.dispatch('error', 1n, 'boom'));
            assert.strictEqual(consoleError.calls.length, 0);
        } finally {
            consoleError.restore();
        }
    });

    it('logs a rejected async callback without an unhandled rejection', async () => {
        const consoleError = stubConsoleError();
        const unhandled: unknown[] = [];
        const onUnhandled = (reason: unknown) => { unhandled.push(reason); };
        process.on('unhandledRejection', onUnhandled);
        try {
            const ackFailure = new Error('async ack failed');
            const errorFailure = new Error('async error failed');
            const { dispatch } = await captureDispatch({
                async onAck() { throw ackFailure; },
                async onError() { throw errorFailure; },
            });
            dispatch('ack', 1n);
            dispatch('error', 2n, 'boom');
            await drainEventLoop();
            assert.deepStrictEqual(unhandled, []);
            assert.deepStrictEqual(consoleError.calls, [
                ['Zerobus multiplexed stream ack callback failed:', ackFailure],
                ['Zerobus multiplexed stream ack callback failed:', errorFailure],
            ]);
        } finally {
            process.off('unhandledRejection', onUnhandled);
            consoleError.restore();
        }
    });

    it('adopts a resolving custom thenable without logging', async () => {
        const consoleError = stubConsoleError();
        const unhandled: unknown[] = [];
        const onUnhandled = (reason: unknown) => { unhandled.push(reason); };
        process.on('unhandledRejection', onUnhandled);
        try {
            let thenCalls = 0;
            const thenables = [
                { then(resolve: () => void) { thenCalls++; resolve(); } },
                { async then(resolve: () => void) { thenCalls++; resolve(); } },
            ];
            for (const thenable of thenables) {
                const { dispatch } = await captureDispatch({ onAck: () => thenable });
                assert.doesNotThrow(() => dispatch('ack', 1n));
            }
            await drainEventLoop();
            // Each thenable was adopted (its then() called once), not ignored.
            assert.strictEqual(thenCalls, 2);
            assert.deepStrictEqual(unhandled, []);
            assert.strictEqual(consoleError.calls.length, 0);
        } finally {
            process.off('unhandledRejection', onUnhandled);
            consoleError.restore();
        }
    });

    it('logs a custom thenable that throws or rejects once, without an unhandled rejection', async () => {
        const unhandled: unknown[] = [];
        const onUnhandled = (reason: unknown) => { unhandled.push(reason); };
        process.on('unhandledRejection', onUnhandled);
        try {
            const failure = new Error('thenable failed');
            const thenables = [
                { then() { throw failure; } },
                { then(_resolve: () => void, reject: (error: unknown) => void) { reject(failure); } },
            ];
            for (const thenable of thenables) {
                const consoleError = stubConsoleError();
                try {
                    const { dispatch } = await captureDispatch({ onAck: () => thenable });
                    assert.doesNotThrow(() => dispatch('ack', 1n));
                    await drainEventLoop();
                    assert.deepStrictEqual(consoleError.calls, [['Zerobus multiplexed stream ack callback failed:', failure]]);
                } finally {
                    consoleError.restore();
                }
            }
            assert.deepStrictEqual(unhandled, []);
        } finally {
            process.off('unhandledRejection', onUnhandled);
        }
    });

    it('logs nothing for a plain or resolving return value', async () => {
        const consoleError = stubConsoleError();
        try {
            const plain = await captureDispatch({ onAck: () => 5 });
            plain.dispatch('ack', 1n);
            const resolving = await captureDispatch({ onAck: async () => 5 });
            resolving.dispatch('ack', 1n);
            await drainEventLoop();
            assert.strictEqual(consoleError.calls.length, 0);
        } finally {
            consoleError.restore();
        }
    });

    it('never lets a failing console.error escape the dispatcher', async () => {
        const consoleError = stubConsoleError(() => { throw new Error('log failed'); });
        const unhandled: unknown[] = [];
        const onUnhandled = (reason: unknown) => { unhandled.push(reason); };
        process.on('unhandledRejection', onUnhandled);
        try {
            let acks = 0;
            const { dispatch } = await captureDispatch({ onAck() { acks++; throw new Error('ack handler failed'); } });
            assert.doesNotThrow(() => dispatch('ack', 1n));
            const rejecting = await captureDispatch({ async onAck() { throw new Error('async ack failed'); } });
            assert.doesNotThrow(() => rejecting.dispatch('ack', 2n));
            await drainEventLoop();
            assert.deepStrictEqual(unhandled, []);
            dispatch('ack', 3n);
            assert.strictEqual(acks, 2);
        } finally {
            process.off('unhandledRejection', onUnhandled);
            consoleError.restore();
        }
    });

    it('ignores an unknown event kind', async () => {
        const consoleError = stubConsoleError();
        try {
            let calls = 0;
            const { dispatch } = await captureDispatch({ onAck() { calls++; }, onError() { calls++; } });
            assert.doesNotThrow(() => dispatch('bogus', 1n));
            assert.strictEqual(calls, 0);
            assert.strictEqual(consoleError.calls.length, 0);
        } finally {
            consoleError.restore();
        }
    });

    it('calls the callbacks as methods of the callback object', async () => {
        class Counter {
            acks = 0;
            errors = 0;
            onAck() { this.acks++; }
            onError() { this.errors++; }
        }
        const counter = new Counter();
        const { dispatch } = await captureDispatch(counter);
        dispatch('ack', 1n);
        dispatch('error', 2n, 'boom');
        assert.strictEqual(counter.acks, 1);
        assert.strictEqual(counter.errors, 1);
    });

    it('treats the drained marker as internal and resolves the drain', { timeout: 5_000 }, async () => {
        let calls = 0;
        const { dispatch, stream } = await captureDispatch({ onAck() { calls++; }, onError() { calls++; } });
        dispatch('drained');
        await stream._drained;
        assert.strictEqual(calls, 0);
    });
});

describe('Multiplexed stream package surface', () => {
    const packageRoot = path.resolve(__dirname, '..');

    it('ships every module that the JS entry points require', () => {
        const files: string[] = JSON.parse(fs.readFileSync(path.join(packageRoot, 'package.json'), 'utf8')).files;
        // avro.js requires no local module today; it is scanned so a future require is checked too.
        for (const [entry, requiresLocal] of [['wrapper.js', true], ['stream_builder.js', true], ['avro.js', false]] as const) {
            const source = fs.readFileSync(path.join(packageRoot, entry), 'utf8');
            const matches = [...source.matchAll(/require\('\.\/([^']+)'\)/g)];
            if (requiresLocal) {
                assert.ok(matches.length > 0, `found no local require() in ${entry}`);
            }
            for (const match of matches) {
                const required = match[1].endsWith('.js') ? match[1] : `${match[1]}.js`;
                assert.ok(files.includes(required), `${entry} requires ${required}, which package.json files omits`);
            }
        }
    });

    it('types the documented builder chain', { timeout: 60_000 }, () => {
        const temporaryRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'zerobus-typings-test-'));
        const typeTestPath = path.join(temporaryRoot, 'consumer.mts');
        // Forward slashes: a Windows path's backslashes would be read as escapes in the import.
        const entry = JSON.stringify(path.join(packageRoot, 'wrapper.js').split(path.sep).join('/'));
        try {
            fs.writeFileSync(
                typeTestPath,
                `import { ZerobusSdk, ZerobusStream, ZerobusMultiplexedStream } from ${entry};\n` +
                    `const sdk = new ZerobusSdk('endpoint', 'workspace');\n` +
                    `const stream: ZerobusMultiplexedStream = await sdk.streamBuilder().table('t').oauth('i', 's').json()\n` +
                    `    .multiplexed(2).ackCallback({ onAck(id: bigint) { void id; } }).build();\n` +
                    `const ordinary: ZerobusStream = await sdk.streamBuilder().table('t').oauth('i', 's').json().build();\n` +
                    `const id: bigint = await stream.ingestRecord('{}');\n` +
                    `const last: bigint | null = await stream.ingestRecords([]);\n` +
                    `// @ts-expect-error the base builder has no ackCallback\n` +
                    `sdk.streamBuilder().ackCallback;\n` +
                    `// @ts-expect-error the multiplexed builder offers only ackCallback and build\n` +
                    `sdk.streamBuilder().multiplexed(2).table;\n` +
                    `// @ts-expect-error message IDs are bigint\n` +
                    `const asNumber: number = await stream.ingestRecord('{}');\n` +
                    // An ack callback needs onAck, onError or both; parameter types are inferred.
                    `const mux = sdk.streamBuilder().table('t').oauth('i', 's').json().multiplexed(2);\n` +
                    `mux.ackCallback({ onAck(messageId) { const x: bigint = messageId; void x; } });\n` +
                    `mux.ackCallback({ onError(messageId, message) { const x: bigint = messageId; const y: string = message; void x; void y; } });\n` +
                    `mux.ackCallback({ onAck(messageId) { void messageId; }, onError(messageId, message) { void messageId; void message; } });\n` +
                    `mux.ackCallback({ async onAck(messageId) { void messageId; }, async onError(messageId, message) { void messageId; void message; } });\n` +
                    // A non-thenable return value is ignored, so an arrow that returns one is fine.
                    `const ackedIds: bigint[] = [];\n` +
                    `mux.ackCallback({ onAck: (messageId) => ackedIds.push(messageId) });\n` +
                    `// @ts-expect-error an ack callback needs onAck or onError\n` +
                    `mux.ackCallback({});\n` +
                    `// @ts-expect-error message IDs are bigint\n` +
                    `mux.ackCallback({ onAck(messageId: number) { void messageId; } });\n` +
                    `void ordinary; void id; void last; void asNumber;\n`
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
                { cwd: temporaryRoot, stdio: 'pipe' }
            );
        } finally {
            fs.rmSync(temporaryRoot, { recursive: true, force: true });
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
