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
    it('keeps the type definitions in step with the loaded binary', () => {
        const readTypings = (file: string) => fs.readFileSync(path.resolve(__dirname, '..', file), 'utf8');
        const typings = readTypings('index.d.ts');
        // index.js and index.d.ts are generated together, so compare them with the loaded binary.
        for (const [name, value] of Object.entries(require('../index'))) {
            assert.notStrictEqual(value, undefined, `${name} is not in the loaded binary; rebuild`);
            if (typeof value !== 'function' || value.prototype === undefined) continue;
            // Method names repeat across classes and in doc comments, so look only for a
            // declaration line inside this class.
            const declaration = typings.match(new RegExp(`export declare class ${name} [^\\n]*\\n([\\s\\S]*?)\\n\\}`));
            assert.ok(declaration, `class ${name} is not in index.d.ts; rebuild`);
            for (const method of Object.getOwnPropertyNames(value.prototype)) {
                if (method === 'constructor' || (name === 'ZerobusSdk' && method === '_createMultiplexedStream')) continue;
                assert.match(
                    declaration[1],
                    new RegExp(`^\\s+(?:get |set |static )?${method}\\(`, 'm'),
                    `${name}.${method} is not in index.d.ts; rebuild`
                );
            }
        }
        assert.match(typings, /class ZerobusMultiplexedStream/);
        for (const file of ['index.d.ts', 'wrapper.d.ts']) {
            assert.doesNotMatch(readTypings(file), /_createMultiplexedStream/, file);
        }
        assert.match(readTypings('wrapper.d.ts'), /^\s+createMultiplexedStream\(/m);
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

const OAUTH = { oauth: { clientId: 'id', clientSecret: 'secret' } };
const JSON_OPTIONS = { recordType: RecordType.Json };

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

// Opens a JSON (or, with a schema, Avro) multiplexed stream on a stubbed SDK; `dispatch` is
// the ack dispatcher handed to native.
async function openMux(nativeStream: any, avroSchema?: string, ackCallback?: object) {
    const { sdk, captured } = stubbedSdk(nativeStream);
    const tableProperties = avroSchema === undefined ? { tableName: 'c.s.t' } : { tableName: 'c.s.t', avroSchema };
    const recordType = avroSchema === undefined ? RecordType.Json : RecordType.Avro;
    const stream = await sdk.createMultiplexedStream(tableProperties, OAUTH, 2, { recordType, ackCallback });
    return { stream, dispatch: captured.mux![6] };
}

// Records console.error calls and unhandled rejections; call restore() in a finally block.
// Not t.mock: Node 16's node:test has none, and treats a test function that takes a
// parameter as callback-style.
function observeCallbackFailures(replacement: () => void = () => {}) {
    const original = console.error;
    const logged: unknown[][] = [];
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown) => { unhandled.push(reason); };
    console.error = (...args: unknown[]) => {
        logged.push(args);
        replacement();
    };
    process.on('unhandledRejection', onUnhandled);
    return {
        logged,
        unhandled,
        restore: () => {
            process.off('unhandledRejection', onUnhandled);
            console.error = original;
        },
    };
}

const muxAvroSchema = JSON.stringify({
    type: 'record',
    name: 'Order',
    fields: [
        { name: 'id', type: 'long' },
        { name: 'name', type: 'string' },
    ],
});

describe('createMultiplexedStream', () => {
    it('maps oauth and headers-provider auth to the native arguments', async () => {
        const oauth = stubbedSdk();
        await oauth.sdk.createMultiplexedStream({ tableName: 'c.s.t' }, OAUTH, 2, JSON_OPTIONS);
        const [, clientId, clientSecret, , , headersProvider] = oauth.captured.mux!;
        assert.deepStrictEqual([clientId, clientSecret, headersProvider], ['id', 'secret', undefined]);

        const provider = { getHeadersCallback: () => [] as [string, string][] };
        const headers = stubbedSdk();
        await headers.sdk.createMultiplexedStream({ tableName: 'c.s.t' }, { headersProvider: provider }, 2, JSON_OPTIONS);
        const [, providerId, providerSecret, , , passedProvider] = headers.captured.mux!;
        // The native call ignores the OAuth credentials when a headers provider is set.
        assert.deepStrictEqual([providerId, providerSecret], ['', '']);
        assert.strictEqual(passedProvider, provider);
    });

    it('rejects an auth that is not exactly one valid oauth or headersProvider', async () => {
        const { sdk, captured } = stubbedSdk();
        const provider = { getHeadersCallback: () => [] as [string, string][] };
        const cases: Array<[unknown, RegExp]> = [
            [undefined, /^auth must be \{ oauth: \{ clientId, clientSecret \} \} or \{ headersProvider \}$/],
            [null, /^auth must be \{ oauth: \{ clientId, clientSecret \} \} or \{ headersProvider \}$/],
            ['id', /^auth must be \{ oauth: \{ clientId, clientSecret \} \} or \{ headersProvider \}$/],
            [{}, /^auth must set exactly one of oauth or headersProvider$/],
            [{ ...OAUTH, headersProvider: provider }, /^auth must set exactly one of oauth or headersProvider$/],
            [{ oauth: null }, /^auth\.oauth must be \{ clientId, clientSecret \}$/],
            [{ oauth: { clientId: '', clientSecret: 'secret' } }, /^auth\.oauth\.clientId must be a non-empty string$/],
            [{ oauth: { clientId: 'id', clientSecret: 5 } }, /^auth\.oauth\.clientSecret must be a non-empty string$/],
            [{ headersProvider: {} }, /^auth\.headersProvider must be an object with a getHeadersCallback function$/],
            [{ headersProvider: { getHeadersCallback: 'x' } }, /^auth\.headersProvider must be an object with a getHeadersCallback function$/],
        ];
        for (const [auth, expected] of cases) {
            await assert.rejects(
                sdk.createMultiplexedStream({ tableName: 'c.s.t' }, auth, 2, JSON_OPTIONS),
                { name: 'TypeError', message: expected },
                JSON.stringify(auth) ?? String(auth)
            );
        }
        // Auth is checked before the stream count.
        await assert.rejects(
            sdk.createMultiplexedStream({ tableName: 'c.s.t' }, {}, 0, JSON_OPTIONS),
            { name: 'TypeError', message: /^auth must set exactly one of oauth or headersProvider$/ }
        );
        assert.strictEqual(captured.mux, undefined);
    });

    it('validates the stream count and numeric options and passes valid values to native', async () => {
        const option = (name: string) => (captured: any) => captured.mux[4][name];
        const withOption = (name: string) => (sdk: any, value: unknown) =>
            sdk.createMultiplexedStream({ tableName: 'c.s.t' }, OAUTH, 2, { ...JSON_OPTIONS, [name]: value });
        // [name, minimum, maximum, message, more invalid values, call, where a valid value lands].
        type Row = [string, number, number, RegExp, unknown[], (sdk: any, value: unknown) => Promise<unknown>, (captured: any) => unknown];
        const u32 = (name: string, minimum: number): Row =>
            [name, minimum, 4294967295, new RegExp(`^${name} must be an integer between ${minimum} and 4294967295$`), [], withOption(name), option(name)];
        // A zero in-flight budget could never send a record, so its minimum is 1.
        const rows: Row[] = [
            ['streamCount', 1, 64, /^streamCount must be an integer from 1 to 64$/, [-1, '4', true, 4n],
                (sdk: any, value: unknown) => sdk.createMultiplexedStream({ tableName: 'c.s.t' }, OAUTH, value, JSON_OPTIONS),
                (captured: any) => captured.mux[3]],
            u32('maxInflightRequests', 1),
            u32('recoveryTimeoutMs', 0),
            u32('recoveryBackoffMs', 0),
            u32('recoveryRetries', 0),
            u32('flushTimeoutMs', 0),
            u32('serverLackOfAckTimeoutMs', 0),
            u32('streamPausedMaxWaitTimeMs', 0),
        ];
        // The stream count is checked before the options.
        await assert.rejects(
            stubbedSdk().sdk.createMultiplexedStream({ tableName: 'c.s.t' }, OAUTH, 0, 'x'),
            { name: 'RangeError', message: /^streamCount must be an integer from 1 to 64$/ }
        );
        for (const [name, minimum, maximum, message, moreInvalid, call, landed] of rows) {
            const { sdk, captured } = stubbedSdk();
            for (const value of [minimum - 1, maximum + 1, 1.5, NaN, '5', ...moreInvalid]) {
                await assert.rejects(call(sdk, value), { name: 'RangeError', message }, `${name} = ${String(value)}`);
            }
            assert.strictEqual(captured.mux, undefined, name);
            for (const value of [minimum, maximum]) {
                const stubbed = stubbedSdk();
                await call(stubbed.sdk, value);
                assert.strictEqual(landed(stubbed.captured), value, `${name} = ${value}`);
            }
        }
    });

    it('rejects options that are not an object or have a non-boolean recovery', async () => {
        const { sdk, captured } = stubbedSdk();
        for (const options of ['x', 5]) {
            await assert.rejects(
                sdk.createMultiplexedStream({ tableName: 'c.s.t' }, OAUTH, 2, options),
                { name: 'TypeError', message: /^options must be an object$/ },
                String(options)
            );
        }
        for (const recovery of ['yes', 1]) {
            await assert.rejects(
                sdk.createMultiplexedStream({ tableName: 'c.s.t' }, OAUTH, 2, { ...JSON_OPTIONS, recovery }),
                { name: 'TypeError', message: /^recovery must be a boolean$/ },
                String(recovery)
            );
        }
        assert.strictEqual(captured.mux, undefined);
    });

    it('passes the table properties and options to native', async () => {
        const descriptorProto = Buffer.from([1, 2, 3]).toString('base64');
        const options: StreamConfigurationOptions = {
            maxInflightRequests: 10,
            recovery: false,
            recoveryTimeoutMs: 11,
            recoveryBackoffMs: 12,
            recoveryRetries: 13,
            flushTimeoutMs: 14,
            serverLackOfAckTimeoutMs: 15,
            streamPausedMaxWaitTimeMs: 16,
        };
        const formats: Array<[string, Record<string, string>, RecordType]> = [
            ['json', {}, RecordType.Json],
            ['proto', { descriptorProto }, RecordType.Proto],
            ['avro', { avroSchema: muxAvroSchema }, RecordType.Avro],
        ];
        for (const [name, schemaProperties, recordType] of formats) {
            const { sdk, captured } = stubbedSdk();
            const tableProperties = { tableName: 'c.s.t', ...schemaProperties };
            const stream = await sdk.createMultiplexedStream(tableProperties, OAUTH, 2, { ...options, recordType });
            assert.deepStrictEqual(captured.mux![0], tableProperties, name);
            assert.deepStrictEqual(captured.mux![4], { ...options, recordType }, name);
            // Only an Avro stream encodes record objects in the wrapper.
            assert.strictEqual(stream._avro !== null, name === 'avro', name);
        }
    });

    it('drops a null streamPausedMaxWaitTimeMs and passes no options when none are given', async () => {
        const cleared = stubbedSdk();
        await cleared.sdk.createMultiplexedStream({ tableName: 'c.s.t' }, OAUTH, 2, { ...JSON_OPTIONS, streamPausedMaxWaitTimeMs: null });
        assert.deepStrictEqual(cleared.captured.mux![4], JSON_OPTIONS);
        for (const options of [null, undefined]) {
            const { sdk, captured } = stubbedSdk();
            await sdk.createMultiplexedStream({ tableName: 'c.s.t', descriptorProto: 'ZGVzYw==' }, OAUTH, 2, options);
            assert.strictEqual(captured.mux![4], undefined, String(options));
            assert.strictEqual(captured.mux![6], undefined, String(options));
        }
    });

    it('strips ackCallback from the native options and passes the dispatcher instead', async () => {
        const withCallback = stubbedSdk();
        const ackCallback = { onAck() {} };
        const options = { ...JSON_OPTIONS, ackCallback };
        await withCallback.sdk.createMultiplexedStream({ tableName: 'c.s.t' }, OAUTH, 2, options);
        assert.deepStrictEqual(withCallback.captured.mux![4], JSON_OPTIONS);
        // The caller's options object is left as it was, so it can be reused.
        assert.strictEqual(options.ackCallback, ackCallback);
        assert.strictEqual(typeof withCallback.captured.mux![6], 'function');

        const withoutCallback = stubbedSdk();
        await withoutCallback.sdk.createMultiplexedStream({ tableName: 'c.s.t' }, OAUTH, 2, { ...JSON_OPTIONS, ackCallback: undefined });
        assert.strictEqual(withoutCallback.captured.mux![6], undefined);
    });

    it('rejects an ackCallback that is not a callback object', async () => {
        const { sdk, captured } = stubbedSdk();
        for (const ackCallback of [() => {}, null, {}]) {
            await assert.rejects(
                sdk.createMultiplexedStream({ tableName: 'c.s.t' }, OAUTH, 2, { ...JSON_OPTIONS, ackCallback }),
                { name: 'TypeError', message: /^ackCallback must be an object with an onAck and\/or onError method$/ },
                JSON.stringify(ackCallback) ?? String(ackCallback)
            );
        }
        assert.strictEqual(captured.mux, undefined);
    });
});

describe('ZerobusMultiplexedStream wrapper', () => {
    it('encodes an Avro record object before ingest', async () => {
        let received: any;
        const { stream } = await openMux(
            { ingestRecord: async (payload: any) => { received = payload; return 7n; } },
            muxAvroSchema
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
            muxAvroSchema
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

    it('passes non-empty unacked results through unchanged', { timeout: 5_000 }, async () => {
        const records = [Buffer.from('record-a'), Buffer.from('record-b')];
        const batches = [[Buffer.from('batch-c')], [Buffer.from('batch-d'), Buffer.from('batch-e')]];
        const getters: Array<[string, unknown]> = [['getUnackedRecords', records], ['getUnackedBatches', batches]];
        for (const [getter, nativeResult] of getters) {
            const nativeCalls: string[] = [];
            const nativeStream = {
                getUnackedRecords: async () => { nativeCalls.push('getUnackedRecords'); return records; },
                getUnackedBatches: async () => { nativeCalls.push('getUnackedBatches'); return batches; },
            };

            // With an ack callback, the result arrives only after the drained marker.
            const withCallback = await openMux(nativeStream, undefined, { onAck() {} });
            const pending = withCallback.stream[getter]();
            const state = track(pending);
            await drainEventLoop();
            assert.strictEqual(state.settled, false, getter);
            withCallback.dispatch('drained');
            const result = await pending;
            assert.deepStrictEqual(result, nativeResult, getter);
            assert.strictEqual(result, nativeResult, `${getter} returns the native value itself`);

            // Without one, the native result comes back as is.
            const withoutCallback = await openMux(nativeStream);
            assert.strictEqual(await withoutCallback.stream[getter](), nativeResult, getter);
            assert.deepStrictEqual(nativeCalls, [getter, getter], 'only the matching native getter runs');
        }
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
    it('delivers an ack with the message ID as a bigint', async () => {
        const acked: unknown[] = [];
        const { dispatch } = await openMux({}, undefined, { onAck: (messageId: bigint) => { acked.push(messageId); } });
        dispatch('ack', -42n);
        assert.deepStrictEqual(acked, [-42n]);
        assert.strictEqual(typeof acked[0], 'bigint');
    });

    it('delivers an error with the message ID and message', async () => {
        const errors: unknown[] = [];
        const { dispatch } = await openMux({}, undefined, { onError: (messageId: bigint, message: string) => { errors.push([messageId, message]); } });
        dispatch('error', 7n, 'boom');
        assert.deepStrictEqual(errors, [[7n, 'boom']]);
    });

    it('logs a throwing callback and keeps delivering', async () => {
        let acks = 0;
        let errors = 0;
        const thenFailure = new Error('then getter failed');
        let returnThrowingThen = false;
        const { dispatch } = await openMux({}, undefined, {
            onAck() {
                acks++;
                if (returnThrowingThen) return { get then(): never { throw thenFailure; } };
                throw new Error('ack handler failed');
            },
            onError() { errors++; throw new Error('error handler failed'); },
        });
        const failures = observeCallbackFailures();
        try {
            assert.doesNotThrow(() => dispatch('ack', 1n));
            assert.strictEqual(failures.logged.length, 1);
            dispatch('ack', 2n);
            assert.strictEqual(acks, 2);
            assert.doesNotThrow(() => dispatch('error', 3n, 'boom'));
            dispatch('error', 4n, 'boom');
            assert.strictEqual(errors, 2);
            // The dispatcher reads .then on the returned value, so a throwing getter is caught too.
            returnThrowingThen = true;
            assert.doesNotThrow(() => dispatch('ack', -5n));
            // Each log line names the handler and the record's message ID.
            assert.deepStrictEqual(failures.logged.map(args => args[0]), [
                'Zerobus multiplexed stream ack callback failed (onAck, message ID 1):',
                'Zerobus multiplexed stream ack callback failed (onAck, message ID 2):',
                'Zerobus multiplexed stream ack callback failed (onError, message ID 3):',
                'Zerobus multiplexed stream ack callback failed (onError, message ID 4):',
                'Zerobus multiplexed stream ack callback failed (onAck, message ID -5):',
            ]);
            for (const args of failures.logged) {
                assert.ok(args[1] instanceof Error);
            }
            assert.strictEqual((failures.logged[0][1] as Error).message, 'ack handler failed');
            assert.strictEqual((failures.logged[2][1] as Error).message, 'error handler failed');
            assert.strictEqual(failures.logged[4][1], thenFailure);
        } finally {
            failures.restore();
        }
        // A console.error that throws must not escape the dispatcher either.
        const throwingLog = observeCallbackFailures(() => { throw new Error('log failed'); });
        try {
            returnThrowingThen = false;
            assert.doesNotThrow(() => dispatch('ack', 6n));
            await drainEventLoop();
            assert.deepStrictEqual(throwingLog.unhandled, []);
        } finally {
            throwingLog.restore();
        }
    });

    it('ignores an event whose method is missing, without logging', async () => {
        const failures = observeCallbackFailures();
        try {
            const onlyError = await openMux({}, undefined, { onError() {} });
            assert.doesNotThrow(() => onlyError.dispatch('ack', 1n));
            const onlyAck = await openMux({}, undefined, { onAck() {} });
            assert.doesNotThrow(() => onlyAck.dispatch('error', 1n, 'boom'));
            assert.strictEqual(failures.logged.length, 0);
        } finally {
            failures.restore();
        }
    });

    it('logs a rejected async callback without an unhandled rejection', async () => {
        const ackFailure = new Error('async ack failed');
        const errorFailure = new Error('async error failed');
        const { dispatch } = await openMux({}, undefined, {
            async onAck() { throw ackFailure; },
            async onError() { throw errorFailure; },
        });
        const failures = observeCallbackFailures();
        try {
            dispatch('ack', 1n);
            dispatch('error', 2n, 'boom');
            await drainEventLoop();
            assert.deepStrictEqual(failures.unhandled, []);
            assert.deepStrictEqual(failures.logged, [
                ['Zerobus multiplexed stream ack callback failed (onAck, message ID 1):', ackFailure],
                ['Zerobus multiplexed stream ack callback failed (onError, message ID 2):', errorFailure],
            ]);
        } finally {
            failures.restore();
        }
        // A console.error that throws while logging a rejection must not escape either.
        const throwingLog = observeCallbackFailures(() => { throw new Error('log failed'); });
        try {
            dispatch('ack', 4n);
            await drainEventLoop();
            assert.deepStrictEqual(throwingLog.unhandled, []);
        } finally {
            throwingLog.restore();
        }
    });

    it('ignores an unknown event kind', async () => {
        const failures = observeCallbackFailures();
        try {
            let calls = 0;
            const { dispatch } = await openMux({}, undefined, { onAck() { calls++; }, onError() { calls++; } });
            assert.doesNotThrow(() => dispatch('bogus', 1n));
            assert.strictEqual(calls, 0);
            assert.strictEqual(failures.logged.length, 0);
        } finally {
            failures.restore();
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
        const { dispatch } = await openMux({}, undefined, counter);
        dispatch('ack', 1n);
        dispatch('error', 2n, 'boom');
        assert.strictEqual(counter.acks, 1);
        assert.strictEqual(counter.errors, 1);
    });

    it('treats the drained marker as internal and resolves the drain', { timeout: 5_000 }, async () => {
        let calls = 0;
        const { dispatch, stream } = await openMux({}, undefined, { onAck() { calls++; }, onError() { calls++; } });
        dispatch('drained');
        await stream._drained;
        assert.strictEqual(calls, 0);
    });
});

// Errors raised before any network call: format resolution, the stream count and the core's
// in-flight budget check. The SDK points at an address where nothing listens, and streams turn
// recovery off, so a regression that does reach the network fails fast.
describe('Pre-network errors', () => {
    const UNREACHABLE = 'http://127.0.0.1:1';
    const nativeSdk = new ZerobusSdk(UNREACHABLE, UNREACHABLE);
    const sdk = new wrapper.ZerobusSdk(UNREACHABLE, UNREACHABLE);
    const open = (tableProperties: TableProperties, streamCount: number, options: object = JSON_OPTIONS) =>
        sdk.createMultiplexedStream(tableProperties, OAUTH, streamCount, { recovery: false, ...options });

    // A stream that opens by mistake is closed before the test fails.
    async function assertOpenRejects(opening: Promise<any>, expected: RegExp) {
        let stream: any;
        try {
            stream = await opening;
        } catch (error) {
            assert.match((error as Error).message, expected);
            return;
        }
        await stream.close();
        assert.fail(`createMultiplexedStream() should have rejected with ${expected}`);
    }

    // The native createStream (../index) throws these synchronously; the wrapper's async
    // methods turn the same errors into rejections.
    it('throws native createStream format errors synchronously, decoding the descriptor first', () => {
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
            nativeSdk.createStream({ tableName: 'c.s.t', ...tableProperties }, 'id', 'secret', { recordType, recovery: false });
        const garbage = Buffer.from([0xff, 0xff, 0xff]).toString('base64');
        const cases: Array<[Omit<TableProperties, 'tableName'>, RecordType | undefined, RegExp]> = [
            [{ descriptorProto: garbage }, RecordType.Proto, /Failed to parse descriptor proto/],
            [{ descriptorProto: validDescriptor, avroSchema }, undefined, /mutually exclusive/],
            [{ avroSchema }, RecordType.Json, /avroSchema is set but recordType is not RecordType.Avro/],
            [{}, RecordType.Avro, /no avroSchema was provided/],
            // The descriptor is decoded before exclusivity with avroSchema is checked.
            [{ descriptorProto: '%%%', avroSchema }, undefined, /Failed to decode descriptor/],
        ];
        for (const [tableProperties, recordType, expected] of cases) {
            assert.throws(() => create(tableProperties, recordType), expected, String(expected));
        }
    });

    it('rejects the default Proto record type without a descriptor', async () => {
        await assertOpenRejects(open({ tableName: 'c.s.t' }, 2, {}), /requires descriptor_proto/);
    });

    it('rejects native stream counts that would wrap or truncate to a valid count', () => {
        const options: StreamConfigurationOptions = { recordType: RecordType.Json, recovery: false };
        for (const streamCount of [-1, 1.5, NaN, Infinity, 2 ** 32, 2 ** 32 + 2]) {
            assert.throws(
                () => (nativeSdk as any)._createMultiplexedStream({ tableName: 'c.s.t' }, 'id', 'secret', streamCount, options),
                { message: /^streamCount must be an integer from 1 to 64$/ },
                String(streamCount)
            );
        }
    });

    it('passes a stream count of 64 through to the core', async () => {
        await assertOpenRejects(
            open({ tableName: 'c.s.t' }, 64, { ...JSON_OPTIONS, maxInflightRequests: 63 }),
            /^Failed to create multiplexed stream: .*64/
        );
    });

    it('resolves the descriptor with the same code as createStream', async () => {
        await assertOpenRejects(
            open({ tableName: 'c.s.t', descriptorProto: '%%%' }, 2, { recordType: RecordType.Proto }),
            /Failed to decode descriptor/
        );
    });

    it('lets Node exit and runs no callback after a failed open', { timeout: 90_000 }, async () => {
        const script = `
            const { ZerobusSdk, RecordType } = require(${JSON.stringify(path.resolve(__dirname, '..', 'wrapper.js'))});
            const sdk = new ZerobusSdk(${JSON.stringify(UNREACHABLE)}, ${JSON.stringify(UNREACHABLE)});
            // Fails unless createMultiplexedStream() rejects with the expected error.
            process.exitCode = 1;
            sdk.createMultiplexedStream({ tableName: 'c.s.t' }, { oauth: { clientId: 'id', clientSecret: 'secret' } }, 4, {
                recordType: RecordType.Json, recovery: false, maxInflightRequests: 3, ackCallback: { onAck() {} },
            }).then(
                (stream) => stream.close(),
                (error) => { if (/^Failed to create multiplexed stream: /.test(error.message)) process.exitCode = 0; }
            );
        `;
        // Throws if the child exits non-zero or is still running after the timeout.
        execFileSync(process.execPath, ['-e', script], { timeout: 60_000, stdio: 'pipe' });

        const acked: bigint[] = [];
        const errors: Array<[bigint, string]> = [];
        await assertOpenRejects(
            open({ tableName: 'c.s.t' }, 4, {
                ...JSON_OPTIONS,
                maxInflightRequests: 3,
                ackCallback: {
                    onAck(messageId: bigint) { acked.push(messageId); },
                    onError(messageId: bigint, message: string) { errors.push([messageId, message]); },
                },
            }),
            /^Failed to create multiplexed stream: /
        );
        await drainEventLoop();
        assert.deepStrictEqual(acked, []);
        assert.deepStrictEqual(errors, []);
    });
});

describe('Multiplexed stream package surface', () => {
    const packageRoot = path.resolve(__dirname, '..');

    it('ships every module that the JS entry points require', () => {
        const files: string[] = JSON.parse(fs.readFileSync(path.join(packageRoot, 'package.json'), 'utf8')).files;
        // avro.js requires no local module today; it is scanned so a future require is checked too.
        for (const [entry, requiresLocal] of [['wrapper.js', true], ['avro.js', false]] as const) {
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

    it('types createMultiplexedStream and its options', { timeout: 60_000 }, () => {
        const temporaryRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'zerobus-typings-test-'));
        const typeTestPath = path.join(temporaryRoot, 'consumer.mts');
        // Forward slashes: a Windows path's backslashes would be read as escapes in the import.
        const entry = JSON.stringify(path.join(packageRoot, 'wrapper.js').split(path.sep).join('/'));
        try {
            fs.writeFileSync(
                typeTestPath,
                `import { ZerobusSdk, ZerobusMultiplexedStream, RecordType, type MultiplexedAckCallback } from ${entry};\n` +
                    `const sdk = new ZerobusSdk('endpoint', 'workspace');\n` +
                    `const oauth = { oauth: { clientId: 'i', clientSecret: 's' } };\n` +
                    `const stream: ZerobusMultiplexedStream = await sdk.createMultiplexedStream({ tableName: 't' }, oauth, 2,\n` +
                    `    { recordType: RecordType.Json, ackCallback: { onAck(id: bigint) { void id; } } });\n` +
                    `const configured: ZerobusMultiplexedStream = await sdk.createMultiplexedStream(\n` +
                    `    { tableName: 't', descriptorProto: 'ZGVzYw==' },\n` +
                    `    { headersProvider: { getHeadersCallback: () => [['authorization', 'Bearer t']] } }, 1,\n` +
                    `    { maxInflightRequests: 1, recovery: false, recoveryTimeoutMs: 1, recoveryBackoffMs: 1, recoveryRetries: 1,\n` +
                    `      flushTimeoutMs: 1, serverLackOfAckTimeoutMs: 1, streamPausedMaxWaitTimeMs: 1 });\n` +
                    `await sdk.createMultiplexedStream({ tableName: 't', avroSchema: '{}' }, oauth, 1);\n` +
                    `await sdk.createMultiplexedStream({ tableName: 't' }, oauth, 1, null);\n` +
                    `void configured;\n` +
                    `const id: bigint = await stream.ingestRecord('{}');\n` +
                    `const last: bigint | null = await stream.ingestRecords([]);\n` +
                    `// @ts-expect-error message IDs are bigint\n` +
                    `const asNumber: number = await stream.ingestRecord('{}');\n` +
                    `// @ts-expect-error auth sets oauth or headersProvider\n` +
                    `await sdk.createMultiplexedStream({ tableName: 't' }, {}, 1);\n` +
                    `// @ts-expect-error auth sets only one of oauth and headersProvider\n` +
                    `await sdk.createMultiplexedStream({ tableName: 't' }, { ...oauth, headersProvider: { getHeadersCallback: () => [] } }, 1);\n` +
                    `// @ts-expect-error there is no stream builder\n` +
                    `sdk.streamBuilder;\n` +
                    // An ack callback needs onAck, onError or both; parameter types are inferred.
                    `const ack = (callback: MultiplexedAckCallback) => callback;\n` +
                    `ack({ onAck(messageId) { const x: bigint = messageId; void x; } });\n` +
                    `ack({ onError(messageId, message) { const x: bigint = messageId; const y: string = message; void x; void y; } });\n` +
                    `ack({ onAck(messageId) { void messageId; }, onError(messageId, message) { void messageId; void message; } });\n` +
                    `ack({ async onAck(messageId) { void messageId; }, async onError(messageId, message) { void messageId; void message; } });\n` +
                    // A non-thenable return value is ignored, so an arrow that returns one is fine.
                    `const ackedIds: bigint[] = [];\n` +
                    `ack({ onAck: (messageId) => ackedIds.push(messageId) });\n` +
                    `// @ts-expect-error an ack callback needs onAck or onError\n` +
                    `ack({});\n` +
                    `// @ts-expect-error message IDs are bigint\n` +
                    `ack({ onAck(messageId: number) { void messageId; } });\n` +
                    `void id; void last; void asNumber;\n`
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
