/* tslint:disable */
/* eslint-disable */
'use strict';

// Public entry point: a thin JS wrapper over the native NAPI binding that encodes
// Avro record objects before ingest and forwards everything else (proto, JSON,
// pre-encoded Buffer, and all other native exports) unchanged.

const native = require('./index.js');

// --- Avro object encoding (internal; `avsc` is an optional dependency) ---

let _avsc = null;
function requireAvsc() {
  if (_avsc === null) {
    try {
      _avsc = require('avsc');
    } catch (e) {
      throw new Error('avsc is required to encode Avro record objects: npm install avsc');
    }
  }
  return _avsc;
}

function isBytes(v) {
  return Buffer.isBuffer(v) || v instanceof Uint8Array;
}

// int64 backed by BigInt: the full range round-trips (a JS number loses precision past
// 2^53) and numeric unions stay unambiguous (number -> int, bigint -> long).
function bigintLongType(avsc) {
  return avsc.types.LongType.__with({
    fromBuffer: (buf) => buf.readBigInt64LE(),
    toBuffer: (n) => {
      const buf = Buffer.alloc(8);
      buf.writeBigInt64LE(n);
      return buf;
    },
    fromJSON: (obj) => BigInt(obj),
    toJSON: (n) => Number(n),
    isValid: (n) => typeof n === 'bigint',
    compare: (a, b) => (a === b ? 0 : a < b ? -1 : 1),
  });
}

// Logical-type handlers accepting convenient JS values (Date, number/bigint, Buffer).
// Timestamps use avsc's number-backed long (millis always safe; micros to ~year 2255).
function logicalTypes(avsc) {
  const MS_PER_DAY = 86400000;

  class DateType extends avsc.types.LogicalType {
    _toValue(v) { return v instanceof Date ? Math.floor(v.getTime() / MS_PER_DAY) : v; }
    _fromValue(v) { return new Date(v * MS_PER_DAY); }
  }
  class TimestampMillisType extends avsc.types.LogicalType {
    _toValue(v) { return v instanceof Date ? v.getTime() : v; }
    _fromValue(v) { return new Date(v); }
  }
  class TimestampMicrosType extends avsc.types.LogicalType {
    _toValue(v) { return v instanceof Date ? v.getTime() * 1000 : v; }
    _fromValue(v) { return new Date(Math.floor(v / 1000)); }
  }
  // decimal: unscaled integer as two's-complement big-endian bytes.
  class DecimalType extends avsc.types.LogicalType {
    constructor(schema, opts) {
      super(schema, opts);
      this._scale = schema.scale || 0;
      const u = this.underlyingType && this.underlyingType.typeName;
      this._fixedSize = u === 'fixed' ? this.underlyingType.size : null;
    }
    _toValue(dec) {
      if (isBytes(dec)) return Buffer.from(dec);
      let unscaled =
        typeof dec === 'bigint'
          ? dec * 10n ** BigInt(this._scale)
          : BigInt(Math.round(Number(dec) * 10 ** this._scale));
      const fits = (nb) => {
        const bits = BigInt(nb * 8 - 1);
        return unscaled >= -(1n << bits) && unscaled <= (1n << bits) - 1n;
      };
      let nbytes = 1;
      while (!fits(nbytes)) nbytes++;
      if (this._fixedSize !== null) {
        if (nbytes > this._fixedSize) throw new Error(`decimal does not fit in fixed(${this._fixedSize})`);
        nbytes = this._fixedSize;
      }
      let v = unscaled < 0n ? (1n << BigInt(nbytes * 8)) + unscaled : unscaled;
      const out = Buffer.alloc(nbytes);
      for (let i = nbytes - 1; i >= 0; i--) {
        out[i] = Number(v & 0xffn);
        v >>= 8n;
      }
      return out;
    }
    _fromValue(buf) { return buf; }
  }
  // uuid is a plain string on the wire; identity handler silences avsc's warning.
  class UuidType extends avsc.types.LogicalType {
    _toValue(v) { return v; }
    _fromValue(v) { return v; }
  }

  return {
    date: DateType,
    'timestamp-millis': TimestampMillisType,
    'timestamp-micros': TimestampMicrosType,
    decimal: DecimalType,
    uuid: UuidType,
  };
}

// Encodes object payloads against the writer schema; a Buffer passes through. The
// schema is parsed lazily, so a bytes-only stream never loads avsc.
class AvroEncoder {
  constructor(schemaJson) {
    this._schemaJson = schemaJson;
    this._type = null;
  }
  _avroType() {
    if (this._type === null) {
      const avsc = requireAvsc();
      this._type = avsc.Type.forSchema(JSON.parse(this._schemaJson), {
        registry: { long: bigintLongType(avsc) },
        logicalTypes: logicalTypes(avsc),
      });
    }
    return this._type;
  }
  encode(payload) {
    return isBytes(payload) ? Buffer.from(payload) : this._avroType().toBuffer(payload);
  }
  encodeBatch(payloads) {
    if (!Array.isArray(payloads)) throw new TypeError('payloads must be an array of records');
    return payloads.map((p) => this.encode(p));
  }
}

function makeEncoder(schemaJson) {
  return schemaJson ? new AvroEncoder(schemaJson) : null;
}

// --- Wrapped SDK / stream ---

class ZerobusStream {
  constructor(nativeStream, avroEncoder) {
    this._native = nativeStream;
    this._avro = avroEncoder || null;
  }
  _encode(payload) {
    return this._avro === null ? payload : this._avro.encode(payload);
  }
  _encodeBatch(payloads) {
    return this._avro === null ? payloads : this._avro.encodeBatch(payloads);
  }
  /** @deprecated Use ingestRecordOffset(). */
  ingestRecord(payload) { return this._native.ingestRecord(this._encode(payload)); }
  /** @deprecated Use ingestRecordsOffset(). */
  ingestRecords(records) { return this._native.ingestRecords(this._encodeBatch(records)); }
  ingestRecordOffset(payload) { return this._native.ingestRecordOffset(this._encode(payload)); }
  ingestRecordsOffset(records) { return this._native.ingestRecordsOffset(this._encodeBatch(records)); }
  waitForOffset(offsetId) { return this._native.waitForOffset(offsetId); }
  flush() { return this._native.flush(); }
  close() { return this._native.close(); }
  getUnackedRecords() { return this._native.getUnackedRecords(); }
  getUnackedBatches() { return this._native.getUnackedBatches(); }
}

class ZerobusSdk {
  constructor(zerobusEndpoint, unityCatalogUrl, options) {
    this._native = new native.ZerobusSdk(zerobusEndpoint, unityCatalogUrl, options);
  }
  // The Avro writer schema comes from tableProperties.avroSchema (the native layer
  // also forwards it to the server); makeEncoder returns null for non-Avro streams.
  async createStream(tableProperties, clientId, clientSecret, options, headersProvider) {
    const encoder = makeEncoder(tableProperties && tableProperties.avroSchema);
    const nativeStream = await this._native.createStream(
      tableProperties, clientId, clientSecret, options, headersProvider
    );
    return new ZerobusStream(nativeStream, encoder);
  }
  async recreateStream(stream) {
    const nativeStream = await this._native.recreateStream(stream._native);
    return new ZerobusStream(nativeStream, stream._avro);
  }
}

// Forward Arrow Flight only when the native binding was built with it, so feature
// detection on the wrapper matches the native surface.
if (typeof native.ZerobusSdk.prototype.createArrowStream === 'function') {
  ZerobusSdk.prototype.createArrowStream = function (tableProperties, clientId, clientSecret, options) {
    return this._native.createArrowStream(tableProperties, clientId, clientSecret, options);
  };
  ZerobusSdk.prototype.recreateArrowStream = function (stream) {
    return this._native.recreateArrowStream(stream);
  };
}

module.exports = Object.assign({}, native, { ZerobusSdk, ZerobusStream });

// Internal, for unit tests only — not part of the public API (absent from wrapper.d.ts).
module.exports._AvroEncoder = AvroEncoder;
