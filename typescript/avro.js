/* tslint:disable */
/* eslint-disable */
'use strict';

// Internal Avro object encoding for the Zerobus TypeScript SDK. Not part of the public
// API — `wrapper.js` uses it, and tests require it directly. `avsc` is an optional
// dependency, loaded lazily on the first object encode (a bytes-only stream never loads it).

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

// String(number) uses exponent form below 1e-6 and from 1e21; rewrite it as plain digits
// (only moves the decimal point, so no rounding) for the decimal string parser.
function expandExponent(s) {
  const m = /^([+-]?)(\d+)(?:\.(\d+))?e([+-]\d+)$/.exec(s);
  if (!m) return s;
  const digits = m[2] + (m[3] || '');
  const point = m[2].length + Number(m[4]);
  if (point <= 0) return `${m[1]}0.${'0'.repeat(-point)}${digits}`;
  if (point >= digits.length) return m[1] + digits + '0'.repeat(point - digits.length);
  return `${m[1]}${digits.slice(0, point)}.${digits.slice(point)}`;
}

// int64 backed by BigInt. Accepts a BigInt or a safe integer by default; `bigintOnly` drops
// the integer so a long in a union beside another numeric branch is unambiguous. Installed
// for every `long` spelling (field, union branch, logical underlying) via typeHook.
function makeLongType(avsc, bigintOnly) {
  return avsc.types.LongType.__with({
    fromBuffer: (buf) => buf.readBigInt64LE(),
    toBuffer: (n) => {
      const buf = Buffer.alloc(8);
      buf.writeBigInt64LE(typeof n === 'bigint' ? n : BigInt(n));
      return buf;
    },
    fromJSON: (obj) => BigInt(obj),
    toJSON: (n) => Number(n),
    isValid: (n) => (typeof n === 'bigint' || (!bigintOnly && Number.isSafeInteger(n))),
    compare: (a, b) => {
      const x = BigInt(a);
      const y = BigInt(b);
      return x === y ? 0 : x < y ? -1 : 1;
    },
  });
}

// Logical-type handlers accepting convenient JS values. A Date is read via getTime() (UTC),
// so local-timestamp reuses the timestamp conversion (no process-timezone dependency).
function logicalTypes(avsc) {
  const MS_PER_DAY = 86400000;

  class DateType extends avsc.types.LogicalType {
    _toValue(v) {
      // A 'YYYY-MM-DD' string is unambiguous; a Date is read as its UTC calendar day.
      // Reject impossible days (e.g. 2025-02-30) rather than let Date roll them over.
      if (typeof v === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(v)) {
        const d = new Date(`${v}T00:00:00Z`);
        if (isNaN(d.getTime()) || d.toISOString().slice(0, 10) !== v) {
          throw new Error(`invalid date: ${v}`);
        }
        v = d;
      }
      return v instanceof Date ? Math.floor(v.getTime() / MS_PER_DAY) : v;
    }
    _fromValue(v) { return new Date(v * MS_PER_DAY); }
  }
  // A Date converts to a BigInt so micros keeps the full int64 range (its underlying long is
  // BigInt-backed via typeHook); a number or BigInt passes through as the raw unit value.
  class TimestampMillisType extends avsc.types.LogicalType {
    _toValue(v) { return v instanceof Date ? BigInt(v.getTime()) : v; }
    _fromValue(v) { return new Date(Number(v)); }
  }
  class TimestampMicrosType extends avsc.types.LogicalType {
    _toValue(v) { return v instanceof Date ? BigInt(v.getTime()) * 1000n : v; }
    _fromValue(v) { return new Date(Number(BigInt(v) / 1000n)); }
  }

  // decimal: a number, BigInt (whole value, e.g. 5n is 5.00), decimal string, or pre-encoded
  // unscaled bytes. Parsed as a string (no float error); rejects excess scale/precision.
  class DecimalType extends avsc.types.LogicalType {
    constructor(schema, opts) {
      super(schema, opts);
      this._scale = schema.scale || 0;
      this._precision = schema.precision;
      const u = this.underlyingType && this.underlyingType.typeName;
      this._fixedSize = u === 'fixed' ? this.underlyingType.size : null;
    }
    _toValue(dec) {
      if (isBytes(dec)) return Buffer.from(dec);
      const s = typeof dec === 'number' ? expandExponent(String(dec))
        : typeof dec === 'bigint' ? dec.toString() : String(dec);
      const m = /^([+-]?)(\d+)(?:\.(\d+))?$/.exec(s);
      if (!m) throw new Error(`invalid decimal: ${s}`);
      const frac = (m[3] || '').replace(/0+$/, '');
      if (frac.length > this._scale) throw new Error(`decimal ${s} has more than ${this._scale} decimal places`);
      const digits = (m[2] + frac.padEnd(this._scale, '0')).replace(/^0+(?=\d)/, '');
      if (this._precision && digits.length > this._precision) {
        throw new Error(`decimal ${s} exceeds precision ${this._precision}`);
      }
      let unscaled = BigInt(m[1] + digits);
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

  // uuid: a string on a `string`-backed field; a canonical string packs to 16 bytes on fixed(16).
  class UuidType extends avsc.types.LogicalType {
    _toValue(v) {
      if (typeof v === 'string' && this.underlyingType.typeName === 'fixed') {
        const hex = v.replace(/-/g, '');
        // Validate before packing: Buffer.from(hex) silently truncates bad input.
        if (!/^[0-9a-fA-F]{32}$/.test(hex)) throw new Error(`invalid uuid: ${v}`);
        return Buffer.from(hex, 'hex');
      }
      return v;
    }
    _fromValue(v) { return v; }
  }

  return {
    date: DateType,
    'timestamp-millis': TimestampMillisType,
    'timestamp-micros': TimestampMicrosType,
    'local-timestamp-millis': TimestampMillisType,
    'local-timestamp-micros': TimestampMicrosType,
    decimal: DecimalType,
    uuid: UuidType,
  };
}

// Encodes object payloads against the writer schema; a Buffer/Uint8Array passes through.
class AvroEncoder {
  constructor(schemaJson) {
    this._schemaJson = schemaJson;
    this._type = null;
  }
  _avroType() {
    if (this._type === null) {
      const avsc = requireAvsc();
      const longType = makeLongType(avsc);
      const bigintOnlyLong = makeLongType(avsc, true);
      const isLong = (s) => s === 'long' || (s && !Array.isArray(s) && s.type === 'long' && !s.logicalType);
      const isNumeric = (s) => ['int', 'float', 'double'].includes(typeof s === 'string' ? s : s && !s.logicalType && s.type);
      const logicalSeen = new WeakSet();
      this._type = avsc.Type.forSchema(JSON.parse(this._schemaJson), {
        logicalTypes: logicalTypes(avsc),
        typeHook: (schema, opts) => {
          const t = typeof schema === 'string' ? schema : schema && schema.type;
          // avsc calls the hook twice for a logical long: let the LogicalType build first, then
          // give its underlying long a BigInt type so timestamp-micros keeps the full int64 range.
          if (t === 'long' && schema && schema.logicalType) {
            if (logicalSeen.has(schema)) return longType;
            logicalSeen.add(schema);
            return undefined;
          }
          // A long beside another numeric branch takes a BigInt only, else avsc can't pick a branch.
          if (Array.isArray(schema) && schema.some(isLong) && schema.some(isNumeric)) {
            return avsc.Type.forSchema(schema.map((s) => (isLong(s) ? bigintOnlyLong : s)), opts);
          }
          if (isLong(schema)) return longType;
        },
      });
    }
    return this._type;
  }
  encode(payload) {
    if (Buffer.isBuffer(payload)) return payload;
    if (payload instanceof Uint8Array) {
      return Buffer.from(payload.buffer, payload.byteOffset, payload.byteLength);
    }
    return this._avroType().toBuffer(payload);
  }
  encodeBatch(payloads) {
    if (!Array.isArray(payloads)) throw new TypeError('payloads must be an array of records');
    return payloads.map((p) => this.encode(p));
  }
}

function makeEncoder(schemaJson) {
  return schemaJson ? new AvroEncoder(schemaJson) : null;
}

module.exports = { AvroEncoder, makeEncoder };
