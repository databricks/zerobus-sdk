/* tslint:disable */
/* eslint-disable */
'use strict';

// Public entry point: a thin JS wrapper over the native NAPI binding that encodes Avro
// record objects before ingest and forwards everything else unchanged.

const native = require('./index.js');
const { makeEncoder } = require('./avro.js');

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
  // The Avro writer schema comes from tableProperties.avroSchema (the native layer also
  // forwards it to the server); makeEncoder returns null for non-Avro streams.
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

// Explicit named exports: Node's ESM interop finds a CommonJS module's named exports by
// scanning for `module.exports.X = ...`, which it cannot do through `Object.assign`.
module.exports.RecordType = native.RecordType;
module.exports.ZerobusError = native.ZerobusError;
module.exports.IpcCompressionType = native.IpcCompressionType;
module.exports.ArrowDataType = native.ArrowDataType;
module.exports.ZerobusArrowStream = native.ZerobusArrowStream;
module.exports.ZerobusSdk = ZerobusSdk;
module.exports.ZerobusStream = ZerobusStream;
