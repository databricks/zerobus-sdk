/* tslint:disable */
/* eslint-disable */
'use strict';

// Public entry point: a thin JS wrapper over the native NAPI binding. It encodes Avro
// record objects before ingest, validates createMultiplexedStream() arguments, and makes a
// multiplexed stream's close() and unacked getters wait until its ack callbacks have run.
// Everything else is forwarded unchanged.

const native = require('./index.js');
const { makeEncoder } = require('./avro.js');

const MAX_UINT32 = 4294967295;
const MAX_MULTIPLEXED_STREAMS = 64;
// Options that cross to native u32 fields: napi would wrap or truncate other numbers.
const UINT32_OPTIONS = [
  'recoveryTimeoutMs', 'recoveryBackoffMs', 'recoveryRetries', 'flushTimeoutMs',
  'serverLackOfAckTimeoutMs',
];

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

function checkUint32(name, value, minimum) {
  if (typeof value !== 'number' || !Number.isInteger(value) || value < minimum ||
      value > MAX_UINT32) {
    throw new RangeError(`${name} must be an integer between ${minimum} and ${MAX_UINT32}`);
  }
}

function checkNonEmptyString(name, value) {
  if (typeof value !== 'string' || value.length === 0) {
    throw new TypeError(`${name} must be a non-empty string`);
  }
}

// Returns the native call's clientId, clientSecret and headersProvider for `auth`.
function resolveMultiplexedAuth(auth) {
  if (auth === null || typeof auth !== 'object') {
    throw new TypeError(
      'auth must be { oauth: { clientId, clientSecret } } or { headersProvider }'
    );
  }
  const hasOauth = auth.oauth !== undefined;
  if (hasOauth === (auth.headersProvider !== undefined)) {
    throw new TypeError('auth must set exactly one of oauth or headersProvider');
  }
  if (hasOauth) {
    const oauth = auth.oauth;
    if (oauth === null || typeof oauth !== 'object') {
      throw new TypeError('auth.oauth must be { clientId, clientSecret }');
    }
    checkNonEmptyString('auth.oauth.clientId', oauth.clientId);
    checkNonEmptyString('auth.oauth.clientSecret', oauth.clientSecret);
    return {
      clientId: oauth.clientId, clientSecret: oauth.clientSecret, headersProvider: undefined,
    };
  }
  const provider = auth.headersProvider;
  if (provider === null || typeof provider !== 'object' ||
      typeof provider.getHeadersCallback !== 'function') {
    throw new TypeError(
      'auth.headersProvider must be an object with a getHeadersCallback function'
    );
  }
  // The native call ignores the OAuth credentials when a headers provider is set.
  return { clientId: '', clientSecret: '', headersProvider: provider };
}

// Validates `options` and splits off the ack callback, which the native call takes separately.
function resolveMultiplexedOptions(options) {
  if (options === null || options === undefined) {
    return { nativeOptions: undefined, ackCallback: null };
  }
  if (typeof options !== 'object') {
    throw new TypeError('options must be an object');
  }
  const nativeOptions = Object.assign({}, options);
  delete nativeOptions.ackCallback;
  // At least 1: with 0 the core would block every ingest.
  if (nativeOptions.maxInflightRequests !== undefined) {
    checkUint32('maxInflightRequests', nativeOptions.maxInflightRequests, 1);
  }
  for (const name of UINT32_OPTIONS) {
    if (nativeOptions[name] !== undefined) checkUint32(name, nativeOptions[name], 0);
  }
  // `null` waits the full server-specified duration, the same as leaving it unset.
  if (nativeOptions.streamPausedMaxWaitTimeMs === null) {
    delete nativeOptions.streamPausedMaxWaitTimeMs;
  } else if (nativeOptions.streamPausedMaxWaitTimeMs !== undefined) {
    checkUint32('streamPausedMaxWaitTimeMs', nativeOptions.streamPausedMaxWaitTimeMs, 0);
  }
  if (nativeOptions.recovery !== undefined && typeof nativeOptions.recovery !== 'boolean') {
    throw new TypeError('recovery must be a boolean');
  }
  const callback = options.ackCallback;
  if (callback === undefined) {
    return { nativeOptions, ackCallback: null };
  }
  if (callback === null || typeof callback !== 'object' ||
      (typeof callback.onAck !== 'function' && typeof callback.onError !== 'function')) {
    throw new TypeError('ackCallback must be an object with an onAck and/or onError method');
  }
  return { nativeOptions, ackCallback: callback };
}

function logCallbackError(handler, messageId, error) {
  try {
    console.error(
      `Zerobus multiplexed stream ack callback failed (${handler}, message ID ${messageId}):`, error
    );
  } catch (ignored) {
    // Logging must not throw either: from the dispatcher it would crash Node.
  }
}

// The function the native layer calls for each ack event, as
// `(kind: 'ack' | 'error' | 'drained', messageId?: bigint, message?: string)`. A throw here
// would crash Node, so every user-code path is caught and logged, including a rejected
// promise returned by an async callback. `drained` resolves on the native drained marker,
// which follows the last ack event after a close. Must not capture the stream, so an
// unclosed stream stays collectable.
function makeAckDispatcher(callback) {
  let resolveDrained;
  const drained = new Promise(function (resolve) { resolveDrained = resolve; });
  function dispatch(kind, messageId, message) {
    const handler = kind === 'ack' ? 'onAck' : 'onError';
    try {
      let result;
      if (kind === 'ack') {
        if (typeof callback.onAck !== 'function') return;
        result = callback.onAck(messageId);
      } else if (kind === 'error') {
        if (typeof callback.onError !== 'function') return;
        result = callback.onError(messageId, message);
      } else {
        if (kind === 'drained') resolveDrained();
        return;
      }
      if (result !== null && (typeof result === 'object' || typeof result === 'function') &&
          typeof result.then === 'function') {
        // Promise.resolve calls a thenable's `then` with real resolve/reject functions and
        // turns a throw from it into a rejection, which the handler then logs.
        Promise.resolve(result).then(undefined, function (error) {
          logCallbackError(handler, messageId, error);
        });
      }
    } catch (error) {
      logCallbackError(handler, messageId, error);
    }
  }
  return { dispatch, drained };
}

// Settles like `start()`, but only after the drained marker when an ack callback is set, so
// every callback has run when close() or an unacked getter settles. The native layer queues
// the marker before any of those calls settles; a synchronous throw comes before any close
// started, so it doesn't wait.
function afterDrain(drained, start) {
  let pending;
  try {
    pending = start();
  } catch (error) {
    return Promise.reject(error);
  }
  if (drained === null) return pending;
  return pending.then(
    function (value) { return drained.then(function () { return value; }); },
    function (error) { return drained.then(function () { throw error; }); }
  );
}

// A multiplexed stream (Beta). Like ZerobusStream, encodes Avro record objects before ingest.
class ZerobusMultiplexedStream {
  constructor(nativeStream, avroEncoder, drained) {
    this._native = nativeStream;
    this._avro = avroEncoder || null;
    this._drained = drained || null;
  }
  _encode(payload) {
    return this._avro === null ? payload : this._avro.encode(payload);
  }
  _encodeBatch(payloads) {
    return this._avro === null ? payloads : this._avro.encodeBatch(payloads);
  }
  ingestRecord(payload) { return this._native.ingestRecord(this._encode(payload)); }
  ingestRecords(records) { return this._native.ingestRecords(this._encodeBatch(records)); }
  waitForMessageId(messageId) { return this._native.waitForMessageId(messageId); }
  flush() { return this._native.flush(); }
  get isClosed() { return this._native.isClosed; }
  close() {
    const nativeStream = this._native;
    return afterDrain(this._drained, function () { return nativeStream.close(); });
  }
  getUnackedRecords() {
    const nativeStream = this._native;
    return afterDrain(this._drained, function () { return nativeStream.getUnackedRecords(); });
  }
  getUnackedBatches() {
    const nativeStream = this._native;
    return afterDrain(this._drained, function () { return nativeStream.getUnackedBatches(); });
  }
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
  // Async, so every validation error rejects, as for createStream.
  async createMultiplexedStream(tableProperties, auth, streamCount, options) {
    const nativeAuth = resolveMultiplexedAuth(auth);
    if (typeof streamCount !== 'number' || !Number.isInteger(streamCount) ||
        streamCount < 1 || streamCount > MAX_MULTIPLEXED_STREAMS) {
      throw new RangeError(`streamCount must be an integer from 1 to ${MAX_MULTIPLEXED_STREAMS}`);
    }
    const resolved = resolveMultiplexedOptions(options);
    const encoder = makeEncoder(tableProperties && tableProperties.avroSchema);
    const ack = resolved.ackCallback === null ? null : makeAckDispatcher(resolved.ackCallback);
    const nativeStream = await this._native._createMultiplexedStream(
      tableProperties, nativeAuth.clientId, nativeAuth.clientSecret, streamCount,
      resolved.nativeOptions, nativeAuth.headersProvider, ack === null ? undefined : ack.dispatch
    );
    return new ZerobusMultiplexedStream(nativeStream, encoder, ack === null ? null : ack.drained);
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
module.exports.ZerobusMultiplexedStream = ZerobusMultiplexedStream;
