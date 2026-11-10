/* tslint:disable */
/* eslint-disable */
'use strict';

// Internal stream builder for the Zerobus TypeScript SDK. Not part of the public API:
// users get a builder from `sdk.streamBuilder()` (wrapper.js), and tests require it directly.
// It mirrors the Rust SDK's `stream_builder()`: collect the table, authentication, record
// format and stream options, then open a stream with `build()`.

const { RecordType } = require('./index.js');

const MAX_UINT32 = 4294967295;
const MAX_MULTIPLEXED_STREAMS = 64;

function checkUint32(name, value, minimum) {
  const min = minimum === undefined ? 0 : minimum;
  if (typeof value !== 'number' || !Number.isInteger(value) || value < min || value > MAX_UINT32) {
    throw new RangeError(`${name} must be an integer between ${min} and ${MAX_UINT32}`);
  }
  return value;
}

function checkNonEmptyString(name, value) {
  if (typeof value !== 'string' || value.length === 0) {
    throw new TypeError(`${name} must be a non-empty string`);
  }
  return value;
}

class StreamBuilder {
  constructor(sdk) {
    this._sdk = sdk;
    this._tableName = null;
    this._clientId = null;
    this._clientSecret = null;
    this._headersProvider = null;
    // { recordType, descriptorProto?, avroSchema? }; the last format setter wins.
    this._format = null;
    // StreamConfigurationOptions without recordType; an unset key keeps the core default.
    this._options = {};
  }

  table(tableName) {
    this._tableName = checkNonEmptyString('tableName', tableName);
    return this;
  }

  // The last authentication setter wins.
  oauth(clientId, clientSecret) {
    // Check both before assigning, so a throw leaves the builder unchanged.
    checkNonEmptyString('clientId', clientId);
    checkNonEmptyString('clientSecret', clientSecret);
    this._clientId = clientId;
    this._clientSecret = clientSecret;
    this._headersProvider = null;
    return this;
  }

  headersProvider(provider) {
    if (provider === null || typeof provider !== 'object' ||
        typeof provider.getHeadersCallback !== 'function') {
      throw new TypeError('headersProvider expects an object with a getHeadersCallback function');
    }
    this._headersProvider = provider;
    this._clientId = null;
    this._clientSecret = null;
    return this;
  }

  json() {
    this._format = { recordType: RecordType.Json };
    return this;
  }

  compiledProto(descriptorBase64) {
    this._format = {
      recordType: RecordType.Proto,
      descriptorProto: checkNonEmptyString('descriptorBase64', descriptorBase64),
    };
    return this;
  }

  avro(schemaJson) {
    this._format = {
      recordType: RecordType.Avro,
      avroSchema: checkNonEmptyString('schemaJson', schemaJson),
    };
    return this;
  }

  // At least 1: with 0 the core would block every ingest.
  maxInflightRequests(n) { return this._setUint32('maxInflightRequests', n, 1); }
  recoveryTimeoutMs(ms) { return this._setUint32('recoveryTimeoutMs', ms); }
  recoveryBackoffMs(ms) { return this._setUint32('recoveryBackoffMs', ms); }
  recoveryRetries(n) { return this._setUint32('recoveryRetries', n); }
  flushTimeoutMs(ms) { return this._setUint32('flushTimeoutMs', ms); }
  serverLackOfAckTimeoutMs(ms) { return this._setUint32('serverLackOfAckTimeoutMs', ms); }

  recovery(enabled) {
    if (typeof enabled !== 'boolean') {
      throw new TypeError('recovery must be a boolean');
    }
    this._options.recovery = enabled;
    return this;
  }

  // `null` or `undefined` waits the full server-specified duration (the default).
  streamPausedMaxWaitTimeMs(ms) {
    if (ms === null || ms === undefined) {
      delete this._options.streamPausedMaxWaitTimeMs;
      return this;
    }
    return this._setUint32('streamPausedMaxWaitTimeMs', ms);
  }

  // Terminal: snapshots the settings as they are now and returns a builder that only offers
  // ackCallback() and build(). Beta.
  multiplexed(streamCount) {
    if (typeof streamCount !== 'number' || !Number.isInteger(streamCount) ||
        streamCount < 1 || streamCount > MAX_MULTIPLEXED_STREAMS) {
      throw new RangeError(
        `streamCount must be an integer between 1 and ${MAX_MULTIPLEXED_STREAMS}`
      );
    }
    return new MultiplexedStreamBuilder(this._snapshot(), streamCount);
  }

  async build() {
    const args = this._nativeArgs();
    return this._sdk.createStream(
      args.tableProperties, args.clientId, args.clientSecret, args.options, args.headersProvider
    );
  }

  _snapshot() {
    const copy = Object.assign(new StreamBuilder(this._sdk), this);
    copy._options = Object.assign({}, this._options);
    return copy;
  }

  _setUint32(name, value, minimum) {
    this._options[name] = checkUint32(name, value, minimum);
    return this;
  }

  // Validates the collected settings and returns createStream's arguments.
  _nativeArgs() {
    if (this._tableName === null) {
      throw new Error('table is required: call table(...) before build()');
    }
    if (this._clientId === null && this._headersProvider === null) {
      throw new Error('authentication is required: call oauth(...) or headersProvider(...)');
    }
    if (this._format === null) {
      throw new Error('record format is required: call json(), compiledProto(...) or avro(...)');
    }
    const tableProperties = { tableName: this._tableName };
    if (this._format.descriptorProto !== undefined) {
      tableProperties.descriptorProto = this._format.descriptorProto;
    }
    if (this._format.avroSchema !== undefined) {
      tableProperties.avroSchema = this._format.avroSchema;
    }
    return {
      tableProperties,
      // createStream ignores the OAuth credentials when a headers provider is set.
      clientId: this._clientId === null ? '' : this._clientId,
      clientSecret: this._clientSecret === null ? '' : this._clientSecret,
      options: Object.assign({}, this._options, { recordType: this._format.recordType }),
      headersProvider: this._headersProvider === null ? undefined : this._headersProvider,
    };
  }
}

class MultiplexedStreamBuilder {
  constructor(base, streamCount) {
    this._base = base;
    this._streamCount = streamCount;
    this._ackCallback = null;
  }

  // One callback object shared by every sub-stream: set `onAck(messageId)`,
  // `onError(messageId, message)` or both. Calls arrive in no global order.
  ackCallback(callback) {
    if (callback === null || typeof callback !== 'object' ||
        (typeof callback.onAck !== 'function' && typeof callback.onError !== 'function')) {
      throw new TypeError('ackCallback expects an object with an onAck and/or onError method');
    }
    this._ackCallback = callback;
    return this;
  }

  async build() {
    return this._base._sdk._openMultiplexedStream(
      this._base._nativeArgs(), this._streamCount, this._ackCallback
    );
  }
}

function logCallbackError(error) {
  try {
    console.error('Zerobus multiplexed stream ack callback failed:', error);
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
        Promise.resolve(result).then(undefined, logCallbackError);
      }
    } catch (error) {
      logCallbackError(error);
    }
  }
  return { dispatch, drained };
}

module.exports = { StreamBuilder, MultiplexedStreamBuilder, makeAckDispatcher };
