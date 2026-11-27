/* tslint:disable */
/* eslint-disable */

// Public type surface. Re-exports the native binding's types; the wrapped stream and SDK
// classes extend the native ones, so Arrow typings are inherited (and absent in builds
// without Arrow).

export * from './index'

import {
  ZerobusSdk as NativeZerobusSdk,
  ZerobusStream as NativeZerobusStream,
  ZerobusMultiplexedStream as NativeZerobusMultiplexedStream,
  TableProperties,
  StreamConfigurationOptions,
  JsHeadersProvider,
} from './index'

/**
 * A stream for ingesting data into a Databricks Delta table.
 *
 * On an Avro stream (TableProperties.avroSchema set), a record object is encoded to Avro
 * bytes against the writer schema before ingestion; a pre-encoded Buffer and proto/JSON
 * payloads are forwarded unchanged.
 */
export declare class ZerobusStream extends NativeZerobusStream {}

/**
 * Acknowledgment callback of a multiplexed stream (Beta): set `onAck`, `onError` or both.
 *
 * One object is shared by all sub-streams, and calls arrive in no global order. Keep the
 * methods short: they run on the Node.js event loop, once per record or batch. A method that
 * throws, or returns a promise that rejects, is logged with `console.error`; delivery continues.
 * When `close()` or an unacked getter settles, every method call has run; async work a method
 * starts may still be pending. They wait for every callback with no time limit: a callback
 * only queues work for the Node.js event loop, so the SDK turns off the usual 5-second
 * cutoff. `flush()` confirms the records are durable, but their `onAck` calls may not have run
 * yet: only `close()` and the unacked getters wait for callbacks. A callback that references
 * its stream keeps an unclosed stream from being garbage-collected, so always call `close()`.
 * `onError` reports records that `getUnackedRecords()` and `getUnackedBatches()` also return:
 * recover each failed record from one of them, not both.
 */
export type MultiplexedAckCallback =
  | {
      /** The record or batch with this message ID was acknowledged. */
      onAck(messageId: bigint): void
      /** The record or batch with this message ID failed. */
      onError?(messageId: bigint, message: string): void
    }
  | {
      /** The record or batch with this message ID was acknowledged. */
      onAck?(messageId: bigint): void
      /** The record or batch with this message ID failed. */
      onError(messageId: bigint, message: string): void
    }

/** Authentication of a multiplexed stream (Beta): set exactly one of the two. */
export type MultiplexedAuth =
  | { oauth: { clientId: string; clientSecret: string }; headersProvider?: never }
  | { headersProvider: JsHeadersProvider; oauth?: never }

/**
 * Options of a multiplexed stream (Beta): the stream options plus an optional acknowledgment
 * callback shared by all sub-streams. `maxInflightRequests` is divided evenly across the
 * sub-streams, so it must be at least the stream count.
 */
export interface MultiplexedStreamOptions extends StreamConfigurationOptions {
  ackCallback?: MultiplexedAckCallback
}

/**
 * A multiplexed stream (Beta): records are spread round-robin over several sub-streams to one
 * table. Records keep their order within a sub-stream only.
 *
 * Ingest calls resolve to a message ID as soon as the record is queued (unlike the deprecated
 * `ZerobusStream.ingestRecord()`, they don't wait for the acknowledgment). Message IDs are opaque,
 * unordered bigints (some are negative): ingest in a loop, then `flush()` to confirm. Await
 * each ingest; it resolves when the record is queued. `flush()` does not wait for an ingest
 * that is still pending.
 * `waitForMessageId(id)` confirms only that record or batch. On an Avro stream, record objects
 * are encoded against the writer schema before ingestion. `getUnackedRecords()` and
 * `getUnackedBatches()` return `Buffer`s; on a JSON stream, re-ingest them as
 * `buf.toString('utf8')`, since a `Buffer` is taken as Protocol Buffers bytes. If its
 * sub-stream is full, an ingest waits up to 30 seconds and then rejects; that record isn't
 * queued, and the stream stays usable. Await ingest calls before `close()`: one that races it
 * either lands first (and `close()` flushes it) or rejects with `Stream has been closed`.
 *
 * ```typescript
 * const stream = await sdk.createMultiplexedStream(
 *   { tableName: 'catalog.schema.table', descriptorProto: descriptorBase64 },
 *   { oauth: { clientId, clientSecret } },
 *   4,
 * );
 * for (const record of records) {
 *   await stream.ingestRecord(record); // resolves once queued
 * }
 * await stream.flush(); // confirms everything ingested so far
 * await stream.close();
 * ```
 */
export declare class ZerobusMultiplexedStream extends NativeZerobusMultiplexedStream {}

/**
 * The main SDK for interacting with the Databricks Zerobus service. `createStream` returns a
 * {@link ZerobusStream} that encodes Avro record objects; Arrow Flight methods are inherited
 * from the native binding when it is built with Arrow support.
 */
export declare class ZerobusSdk extends NativeZerobusSdk {
  createStream(tableProperties: TableProperties, clientId: string, clientSecret: string, options?: StreamConfigurationOptions | undefined | null, headersProvider?: JsHeadersProvider | undefined | null): Promise<ZerobusStream>
  recreateStream(stream: ZerobusStream): Promise<ZerobusStream>
  /**
   * Opens a multiplexed stream (Beta) of `streamCount` sub-streams, an integer from 1 to 64,
   * to one table. The record format follows `tableProperties` and `options.recordType`, as for
   * `createStream`. Invalid arguments reject the returned promise.
   */
  createMultiplexedStream(tableProperties: TableProperties, auth: MultiplexedAuth, streamCount: number, options?: MultiplexedStreamOptions | null): Promise<ZerobusMultiplexedStream>
}
