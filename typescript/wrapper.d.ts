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
 * Configures and opens a stream; returned by {@link ZerobusSdk.streamBuilder}.
 *
 * Set the table, exactly one authentication method and one record format, optionally the
 * stream options, then call `build()`. For each group the last setter called wins. Setters
 * validate their arguments and throw on invalid values; `build()` rejects when the table,
 * the authentication or the record format is missing.
 *
 * ```typescript
 * const stream = await sdk.streamBuilder()
 *   .table('catalog.schema.table')
 *   .oauth(clientId, clientSecret)
 *   .compiledProto(descriptorBase64)
 *   .build();
 * for (const record of records) {
 *   await stream.ingestRecordOffset(record); // resolves once queued
 * }
 * await stream.flush(); // confirms everything ingested so far
 * await stream.close();
 * ```
 */
export interface StreamBuilder {
  /** Target table, `catalog.schema.table`. */
  table(tableName: string): this
  /** OAuth 2.0 client credentials of a service principal. */
  oauth(clientId: string, clientSecret: string): this
  /** Custom authentication headers instead of OAuth. */
  headersProvider(provider: JsHeadersProvider): this
  /** JSON records. */
  json(): this
  /** Protocol Buffers records; the descriptor as a base64-encoded `DescriptorProto`. */
  compiledProto(descriptorBase64: string): this
  /** Avro records against this writer schema (JSON). Requires the `avsc` package for record objects. */
  avro(schemaJson: string): this
  /** See {@link StreamConfigurationOptions.maxInflightRequests}. At least 1 (throws otherwise). */
  maxInflightRequests(n: number): this
  /** See {@link StreamConfigurationOptions.recovery}. */
  recovery(enabled: boolean): this
  /** See {@link StreamConfigurationOptions.recoveryTimeoutMs}. */
  recoveryTimeoutMs(ms: number): this
  /** See {@link StreamConfigurationOptions.recoveryBackoffMs}. */
  recoveryBackoffMs(ms: number): this
  /** See {@link StreamConfigurationOptions.recoveryRetries}. */
  recoveryRetries(n: number): this
  /** See {@link StreamConfigurationOptions.flushTimeoutMs}. */
  flushTimeoutMs(ms: number): this
  /** See {@link StreamConfigurationOptions.serverLackOfAckTimeoutMs}. */
  serverLackOfAckTimeoutMs(ms: number): this
  /** See {@link StreamConfigurationOptions.streamPausedMaxWaitTimeMs}; `null` restores the default. */
  streamPausedMaxWaitTimeMs(ms: number | null): this
  /**
   * Selects a multiplexed stream (Beta) of `streamCount` sub-streams, an integer from 1 to 64.
   *
   * Terminal: call it after the other setters. The returned builder keeps the settings as they
   * are now and only offers `ackCallback()` and `build()`. `maxInflightRequests` is divided
   * evenly across the sub-streams, so it must be at least `streamCount`.
   */
  multiplexed(streamCount: number): MultiplexedStreamBuilder
  /** Opens an ordinary stream with these settings. */
  build(): Promise<ZerobusStream>
}

/**
 * Acknowledgment callback of a multiplexed stream (Beta): set `onAck`, `onError` or both.
 *
 * One object is shared by all sub-streams, and calls arrive in no global order. Keep the
 * methods short: they run on the Node.js event loop, once per record or batch. A method that
 * throws, or returns a promise that rejects, is logged with `console.error`; delivery continues.
 * When `close()` or an unacked getter settles, every method call has run; async work a method
 * starts may still be pending. `flush()` confirms the records are durable, but their `onAck`
 * calls may not have run yet: only `close()` and the unacked getters wait for callbacks. A
 * callback that references its stream keeps an unclosed stream from being garbage-collected,
 * so always call `close()`.
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

/** Builds a multiplexed stream (Beta); returned by {@link StreamBuilder.multiplexed}. */
export interface MultiplexedStreamBuilder {
  /** Sets the acknowledgment callback shared by all sub-streams. */
  ackCallback(callback: MultiplexedAckCallback): this
  /** Opens every sub-stream; resolves only once all of them are open. */
  build(): Promise<ZerobusMultiplexedStream>
}

/**
 * A multiplexed stream (Beta): records are spread round-robin over several sub-streams to one
 * table. Records keep their order within a sub-stream only.
 *
 * Ingest calls resolve to a message ID as soon as the record is queued. Message IDs are opaque,
 * unordered bigints (some are negative): ingest in a loop, then `flush()` to confirm.
 * `waitForMessageId(id)` confirms only that record or batch. On an Avro stream, record objects
 * are encoded against the writer schema before ingestion. `getUnackedRecords()` and
 * `getUnackedBatches()` return `Buffer`s; on a JSON stream, re-ingest them as
 * `buf.toString('utf8')`, since a `Buffer` is taken as Protocol Buffers bytes.
 *
 * ```typescript
 * const stream = await sdk.streamBuilder()
 *   .table('catalog.schema.table')
 *   .oauth(clientId, clientSecret)
 *   .compiledProto(descriptorBase64)
 *   .multiplexed(4)
 *   .build();
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
  /** Returns a {@link StreamBuilder} for a stream on this SDK. */
  streamBuilder(): StreamBuilder
}
