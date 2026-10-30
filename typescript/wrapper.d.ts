/* tslint:disable */
/* eslint-disable */

// Public type surface. Re-exports the native binding's types; the two wrapped classes extend
// the native ones, so Arrow typings are inherited (and absent in builds without Arrow).

export * from './index'

import {
  ZerobusSdk as NativeZerobusSdk,
  ZerobusStream as NativeZerobusStream,
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
 * The main SDK for interacting with the Databricks Zerobus service. `createStream` returns a
 * {@link ZerobusStream} that encodes Avro record objects; Arrow Flight methods are inherited
 * from the native binding when it is built with Arrow support.
 */
export declare class ZerobusSdk extends NativeZerobusSdk {
  createStream(tableProperties: TableProperties, clientId: string, clientSecret: string, options?: StreamConfigurationOptions | undefined | null, headersProvider?: JsHeadersProvider | undefined | null): Promise<ZerobusStream>
  recreateStream(stream: ZerobusStream): Promise<ZerobusStream>
}
