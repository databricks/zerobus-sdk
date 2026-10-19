/* tslint:disable */
/* eslint-disable */

// Public type surface. Re-exports the native binding's types and overrides the two
// classes the wrapper replaces (RecordType, ZerobusError, Arrow types come from there).

export * from './index'

import type {
  TableProperties,
  StreamConfigurationOptions,
  JsHeadersProvider,
} from './index'

/**
 * A stream for ingesting data into a Databricks Delta table.
 *
 * On an Avro stream (TableProperties.avroSchema set), a record object is encoded to
 * Avro bytes against the writer schema before ingestion; a pre-encoded Buffer and
 * proto/JSON payloads are forwarded unchanged.
 */
export declare class ZerobusStream {
  /** @deprecated Use ingestRecordOffset(). */
  ingestRecord(payload: unknown): Promise<bigint>
  /** @deprecated Use ingestRecordsOffset(). */
  ingestRecords(records: Array<unknown>): Promise<bigint | null>
  ingestRecordOffset(payload: unknown): Promise<bigint>
  ingestRecordsOffset(records: Array<unknown>): Promise<bigint | null>
  waitForOffset(offsetId: bigint): Promise<void>
  flush(): Promise<void>
  close(): Promise<void>
  getUnackedRecords(): Promise<Array<Buffer>>
  getUnackedBatches(): Promise<Array<Array<Buffer>>>
}

export declare class ZerobusSdk {
  constructor(zerobusEndpoint: string, unityCatalogUrl: string, options?: import('./index').ZerobusSdkOptions | undefined | null)
  createStream(tableProperties: TableProperties, clientId: string, clientSecret: string, options?: StreamConfigurationOptions | undefined | null, headersProvider?: JsHeadersProvider | undefined | null): Promise<ZerobusStream>
  recreateStream(stream: ZerobusStream): Promise<ZerobusStream>
  /** Arrow Flight: present only when the native binding is built with Arrow support. */
  createArrowStream(tableProperties: any, clientId: string, clientSecret: string, options?: any): Promise<any>
  /** Arrow Flight: present only when the native binding is built with Arrow support. */
  recreateArrowStream(stream: any): Promise<any>
}
