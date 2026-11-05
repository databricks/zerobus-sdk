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

/**
 * A Zerobus-scoped Databricks OAuth access token, returned by {@link getToken}.
 * `util.inspect` (and so `console.log`) redacts `accessToken`.
 */
export declare class OAuthToken {
  /** The bearer token. */
  readonly accessToken: string
  /**
   * When the token expires, measured from when the request was sent; `undefined` if the
   * server reported no lifetime.
   */
  readonly expiresAt: Date | undefined
}

/**
 * Fetches a Zerobus-scoped OAuth token for use outside the SDK's own streams.
 *
 * Mints the same token the SDK uses for its streams (OAuth client credentials, scoped to
 * writing `tableName`), for clients that take an OAuth token callback, e.g. a Kafka producer
 * using SASL/OAUTHBEARER. Each call fetches a new token; there is no caching, so call it from
 * the client's refresh hook.
 *
 * @param workspaceUrl - The Databricks workspace URL.
 * @param tableName - The target table, as `catalog.schema.table`.
 * @param clientId - The service principal's OAuth client ID.
 * @param clientSecret - The service principal's OAuth client secret.
 * @param workspaceId - The numeric workspace ID.
 * @throws If the input is invalid, the token endpoint rejects the request, or it is unreachable.
 */
export declare function getToken(workspaceUrl: string, tableName: string, clientId: string, clientSecret: string, workspaceId: string): Promise<OAuthToken>
