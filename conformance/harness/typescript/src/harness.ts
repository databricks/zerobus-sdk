// `TestHarness` service: maps each contract RPC onto the TypeScript Zerobus SDK.
//
// Supported: protocol=PROTOCOL_GRPC with format=RECORD_FORMAT_PROTO or RECORD_FORMAT_JSON, and protocol=PROTOCOL_ARROW_FLIGHT with
// format=RECORD_FORMAT_ARROW. Every other combination returns INVALID_ARGUMENT or UNIMPLEMENTED with a TODO
// (see checkSliceSupported).
//
// Pass-through: each RPC calls the SDK method directly and reports its result. The harness adds
// no retries, timers, pre-validation, or error rewriting. Its only work is converting records into
// SDK input (see records.ts) and mapping stream_id onto the SDK handle.
//
// Error model (per proto/test_harness.proto): when the SDK under test rejects an action, the
// response's `error` is set to the SDK's message and the RPC returns OK. A gRPC error status is
// reserved for harness faults: unknown stream_id, unsupported protocol/format, records or schemas
// the harness cannot convert, and an SDK ingest that returns no offset.

import * as grpc from "@grpc/grpc-js";
import {
  RecordType,
  ZerobusArrowStream,
  ZerobusSdk,
  ZerobusStream,
} from "@databricks/zerobus-ingest-sdk";
import type * as protobuf from "protobufjs";
import type * as arrow from "apache-arrow";
import * as records from "./records";

type RecordFormatName = "RECORD_FORMAT_PROTO" | "RECORD_FORMAT_JSON" | "RECORD_FORMAT_ARROW";

// Request and response shapes from proto/test_harness.proto as loaded in main.ts
// (keepCase=false, so camelCase; int64 as string; enums as their names).

interface CreateStreamRequest {
  protocol: string;
  format: string;
  serverEndpoint: string;
  tablePath: string;
  descriptorProtoBytes: Buffer;
  arrowSchemaIpc: Buffer;
}

interface CreateStreamResponse {
  streamId?: string;
  error?: SdkErrorMsg;
}

interface IngestRecordRequest {
  streamId: string;
  recordJson: string;
}

interface IngestRecordsRequest {
  streamId: string;
  recordJson: string[];
}

interface OffsetResponse {
  offset?: string;
  error?: SdkErrorMsg;
}

interface AwaitRequest {
  streamId: string;
  offset: string;
}

interface StreamIdRequest {
  streamId: string;
}

interface ErrorResponse {
  error?: SdkErrorMsg;
}

/** Mirror of the contract's `SdkError` message. */
interface SdkErrorMsg {
  message: string;
  retryable: boolean;
}

/** One open SDK stream plus what its records are built against. Kept after Close. */
interface StreamEntry {
  /** Kept alive for as long as the stream it created. */
  sdk: ZerobusSdk;
  /** PROTOCOL_GRPC streams are ZerobusStream; RECORD_FORMAT_ARROW streams are ZerobusArrowStream. */
  stream: ZerobusStream | ZerobusArrowStream;
  format: RecordFormatName;
  /** Only set for RECORD_FORMAT_PROTO: the serialized DescriptorProto, decoded on first record. */
  descriptor: Buffer | null;
  /** Only set for RECORD_FORMAT_PROTO, once decoded from `descriptor`. */
  messageType: protobuf.Type | null;
  /** Only set for RECORD_FORMAT_ARROW. */
  arrowSchema: arrow.Schema | null;
}

/** Build a grpc-js error with a status code and a message (grpc-js reads `details`). */
function statusError(code: grpc.status, message: string): Error & { code: grpc.status; details: string } {
  return Object.assign(new Error(message), { code, details: message });
}

/**
 * Reject every protocol/format combination this slice does not implement. Each TODO names
 * the work that remains. Enum values arrive as their names (proto-loader `enums: String`).
 */
function checkSliceSupported(protocol: string, format: string): RecordFormatName {
  if (protocol === "PROTOCOL_ARROW_FLIGHT") {
    if (format !== "RECORD_FORMAT_ARROW") {
      throw statusError(grpc.status.INVALID_ARGUMENT, "PROTOCOL_ARROW_FLIGHT carries only RECORD_FORMAT_ARROW");
    }
    return "RECORD_FORMAT_ARROW";
  }
  if (protocol !== "PROTOCOL_GRPC") {
    throw statusError(grpc.status.INVALID_ARGUMENT, "protocol and format must be specified");
  }
  switch (format) {
    case "RECORD_FORMAT_PROTO":
    case "RECORD_FORMAT_JSON":
      return format;
    case "RECORD_FORMAT_AVRO":
      throw statusError(
        grpc.status.UNIMPLEMENTED,
        "TODO(conformance): PROTOCOL_GRPC + RECORD_FORMAT_AVRO not implemented in the TypeScript harness yet (the NAPI RecordType.Avro path has no record materialization here)",
      );
    case "RECORD_FORMAT_ARROW":
      throw statusError(
        grpc.status.INVALID_ARGUMENT,
        "PROTOCOL_GRPC + RECORD_FORMAT_ARROW is not a valid pairing; RECORD_FORMAT_ARROW rides PROTOCOL_ARROW_FLIGHT",
      );
    default:
      throw statusError(grpc.status.INVALID_ARGUMENT, "format must be specified");
  }
}

/**
 * Map a thrown value onto a grpc status. Harness conversion failures (RecordError) are
 * INVALID_ARGUMENT; a status error from this module passes through; anything else is a harness bug.
 */
function toStatusError(err: unknown): Error {
  if (err instanceof records.RecordError) {
    return statusError(grpc.status.INVALID_ARGUMENT, err.message);
  }
  const e = err as { code?: unknown; details?: unknown; message?: unknown };
  if (typeof e?.code === "number" && typeof e?.details === "string") {
    return err as Error;
  }
  return statusError(grpc.status.INTERNAL, e?.message != null ? String(e.message) : String(err));
}

/** Run a harness-side conversion. Any failure is a harness fault (a gRPC status), never an SdkError. */
function harnessFault<T>(convert: () => T): T {
  try {
    return convert();
  } catch (err) {
    throw toStatusError(err);
  }
}

/**
 * Map an error the SDK under test threw onto the contract's SdkError, with the SDK's message
 * verbatim. `retryable` reads the SDK's `isRetryable` when the thrown error carries it. The NAPI
 * binding currently throws plain Errors with only a message, so this is false today.
 */
function sdkError(err: unknown): SdkErrorMsg {
  const e = err as { message?: unknown; isRetryable?: unknown };
  const message = e?.message != null ? String(e.message) : String(err);
  return { message, retryable: e?.isRetryable === true };
}

/**
 * Build the untyped grpc-js implementation of `TestHarness`. `triggerShutdown` is called
 * after the Shutdown RPC has been answered.
 */
export function buildHarnessImpl(triggerShutdown: () => void): grpc.UntypedServiceImplementation {
  const streams = new Map<string, StreamEntry>();
  let nextId = 0;

  /**
   * Look up a stream or fail with NOT_FOUND. Closed streams stay in the map on purpose: later
   * calls on them reach the SDK, which rejects them with its own error.
   */
  function entryFor(streamId: string): StreamEntry {
    const entry = streams.get(streamId);
    if (!entry) {
      throw statusError(grpc.status.NOT_FOUND, `unknown stream_id '${streamId}'`);
    }
    return entry;
  }

  /** Convert one non-Arrow record into the value the SDK takes for that format. */
  function toSdkPayload(entry: StreamEntry, recordJson: string): unknown {
    if (entry.format === "RECORD_FORMAT_PROTO") {
      entry.messageType ??= records.messageTypeFromDescriptor(entry.descriptor!);
      return records.protoMessageFromJson(entry.messageType, recordJson);
    }
    // RECORD_FORMAT_JSON: the SDK receives the record string unchanged and judges it.
    return recordJson;
  }

  /**
   * Convert one record and return the SDK call that queues it. Conversion runs here, before the
   * SDK is called, so a harness fault throws without the SDK ever seeing the record.
   */
  function prepareIngestOne(entry: StreamEntry, recordJson: string): () => Promise<bigint | null> {
    if (entry.format === "RECORD_FORMAT_ARROW") {
      const batch = harnessFault(() => records.arrowBatchIpcFromJsonRows(entry.arrowSchema!, [recordJson]));
      const stream = entry.stream as ZerobusArrowStream;
      return () => stream.ingestBatch(batch);
    }
    const payload = harnessFault(() => toSdkPayload(entry, recordJson));
    const stream = entry.stream as ZerobusStream;
    return () => stream.ingestRecordOffset(payload as any);
  }

  /** As prepareIngestOne, for a batch: one SDK batch call for the whole list. */
  function prepareIngestMany(entry: StreamEntry, recordJsons: string[]): () => Promise<bigint | null> {
    if (entry.format === "RECORD_FORMAT_ARROW") {
      const batch = harnessFault(() => records.arrowBatchIpcFromJsonRows(entry.arrowSchema!, recordJsons));
      const stream = entry.stream as ZerobusArrowStream;
      return () => stream.ingestBatch(batch);
    }
    const payloads = harnessFault(() => recordJsons.map((json) => toSdkPayload(entry, json)));
    const stream = entry.stream as ZerobusStream;
    return () => stream.ingestRecordsOffset(payloads as any[]);
  }

  /**
   * Run one SDK ingest call. The SDK's rejection becomes the response's error. A null offset is the
   * SDK's signal that it queued nothing (an empty batch), so it is INVALID_ARGUMENT, not offset 0.
   */
  async function ingestResult(send: () => Promise<bigint | null>): Promise<OffsetResponse> {
    let offset: bigint | null;
    try {
      // Queue-only: resolves once the SDK has enqueued the work, not once it is acked.
      offset = await send();
    } catch (err) {
      return { error: sdkError(err) };
    }
    if (offset === null) {
      throw statusError(grpc.status.INVALID_ARGUMENT, "the SDK returned no offset (an empty batch has no offset)");
    }
    return { offset: String(offset) };
  }

  /** Adapt an async handler to grpc-js's callback style. Harness faults become status errors. */
  function unary<Req, Res>(
    handler: (req: Req) => Promise<Res>,
  ): (call: grpc.ServerUnaryCall<Req, Res>, callback: grpc.sendUnaryData<Res>) => void {
    return (call, callback) => {
      handler(call.request).then(
        (res) => callback(null, res),
        (err) => callback(toStatusError(err)),
      );
    };
  }

  const impl: grpc.UntypedServiceImplementation = {
    createStream: unary<CreateStreamRequest, CreateStreamResponse>(async (req) => {
      const format = checkSliceSupported(req.protocol, req.format);

      // The SDK judges the descriptor (it is passed through below). Arrow needs the field list
      // before the SDK is called, so its schema is decoded here; a schema the harness cannot decode
      // is a harness fault.
      const arrowSchema =
        format === "RECORD_FORMAT_ARROW" ? harnessFault(() => records.arrowSchemaFromIpc(req.arrowSchemaIpc)) : null;

      console.error(`CreateStream endpoint=${req.serverEndpoint} table=${req.tablePath} format=${format}`);

      try {
        // The mock is a local plaintext server that checks no credentials, so the SDK runs
        // with no TLS (`http://`) and no auth. These are TESTING-ONLY options backed by the
        // Rust SDK `testing` feature (see typescript/src/lib.rs, `noTls` / `noAuth`).
        const sdk = new ZerobusSdk(`http://${req.serverEndpoint}`, "", { noTls: true });
        let stream: ZerobusStream | ZerobusArrowStream;
        if (format === "RECORD_FORMAT_ARROW") {
          stream = await sdk.createArrowStream(
            {
              tableName: req.tablePath,
              schemaFields: records.arrowFieldsFromSchema(arrowSchema!),
            },
            "",
            "",
            { noAuth: true },
          );
        } else {
          stream = await sdk.createStream(
            {
              tableName: req.tablePath,
              descriptorProto:
                format === "RECORD_FORMAT_PROTO" ? Buffer.from(req.descriptorProtoBytes).toString("base64") : undefined,
            },
            "",
            "",
            {
              recordType: format === "RECORD_FORMAT_PROTO" ? RecordType.Proto : RecordType.Json,
              noAuth: true,
            },
          );
        }

        const streamId = `ts-stream-${nextId++}`;
        streams.set(streamId, {
          sdk,
          stream,
          format,
          descriptor: format === "RECORD_FORMAT_PROTO" ? Buffer.from(req.descriptorProtoBytes) : null,
          messageType: null,
          arrowSchema,
        });
        return { streamId };
      } catch (err) {
        return { error: sdkError(err) };
      }
    }),

    ingestRecord: unary<IngestRecordRequest, OffsetResponse>(async (req) => {
      const entry = entryFor(req.streamId);
      return ingestResult(prepareIngestOne(entry, req.recordJson));
    }),

    ingestRecords: unary<IngestRecordsRequest, OffsetResponse>(async (req) => {
      const entry = entryFor(req.streamId);
      return ingestResult(prepareIngestMany(entry, req.recordJson ?? []));
    }),

    awaitWriteCompleted: unary<AwaitRequest, ErrorResponse>(async (req) => {
      const entry = entryFor(req.streamId);
      try {
        // No harness deadline: the wait is bounded only by the SDK's own ack/flush timeout.
        await entry.stream.waitForOffset(BigInt(req.offset));
        return {};
      } catch (err) {
        return { error: sdkError(err) };
      }
    }),

    flush: unary<StreamIdRequest, ErrorResponse>(async (req) => {
      const entry = entryFor(req.streamId);
      try {
        await entry.stream.flush();
        return {};
      } catch (err) {
        return { error: sdkError(err) };
      }
    }),

    close: unary<StreamIdRequest, ErrorResponse>(async (req) => {
      const entry = entryFor(req.streamId);
      try {
        // The SDK's close is idempotent, and it rejects later RPCs on this stream itself.
        await entry.stream.close();
        console.error(`stream closed stream_id=${req.streamId}`);
        return {};
      } catch (err) {
        return { error: sdkError(err) };
      }
    }),

    shutdown: unary<Record<string, never>, Record<string, never>>(async () => {
      console.error("Shutdown RPC received");
      // The response is sent by the caller before the server is torn down.
      setImmediate(triggerShutdown);
      return {};
    }),
  };

  return impl;
}
