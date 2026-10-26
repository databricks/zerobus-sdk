// `TestHarness` service: maps each contract RPC onto the TypeScript Zerobus SDK.
//
// Supported: protocol=GRPC with format=PROTO or format=JSON, and protocol=ARROW_FLIGHT with
// format=ARROW. Every other combination returns INVALID_ARGUMENT or UNIMPLEMENTED with a TODO
// (see checkSliceSupported).

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

type RecordFormatName = "PROTO" | "JSON" | "ARROW";

/** One open SDK stream plus what its records are built against. */
interface StreamEntry {
  /** Kept alive for as long as the stream it created. */
  sdk: ZerobusSdk;
  /** null after Close. GRPC streams are ZerobusStream; ARROW streams are ZerobusArrowStream. */
  stream: ZerobusStream | ZerobusArrowStream | null;
  format: RecordFormatName;
  /** Only set for PROTO. */
  messageType: protobuf.Type | null;
  /** Only set for ARROW. */
  arrowSchema: arrow.Schema | null;
  /** Tail of the per-stream chain: RPCs on one stream run strictly in order. */
  chain: Promise<unknown>;
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
  if (protocol === "ARROW_FLIGHT") {
    if (format !== "ARROW") {
      throw statusError(grpc.status.INVALID_ARGUMENT, "ARROW_FLIGHT carries only the ARROW format");
    }
    return "ARROW";
  }
  if (protocol !== "GRPC") {
    throw statusError(grpc.status.INVALID_ARGUMENT, "protocol and format must be specified");
  }
  switch (format) {
    case "PROTO":
    case "JSON":
      return format;
    case "AVRO":
      throw statusError(
        grpc.status.UNIMPLEMENTED,
        "TODO(conformance): GRPC + AVRO not implemented in the TypeScript harness yet (the NAPI RecordType.Avro path has no record materialization here)",
      );
    case "ARROW":
      throw statusError(
        grpc.status.INVALID_ARGUMENT,
        "GRPC + ARROW is not a valid pairing; ARROW rides ARROW_FLIGHT",
      );
    default:
      throw statusError(grpc.status.INVALID_ARGUMENT, "format must be specified");
  }
}

/** Map a thrown value onto a grpc status. Our own argument problems stay INVALID_ARGUMENT. */
function toStatusError(err: unknown, fallbackPrefix: string): Error {
  if (err instanceof records.RecordError) {
    return statusError(grpc.status.INVALID_ARGUMENT, err.message);
  }
  const e = err as { code?: unknown; details?: unknown; message?: unknown };
  if (typeof e?.code === "number" && typeof e?.details === "string") {
    return err as Error; // already a status error from this module
  }
  const msg = e?.message != null ? String(e.message) : String(err);
  return statusError(grpc.status.INTERNAL, `${fallbackPrefix}${msg}`);
}

/**
 * Build the untyped grpc-js implementation of `TestHarness`. `triggerShutdown` is called
 * after the Shutdown RPC has been answered.
 */
export function buildHarnessImpl(triggerShutdown: () => void): grpc.UntypedServiceImplementation {
  const streams = new Map<string, StreamEntry>();
  let nextId = 0;

  /** Look up a stream or fail with NOT_FOUND. */
  function entryFor(streamId: string): StreamEntry {
    const entry = streams.get(streamId);
    if (!entry) {
      throw statusError(grpc.status.NOT_FOUND, `unknown stream_id '${streamId}'`);
    }
    return entry;
  }

  /** Run `task` after every earlier RPC on the same stream has finished. */
  function serialize<T>(entry: StreamEntry, task: () => Promise<T>): Promise<T> {
    const run = entry.chain.then(task);
    entry.chain = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  /** Adapt an async handler to grpc-js's callback style. */
  function unary<Req, Res>(
    handler: (req: Req) => Promise<Res>,
  ): (call: grpc.ServerUnaryCall<Req, Res>, callback: grpc.sendUnaryData<Res>) => void {
    return (call, callback) => {
      handler(call.request).then(
        (res) => callback(null, res),
        (err) => callback(err as grpc.ServiceError),
      );
    };
  }

  const impl: grpc.UntypedServiceImplementation = {
    createStream: unary(async (req: any) => {
      const format = checkSliceSupported(req.protocol, req.format);

      if (!req.serverEndpoint) {
        throw statusError(grpc.status.INVALID_ARGUMENT, "server_endpoint required");
      }
      if (!req.tablePath) {
        throw statusError(grpc.status.INVALID_ARGUMENT, "table_path required");
      }

      let messageType: protobuf.Type | null = null;
      let arrowSchema: arrow.Schema | null = null;
      if (format === "PROTO") {
        const bytes = Buffer.from(req.descriptorProtoBytes ?? []);
        try {
          messageType = records.messageTypeFromDescriptor(bytes);
        } catch (err) {
          throw toStatusError(err, "");
        }
      } else if (format === "ARROW") {
        try {
          arrowSchema = records.arrowSchemaFromIpc(Buffer.from(req.arrowSchemaIpc ?? []));
        } catch (err) {
          throw toStatusError(err, "");
        }
      }

      console.error(`CreateStream endpoint=${req.serverEndpoint} table=${req.tablePath} format=${format}`);

      // The mock is a local plaintext server that checks no credentials, so the SDK runs
      // with no TLS (`http://`) and no auth. These are TESTING-ONLY options backed by the
      // Rust SDK `testing` feature (see typescript/src/lib.rs, `noTls` / `noAuth`).
      const sdk = new ZerobusSdk(`http://${req.serverEndpoint}`, "", { noTls: true });
      try {
        let stream: ZerobusStream | ZerobusArrowStream;
        if (format === "ARROW") {
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
                format === "PROTO" ? Buffer.from(req.descriptorProtoBytes).toString("base64") : undefined,
            },
            "",
            "",
            {
              recordType: format === "PROTO" ? RecordType.Proto : RecordType.Json,
              noAuth: true,
            },
          );
        }

        const streamId = `ts-stream-${nextId++}`;
        streams.set(streamId, { sdk, stream, format, messageType, arrowSchema, chain: Promise.resolve() });
        return { streamId };
      } catch (err) {
        throw toStatusError(err, "create_stream failed: ");
      }
    }),

    ingestRecord: unary(async (req: any) => {
      const entry = entryFor(req.streamId);
      return serialize(entry, async () => {
        if (!entry.stream) {
          throw statusError(grpc.status.FAILED_PRECONDITION, "stream closed");
        }
        if (entry.format === "ARROW") {
          let ipc: Buffer;
          try {
            ipc = records.arrowBatchIpcFromJson(entry.arrowSchema!, req.recordJson);
          } catch (err) {
            throw toStatusError(err, "");
          }
          // Queue only: ingestBatch resolves once the batch is enqueued, not once it is acked.
          const offset = await (entry.stream as ZerobusArrowStream).ingestBatch(ipc);
          return { offset: offset.toString() };
        }
        let payload: unknown;
        try {
          if (entry.format === "PROTO") {
            payload = records.protoMessageFromJson(entry.messageType!, req.recordJson);
          } else {
            // JSON: validate the object, then the SDK sends the JSON string as-is.
            records.parseRecordObject(req.recordJson);
            payload = req.recordJson;
          }
        } catch (err) {
          throw toStatusError(err, "");
        }
        // Queue only: resolves once the SDK has enqueued the record, not once it is acked.
        // The SDK encodes the record itself.
        const offset = await (entry.stream as ZerobusStream).ingestRecordOffset(payload as any);
        return { offset: offset.toString() };
      });
    }),

    awaitWriteCompleted: unary(async (req: any) => {
      const entry = entryFor(req.streamId);
      return serialize(entry, async () => {
        if (!entry.stream) {
          return { errorMessage: "stream has been closed" };
        }
        const offset = BigInt(req.offset);
        const timeoutMs = Number(req.timeoutMs);
        const wait = entry.stream.waitForOffset(offset);
        try {
          // waitForOffset has no timeout of its own; a value <= 0 waits with no deadline.
          if (timeoutMs > 0) {
            let timer: NodeJS.Timeout | undefined;
            const timedOut = new Promise<never>((_, reject) => {
              timer = setTimeout(
                () => reject(new Error(`timed out after ${timeoutMs} ms waiting for offset ${offset}`)),
                timeoutMs,
              );
            });
            try {
              await Promise.race([wait, timedOut]);
            } finally {
              clearTimeout(timer);
            }
          } else {
            await wait;
          }
          return { errorMessage: "" };
        } catch (e: any) {
          // Per the contract, a failed wait is reported in the body, not as a gRPC error.
          return { errorMessage: `wait_for_offset failed: ${e?.message ?? e}` };
        }
      });
    }),

    flush: unary(async (req: any) => {
      const entry = entryFor(req.streamId);
      return serialize(entry, async () => {
        if (!entry.stream) {
          throw statusError(grpc.status.FAILED_PRECONDITION, "stream closed");
        }
        try {
          await entry.stream.flush();
        } catch (err) {
          throw toStatusError(err, "flush failed: ");
        }
        return {};
      });
    }),

    close: unary(async (req: any) => {
      // Remove first so new RPCs on this id see NOT_FOUND. The entry is dropped even if the
      // SDK close fails.
      const entry = entryFor(req.streamId);
      streams.delete(req.streamId);
      return serialize(entry, async () => {
        if (entry.stream) {
          const stream = entry.stream;
          entry.stream = null;
          try {
            await stream.close();
          } catch (err) {
            throw toStatusError(err, "close failed: ");
          }
          console.error(`stream closed stream_id=${req.streamId}`);
        }
        return {};
      });
    }),

    shutdown: unary(async () => {
      console.error("Shutdown RPC received");
      // The response is sent by the caller before the server is torn down.
      setImmediate(triggerShutdown);
      return {};
    }),
  };

  return impl;
}
