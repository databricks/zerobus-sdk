// Logical record -> SDK-native record conversion. The only place this harness
// understands record bodies.
//
// Pipeline (PROTO): CreateStreamRequest.descriptor_proto_bytes -> messageTypeFromDescriptor
// -> protobufjs Type; IngestRecordRequest.record_json + Type -> protoMessageFromJson -> a
// protobufjs Message. The TS SDK encodes that Message itself (it calls the message
// constructor's static `encode`, see typescript/src/lib.rs convert_js_to_record_payload).
//
// Pipeline (JSON): record_json is validated as a JSON object and passed to the SDK as the
// JSON string; the SDK sends it as-is.

import * as protobuf from "protobufjs";
// Side-effect import: adds `Type.fromDescriptor` (DescriptorProto bytes -> Type).
import "protobufjs/ext/descriptor";

/** Raised for caller-side record/descriptor problems; mapped to INVALID_ARGUMENT. */
export class RecordError extends Error {}

/** Decode a serialized `google.protobuf.DescriptorProto` into a protobufjs Type. */
export function messageTypeFromDescriptor(bytes: Buffer): protobuf.Type {
  if (bytes.length === 0) {
    throw new RecordError("descriptor_proto_bytes is empty");
  }
  let type: protobuf.Type;
  try {
    type = protobuf.Type.fromDescriptor(bytes);
    type.resolveAll();
  } catch (e: any) {
    throw new RecordError(`descriptor_proto_bytes is not a valid DescriptorProto: ${e?.message ?? e}`);
  }
  return type;
}

/**
 * Parse `record_json` as a JSON object. Shared by both formats so a malformed
 * fixture fails the same way regardless of the record format.
 */
export function parseRecordObject(recordJson: string): Record<string, unknown> {
  let value: unknown;
  try {
    value = JSON.parse(recordJson);
  } catch (e: any) {
    throw new RecordError(`record_json is not valid JSON: ${e?.message ?? e}`);
  }
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new RecordError("record_json must be a JSON object");
  }
  return value as Record<string, unknown>;
}

/**
 * Convert `record_json` into a protobufjs Message of `type`. Unknown fields are rejected
 * (mirrors the Rust harness's prost-reflect default), so a fixture typo fails loudly.
 * Field matching is by declared field name; the conformance schemas use names that are
 * identical to their protobuf-JSON names.
 */
export function protoMessageFromJson(type: protobuf.Type, recordJson: string): protobuf.Message {
  const obj = parseRecordObject(recordJson);
  const known = new Set(type.fieldsArray.map((f) => f.name));
  for (const key of Object.keys(obj)) {
    if (!known.has(key)) {
      throw new RecordError(`record_json does not match the schema: unknown field '${key}'`);
    }
  }
  const problem = type.verify(obj);
  if (problem) {
    throw new RecordError(`record_json does not match the schema: ${problem}`);
  }
  return type.fromObject(obj);
}
