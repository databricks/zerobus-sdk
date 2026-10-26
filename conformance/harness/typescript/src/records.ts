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
//
// Pipeline (ARROW): CreateStreamRequest.arrow_schema_ipc -> arrowSchemaFromIpc (apache-arrow
// Schema) -> arrowFieldsFromSchema (SDK ArrowField[]); IngestRecordRequest.record_json +
// Schema -> arrowBatchIpcFromJson -> a one-row Arrow IPC stream the SDK ingests as-is.

import * as protobuf from "protobufjs";
// Side-effect import: adds `Type.fromDescriptor` (DescriptorProto bytes -> Type).
import "protobufjs/ext/descriptor";
import * as arrow from "apache-arrow";
import { ArrowDataType, ArrowField } from "@databricks/zerobus-ingest-sdk";

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

/** Decode `arrow_schema_ipc` (an Arrow IPC stream carrying just the schema). */
export function arrowSchemaFromIpc(bytes: Buffer): arrow.Schema {
  if (bytes.length === 0) {
    throw new RecordError("arrow_schema_ipc is empty");
  }
  let schema: arrow.Schema;
  try {
    schema = arrow.tableFromIPC(bytes).schema;
  } catch (e: any) {
    throw new RecordError(`arrow_schema_ipc is not a valid Arrow IPC stream: ${e?.message ?? e}`);
  }
  return schema;
}

/**
 * Map the decoded Arrow schema onto the SDK's `ArrowField` form. Only the types the
 * conformance schemas use are mapped, mirroring the Rust harness; others fail loudly.
 */
export function arrowFieldsFromSchema(schema: arrow.Schema): ArrowField[] {
  return schema.fields.map((field) => ({
    name: field.name,
    dataType: arrowDataTypeOf(field),
    nullable: field.nullable,
  }));
}

function arrowDataTypeOf(field: arrow.Field): ArrowDataType {
  const t = field.type;
  if (arrow.DataType.isInt(t)) {
    if (t.bitWidth === 64 && t.isSigned) return ArrowDataType.Int64;
    if (t.bitWidth === 32 && t.isSigned) return ArrowDataType.Int32;
  } else if (arrow.DataType.isUtf8(t)) {
    return ArrowDataType.Utf8;
  } else if (arrow.DataType.isLargeUtf8(t)) {
    return ArrowDataType.LargeUtf8;
  }
  throw new RecordError(
    `field '${field.name}': arrow type ${t} is not supported by the harness yet`,
  );
}

/**
 * Build a one-row Arrow IPC stream (schema + batch) from `record_json`, shaped by `schema`.
 * Unknown JSON keys are rejected. Absent keys become nulls (every field is nullable in the
 * conformance schemas; a non-nullable absent field is rejected).
 */
export function arrowBatchIpcFromJson(schema: arrow.Schema, recordJson: string): Buffer {
  const obj = parseRecordObject(recordJson);
  const known = new Set(schema.fields.map((f) => f.name));
  for (const key of Object.keys(obj)) {
    if (!known.has(key)) {
      throw new RecordError(`record_json has unknown field '${key}'`);
    }
  }

  const columns = schema.fields.map((field) => {
    const value = obj[field.name];
    if (value === undefined && !field.nullable) {
      throw new RecordError(`record_json is missing non-nullable field '${field.name}'`);
    }
    const cell = value ?? null;
    const t = field.type;
    let typed: unknown[];
    if (cell === null) {
      typed = [null];
    } else if (arrow.DataType.isInt(t) && t.bitWidth === 64) {
      if (typeof cell !== "number" && typeof cell !== "string") {
        throw new RecordError(`field '${field.name}': expected int64, got ${JSON.stringify(cell)}`);
      }
      typed = [BigInt(cell)];
    } else if (arrow.DataType.isInt(t) && t.bitWidth === 32) {
      if (typeof cell !== "number") {
        throw new RecordError(`field '${field.name}': expected int32, got ${JSON.stringify(cell)}`);
      }
      typed = [cell];
    } else if (arrow.DataType.isUtf8(t) || arrow.DataType.isLargeUtf8(t)) {
      if (typeof cell !== "string") {
        throw new RecordError(`field '${field.name}': expected string, got ${JSON.stringify(cell)}`);
      }
      typed = [cell];
    } else {
      // arrowDataTypeOf already rejected unsupported types at CreateStream time.
      throw new RecordError(`field '${field.name}': arrow type ${t} is not supported by the harness yet`);
    }
    return arrow.vectorFromArray(typed, t).data[0];
  });

  const data = arrow.makeData({
    type: new arrow.Struct(schema.fields),
    length: 1,
    children: columns,
  });
  const batch = new arrow.RecordBatch(schema, data);
  return Buffer.from(arrow.tableToIPC(new arrow.Table(batch), "stream"));
}
