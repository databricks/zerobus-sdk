// Logical record -> SDK-native record conversion: the only place this harness understands record
// bodies. RECORD_FORMAT_PROTO builds a protobufjs Message the SDK encodes, RECORD_FORMAT_JSON
// hands the record string to the SDK unchanged, and RECORD_FORMAT_ARROW builds an Arrow IPC stream
// (one row per record) the SDK ingests as-is.
//
// Only what the conversion needs is checked here: keys the schema does not declare, types that
// cannot be materialized, and integers that cannot be carried exactly. Everything else is the SDK's
// to judge.

import * as protobuf from "protobufjs";
// Side-effect import: adds `Type.fromDescriptor` (DescriptorProto bytes -> Type).
import "protobufjs/ext/descriptor";
import * as arrow from "apache-arrow";
import { ArrowDataType, ArrowField } from "@databricks/zerobus-ingest-sdk";

/** Raised for caller-side record/schema problems the harness cannot convert; mapped to INVALID_ARGUMENT. */
export class RecordError extends Error {}

const INT32_MIN = -(2 ** 31);
const INT32_MAX = 2 ** 31 - 1;
const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;

/**
 * Check one integer value against its width. JSON numbers are doubles, so an int64 given as a
 * number is accepted only when it is a safe integer; larger values must be decimal strings.
 * Nothing is wrapped or rounded silently.
 */
export function checkIntField(name: string, bits: 32 | 64, value: unknown): void {
  if (bits === 32) {
    if (typeof value !== "number" || !Number.isInteger(value) || value < INT32_MIN || value > INT32_MAX) {
      throw new RecordError(`field '${name}': ${JSON.stringify(value)} is out of range for int32`);
    }
    return;
  }
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value)) {
      throw new RecordError(`field '${name}': ${value} is not a safe integer; send int64 values beyond 2^53 as decimal strings`);
    }
    return;
  }
  if (typeof value === "string" && /^-?\d+$/.test(value)) {
    const v = BigInt(value);
    if (v < INT64_MIN || v > INT64_MAX) {
      throw new RecordError(`field '${name}': ${value} is out of range for int64`);
    }
    return;
  }
  throw new RecordError(`field '${name}': ${JSON.stringify(value)} is not a valid int64`);
}

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

/** Parse `record_json` as a JSON object. */
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

/** Protobuf scalar integer types and their widths. */
const PROTO_INT_BITS: Record<string, 32 | 64 | undefined> = {
  int32: 32,
  sint32: 32,
  sfixed32: 32,
  int64: 64,
  sint64: 64,
  sfixed64: 64,
};

/**
 * Convert `record_json` into a protobufjs Message of `type`. Keys the descriptor does not declare
 * are rejected, since dropping them would silently change the record. Field values are checked
 * against the declared type and integer width.
 */
export function protoMessageFromJson(type: protobuf.Type, recordJson: string): protobuf.Message {
  const obj = parseRecordObject(recordJson);
  const fields = new Map(type.fieldsArray.map((f) => [f.name, f]));
  for (const key of Object.keys(obj)) {
    if (!fields.has(key)) {
      throw new RecordError(`record_json does not match the schema: unknown field '${key}'`);
    }
  }
  const problem = type.verify(obj);
  if (problem) {
    throw new RecordError(`record_json does not match the schema: ${problem}`);
  }
  for (const field of type.fieldsArray) {
    const bits = PROTO_INT_BITS[field.type];
    const value = obj[field.name];
    if (bits === undefined || value == null) continue;
    for (const v of field.repeated ? (value as unknown[]) : [value]) {
      checkIntField(field.name, bits, v);
    }
  }
  return type.fromObject(obj);
}

/** Decode `arrow_schema_ipc` (an Arrow IPC stream carrying just the schema). */
export function arrowSchemaFromIpc(bytes: Buffer): arrow.Schema {
  if (bytes.length === 0) {
    throw new RecordError("arrow_schema_ipc is empty");
  }
  try {
    return arrow.tableFromIPC(bytes).schema;
  } catch (e: any) {
    throw new RecordError(`arrow_schema_ipc is not a valid Arrow IPC stream: ${e?.message ?? e}`);
  }
}

/**
 * Map the decoded Arrow schema onto the SDK's `ArrowField` form. Only the types the
 * conformance schemas use are mapped; others fail loudly.
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
  throw new RecordError(`field '${field.name}': arrow type ${t} is not supported by the harness yet`);
}

/**
 * Build one Arrow IPC stream (schema + a single batch) holding one row per `record_json`, shaped
 * by `schema`. Unknown JSON keys are rejected. Absent keys become nulls (a non-nullable absent
 * field is rejected).
 */
export function arrowBatchIpcFromJsonRows(schema: arrow.Schema, recordJsons: string[]): Buffer {
  const known = new Set(schema.fields.map((f) => f.name));
  const objs = recordJsons.map((json) => {
    const obj = parseRecordObject(json);
    for (const key of Object.keys(obj)) {
      if (!known.has(key)) {
        throw new RecordError(`record_json has unknown field '${key}'`);
      }
    }
    return obj;
  });

  const columns = schema.fields.map((field) => {
    const t = field.type;
    const typed = objs.map((obj) => {
      const cell = obj[field.name] ?? null;
      if (cell === null) {
        if (!field.nullable) {
          throw new RecordError(`record_json is missing non-nullable field '${field.name}'`);
        }
        return null;
      }
      if (arrow.DataType.isInt(t)) {
        checkIntField(field.name, t.bitWidth as 32 | 64, cell);
        return t.bitWidth === 64 ? BigInt(cell as number | string) : cell;
      }
      if (arrow.DataType.isUtf8(t) || arrow.DataType.isLargeUtf8(t)) {
        if (typeof cell !== "string") {
          throw new RecordError(`field '${field.name}': expected string, got ${JSON.stringify(cell)}`);
        }
        return cell;
      }
      // arrowDataTypeOf already rejected unsupported types at CreateStream time.
      throw new RecordError(`field '${field.name}': arrow type ${t} is not supported by the harness yet`);
    });
    return arrow.vectorFromArray(typed, t).data[0];
  });

  const data = arrow.makeData({
    type: new arrow.Struct(schema.fields),
    length: objs.length,
    children: columns,
  });
  const batch = new arrow.RecordBatch(schema, data);
  return Buffer.from(arrow.tableToIPC(new arrow.Table(batch), "stream"));
}
