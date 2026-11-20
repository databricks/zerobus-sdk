package com.databricks.zerobus.conformance;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Logical record -> SDK-native record conversion: the only place this harness understands record
 * bodies. Each format builds the SDK's input (a {@link DynamicMessage} or a {@link
 * VectorSchemaRoot}); the SDK does the encoding on ingest.
 */
final class Records {
  private static final String DESCRIPTOR_FILE_NAME = "conformance_record.proto";

  private Records() {}

  /** Decodes a serialized {@code google.protobuf.DescriptorProto}. */
  static DescriptorProto descriptorProto(ByteString bytes) throws InvalidProtocolBufferException {
    return DescriptorProto.parseFrom(bytes);
  }

  /**
   * Resolves a standalone {@code DescriptorProto} into a message {@link Descriptor}. The message is
   * wrapped in a synthetic file with no package and no dependencies, which is all the SDK's
   * simple_record fixture needs.
   */
  static Descriptor messageDescriptor(DescriptorProto descriptorProto)
      throws DescriptorValidationException {
    FileDescriptorProto file =
        FileDescriptorProto.newBuilder()
            .setName(DESCRIPTOR_FILE_NAME)
            .addMessageType(descriptorProto)
            .build();
    FileDescriptor fd = FileDescriptor.buildFrom(file, new FileDescriptor[0]);
    Descriptor d = fd.findMessageTypeByName(descriptorProto.getName());
    if (d == null) {
      throw new IllegalArgumentException("message not found: " + descriptorProto.getName());
    }
    return d;
  }

  /**
   * Parses a canonical protobuf-JSON object into a message of {@code descriptor}. Unknown fields
   * are rejected (the parser default), so a fixture typo fails loudly instead of being dropped. A
   * JSON null leaves its field unset (the parser default). The Arrow path matches both.
   */
  static Message toMessage(Descriptor descriptor, String recordJson)
      throws InvalidProtocolBufferException {
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
    JsonFormat.parser().merge(recordJson, builder);
    return builder.build();
  }

  /**
   * Decodes the Arrow IPC stream carrying only the schema (the orchestrator's {@code
   * arrow_schema_ipc}). The temporary allocator is closed before returning; the {@link Schema} is a
   * plain value object and does not reference it.
   */
  static Schema arrowSchema(ByteString bytes) throws IOException {
    try (BufferAllocator allocator = new RootAllocator();
        ArrowStreamReader reader = new ArrowStreamReader(bytes.newInput(), allocator)) {
      return reader.getVectorSchemaRoot().getSchema();
    }
  }

  /**
   * Builds one batch shaped by {@code schema} with one row per entry of {@code recordJsons}, in
   * order. Unknown JSON keys are rejected. A missing field or an explicit JSON null is null (unset),
   * as in the proto path. Only int64 and (large) utf8 columns are supported. Every value of every
   * row is converted before any Arrow memory is allocated, so a rejected record leaks nothing. The
   * caller owns and must close the result.
   */
  static VectorSchemaRoot batch(Schema schema, BufferAllocator allocator, List<String> recordJsons) {
    List<Field> fields = schema.getFields();
    Set<String> names = new HashSet<>();
    for (Field f : fields) {
      names.add(f.getName());
    }

    List<Object[]> rows = new ArrayList<>(recordJsons.size());
    for (String recordJson : recordJsons) {
      JsonObject object = parseObject(recordJson);
      for (String key : object.keySet()) {
        if (!names.contains(key)) {
          throw new IllegalArgumentException("record_json has unknown field '" + key + "'");
        }
      }
      Object[] cells = new Object[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        cells[i] = cell(fields.get(i), object);
      }
      rows.add(cells);
    }

    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    try {
      for (int row = 0; row < rows.size(); row++) {
        for (int col = 0; col < fields.size(); col++) {
          setCell(root.getVector(col), row, rows.get(row)[col], fields.get(col).getName());
        }
      }
      root.setRowCount(rows.size());
      return root;
    } catch (RuntimeException e) {
      root.close();
      throw e;
    }
  }

  /** Writes one converted cell into {@code vector} at {@code row}. {@code null} clears the slot. */
  private static void setCell(FieldVector vector, int row, Object cell, String name) {
    if (vector instanceof BigIntVector) {
      BigIntVector v = (BigIntVector) vector;
      if (cell == null) {
        v.setNull(row);
      } else {
        v.setSafe(row, (Long) cell);
      }
    } else if (vector instanceof LargeVarCharVector) {
      LargeVarCharVector v = (LargeVarCharVector) vector;
      if (cell == null) {
        v.setNull(row);
      } else {
        v.setSafe(row, (byte[]) cell);
      }
    } else if (vector instanceof VarCharVector) {
      VarCharVector v = (VarCharVector) vector;
      if (cell == null) {
        v.setNull(row);
      } else {
        v.setSafe(row, (byte[]) cell);
      }
    } else {
      throw new IllegalArgumentException(
          "field '" + name + "': arrow type " + vector.getMinorType()
              + " is not supported by the harness yet");
    }
  }

  /**
   * Converts one field's JSON value to its cell: {@code Long} for int64, UTF-8 {@code byte[]} for
   * (large) utf8, {@code null} when the key is absent or JSON null. Unsupported column types fail
   * even when the key is absent, matching the Rust harness.
   */
  private static Object cell(Field field, JsonObject object) {
    ArrowType type = field.getType();
    boolean isInt64 =
        type instanceof ArrowType.Int
            && ((ArrowType.Int) type).getBitWidth() == 64
            && ((ArrowType.Int) type).getIsSigned();
    boolean isUtf8 = type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8;
    if (!isInt64 && !isUtf8) {
      throw new IllegalArgumentException(
          "field '" + field.getName() + "': arrow type " + type
              + " is not supported by the harness yet");
    }
    // Explicit JSON null is unset, as an absent key is (JsonFormat does the same on the proto path).
    if (!object.has(field.getName()) || object.get(field.getName()).isJsonNull()) {
      return null;
    }
    JsonElement json = object.get(field.getName());
    if (isInt64) {
      if (json.isJsonPrimitive() && json.getAsJsonPrimitive().isNumber()) {
        try {
          // Integral literals only, like serde_json's as_i64: "1.0" and "1e3" are rejected.
          return Long.parseLong(json.getAsString());
        } catch (NumberFormatException e) {
          // fall through to the shared error below
        }
      }
      throw new IllegalArgumentException(
          "field '" + field.getName() + "': expected int64, got " + json);
    }
    if (json.isJsonPrimitive() && json.getAsJsonPrimitive().isString()) {
      return json.getAsString().getBytes(StandardCharsets.UTF_8);
    }
    throw new IllegalArgumentException(
        "field '" + field.getName() + "': expected string, got " + json);
  }

  private static JsonObject parseObject(String recordJson) {
    JsonElement root;
    try {
      root = JsonParser.parseString(recordJson);
    } catch (JsonParseException e) {
      throw new IllegalArgumentException("record_json is not valid JSON: " + e.getMessage(), e);
    }
    if (!root.isJsonObject()) {
      throw new IllegalArgumentException("record_json must be a JSON object");
    }
    return root.getAsJsonObject();
  }
}
