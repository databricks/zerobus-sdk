package com.databricks.zerobus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

/**
 * Encodes Avro records to the bare binary datum the Zerobus service expects.
 *
 * <p>The writer schema is parsed once. The data model registers the standard logical-type
 * conversions, so date/time values may be supplied as {@code java.time} types, decimals as {@link
 * java.math.BigDecimal}, and uuids as {@link java.util.UUID} (or any of these as their raw
 * underlying primitive).
 *
 * <p>Not thread-safe: one encoder belongs to a single stream.
 */
final class AvroEncoder {

  private final GenericDatumWriter<Object> writer;
  // Buffer and encoder are reused across calls (streams are single-threaded).
  private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
  private BinaryEncoder encoder;

  AvroEncoder(String schemaJson) throws ZerobusException {
    Schema writerSchema;
    try {
      writerSchema = new Schema.Parser().parse(schemaJson);
    } catch (AvroRuntimeException e) { // SchemaParseException, AvroTypeException, ...
      throw new NonRetriableException("Invalid Avro writer schema: " + e.getMessage(), e);
    }
    this.writer = new CheckedDatumWriter(writerSchema, logicalTypeModel());
  }

  /** Encodes one record to an Avro binary datum. */
  byte[] encode(GenericRecord record) throws ZerobusException {
    try {
      buffer.reset();
      encoder = EncoderFactory.get().binaryEncoder(buffer, encoder);
      writer.write(record, encoder);
      encoder.flush();
      return buffer.toByteArray();
    } catch (IOException | RuntimeException e) {
      encoder = null; // drop: it may hold this record's partial bytes
      throw new NonRetriableException("Failed to encode Avro record: " + e.getMessage(), e);
    }
  }

  /** Encodes a batch of records to Avro binary datums. */
  List<byte[]> encodeBatch(Iterable<? extends GenericRecord> records) throws ZerobusException {
    List<byte[]> out = new ArrayList<>();
    for (GenericRecord record : records) {
      out.add(encode(record));
    }
    return out;
  }

  /** A data model with the standard logical-type conversions registered. */
  static GenericData logicalTypeModel() {
    GenericData model = new GenericData();
    model.addLogicalTypeConversion(new Conversions.DecimalConversion());
    model.addLogicalTypeConversion(new Conversions.UUIDConversion());
    model.addLogicalTypeConversion(new TimeConversions.DateConversion());
    model.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
    model.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
    model.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
    model.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
    model.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
    model.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
    return model;
  }

  /**
   * Verifies every record — nested ones included — against the schema it is written against. {@code
   * GenericDatumWriter} writes fields by position, so a record built from a reordered schema would
   * otherwise silently swap values. Schemas are compared by Avro parsing-canonical-form
   * fingerprint, which ignores metadata ({@code avro.java.string}, {@code connect.name}, docs,
   * defaults) but not field order or a differing namespace.
   */
  private static final class CheckedDatumWriter extends GenericDatumWriter<Object> {
    // writer record schema -> last record-schema instance already verified against it
    private final Map<Schema, Schema> lastMatched = new IdentityHashMap<>();

    CheckedDatumWriter(Schema schema, GenericData model) {
      super(schema, model);
    }

    @Override
    protected void writeRecord(Schema schema, Object datum, Encoder out) throws IOException {
      if (datum instanceof GenericContainer) {
        Schema actual = ((GenericContainer) datum).getSchema();
        if (actual != schema && lastMatched.get(schema) != actual) {
          if (SchemaNormalization.parsingFingerprint64(actual)
              != SchemaNormalization.parsingFingerprint64(schema)) {
            throw new AvroTypeException(
                "Record schema " + actual.getFullName() + " does not match the writer schema");
          }
          lastMatched.put(schema, actual);
        }
      }
      super.writeRecord(schema, datum, out);
    }
  }
}
