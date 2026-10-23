package com.databricks.zerobus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
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
  // Output buffer and encoder are reused across calls to avoid per-record allocation; safe because
  // streams are single-threaded. toByteArray() copies, so each result is independent of the reset.
  private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
  private BinaryEncoder encoder;

  AvroEncoder(String schemaJson) {
    Schema schema = new Schema.Parser().parse(schemaJson);
    this.writer = new GenericDatumWriter<>(schema, logicalTypeModel());
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
      throw new ZerobusException("Failed to encode Avro record: " + e.getMessage(), e);
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
}
