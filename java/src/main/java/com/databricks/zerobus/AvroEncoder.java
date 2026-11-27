package com.databricks.zerobus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
 * <p>An encoder built with the constructor reuses one buffer across calls and is not thread-safe:
 * it belongs to a single stream. One built with {@link #threadSafe(String)} encodes each call into
 * its own buffer, so a multiplexed stream can share it across threads; the parsed schema and the
 * datum writer are only read after construction (apart from a concurrent cache of verified record
 * schemas).
 */
final class AvroEncoder {

  private final GenericDatumWriter<Object> writer;
  // Reused across calls by a single-threaded stream; null when the encoder is shared by threads.
  private final Scratch reused;

  /** Creates an encoder for a single stream; not thread-safe. */
  AvroEncoder(String schemaJson) throws ZerobusException {
    this(schemaJson, true);
  }

  /** Creates an encoder that several threads can use at once. */
  static AvroEncoder threadSafe(String schemaJson) throws ZerobusException {
    return new AvroEncoder(schemaJson, false);
  }

  private AvroEncoder(String schemaJson, boolean reuseBuffer) throws ZerobusException {
    Schema writerSchema;
    try {
      writerSchema = new Schema.Parser().parse(schemaJson);
    } catch (AvroRuntimeException e) { // SchemaParseException, AvroTypeException, ...
      throw new NonRetriableException("Invalid Avro writer schema: " + e.getMessage(), e);
    }
    this.writer = new CheckedDatumWriter(writerSchema, logicalTypeModel());
    this.reused = reuseBuffer ? new Scratch() : null;
  }

  /** Encodes one record to an Avro binary datum. */
  byte[] encode(GenericRecord record) throws ZerobusException {
    return encode(record, scratch());
  }

  /** Encodes a batch of records to Avro binary datums. */
  List<byte[]> encodeBatch(Iterable<? extends GenericRecord> records) throws ZerobusException {
    // One scratch per call, even when threads share the encoder.
    Scratch scratch = scratch();
    List<byte[]> out = new ArrayList<>();
    for (GenericRecord record : records) {
      out.add(encode(record, scratch));
    }
    return out;
  }

  private Scratch scratch() {
    return reused != null ? reused : new Scratch();
  }

  private byte[] encode(GenericRecord record, Scratch scratch) throws ZerobusException {
    try {
      scratch.buffer.reset();
      scratch.encoder = EncoderFactory.get().binaryEncoder(scratch.buffer, scratch.encoder);
      writer.write(record, scratch.encoder);
      scratch.encoder.flush();
      return scratch.buffer.toByteArray();
    } catch (IOException | RuntimeException e) {
      // Drop the encoder: it may hold this record's partial bytes.
      scratch.encoder = null;
      throw new NonRetriableException("Failed to encode Avro record: " + e.getMessage(), e);
    }
  }

  /** A buffer and the binary encoder writing to it, reused for consecutive records. */
  private static final class Scratch {
    final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    BinaryEncoder encoder;
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
    // Writer record schema -> its fingerprint and the record-schema instances already verified
    // against it. Concurrent because multiplexed streams encode on several threads. Lookups pass
    // the writer's own schema instance, so equals() short-circuits on identity and the hash code is
    // cached by Schema.
    private final Map<Schema, VerifiedSchemas> verified = new ConcurrentHashMap<>();

    CheckedDatumWriter(Schema schema, GenericData model) {
      super(schema, model);
    }

    @Override
    protected void writeRecord(Schema schema, Object datum, Encoder out) throws IOException {
      if (datum instanceof GenericContainer) {
        Schema actual = ((GenericContainer) datum).getSchema();
        if (actual != schema) {
          VerifiedSchemas seen = verified.computeIfAbsent(schema, VerifiedSchemas::new);
          if (!seen.contains(actual)) {
            if (SchemaNormalization.parsingFingerprint64(actual) != seen.fingerprint) {
              throw new AvroTypeException(
                  "Record schema " + actual.getFullName() + " does not match the writer schema");
            }
            seen.add(actual);
          }
        }
      }
      super.writeRecord(schema, datum, out);
    }
  }

  /**
   * A writer record schema's fingerprint and the last few record-schema instances that matched it,
   * compared by identity. Several are kept because threads may build records from their own, equal
   * schema instances.
   */
  private static final class VerifiedSchemas {
    private static final int MAX_INSTANCES = 64;

    final long fingerprint;
    // Replaced on every add, so readers scan a stable array without locking.
    private volatile Schema[] instances = new Schema[0];

    VerifiedSchemas(Schema writerSchema) {
      this.fingerprint = SchemaNormalization.parsingFingerprint64(writerSchema);
    }

    boolean contains(Schema schema) {
      for (Schema instance : instances) {
        if (instance == schema) {
          return true;
        }
      }
      return false;
    }

    // Keeps the newest MAX_INSTANCES, dropping the oldest; a dropped instance is only rechecked.
    synchronized void add(Schema schema) {
      if (contains(schema)) {
        return;
      }
      Schema[] current = instances;
      int keep = Math.min(current.length, MAX_INSTANCES - 1);
      Schema[] next = new Schema[keep + 1];
      System.arraycopy(current, current.length - keep, next, 0, keep);
      next[keep] = schema;
      instances = next;
    }
  }
}
