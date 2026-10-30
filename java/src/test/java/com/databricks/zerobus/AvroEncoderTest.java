package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.junit.jupiter.api.Test;

/** Round-trip tests for {@link AvroEncoder}, covering the supported Avro types. */
class AvroEncoderTest {

  // One record exercising primitives, bytes, collections, union, enum, fixed, and logical types
  // (date, timestamp-millis/micros, local-timestamp, decimal, uuid).
  private static final String SCHEMA_JSON =
      "{\"type\":\"record\",\"name\":\"All\",\"fields\":["
          + "{\"name\":\"id\",\"type\":\"long\"},"
          + "{\"name\":\"name\",\"type\":\"string\"},"
          + "{\"name\":\"active\",\"type\":\"boolean\"},"
          + "{\"name\":\"score\",\"type\":\"double\"},"
          + "{\"name\":\"count\",\"type\":\"int\"},"
          + "{\"name\":\"payload\",\"type\":\"bytes\"},"
          + "{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},"
          + "{\"name\":\"attrs\",\"type\":{\"type\":\"map\",\"values\":\"long\"}},"
          + "{\"name\":\"nickname\",\"type\":[\"null\",\"string\"]},"
          + "{\"name\":\"status\",\"type\":{\"type\":\"enum\",\"name\":\"Status\",\"symbols\":[\"ON\",\"OFF\"]}},"
          + "{\"name\":\"code\",\"type\":{\"type\":\"fixed\",\"name\":\"Code\",\"size\":4}},"
          + "{\"name\":\"uid\",\"type\":{\"type\":\"string\",\"logicalType\":\"uuid\"}},"
          + "{\"name\":\"created\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},"
          + "{\"name\":\"ts\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},"
          + "{\"name\":\"ts_us\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}},"
          + "{\"name\":\"local_ts\",\"type\":{\"type\":\"long\",\"logicalType\":\"local-timestamp-millis\"}},"
          + "{\"name\":\"price\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":10,\"scale\":2}}"
          + "]}";

  private static final UUID UID = UUID.fromString("00000000-0000-0000-0000-0000000000ff");
  private static final Instant TS = Instant.ofEpochMilli(1_700_000_000_000L);
  private static final Instant TS_US = Instant.ofEpochSecond(1_700_000_000L, 123_000L);
  private static final LocalDateTime LOCAL_TS = LocalDateTime.of(2026, 10, 20, 1, 2, 3);

  private static GenericRecord sampleRecord(Schema schema, long id, String name) {
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", id);
    record.put("name", name);
    record.put("active", true);
    record.put("score", 3.5);
    record.put("count", 7);
    record.put("payload", ByteBuffer.wrap(new byte[] {1, 2, 3}));
    record.put("tags", Arrays.asList("a", "b"));
    Map<String, Long> attrs = new LinkedHashMap<>();
    attrs.put("x", 1L);
    record.put("attrs", attrs);
    record.put("nickname", null);
    record.put("status", new GenericData.EnumSymbol(schema.getField("status").schema(), "ON"));
    record.put(
        "code", new GenericData.Fixed(schema.getField("code").schema(), new byte[] {9, 8, 7, 6}));
    record.put("uid", UID);
    record.put("created", LocalDate.of(2026, 10, 20));
    record.put("ts", TS);
    record.put("ts_us", TS_US);
    record.put("local_ts", LOCAL_TS);
    record.put("price", new BigDecimal("12.34"));
    return record;
  }

  // Reuse the encoder's own data model so the read path applies the same logical-type conversions.
  private static GenericRecord decode(Schema schema, byte[] datum) throws Exception {
    GenericDatumReader<GenericRecord> reader =
        new GenericDatumReader<>(schema, schema, AvroEncoder.logicalTypeModel());
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(datum, null);
    return reader.read(null, decoder);
  }

  @Test
  void encodesAndRoundTripsAllTypes() throws Exception {
    Schema schema = new Schema.Parser().parse(SCHEMA_JSON);
    byte[] datum = new AvroEncoder(SCHEMA_JSON).encode(sampleRecord(schema, 42L, "alice"));
    assertTrue(datum.length > 0);

    GenericRecord out = decode(schema, datum);
    assertEquals(42L, out.get("id"));
    assertEquals("alice", out.get("name").toString());
    assertArrayEquals(new byte[] {1, 2, 3}, ((ByteBuffer) out.get("payload")).array());
    assertEquals("ON", out.get("status").toString());
    // Logical types round-trip to their java.time / BigDecimal / UUID forms.
    assertEquals(UID, out.get("uid"));
    assertEquals(LocalDate.of(2026, 10, 20), out.get("created"));
    assertEquals(TS, out.get("ts"));
    assertEquals(TS_US, out.get("ts_us"));
    assertEquals(LOCAL_TS, out.get("local_ts"));
    assertEquals(new BigDecimal("12.34"), out.get("price"));
  }

  @Test
  void encodeBatchEncodesEachRecordDistinctly() throws Exception {
    Schema schema = new Schema.Parser().parse(SCHEMA_JSON);
    List<byte[]> datums =
        new AvroEncoder(SCHEMA_JSON)
            .encodeBatch(
                Arrays.asList(sampleRecord(schema, 1L, "alice"), sampleRecord(schema, 2L, "bob")));
    assertEquals(2, datums.size());
    GenericRecord first = decode(schema, datums.get(0));
    GenericRecord second = decode(schema, datums.get(1));
    assertEquals(1L, first.get("id"));
    assertEquals("alice", first.get("name").toString());
    assertEquals(2L, second.get("id"));
    assertEquals("bob", second.get("name").toString());
  }

  @Test
  void encodeRejectsValueThatDoesNotMatchSchema() {
    Schema schema = new Schema.Parser().parse(SCHEMA_JSON);
    GenericRecord bad = sampleRecord(schema, 42L, "alice");
    bad.put("id", "not-a-long");
    assertThrows(NonRetriableException.class, () -> new AvroEncoder(SCHEMA_JSON).encode(bad));
  }

  // {a: long, b: string} — a record that fails on field "b" leaves "a"'s bytes in the reused
  // encoder.
  private static final String AB_SCHEMA =
      "{\"type\":\"record\",\"name\":\"AB\",\"fields\":["
          + "{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"string\"}]}";

  @Test
  void encoderIsDroppedAfterFailure() throws Exception {
    Schema schema = new Schema.Parser().parse(AB_SCHEMA);
    AvroEncoder encoder = new AvroEncoder(AB_SCHEMA);

    GenericRecord bad = new GenericData.Record(schema);
    bad.put("a", 11L);
    bad.put("b", 123); // not a string: fails after "a" is written
    assertThrows(NonRetriableException.class, () -> encoder.encode(bad));

    GenericRecord good = new GenericData.Record(schema);
    good.put("a", 22L);
    good.put("b", "ok");
    // After the failure the encoder is dropped, so this matches a fresh encoder (no leftover "a").
    assertArrayEquals(new AvroEncoder(AB_SCHEMA).encode(good), encoder.encode(good));
  }

  @Test
  void rejectsRecordWhoseSchemaFieldsAreReordered() {
    String reordered =
        "{\"type\":\"record\",\"name\":\"AB\",\"fields\":["
            + "{\"name\":\"b\",\"type\":\"string\"},{\"name\":\"a\",\"type\":\"long\"}]}";
    GenericRecord record = new GenericData.Record(new Schema.Parser().parse(reordered));
    record.put("a", 1L);
    record.put("b", "x");
    assertThrows(NonRetriableException.class, () -> new AvroEncoder(AB_SCHEMA).encode(record));
  }

  @Test
  void rejectsReorderedNestedRecord() throws Exception {
    String schemaJson =
        "{\"type\":\"record\",\"name\":\"Outer\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"long\"},"
            + "{\"name\":\"child\",\"type\":{\"type\":\"record\",\"name\":\"Child\","
            + "\"fields\":[{\"name\":\"x\",\"type\":\"int\"},{\"name\":\"y\",\"type\":\"int\"}]}}]}";
    // Same names, but the child's fields are reordered.
    String reorderedChild =
        "{\"type\":\"record\",\"name\":\"Child\","
            + "\"fields\":[{\"name\":\"y\",\"type\":\"int\"},{\"name\":\"x\",\"type\":\"int\"}]}";
    Schema outer = new Schema.Parser().parse(schemaJson);
    GenericRecord child = new GenericData.Record(new Schema.Parser().parse(reorderedChild));
    child.put("x", 1);
    child.put("y", 2);
    GenericRecord record = new GenericData.Record(outer);
    record.put("id", 1L);
    record.put("child", child);
    assertThrows(NonRetriableException.class, () -> new AvroEncoder(schemaJson).encode(record));
  }

  // A reordered Child built standalone, used inside a union and an array below.
  private static final String REORDERED_CHILD =
      "{\"type\":\"record\",\"name\":\"Child\","
          + "\"fields\":[{\"name\":\"y\",\"type\":\"int\"},{\"name\":\"x\",\"type\":\"int\"}]}";

  private static GenericRecord reorderedChild() {
    GenericRecord child = new GenericData.Record(new Schema.Parser().parse(REORDERED_CHILD));
    child.put("x", 1);
    child.put("y", 2);
    return child;
  }

  @Test
  void rejectsReorderedRecordInUnion() {
    String schemaJson =
        "{\"type\":\"record\",\"name\":\"Outer\",\"fields\":["
            + "{\"name\":\"child\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Child\","
            + "\"fields\":[{\"name\":\"x\",\"type\":\"int\"},{\"name\":\"y\",\"type\":\"int\"}]}]}]}";
    GenericRecord record = new GenericData.Record(new Schema.Parser().parse(schemaJson));
    record.put("child", reorderedChild());
    assertThrows(NonRetriableException.class, () -> new AvroEncoder(schemaJson).encode(record));
  }

  @Test
  void rejectsReorderedRecordInArray() {
    String schemaJson =
        "{\"type\":\"record\",\"name\":\"Outer\",\"fields\":["
            + "{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\","
            + "\"name\":\"Child\",\"fields\":["
            + "{\"name\":\"x\",\"type\":\"int\"},{\"name\":\"y\",\"type\":\"int\"}]}}}]}";
    GenericRecord record = new GenericData.Record(new Schema.Parser().parse(schemaJson));
    record.put("items", Arrays.asList(reorderedChild()));
    assertThrows(NonRetriableException.class, () -> new AvroEncoder(schemaJson).encode(record));
  }

  @Test
  void acceptsRecordSchemaDifferingOnlyInMetadata() throws Exception {
    // Same fields as AB_SCHEMA, but with extra metadata (doc, avro.java.string) the fingerprint
    // ignores — so it must be accepted, not rejected.
    String withMetadata =
        "{\"type\":\"record\",\"name\":\"AB\",\"doc\":\"d\",\"fields\":["
            + "{\"name\":\"a\",\"type\":\"long\"},"
            + "{\"name\":\"b\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}]}";
    GenericRecord record = new GenericData.Record(new Schema.Parser().parse(withMetadata));
    record.put("a", 1L);
    record.put("b", "x");
    assertTrue(new AvroEncoder(AB_SCHEMA).encode(record).length > 0);
  }

  @Test
  void constructorRejectsMalformedSchema() {
    assertThrows(NonRetriableException.class, () -> new AvroEncoder("{ not valid json"));
  }
}
