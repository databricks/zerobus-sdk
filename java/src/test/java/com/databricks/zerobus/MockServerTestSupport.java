package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.databricks.test.table.AirQualityRow.AirQuality;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

/** Records and helpers shared by the native tests that run streams against the mock server. */
final class MockServerTestSupport {

  static final HeadersProvider PROVIDER =
      () -> Collections.singletonMap("authorization", "Bearer token");

  /** How long to wait for records or ACKs; below the test classes' 20-second timeout. */
  static final long WAIT_SECONDS = 10;

  static final Schema AVRO_SCHEMA =
      new Schema.Parser()
          .parse(
              "{\"type\":\"record\",\"name\":\"AirQuality\",\"fields\":["
                  + "{\"name\":\"device_name\",\"type\":\"string\"},"
                  + "{\"name\":\"temp\",\"type\":\"int\"},"
                  + "{\"name\":\"humidity\",\"type\":\"long\"}]}");

  private MockServerTestSupport() {}

  /** Skips the calling test when the native library is not available. */
  static void assumeNativeLibrary() {
    boolean available;
    try {
      NativeLoader.ensureLoaded();
      available = true;
    } catch (UnsatisfiedLinkError | ExceptionInInitializerError e) {
      available = false;
    }
    assumeTrue(available, "Native library not available");
  }

  /** A builder for a table the mock server never acknowledges, so records stay unacked. */
  static StreamBuilder neverAcked(ZerobusSdk sdk) {
    return sdk.streamBuilder()
        .table("catalog.schema.never_acked")
        .headersProvider(PROVIDER)
        .recovery(false)
        .flushTimeoutMs(500);
  }

  /** Returns how many records the mock server has received, from every test that shares it. */
  static long writeCount(String url) throws Exception {
    return NativeTestHelper.nativeTestMockServerWriteCount(url);
  }

  /**
   * Waits until the mock server has received exactly {@code added} records after {@code baseline},
   * the {@link #writeCount} read before the test ingested.
   */
  static void awaitWriteCount(String url, long baseline, long added) throws Exception {
    long expected = baseline + added;
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
    while (writeCount(url) < expected && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(expected, writeCount(url));
  }

  static AirQuality airQuality(String deviceName) {
    return AirQuality.newBuilder().setDeviceName(deviceName).setTemp(21).setHumidity(55).build();
  }

  static String json(String deviceName) {
    return "{\"device_name\": \"" + deviceName + "\", \"temp\": 21, \"humidity\": 55}";
  }

  static GenericRecord avroRecord(String deviceName) {
    GenericRecord record = new GenericData.Record(AVRO_SCHEMA);
    record.put("device_name", deviceName);
    record.put("temp", 21);
    record.put("humidity", 55L);
    return record;
  }

  static byte[] encode(GenericRecord record) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<GenericRecord>(AVRO_SCHEMA).write(record, encoder);
    encoder.flush();
    return out.toByteArray();
  }

  static String base64(byte[] bytes) {
    return Base64.getEncoder().encodeToString(bytes);
  }

  static List<String> sorted(Stream<String> values) {
    return values.sorted().collect(Collectors.toList());
  }

  static List<Integer> batchSizes(List<EncodedBatch> batches) {
    return batches.stream().map(b -> b.getRecords().size()).sorted().collect(Collectors.toList());
  }
}
