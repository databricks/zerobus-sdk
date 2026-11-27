package com.databricks.zerobus;

import static com.databricks.zerobus.MockServerTestSupport.AVRO_SCHEMA;
import static com.databricks.zerobus.MockServerTestSupport.airQuality;
import static com.databricks.zerobus.MockServerTestSupport.avroRecord;
import static com.databricks.zerobus.MockServerTestSupport.awaitWriteCount;
import static com.databricks.zerobus.MockServerTestSupport.base64;
import static com.databricks.zerobus.MockServerTestSupport.batchSizes;
import static com.databricks.zerobus.MockServerTestSupport.encode;
import static com.databricks.zerobus.MockServerTestSupport.json;
import static com.databricks.zerobus.MockServerTestSupport.neverAcked;
import static com.databricks.zerobus.MockServerTestSupport.writeCount;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.test.table.AirQualityRow.AirQuality;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Runs ordinary streams of every format through the real native library, against an in-process mock
 * server that never acknowledges, so every record comes back through the unacked records.
 *
 * <p>Covers the native code that reads stream creation arguments and batch payloads and returns
 * unacked records and batches. Skipped when the native library is not available.
 */
@Timeout(value = 20, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class OrdinaryStreamNativeTest {

  // One mock server for the class; each test reads the write count before it ingests.
  private static String url;

  @BeforeAll
  static void startMockServer() throws Exception {
    MockServerTestSupport.assumeNativeLibrary();
    url = NativeTestHelper.nativeTestStartMockServer();
  }

  @Test
  void jsonRecordsCrossTheBridgeInBothDirections() throws Exception {
    long before = writeCount(url);
    List<String> records = Arrays.asList(json("json-0"), json("json-1"), json("json-2"));

    try (ZerobusSdk sdk = new ZerobusSdk(url, url)) {
      ZerobusJsonStream stream = neverAcked(sdk).json().build().join();
      stream.ingestRecordOffset(records.get(0));
      assertTrue(stream.ingestRecordsOffset(records.subList(1, 3)).isPresent());
      awaitWriteCount(url, before, records.size());

      assertThrows(ZerobusException.class, stream::close);
      assertEquals(records, stream.getUnackedRecords());
      assertEquals(Arrays.asList(1, 2), batchSizes(stream.getUnackedBatches()));
    }
  }

  @Test
  void protoRecordsCrossTheBridgeInBothDirections() throws Exception {
    long before = writeCount(url);
    List<AirQuality> records =
        Arrays.asList(airQuality("proto-0"), airQuality("proto-1"), airQuality("proto-2"));

    try (ZerobusSdk sdk = new ZerobusSdk(url, url)) {
      ZerobusProtoStream stream =
          neverAcked(sdk).compiledProto(AirQuality.getDescriptor().toProto()).build().join();
      stream.ingestRecordOffset(records.get(0));
      assertTrue(stream.ingestRecordsOffset(records.subList(1, 3)).isPresent());
      awaitWriteCount(url, before, records.size());

      assertThrows(ZerobusException.class, stream::close);
      assertEquals(records, stream.getUnackedRecords(AirQuality.parser()));
      assertEquals(Arrays.asList(1, 2), batchSizes(stream.getUnackedBatches()));
    }
  }

  @Test
  void avroRecordsCrossTheBridgeInBothDirections() throws Exception {
    long before = writeCount(url);
    List<GenericRecord> records =
        Arrays.asList(avroRecord("avro-0"), avroRecord("avro-1"), avroRecord("avro-2"));

    try (ZerobusSdk sdk = new ZerobusSdk(url, url)) {
      ZerobusAvroStream stream = neverAcked(sdk).avro(AVRO_SCHEMA.toString()).build().join();
      stream.ingestRecordOffset(records.get(0));
      assertTrue(stream.ingestRecordsOffset(records.subList(1, 3)).isPresent());
      awaitWriteCount(url, before, records.size());

      assertThrows(ZerobusException.class, stream::close);
      List<String> expected = new ArrayList<>();
      for (GenericRecord record : records) {
        expected.add(base64(encode(record)));
      }
      List<String> recovered = new ArrayList<>();
      for (byte[] bytes : stream.getUnackedRecords()) {
        recovered.add(base64(bytes));
      }
      assertEquals(expected, recovered);
      assertEquals(Arrays.asList(1, 2), batchSizes(stream.getUnackedBatches()));
    }
  }
}
