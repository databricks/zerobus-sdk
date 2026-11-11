package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import java.util.Collections;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Runs a proto stream against the in-process mock server through the native library. */
class NativeStreamStatsTest {

  private static final String TABLE = "test_catalog.test_schema.stats_table";

  @Test
  void protoStreamStatsFollowIngestFlushAndCloseThroughNativeLayer() throws Exception {
    assumeTrue(NativeLoader.isLoaded(), "Native library not available");
    String endpoint = NativeTestHelper.nativeTestStartStatsMockServer();
    NativeTestHelper.nativeTestInjectStatsScript(TABLE);

    DescriptorProto descriptor =
        DescriptorProto.newBuilder()
            .setName("StatsRow")
            .addField(
                FieldDescriptorProto.newBuilder()
                    .setName("id")
                    .setNumber(1)
                    .setType(FieldDescriptorProto.Type.TYPE_INT64)
                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
            .build();

    try (ZerobusSdk sdk = new ZerobusSdk(endpoint, "https://mock-uc.invalid")) {
      ZerobusProtoStream stream =
          sdk.streamBuilder()
              .table(TABLE)
              .headersProvider(staticHeaders())
              .compiledProto(descriptor)
              .build()
              .get(30, TimeUnit.SECONDS);

      assertEquals(0, stream.getStats().getRecordsInFlight());

      // The mock withholds its acknowledgment for 500 ms, so the records stay in flight.
      for (int i = 0; i < 3; i++) {
        stream.ingestRecordOffset(new byte[] {0x08, (byte) i});
      }
      StreamStats pending = stream.getStats();
      assertEquals(3, pending.getRecordsInFlight());
      assertFalse(pending.getLastAckedOffset().isPresent());

      stream.flush();
      StreamStats acked = stream.getStats();
      assertEquals(0, acked.getRecordsInFlight());
      assertEquals(OptionalLong.of(2), acked.getLastAckedOffset());
      assertEquals(0, acked.getReconnects());
      assertFalse(acked.getLastError().isPresent());

      stream.close();
      assertEquals(acked, stream.getStats());
    }
  }

  private static HeadersProvider staticHeaders() {
    return new HeadersProvider() {
      @Override
      public Map<String, String> getHeaders() {
        return Collections.singletonMap("Authorization", "Bearer test-token");
      }
    };
  }
}
