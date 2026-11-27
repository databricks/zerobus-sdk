package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.test.table.TestTableRow.CityPopulationTableRow;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;

/**
 * Round-trips generated messages through the protobuf runtime on the test classpath. Runs on every
 * CI leg, so the protobuf-java 3.25 leg exercises serialization without a server.
 */
public class GeneratedMessageRuntimeTest {

  @Test
  void serializesAndParsesIngestRecordResponse() throws InvalidProtocolBufferException {
    IngestRecordResponse response =
        IngestRecordResponse.newBuilder().setDurabilityAckUpToOffset(42L).build();

    IngestRecordResponse parsed = IngestRecordResponse.parseFrom(response.toByteArray());

    assertEquals(42L, parsed.getDurabilityAckUpToOffset());
    assertEquals(response, parsed);
  }

  @Test
  void serializesAndParsesTestTableRow() throws InvalidProtocolBufferException {
    CityPopulationTableRow row =
        CityPopulationTableRow.newBuilder().setCityName("Zagreb").setPopulation(790_000).build();

    CityPopulationTableRow parsed = CityPopulationTableRow.parser().parseFrom(row.toByteArray());

    assertEquals("Zagreb", parsed.getCityName());
    assertEquals(790_000, parsed.getPopulation());
  }
}
