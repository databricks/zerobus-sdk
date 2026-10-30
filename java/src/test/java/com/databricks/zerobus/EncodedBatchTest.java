package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import org.junit.jupiter.api.Test;

/** Tests for {@link RecordFormat} and {@link EncodedBatch}. */
class EncodedBatchTest {

  @Test
  void formatRoundTripsThroughWireCode() {
    for (RecordFormat format : RecordFormat.values()) {
      EncodedBatch batch = new EncodedBatch(Collections.emptyList(), format.code());
      assertEquals(format, batch.getFormat());
      assertEquals(format == RecordFormat.JSON, batch.isJson());
    }
  }

  @Test
  void getRecordsAsStringsRejectsNonJson() {
    assertThrows(
        IllegalStateException.class,
        () -> new EncodedBatch(Collections.emptyList(), RecordFormat.AVRO).getRecordsAsStrings());
    assertThrows(
        IllegalStateException.class,
        () -> new EncodedBatch(Collections.emptyList(), RecordFormat.PROTO).getRecordsAsStrings());
  }

  @Test
  void rejectsNullFormat() {
    assertThrows(
        NullPointerException.class,
        () -> new EncodedBatch(Collections.emptyList(), (RecordFormat) null));
  }
}
