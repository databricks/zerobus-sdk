package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class StreamStatsTest {

  @Test
  void noAckAndNoErrorMapToEmptyOptionals() {
    StreamStats stats = StreamStats.empty();
    assertEquals(0, stats.getRecordsInFlight());
    assertFalse(stats.getLastAckedOffset().isPresent());
    assertEquals(0, stats.getReconnects());
    assertFalse(stats.getLastError().isPresent());
  }

  @Test
  void acknowledgedOffsetIsExposedWhenPresent() {
    StreamStats stats = new StreamStats(3, 7, 2, null, false, 0);
    assertEquals(OptionalLong.of(7), stats.getLastAckedOffset());
    assertEquals(3, stats.getRecordsInFlight());
    assertEquals(2, stats.getReconnects());
  }

  @Test
  void lastErrorCarriesMessageRetryabilityAndTimestamp() {
    StreamStats stats = new StreamStats(0, -1, 1, "connection dropped", true, 1_700_000_000_123L);
    StreamStats.LastError error = stats.getLastError().orElseThrow(AssertionError::new);
    assertEquals("connection dropped", error.getMessage());
    assertTrue(error.isRetryable());
    assertEquals(Instant.ofEpochMilli(1_700_000_000_123L), error.getTimestamp());
  }

  @Test
  void equalSnapshotsAreEqualAndHashEqually() {
    StreamStats a = new StreamStats(2, 5, 0, "boom", false, 42L);
    StreamStats b = new StreamStats(2, 5, 0, "boom", false, 42L);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertEquals(a.toString(), b.toString());
  }

  @Test
  void differingFieldsAreNotEqual() {
    StreamStats base = new StreamStats(2, 5, 0, null, false, 0);
    assertNotEquals(base, new StreamStats(3, 5, 0, null, false, 0));
    assertNotEquals(base, new StreamStats(2, 6, 0, null, false, 0));
    assertNotEquals(base, new StreamStats(2, 5, 1, null, false, 0));
    assertNotEquals(base, new StreamStats(2, 5, 0, "x", false, 0));
  }
}
