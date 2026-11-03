package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class AckCallbackNativeTest {

  @Test
  void throwingCallbackIsLoggedAndDoesNotSuppressLaterCallbacks() {
    assumeTrue(NativeLoader.isLoaded(), "Native library not available");
    AtomicInteger acks = new AtomicInteger();
    AtomicInteger errors = new AtomicInteger();

    AckCallback callback =
        new AckCallback() {
          @Override
          public void onAck(long offsetId) {
            acks.incrementAndGet();
            throw new IllegalStateException("onAck failed");
          }

          @Override
          public void onError(long offsetId, String errorMessage) {
            errors.incrementAndGet();
            throw new IllegalStateException("onError failed");
          }
        };

    PrintStream originalErr = System.err;
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    String result;
    System.setErr(new PrintStream(captured, true));
    try {
      result = NativeTestHelper.nativeTestAckCallbackExceptions(callback, 3);
    } finally {
      System.setErr(originalErr);
    }

    assertEquals("OK", result);
    assertEquals(3, acks.get());
    assertEquals(1, errors.get());
    String log = captured.toString();
    assertTrue(log.contains("AckCallback.onAck threw"), log);
    assertTrue(log.contains("java.lang.IllegalStateException: onAck failed"), log);
    assertTrue(log.contains("AckCallback.onError threw"), log);
  }
}
