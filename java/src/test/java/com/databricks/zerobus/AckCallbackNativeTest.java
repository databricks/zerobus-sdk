package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class AckCallbackNativeTest {

  @Test
  void throwingCallbackDoesNotSuppressLaterCallbacks() {
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

    assertEquals("OK", NativeTestHelper.nativeTestAckCallbackExceptions(callback, 3));
    assertEquals(3, acks.get());
    assertEquals(1, errors.get());
  }
}
