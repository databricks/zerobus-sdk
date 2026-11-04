package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class NativePanicGuardTest {

  @Test
  void nativePanicThrowsNonRetriableException() {
    assumeTrue(NativeLoader.isLoaded(), "Native library not available");

    NonRetriableException exception =
        assertThrows(NonRetriableException.class, NativeTestHelper::nativeTestPanic);
    assertTrue(exception.getMessage().contains("test panic"), exception.getMessage());
  }

  @Test
  void blockingCallFromAckCallbackThrowsIllegalStateException() {
    assumeTrue(NativeLoader.isLoaded(), "Native library not available");
    AtomicReference<Throwable> fromOnAck = new AtomicReference<>();
    AtomicReference<Throwable> fromOnError = new AtomicReference<>();

    AckCallback callback =
        new AckCallback() {
          @Override
          public void onAck(long offsetId) {
            fromOnAck.set(blockingCall());
          }

          @Override
          public void onError(long offsetId, String errorMessage) {
            fromOnError.set(blockingCall());
          }
        };

    assertEquals("OK", NativeTestHelper.nativeTestAckCallbackOnRuntimeWorker(callback));
    for (Throwable thrown : new Throwable[] {fromOnAck.get(), fromOnError.get()}) {
      assertInstanceOf(IllegalStateException.class, thrown);
      assertTrue(
          thrown.getMessage().contains("cannot be called from an AckCallback"),
          thrown.getMessage());
    }
  }

  @Test
  void escapedIllegalStateExceptionDoesNotDropLaterCallbacks() {
    assumeTrue(NativeLoader.isLoaded(), "Native library not available");
    AtomicInteger errors = new AtomicInteger();

    AckCallback callback =
        new AckCallback() {
          @Override
          public void onAck(long offsetId) {
            try {
              NativeTestHelper.nativeTestBlockOn();
            } catch (ZerobusException e) {
              // A typical catch: the unchecked IllegalStateException escapes onAck.
            }
          }

          @Override
          public void onError(long offsetId, String errorMessage) {
            errors.incrementAndGet();
          }
        };

    assertEquals("OK", NativeTestHelper.nativeTestAckCallbackOnRuntimeWorker(callback));
    assertEquals(1, errors.get(), "onError after an escaped exception was not delivered");
  }

  private static Throwable blockingCall() {
    try {
      NativeTestHelper.nativeTestBlockOn();
      return null;
    } catch (Throwable t) {
      return t;
    }
  }

  @Test
  void blockingCallFromSdkThreadThrowsInsteadOfAborting() {
    assumeTrue(NativeLoader.isLoaded(), "Native library not available");
    AtomicReference<Throwable> thrown = new AtomicReference<>();

    String result =
        NativeTestHelper.nativeTestRunOnRuntimeWorker(
            () -> {
              try {
                NativeTestHelper.nativeTestBlockOn();
              } catch (Throwable t) {
                thrown.set(t);
              }
            });

    assertEquals("OK", result);
    assertInstanceOf(NonRetriableException.class, thrown.get());
    assertTrue(
        thrown.get().getMessage().startsWith("Internal error in Zerobus native code"),
        thrown.get().getMessage());
  }
}
