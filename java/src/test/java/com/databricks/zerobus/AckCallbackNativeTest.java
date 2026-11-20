package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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

  @Test
  void thrownExceptionIsNotRetainedAfterCallback() throws InterruptedException {
    assumeTrue(NativeLoader.isLoaded(), "Native library not available");
    assumeExplicitGcCollects();
    AtomicReference<WeakReference<Throwable>> thrown = new AtomicReference<>();

    AckCallback callback =
        new AckCallback() {
          @Override
          public void onAck(long offsetId) {
            IllegalStateException failure = new IllegalStateException("onAck failed");
            thrown.set(new WeakReference<>(failure));
            throw failure;
          }

          @Override
          public void onError(long offsetId, String errorMessage) {}
        };

    assertEquals("OK", NativeTestHelper.nativeTestAckCallbackExceptions(callback, 1));
    assertTrue(
        isCollected(thrown.get()),
        "the thrown exception is still referenced by native code after the callback returned");
  }

  @Test
  void errorMessageIsNotRetainedAfterCallback() throws InterruptedException {
    assumeTrue(NativeLoader.isLoaded(), "Native library not available");
    assumeExplicitGcCollects();
    AtomicReference<WeakReference<String>> message = new AtomicReference<>();

    AckCallback callback =
        new AckCallback() {
          @Override
          public void onAck(long offsetId) {}

          @Override
          public void onError(long offsetId, String errorMessage) {
            message.set(new WeakReference<>(errorMessage));
          }
        };

    assertEquals("OK", NativeTestHelper.nativeTestAckCallbackExceptions(callback, 0));
    assertTrue(
        isCollected(message.get()),
        "the onError message is still referenced by native code after the callback returned");
  }

  @Test
  void exceptionThatCannotBeLoggedDoesNotSuppressLaterCallbacks() {
    assumeTrue(NativeLoader.isLoaded(), "Native library not available");
    AtomicInteger acks = new AtomicInteger();
    AtomicInteger errors = new AtomicInteger();

    AckCallback callback =
        new AckCallback() {
          @Override
          public void onAck(long offsetId) {
            acks.incrementAndGet();
            throw new UnprintableException();
          }

          @Override
          public void onError(long offsetId, String errorMessage) {
            errors.incrementAndGet();
          }
        };

    assertEquals("OK", NativeTestHelper.nativeTestAckCallbackExceptions(callback, 3));
    assertEquals(3, acks.get());
    assertEquals(1, errors.get());
  }

  /**
   * Skips the test when {@code System.gc()} does not collect garbage (for example under {@code
   * -XX:+DisableExplicitGC}), so the leak tests cannot fail for that reason.
   */
  private static void assumeExplicitGcCollects() throws InterruptedException {
    WeakReference<Object> control = new WeakReference<>(new Object());
    assumeTrue(isCollected(control), "System.gc() does not collect garbage in this JVM");
  }

  /** Forces garbage collections until {@code ref} is cleared, for up to about a second. */
  private static boolean isCollected(WeakReference<?> ref) throws InterruptedException {
    for (int i = 0; i < 50 && ref.get() != null; i++) {
      System.gc();
      Thread.sleep(20);
    }
    return ref.get() == null;
  }

  /** An exception that cannot be logged: printing its stack trace calls {@code toString()}. */
  private static final class UnprintableException extends RuntimeException {
    @Override
    public String toString() {
      throw new IllegalStateException("toString failed");
    }
  }
}
