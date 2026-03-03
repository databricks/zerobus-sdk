package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for the exception hierarchy: {@link ZerobusException} and {@link
 * NonRetriableException}.
 */
public class ExceptionTest {

  // ==================== ZerobusException Tests ====================

  @Test
  void testZerobusExceptionWithMessage() {
    String message = "Connection failed";
    ZerobusException exception = new ZerobusException(message);

    assertEquals(message, exception.getMessage());
    assertNull(exception.getCause());
  }

  @Test
  void testZerobusExceptionWithMessageAndCause() {
    String message = "Stream error";
    Throwable cause = new RuntimeException("Underlying cause");
    ZerobusException exception = new ZerobusException(message, cause);

    assertEquals(message, exception.getMessage());
    assertEquals(cause, exception.getCause());
  }

  @Test
  void testZerobusExceptionIsRetryable() {
    ZerobusException exception = new ZerobusException("Temporary failure");

    // ZerobusException represents retriable errors
    assertTrue(exception instanceof Exception);
    assertFalse(exception instanceof NonRetriableException);
  }

  @Test
  void testZerobusExceptionThrowAndCatch() {
    assertThrows(
        ZerobusException.class,
        () -> {
          throw new ZerobusException("Test exception");
        });
  }

  // ==================== NonRetriableException Tests ====================

  @Test
  void testNonRetriableExceptionWithMessage() {
    String message = "Invalid credentials";
    NonRetriableException exception = new NonRetriableException(message);

    assertEquals(message, exception.getMessage());
    assertNull(exception.getCause());
  }

  @Test
  void testNonRetriableExceptionWithMessageAndCause() {
    String message = "Table not found";
    Throwable cause = new IllegalArgumentException("Bad table name");
    NonRetriableException exception = new NonRetriableException(message, cause);

    assertEquals(message, exception.getMessage());
    assertEquals(cause, exception.getCause());
  }

  @Test
  void testNonRetriableExceptionExtendsZerobusException() {
    NonRetriableException exception = new NonRetriableException("Fatal error");

    assertTrue(exception instanceof ZerobusException);
  }

  @Test
  void testNonRetriableExceptionCanBeCaughtAsZerobus() {
    try {
      throw new NonRetriableException("Fatal");
    } catch (ZerobusException e) {
      // Should be caught here
      assertTrue(e instanceof NonRetriableException);
    }
  }

  @Test
  void testNonRetriableExceptionThrowAndCatch() {
    assertThrows(
        NonRetriableException.class,
        () -> {
          throw new NonRetriableException("Test non-retriable");
        });
  }

  // ==================== Exception Hierarchy Tests ====================

  @Test
  void testExceptionHierarchy() {
    ZerobusException retriable = new ZerobusException("Retriable");
    NonRetriableException nonRetriable = new NonRetriableException("Non-retriable");

    // Type checks
    assertTrue(retriable instanceof Exception);
    assertTrue(nonRetriable instanceof Exception);
    assertTrue(nonRetriable instanceof ZerobusException);

    // NonRetriableException is a subtype of ZerobusException
    assertFalse(retriable instanceof NonRetriableException);
    assertTrue(nonRetriable instanceof ZerobusException);
  }

  @Test
  void testDistinguishExceptionTypes() {
    // Simulate error handling logic that distinguishes between exception types
    Exception[] exceptions = {
      new ZerobusException("Network timeout"),
      new NonRetriableException("Invalid token"),
      new ZerobusException("Server busy"),
      new NonRetriableException("Missing table")
    };

    int retriableCount = 0;
    int nonRetriableCount = 0;

    for (Exception e : exceptions) {
      if (e instanceof NonRetriableException) {
        nonRetriableCount++;
      } else if (e instanceof ZerobusException) {
        retriableCount++;
      }
    }

    assertEquals(2, retriableCount);
    assertEquals(2, nonRetriableCount);
  }

  @Test
  void testExceptionChaining() {
    Throwable root = new IllegalStateException("Root cause");
    ZerobusException middle = new ZerobusException("Middle", root);
    NonRetriableException top = new NonRetriableException("Top level", middle);

    assertEquals("Top level", top.getMessage());
    assertEquals(middle, top.getCause());
    assertEquals(root, top.getCause().getCause());
  }
}
