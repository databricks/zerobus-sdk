package com.databricks.zerobus;

/**
 * An exception that indicates a non-retriable error has occurred. This is used to signal that
 * stream creation or recovery should not be retried.
 */
public class NonRetriableException extends ZerobusException {

  /**
   * Constructs a new NonRetriableException with the specified detail message.
   *
   * @param message the detail message
   */
  public NonRetriableException(String message) {
    super(message);
  }

  /**
   * Constructs a new NonRetriableException with the specified detail message and cause.
   *
   * @param message the detail message
   * @param cause the cause of the exception
   */
  public NonRetriableException(String message, Throwable cause) {
    super(message, cause);
  }
}
