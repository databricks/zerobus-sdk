package com.databricks.zerobus;

/**
 * Base exception class for all Ingest API related errors. This allows clients to catch all Ingest
 * API specific exceptions with a single catch block.
 */
public class ZerobusException extends Exception {

  /**
   * Constructs a new ZerobusException with the specified detail message.
   *
   * @param message the detail message
   */
  public ZerobusException(String message) {
    super(message);
  }

  /**
   * Constructs a new ZerobusException with the specified detail message and cause.
   *
   * @param message the detail message
   * @param cause the cause of the exception
   */
  public ZerobusException(String message, Throwable cause) {
    super(message, cause);
  }
}
