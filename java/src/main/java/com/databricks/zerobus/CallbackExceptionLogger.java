package com.databricks.zerobus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs exceptions thrown by user callbacks that the native layer invokes on SDK threads.
 *
 * <p>Called only from native code, which caches this class when the library loads.
 */
final class CallbackExceptionLogger {
  private static final Logger logger = LoggerFactory.getLogger(AckCallback.class);

  private CallbackExceptionLogger() {}

  static void log(String callback, Throwable failure) {
    logger.error("{} threw; later callbacks are still delivered", callback, failure);
  }
}
