package com.databricks.zerobus;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Forwards log events from the native Rust core to SLF4J.
 *
 * <p>Events are logged under {@value #ROOT_LOGGER}, followed by the Rust module path with {@code
 * ::} replaced by {@code .}. The crate prefix is dropped, so an event from {@code
 * databricks_zerobus_ingest_sdk::stream::arrow} is logged as {@code
 * com.databricks.zerobus.core.stream.arrow}. The level of {@value #ROOT_LOGGER} decides which
 * events the native side forwards; set it to DEBUG to see core debug output.
 *
 * <p>The native side reads the level when the library loads, when an {@link ZerobusSdk} is created,
 * and at JVM shutdown (when forwarding is turned off).
 */
final class NativeLogBridge {
  static final String ROOT_LOGGER = "com.databricks.zerobus.core";

  private static final String[] CRATE_PREFIXES = {"databricks_zerobus_ingest_sdk", "zerobus_jni"};

  private static final int LEVEL_OFF = 0;
  private static final int LEVEL_ERROR = 1;
  private static final int LEVEL_WARN = 2;
  private static final int LEVEL_INFO = 3;
  private static final int LEVEL_DEBUG = 4;
  private static final int LEVEL_TRACE = 5;

  private static final ConcurrentMap<String, Logger> LOGGERS = new ConcurrentHashMap<>();

  private NativeLogBridge() {
    // Utility class.
  }

  /** Pushes the current level and turns forwarding off at JVM shutdown. */
  static void install() {
    try {
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(() -> nativeSetLogLevel(LEVEL_OFF), "zerobus-native-log-shutdown"));
      refreshLevel();
    } catch (RuntimeException e) {
      // Logging is best effort; the SDK works without it.
    }
  }

  /** Reads the effective level of {@value #ROOT_LOGGER} and pushes it to the native side. */
  static void refreshLevel() {
    try {
      nativeSetLogLevel(levelOf(LoggerFactory.getLogger(ROOT_LOGGER)));
    } catch (RuntimeException e) {
      // Keep the previous level if SLF4J cannot report one.
    }
  }

  /**
   * Called on the native forwarder thread. Must not throw, since an exception would stay pending on
   * that thread.
   */
  static void log(int level, String target, String message) {
    try {
      Logger logger =
          LOGGERS.computeIfAbsent(
              target == null ? "" : target, t -> LoggerFactory.getLogger(loggerName(t)));
      String text = message == null ? "" : message;
      switch (level) {
        case LEVEL_ERROR:
          logger.error(text);
          break;
        case LEVEL_WARN:
          logger.warn(text);
          break;
        case LEVEL_INFO:
          logger.info(text);
          break;
        case LEVEL_DEBUG:
          logger.debug(text);
          break;
        case LEVEL_TRACE:
          logger.trace(text);
          break;
        default:
          break;
      }
    } catch (RuntimeException e) {
      // A failing appender must not break the forwarder.
    }
  }

  static String loggerName(String target) {
    String path = target.replace("::", ".");
    for (String crate : CRATE_PREFIXES) {
      if (path.equals(crate)) {
        return ROOT_LOGGER;
      }
      if (path.startsWith(crate + ".")) {
        return ROOT_LOGGER + "." + path.substring(crate.length() + 1);
      }
    }
    return path.isEmpty() ? ROOT_LOGGER : ROOT_LOGGER + "." + path;
  }

  private static int levelOf(Logger logger) {
    if (logger.isTraceEnabled()) {
      return LEVEL_TRACE;
    }
    if (logger.isDebugEnabled()) {
      return LEVEL_DEBUG;
    }
    if (logger.isInfoEnabled()) {
      return LEVEL_INFO;
    }
    if (logger.isWarnEnabled()) {
      return LEVEL_WARN;
    }
    if (logger.isErrorEnabled()) {
      return LEVEL_ERROR;
    }
    return LEVEL_OFF;
  }

  private static native void nativeSetLogLevel(int level);
}
