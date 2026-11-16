package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class NativeLogForwardingTest {

  private static final String CORE_LOGGER = NativeLogBridge.ROOT_LOGGER;
  private static final String NATIVE_TEST_LOGGER = CORE_LOGGER + ".native_log_test";
  private static final String FORWARDER_THREAD = "zerobus-native-log-forwarder";

  private Logger core;
  private Level previousLevel;
  private CapturingAppender appender;

  private static final class CapturingAppender extends AppenderBase<ILoggingEvent> {
    private final List<ILoggingEvent> events = new CopyOnWriteArrayList<>();
    private volatile ClassLoader forwarderContextLoader;

    @Override
    protected void append(ILoggingEvent event) {
      if (FORWARDER_THREAD.equals(event.getThreadName())) {
        forwarderContextLoader = Thread.currentThread().getContextClassLoader();
      }
      events.add(event);
    }
  }

  @BeforeEach
  void setUp() {
    assumeTrue(NativeLoader.isLoaded(), "Native library not available");
    core = (Logger) LoggerFactory.getLogger(CORE_LOGGER);
    previousLevel = core.getLevel();
    appender = new CapturingAppender();
    appender.start();
    core.addAppender(appender);
  }

  @AfterEach
  void tearDown() {
    if (core != null) {
      core.detachAppender(appender);
      core.setLevel(previousLevel);
      NativeLogBridge.refreshLevel();
    }
  }

  @Test
  void coreEventReachesSlf4jWithLoggerNameAndFields() throws InterruptedException {
    core.setLevel(Level.DEBUG);
    NativeLogBridge.refreshLevel();

    NativeTestHelper.nativeTestEmitLog(2, "connection failed");

    ILoggingEvent event = awaitMessage("connection failed");
    assertEquals(NATIVE_TEST_LOGGER, event.getLoggerName());
    assertEquals(Level.WARN, event.getLevel());
    assertEquals("connection failed table=main.default.t1", event.getFormattedMessage());
  }

  @Test
  void forwarderThreadIsNamedAndUsesBridgeContextClassLoader() throws InterruptedException {
    core.setLevel(Level.DEBUG);
    NativeLogBridge.refreshLevel();

    NativeTestHelper.nativeTestEmitLog(2, "thread probe");

    ILoggingEvent event = awaitMessage("thread probe");
    assertEquals(FORWARDER_THREAD, event.getThreadName());
    assertSame(NativeLogBridge.class.getClassLoader(), appender.forwarderContextLoader);
  }

  @Test
  void eventBelowLoggerLevelIsNotForwarded() throws InterruptedException {
    core.setLevel(Level.WARN);
    NativeLogBridge.refreshLevel();

    NativeTestHelper.nativeTestEmitLog(4, "debug suppressed");
    NativeTestHelper.nativeTestEmitLog(1, "error sentinel");

    // The forwarder is FIFO, so a forwarded DEBUG would already be in the list here.
    awaitMessage("error sentinel");
    assertTrue(
        messages().stream().noneMatch(m -> m.startsWith("debug suppressed")),
        "DEBUG event forwarded although the logger level is WARN");
  }

  @Test
  void loggerNamesFollowRustModulePaths() {
    assertEquals(
        "com.databricks.zerobus.core", NativeLogBridge.loggerName("databricks_zerobus_ingest_sdk"));
    assertEquals(
        "com.databricks.zerobus.core.stream.arrow",
        NativeLogBridge.loggerName("databricks_zerobus_ingest_sdk::stream::arrow"));
    assertEquals(
        "com.databricks.zerobus.core.forwarder",
        NativeLogBridge.loggerName("zerobus_jni::forwarder"));
  }

  private ILoggingEvent awaitMessage(String prefix) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      for (ILoggingEvent event : events()) {
        if (event.getFormattedMessage().startsWith(prefix)) {
          return event;
        }
      }
      Thread.sleep(10);
    }
    fail("Timed out waiting for forwarded event: " + prefix);
    return null;
  }

  private List<String> messages() {
    return events().stream().map(ILoggingEvent::getFormattedMessage).collect(Collectors.toList());
  }

  private List<ILoggingEvent> events() {
    return new ArrayList<>(appender.events);
  }
}
