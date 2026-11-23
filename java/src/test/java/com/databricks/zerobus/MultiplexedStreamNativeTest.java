package com.databricks.zerobus;

import static com.databricks.zerobus.MockServerTestSupport.PROVIDER;
import static com.databricks.zerobus.MockServerTestSupport.WAIT_SECONDS;
import static com.databricks.zerobus.MockServerTestSupport.airQuality;
import static com.databricks.zerobus.MockServerTestSupport.awaitWriteCount;
import static com.databricks.zerobus.MockServerTestSupport.base64;
import static com.databricks.zerobus.MockServerTestSupport.json;
import static com.databricks.zerobus.MockServerTestSupport.neverAcked;
import static com.databricks.zerobus.MockServerTestSupport.sorted;
import static com.databricks.zerobus.MockServerTestSupport.writeCount;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.test.table.AirQualityRow.AirQuality;
import java.nio.charset.StandardCharsets;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Exercises multiplexed streams through the real native library, against an in-process mock server,
 * so they run without credentials.
 *
 * <p>The Rust SDK tests cover multiplexing itself. These tests cover what the Java bridge adds: the
 * native method signatures and arguments, typed record encoding and decoding, message IDs in both
 * directions, exception mapping, ack callbacks, and the Java lifecycle around the native handle.
 * They are skipped when the native library is not available.
 */
@Timeout(value = 20, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class MultiplexedStreamNativeTest {

  // Nothing listens here, so a stream open fails fast without network access.
  private static final String UNREACHABLE_ENDPOINT = "http://127.0.0.1:1";
  private static final String UNREACHABLE_UC = "http://127.0.0.1:1";
  private static final String TABLE = "catalog.schema.table";

  /** Bits of a message ID above the sub-stream offset, holding the sub-stream index. */
  private static final int STREAM_INDEX_SHIFT = 58;

  // One mock server for the class. Scripted tests use their own table, and tests that count
  // writes read the count before they ingest.
  private static String url;

  @BeforeAll
  static void startMockServer() throws Exception {
    MockServerTestSupport.assumeNativeLibrary();
    url = NativeTestHelper.nativeTestStartMockServer();
  }

  // ==================== Creation ====================

  @Test
  void createFailuresKeepTheirRetryClass() throws Exception {
    try (ZerobusSdk sdk = new ZerobusSdk(UNREACHABLE_ENDPOINT, UNREACHABLE_UC)) {
      // Java checks only the stream count; the inflight budget is a core rule. A valid
      // configuration reaches the native layer and fails only on the retryable open.
      assertRetryable(
          failureOf(
              unreachableBase(sdk)
                  .maxInflightRecords(4)
                  .compiledProto(AirQuality.getDescriptor().toProto())
                  .multiplexed(4)
                  .ackCallback(new RecordingCallback())
                  .build()));
      assertRetryable(
          failureOf(
              unreachableBase(sdk)
                  .compiledProto(AirQuality.getDescriptor().toProto())
                  .multiplexed(1)
                  .build()));
    }
  }

  // ==================== Records, in and back out ====================
  //
  // The mock server never acknowledges these records, so close() fails and every record comes back
  // through getUnackedRecords(). Two sub-streams receive the single records alternately and each
  // batch whole.

  @Test
  void protoRecordsCrossTheBridgeInBothDirections() throws Exception {
    long before = writeCount(url);
    List<AirQuality> records = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
      records.add(airQuality("proto-" + i));
    }

    try (ZerobusSdk sdk = new ZerobusSdk(url, url)) {
      ZerobusMultiplexedProtoStream stream =
          neverAcked(sdk)
              .compiledProto(AirQuality.getDescriptor().toProto())
              .multiplexed(2)
              .build()
              .join();
      stream.ingestRecord(records.get(0));
      stream.ingestRecord(records.get(1).toByteArray());
      assertTrue(stream.ingestRecords(records.subList(2, 5)).isPresent());
      assertTrue(
          stream
              .ingestRecords(
                  Arrays.asList(records.get(5).toByteArray(), records.get(6).toByteArray()))
              .isPresent());
      assertFalse(stream.ingestRecords(Collections.<byte[]>emptyList()).isPresent());
      assertFalse(stream.ingestRecords(Collections.<AirQuality>emptyList()).isPresent());
      awaitWriteCount(url, before, records.size());

      assertThrows(ZerobusException.class, stream::close);
      // Each batch comes back whole and in order; batches have no order across sub-streams.
      assertEquals(
          grouped(records.stream().map(r -> base64(r.toByteArray())), 1, 1, 3, 2),
          batchContents(stream.getUnackedBatches(), MockServerTestSupport::base64));
      assertEquals(
          sorted(records.stream().map(AirQuality::toString)),
          sorted(stream.getUnackedRecords(AirQuality.parser()).stream().map(AirQuality::toString)));
    }
  }

  @Test
  void jsonRecordsCrossTheBridgeInBothDirections() throws Exception {
    long before = writeCount(url);
    List<String> records = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
      records.add(json("json-" + i));
    }

    try (ZerobusSdk sdk = new ZerobusSdk(url, url)) {
      ZerobusMultiplexedJsonStream stream = neverAcked(sdk).json().multiplexed(2).build().join();
      long id = stream.ingestRecord(records.get(0));
      stream.ingestRecord("json-1", MockServerTestSupport::json);
      assertTrue(stream.ingestRecords(records.subList(2, 5)).isPresent());
      assertTrue(
          stream
              .ingestRecords(Arrays.asList("json-5", "json-6"), MockServerTestSupport::json)
              .isPresent());
      assertFalse(stream.ingestRecords(Collections.<String>emptyList()).isPresent());
      awaitWriteCount(url, before, records.size());

      // On an open stream, getUnackedBatches() closes it first so the result is complete; reading
      // again returns the same batches.
      List<List<String>> expected = grouped(records.stream(), 1, 1, 3, 2);
      assertEquals(
          expected, batchContents(stream.getUnackedBatches(), MultiplexedStreamNativeTest::utf8));
      assertTrue(stream.isClosed());
      assertEquals(
          expected, batchContents(stream.getUnackedBatches(), MultiplexedStreamNativeTest::utf8));

      // The flush inside close() still fails, and the records stay recoverable afterwards.
      assertThrows(ZerobusException.class, stream::close);
      assertEquals(
          sorted(records.stream().map(json -> "decoded " + json)),
          sorted(stream.getUnackedRecords(json -> "decoded " + json).stream()));

      // After close() the native stream is gone: calls throw without reaching it, and a second
      // close() does nothing.
      assertThrows(NonRetriableException.class, () -> stream.ingestRecord(json("after-close")));
      assertThrows(
          NonRetriableException.class, () -> stream.ingestRecords(Collections.<String>emptyList()));
      assertThrows(NonRetriableException.class, stream::flush);
      assertThrows(NonRetriableException.class, () -> stream.waitForMessageId(id));
      stream.close();
      assertEquals(sorted(records.stream()), sorted(stream.getUnackedRecords().stream()));
    }
  }

  // ==================== Message IDs ====================

  @Test
  void messageIdsCrossTheBridgeInBothDirections() throws Exception {
    // The most sub-streams a multiplexed stream can have; those from 32 up return negative IDs.
    final int streamCount = 64;
    String table = "catalog.schema.message_ids";
    // Each sub-stream acknowledges two single records and one batch, and never its fourth request.
    NativeTestHelper.nativeTestScriptMockServer(url, table, 3, null);
    RecordingCallback callback = new RecordingCallback();
    Set<Long> ackedIds = new HashSet<>();
    Map<Integer, Long> batchIdPerStream = new HashMap<>();
    List<AirQuality> unacked = new ArrayList<>();
    long unackedId = 0;

    try (ZerobusSdk sdk = new ZerobusSdk(url, url)) {
      ZerobusMultiplexedProtoStream stream =
          sdk.streamBuilder()
              .table(table)
              .headersProvider(PROVIDER)
              .recovery(false)
              .flushTimeoutMs(500)
              .compiledProto(AirQuality.getDescriptor().toProto())
              .multiplexed(streamCount)
              .ackCallback(callback)
              .build()
              .join();
      for (int i = 0; i < 2 * streamCount; i++) {
        ackedIds.add(stream.ingestRecord(airQuality("id-" + i)));
      }
      for (int i = 0; i < streamCount; i++) {
        Optional<Long> id =
            stream.ingestRecords(
                Arrays.asList(airQuality("batch-" + i + "-a"), airQuality("batch-" + i + "-b")));
        assertTrue(id.isPresent());
        ackedIds.add(id.get());
        batchIdPerStream.put(streamIndex(id.get()), id.get());
      }
      for (int i = 0; i < streamCount; i++) {
        AirQuality record = airQuality("unacked-" + i);
        unackedId = stream.ingestRecord(record);
        unacked.add(record);
      }
      assertEquals(3 * streamCount, ackedIds.size());
      assertEquals(streamCount, batchIdPerStream.size());
      assertTrue(ackedIds.stream().anyMatch(id -> id < 0), "expected negative message IDs");

      // Java to native: the index and offset bits reach the native layer intact. Every sub-stream's
      // batch ID is acknowledged, negative ones included, and a record after it never is.
      for (long id : batchIdPerStream.values()) {
        stream.waitForMessageId(id);
      }
      long neverAckedId = unackedId;
      assertThrows(ZerobusException.class, () -> stream.waitForMessageId(neverAckedId));
      // Native to Java: the callback receives exactly the acknowledged IDs, once each.
      callback.awaitAcked(ackedIds);

      assertThrows(ZerobusException.class, stream::close);
      assertEquals(
          sorted(unacked.stream().map(AirQuality::toString)),
          sorted(stream.getUnackedRecords(AirQuality.parser()).stream().map(AirQuality::toString)));
    }
    assertTrue(callback.errors.isEmpty(), "unexpected ACK errors: " + callback.errors);
  }

  // ==================== Errors ====================

  @Test
  void serverErrorReachesOnErrorAndKeepsTheRecord() throws Exception {
    String table = "catalog.schema.rejected";
    // The first record is acknowledged and the second rejected.
    NativeTestHelper.nativeTestScriptMockServer(url, table, 1, "mock rejected the record");
    RecordingCallback callback = new RecordingCallback();
    AirQuality accepted = airQuality("accepted");
    AirQuality rejected = airQuality("rejected");

    try (ZerobusSdk sdk = new ZerobusSdk(url, url)) {
      ZerobusMultiplexedProtoStream stream =
          sdk.streamBuilder()
              .table(table)
              .headersProvider(PROVIDER)
              .recovery(false)
              .compiledProto(AirQuality.getDescriptor().toProto())
              .multiplexed(1)
              .ackCallback(callback)
              .build()
              .join();
      long acceptedId = stream.ingestRecord(accepted);
      long rejectedId = stream.ingestRecord(rejected);

      // INVALID_ARGUMENT from the server is not retryable.
      assertThrows(NonRetriableException.class, stream::flush);
      // onError receives the rejected record's own message ID.
      String error = callback.awaitError(rejectedId);
      assertTrue(error.contains("mock rejected the record"), error);
      callback.awaitAcked(Collections.singleton(acceptedId));
      assertEquals(Collections.singleton(rejectedId), callback.errors.keySet());

      assertTrue(stream.isClosed());
      // An empty batch sends nothing but still reports the stored failure.
      assertThrows(
          NonRetriableException.class,
          () -> stream.ingestRecords(Collections.<AirQuality>emptyList()));
      // The native stream still exists, so this wait reaches it and reports the failure.
      assertThrows(ZerobusException.class, () -> stream.waitForMessageId(rejectedId));
      assertThrows(ZerobusException.class, stream::close);
      assertEquals(
          Collections.singletonList(rejected), stream.getUnackedRecords(AirQuality.parser()));
    }
  }

  // ==================== Callbacks ====================

  @Test
  void onAckCannotBlockOrStopDelivery() throws Exception {
    long before = writeCount(url);
    String table = "catalog.schema.callback";
    NativeTestHelper.nativeTestScriptMockServer(url, table, 5, null);
    AtomicReference<ZerobusMultiplexedProtoStream> streamRef = new AtomicReference<>();
    AtomicBoolean attempted = new AtomicBoolean();
    Map<String, Throwable> outcomes = new ConcurrentHashMap<>();
    RecordingCallback recorder = new RecordingCallback();
    // On its first call, the callback tries blocking stream methods; every call then throws.
    AckCallback callback =
        new AckCallback() {
          @Override
          public void onAck(long messageId) {
            recorder.onAck(messageId);
            ZerobusMultiplexedProtoStream stream = streamRef.get();
            if (stream != null && attempted.compareAndSet(false, true)) {
              outcomes.put(
                  "ingestRecord", outcome(() -> stream.ingestRecord(airQuality("inside"))));
              outcomes.put("waitForMessageId", outcome(() -> stream.waitForMessageId(messageId)));
              outcomes.put("close", outcome(stream::close));
            }
            throw new RuntimeException("deliberate failure in onAck");
          }

          @Override
          public void onError(long messageId, String errorMessage) {
            recorder.onError(messageId, errorMessage);
          }
        };
    Set<Long> ingestedIds = new HashSet<>();

    try (ZerobusSdk sdk = new ZerobusSdk(url, url)) {
      ZerobusMultiplexedProtoStream stream =
          sdk.streamBuilder()
              .table(table)
              .headersProvider(PROVIDER)
              .compiledProto(AirQuality.getDescriptor().toProto())
              .multiplexed(2)
              .ackCallback(callback)
              .build()
              .join();
      streamRef.set(stream);
      for (int i = 0; i < 10; i++) {
        ingestedIds.add(stream.ingestRecord(airQuality("outside-" + i)));
      }
      stream.flush();
      // Every record is still acknowledged after the callback threw.
      recorder.awaitAcked(ingestedIds);
      stream.close();
    }

    assertEquals(
        new HashSet<>(Arrays.asList("ingestRecord", "waitForMessageId", "close")),
        outcomes.keySet());
    outcomes.forEach(
        (method, thrown) ->
            assertInstanceOf(
                IllegalStateException.class, thrown, method + " inside onAck: " + thrown));
    // The ingest call rejected inside onAck sent nothing.
    assertEquals(before + ingestedIds.size(), writeCount(url));
    assertTrue(recorder.errors.isEmpty(), "unexpected ACK errors: " + recorder.errors);
  }

  // ==================== Concurrency ====================

  @Test
  void threadsShareOneStream() throws Exception {
    final int threads = 8;
    final int recordsPerThread = 50;
    String table = "catalog.schema.shared";
    // Acknowledge every request any sub-stream can receive.
    NativeTestHelper.nativeTestScriptMockServer(url, table, threads * recordsPerThread, null);
    RecordingCallback callback = new RecordingCallback();
    Set<Long> ingestedIds = ConcurrentHashMap.newKeySet();

    try (ZerobusSdk sdk = new ZerobusSdk(url, url)) {
      ZerobusMultiplexedProtoStream stream =
          sdk.streamBuilder()
              .table(table)
              .headersProvider(PROVIDER)
              .compiledProto(AirQuality.getDescriptor().toProto())
              .multiplexed(4)
              .ackCallback(callback)
              .build()
              .join();

      // Every thread ingests in a loop and then flushes, all at the same time, while some also wait
      // on message IDs, so ingest, wait, and flush overlap on the same stream.
      List<Throwable> failures =
          runConcurrently(
              threads,
              thread -> {
                for (int i = 0; i < recordsPerThread; i++) {
                  long id = stream.ingestRecord(airQuality("thread-" + thread + "-" + i));
                  assertTrue(ingestedIds.add(id), "duplicate message ID " + id);
                  if (thread % 2 == 0 && i % 10 == 9) {
                    stream.waitForMessageId(id);
                  }
                }
                stream.flush();
                assertFalse(stream.isClosed());
              });
      assertEquals(Collections.emptyList(), failures);
      assertEquals(threads * recordsPerThread, ingestedIds.size());
      callback.awaitAcked(ingestedIds);

      stream.close();
      assertTrue(stream.getUnackedRecords().isEmpty());
    }
    assertTrue(callback.errors.isEmpty(), "unexpected ACK errors: " + callback.errors);
  }

  @Test
  void closeWaitsForACallInsideNative() throws Exception {
    byte[] record = airQuality("parked").toByteArray();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    // The native layer reads the batch through get(); this batch parks that read inside the
    // ingest call until the test releases it.
    List<byte[]> parkedBatch =
        new AbstractList<byte[]>() {
          @Override
          public byte[] get(int index) {
            entered.countDown();
            try {
              release.await(WAIT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            return record;
          }

          @Override
          public int size() {
            return 1;
          }
        };

    try (ZerobusSdk sdk = new ZerobusSdk(url, url)) {
      ZerobusMultiplexedProtoStream stream =
          neverAcked(sdk)
              .compiledProto(AirQuality.getDescriptor().toProto())
              .multiplexed(1)
              .build()
              .join();
      AtomicReference<Throwable> ingestOutcome = new AtomicReference<>();
      AtomicReference<Throwable> closeOutcome = new AtomicReference<>();
      AtomicReference<Throwable> secondCloseOutcome = new AtomicReference<>();
      Thread ingester =
          new Thread(() -> ingestOutcome.set(outcome(() -> stream.ingestRecords(parkedBatch))));
      ingester.start();
      assertTrue(entered.await(WAIT_SECONDS, TimeUnit.SECONDS), "ingest did not reach native");

      // A blocked close() is WAITING, while native code reports RUNNABLE. Polling a stream method
      // instead would queue behind close() and block.
      Thread closer = new Thread(() -> closeOutcome.set(outcome(stream::close)));
      closer.start();
      awaitWaiting(closer);
      Thread secondCloser = new Thread(() -> secondCloseOutcome.set(outcome(stream::close)));
      secondCloser.start();
      awaitWaiting(secondCloser);

      release.countDown();
      for (Thread thread : Arrays.asList(ingester, closer, secondCloser)) {
        thread.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
        assertFalse(thread.isAlive(), thread + " did not return");
      }

      // The ingest call finds the stream closed; each close() returns or fails like a close.
      assertInstanceOf(ZerobusException.class, ingestOutcome.get());
      for (Throwable closeFailure : Arrays.asList(closeOutcome.get(), secondCloseOutcome.get())) {
        assertTrue(
            closeFailure == null || closeFailure instanceof ZerobusException,
            "unexpected close failure: " + closeFailure);
      }
      assertTrue(stream.isClosed());
    }
  }

  // ==================== Helpers ====================

  /**
   * Runs {@code body} on {@code threads} threads at once, passing each its index, and returns what
   * the threads threw.
   */
  private static List<Throwable> runConcurrently(int threads, ThreadBody body) throws Exception {
    List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
    CountDownLatch start = new CountDownLatch(1);
    List<Thread> workers = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      final int thread = t;
      Thread worker =
          new Thread(
              () -> {
                try {
                  start.await();
                  body.run(thread);
                } catch (Throwable e) {
                  failures.add(e);
                }
              });
      worker.start();
      workers.add(worker);
    }
    start.countDown();
    for (Thread worker : workers) {
      worker.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
      assertFalse(worker.isAlive(), "worker thread did not finish");
    }
    return failures;
  }

  /**
   * Splits {@code records} into consecutive batches of {@code sizes}, in the order {@link
   * #batchContents} returns them.
   */
  private static List<List<String>> grouped(Stream<String> records, int... sizes) {
    List<String> flat = records.collect(Collectors.toList());
    List<List<String>> batches = new ArrayList<>();
    int from = 0;
    for (int size : sizes) {
      batches.add(new ArrayList<>(flat.subList(from, from + size)));
      from += size;
    }
    assertEquals(flat.size(), from, "sizes must cover every record");
    return inAnyBatchOrder(batches);
  }

  /**
   * Decodes each batch's records in order. Batches have no order across sub-streams, so they are
   * sorted.
   */
  private static List<List<String>> batchContents(List<EncodedBatch> batches, RecordDecoder decoder)
      throws Exception {
    List<List<String>> contents = new ArrayList<>();
    for (EncodedBatch batch : batches) {
      List<String> records = new ArrayList<>();
      for (byte[] bytes : batch.getRecords()) {
        records.add(decoder.decode(bytes));
      }
      contents.add(records);
    }
    return inAnyBatchOrder(contents);
  }

  private static List<List<String>> inAnyBatchOrder(List<List<String>> batches) {
    List<List<String>> sorted = new ArrayList<>(batches);
    sorted.sort(Comparator.comparing(List::toString));
    return sorted;
  }

  private static String utf8(byte[] bytes) {
    return new String(bytes, StandardCharsets.UTF_8);
  }

  @FunctionalInterface
  private interface RecordDecoder {
    String decode(byte[] bytes) throws Exception;
  }

  @FunctionalInterface
  private interface ThreadBody {
    void run(int thread) throws Exception;
  }

  /** Waits until {@code thread} parks on a lock. */
  private static void awaitWaiting(Thread thread) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
    while (thread.getState() != Thread.State.WAITING && System.nanoTime() < deadline) {
      Thread.sleep(1);
    }
    assertEquals(Thread.State.WAITING, thread.getState(), "close() did not wait for the ingest");
  }

  private static void assertRetryable(Throwable failure) {
    assertTrue(failure instanceof ZerobusException, "unexpected failure: " + failure);
    assertFalse(failure instanceof NonRetriableException, "unexpected failure: " + failure);
  }

  /** A builder for {@link #UNREACHABLE_ENDPOINT} that fails on the first connection attempt. */
  private static StreamBuilder unreachableBase(ZerobusSdk sdk) {
    return sdk.streamBuilder()
        .table(TABLE)
        .headersProvider(PROVIDER)
        .recovery(false)
        .recoveryTimeoutMs(5_000);
  }

  private static Throwable failureOf(CompletableFuture<?> future) {
    ExecutionException ex =
        assertThrows(ExecutionException.class, () -> future.get(60, TimeUnit.SECONDS));
    return ex.getCause();
  }

  /** Returns the sub-stream index packed into the top bits of a message ID. */
  private static int streamIndex(long messageId) {
    return (int) (messageId >>> STREAM_INDEX_SHIFT);
  }

  /** Runs {@code action} and returns what it threw, or {@code null} if it returned normally. */
  private static Throwable outcome(ThrowingRunnable action) {
    try {
      action.run();
      return null;
    } catch (Throwable t) {
      return t;
    }
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  /** Records every message ID the stream acknowledges or fails. */
  private static final class RecordingCallback implements AckCallback {
    final Set<Long> acked = ConcurrentHashMap.newKeySet();
    final Map<Long, String> errors = new ConcurrentHashMap<>();

    @Override
    public void onAck(long messageId) {
      if (!acked.add(messageId)) {
        errors.put(messageId, "duplicate ACK");
      }
    }

    @Override
    public void onError(long messageId, String errorMessage) {
      errors.put(messageId, errorMessage);
    }

    /** Waits until exactly {@code expected} has been acknowledged. */
    void awaitAcked(Set<Long> expected) throws InterruptedException {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
      while (acked.size() < expected.size() && System.nanoTime() < deadline) {
        Thread.sleep(10);
      }
      assertEquals(expected, new HashSet<>(acked), "ACKed message IDs must match ingested IDs");
    }

    /** Waits for {@code onError} on {@code messageId} and returns its message. */
    String awaitError(long messageId) throws InterruptedException {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
      while (!errors.containsKey(messageId) && System.nanoTime() < deadline) {
        Thread.sleep(10);
      }
      assertTrue(errors.containsKey(messageId), "no onError for " + messageId + ": " + errors);
      return errors.get(messageId);
    }
  }
}
