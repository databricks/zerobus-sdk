package com.databricks.zerobus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for Zerobus streams providing common native method declarations and lifecycle
 * management.
 *
 * <p>This class is not intended to be used directly. Use {@link ZerobusJsonStream} for JSON
 * ingestion, {@link ZerobusProtoStream} for Protocol Buffer ingestion, or the deprecated {@link
 * ZerobusStream} for backward compatibility.
 *
 * <h3>Resource Management</h3>
 *
 * <p>Streams hold native resources that are not automatically released by the garbage collector.
 * You <b>must</b> call {@link #close()} when done to avoid native memory leaks. Use
 * try-with-resources for automatic cleanup:
 *
 * <pre>{@code
 * try (ZerobusJsonStream stream = sdk.streamBuilder()
 *         .table("catalog.schema.table")
 *         .oauth(clientId, clientSecret)
 *         .json()
 *         .build()
 *         .join()) {
 *     stream.ingestRecordOffset(record);
 * }
 * }</pre>
 *
 * <h3>Thread Safety</h3>
 *
 * <p>Ingest, acknowledgment, flush, close and recovery methods are <b>not thread-safe</b>. Use them
 * from a single thread, and do not call them on the same stream concurrently from multiple threads.
 *
 * <p>{@link #getStats()} and {@link #isClosed()} may be called from any thread. {@code getStats()}
 * does not wait for ingest or acknowledgment work. {@code isClosed()} can wait while an ingest call
 * is blocked waiting for capacity. Both may wait briefly while {@code close()} releases the native
 * handle.
 */
abstract class BaseZerobusStream implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(BaseZerobusStream.class);

  // Ensure native library is loaded.
  static {
    NativeLoader.ensureLoaded();
  }

  // Native handle to the Rust stream object.
  protected volatile long nativeHandle;
  // Read-locked by getStats() and isClosed(), write-locked while the native handle is destroyed.
  private final ReentrantReadWriteLock handleLock = new ReentrantReadWriteLock();
  private volatile StreamStats finalStats;

  // Stream properties.
  protected final String tableName;
  protected final StreamConfigurationOptions options;

  // Cached unacked records (populated on close for use in recreateStream).
  protected volatile List<byte[]> cachedUnackedRecords;
  protected volatile List<EncodedBatch> cachedUnackedBatches;

  /**
   * Creates a new BaseZerobusStream.
   *
   * <p>The record format is fixed natively at stream creation, so the ingest paths do not carry it.
   *
   * @param nativeHandle the native handle from JNI
   * @param tableName the table name
   * @param options the stream configuration options
   */
  protected BaseZerobusStream(
      long nativeHandle, String tableName, StreamConfigurationOptions options) {
    this.nativeHandle = nativeHandle;
    this.tableName = tableName;
    this.options = options;
  }

  /**
   * Ingests one pre-encoded record and returns its offset. Shared by the typed stream subclasses;
   * the native layer wraps the bytes according to the stream's record format.
   *
   * @param payload the encoded record bytes
   * @return the offset ID assigned to this record
   * @throws ZerobusException if the stream is closed or an error occurs
   */
  protected final long ingestEncoded(byte[] payload) throws ZerobusException {
    ensureOpen();
    return nativeIngestRecordOffset(nativeHandle, payload);
  }

  /**
   * Ingests a batch of pre-encoded records and returns the batch offset (empty if the list is
   * empty).
   *
   * @param payloads the encoded record byte arrays
   * @return the batch offset ID, or empty if {@code payloads} is empty
   * @throws ZerobusException if the stream is closed or an error occurs
   */
  protected final java.util.Optional<Long> ingestEncoded(List<byte[]> payloads)
      throws ZerobusException {
    if (payloads.isEmpty()) {
      return java.util.Optional.empty();
    }
    ensureOpen();
    return java.util.Optional.of(nativeIngestRecordsOffset(nativeHandle, payloads));
  }

  /**
   * Waits for a specific offset to be acknowledged by the server.
   *
   * <p>Use this when you need to confirm a specific record before continuing. Because the
   * acknowledgment watermark is monotonic, passing the <b>last</b> offset from an ingest loop
   * confirms every prior record too. For confirming a whole batch, prefer {@link #flush()}; for
   * non-blocking progress tracking, register an {@link AckCallback}. (Calling this after every
   * record in a tight loop limits throughput to one record per round-trip.)
   *
   * @param offset the offset to wait for (typically the last offset returned by an ingest loop)
   * @throws ZerobusException if an error occurs or the wait times out
   */
  public void waitForOffset(long offset) throws ZerobusException {
    ensureOpen();
    nativeWaitForOffset(nativeHandle, offset);
  }

  /**
   * Flushes the stream, waiting for all queued records to be acknowledged.
   *
   * <p>This is the idiomatic way to confirm durability: ingest in a loop, then call {@code flush()}
   * once (after a bounded batch, or periodically for a long-running stream). It returns once
   * everything queued so far is acknowledged.
   *
   * @throws ZerobusException if an error occurs or the flush times out
   */
  public void flush() throws ZerobusException {
    ensureOpen();
    nativeFlush(nativeHandle);
    logger.info("All records have been flushed");
  }

  /**
   * Closes the stream, flushing all pending records first.
   *
   * <p>After closing, unacknowledged records can still be retrieved via subclass methods for use in
   * stream recreation.
   *
   * @throws ZerobusException if an error occurs during close
   */
  @Override
  public void close() throws ZerobusException {
    long handle = nativeHandle;
    if (handle != 0) {
      ZerobusException closeFailure = null;
      try {
        // Close the stream first (flushes pending records).
        nativeClose(handle);
      } catch (ZerobusException e) {
        // Closing marks the Rust stream closed even when flushing fails. Keep going so its
        // unacknowledged records can be recovered before the native handle is destroyed.
        closeFailure = e;
      }

      // Cache unacked records before destroying the handle (for recreateStream)
      List<byte[]> unackedRecords;
      List<EncodedBatch> unackedBatches;
      try {
        unackedRecords = nativeGetUnackedRecords(handle);
        unackedBatches = nativeGetUnackedBatches(handle);
      } catch (ZerobusException cacheFailure) {
        // Retain the native handle so callers can retry recovery instead of reporting an empty
        // result and silently losing data.
        if (closeFailure != null) {
          closeFailure.addSuppressed(cacheFailure);
          throw closeFailure;
        }
        throw cacheFailure;
      }
      cachedUnackedRecords = unackedRecords;
      cachedUnackedBatches = unackedBatches;

      // Recovery data is safely owned by Java; the native stream can now be destroyed.
      releaseNativeHandle(handle);
      logger.info("Stream closed");

      if (closeFailure != null) {
        throw closeFailure;
      }
    }
  }

  /**
   * Returns the cached unacknowledged records as raw bytes.
   *
   * <p>This method returns records that were cached when the stream was closed. It is used
   * internally by recreateStream.
   *
   * @return a list of unacknowledged records as raw bytes, or empty list if none
   */
  protected List<byte[]> getCachedUnackedRecords() {
    return cachedUnackedRecords != null ? cachedUnackedRecords : new ArrayList<>();
  }

  /**
   * Returns the cached unacknowledged batches.
   *
   * <p>This method returns batches that were cached when the stream was closed. It is used
   * internally by recreateStream.
   *
   * @return a list of unacknowledged batches, or empty list if none
   */
  protected List<EncodedBatch> getCachedUnackedBatches() {
    return cachedUnackedBatches != null ? cachedUnackedBatches : new ArrayList<>();
  }

  /**
   * Ensures recovery batches are owned by Java and releases any retained native source stream.
   *
   * <p>A failed {@link #close()} can leave the closed Rust stream alive when its recovery data
   * could not be copied. Recreation calls this method to retry that copy before creating a
   * replacement stream.
   */
  final List<EncodedBatch> cacheAndReleaseUnackedBatches() throws ZerobusException {
    long handle = nativeHandle;
    if (handle == 0) {
      return getCachedUnackedBatches();
    }

    List<EncodedBatch> batches = nativeGetUnackedBatches(handle);
    List<byte[]> records = new ArrayList<>();
    for (EncodedBatch batch : batches) {
      records.addAll(batch.getRecords());
    }
    cachedUnackedBatches = batches;
    cachedUnackedRecords = records;
    releaseNativeHandle(handle);
    return batches;
  }

  /** Returns Java-owned recovery records, releasing any retained native source stream. */
  final List<byte[]> cacheAndReleaseUnackedRecords() throws ZerobusException {
    cacheAndReleaseUnackedBatches();
    return getCachedUnackedRecords();
  }

  /** Destroys a replacement stream whose recovery replay failed, without flushing it again. */
  final void discardFailedRecreation() {
    long handle = nativeHandle;
    if (handle != 0) {
      releaseNativeHandle(handle);
    }
  }

  /**
   * Returns a snapshot of the stream's counters.
   *
   * <p>Reads in-memory counters and never waits for the stream's ingest or acknowledgment work, so
   * it can be called from any thread, including while another thread is blocked in {@code
   * ingestRecordOffset} or {@code flush}. After {@link #close()}, returns the snapshot taken when
   * the stream closed.
   *
   * @return a snapshot of the stream's counters
   */
  public StreamStats getStats() {
    handleLock.readLock().lock();
    try {
      long handle = nativeHandle;
      if (handle == 0) {
        StreamStats last = finalStats;
        return last != null ? last : StreamStats.empty();
      }
      return nativeGetStats(handle);
    } finally {
      handleLock.readLock().unlock();
    }
  }

  /**
   * Captures the final counters and destroys the native handle. Runs under the write lock, so it
   * waits for in-flight {@code getStats()} and {@code isClosed()} reads, and destroys the handle at
   * most once even if two closes race.
   */
  private void releaseNativeHandle(long handle) {
    handleLock.writeLock().lock();
    try {
      if (nativeHandle != handle) {
        return;
      }
      try {
        finalStats = snapshotOrEmpty(handle);
      } finally {
        nativeHandle = 0;
        nativeDestroy(handle);
      }
    } finally {
      handleLock.writeLock().unlock();
    }
  }

  private StreamStats snapshotOrEmpty(long handle) {
    try {
      return nativeGetStats(handle);
    } catch (Exception e) {
      return StreamStats.empty();
    }
  }

  /**
   * Returns whether the stream is closed.
   *
   * @return true if the stream is closed
   */
  public boolean isClosed() {
    handleLock.readLock().lock();
    try {
      long handle = nativeHandle;
      return handle == 0 || nativeIsClosed(handle);
    } finally {
      handleLock.readLock().unlock();
    }
  }

  /**
   * Returns the table name for this stream.
   *
   * @return the table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Returns the stream configuration options.
   *
   * @return the stream configuration options
   */
  public StreamConfigurationOptions getOptions() {
    return options;
  }

  /**
   * Ensures the stream is open.
   *
   * @throws ZerobusException if the stream is closed
   */
  protected void ensureOpen() throws ZerobusException {
    if (nativeHandle == 0) {
      throw new ZerobusException("Stream is closed");
    }
    if (nativeIsClosed(nativeHandle)) {
      throw new ZerobusException("Stream is closed");
    }
  }

  // ==================== Native methods implemented in Rust ====================

  private static native void nativeDestroy(long handle);

  private native StreamStats nativeGetStats(long handle);

  protected native CompletableFuture<Void> nativeIngestRecord(long handle, byte[] payload);

  private native long nativeIngestRecordOffset(long handle, byte[] payload);

  private native long nativeIngestRecordsOffset(long handle, List<byte[]> payloads);

  private native void nativeWaitForOffset(long handle, long offset);

  private native void nativeFlush(long handle);

  private native void nativeClose(long handle) throws ZerobusException;

  private native boolean nativeIsClosed(long handle);

  protected native List<byte[]> nativeGetUnackedRecords(long handle) throws ZerobusException;

  protected native List<EncodedBatch> nativeGetUnackedBatches(long handle) throws ZerobusException;
}
