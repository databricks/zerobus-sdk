package com.databricks.zerobus;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for multiplexed streams (Beta), providing common native method declarations and
 * lifecycle management.
 *
 * <p>This class is not intended to be used directly. Use {@link ZerobusMultiplexedJsonStream},
 * {@link ZerobusMultiplexedProtoStream}, or {@link ZerobusMultiplexedAvroStream}, created with
 * {@code multiplexed(int)} on the matching {@link StreamBuilder} format sub-builder.
 *
 * <p>Its {@code ingestRecord} and {@code ingestRecords} return a message ID ({@code long} or {@code
 * Optional<Long>}), unlike the deprecated {@code ZerobusStream.ingestRecord}, which returns a
 * {@code CompletableFuture<Void>}.
 *
 * <p>A multiplexed stream distributes ingestion round-robin across a fixed set of sub-streams to
 * raise aggregate throughput when one stream is the bottleneck. Records keep their order within a
 * sub-stream, but there is no global record, message ID, acknowledgment, or callback order across
 * sub-streams.
 *
 * <p>See {@link StreamBuilder.MultiplexedStreamBuilder} for message IDs, failures, resource
 * management, and thread safety.
 */
abstract class BaseZerobusMultiplexedStream implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(BaseZerobusMultiplexedStream.class);

  // Ensure native library is loaded.
  static {
    NativeLoader.ensureLoaded();
  }

  /** Wraps a native multiplexed stream handle in its typed stream class. */
  @FunctionalInterface
  interface Factory<S> {
    S create(long nativeHandle, String tableName, StreamConfigurationOptions options);
  }

  // Native handle to the Rust multiplexed stream object.
  private volatile long nativeHandle;

  // Guards the native handle's lifetime: every native call holds the read lock, so calls run
  // concurrently, while freeing the handle takes the write lock and waits for them to return.
  // The native layer serializes close and recovery against ingest itself.
  private final ReentrantReadWriteLock handleLock = new ReentrantReadWriteLock();
  private final Lock readLock = handleLock.readLock();
  private final Lock writeLock = handleLock.writeLock();

  // Stream properties.
  protected final String tableName;
  protected final StreamConfigurationOptions options;

  // Unacked batches, cached on close so they remain available after the native handle is freed.
  private volatile List<EncodedBatch> cachedUnackedBatches;

  /**
   * Creates a new BaseZerobusMultiplexedStream.
   *
   * <p>The record format is fixed natively at stream creation, so the ingest paths do not carry it.
   *
   * @param nativeHandle the native handle from JNI
   * @param tableName the table name
   * @param options the stream configuration options
   */
  protected BaseZerobusMultiplexedStream(
      long nativeHandle, String tableName, StreamConfigurationOptions options) {
    this.nativeHandle = nativeHandle;
    this.tableName = tableName;
    this.options = options;
  }

  /**
   * Ingests one pre-encoded record into the next sub-stream and returns its message ID. Shared by
   * the typed stream subclasses; the native layer wraps the bytes according to the stream's record
   * format.
   *
   * @param payload the encoded record bytes
   * @return the message ID assigned to this record
   * @throws ZerobusException if the stream is closed or an error occurs
   */
  protected final long ingestEncoded(byte[] payload) throws ZerobusException {
    long handle = lockOpenHandle();
    try {
      return nativeIngestRecord(handle, payload);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Ingests a batch of pre-encoded records into one sub-stream and returns the batch's message ID
   * (empty if the list is empty).
   *
   * @param payloads the encoded record byte arrays
   * @return the batch message ID, or empty if {@code payloads} is empty
   * @throws ZerobusException if the stream is closed, also for an empty batch, or an error occurs
   */
  protected final Optional<Long> ingestEncoded(List<byte[]> payloads) throws ZerobusException {
    long handle = lockOpenHandle();
    try {
      // Empty batches go to the native stream too, which sends nothing but reports a terminal
      // failure the same way as for a non-empty batch.
      long messageId = nativeIngestRecords(handle, payloads);
      return payloads.isEmpty() ? Optional.empty() : Optional.of(messageId);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Waits until the record or batch behind a message ID is acknowledged by the server.
   *
   * <p>Use this when a specific record must be confirmed before continuing. Unlike offsets, message
   * IDs are not ordered, so acknowledging one says nothing about the others. To confirm everything
   * ingested so far, call {@link #flush()}; for non-blocking progress tracking, register an {@link
   * AckCallback}. (Calling this after every record in a tight loop limits throughput to one record
   * per round-trip.)
   *
   * @param messageId a message ID returned by this stream's ingest methods
   * @throws ZerobusException if an error occurs or the wait times out
   */
  public void waitForMessageId(long messageId) throws ZerobusException {
    long handle = lockOpenHandle();
    try {
      nativeWaitForMessageId(handle, messageId);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Flushes every sub-stream, waiting for all queued records to be acknowledged.
   *
   * <p>This is the idiomatic way to confirm durability: ingest in a loop, then call {@code flush()}
   * once (after a bounded batch, or periodically for a long-running stream).
   *
   * @throws ZerobusException if an error occurs or the flush times out
   */
  public void flush() throws ZerobusException {
    long handle = lockOpenHandle();
    try {
      nativeFlush(handle);
    } finally {
      readLock.unlock();
    }
    logger.info("All records have been flushed");
  }

  /**
   * Closes every sub-stream, flushing all pending records first.
   *
   * <p>When this returns, all callbacks have run, unless it waited longer than the callback wait
   * time (up to 5 seconds; sub-streams wait in parallel; not configurable in Java). To confirm that
   * every record was acknowledged, rely on {@link #flush()} and {@code getUnackedRecords()}, not on
   * counting callbacks.
   *
   * <p>After closing, unacknowledged records can still be retrieved with {@code
   * getUnackedRecords()} or {@link #getUnackedBatches()}. If the flush fails, this still reads the
   * unacknowledged records and releases the stream before it throws, so a second call returns
   * without an error. If reading the unacknowledged records fails, the stream keeps its native
   * resources and this can be called again.
   *
   * @throws ZerobusException if an error occurs during close
   */
  @Override
  public void close() throws ZerobusException {
    ZerobusException closeFailure = null;
    List<EncodedBatch> unackedBatches;
    // Close and copy the recovery data under the read lock only: the native layer makes close wait
    // for native calls already in progress, and an AckCallback that calls into the stream during
    // the flush then gets IllegalStateException instead of waiting on a write lock held by close().
    readLock.lock();
    try {
      long handle = nativeHandle;
      if (handle == 0) {
        return;
      }
      try {
        // Close every sub-stream first (flushes pending records).
        nativeClose(handle);
      } catch (ZerobusException e) {
        // Closing marks the Rust stream closed even when flushing fails. Keep going so its
        // unacknowledged records can be recovered before the native handle is destroyed.
        closeFailure = e;
      }

      // Cache unacked batches before destroying the handle. Records are derived from them.
      try {
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
    } finally {
      readLock.unlock();
    }

    // Recovery data is safely owned by Java; destroy the native stream once no other thread is
    // using it. A concurrent close() may already have destroyed it.
    writeLock.lock();
    try {
      long handle = nativeHandle;
      if (handle != 0) {
        cachedUnackedBatches = unackedBatches;
        nativeHandle = 0;
        nativeDestroy(handle);
        logger.info("Multiplexed stream closed");
      }
    } finally {
      writeLock.unlock();
    }

    if (closeFailure != null) {
      throw closeFailure;
    }
  }

  /**
   * Returns the batches that were ingested but not acknowledged, across every sub-stream.
   *
   * <p>If the stream is still open, this closes it first so the result is complete, without
   * reporting a close failure. You must still call {@link #close()} to release native resources; it
   * throws that failure. After {@link #close()}, it returns the batches cached at close. Batches
   * keep their original grouping, which can be useful for re-ingesting records in the same batches.
   * There is no order across sub-streams.
   *
   * @return a list of unacknowledged batches
   * @throws ZerobusException if an error occurs
   */
  public List<EncodedBatch> getUnackedBatches() throws ZerobusException {
    readLock.lock();
    try {
      long handle = nativeHandle;
      if (handle == 0) {
        return cachedUnackedBatches != null ? cachedUnackedBatches : new ArrayList<>();
      }
      return nativeGetUnackedBatches(handle);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Returns the unacknowledged records as raw bytes, flattened from {@code getUnackedBatches()}.
   *
   * @return a list of unacknowledged records as raw bytes
   * @throws ZerobusException if an error occurs
   */
  protected final List<byte[]> getUnackedRecordBytes() throws ZerobusException {
    List<byte[]> records = new ArrayList<>();
    for (EncodedBatch batch : getUnackedBatches()) {
      records.addAll(batch.getRecords());
    }
    return records;
  }

  /**
   * Returns whether the stream is closed, either by {@link #close()} or because an operation
   * observed a terminal sub-stream failure.
   *
   * <p>A stream closed by a failure or by {@code getUnackedRecords()} still holds native resources:
   * always call {@link #close()}, even when this returns true.
   *
   * @return true if the stream is closed
   */
  public boolean isClosed() {
    readLock.lock();
    try {
      long handle = nativeHandle;
      return handle == 0 || nativeIsClosed(handle);
    } finally {
      readLock.unlock();
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
   * Ensures the native handle has not been released. A stream that is closed natively rejects the
   * operation itself, so this avoids an extra native call per record.
   *
   * @throws NonRetriableException if the stream has been closed with {@link #close()}
   */
  protected void ensureOpen() throws ZerobusException {
    if (nativeHandle == 0) {
      throw new NonRetriableException("Stream is closed");
    }
  }

  /**
   * Acquires the read lock and returns the native handle; the caller must release {@link #readLock}
   * in a {@code finally} block. The lock keeps another thread's {@link #close()} from freeing the
   * handle during the native call.
   *
   * @throws NonRetriableException if the stream has been closed with {@link #close()}; the lock is
   *     not held in that case
   */
  private long lockOpenHandle() throws ZerobusException {
    readLock.lock();
    long handle = nativeHandle;
    if (handle == 0) {
      readLock.unlock();
      throw new NonRetriableException("Stream is closed");
    }
    return handle;
  }

  // ==================== Native methods implemented in Rust ====================

  private static native void nativeDestroy(long handle);

  private native long nativeIngestRecord(long handle, byte[] payload);

  private native long nativeIngestRecords(long handle, List<byte[]> payloads);

  private native void nativeWaitForMessageId(long handle, long messageId);

  private native void nativeFlush(long handle);

  private native void nativeClose(long handle) throws ZerobusException;

  private native boolean nativeIsClosed(long handle);

  private native List<EncodedBatch> nativeGetUnackedBatches(long handle) throws ZerobusException;
}
