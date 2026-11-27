package com.databricks.zerobus;

/**
 * Callback interface for receiving acknowledgment notifications from the Zerobus stream.
 *
 * <p>This interface provides methods for handling both successful acknowledgments and errors. It
 * replaces the deprecated {@code Consumer<IngestRecordResponse>} callback with a more type-safe and
 * flexible API.
 *
 * <p>A callback is invoked once per logical ingest submission. In particular, a batch ingest call
 * produces one callback rather than one callback per record in the batch.
 *
 * <p>Implementations should be thread-safe as callbacks may be invoked from multiple threads.
 * Callbacks should be lightweight to avoid blocking the internal processing threads.
 *
 * <p>Callbacks run on internal SDK threads, so they must not call stream methods that block on
 * native work: {@code ingestRecordOffset}, {@code ingestRecordsOffset}, the deprecated {@code
 * ZerobusStream.ingestRecord}, {@code ingestBatch} (Arrow), {@code waitForOffset}, {@code flush},
 * {@code close}, {@code isClosed}, {@code getUnackedRecords}, and {@code getUnackedBatches}, as
 * well as {@code ZerobusSdk.recreateStream} and {@code recreateArrowStream}, which can call them.
 * Called from a callback, these throw {@link IllegalStateException}; hand such work to another
 * thread instead. On multiplexed streams, {@code ingestRecord}, {@code ingestRecords}, and {@code
 * waitForMessageId} block too.
 *
 * <h3>Multiplexed streams</h3>
 *
 * <p>On a multiplexed stream (Beta), registered with {@code multiplexed(n).ackCallback(...)}, the
 * {@code long} argument of both methods is a message ID returned by the stream's ingest methods,
 * not an offset. Message IDs are not ordered and may be negative, so an acknowledgment covers only
 * the record or batch with that ID: the callback is invoked once for each of them, in no global
 * order, and sub-streams may invoke it concurrently.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * AckCallback callback = new AckCallback() {
 *     @Override
 *     public void onAck(long offsetId) {
 *         System.out.println("Record at offset " + offsetId + " acknowledged");
 *     }
 *
 *     @Override
 *     public void onError(long offsetId, String errorMessage) {
 *         System.err.println("Error for offset " + offsetId + ": " + errorMessage);
 *     }
 * };
 *
 * StreamConfigurationOptions options = StreamConfigurationOptions.builder()
 *     .setAckCallback(callback)
 *     .build();
 * }</pre>
 *
 * @see StreamConfigurationOptions.StreamConfigurationOptionsBuilder#setAckCallback(AckCallback)
 */
public interface AckCallback {

  /**
   * Called when a logical ingest submission has been durably acknowledged by the server.
   *
   * <p>The offset ID represents the durability acknowledgment up to and including this offset. All
   * records with offset IDs less than or equal to this value have been durably stored. On a
   * multiplexed stream, the argument is instead the message ID of the one record or batch that was
   * acknowledged; see the class documentation.
   *
   * <p>Implementations should not throw. An exception thrown from this method is normally logged at
   * ERROR level with its stack trace through the SDK's SLF4J logger, which, like all SDK logging,
   * is visible only when an SLF4J binding is configured; if logging itself fails, the exception is
   * dropped. Later callbacks are still delivered.
   *
   * @param offsetId the offset ID that has been acknowledged
   */
  void onAck(long offsetId);

  /**
   * Called when an error occurs for a logical ingest submission.
   *
   * <p>This method is called when the SDK encounters an error that affects a specific submission
   * offset. The error may be retryable or non-retryable depending on the nature of the failure. On
   * a multiplexed stream, the argument is the message ID of the affected record or batch.
   *
   * <p>Implementations should not throw. An exception thrown from this method is normally logged at
   * ERROR level with its stack trace through the SDK's SLF4J logger, which, like all SDK logging,
   * is visible only when an SLF4J binding is configured; if logging itself fails, the exception is
   * dropped. Later callbacks are still delivered.
   *
   * @param offsetId the offset ID that encountered an error
   * @param errorMessage a description of the error that occurred
   */
  void onError(long offsetId, String errorMessage);
}
