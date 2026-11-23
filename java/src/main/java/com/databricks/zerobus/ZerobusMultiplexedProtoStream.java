package com.databricks.zerobus;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Multiplexed stream for ingesting Protocol Buffer records into a table (Beta).
 *
 * <p>Distributes records round-robin across a fixed set of sub-streams. Use it when one stream is
 * the throughput bottleneck and global ordering is not required.
 *
 * <p>Create instances using {@link ZerobusSdk#streamBuilder()}:
 *
 * <pre>{@code
 * try (ZerobusMultiplexedProtoStream stream = sdk.streamBuilder()
 *         .table("catalog.schema.table")
 *         .oauth(clientId, clientSecret)
 *         .compiledProto(MyProto.getDescriptor().toProto())
 *         .multiplexed(4)
 *         .build()
 *         .join()) {
 *     for (MyProto record : records) {
 *         stream.ingestRecord(record);
 *     }
 *     stream.flush();
 * }
 * }</pre>
 *
 * <p>See {@link StreamBuilder.MultiplexedStreamBuilder} for message IDs, failures, resource
 * management, and thread safety.
 *
 * @see ZerobusSdk#streamBuilder()
 */
public class ZerobusMultiplexedProtoStream extends BaseZerobusMultiplexedStream {

  /** Package-private constructor. Use {@link ZerobusSdk#streamBuilder()} to create instances. */
  ZerobusMultiplexedProtoStream(
      long nativeHandle, String tableName, StreamConfigurationOptions options) {
    super(nativeHandle, tableName, options);
  }

  // ==================== Single Record Ingestion ====================

  /**
   * Ingests a Protocol Buffer message into the next sub-stream and returns its message ID.
   *
   * <p>Returns as soon as the record is queued; the SDK sends it and tracks its acknowledgment in
   * the background. The idiomatic flow is to call this in a loop, then confirm durability once via
   * {@link #flush()}. If the selected sub-stream is at capacity, this waits up to 30 seconds for it
   * to drain rather than rerouting, then throws {@link ZerobusException}.
   *
   * @param record the Protocol Buffer message to ingest
   * @param <T> the message type
   * @return the message ID assigned to this record
   * @throws ZerobusException if the stream is not in a valid state or an error occurs
   */
  public <T extends Message> long ingestRecord(T record) throws ZerobusException {
    return ingestEncoded(record.toByteArray());
  }

  /**
   * Ingests pre-encoded bytes into the next sub-stream and returns its message ID.
   *
   * @param encodedBytes the pre-encoded protobuf bytes
   * @return the message ID assigned to this record
   * @throws ZerobusException if the stream is not in a valid state or an error occurs
   */
  public long ingestRecord(byte[] encodedBytes) throws ZerobusException {
    return ingestEncoded(encodedBytes);
  }

  // ==================== Batch Ingestion ====================

  /**
   * Ingests multiple Protocol Buffer messages into a single sub-stream and returns the batch's
   * message ID.
   *
   * <p>The whole batch lands on one sub-stream, so one message ID covers it. Returns as soon as the
   * batch is queued; confirm durability once via {@link #flush()}.
   *
   * @param records the Protocol Buffer messages to ingest
   * @param <T> the message type
   * @return the batch message ID, or empty if the iterable is empty
   * @throws ZerobusException if the stream is not in a valid state or an error occurs
   */
  public <T extends Message> Optional<Long> ingestRecords(Iterable<T> records)
      throws ZerobusException {
    List<byte[]> payloads = new ArrayList<>();
    for (T record : records) {
      payloads.add(record.toByteArray());
    }
    return ingestEncoded(payloads);
  }

  /**
   * Ingests multiple pre-encoded byte arrays into a single sub-stream and returns the batch's
   * message ID.
   *
   * @param encodedRecords the pre-encoded protobuf byte arrays
   * @return the batch message ID, or empty if the list is empty
   * @throws ZerobusException if the stream is not in a valid state or an error occurs
   */
  public Optional<Long> ingestRecords(List<byte[]> encodedRecords) throws ZerobusException {
    return ingestEncoded(encodedRecords);
  }

  // ==================== Unacknowledged Records ====================

  /**
   * Returns the unacknowledged records as raw byte arrays, across every sub-stream.
   *
   * <p>If the stream is still open, this closes it first so the result is complete, without
   * reporting a close failure. You must still call {@link #close()} to release native resources; it
   * throws that failure. After {@link #close()}, it returns the records cached at close.
   *
   * @return a list of raw encoded records
   * @throws ZerobusException if an error occurs
   */
  public List<byte[]> getUnackedRecords() throws ZerobusException {
    return getUnackedRecordBytes();
  }

  /**
   * Returns the unacknowledged records parsed into Protocol Buffer messages, across every
   * sub-stream.
   *
   * <p>If the stream is still open, this closes it first so the result is complete, without
   * reporting a close failure. You must still call {@link #close()} to release native resources; it
   * throws that failure. After {@link #close()}, it returns the records cached at close.
   *
   * @param parser the protobuf parser for the message type
   * @param <T> the message type
   * @return a list of parsed records
   * @throws ZerobusException if an error occurs or parsing fails
   */
  public <T extends Message> List<T> getUnackedRecords(Parser<T> parser) throws ZerobusException {
    List<byte[]> rawRecords = getUnackedRecords();
    List<T> result = new ArrayList<>(rawRecords.size());
    for (byte[] bytes : rawRecords) {
      try {
        result.add(parser.parseFrom(bytes));
      } catch (InvalidProtocolBufferException e) {
        throw new ZerobusException("Failed to parse unacked record: " + e.getMessage(), e);
      }
    }
    return result;
  }
}
