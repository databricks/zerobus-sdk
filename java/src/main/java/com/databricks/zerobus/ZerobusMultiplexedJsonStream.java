package com.databricks.zerobus;

import com.databricks.zerobus.ZerobusJsonStream.JsonDeserializer;
import com.databricks.zerobus.ZerobusJsonStream.JsonSerializer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Multiplexed stream for ingesting JSON records into a table (Beta).
 *
 * <p>Distributes records round-robin across a fixed set of sub-streams. Use it when one stream is
 * the throughput bottleneck and global ordering is not required. For JSON, first migrate to
 * Protocol Buffers and measure again before considering multiplexing.
 *
 * <p>Create instances using {@link ZerobusSdk#streamBuilder()}:
 *
 * <pre>{@code
 * Gson gson = new Gson();
 * try (ZerobusMultiplexedJsonStream stream = sdk.streamBuilder()
 *         .table("catalog.schema.table")
 *         .oauth(clientId, clientSecret)
 *         .json()
 *         .multiplexed(4)
 *         .build()
 *         .join()) {
 *     for (MyData record : records) {
 *         stream.ingestRecord(record, gson::toJson);
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
public class ZerobusMultiplexedJsonStream extends BaseZerobusMultiplexedStream {

  /** Package-private constructor. Use {@link ZerobusSdk#streamBuilder()} to create instances. */
  ZerobusMultiplexedJsonStream(
      long nativeHandle, String tableName, StreamConfigurationOptions options) {
    super(nativeHandle, tableName, options);
  }

  // ==================== Single Record Ingestion ====================

  /**
   * Ingests an object as JSON into the next sub-stream and returns its message ID.
   *
   * <p>Returns as soon as the record is queued; the SDK sends it and tracks its acknowledgment in
   * the background. The idiomatic flow is to call this in a loop, then confirm durability once via
   * {@link #flush()}. If the selected sub-stream is at capacity, this waits up to 30 seconds for it
   * to drain rather than rerouting, then throws {@link ZerobusException}.
   *
   * @param object the object to serialize and ingest
   * @param serializer a function that converts the object to a JSON string
   * @param <T> the type of the object
   * @return the message ID assigned to this record
   * @throws ZerobusException if the stream is not in a valid state or an error occurs
   */
  public <T> long ingestRecord(T object, JsonSerializer<T> serializer) throws ZerobusException {
    ensureOpen();
    return ingestEncoded(serializer.serialize(object).getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Ingests a JSON string into the next sub-stream and returns its message ID.
   *
   * @param json the JSON string to ingest
   * @return the message ID assigned to this record
   * @throws ZerobusException if the stream is not in a valid state or an error occurs
   */
  public long ingestRecord(String json) throws ZerobusException {
    return ingestEncoded(json.getBytes(StandardCharsets.UTF_8));
  }

  // ==================== Batch Ingestion ====================

  /**
   * Ingests multiple objects as JSON into a single sub-stream and returns the batch's message ID.
   *
   * <p>The whole batch lands on one sub-stream, so one message ID covers it. Returns as soon as the
   * batch is queued; confirm durability once via {@link #flush()}.
   *
   * @param objects the objects to serialize and ingest
   * @param serializer a function that converts each object to a JSON string
   * @param <T> the type of the objects
   * @return the batch message ID, or empty if the iterable is empty
   * @throws ZerobusException if the stream is not in a valid state or an error occurs
   */
  public <T> Optional<Long> ingestRecords(Iterable<T> objects, JsonSerializer<T> serializer)
      throws ZerobusException {
    ensureOpen();
    List<byte[]> payloads = new ArrayList<>();
    for (T obj : objects) {
      payloads.add(serializer.serialize(obj).getBytes(StandardCharsets.UTF_8));
    }
    return ingestEncoded(payloads);
  }

  /**
   * Ingests multiple JSON strings into a single sub-stream and returns the batch's message ID.
   *
   * @param jsonStrings the JSON strings to ingest
   * @return the batch message ID, or empty if the iterable is empty
   * @throws ZerobusException if the stream is not in a valid state or an error occurs
   */
  public Optional<Long> ingestRecords(Iterable<String> jsonStrings) throws ZerobusException {
    List<byte[]> payloads = new ArrayList<>();
    for (String json : jsonStrings) {
      payloads.add(json.getBytes(StandardCharsets.UTF_8));
    }
    return ingestEncoded(payloads);
  }

  // ==================== Unacknowledged Records ====================

  /**
   * Returns the unacknowledged records as JSON strings, across every sub-stream.
   *
   * <p>If the stream is still open, this closes it first so the result is complete, without
   * reporting a close failure. You must still call {@link #close()} to release native resources; it
   * throws that failure. After {@link #close()}, it returns the records cached at close.
   *
   * @return a list of JSON strings
   * @throws ZerobusException if an error occurs
   */
  public List<String> getUnackedRecords() throws ZerobusException {
    List<byte[]> rawRecords = getUnackedRecordBytes();
    List<String> result = new ArrayList<>(rawRecords.size());
    for (byte[] bytes : rawRecords) {
      result.add(new String(bytes, StandardCharsets.UTF_8));
    }
    return result;
  }

  /**
   * Returns the unacknowledged records deserialized into objects, across every sub-stream.
   *
   * <p>If the stream is still open, this closes it first so the result is complete, without
   * reporting a close failure. You must still call {@link #close()} to release native resources; it
   * throws that failure. After {@link #close()}, it returns the records cached at close.
   *
   * @param deserializer a function that converts JSON strings to objects
   * @param <T> the type of the objects
   * @return a list of deserialized objects
   * @throws ZerobusException if an error occurs
   */
  public <T> List<T> getUnackedRecords(JsonDeserializer<T> deserializer) throws ZerobusException {
    List<String> jsonRecords = getUnackedRecords();
    List<T> result = new ArrayList<>(jsonRecords.size());
    for (String json : jsonRecords) {
      result.add(deserializer.deserialize(json));
    }
    return result;
  }
}
