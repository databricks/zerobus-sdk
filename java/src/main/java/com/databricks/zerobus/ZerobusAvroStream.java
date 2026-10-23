package com.databricks.zerobus;

import java.util.List;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;

/**
 * Stream for ingesting Avro records into a table (Beta).
 *
 * <p>Create it with {@link ZerobusSdk#streamBuilder()}, passing the Avro writer schema as JSON.
 * Ingest either {@link GenericRecord} objects — encoded against the writer schema — or pre-encoded
 * Avro datums ({@code byte[]}), one at a time or in batches. Ingestion is asynchronous: each {@code
 * ingest} returns once the record is queued; call {@link #flush()} once (after a bounded batch, or
 * periodically for a long-running stream) to confirm durability, or register an {@link
 * AckCallback}. Avoid calling {@link #waitForOffset(long)} after every record — that forces a
 * round-trip per record.
 *
 * <pre>{@code
 * String schemaJson = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":["
 *     + "{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"customer\",\"type\":\"string\"}]}";
 * Schema schema = new Schema.Parser().parse(schemaJson);
 * try (ZerobusAvroStream stream = sdk.streamBuilder()
 *         .table("catalog.schema.table")
 *         .oauth(clientId, clientSecret)
 *         .avro(schemaJson)
 *         .build()
 *         .join()) {
 *     for (Order order : orders) {
 *         GenericRecord record = new GenericData.Record(schema);
 *         record.put("id", order.id);
 *         record.put("customer", order.customer);
 *         stream.ingestRecordOffset(record);        // GenericRecord, or a pre-encoded byte[]
 *     }
 *     stream.flush();
 * }
 * }</pre>
 *
 * <p>For the Java value each Avro type expects (e.g. {@code ByteBuffer} for {@code bytes}, {@code
 * GenericData.Fixed} for {@code fixed}), see the README.
 *
 * <h3>Requirements</h3>
 *
 * <p>Requires Java 11+ and the {@code org.apache.avro:avro} dependency on the classpath.
 *
 * <h3>Resource Management / Thread Safety</h3>
 *
 * <p>Holds native resources not freed by the garbage collector — call {@link #close()} (or use
 * try-with-resources). Not thread-safe; use one instance per thread.
 *
 * @see ZerobusSdk#streamBuilder()
 */
public class ZerobusAvroStream extends BaseZerobusStream {

  private final String avroSchemaJson;
  private final String clientId;
  private final String clientSecret;
  private final HeadersProvider headersProvider;
  private final AvroEncoder encoder;

  /**
   * Package-private constructor. Use {@link ZerobusSdk#streamBuilder()} to create instances.
   *
   * <p>The {@link AvroEncoder} is built by the caller <b>before</b> the native stream is created,
   * so that a bad writer schema fails fast without leaking a native stream handle.
   */
  ZerobusAvroStream(
      long nativeHandle,
      String tableName,
      StreamConfigurationOptions options,
      String avroSchemaJson,
      AvroEncoder encoder,
      String clientId,
      String clientSecret,
      HeadersProvider headersProvider) {
    super(nativeHandle, tableName, options);
    this.avroSchemaJson = avroSchemaJson;
    this.encoder = encoder;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.headersProvider = headersProvider;
  }

  // ==================== Single Record Ingestion ====================

  /**
   * Ingests an Avro record, encoding it against the writer schema, and returns the offset.
   *
   * @param record the record to ingest
   * @return the offset ID assigned to this record
   * @throws ZerobusException if the record cannot be encoded or the stream is not in a valid state
   */
  public long ingestRecordOffset(GenericRecord record) throws ZerobusException {
    return ingestEncoded(encoder.encode(record));
  }

  /**
   * Ingests a pre-encoded Avro datum and returns the offset.
   *
   * @param encodedBytes an Avro binary datum encoded against the stream's writer schema
   * @return the offset ID assigned to this record
   * @throws ZerobusException if the stream is not in a valid state or an error occurs
   */
  public long ingestRecordOffset(byte[] encodedBytes) throws ZerobusException {
    return ingestEncoded(encodedBytes);
  }

  // ==================== Batch Ingestion ====================

  /**
   * Ingests a batch of Avro records, encoding each against the writer schema.
   *
   * @param records the records to ingest
   * @return the batch offset ID, or empty if there are no records
   * @throws ZerobusException if a record cannot be encoded or the stream is not in a valid state
   */
  public Optional<Long> ingestRecordsOffset(Iterable<? extends GenericRecord> records)
      throws ZerobusException {
    return ingestEncoded(encoder.encodeBatch(records));
  }

  /**
   * Ingests a batch of pre-encoded Avro datums.
   *
   * @param encodedRecords Avro binary datums encoded against the stream's writer schema
   * @return the batch offset ID, or empty if the list is empty
   * @throws ZerobusException if the stream is not in a valid state or an error occurs
   */
  public Optional<Long> ingestRecordsOffset(List<byte[]> encodedRecords) throws ZerobusException {
    return ingestEncoded(encodedRecords);
  }

  // ==================== Unacknowledged Records ====================

  /**
   * Returns the unacknowledged records as raw Avro datums (returns cached data after close).
   *
   * @return a list of Avro binary datums
   * @throws ZerobusException if an error occurs
   */
  public List<byte[]> getUnackedRecords() throws ZerobusException {
    if (nativeHandle == 0) {
      return getCachedUnackedRecords();
    }
    return nativeGetUnackedRecords(nativeHandle);
  }

  /**
   * Returns the unacknowledged batches after stream failure (returns cached data after close).
   *
   * @return a list of unacknowledged batches
   * @throws ZerobusException if an error occurs
   */
  public List<EncodedBatch> getUnackedBatches() throws ZerobusException {
    if (nativeHandle == 0) {
      return getCachedUnackedBatches();
    }
    return nativeGetUnackedBatches(nativeHandle);
  }

  // ==================== Package-Private Accessors (used by recreateStream) ====================

  /** Returns the Avro writer schema (JSON) used to create this stream. */
  String getAvroSchemaJson() {
    return avroSchemaJson;
  }

  /** Returns the encoder used by this stream (reused when recreating the stream). */
  AvroEncoder getEncoder() {
    return encoder;
  }

  /** Returns the OAuth client ID used to create this stream. */
  String getClientId() {
    return clientId;
  }

  /** Returns the OAuth client secret used to create this stream. */
  String getClientSecret() {
    return clientSecret;
  }

  HeadersProvider getHeadersProvider() {
    return headersProvider;
  }
}
