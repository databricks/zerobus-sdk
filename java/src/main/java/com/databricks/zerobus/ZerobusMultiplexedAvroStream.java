package com.databricks.zerobus;

import java.util.List;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;

/**
 * Multiplexed stream for ingesting Avro records into a table (Beta).
 *
 * <p>Distributes records round-robin across a fixed set of sub-streams that share one Avro writer
 * schema. Use it when one stream is the throughput bottleneck and global ordering is not required.
 * Ingest either {@link GenericRecord} objects, encoded against the writer schema, or pre-encoded
 * Avro datums ({@code byte[]}).
 *
 * <pre>{@code
 * try (ZerobusMultiplexedAvroStream stream = sdk.streamBuilder()
 *         .table("catalog.schema.table")
 *         .oauth(clientId, clientSecret)
 *         .avro(schemaJson)
 *         .multiplexed(4)
 *         .build()
 *         .join()) {
 *     for (GenericRecord record : records) {
 *         stream.ingestRecord(record);
 *     }
 *     stream.flush();
 * }
 * }</pre>
 *
 * <h3>Requirements</h3>
 *
 * <p>Ingesting {@link GenericRecord}s requires Java 11+ and the {@code org.apache.avro:avro}
 * dependency on the classpath. Pre-encoded {@code byte[]} ingestion needs neither.
 *
 * <p>See {@link StreamBuilder.MultiplexedStreamBuilder} for message IDs, failures, resource
 * management, and thread safety.
 *
 * @see ZerobusSdk#streamBuilder()
 */
public class ZerobusMultiplexedAvroStream extends BaseZerobusMultiplexedStream {

  private final String avroSchemaJson;
  // Built lazily on first GenericRecord ingest, so a bytes-only stream never loads Apache Avro.
  // Volatile and built under encoderLock, so concurrent first ingests parse the schema once and
  // every thread sees a fully constructed encoder, which is thread-safe.
  private volatile AvroEncoder encoder;
  private final Object encoderLock = new Object();

  /** Package-private constructor. Use {@link ZerobusSdk#streamBuilder()} to create instances. */
  ZerobusMultiplexedAvroStream(
      long nativeHandle,
      String tableName,
      StreamConfigurationOptions options,
      String avroSchemaJson) {
    super(nativeHandle, tableName, options);
    this.avroSchemaJson = avroSchemaJson;
  }

  private AvroEncoder encoder() throws ZerobusException {
    AvroEncoder current = encoder;
    if (current != null) {
      return current;
    }
    synchronized (encoderLock) {
      if (encoder == null) {
        try {
          encoder = AvroEncoder.threadSafe(avroSchemaJson);
        } catch (LinkageError e) {
          // Missing/incompatible Apache Avro (or a too-old JVM). Catching the JDK LinkageError
          // here (not an Avro type) keeps the pre-encoded byte[] path free of the Avro dependency.
          throw new NonRetriableException(
              "Ingesting Avro records requires the org.apache.avro:avro dependency and Java 11+: "
                  + e,
              e);
        }
      }
      return encoder;
    }
  }

  // ==================== Single Record Ingestion ====================

  /**
   * Ingests an Avro record into the next sub-stream, encoding it against the writer schema, and
   * returns its message ID.
   *
   * <p>Returns as soon as the record is queued; confirm durability once via {@link #flush()}. If
   * the selected sub-stream is at capacity, this waits up to 30 seconds for it to drain rather than
   * rerouting, then throws {@link ZerobusException}.
   *
   * @param record the record to ingest
   * @return the message ID assigned to this record
   * @throws ZerobusException if the stream is not in a valid state or an error occurs; a {@link
   *     NonRetriableException} if the record cannot be encoded, in which case nothing is sent and
   *     the stream stays usable
   */
  public long ingestRecord(GenericRecord record) throws ZerobusException {
    ensureOpen();
    return ingestEncoded(encoder().encode(record));
  }

  /**
   * Ingests a pre-encoded Avro datum into the next sub-stream and returns its message ID.
   *
   * <p>The bytes must be a <b>bare</b> Avro binary datum written with the stream's writer schema.
   * Framed forms — Avro container files, single-object encoding, and Confluent-framed bytes — are
   * not accepted and are not detected.
   *
   * @param encodedBytes a bare Avro binary datum encoded against the stream's writer schema
   * @return the message ID assigned to this record
   * @throws ZerobusException if the stream is not in a valid state or an error occurs
   */
  public long ingestRecord(byte[] encodedBytes) throws ZerobusException {
    return ingestEncoded(encodedBytes);
  }

  // ==================== Batch Ingestion ====================

  /**
   * Ingests a batch of Avro records into a single sub-stream, encoding each against the writer
   * schema.
   *
   * @param records the records to ingest
   * @return the batch message ID, or empty if there are no records
   * @throws ZerobusException if the stream is not in a valid state or an error occurs; a {@link
   *     NonRetriableException} if a record cannot be encoded, in which case nothing from the batch
   *     is sent and the stream stays usable
   */
  public Optional<Long> ingestRecords(Iterable<? extends GenericRecord> records)
      throws ZerobusException {
    ensureOpen();
    return ingestEncoded(encoder().encodeBatch(records));
  }

  /**
   * Ingests a batch of pre-encoded Avro datums into a single sub-stream.
   *
   * <p>Each element must be a <b>bare</b> Avro binary datum written with the stream's writer
   * schema; container-file, single-object, and Confluent framing are not accepted.
   *
   * @param encodedRecords bare Avro binary datums encoded against the stream's writer schema
   * @return the batch message ID, or empty if the list is empty
   * @throws ZerobusException if the stream is not in a valid state or an error occurs
   */
  public Optional<Long> ingestRecords(List<byte[]> encodedRecords) throws ZerobusException {
    return ingestEncoded(encodedRecords);
  }

  // ==================== Unacknowledged Records ====================

  /**
   * Returns the unacknowledged records as raw Avro datums, across every sub-stream.
   *
   * <p>If the stream is still open, this closes it first so the result is complete, without
   * reporting a close failure. You must still call {@link #close()} to release native resources; it
   * throws that failure. After {@link #close()}, it returns the records cached at close.
   *
   * @return a list of Avro binary datums
   * @throws ZerobusException if an error occurs
   */
  public List<byte[]> getUnackedRecords() throws ZerobusException {
    return getUnackedRecordBytes();
  }
}
