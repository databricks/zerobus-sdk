package com.databricks.zerobus;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a batch of encoded records that were ingested together.
 *
 * <p>This class is used when retrieving unacknowledged batches from the stream after a failure. It
 * preserves the batch grouping from the original ingestion, which can be useful for re-ingesting
 * records in the same batches.
 *
 * <p>Each EncodedBatch contains a list of raw byte arrays (the encoded records) and the {@link
 * RecordFormat} they are encoded in.
 *
 * @see ZerobusProtoStream#getUnackedBatches()
 * @see ZerobusJsonStream#getUnackedBatches()
 * @see ZerobusAvroStream#getUnackedBatches()
 */
public class EncodedBatch {

  private final List<byte[]> records;
  private final RecordFormat format;

  /**
   * Creates a new EncodedBatch with the given records and format.
   *
   * @param records the list of encoded record byte arrays
   * @param format the record format of the batch
   */
  public EncodedBatch(List<byte[]> records, RecordFormat format) {
    this.records = records != null ? new ArrayList<>(records) : new ArrayList<>();
    this.format = Objects.requireNonNull(format, "format");
  }

  /**
   * Creates a new EncodedBatch with the given records.
   *
   * @param records the list of encoded record byte arrays
   * @param isJson true if the records are JSON encoded, false if Protocol Buffer encoded
   */
  public EncodedBatch(List<byte[]> records, boolean isJson) {
    this(records, isJson ? RecordFormat.JSON : RecordFormat.PROTO);
  }

  /** Package-private constructor used by the native layer (format passed as a wire code). */
  EncodedBatch(List<byte[]> records, int formatCode) {
    this(records, RecordFormat.fromCode(formatCode));
  }

  /**
   * Returns the encoded records in this batch as raw byte arrays.
   *
   * <p>The returned list is unmodifiable.
   *
   * @return an unmodifiable list of encoded record byte arrays
   */
  public List<byte[]> getRecords() {
    return Collections.unmodifiableList(records);
  }

  /**
   * Returns the records in this batch as JSON strings.
   *
   * <p>This method is only valid for JSON batches. For Protocol Buffer batches, use {@link
   * #getRecords()} and deserialize using the appropriate parser.
   *
   * @return a list of JSON strings
   * @throws IllegalStateException if this batch is not JSON encoded
   */
  public List<String> getRecordsAsStrings() {
    if (format != RecordFormat.JSON) {
      throw new IllegalStateException(
          "Cannot convert " + format + " batch to strings. Use getRecords() instead.");
    }
    List<String> result = new ArrayList<>(records.size());
    for (byte[] bytes : records) {
      result.add(new String(bytes, StandardCharsets.UTF_8));
    }
    return result;
  }

  /**
   * Returns the record format of this batch.
   *
   * @return the record format
   */
  public RecordFormat getFormat() {
    return format;
  }

  /**
   * Returns whether the records in this batch are JSON encoded.
   *
   * @return true if JSON encoded, false otherwise
   */
  public boolean isJson() {
    return format == RecordFormat.JSON;
  }

  /**
   * Returns the number of records in this batch.
   *
   * @return the number of records
   */
  public int size() {
    return records.size();
  }

  /**
   * Returns whether this batch is empty.
   *
   * @return true if empty, false otherwise
   */
  public boolean isEmpty() {
    return records.isEmpty();
  }
}
