package com.databricks.zerobus;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents a batch of encoded records that were ingested together.
 *
 * <p>This class is used when retrieving unacknowledged batches from the stream after a failure. It
 * preserves the batch grouping from the original ingestion, which can be useful for re-ingesting
 * records in the same batches.
 *
 * <p>Each EncodedBatch contains a list of raw byte arrays (the encoded records) and a flag
 * indicating whether they are JSON or Protocol Buffer encoded.
 *
 * @see ZerobusProtoStream#getUnackedBatches()
 * @see ZerobusJsonStream#getUnackedBatches()
 */
public class EncodedBatch {

  private final List<byte[]> records;
  private final boolean isJson;

  /**
   * Creates a new EncodedBatch with the given records.
   *
   * @param records the list of encoded record byte arrays
   * @param isJson true if the records are JSON encoded, false if Protocol Buffer encoded
   */
  public EncodedBatch(List<byte[]> records, boolean isJson) {
    this.records = records != null ? new ArrayList<>(records) : new ArrayList<>();
    this.isJson = isJson;
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
    if (!isJson) {
      throw new IllegalStateException(
          "Cannot convert Protocol Buffer batch to strings. Use getRecords() instead.");
    }
    List<String> result = new ArrayList<>(records.size());
    for (byte[] bytes : records) {
      result.add(new String(bytes, StandardCharsets.UTF_8));
    }
    return result;
  }

  /**
   * Returns whether the records in this batch are JSON encoded.
   *
   * @return true if JSON encoded, false if Protocol Buffer encoded
   */
  public boolean isJson() {
    return isJson;
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
