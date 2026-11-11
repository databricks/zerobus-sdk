package com.databricks.zerobus;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * An immutable snapshot of a stream's counters, returned by {@code getStats()} on the stream
 * classes.
 *
 * <p>Each field is read separately from the live stream, so a snapshot taken while another thread
 * is ingesting can mix values from slightly different moments.
 *
 * <p>Multiplexed streams do not expose these counters.
 *
 * @since 1.9.0
 */
public final class StreamStats {

  private final long recordsInFlight;
  private final long lastAckedOffset;
  private final long reconnects;
  private final LastError lastError;

  StreamStats(
      long recordsInFlight,
      long lastAckedOffset,
      long reconnects,
      String lastErrorMessage,
      boolean lastErrorRetryable,
      long lastErrorTimestampMillis) {
    this.recordsInFlight = recordsInFlight;
    this.lastAckedOffset = lastAckedOffset;
    this.reconnects = reconnects;
    this.lastError =
        lastErrorMessage == null
            ? null
            : new LastError(
                lastErrorMessage,
                lastErrorRetryable,
                Instant.ofEpochMilli(lastErrorTimestampMillis));
  }

  static StreamStats empty() {
    return new StreamStats(0, -1, 0, null, false, 0);
  }

  /**
   * Returns the number of records accepted by ingest that the server has not yet acknowledged.
   *
   * <p>Counts records, not batches: a batch of N records adds N. Records rejected before they were
   * queued are not counted.
   *
   * @return the number of unacknowledged records
   */
  public long getRecordsInFlight() {
    return recordsInFlight;
  }

  /**
   * Returns the offset of the most recent batch the server fully acknowledged.
   *
   * <p>Empty until the first acknowledgment. The value never decreases.
   *
   * @return the last acknowledged offset, or an empty value if none has been acknowledged
   */
  public OptionalLong getLastAckedOffset() {
    return lastAckedOffset < 0 ? OptionalLong.empty() : OptionalLong.of(lastAckedOffset);
  }

  /**
   * Returns the number of automatic recoveries that completed successfully. The initial stream
   * creation is not counted.
   *
   * @return the number of successful reconnects
   */
  public long getReconnects() {
    return reconnects;
  }

  /**
   * Returns the most recent error the stream recorded, including errors it recovered from. The
   * error is kept after a successful recovery.
   *
   * @return the last error, or an empty value if the stream has not recorded any error
   */
  public Optional<LastError> getLastError() {
    return Optional.ofNullable(lastError);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StreamStats)) {
      return false;
    }
    StreamStats other = (StreamStats) o;
    return recordsInFlight == other.recordsInFlight
        && lastAckedOffset == other.lastAckedOffset
        && reconnects == other.reconnects
        && Objects.equals(lastError, other.lastError);
  }

  @Override
  public int hashCode() {
    return Objects.hash(recordsInFlight, lastAckedOffset, reconnects, lastError);
  }

  @Override
  public String toString() {
    return "StreamStats{recordsInFlight="
        + recordsInFlight
        + ", lastAckedOffset="
        + getLastAckedOffset()
        + ", reconnects="
        + reconnects
        + ", lastError="
        + lastError
        + "}";
  }

  /** The most recent error a stream recorded. Immutable. */
  public static final class LastError {

    private final String message;
    private final boolean retryable;
    private final Instant timestamp;

    LastError(String message, boolean retryable, Instant timestamp) {
      this.message = message;
      this.retryable = retryable;
      this.timestamp = timestamp;
    }

    /**
     * Returns the error message, the same text as the error's {@code toString()} in the SDK.
     *
     * @return the error message
     */
    public String getMessage() {
      return message;
    }

    /**
     * Returns whether the stream classified the error as retryable.
     *
     * @return {@code true} if the error is retryable
     */
    public boolean isRetryable() {
      return retryable;
    }

    /**
     * Returns when the stream recorded the error.
     *
     * @return the time the error was recorded
     */
    public Instant getTimestamp() {
      return timestamp;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof LastError)) {
        return false;
      }
      LastError other = (LastError) o;
      return retryable == other.retryable
          && message.equals(other.message)
          && timestamp.equals(other.timestamp);
    }

    @Override
    public int hashCode() {
      return Objects.hash(message, retryable, timestamp);
    }

    @Override
    public String toString() {
      return "LastError{message="
          + message
          + ", retryable="
          + retryable
          + ", timestamp="
          + timestamp
          + "}";
    }
  }
}
