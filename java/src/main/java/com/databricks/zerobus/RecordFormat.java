package com.databricks.zerobus;

/**
 * The record encoding a stream uses: Protocol Buffers, JSON, or Avro.
 *
 * <p>The format is fixed when the stream is created (via the {@code compiledProto()}, {@code
 * json()}, or {@code avro()} builder methods) and reported by {@link EncodedBatch#getFormat()}.
 */
public enum RecordFormat {
  PROTO(1),
  JSON(2),
  AVRO(4);

  private final int code;

  RecordFormat(int code) {
    this.code = code;
  }

  /**
   * Wire code passed to the native layer; must equal {@code RecordType} in zerobus_service.proto.
   */
  int code() {
    return code;
  }

  /** Returns the format for a native wire code. */
  static RecordFormat fromCode(int code) {
    for (RecordFormat format : values()) {
      if (format.code == code) {
        return format;
      }
    }
    throw new IllegalArgumentException("Unknown record format code: " + code);
  }
}
