package com.databricks.zerobus;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Fluent builder for creating Zerobus ingestion streams.
 *
 * <p>This is the recommended way to create a stream. It mirrors the {@code stream_builder()} API of
 * the Rust SDK. Shared setters may be called in any order before selecting a record format. Because
 * Java cannot return different stream types from a single {@code build()}, the record format is
 * selected with a terminal method that returns a typed sub-builder: {@link #json()}, {@link
 * #compiledProto(DescriptorProto)}, {@link #avro(String)}, or {@link #arrow(Schema)}. Each
 * sub-builder exposes a {@code build()} that returns the matching stream type. The JSON and
 * Protocol Buffer sub-builders also offer {@code multiplexed(int)}, which selects a multiplexed
 * stream (Beta) built by a {@link MultiplexedStreamBuilder}.
 *
 * <h3>Thread Safety</h3>
 *
 * <p>A {@code StreamBuilder} is not thread-safe and is intended to be configured and built from a
 * single thread.
 *
 * @see ZerobusSdk#streamBuilder()
 * @see StreamConfigurationOptions
 * @see ArrowStreamConfigurationOptions
 */
public final class StreamBuilder {

  /** Upper bound on {@code multiplexed(int)} sub-streams, matching the native SDK. */
  private static final int MAX_MULTIPLEXED_STREAMS = 64;

  private final ZerobusSdk sdk;

  private String tableName;

  // Authentication. OAuth credentials, a custom headers provider, and federated auth are mutually
  // exclusive. Federated auth is stored as a FederatedAuthHeadersProvider so that it follows the
  // same create and recreate path as a headers provider.
  private String clientId;
  private String clientSecret;
  private HeadersProvider headersProvider;

  // Shared and JSON/protobuf configuration. A {@code null} value means "not set", so each record
  // format falls back to its own defaults (for example Arrow defaults to 4 recovery retries while
  // JSON/protobuf defaults to 3). This mirrors the Rust builder, which keeps separate JSON/protobuf
  // and Arrow configs.
  private Integer maxInflightRecords;
  private Boolean recovery;
  private Integer recoveryTimeoutMs;
  private Integer recoveryBackoffMs;
  private Integer recoveryRetries;
  private Integer serverLackOfAckTimeoutMs;
  private Integer flushTimeoutMs;
  private AckCallback ackCallback;

  /** Package-private constructor. Use {@link ZerobusSdk#streamBuilder()} to create instances. */
  StreamBuilder(ZerobusSdk sdk) {
    this.sdk = sdk;
  }

  /**
   * Sets the fully qualified Unity Catalog table name (for example {@code "catalog.schema.table"}).
   *
   * @param tableName the fully qualified table name
   * @return this builder for method chaining
   */
  public StreamBuilder table(String tableName) {
    this.tableName = requireNonBlank(tableName, "tableName");
    return this;
  }

  /**
   * Authenticates with OAuth client credentials.
   *
   * @param clientId the OAuth client ID
   * @param clientSecret the OAuth client secret
   * @return this builder for method chaining
   */
  public StreamBuilder oauth(String clientId, String clientSecret) {
    this.clientId = requireNonBlank(clientId, "clientId");
    this.clientSecret = requireNonBlank(clientSecret, "clientSecret");
    this.headersProvider = null;
    return this;
  }

  /**
   * Authenticates with a custom headers provider.
   *
   * @param headersProvider the provider for authentication and request headers
   * @return this builder for method chaining
   */
  public StreamBuilder headersProvider(HeadersProvider headersProvider) {
    this.headersProvider = Objects.requireNonNull(headersProvider, "headersProvider");
    this.clientId = null;
    this.clientSecret = null;
    return this;
  }

  /**
   * Authenticates by federating an external identity provider token (for example from Microsoft
   * Entra ID). The SDK exchanges it for a Zerobus-scoped Databricks token and refreshes it before
   * it expires. See {@link FederatedToken} for the supported modes.
   *
   * @param federatedToken the federated identity to authenticate as
   * @return this builder for method chaining
   */
  public StreamBuilder federatedAuth(FederatedToken federatedToken) {
    this.headersProvider =
        new FederatedAuthHeadersProvider(Objects.requireNonNull(federatedToken, "federatedToken"));
    this.clientId = null;
    this.clientSecret = null;
    return this;
  }

  /**
   * Enables or disables automatic stream recovery.
   *
   * @param recovery true to enable automatic recovery, false to disable
   * @return this builder for method chaining
   */
  public StreamBuilder recovery(boolean recovery) {
    this.recovery = recovery;
    return this;
  }

  /**
   * Sets the timeout for recovery operations.
   *
   * @param recoveryTimeoutMs the recovery timeout in milliseconds
   * @return this builder for method chaining
   * @throws IllegalArgumentException if {@code recoveryTimeoutMs} is not positive
   */
  public StreamBuilder recoveryTimeoutMs(int recoveryTimeoutMs) {
    this.recoveryTimeoutMs = requirePositive(recoveryTimeoutMs, "recoveryTimeoutMs");
    return this;
  }

  /**
   * Sets the backoff delay between recovery attempts.
   *
   * @param recoveryBackoffMs the recovery backoff delay in milliseconds
   * @return this builder for method chaining
   * @throws IllegalArgumentException if {@code recoveryBackoffMs} is negative
   */
  public StreamBuilder recoveryBackoffMs(int recoveryBackoffMs) {
    this.recoveryBackoffMs = requireNonNegative(recoveryBackoffMs, "recoveryBackoffMs");
    return this;
  }

  /**
   * Sets the maximum number of recovery attempts.
   *
   * @param recoveryRetries the maximum number of recovery attempts
   * @return this builder for method chaining
   * @throws IllegalArgumentException if {@code recoveryRetries} is negative
   */
  public StreamBuilder recoveryRetries(int recoveryRetries) {
    this.recoveryRetries = requireNonNegative(recoveryRetries, "recoveryRetries");
    return this;
  }

  /**
   * Sets the timeout for server acknowledgment.
   *
   * @param serverLackOfAckTimeoutMs the server acknowledgment timeout in milliseconds
   * @return this builder for method chaining
   * @throws IllegalArgumentException if {@code serverLackOfAckTimeoutMs} is not positive
   */
  public StreamBuilder serverLackOfAckTimeoutMs(int serverLackOfAckTimeoutMs) {
    this.serverLackOfAckTimeoutMs =
        requirePositive(serverLackOfAckTimeoutMs, "serverLackOfAckTimeoutMs");
    return this;
  }

  /**
   * Sets the timeout for flush operations.
   *
   * @param flushTimeoutMs the flush timeout in milliseconds
   * @return this builder for method chaining
   * @throws IllegalArgumentException if {@code flushTimeoutMs} is not positive
   */
  public StreamBuilder flushTimeoutMs(int flushTimeoutMs) {
    this.flushTimeoutMs = requirePositive(flushTimeoutMs, "flushTimeoutMs");
    return this;
  }

  /**
   * Sets the maximum number of in-flight records.
   *
   * <p>Applies to JSON, Protocol Buffer, and Avro streams. It is ignored for Arrow streams, which
   * use {@link ArrowStreamBuilder#maxInflightBatches(int)} instead.
   *
   * @param maxInflightRecords the maximum number of in-flight records
   * @return this builder for method chaining
   * @throws IllegalArgumentException if {@code maxInflightRecords} is not positive
   */
  public StreamBuilder maxInflightRecords(int maxInflightRecords) {
    this.maxInflightRecords = requirePositive(maxInflightRecords, "maxInflightRecords");
    return this;
  }

  /**
   * Sets the acknowledgment callback.
   *
   * <p>Applies to JSON, Protocol Buffer, and Avro streams. Arrow Flight streams do not support ACK
   * callbacks; configuring one and calling {@link ArrowStreamBuilder#build()} throws {@link
   * IllegalStateException}. Multiplexed streams take their callback through {@link
   * MultiplexedStreamBuilder#ackCallback(AckCallback)} instead; configuring one here and calling
   * {@link MultiplexedStreamBuilder#build()} throws {@link IllegalStateException}.
   *
   * @param ackCallback the acknowledgment callback
   * @return this builder for method chaining
   */
  public StreamBuilder ackCallback(AckCallback ackCallback) {
    this.ackCallback = Objects.requireNonNull(ackCallback, "ackCallback");
    return this;
  }

  /**
   * Selects the JSON record format.
   *
   * @return a {@link JsonStreamBuilder} that builds a {@link ZerobusJsonStream}
   */
  public JsonStreamBuilder json() {
    return new JsonStreamBuilder(this);
  }

  /**
   * Selects the Protocol Buffer record format.
   *
   * @param descriptorProto the Protocol Buffer descriptor proto for the message type
   * @return a {@link ProtoStreamBuilder} that builds a {@link ZerobusProtoStream}
   */
  public ProtoStreamBuilder compiledProto(DescriptorProto descriptorProto) {
    Objects.requireNonNull(descriptorProto, "descriptorProto");
    return new ProtoStreamBuilder(this, descriptorProto);
  }

  /**
   * Selects the Arrow Flight record format.
   *
   * @param schema the Arrow schema describing the columns of the target table
   * @return an {@link ArrowStreamBuilder} that builds a {@link ZerobusArrowStream}
   */
  public ArrowStreamBuilder arrow(Schema schema) {
    Objects.requireNonNull(schema, "schema");
    return new ArrowStreamBuilder(this, schema);
  }

  /**
   * Selects the Avro record format (Beta).
   *
   * <p>Ingesting record objects requires Java 11+ and the {@code org.apache.avro:avro} dependency
   * on the classpath; the pre-encoded {@code byte[]} path needs neither.
   *
   * @param schemaJson the Avro writer schema as a JSON string
   * @return an {@link AvroStreamBuilder} that builds a {@link ZerobusAvroStream}
   */
  public AvroStreamBuilder avro(String schemaJson) {
    return new AvroStreamBuilder(this, requireNonBlank(schemaJson, "schemaJson"));
  }

  // ==================== Package-private helpers ====================

  /** Validates that the required table name and authentication have been configured. */
  void validateRequired() {
    if (isBlank(tableName)) {
      throw new IllegalStateException("table name is required: call table()");
    }
    if (headersProvider == null && (isBlank(clientId) || isBlank(clientSecret))) {
      throw new IllegalStateException(
          "authentication is required: call oauth(), headersProvider(), or federatedAuth()");
    }
  }

  // Numeric setters cross the JNI boundary and are cast to unsigned Rust integers (usize / u64 /
  // u32). A negative value would silently become an enormous positive limit or timeout, so reject
  // out-of-range values here rather than letting them through. {@code streamPausedMaxWaitTimeMs} is
  // the one knob where a negative value is meaningful (it means "wait the full server-specified
  // duration") and is handled separately on the Arrow sub-builder.

  private static int requirePositive(int value, String name) {
    if (value <= 0) {
      throw new IllegalArgumentException(name + " must be positive, got: " + value);
    }
    return value;
  }

  private static long requirePositive(long value, String name) {
    if (value <= 0) {
      throw new IllegalArgumentException(name + " must be positive, got: " + value);
    }
    return value;
  }

  private static int requireNonNegative(int value, String name) {
    if (value < 0) {
      throw new IllegalArgumentException(name + " must not be negative, got: " + value);
    }
    return value;
  }

  private static int requireStreamCount(int streamCount) {
    if (streamCount < 1 || streamCount > MAX_MULTIPLEXED_STREAMS) {
      throw new IllegalArgumentException(
          "streamCount must be between 1 and " + MAX_MULTIPLEXED_STREAMS + ", got: " + streamCount);
    }
    return streamCount;
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (isBlank(value)) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }

  /**
   * Builds JSON and Protocol Buffer stream options, applying only the values that were explicitly
   * set.
   */
  StreamConfigurationOptions toStreamOptions() {
    StreamConfigurationOptions.StreamConfigurationOptionsBuilder builder =
        StreamConfigurationOptions.builder();
    if (maxInflightRecords != null) {
      builder.setMaxInflightRecords(maxInflightRecords);
    }
    if (recovery != null) {
      builder.setRecovery(recovery);
    }
    if (recoveryTimeoutMs != null) {
      builder.setRecoveryTimeoutMs(recoveryTimeoutMs);
    }
    if (recoveryBackoffMs != null) {
      builder.setRecoveryBackoffMs(recoveryBackoffMs);
    }
    if (recoveryRetries != null) {
      builder.setRecoveryRetries(recoveryRetries);
    }
    if (serverLackOfAckTimeoutMs != null) {
      builder.setServerLackOfAckTimeoutMs(serverLackOfAckTimeoutMs);
    }
    if (flushTimeoutMs != null) {
      builder.setFlushTimeoutMs(flushTimeoutMs);
    }
    if (ackCallback != null) {
      builder.setAckCallback(ackCallback);
    }
    return builder.build();
  }

  // ==================== Typed sub-builders ====================

  /** Builds a {@link ZerobusJsonStream}. Created via {@link StreamBuilder#json()}. */
  public static final class JsonStreamBuilder {
    private final StreamBuilder base;

    private JsonStreamBuilder(StreamBuilder base) {
      this.base = base;
    }

    /**
     * Builds and opens the JSON stream.
     *
     * @return a future that completes with the {@link ZerobusJsonStream} when ready
     * @throws IllegalStateException if the table name or authentication has not been set
     */
    public CompletableFuture<ZerobusJsonStream> build() {
      base.validateRequired();
      return base.sdk.createJsonStreamInternal(
          base.tableName,
          base.clientId,
          base.clientSecret,
          base.headersProvider,
          base.toStreamOptions());
    }

    /**
     * Selects a multiplexed stream (Beta) of {@code streamCount} sub-streams.
     *
     * <p>Use multiplexing when one stream is the throughput bottleneck and global ordering is not
     * required. Records keep their order within a sub-stream, but there is no order across
     * sub-streams. The {@link StreamBuilder#maxInflightRecords(int)} budget is divided evenly
     * across sub-streams and must be at least {@code streamCount}; otherwise the future returned by
     * {@link MultiplexedStreamBuilder#build()} fails with a {@link NonRetriableException}.
     *
     * @param streamCount the number of sub-streams, from 1 to 64
     * @return a {@link MultiplexedStreamBuilder} that builds a {@link ZerobusMultiplexedJsonStream}
     * @throws IllegalArgumentException if {@code streamCount} is not between 1 and 64
     */
    public MultiplexedStreamBuilder<ZerobusMultiplexedJsonStream> multiplexed(int streamCount) {
      return new MultiplexedStreamBuilder<>(
          base,
          requireStreamCount(streamCount),
          RecordFormat.JSON,
          null,
          null,
          ZerobusMultiplexedJsonStream::new);
    }
  }

  /** Builds a {@link ZerobusAvroStream}. Created via {@link StreamBuilder#avro(String)}. */
  public static final class AvroStreamBuilder {
    private final StreamBuilder base;
    private final String schemaJson;

    private AvroStreamBuilder(StreamBuilder base, String schemaJson) {
      this.base = base;
      this.schemaJson = schemaJson;
    }

    /**
     * Builds and opens the Avro stream.
     *
     * @return a future that completes with the {@link ZerobusAvroStream} when ready
     * @throws IllegalStateException if the table name or authentication has not been set
     */
    public CompletableFuture<ZerobusAvroStream> build() {
      base.validateRequired();
      return base.sdk.createAvroStreamInternal(
          base.tableName,
          schemaJson,
          base.clientId,
          base.clientSecret,
          base.headersProvider,
          base.toStreamOptions());
    }
  }

  /** Builds a {@link ZerobusProtoStream}. Created via {@link StreamBuilder#compiledProto}. */
  public static final class ProtoStreamBuilder {
    private final StreamBuilder base;
    private final DescriptorProto descriptorProto;

    private ProtoStreamBuilder(StreamBuilder base, DescriptorProto descriptorProto) {
      this.base = base;
      this.descriptorProto = descriptorProto;
    }

    /**
     * Builds and opens the Protocol Buffer stream.
     *
     * @return a future that completes with the {@link ZerobusProtoStream} when ready
     * @throws IllegalStateException if the table name or authentication has not been set
     */
    public CompletableFuture<ZerobusProtoStream> build() {
      base.validateRequired();
      return base.sdk.createProtoStreamInternal(
          base.tableName,
          descriptorProto,
          base.clientId,
          base.clientSecret,
          base.headersProvider,
          base.toStreamOptions());
    }

    /**
     * Selects a multiplexed stream (Beta) of {@code streamCount} sub-streams.
     *
     * <p>Use multiplexing when one stream is the throughput bottleneck and global ordering is not
     * required. Records keep their order within a sub-stream, but there is no order across
     * sub-streams. The {@link StreamBuilder#maxInflightRecords(int)} budget is divided evenly
     * across sub-streams and must be at least {@code streamCount}; otherwise the future returned by
     * {@link MultiplexedStreamBuilder#build()} fails with a {@link NonRetriableException}.
     *
     * @param streamCount the number of sub-streams, from 1 to 64
     * @return a {@link MultiplexedStreamBuilder} that builds a {@link
     *     ZerobusMultiplexedProtoStream}
     * @throws IllegalArgumentException if {@code streamCount} is not between 1 and 64
     */
    public MultiplexedStreamBuilder<ZerobusMultiplexedProtoStream> multiplexed(int streamCount) {
      return new MultiplexedStreamBuilder<>(
          base,
          requireStreamCount(streamCount),
          RecordFormat.PROTO,
          descriptorProto.toByteArray(),
          null,
          ZerobusMultiplexedProtoStream::new);
    }
  }

  /**
   * Builds a multiplexed stream (Beta). Created via {@code multiplexed(int)} on {@link
   * JsonStreamBuilder} or {@link ProtoStreamBuilder}.
   *
   * <p>Construction opens every sub-stream concurrently, each after a random delay of up to one
   * second when more than one is requested. It is atomic: if any sub-stream fails to open, the
   * others are closed and the returned future fails.
   *
   * <h3>Message IDs</h3>
   *
   * <p>Ingest methods return a message ID: an opaque {@code long} that identifies the sub-stream
   * and the record's position in it. Message IDs are not ordered and may be negative. Pass one to
   * {@code waitForMessageId(long)} on the stream that returned it; do not compare them or use them
   * with another stream.
   *
   * <h3>Failures</h3>
   *
   * <p>After any exception, check {@code isClosed()}: a retryable error is also terminal when
   * recovery is off or exhausted. If it returns true, a sub-stream failed terminally and the stream
   * rejects new ingestion: recover the records that were never acknowledged with {@code
   * getUnackedRecords()} or {@code getUnackedBatches()}, call {@code close()}, and ingest them into
   * a new stream. If it returns false, only that call failed.
   *
   * <h3>Resource Management</h3>
   *
   * <p>Multiplexed streams hold native resources that are not automatically released by the garbage
   * collector. You <b>must</b> call {@code close()} when done to avoid native memory leaks, even if
   * {@code isClosed()} returns true or the unacknowledged records were already read. Use
   * try-with-resources for automatic cleanup.
   *
   * <h3>Thread Safety</h3>
   *
   * <p>Multiplexed streams are <b>thread-safe</b>, unlike the other stream types. Several threads
   * can ingest, wait for message IDs, and flush the same stream concurrently. Records from
   * different threads have no order relative to each other. {@code close()} and the
   * unacknowledged-record getters can be called from any thread. {@code close()} waits for calls
   * already in progress to return, and later ingest, wait, and flush calls throw {@link
   * NonRetriableException}. When {@code close()} returns, all callbacks have run, unless it waited
   * longer than the callback wait time (up to 5 seconds; sub-streams wait in parallel; not
   * configurable in Java). After a failed {@code close()}, a second call returns without an error,
   * unless reading the unacknowledged records failed: then the stream keeps its native resources
   * and {@code close()} can be called again. An {@link AckCallback} must still not call the
   * stream's blocking methods.
   *
   * @param <S> the multiplexed stream type
   */
  public static final class MultiplexedStreamBuilder<S> {
    private final StreamBuilder base;
    private final int streamCount;
    private final RecordFormat format;
    private final byte[] descriptorProto;
    private final String avroSchemaJson;
    private final BaseZerobusMultiplexedStream.Factory<S> factory;

    private AckCallback ackCallback;

    private MultiplexedStreamBuilder(
        StreamBuilder base,
        int streamCount,
        RecordFormat format,
        byte[] descriptorProto,
        String avroSchemaJson,
        BaseZerobusMultiplexedStream.Factory<S> factory) {
      this.base = base;
      this.streamCount = streamCount;
      this.format = format;
      this.descriptorProto = descriptorProto;
      this.avroSchemaJson = avroSchemaJson;
      this.factory = factory;
    }

    /**
     * Sets the acknowledgment callback, shared by every sub-stream.
     *
     * <p>The callback's {@code long} argument is the message ID returned by the ingest methods, not
     * an offset. Sub-streams may invoke the callback concurrently and in no global order, so it
     * must be thread-safe.
     *
     * @param ackCallback the acknowledgment callback
     * @return this builder for method chaining
     */
    public MultiplexedStreamBuilder<S> ackCallback(AckCallback ackCallback) {
      this.ackCallback = Objects.requireNonNull(ackCallback, "ackCallback");
      return this;
    }

    /**
     * Builds and opens the multiplexed stream.
     *
     * @return a future that completes with the multiplexed stream when every sub-stream is ready
     * @throws IllegalStateException if the table name or authentication has not been set, or if an
     *     ACK callback was configured via {@link StreamBuilder#ackCallback(AckCallback)}
     */
    public CompletableFuture<S> build() {
      base.validateRequired();
      if (base.ackCallback != null) {
        throw new IllegalStateException(
            "StreamBuilder.ackCallback() is only valid for ordinary streams; use"
                + " multiplexed(n).ackCallback() for multiplexed streams");
      }
      return base.sdk.createMultiplexedStreamInternal(
          base.tableName,
          format,
          descriptorProto,
          avroSchemaJson,
          base.clientId,
          base.clientSecret,
          base.headersProvider,
          base.toStreamOptions(),
          ackCallback,
          streamCount,
          factory);
    }
  }

  /** Builds a {@link ZerobusArrowStream}. Created via {@link StreamBuilder#arrow(Schema)}. */
  public static final class ArrowStreamBuilder {
    private final StreamBuilder base;
    private final Schema schema;

    private Integer maxInflightBatches;
    private Long connectionTimeoutMs;
    private IPCCompressionType ipcCompression;
    private Long streamPausedMaxWaitTimeMs;

    private ArrowStreamBuilder(StreamBuilder base, Schema schema) {
      this.base = base;
      this.schema = schema;
    }

    /**
     * Sets the maximum number of in-flight Arrow batches.
     *
     * @param maxInflightBatches the maximum number of in-flight batches
     * @return this builder for method chaining
     * @throws IllegalArgumentException if {@code maxInflightBatches} is not positive
     */
    public ArrowStreamBuilder maxInflightBatches(int maxInflightBatches) {
      this.maxInflightBatches = requirePositive(maxInflightBatches, "maxInflightBatches");
      return this;
    }

    /**
     * Sets the connection timeout for the Arrow Flight connection.
     *
     * @param connectionTimeoutMs the connection timeout in milliseconds
     * @return this builder for method chaining
     * @throws IllegalArgumentException if {@code connectionTimeoutMs} is not positive
     */
    public ArrowStreamBuilder connectionTimeoutMs(long connectionTimeoutMs) {
      this.connectionTimeoutMs = requirePositive(connectionTimeoutMs, "connectionTimeoutMs");
      return this;
    }

    /**
     * Sets the Arrow IPC compression codec. Defaults to {@link IPCCompressionType#ZSTD}; pass
     * {@link IPCCompressionType#NONE} to send uncompressed payloads.
     *
     * @param ipcCompression the compression codec to use, or {@code null} for the default
     * @return this builder for method chaining
     */
    public ArrowStreamBuilder ipcCompression(IPCCompressionType ipcCompression) {
      this.ipcCompression = ipcCompression;
      return this;
    }

    /**
     * Sets the maximum wait time during a graceful stream pause.
     *
     * @param streamPausedMaxWaitTimeMs the maximum wait time in milliseconds, or a negative value
     *     to wait the full server-specified duration
     * @return this builder for method chaining
     */
    public ArrowStreamBuilder streamPausedMaxWaitTimeMs(long streamPausedMaxWaitTimeMs) {
      this.streamPausedMaxWaitTimeMs = streamPausedMaxWaitTimeMs;
      return this;
    }

    /** Builds the Arrow stream options, applying only the values that were explicitly set. */
    ArrowStreamConfigurationOptions buildOptions() {
      ArrowStreamConfigurationOptions.ArrowStreamConfigurationOptionsBuilder builder =
          ArrowStreamConfigurationOptions.builder();
      if (base.recovery != null) {
        builder.setRecovery(base.recovery);
      }
      if (base.recoveryTimeoutMs != null) {
        builder.setRecoveryTimeoutMs(base.recoveryTimeoutMs);
      }
      if (base.recoveryBackoffMs != null) {
        builder.setRecoveryBackoffMs(base.recoveryBackoffMs);
      }
      if (base.recoveryRetries != null) {
        builder.setRecoveryRetries(base.recoveryRetries);
      }
      if (base.serverLackOfAckTimeoutMs != null) {
        builder.setServerLackOfAckTimeoutMs(base.serverLackOfAckTimeoutMs);
      }
      if (base.flushTimeoutMs != null) {
        builder.setFlushTimeoutMs(base.flushTimeoutMs);
      }
      if (maxInflightBatches != null) {
        builder.setMaxInflightBatches(maxInflightBatches);
      }
      if (connectionTimeoutMs != null) {
        builder.setConnectionTimeoutMs(connectionTimeoutMs);
      }
      if (ipcCompression != null) {
        builder.setIpcCompression(ipcCompression);
      }
      if (streamPausedMaxWaitTimeMs != null) {
        builder.setStreamPausedMaxWaitTimeMs(streamPausedMaxWaitTimeMs);
      }
      return builder.build();
    }

    /**
     * Builds and opens the Arrow Flight stream.
     *
     * @return a future that completes with the {@link ZerobusArrowStream} when ready
     * @throws IllegalStateException if the table name or authentication has not been set, or if an
     *     ACK callback was configured via {@link StreamBuilder#ackCallback(AckCallback)}
     */
    public CompletableFuture<ZerobusArrowStream> build() {
      base.validateRequired();
      if (base.ackCallback != null) {
        throw new IllegalStateException("ackCallback is not supported for Arrow Flight streams");
      }
      return base.sdk.createArrowStreamInternal(
          base.tableName,
          schema,
          base.clientId,
          base.clientSecret,
          base.headersProvider,
          buildOptions());
    }
  }
}
