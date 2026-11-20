package com.databricks.zerobus.conformance;

import com.databricks.zerobus.HeadersProvider;
import com.databricks.zerobus.NonRetriableException;
import com.databricks.zerobus.ZerobusArrowStream;
import com.databricks.zerobus.ZerobusException;
import com.databricks.zerobus.ZerobusProtoStream;
import com.databricks.zerobus.ZerobusSdk;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zerobus.conformance.harness.TestHarnessGrpc;
import zerobus.conformance.harness.TestHarnessOuterClass.ActionResponse;
import zerobus.conformance.harness.TestHarnessOuterClass.AwaitRequest;
import zerobus.conformance.harness.TestHarnessOuterClass.AwaitResponse;
import zerobus.conformance.harness.TestHarnessOuterClass.CloseRequest;
import zerobus.conformance.harness.TestHarnessOuterClass.CreateStreamRequest;
import zerobus.conformance.harness.TestHarnessOuterClass.CreateStreamResponse;
import zerobus.conformance.harness.TestHarnessOuterClass.Empty;
import zerobus.conformance.harness.TestHarnessOuterClass.FlushRequest;
import zerobus.conformance.harness.TestHarnessOuterClass.IngestRecordRequest;
import zerobus.conformance.harness.TestHarnessOuterClass.IngestRecordResponse;
import zerobus.conformance.harness.TestHarnessOuterClass.IngestRecordsRequest;
import zerobus.conformance.harness.TestHarnessOuterClass.IngestRecordsResponse;
import zerobus.conformance.harness.TestHarnessOuterClass.Protocol;
import zerobus.conformance.harness.TestHarnessOuterClass.RecordFormat;
import zerobus.conformance.harness.TestHarnessOuterClass.SdkError;

/**
 * {@code TestHarness} service: maps each contract RPC onto the Java SDK.
 *
 * <p>Supported: {@code PROTOCOL_GRPC} with {@code RECORD_FORMAT_PROTO}, and {@code
 * PROTOCOL_ARROW_FLIGHT} with {@code RECORD_FORMAT_ARROW}. Every other combination is rejected
 * (see {@link #checkSliceSupported}).
 *
 * <p>Pass-through: each RPC makes the SDK call the contract names. An SDK {@link ZerobusException}
 * is reported in the response body; any other exception is a harness fault and becomes a gRPC
 * status. The harness-side work is converting JSON records into SDK inputs, mapping {@code
 * stream_id} to the open SDK stream, and rejecting an empty batch (the SDK assigns it no offset).
 *
 * <p>Concurrency: the Java SDK is <b>not thread-safe</b>. Every RPC that touches a stream runs under
 * that stream's lock (see {@link #callLocked}), which serializes ingest / await / flush / close per
 * stream. The stream map itself is a {@link ConcurrentHashMap}.
 *
 * <p>Auth: the mock checks no credentials, so each stream uses a headers provider that returns no
 * headers (equivalent to the Rust harness's {@code no_auth()}). The SDK is built with {@link
 * ZerobusSdk#forTestingNoTls(String)} (plaintext, {@code http://} endpoint).
 */
final class HarnessService extends TestHarnessGrpc.TestHarnessImplBase {
  private static final Logger logger = LoggerFactory.getLogger(HarnessService.class);

  /** No auth headers at all. The mock accepts unauthenticated streams. */
  private static final HeadersProvider NO_AUTH_HEADERS =
      () -> Collections.<String, String>emptyMap();

  private final ConcurrentHashMap<String, StreamEntry> streams = new ConcurrentHashMap<>();
  private final AtomicLong nextStreamId = new AtomicLong();
  private final CountDownLatch shutdownRequested;

  /**
   * One open SDK stream. Subclasses hold the SDK stream of their protocol and the schema its records
   * are built against. Each method must be called with {@link #lock} held.
   */
  abstract static class StreamEntry {
    final ZerobusSdk sdk;
    /** Serializes all SDK calls on this stream (the Java SDK is not thread-safe). */
    final Object lock = new Object();

    StreamEntry(ZerobusSdk sdk) {
      this.sdk = sdk;
    }

    /** Queues one record (queue-only, no ack wait) and returns its offset. */
    abstract long ingest(String recordJson) throws ZerobusException;

    /** Queues a batch through the SDK's batch ingest. An empty batch gets no offset. */
    abstract Optional<Long> ingestBatch(List<String> recordJsons) throws ZerobusException;

    abstract void waitForOffset(long offset) throws ZerobusException;

    abstract void flush() throws ZerobusException;

    /**
     * Closes the SDK stream. The entry stays registered, so later calls reach the SDK and it reports
     * its own error. Caller holds the lock.
     */
    abstract void closeStream() throws ZerobusException;

    /**
     * Shutdown only: closes the stream, then the SDK handle (Java has no finalizers). Caller holds
     * the lock.
     */
    void release() throws ZerobusException {
      try {
        closeStream();
      } finally {
        sdk.close();
      }
    }
  }

  /**
   * {@code PROTOCOL_GRPC} + {@code RECORD_FORMAT_PROTO}: records are dynamic messages encoded by the
   * SDK.
   */
  static final class ProtoStreamEntry extends StreamEntry {
    final ZerobusProtoStream stream;
    final Descriptor descriptor;

    ProtoStreamEntry(ZerobusSdk sdk, ZerobusProtoStream stream, Descriptor descriptor) {
      super(sdk);
      this.stream = stream;
      this.descriptor = descriptor;
    }

    @Override
    long ingest(String recordJson) throws ZerobusException {
      // Queue only. This returns once the SDK has enqueued the record, not once it is
      // acknowledged. The SDK encodes the message itself.
      return stream.ingestRecordOffset(toMessage(recordJson));
    }

    @Override
    Optional<Long> ingestBatch(List<String> recordJsons) throws ZerobusException {
      List<Message> messages = new ArrayList<>(recordJsons.size());
      for (String recordJson : recordJsons) {
        messages.add(toMessage(recordJson));
      }
      return stream.ingestRecordsOffset(messages);
    }

    private Message toMessage(String recordJson) {
      try {
        return Records.toMessage(descriptor, recordJson);
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException(
            "record_json does not match the schema: " + e.getMessage(), e);
      }
    }

    @Override
    void waitForOffset(long offset) throws ZerobusException {
      stream.waitForOffset(offset);
    }

    @Override
    void flush() throws ZerobusException {
      stream.flush();
    }

    @Override
    void closeStream() throws ZerobusException {
      stream.close();
    }
  }

  /**
   * {@code PROTOCOL_ARROW_FLIGHT} + {@code RECORD_FORMAT_ARROW}: each batch is a {@link
   * VectorSchemaRoot} with one row per record. The SDK serializes the batch to IPC synchronously
   * inside {@code ingestBatch}, so the root is closed as soon as ingest returns.
   */
  static final class ArrowStreamEntry extends StreamEntry {
    final ZerobusArrowStream stream;
    final Schema schema;
    /**
     * Owns the batch buffers. Stays open until {@link #release()}, so calls after close still reach
     * the SDK instead of failing on a closed allocator.
     */
    final BufferAllocator allocator = new RootAllocator();

    ArrowStreamEntry(ZerobusSdk sdk, ZerobusArrowStream stream, Schema schema) {
      super(sdk);
      this.stream = stream;
      this.schema = schema;
    }

    @Override
    long ingest(String recordJson) throws ZerobusException {
      return ingestBatch(Collections.singletonList(recordJson))
          .orElseThrow(
              () -> new IllegalStateException("ingestBatch returned no offset for a one-row batch"));
    }

    @Override
    Optional<Long> ingestBatch(List<String> recordJsons) throws ZerobusException {
      try (VectorSchemaRoot batch = Records.batch(schema, allocator, recordJsons)) {
        return stream.ingestBatch(batch);
      }
    }

    @Override
    void waitForOffset(long offset) throws ZerobusException {
      stream.waitForOffset(offset);
    }

    @Override
    void flush() throws ZerobusException {
      stream.flush();
    }

    @Override
    void closeStream() throws ZerobusException {
      stream.close();
    }

    @Override
    void release() throws ZerobusException {
      try {
        super.release();
      } finally {
        allocator.close();
      }
    }
  }

  /** The SDK's reply to one call: {@code value} on success, or {@code error} when the SDK rejected it. */
  static final class SdkReply<T> {
    final T value;
    final SdkError error;

    private SdkReply(T value, SdkError error) {
      this.value = value;
      this.error = error;
    }
  }

  /** One SDK call on a stream. An SDK failure is thrown as {@link ZerobusException}. */
  @FunctionalInterface
  interface SdkCall<T> {
    T call() throws ZerobusException;
  }

  /**
   * Runs one SDK call under the stream's lock. An SDK {@link ZerobusException} becomes the reply's
   * error. Any other exception propagates and maps onto a gRPC status.
   */
  static <T> SdkReply<T> callLocked(StreamEntry entry, SdkCall<T> call) {
    synchronized (entry.lock) {
      try {
        return new SdkReply<>(call.call(), null);
      } catch (ZerobusException e) {
        return new SdkReply<>(null, sdkError(e));
      }
    }
  }

  HarnessService(CountDownLatch shutdownRequested) {
    this.shutdownRequested = shutdownRequested;
  }

  @Override
  public void createStream(CreateStreamRequest req, StreamObserver<CreateStreamResponse> out) {
    try {
      checkSliceSupported(req.getProtocol(), req.getFormat());

      logger.info(
          "CreateStream protocol={} endpoint={} table={}",
          req.getProtocol(),
          req.getServerEndpoint(),
          req.getTablePath());
      // The mock is a local plaintext server, so no TLS (`http://`) and no auth headers.
      ZerobusSdk sdk = ZerobusSdk.forTestingNoTls("http://" + req.getServerEndpoint());
      StreamEntry entry;
      try {
        entry =
            req.getProtocol() == Protocol.PROTOCOL_ARROW_FLIGHT
                ? openArrowStream(sdk, req)
                : openProtoStream(sdk, req);
      } catch (Throwable t) {
        sdk.close();
        throw t;
      }

      String streamId = "stream-" + nextStreamId.getAndIncrement();
      streams.put(streamId, entry);
      out.onNext(CreateStreamResponse.newBuilder().setStreamId(streamId).build());
      out.onCompleted();
    } catch (ZerobusException e) {
      // The SDK rejected the stream: report it in the body; stream_id stays unset.
      out.onNext(CreateStreamResponse.newBuilder().setError(sdkError(e)).build());
      out.onCompleted();
    } catch (Throwable t) {
      out.onError(toStatus(t));
    }
  }

  /** Parses the proto writer schema, then opens the gRPC stream. */
  private static StreamEntry openProtoStream(ZerobusSdk sdk, CreateStreamRequest req)
      throws Exception {
    DescriptorProto descriptorProto = Records.descriptorProto(req.getDescriptorProtoBytes());
    Descriptor descriptor = Records.messageDescriptor(descriptorProto);
    ZerobusProtoStream stream =
        awaitStream(
            sdk.streamBuilder()
                .table(req.getTablePath())
                .headersProvider(NO_AUTH_HEADERS)
                .compiledProto(descriptorProto)
                .build());
    return new ProtoStreamEntry(sdk, stream, descriptor);
  }

  /** Decodes the Arrow schema from IPC, then opens the Arrow Flight stream. */
  private static StreamEntry openArrowStream(ZerobusSdk sdk, CreateStreamRequest req)
      throws Exception {
    Schema schema = Records.arrowSchema(req.getArrowSchemaIpc());
    ZerobusArrowStream stream =
        awaitStream(
            sdk.streamBuilder()
                .table(req.getTablePath())
                .headersProvider(NO_AUTH_HEADERS)
                .arrow(schema)
                .build());
    return new ArrowStreamEntry(sdk, stream, schema);
  }

  /**
   * Blocks on a stream-creation future. An SDK failure is rethrown as {@link ZerobusException} (it
   * goes into the response body); anything else maps onto a gRPC status.
   */
  private static <T> T awaitStream(CompletableFuture<T> future) throws ZerobusException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Status.CANCELLED.withDescription("interrupted").asRuntimeException();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof ZerobusException) {
        throw (ZerobusException) e.getCause();
      }
      throw toStatus(e.getCause());
    }
  }

  /**
   * Maps an SDK {@link ZerobusException} onto the contract's {@code SdkError}. The message passes
   * through verbatim. {@code retryable} follows the SDK's own classification: only {@link
   * NonRetriableException} is non-retryable.
   */
  static SdkError sdkError(ZerobusException e) {
    return SdkError.newBuilder()
        // A null message becomes "": the SdkError's presence, not its text, marks the failure.
        .setMessage(Optional.ofNullable(e.getMessage()).orElse(""))
        .setRetryable(!(e instanceof NonRetriableException))
        .build();
  }

  @Override
  public void ingestRecord(IngestRecordRequest req, StreamObserver<IngestRecordResponse> out) {
    try {
      StreamEntry entry = lookup(req.getStreamId());
      SdkReply<Long> reply = callLocked(entry, () -> entry.ingest(req.getRecordJson()));
      IngestRecordResponse.Builder response = IngestRecordResponse.newBuilder();
      if (reply.error != null) {
        response.setError(reply.error);
      } else {
        response.setOffset(reply.value);
      }
      out.onNext(response.build());
      out.onCompleted();
    } catch (Throwable t) {
      out.onError(toStatus(t));
    }
  }

  /**
   * Queues the batch through the SDK's batch ingest, which returns one offset for the whole batch.
   * The response carries that offset, or the SDK's error. The SDK gives an empty batch no offset, so
   * the harness rejects it as INVALID_ARGUMENT.
   */
  @Override
  public void ingestRecords(IngestRecordsRequest req, StreamObserver<IngestRecordsResponse> out) {
    try {
      StreamEntry entry = lookup(req.getStreamId());
      SdkReply<Optional<Long>> reply =
          callLocked(entry, () -> entry.ingestBatch(req.getRecordJsonList()));
      IngestRecordsResponse.Builder response = IngestRecordsResponse.newBuilder();
      if (reply.error != null) {
        response.setError(reply.error);
      } else if (!reply.value.isPresent()) {
        throw Status.INVALID_ARGUMENT
            .withDescription("batch is empty: the SDK assigns no offset to an empty batch")
            .asRuntimeException();
      } else {
        response.setOffset(reply.value.get());
      }
      out.onNext(response.build());
      out.onCompleted();
    } catch (Throwable t) {
      out.onError(toStatus(t));
    }
  }

  @Override
  public void awaitWriteCompleted(AwaitRequest req, StreamObserver<AwaitResponse> out) {
    try {
      StreamEntry entry = lookup(req.getStreamId());
      SdkReply<Void> reply =
          callLocked(
              entry,
              () -> {
                entry.waitForOffset(req.getOffset());
                return null;
              });
      AwaitResponse.Builder response = AwaitResponse.newBuilder();
      // An unset error means the write was acknowledged.
      if (reply.error != null) {
        response.setError(reply.error);
      }
      out.onNext(response.build());
      out.onCompleted();
    } catch (Throwable t) {
      out.onError(toStatus(t));
    }
  }

  @Override
  public void flush(FlushRequest req, StreamObserver<ActionResponse> out) {
    try {
      StreamEntry entry = lookup(req.getStreamId());
      SdkReply<Void> reply =
          callLocked(
              entry,
              () -> {
                entry.flush();
                return null;
              });
      ActionResponse.Builder response = ActionResponse.newBuilder();
      if (reply.error != null) {
        response.setError(reply.error);
      }
      out.onNext(response.build());
      out.onCompleted();
    } catch (Throwable t) {
      out.onError(toStatus(t));
    }
  }

  @Override
  public void close(CloseRequest req, StreamObserver<ActionResponse> out) {
    try {
      // The entry stays registered after close, so later calls on this id reach the SDK. Only an id
      // that was never created is NOT_FOUND. The SDK handle is released at shutdown (closeAll).
      StreamEntry entry = lookup(req.getStreamId());
      SdkReply<Void> reply =
          callLocked(
              entry,
              () -> {
                entry.closeStream();
                return null;
              });
      ActionResponse.Builder response = ActionResponse.newBuilder();
      if (reply.error != null) {
        response.setError(reply.error);
      }
      logger.info("stream closed: {}", req.getStreamId());
      out.onNext(response.build());
      out.onCompleted();
    } catch (Throwable t) {
      out.onError(toStatus(t));
    }
  }

  @Override
  public void shutdown(Empty req, StreamObserver<Empty> out) {
    logger.info("Shutdown RPC received");
    // The server finishes this response before exiting (graceful shutdown, see Main).
    shutdownRequested.countDown();
    out.onNext(Empty.getDefaultInstance());
    out.onCompleted();
  }

  /** Releases every stream still registered. Called from Main during shutdown. */
  void closeAll() {
    for (Map.Entry<String, StreamEntry> e : streams.entrySet()) {
      synchronized (e.getValue().lock) {
        try {
          e.getValue().release();
        } catch (Throwable t) {
          logger.warn("closing stream {} failed: {}", e.getKey(), t.toString());
        }
      }
    }
  }

  private StreamEntry lookup(String streamId) {
    StreamEntry entry = streams.get(streamId);
    if (entry == null) {
      throw Status.NOT_FOUND
          .withDescription("unknown stream_id '" + streamId + "'")
          .asRuntimeException();
    }
    return entry;
  }

  /**
   * Rejects every protocol/format combination this harness does not support. A valid pairing the
   * harness does not wire yet is UNIMPLEMENTED. An invalid pairing is INVALID_ARGUMENT.
   */
  static void checkSliceSupported(Protocol protocol, RecordFormat format) {
    if (protocol == Protocol.PROTOCOL_GRPC && format == RecordFormat.RECORD_FORMAT_PROTO) {
      return;
    }
    if (protocol == Protocol.PROTOCOL_ARROW_FLIGHT && format == RecordFormat.RECORD_FORMAT_ARROW) {
      return;
    }
    if (protocol == Protocol.PROTOCOL_GRPC && format == RecordFormat.RECORD_FORMAT_JSON) {
      // Valid pairing. The SDK has ZerobusJsonStream; this harness has no JSON stream path yet.
      throw Status.UNIMPLEMENTED
          .withDescription("GRPC + JSON is not implemented in the Java harness yet")
          .asRuntimeException();
    }
    if (protocol == Protocol.PROTOCOL_GRPC && format == RecordFormat.RECORD_FORMAT_AVRO) {
      // Valid pairing. The SDK has ZerobusAvroStream; this harness has no Avro stream path yet.
      throw Status.UNIMPLEMENTED
          .withDescription("GRPC + AVRO is not implemented in the Java harness yet")
          .asRuntimeException();
    }
    if (protocol == Protocol.PROTOCOL_GRPC && format == RecordFormat.RECORD_FORMAT_ARROW) {
      throw Status.INVALID_ARGUMENT
          .withDescription("GRPC + ARROW is not a valid pairing; ARROW rides ARROW_FLIGHT")
          .asRuntimeException();
    }
    if (protocol == Protocol.PROTOCOL_ARROW_FLIGHT) {
      throw Status.INVALID_ARGUMENT
          .withDescription("ARROW_FLIGHT carries only the ARROW format")
          .asRuntimeException();
    }
    throw Status.INVALID_ARGUMENT
        .withDescription("protocol and format must be specified")
        .asRuntimeException();
  }

  /** Maps a harness fault onto a gRPC status. Argument problems stay INVALID_ARGUMENT. */
  static StatusRuntimeException toStatus(Throwable t) {
    if (t instanceof StatusRuntimeException) {
      return (StatusRuntimeException) t;
    }
    if (t instanceof InvalidProtocolBufferException || t instanceof DescriptorValidationException) {
      return Status.INVALID_ARGUMENT.withDescription(t.getMessage()).withCause(t).asRuntimeException();
    }
    if (t instanceof IllegalArgumentException) {
      return Status.INVALID_ARGUMENT.withDescription(t.getMessage()).withCause(t).asRuntimeException();
    }
    if (t instanceof IllegalStateException) {
      return Status.FAILED_PRECONDITION.withDescription(t.getMessage()).withCause(t).asRuntimeException();
    }
    return Status.INTERNAL.withDescription(String.valueOf(t.getMessage())).withCause(t).asRuntimeException();
  }
}
