package com.databricks.zerobus.conformance;

import com.databricks.zerobus.HeadersProvider;
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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zerobus.conformance.harness.TestHarnessGrpc;
import zerobus.conformance.harness.TestHarnessOuterClass.AwaitRequest;
import zerobus.conformance.harness.TestHarnessOuterClass.AwaitResponse;
import zerobus.conformance.harness.TestHarnessOuterClass.CloseRequest;
import zerobus.conformance.harness.TestHarnessOuterClass.CreateStreamRequest;
import zerobus.conformance.harness.TestHarnessOuterClass.CreateStreamResponse;
import zerobus.conformance.harness.TestHarnessOuterClass.Empty;
import zerobus.conformance.harness.TestHarnessOuterClass.FlushRequest;
import zerobus.conformance.harness.TestHarnessOuterClass.IngestRecordRequest;
import zerobus.conformance.harness.TestHarnessOuterClass.IngestRecordResponse;
import zerobus.conformance.harness.TestHarnessOuterClass.Protocol;
import zerobus.conformance.harness.TestHarnessOuterClass.RecordFormat;

/**
 * {@code TestHarness} service: maps each contract RPC onto the Java SDK. Mirrors {@code
 * conformance/harness/rust/src/harness.rs}.
 *
 * <p>Supported: {@code GRPC} with {@code PROTO}, and {@code ARROW_FLIGHT} with {@code ARROW}. Every
 * other combination is rejected (see {@link #checkSliceSupported}).
 *
 * <p>Concurrency: the Java SDK is <b>not thread-safe</b>. Every RPC that touches a stream runs under
 * that stream's lock ({@link StreamEntry#lock}), which serializes ingest / await / flush / close per
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

  /**
   * Runs {@code waitForOffset} off the RPC thread so {@code timeout_ms} can be enforced. The worker
   * still holds the stream lock while it waits (the SDK call cannot be cancelled), so later RPCs on
   * the same stream queue behind it rather than racing the SDK.
   */
  private final ExecutorService waiters =
      Executors.newCachedThreadPool(
          r -> {
            Thread t = new Thread(r, "harness-await");
            t.setDaemon(true);
            return t;
          });

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

    abstract void waitForOffset(long offset) throws ZerobusException;

    abstract void flush() throws ZerobusException;

    /** Closes the SDK stream and any Arrow resources it owns. Does not close {@link #sdk}. */
    abstract void closeStream() throws ZerobusException;

    /** Closes the stream, then the SDK handle (Java has no finalizers). Caller holds the lock. */
    final void release() throws ZerobusException {
      try {
        closeStream();
      } finally {
        sdk.close();
      }
    }
  }

  /** {@code GRPC} + {@code PROTO}: records are dynamic messages encoded by the SDK. */
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
      Message message;
      try {
        message = Records.toMessage(descriptor, recordJson);
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException(
            "record_json does not match the schema: " + e.getMessage(), e);
      }
      // Queue only. This returns once the SDK has enqueued the record, not once it is
      // acknowledged. The SDK encodes the message itself.
      return stream.ingestRecordOffset(message);
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
   * {@code ARROW_FLIGHT} + {@code ARROW}: each record is a one-row {@link VectorSchemaRoot}. The SDK
   * serializes the batch to IPC synchronously inside {@code ingestBatch}, so the root is closed as
   * soon as ingest returns.
   */
  static final class ArrowStreamEntry extends StreamEntry {
    final ZerobusArrowStream stream;
    final Schema schema;
    /** Owns the batch buffers. Created per stream and closed with it. */
    final BufferAllocator allocator = new RootAllocator();

    ArrowStreamEntry(ZerobusSdk sdk, ZerobusArrowStream stream, Schema schema) {
      super(sdk);
      this.stream = stream;
      this.schema = schema;
    }

    @Override
    long ingest(String recordJson) throws ZerobusException {
      try (VectorSchemaRoot batch = Records.oneRowBatch(schema, allocator, recordJson)) {
        return stream
            .ingestBatch(batch)
            .orElseThrow(
                () -> new IllegalStateException("ingestBatch returned no offset for a one-row batch"));
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
      try {
        stream.close();
      } finally {
        allocator.close();
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
            req.getProtocol() == Protocol.ARROW_FLIGHT
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

  /** Blocks on a stream-creation future, mapping interrupt and SDK failure onto gRPC statuses. */
  private static <T> T awaitStream(CompletableFuture<T> future) {
    try {
      return future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Status.CANCELLED.withDescription("interrupted").asRuntimeException();
    } catch (ExecutionException e) {
      throw toStatus(e.getCause());
    }
  }

  @Override
  public void ingestRecord(IngestRecordRequest req, StreamObserver<IngestRecordResponse> out) {
    try {
      StreamEntry entry = lookup(req.getStreamId());
      long offset;
      synchronized (entry.lock) {
        offset = entry.ingest(req.getRecordJson());
      }
      out.onNext(IngestRecordResponse.newBuilder().setOffset(offset).build());
      out.onCompleted();
    } catch (Throwable t) {
      out.onError(toStatus(t));
    }
  }

  @Override
  public void awaitWriteCompleted(AwaitRequest req, StreamObserver<AwaitResponse> out) {
    try {
      StreamEntry entry = lookup(req.getStreamId());
      String error = awaitOffset(entry, req.getOffset(), req.getTimeoutMs());
      // Per the contract, a failed wait is reported in the body, not as a gRPC error. Empty
      // error_message means success.
      out.onNext(AwaitResponse.newBuilder().setErrorMessage(error).build());
      out.onCompleted();
    } catch (Throwable t) {
      out.onError(toStatus(t));
    }
  }

  @Override
  public void flush(FlushRequest req, StreamObserver<Empty> out) {
    try {
      StreamEntry entry = lookup(req.getStreamId());
      synchronized (entry.lock) {
        entry.flush();
      }
      out.onNext(Empty.getDefaultInstance());
      out.onCompleted();
    } catch (Throwable t) {
      out.onError(toStatus(t));
    }
  }

  @Override
  public void close(CloseRequest req, StreamObserver<Empty> out) {
    try {
      // Remove first so new RPCs on this id see NOT_FOUND. The entry is dropped even if the SDK
      // close fails.
      StreamEntry entry = streams.remove(req.getStreamId());
      if (entry == null) {
        throw Status.NOT_FOUND
            .withDescription("unknown stream_id '" + req.getStreamId() + "'")
            .asRuntimeException();
      }
      synchronized (entry.lock) {
        entry.release();
      }
      logger.info("stream closed: {}", req.getStreamId());
      out.onNext(Empty.getDefaultInstance());
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

  /** Closes every stream still open. Called from Main during shutdown. */
  void closeAll() {
    waiters.shutdownNow();
    for (Map.Entry<String, StreamEntry> e : streams.entrySet()) {
      if (streams.remove(e.getKey()) == null) {
        continue;
      }
      synchronized (e.getValue().lock) {
        try {
          e.getValue().release();
        } catch (Throwable t) {
          logger.warn("closing stream {} failed: {}", e.getKey(), t.toString());
        }
      }
    }
  }

  /**
   * Waits for {@code offset} to be acknowledged, under the stream lock. A positive {@code
   * timeoutMs} bounds the caller's wait; a non-positive value waits with no deadline. Returns the
   * error text, or an empty string on success.
   */
  private String awaitOffset(StreamEntry entry, long offset, long timeoutMs) {
    if (timeoutMs <= 0) {
      synchronized (entry.lock) {
        return waitOrError(entry, offset);
      }
    }
    Future<String> waiter =
        waiters.submit(
            () -> {
              synchronized (entry.lock) {
                return waitOrError(entry, offset);
              }
            });
    try {
      return waiter.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      return "timed out after " + timeoutMs + " ms waiting for offset " + offset;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return "interrupted waiting for offset " + offset;
    } catch (ExecutionException e) {
      return String.valueOf(e.getCause().getMessage());
    }
  }

  private static String waitOrError(StreamEntry entry, long offset) {
    try {
      entry.waitForOffset(offset);
      return "";
    } catch (ZerobusException e) {
      return String.valueOf(e.getMessage());
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
   * Rejects every protocol/format combination this harness does not implement. Each TODO names the
   * work that remains.
   */
  static void checkSliceSupported(Protocol protocol, RecordFormat format) {
    if (protocol == Protocol.GRPC && format == RecordFormat.PROTO) {
      return;
    }
    if (protocol == Protocol.ARROW_FLIGHT && format == RecordFormat.ARROW) {
      return;
    }
    if (protocol == Protocol.GRPC && format == RecordFormat.JSON) {
      // TODO(conformance): GRPC + JSON needs ZerobusJsonStream + a JSON-encoding path here.
      throw Status.UNIMPLEMENTED
          .withDescription("TODO(conformance): GRPC + JSON not implemented in the Java harness yet")
          .asRuntimeException();
    }
    if (protocol == Protocol.GRPC && format == RecordFormat.AVRO) {
      // TODO(conformance): Java SDK has no Avro record format; needs SDK support first.
      throw Status.UNIMPLEMENTED
          .withDescription("TODO(conformance): GRPC + AVRO not implemented in the Java harness yet")
          .asRuntimeException();
    }
    if (protocol == Protocol.GRPC && format == RecordFormat.ARROW) {
      throw Status.UNIMPLEMENTED
          .withDescription("TODO(conformance): GRPC + ARROW is not a valid pairing; ARROW rides ARROW_FLIGHT")
          .asRuntimeException();
    }
    if (protocol == Protocol.ARROW_FLIGHT) {
      throw Status.INVALID_ARGUMENT
          .withDescription("ARROW_FLIGHT carries only the ARROW format")
          .asRuntimeException();
    }
    throw Status.INVALID_ARGUMENT
        .withDescription("protocol and format must be specified")
        .asRuntimeException();
  }

  /** Maps an SDK or argument error onto a gRPC status. Argument problems stay INVALID_ARGUMENT. */
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
