package com.databricks.zerobus.conformance;

import com.databricks.zerobus.HeadersProvider;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
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
 * <p>First slice: {@code protocol=GRPC}, {@code format=PROTO} only. Every other combination returns
 * {@code UNIMPLEMENTED} with a TODO (see {@link #checkSliceSupported}).
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

  /** One open SDK stream plus the descriptor its records are built against. */
  static final class StreamEntry {
    final ZerobusSdk sdk;
    final ZerobusProtoStream stream;
    final Descriptor descriptor;
    /** Serializes all SDK calls on this stream (the Java SDK is not thread-safe). */
    final Object lock = new Object();

    StreamEntry(ZerobusSdk sdk, ZerobusProtoStream stream, Descriptor descriptor) {
      this.sdk = sdk;
      this.stream = stream;
      this.descriptor = descriptor;
    }
  }

  HarnessService(CountDownLatch shutdownRequested) {
    this.shutdownRequested = shutdownRequested;
  }

  @Override
  public void createStream(CreateStreamRequest req, StreamObserver<CreateStreamResponse> out) {
    try {
      checkSliceSupported(req.getProtocol(), req.getFormat());

      DescriptorProto descriptorProto = Records.descriptorProto(req.getDescriptorProtoBytes());
      Descriptor descriptor = Records.messageDescriptor(descriptorProto);

      logger.info("CreateStream endpoint={} table={}", req.getServerEndpoint(), req.getTablePath());
      // The mock is a local plaintext server, so no TLS (`http://`) and no auth headers.
      ZerobusSdk sdk = ZerobusSdk.forTestingNoTls("http://" + req.getServerEndpoint());
      ZerobusProtoStream stream;
      try {
        stream =
            sdk.streamBuilder()
                .table(req.getTablePath())
                .headersProvider(NO_AUTH_HEADERS)
                .compiledProto(descriptorProto)
                .build()
                .get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        sdk.close();
        throw Status.CANCELLED.withDescription("interrupted").asRuntimeException();
      } catch (ExecutionException e) {
        sdk.close();
        throw toStatus(e.getCause());
      } catch (RuntimeException e) {
        sdk.close();
        throw e;
      }

      String streamId = "stream-" + nextStreamId.getAndIncrement();
      streams.put(streamId, new StreamEntry(sdk, stream, descriptor));
      out.onNext(CreateStreamResponse.newBuilder().setStreamId(streamId).build());
      out.onCompleted();
    } catch (Throwable t) {
      out.onError(toStatus(t));
    }
  }

  @Override
  public void ingestRecord(IngestRecordRequest req, StreamObserver<IngestRecordResponse> out) {
    try {
      StreamEntry entry = lookup(req.getStreamId());
      Message message;
      try {
        message = Records.toMessage(entry.descriptor, req.getRecordJson());
      } catch (InvalidProtocolBufferException e) {
        throw Status.INVALID_ARGUMENT
            .withDescription("record_json does not match the schema: " + e.getMessage())
            .asRuntimeException();
      }

      long offset;
      synchronized (entry.lock) {
        // Queue only. This returns once the SDK has enqueued the record, not once it is
        // acknowledged. The SDK encodes the message itself.
        offset = entry.stream.ingestRecordOffset(message);
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
        entry.stream.flush();
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
        try {
          entry.stream.close();
        } finally {
          // Java has no finalizers: the SDK handle must be released explicitly.
          entry.sdk.close();
        }
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
          e.getValue().stream.close();
        } catch (Throwable t) {
          logger.warn("closing stream {} failed: {}", e.getKey(), t.toString());
        } finally {
          e.getValue().sdk.close();
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
      entry.stream.waitForOffset(offset);
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
   * Rejects every protocol/format combination this slice does not implement. Each TODO names the
   * work that remains.
   */
  static void checkSliceSupported(Protocol protocol, RecordFormat format) {
    if (protocol == Protocol.GRPC && format == RecordFormat.PROTO) {
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
      // TODO(conformance): ARROW_FLIGHT needs ZerobusArrowStream + an Arrow schema path here.
      throw Status.UNIMPLEMENTED
          .withDescription("TODO(conformance): ARROW_FLIGHT protocol not implemented in the Java harness yet")
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
