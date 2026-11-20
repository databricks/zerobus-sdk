// TestHarness service: maps each contract RPC onto the .NET Zerobus SDK.
//
// Each RPC is a pass-through to the SDK call of the same shape. The only harness-side work is:
//   (1) JSON record -> SDK input conversion (proto records are encoded to wire bytes here),
//   (2) streamId -> SDK handle bookkeeping,
//   (3) mapping the SDK's ZerobusException (message + IsRetryable) into the response's SdkError.
//
// First slice: protocol=GRPC with format=PROTO or JSON. ARROW_FLIGHT and AVRO are not wired
// yet (see the TODOs in EnsureSupported).
//
// Error model: a ZerobusException from the SDK under test is returned in the response's
// SdkError and the RPC returns OK. Any other exception is a harness fault and returns a
// non-OK gRPC status: INVALID_ARGUMENT for input the harness cannot convert, NOT_FOUND for an
// unknown streamId, INTERNAL otherwise (including ObjectDisposedException from a stream the
// SDK has already disposed).
//
// Concurrency: no harness-side gate. The SDK's stream is thread-safe, so concurrent RPCs on one
// stream go straight to it.

using System.Collections.Concurrent;
using Databricks.Zerobus;
using Grpc.Core;

namespace Zerobus.Conformance.Harness;

public sealed class HarnessService(ILogger<HarnessService> logger, IHostApplicationLifetime lifetime)
    : TestHarness.TestHarnessBase
{
    private readonly ConcurrentDictionary<string, StreamEntry> _streams = new();
    private long _nextStreamId;

    public override async Task<CreateStreamResponse> CreateStream(CreateStreamRequest request, ServerCallContext context)
    {
        EnsureSupported(request.Protocol, request.Format);

        ProtoRecordEncoder? encoder = null;
        var isProto = request.Format == RecordFormat.Proto;
        if (isProto)
        {
            try
            {
                encoder = ProtoRecordEncoder.FromDescriptorProto(request.DescriptorProtoBytes.ToByteArray());
            }
            catch (ArgumentException e)
            {
                throw new RpcException(new Status(StatusCode.InvalidArgument, e.Message));
            }
        }

        logger.LogInformation("CreateStream endpoint={Endpoint} table={Table} format={Format}",
            request.ServerEndpoint, request.TablePath, request.Format);

        ZerobusSdk? sdk = null;
        try
        {
            // The mock is a local plaintext server that checks no credentials, so the SDK runs
            // with no TLS (http://) and a no-op headers provider (no auth headers). The
            // headers provider path needs no Unity Catalog URL.
            // The .NET SDK ships DisableTls() as a public GA method; the other SDKs gate the
            // equivalent no-TLS path behind a testing feature. Left ungated here to avoid a
            // breaking change to a released method.
            sdk = ZerobusSdk.CreateBuilder()
                .Endpoint($"http://{request.ServerEndpoint}")
                .DisableTls()
                .Build();

            var options = StreamConfigurationOptions.Default with
            {
                RecordType = isProto ? RecordType.Proto : RecordType.Json,
            };
            var tableProperties = isProto
                ? new TableProperties(request.TablePath, request.DescriptorProtoBytes.ToByteArray())
                : new TableProperties(request.TablePath);

            var stream = await sdk.CreateStreamWithHeadersProviderAsync(
                tableProperties, NoAuthHeadersProvider.Instance, options);

            var streamId = $"stream-{Interlocked.Increment(ref _nextStreamId) - 1}";
            _streams[streamId] = new StreamEntry(sdk, stream, isProto ? RecordFormat.Proto : RecordFormat.Json, encoder);
            return new CreateStreamResponse { StreamId = streamId };
        }
        catch (ZerobusException e)
        {
            sdk?.Dispose();
            return new CreateStreamResponse { Error = ToSdkError(e) };
        }
        catch (Exception e)
        {
            sdk?.Dispose();
            throw ToRpc(e);
        }
    }

    public override Task<IngestRecordResponse> IngestRecord(IngestRecordRequest request, ServerCallContext context) =>
        RunOnStreamAsync(
            request.StreamId,
            async entry => new IngestRecordResponse { Offset = await QueueRecordAsync(entry, request.RecordJson) },
            error => new IngestRecordResponse { Error = error });

    public override Task<IngestRecordsResponse> IngestRecords(IngestRecordsRequest request, ServerCallContext context) =>
        RunOnStreamAsync(
            request.StreamId,
            async entry =>
            {
                if (request.RecordJson.Count == 0)
                {
                    throw new RpcException(new Status(StatusCode.InvalidArgument,
                        "record_json must contain at least one record"));
                }

                // One call to the SDK's batch ingest; it returns the single offset for the batch.
                // Proto batches are encoded to wire bytes first (the SDK takes byte[][]); JSON
                // batches go to the SDK's string[] overload unchanged.
                var offset = entry.Format == RecordFormat.Proto
                    ? await entry.Stream.IngestRecordsAsync(
                        request.RecordJson.Select(r => entry.Encoder!.Encode(r)).ToArray())
                    : await entry.Stream.IngestRecordsAsync(request.RecordJson.ToArray());
                return new IngestRecordsResponse { Offset = offset };
            },
            error => new IngestRecordsResponse { Error = error });

    public override Task<AwaitResponse> AwaitWriteCompleted(AwaitRequest request, ServerCallContext context) =>
        RunOnStreamAsync(
            request.StreamId,
            async entry =>
            {
                // No harness-side deadline: the SDK's wait is bounded by its own ack/flush timeout
                // and reports its own failure.
                await entry.Stream.WaitForOffsetAsync(request.Offset);
                return new AwaitResponse();
            },
            error => new AwaitResponse { Error = error });

    public override Task<ActionResponse> Flush(FlushRequest request, ServerCallContext context) =>
        RunOnStreamAsync(
            request.StreamId,
            async entry =>
            {
                await entry.Stream.FlushAsync();
                return new ActionResponse();
            },
            error => new ActionResponse { Error = error });

    public override Task<ActionResponse> Close(CloseRequest request, ServerCallContext context) =>
        RunOnStreamAsync(
            request.StreamId,
            async entry =>
            {
                // The entry stays registered after close (success or failure), so later calls on
                // this id reach the SDK and get its own error. Only ids never created are NOT_FOUND.
                try
                {
                    await entry.Stream.CloseAsync();
                    logger.LogInformation("stream closed stream_id={StreamId}", request.StreamId);
                    return new ActionResponse();
                }
                finally
                {
                    await DisposeEntryAsync(entry);
                }
            },
            error => new ActionResponse { Error = error });

    public override Task<Empty> Shutdown(Empty request, ServerCallContext context)
    {
        logger.LogInformation("Shutdown RPC received");
        // Kestrel finishes this response before the host stops (graceful shutdown).
        lifetime.StopApplication();
        return Task.FromResult(new Empty());
    }

    /// <summary>Closes every stream still open at process exit. Best effort.</summary>
    public async Task CloseAllAsync()
    {
        foreach (var (id, entry) in _streams)
        {
            if (entry.Disposed) continue;
            try
            {
                await entry.Stream.CloseAsync();
            }
            catch (Exception e)
            {
                logger.LogWarning(e, "error closing stream {StreamId} at shutdown", id);
            }
            finally
            {
                await DisposeEntryAsync(entry);
            }
        }
    }

    /// <summary>
    /// Runs one per-stream RPC. A ZerobusException from the SDK becomes the response's SdkError
    /// via <paramref name="fail"/>; any other exception is a harness fault and becomes a gRPC status.
    /// </summary>
    private async Task<TResponse> RunOnStreamAsync<TResponse>(
        string streamId,
        Func<StreamEntry, Task<TResponse>> call,
        Func<SdkError, TResponse> fail)
    {
        var entry = GetEntry(streamId);
        try
        {
            return await call(entry);
        }
        catch (ZerobusException e)
        {
            return fail(ToSdkError(e));
        }
        catch (Exception e)
        {
            throw ToRpc(e);
        }
    }

    private StreamEntry GetEntry(string streamId) =>
        _streams.TryGetValue(streamId, out var entry)
            ? entry
            : throw new RpcException(new Status(StatusCode.NotFound, $"unknown stream_id '{streamId}'"));

    /// <summary>Queues one logical record (no ack wait) and returns its offset.</summary>
    private static async Task<long> QueueRecordAsync(StreamEntry entry, string recordJson)
    {
        // The SDK encodes the record itself (JSON streams) or receives the bytes we encoded
        // (proto streams).
        if (entry.Format == RecordFormat.Proto)
        {
            return await entry.Stream.IngestRecordAsync(entry.Encoder!.Encode(recordJson));
        }

        return await entry.Stream.IngestRecordAsync(recordJson);
    }

    /// <summary>Disposes the stream and its SDK once. Safe to call again; the entry stays registered.</summary>
    private async Task DisposeEntryAsync(StreamEntry entry)
    {
        entry.Disposed = true;
        try
        {
            await entry.Stream.DisposeAsync();
        }
        catch (Exception e)
        {
            logger.LogWarning(e, "error disposing stream");
        }
        finally
        {
            entry.Sdk.Dispose();
        }
    }

    /// <summary>Rejects every protocol/format combination this slice does not implement.</summary>
    private static void EnsureSupported(Protocol protocol, RecordFormat format)
    {
        switch (protocol, format)
        {
            case (Protocol.Grpc, RecordFormat.Proto):
            case (Protocol.Grpc, RecordFormat.Json):
                return;
            case (Protocol.Grpc, RecordFormat.Avro):
                throw new RpcException(new Status(StatusCode.Unimplemented,
                    "TODO(conformance): GRPC + AVRO not implemented in the .NET harness yet (the .NET SDK has no Avro support)"));
            case (Protocol.Grpc, RecordFormat.Arrow):
                throw new RpcException(new Status(StatusCode.Unimplemented,
                    "TODO(conformance): GRPC + ARROW is not a valid pairing; ARROW rides ARROW_FLIGHT"));
            case (Protocol.ArrowFlight, _):
                throw new RpcException(new Status(StatusCode.Unimplemented,
                    "TODO(conformance): ARROW_FLIGHT protocol not implemented in the .NET harness yet (the .NET wrapper does not expose Arrow Flight)"));
            default:
                throw new RpcException(new Status(StatusCode.InvalidArgument,
                    "protocol and format must be specified"));
        }
    }

    /// <summary>Maps the SDK's error onto the contract's SdkError, keeping the SDK's retryable flag.</summary>
    private static SdkError ToSdkError(ZerobusException e) =>
        new() { Message = e.Message, Retryable = e.IsRetryable };

    /// <summary>Maps a harness-internal error onto a gRPC status.</summary>
    private static RpcException ToRpc(Exception e) => e switch
    {
        RpcException rpc => rpc,
        ArgumentException arg => new RpcException(new Status(StatusCode.InvalidArgument, arg.Message)),
        _ => new RpcException(new Status(StatusCode.Internal, e.Message)),
    };

    private sealed class StreamEntry(ZerobusSdk sdk, ZerobusStream stream, RecordFormat format, ProtoRecordEncoder? encoder)
    {
        /// <summary>The SDK is kept alive for as long as the stream it created.</summary>
        public ZerobusSdk Sdk { get; } = sdk;
        public ZerobusStream Stream { get; } = stream;
        public RecordFormat Format { get; } = format;
        public ProtoRecordEncoder? Encoder { get; } = encoder;

        /// <summary>Set once the stream and its SDK have been disposed.</summary>
        public bool Disposed { get; set; }
    }
}
