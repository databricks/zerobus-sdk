// TestHarness service: maps each contract RPC onto the .NET Zerobus SDK.
//
// First slice: protocol=GRPC with format=PROTO or JSON. ARROW_FLIGHT and AVRO are not wired
// yet (see the TODOs in EnsureSupported). Mirrors conformance/harness/rust/src/harness.rs.

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
            // with no TLS (`http://`) and a no-op headers provider (no auth headers). The
            // headers provider path needs no Unity Catalog URL.
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
        catch (Exception e)
        {
            sdk?.Dispose();
            throw ToRpc(e);
        }
    }

    public override async Task<IngestRecordResponse> IngestRecord(IngestRecordRequest request, ServerCallContext context)
    {
        var entry = GetEntry(request.StreamId);
        await entry.Gate.WaitAsync(context.CancellationToken);
        try
        {
            // Queue only. This returns once the SDK has enqueued the record, not once it is
            // acknowledged. The SDK encodes the record itself (JSON streams) or receives the
            // bytes we encoded (proto streams).
            long offset;
            if (entry.Format == RecordFormat.Proto)
            {
                var bytes = entry.Encoder!.Encode(request.RecordJson);
                offset = await entry.Stream.IngestRecordAsync(bytes);
            }
            else
            {
                ProtoRecordEncoder.RequireJsonObject(request.RecordJson);
                offset = await entry.Stream.IngestRecordAsync(request.RecordJson);
            }

            return new IngestRecordResponse { Offset = offset };
        }
        catch (Exception e) when (e is not RpcException)
        {
            throw ToRpc(e);
        }
        finally
        {
            entry.Gate.Release();
        }
    }

    public override async Task<AwaitResponse> AwaitWriteCompleted(AwaitRequest request, ServerCallContext context)
    {
        var entry = GetEntry(request.StreamId);
        await entry.Gate.WaitAsync(context.CancellationToken);
        try
        {
            var wait = entry.Stream.WaitForOffsetAsync(request.Offset);

            // The SDK's wait has no timeout of its own, so the contract's timeout_ms is applied
            // here. A value <= 0 waits with no deadline.
            if (request.TimeoutMs > 0)
            {
                var timeout = Task.Delay(TimeSpan.FromMilliseconds(request.TimeoutMs), context.CancellationToken);
                if (await Task.WhenAny(wait, timeout) != wait)
                {
                    return new AwaitResponse
                    {
                        ErrorMessage = $"timed out after {request.TimeoutMs} ms waiting for offset {request.Offset}",
                    };
                }
            }

            // A failed wait is reported in the response body, not as a gRPC error.
            // Empty error_message means success.
            try
            {
                await wait;
                return new AwaitResponse();
            }
            catch (Exception e)
            {
                return new AwaitResponse { ErrorMessage = e.Message };
            }
        }
        finally
        {
            entry.Gate.Release();
        }
    }

    public override async Task<Empty> Flush(FlushRequest request, ServerCallContext context)
    {
        var entry = GetEntry(request.StreamId);
        await entry.Gate.WaitAsync(context.CancellationToken);
        try
        {
            await entry.Stream.FlushAsync();
            return new Empty();
        }
        catch (Exception e) when (e is not RpcException)
        {
            throw ToRpc(e);
        }
        finally
        {
            entry.Gate.Release();
        }
    }

    public override async Task<Empty> Close(CloseRequest request, ServerCallContext context)
    {
        // Remove first so new RPCs on this id see NOT_FOUND. The entry is dropped even if the
        // SDK close fails.
        if (!_streams.TryRemove(request.StreamId, out var entry))
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"unknown stream_id '{request.StreamId}'"));
        }

        await entry.Gate.WaitAsync(context.CancellationToken);
        try
        {
            await entry.Stream.CloseAsync();
            logger.LogInformation("stream closed stream_id={StreamId}", request.StreamId);
            return new Empty();
        }
        catch (Exception e) when (e is not RpcException)
        {
            throw ToRpc(e);
        }
        finally
        {
            await DisposeEntryAsync(entry);
            entry.Gate.Release();
        }
    }

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
        foreach (var id in _streams.Keys)
        {
            if (!_streams.TryRemove(id, out var entry)) continue;
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

    private StreamEntry GetEntry(string streamId) =>
        _streams.TryGetValue(streamId, out var entry)
            ? entry
            : throw new RpcException(new Status(StatusCode.NotFound, $"unknown stream_id '{streamId}'"));

    private async Task DisposeEntryAsync(StreamEntry entry)
    {
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

    /// <summary>Maps an SDK or argument error onto a gRPC status.</summary>
    private static RpcException ToRpc(Exception e) => e switch
    {
        RpcException rpc => rpc,
        ArgumentException arg => new RpcException(new Status(StatusCode.InvalidArgument, arg.Message)),
        ObjectDisposedException => new RpcException(new Status(StatusCode.FailedPrecondition, $"stream closed: {e.Message}")),
        _ => new RpcException(new Status(StatusCode.Internal, e.Message)),
    };

    private sealed class StreamEntry(ZerobusSdk sdk, ZerobusStream stream, RecordFormat format, ProtoRecordEncoder? encoder)
    {
        /// <summary>The SDK is kept alive for as long as the stream it created.</summary>
        public ZerobusSdk Sdk { get; } = sdk;
        public ZerobusStream Stream { get; } = stream;
        public RecordFormat Format { get; } = format;
        public ProtoRecordEncoder? Encoder { get; } = encoder;

        /// <summary>Serializes per-stream RPCs (ingest / await / flush / close).</summary>
        public SemaphoreSlim Gate { get; } = new(1, 1);
    }
}
