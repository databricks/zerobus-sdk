using System.Runtime.InteropServices;
using Databricks.Zerobus.Native;

namespace Databricks.Zerobus;

/// <summary>
/// The main entry point for interacting with the Zerobus ingestion service.
/// Manages the connection to the Zerobus endpoint and Unity Catalog.
/// </summary>
/// <remarks>
/// <para>
/// This class wraps a native Rust SDK via P/Invoke and manages the lifecycle
/// of the underlying unmanaged resource. Always dispose when finished.
/// </para>
/// <para>
/// The SDK is thread-safe — you may create multiple streams from one instance.
/// </para>
/// <para>
/// See the <see href="https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id">connection URL table</see>
/// for cloud-specific endpoint formats.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// using var sdk = ZerobusSdk.CreateBuilder()
///     .Endpoint(serverEndpoint)
///     .UnityCatalogUrl(workspaceUrl)
///     .Build();
///
/// using var stream = sdk.CreateJsonStream(
///     "catalog.schema.table",
///     clientId,
///     clientSecret);
/// </code>
/// </example>
public sealed class ZerobusSdk : IDisposable
{
    private IntPtr _ptr;
    private int _disposed;

    /// <summary>
    /// Internal constructor used by <see cref="ZerobusSdkBuilder.Build"/>.
    /// Takes ownership of the native pointer.
    /// </summary>
    internal ZerobusSdk(IntPtr ptr)
    {
        _ptr = ptr;
    }

    /// <summary>
    /// Returns a new builder for constructing a <see cref="ZerobusSdk"/> with
    /// optional settings such as application name, TLS override, or a Unity
    /// Catalog URL that can be omitted when using a custom headers provider.
    /// </summary>
    /// <returns>A <see cref="ZerobusSdkBuilder"/> ready to configure and build.</returns>
    /// <example>
    /// <code>
    /// using var sdk = ZerobusSdk.CreateBuilder()
    ///     .Endpoint(serverEndpoint)
    ///     .UnityCatalogUrl(workspaceUrl)
    ///     .ApplicationName("my-service")
    ///     .Build();
    /// </code>
    /// </example>
    public static ZerobusSdkBuilder CreateBuilder() => new();

    /// <summary>
    /// Fetches a Zerobus-scoped OAuth 2.0 access token with the client credentials flow.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Use it to authenticate any client that takes an OAuth token callback, such as a Kafka
    /// client's SASL/OAUTHBEARER token refresh handler. Every call mints a new token; nothing is
    /// cached, so let the client decide when to call it (for example, on reconnect or shortly
    /// before <see cref="OAuthToken.ExpiresAt"/>).
    /// </para>
    /// <para>
    /// Needs no <see cref="ZerobusSdk"/> instance. Blocks until the token endpoint responds, for at most 30 seconds.
    /// </para>
    /// </remarks>
    /// <param name="workspaceUrl">
    /// The Databricks workspace URL. See the
    /// <see href="https://github.com/databricks/zerobus-sdk/blob/main/README.md#1-connection-urls-and-workspace-id">connection URL table</see>.
    /// </param>
    /// <param name="tableName">
    /// Fully qualified table name in the form <c>catalog.schema.table</c> that the token is scoped to.
    /// </param>
    /// <param name="clientId">OAuth 2.0 client ID.</param>
    /// <param name="clientSecret">OAuth 2.0 client secret.</param>
    /// <param name="workspaceId">The Databricks workspace ID.</param>
    /// <returns>The access token and its expiry.</returns>
    /// <exception cref="ArgumentNullException">Thrown if any argument is <c>null</c>.</exception>
    /// <exception cref="ZerobusException">
    /// Thrown if the token cannot be fetched. <see cref="ZerobusException.IsRetryable"/> is
    /// <c>false</c> for invalid input or a rejected request (for example, bad credentials or an
    /// invalid table name) and <c>true</c> for network errors and server (5xx) errors.
    /// </exception>
    /// <example>
    /// <code>
    /// var token = ZerobusSdk.GetToken(
    ///     workspaceUrl,
    ///     "catalog.schema.table",
    ///     clientId,
    ///     clientSecret,
    ///     workspaceId);
    /// </code>
    /// </example>
    public static OAuthToken GetToken(
        string workspaceUrl,
        string tableName,
        string clientId,
        string clientSecret,
        string workspaceId)
    {
        ArgumentNullException.ThrowIfNull(workspaceUrl);
        ArgumentNullException.ThrowIfNull(tableName);
        ArgumentNullException.ThrowIfNull(clientId);
        ArgumentNullException.ThrowIfNull(clientSecret);
        ArgumentNullException.ThrowIfNull(workspaceId);

        return NativeInterop.GetToken(workspaceUrl, tableName, clientId, clientSecret, workspaceId);
    }

    /// <summary>
    /// Creates a new JSON or Protocol Buffer stream for ingesting records into a Databricks table.
    /// Uses OAuth 2.0 client credentials flow for authentication.
    /// </summary>
    /// <param name="tableProperties">Table properties including name and optional protobuf descriptor.</param>
    /// <param name="clientId">OAuth 2.0 client ID.</param>
    /// <param name="clientSecret">OAuth 2.0 client secret.</param>
    /// <param name="options">
    /// Stream configuration options. Pass <c>null</c> or omit to use defaults.
    /// </param>
    /// <returns>A new <see cref="ZerobusStream"/> ready for record ingestion.</returns>
    /// <exception cref="ZerobusException">
    /// Thrown if the stream cannot be created (auth failure, invalid table, etc.).
    /// </exception>
    /// <exception cref="ObjectDisposedException">Thrown if the SDK has been disposed.</exception>
    /// <example>
    /// <code>
    /// using var stream = sdk.CreateStream(
    ///     new TableProperties("catalog.schema.table"),
    ///     clientId,
    ///     clientSecret);
    /// </code>
    /// </example>
    public ZerobusStream CreateStream(
        TableProperties tableProperties,
        string clientId,
        string clientSecret,
        StreamConfigurationOptions? options = null)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        ArgumentNullException.ThrowIfNull(tableProperties);
        ArgumentNullException.ThrowIfNull(clientId);
        ArgumentNullException.ThrowIfNull(clientSecret);

        return CreateStreamCore(tableProperties, clientId, clientSecret, options);
    }

    /// <summary>
    /// Creates a new JSON or Protocol Buffer stream asynchronously.
    /// </summary>
    public Task<ZerobusStream> CreateStreamAsync(
        TableProperties tableProperties,
        string clientId,
        string clientSecret,
        StreamConfigurationOptions? options = null)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        ArgumentNullException.ThrowIfNull(tableProperties);
        ArgumentNullException.ThrowIfNull(clientId);
        ArgumentNullException.ThrowIfNull(clientSecret);

        return CreateStreamCoreAsync(tableProperties, clientId, clientSecret, options);
    }

    /// <summary>
    /// Creates a JSON-only stream with OAuth 2.0 client credentials authentication.
    /// This factory sets <see cref="StreamConfigurationOptions.RecordType"/> to
    /// <see cref="RecordType.Json"/> automatically and returns a stream wrapper that
    /// exposes JSON ingest overloads only.
    /// </summary>
    /// <param name="tableName">Fully qualified table name in the form <c>catalog.schema.table</c>.</param>
    /// <param name="clientId">OAuth 2.0 client ID.</param>
    /// <param name="clientSecret">OAuth 2.0 client secret.</param>
    /// <param name="options">Optional stream configuration overrides.</param>
    /// <returns>A new <see cref="JsonZerobusStream"/> ready for JSON ingestion.</returns>
    public JsonZerobusStream CreateJsonStream(
        string tableName,
        string clientId,
        string clientSecret,
        StreamConfigurationOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        ArgumentNullException.ThrowIfNull(clientId);
        ArgumentNullException.ThrowIfNull(clientSecret);

        var stream = CreateStreamCore(
            new TableProperties(tableName),
            clientId,
            clientSecret,
            NormalizeStreamOptions(options, RecordType.Json));

        return new JsonZerobusStream(stream);
    }

    /// <summary>
    /// Creates a JSON-only stream asynchronously.
    /// </summary>
    public async Task<JsonZerobusStream> CreateJsonStreamAsync(
        string tableName,
        string clientId,
        string clientSecret,
        StreamConfigurationOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        ArgumentNullException.ThrowIfNull(clientId);
        ArgumentNullException.ThrowIfNull(clientSecret);

        var stream = await CreateStreamCoreAsync(
                new TableProperties(tableName),
                clientId,
                clientSecret,
                NormalizeStreamOptions(options, RecordType.Json))
            .ConfigureAwait(false);

        return new JsonZerobusStream(stream);
    }

    /// <summary>
    /// Creates a protobuf-only stream with OAuth 2.0 client credentials authentication.
    /// This factory sets <see cref="StreamConfigurationOptions.RecordType"/> to
    /// <see cref="RecordType.Proto"/> automatically and returns a stream wrapper that
    /// exposes protobuf ingest overloads only.
    /// </summary>
    /// <param name="tableName">Fully qualified table name in the form <c>catalog.schema.table</c>.</param>
    /// <param name="descriptorProto">Serialized protobuf descriptor for the target table schema.</param>
    /// <param name="clientId">OAuth 2.0 client ID.</param>
    /// <param name="clientSecret">OAuth 2.0 client secret.</param>
    /// <param name="options">Optional stream configuration overrides.</param>
    /// <returns>A new <see cref="ProtoZerobusStream"/> ready for protobuf ingestion.</returns>
    public ProtoZerobusStream CreateProtoStream(
        string tableName,
        byte[] descriptorProto,
        string clientId,
        string clientSecret,
        StreamConfigurationOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        ArgumentNullException.ThrowIfNull(descriptorProto);
        ArgumentNullException.ThrowIfNull(clientId);
        ArgumentNullException.ThrowIfNull(clientSecret);

        var stream = CreateStreamCore(
            new TableProperties(tableName, descriptorProto),
            clientId,
            clientSecret,
            NormalizeStreamOptions(options, RecordType.Proto));

        return new ProtoZerobusStream(stream);
    }

    /// <summary>
    /// Creates a protobuf-only stream asynchronously.
    /// </summary>
    public async Task<ProtoZerobusStream> CreateProtoStreamAsync(
        string tableName,
        byte[] descriptorProto,
        string clientId,
        string clientSecret,
        StreamConfigurationOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        ArgumentNullException.ThrowIfNull(descriptorProto);
        ArgumentNullException.ThrowIfNull(clientId);
        ArgumentNullException.ThrowIfNull(clientSecret);

        var stream = await CreateStreamCoreAsync(
                new TableProperties(tableName, descriptorProto),
                clientId,
                clientSecret,
                NormalizeStreamOptions(options, RecordType.Proto))
            .ConfigureAwait(false);

        return new ProtoZerobusStream(stream);
    }

    private ZerobusStream CreateStreamCore(
        TableProperties tableProperties,
        string clientId,
        string clientSecret,
        StreamConfigurationOptions? options)
    {
        ValidateStreamConfiguration(tableProperties, options);

        var nativeOpts = NativeInterop.ConvertConfig(options);

        var streamPtr = NativeInterop.SdkCreateStream(
            _ptr,
            tableProperties.TableName,
            tableProperties.DescriptorProto ?? [],
            clientId,
            clientSecret,
            ref nativeOpts);

        return new ZerobusStream(streamPtr);
    }

    private async Task<ZerobusStream> CreateStreamCoreAsync(
        TableProperties tableProperties,
        string clientId,
        string clientSecret,
        StreamConfigurationOptions? options)
    {
        ValidateStreamConfiguration(tableProperties, options);

        var nativeOpts = NativeInterop.ConvertConfig(options);

        var streamPtr = await NativeInterop.SdkCreateStreamAsync(
                _ptr,
                tableProperties.TableName,
                tableProperties.DescriptorProto ?? [],
                clientId,
                clientSecret,
                ref nativeOpts)
            .ConfigureAwait(false);

        return new ZerobusStream(streamPtr);
    }

    /// <summary>
    /// Creates a new JSON or Protocol Buffer stream using a custom headers provider.
    /// Use this for custom authentication logic (managed identity, vaults, etc.).
    /// </summary>
    /// <param name="tableProperties">Table properties including name and optional protobuf descriptor.</param>
    /// <param name="headersProvider">Custom implementation of <see cref="IHeadersProvider"/>.</param>
    /// <param name="options">
    /// Stream configuration options. Pass <c>null</c> or omit to use defaults.
    /// </param>
    /// <returns>A new <see cref="ZerobusStream"/> ready for record ingestion.</returns>
    /// <exception cref="ZerobusException">
    /// Thrown if the stream cannot be created (headers provider error, network issues, etc.).
    /// </exception>
    /// <exception cref="ObjectDisposedException">Thrown if the SDK has been disposed.</exception>
    /// <example>
    /// <code>
    /// var provider = new CustomHeadersProvider();
    /// using var stream = sdk.CreateStreamWithHeadersProvider(
    ///     new TableProperties("catalog.schema.table"),
    ///     provider);
    /// </code>
    /// </example>
    public ZerobusStream CreateStreamWithHeadersProvider(
        TableProperties tableProperties,
        IHeadersProvider headersProvider,
        StreamConfigurationOptions? options = null)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        ArgumentNullException.ThrowIfNull(tableProperties);
        ArgumentNullException.ThrowIfNull(headersProvider);

        return CreateStreamWithHeadersProviderCore(tableProperties, headersProvider, options);
    }

    /// <summary>
    /// Creates a new JSON or Protocol Buffer stream using a custom headers provider asynchronously.
    /// </summary>
    public Task<ZerobusStream> CreateStreamWithHeadersProviderAsync(
        TableProperties tableProperties,
        IHeadersProvider headersProvider,
        StreamConfigurationOptions? options = null)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        ArgumentNullException.ThrowIfNull(tableProperties);
        ArgumentNullException.ThrowIfNull(headersProvider);

        return CreateStreamWithHeadersProviderCoreAsync(tableProperties, headersProvider, options);
    }

    /// <summary>
    /// Creates a JSON-only stream using a custom headers provider.
    /// </summary>
    /// <param name="tableName">Fully qualified table name in the form <c>catalog.schema.table</c>.</param>
    /// <param name="headersProvider">Custom authentication headers provider.</param>
    /// <param name="options">Optional stream configuration overrides.</param>
    /// <returns>A new <see cref="JsonZerobusStream"/> ready for JSON ingestion.</returns>
    public JsonZerobusStream CreateJsonStreamWithHeadersProvider(
        string tableName,
        IHeadersProvider headersProvider,
        StreamConfigurationOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        ArgumentNullException.ThrowIfNull(headersProvider);

        var stream = CreateStreamWithHeadersProviderCore(
            new TableProperties(tableName),
            headersProvider,
            NormalizeStreamOptions(options, RecordType.Json));

        return new JsonZerobusStream(stream);
    }

    /// <summary>
    /// Creates a JSON-only stream using a custom headers provider asynchronously.
    /// </summary>
    public async Task<JsonZerobusStream> CreateJsonStreamWithHeadersProviderAsync(
        string tableName,
        IHeadersProvider headersProvider,
        StreamConfigurationOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        ArgumentNullException.ThrowIfNull(headersProvider);

        var stream = await CreateStreamWithHeadersProviderAsync(
                new TableProperties(tableName),
                headersProvider,
                NormalizeStreamOptions(options, RecordType.Json))
            .ConfigureAwait(false);

        return new JsonZerobusStream(stream);
    }

    /// <summary>
    /// Creates a protobuf-only stream using a custom headers provider.
    /// </summary>
    /// <param name="tableName">Fully qualified table name in the form <c>catalog.schema.table</c>.</param>
    /// <param name="descriptorProto">Serialized protobuf descriptor for the target table schema.</param>
    /// <param name="headersProvider">Custom authentication headers provider.</param>
    /// <param name="options">Optional stream configuration overrides.</param>
    /// <returns>A new <see cref="ProtoZerobusStream"/> ready for protobuf ingestion.</returns>
    public ProtoZerobusStream CreateProtoStreamWithHeadersProvider(
        string tableName,
        byte[] descriptorProto,
        IHeadersProvider headersProvider,
        StreamConfigurationOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        ArgumentNullException.ThrowIfNull(descriptorProto);
        ArgumentNullException.ThrowIfNull(headersProvider);

        var stream = CreateStreamWithHeadersProviderCore(
            new TableProperties(tableName, descriptorProto),
            headersProvider,
            NormalizeStreamOptions(options, RecordType.Proto));

        return new ProtoZerobusStream(stream);
    }

    /// <summary>
    /// Creates a protobuf-only stream using a custom headers provider asynchronously.
    /// </summary>
    public async Task<ProtoZerobusStream> CreateProtoStreamWithHeadersProviderAsync(
        string tableName,
        byte[] descriptorProto,
        IHeadersProvider headersProvider,
        StreamConfigurationOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        ArgumentNullException.ThrowIfNull(descriptorProto);
        ArgumentNullException.ThrowIfNull(headersProvider);

        var stream = await CreateStreamWithHeadersProviderAsync(
                new TableProperties(tableName, descriptorProto),
                headersProvider,
                NormalizeStreamOptions(options, RecordType.Proto))
            .ConfigureAwait(false);

        return new ProtoZerobusStream(stream);
    }

    private ZerobusStream CreateStreamWithHeadersProviderCore(
        TableProperties tableProperties,
        IHeadersProvider headersProvider,
        StreamConfigurationOptions? options)
    {
        ValidateStreamConfiguration(tableProperties, options);

        var nativeOpts = NativeInterop.ConvertConfig(options);

        // Create the callback bridge that the native code will invoke. The bridge
        // holds both delegate instances, so rooting the bridge keeps their native
        // thunks alive.
        var bridge = new HeadersProviderBridge(headersProvider);

        // Ownership transfer: the GCHandle to the bridge is handed to the FFI as
        // user_data. The FFI releases it via the free callback (NativeFree)
        // exactly once — after any in-flight GetHeaders has returned — which
        // closes the recovery-vs-teardown use-after-free. The stream therefore
        // owns nothing and never frees this handle itself.
        var handle = GCHandle.Alloc(bridge);
        var handlePtr = GCHandle.ToIntPtr(handle);
        var tableName = tableProperties.TableName;
        var descriptorProto = tableProperties.DescriptorProto ?? [];

        IntPtr streamPtr;
        try
        {
            streamPtr = NativeInterop.SdkCreateStreamWithHeadersProvider(
                _ptr,
                tableName,
                descriptorProto,
                bridge.Callback,
                handlePtr,
                bridge.FreeCallback,
                ref nativeOpts);
        }
        catch
        {
            // On a thrown failure the FFI already invoked the free callback (the
            // provider is built before any fallible work), so the handle is
            // already freed — do NOT free it again here (double-free).
            throw;
        }

        // The FFI owns the provider handle now; the stream keeps no reference.
        return new ZerobusStream(streamPtr);
    }

    private async Task<ZerobusStream> CreateStreamWithHeadersProviderCoreAsync(
        TableProperties tableProperties,
        IHeadersProvider headersProvider,
        StreamConfigurationOptions? options)
    {
        ValidateStreamConfiguration(tableProperties, options);

        var nativeOpts = NativeInterop.ConvertConfig(options);

        var bridge = new HeadersProviderBridge(headersProvider);

        var handle = GCHandle.Alloc(bridge);
        var handlePtr = GCHandle.ToIntPtr(handle);
        var tableName = tableProperties.TableName;
        var descriptorProto = tableProperties.DescriptorProto ?? [];

        // Ownership transfer point: this call hands the bridge GCHandle to the
        // FFI as user_data. On failure, the native side has already invoked the
        // free callback; on success, it owns cleanup for the full stream lifetime.
        var streamPtr = await NativeInterop.SdkCreateStreamWithHeadersProviderAsync(
                _ptr,
                tableName,
                descriptorProto,
                bridge.Callback,
                handlePtr,
                bridge.FreeCallback,
                nativeOpts)
            .ConfigureAwait(false);

        // The FFI owns the provider handle now; the stream keeps no reference.
        return new ZerobusStream(streamPtr);
    }

    /// <summary>
    /// Recreates a JSON or Protocol Buffer stream from an existing stream.
    /// This is used for recovery scenarios where a stream needs to be re-established
    /// using the existing stream's configuration and state.
    /// </summary>
    /// <param name="stream">The existing stream to recreate from.</param>
    /// <remarks>
    /// <para>
    /// This method transfers ownership from <paramref name="stream"/> to the returned stream.
    /// The input stream is disposed as part of recreation and cannot be used afterward.
    /// </para>
    /// <para>
    /// A later <see cref="IDisposable.Dispose"/> call on the original stream wrapper is a no-op,
    /// so <c>using</c> declarations remain safe:
    /// <code>
    /// using var stream = sdk.CreateStream(...);
    /// stream.Close();
    /// using var recreated = sdk.RecreateStream(stream);
    /// </code>
    /// </para>
    /// </remarks>
    /// <returns>A new <see cref="ZerobusStream"/> ready for record ingestion.</returns>
    /// <exception cref="ZerobusException">Thrown if the stream cannot be recreated.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the SDK has been disposed.</exception>
    public ZerobusStream RecreateStream(ZerobusStream stream)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        ArgumentNullException.ThrowIfNull(stream);

        var ptr = NativeInterop.SdkRecreateStream(_ptr, stream.NativePointer);
        return stream.Recreate(ptr);
    }

    /// <summary>
    /// Recreates a JSON or Protocol Buffer stream asynchronously from an existing stream.
    /// </summary>
    public async Task<ZerobusStream> RecreateStreamAsync(
        ZerobusStream stream)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        ArgumentNullException.ThrowIfNull(stream);

        var ptr = await NativeInterop.SdkRecreateStreamAsync(_ptr, stream.NativePointer)
            .ConfigureAwait(false);
        return stream.Recreate(ptr);
    }

    /// <summary>
    /// Recreates a JSON-only stream after the original stream has failed or closed.
    /// </summary>
    public JsonZerobusStream RecreateStream(JsonZerobusStream stream)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        ArgumentNullException.ThrowIfNull(stream);

        return new JsonZerobusStream(RecreateStream(stream.InnerStream));
    }

    /// <summary>
    /// Recreates a JSON-only stream asynchronously after the original stream has failed or closed.
    /// </summary>
    public async Task<JsonZerobusStream> RecreateStreamAsync(
        JsonZerobusStream stream)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        ArgumentNullException.ThrowIfNull(stream);

        var recreated = await RecreateStreamAsync(stream.InnerStream).ConfigureAwait(false);
        return new JsonZerobusStream(recreated);
    }

    /// <summary>
    /// Recreates a protobuf-only stream after the original stream has failed or closed.
    /// </summary>
    public ProtoZerobusStream RecreateStream(ProtoZerobusStream stream)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        ArgumentNullException.ThrowIfNull(stream);

        return new ProtoZerobusStream(RecreateStream(stream.InnerStream));
    }

    /// <summary>
    /// Recreates a protobuf-only stream asynchronously after the original stream has failed or closed.
    /// </summary>
    public async Task<ProtoZerobusStream> RecreateStreamAsync(ProtoZerobusStream stream)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        ArgumentNullException.ThrowIfNull(stream);

        var recreated = await RecreateStreamAsync(stream.InnerStream).ConfigureAwait(false);
        return new ProtoZerobusStream(recreated);
    }

    private static StreamConfigurationOptions NormalizeStreamOptions(
        StreamConfigurationOptions? options,
        RecordType recordType)
    {
        return (options ?? StreamConfigurationOptions.Default) with { RecordType = recordType };
    }

    private static void ValidateStreamConfiguration(
        TableProperties tableProperties,
        StreamConfigurationOptions? options)
    {
        ArgumentNullException.ThrowIfNull(tableProperties);

        var descriptor = tableProperties.DescriptorProto;
        var effectiveRecordType = options?.RecordType switch
        {
            null => StreamConfigurationOptions.Default.RecordType,
            RecordType.Unspecified => StreamConfigurationOptions.Default.RecordType,
            var recordType => recordType,
        };

        if (descriptor is { Length: 0 })
        {
            throw new ArgumentException(
                "DescriptorProto must be null for JSON streams or a non-empty serialized DescriptorProto for protobuf streams.",
                nameof(tableProperties));
        }

        var hasDescriptor = descriptor is { Length: > 0 };

        if (effectiveRecordType == RecordType.Json && hasDescriptor)
        {
            throw new ArgumentException(
                "JSON streams cannot specify DescriptorProto. Use TableProperties(tableName) or CreateJsonStream(...).",
                nameof(tableProperties));
        }

        if (effectiveRecordType == RecordType.Proto && !hasDescriptor)
        {
            throw new ArgumentException(
                "Proto streams require a non-empty DescriptorProto. Use TableProperties(tableName, descriptorProto) or CreateProtoStream(...).",
                nameof(tableProperties));
        }
    }

    private void Free()
    {
        var ptr = Interlocked.Exchange(ref _ptr, IntPtr.Zero);
        if (ptr != IntPtr.Zero)
        {
            NativeMethods.SdkFree(ptr);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0) return;
        Free();
        GC.SuppressFinalize(this);
    }

    /// <summary>Safety-net release of native memory for leaked instances.</summary>
    ~ZerobusSdk() => Free();
}
