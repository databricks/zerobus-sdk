namespace Databricks.Zerobus;

/// <summary>
/// Fluent builder for creating a Zerobus stream. Obtain one from
/// <see cref="ZerobusSdk.CreateStreamBuilder"/>.
/// </summary>
/// <remarks>
/// <para>
/// Set the table, authentication, and any configuration overrides in any order, then
/// select the record format with <see cref="Json"/> or <see cref="Proto"/>. The format
/// selector returns a typed builder whose <c>Build</c> / <c>BuildAsync</c> opens a
/// stream of the matching type. Settings left unset use the SDK defaults.
/// </para>
/// <para>
/// A builder is not thread-safe. It can build several streams; each build uses the
/// settings at that moment.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// await using var stream = await sdk.CreateStreamBuilder()
///     .Table("catalog.schema.table")
///     .OAuth(clientId, clientSecret)
///     .Json()
///     .BuildAsync();
/// </code>
/// </example>
public sealed class StreamBuilder
{
    private readonly ZerobusSdk _sdk;
    private string? _tableName;
    private string? _clientId;
    private string? _clientSecret;
    private IHeadersProvider? _headersProvider;
    private StreamConfigurationOptions _options = new();

    internal StreamBuilder(ZerobusSdk sdk)
    {
        _sdk = sdk;
    }

    /// <summary>Sets the target table (required).</summary>
    /// <param name="tableName">Fully qualified table name in the form <c>catalog.schema.table</c>.</param>
    /// <returns>This builder, for chaining.</returns>
    public StreamBuilder Table(string tableName)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        _tableName = tableName;
        return this;
    }

    /// <summary>
    /// Authenticates with OAuth 2.0 client credentials. Requires the SDK to be built
    /// with <see cref="ZerobusSdkBuilder.UnityCatalogUrl"/>.
    /// Of this and <see cref="HeadersProvider"/>, the last call wins.
    /// </summary>
    /// <param name="clientId">OAuth 2.0 client ID.</param>
    /// <param name="clientSecret">OAuth 2.0 client secret.</param>
    /// <returns>This builder, for chaining.</returns>
    public StreamBuilder OAuth(string clientId, string clientSecret)
    {
        ArgumentNullException.ThrowIfNull(clientId);
        ArgumentNullException.ThrowIfNull(clientSecret);
        _clientId = clientId;
        _clientSecret = clientSecret;
        _headersProvider = null;
        return this;
    }

    /// <summary>
    /// Authenticates with a custom headers provider (managed identity, vaults, etc.).
    /// Of this and <see cref="OAuth"/>, the last call wins.
    /// </summary>
    /// <param name="headersProvider">Supplies the authentication headers.</param>
    /// <returns>This builder, for chaining.</returns>
    public StreamBuilder HeadersProvider(IHeadersProvider headersProvider)
    {
        ArgumentNullException.ThrowIfNull(headersProvider);
        _headersProvider = headersProvider;
        _clientId = null;
        _clientSecret = null;
        return this;
    }

    /// <summary>Sets the maximum number of requests awaiting acknowledgment. Default: 1,000,000.</summary>
    /// <param name="maxInflightRequests">Maximum in-flight requests.</param>
    /// <returns>This builder, for chaining.</returns>
    public StreamBuilder MaxInflightRequests(ulong maxInflightRequests)
    {
        _options = _options with { MaxInflightRequests = maxInflightRequests };
        return this;
    }

    /// <summary>Enables or disables automatic recovery on retryable failures. Default: enabled.</summary>
    /// <param name="enabled">Whether to recover automatically.</param>
    /// <returns>This builder, for chaining.</returns>
    public StreamBuilder Recovery(bool enabled)
    {
        _options = _options with { Recovery = enabled };
        return this;
    }

    /// <summary>Sets the timeout for each recovery attempt. Default: 15,000 ms.</summary>
    /// <param name="timeoutMs">Timeout in milliseconds.</param>
    /// <returns>This builder, for chaining.</returns>
    public StreamBuilder RecoveryTimeoutMs(ulong timeoutMs)
    {
        _options = _options with { RecoveryTimeoutMs = timeoutMs };
        return this;
    }

    /// <summary>Sets the backoff between recovery attempts. Default: 2,000 ms.</summary>
    /// <param name="backoffMs">Backoff in milliseconds.</param>
    /// <returns>This builder, for chaining.</returns>
    public StreamBuilder RecoveryBackoffMs(ulong backoffMs)
    {
        _options = _options with { RecoveryBackoffMs = backoffMs };
        return this;
    }

    /// <summary>Sets the maximum number of recovery attempts. Default: 4.</summary>
    /// <param name="retries">Maximum recovery attempts.</param>
    /// <returns>This builder, for chaining.</returns>
    public StreamBuilder RecoveryRetries(uint retries)
    {
        _options = _options with { RecoveryRetries = retries };
        return this;
    }

    /// <summary>Sets how long to wait for a server acknowledgment before failing the stream. Default: 60,000 ms.</summary>
    /// <param name="timeoutMs">Timeout in milliseconds.</param>
    /// <returns>This builder, for chaining.</returns>
    public StreamBuilder ServerLackOfAckTimeoutMs(ulong timeoutMs)
    {
        _options = _options with { ServerLackOfAckTimeoutMs = timeoutMs };
        return this;
    }

    /// <summary>Sets the timeout for <c>Flush</c> operations. Default: 300,000 ms.</summary>
    /// <param name="timeoutMs">Timeout in milliseconds.</param>
    /// <returns>This builder, for chaining.</returns>
    public StreamBuilder FlushTimeoutMs(ulong timeoutMs)
    {
        _options = _options with { FlushTimeoutMs = timeoutMs };
        return this;
    }

    /// <summary>Sets how long a graceful close waits when the server sends a close signal: <c>null</c> waits the full server-specified duration (default), <c>0</c> recovers immediately, and a positive value waits up to <c>min(value, serverDuration)</c>.</summary>
    /// <param name="maxWaitTimeMs">Maximum wait in milliseconds, or <c>null</c>.</param>
    /// <returns>This builder, for chaining.</returns>
    public StreamBuilder StreamPausedMaxWaitTimeMs(ulong? maxWaitTimeMs)
    {
        _options = _options with { StreamPausedMaxWaitTimeMs = maxWaitTimeMs };
        return this;
    }

    /// <summary>Selects the JSON record format.</summary>
    /// <returns>A builder that opens a <see cref="JsonZerobusStream"/>.</returns>
    public JsonStreamBuilder Json() => new(this);

    /// <summary>Selects the protobuf record format.</summary>
    /// <param name="descriptorProto">Serialized <c>DescriptorProto</c> of the record message.</param>
    /// <returns>A builder that opens a <see cref="ProtoZerobusStream"/>.</returns>
    /// <exception cref="ArgumentException">Thrown if <paramref name="descriptorProto"/> is empty.</exception>
    public ProtoStreamBuilder Proto(byte[] descriptorProto)
    {
        ArgumentNullException.ThrowIfNull(descriptorProto);
        if (descriptorProto.Length == 0)
        {
            throw new ArgumentException(
                "descriptorProto must be a non-empty serialized DescriptorProto.",
                nameof(descriptorProto));
        }

        return new ProtoStreamBuilder(this, descriptorProto);
    }

    internal ZerobusStream Build(byte[]? descriptorProto, RecordType recordType)
    {
        var (table, options) = Prepare(descriptorProto, recordType);
        return _headersProvider is not null
            ? _sdk.CreateStreamWithHeadersProviderCore(table, _headersProvider, options)
            : _sdk.CreateStreamCore(table, _clientId!, _clientSecret!, options);
    }

    internal Task<ZerobusStream> BuildAsync(byte[]? descriptorProto, RecordType recordType)
    {
        var (table, options) = Prepare(descriptorProto, recordType);
        return _headersProvider is not null
            ? _sdk.CreateStreamWithHeadersProviderCoreAsync(table, _headersProvider, options)
            : _sdk.CreateStreamCoreAsync(table, _clientId!, _clientSecret!, options);
    }

    private (TableProperties, StreamConfigurationOptions) Prepare(
        byte[]? descriptorProto,
        RecordType recordType)
    {
        if (string.IsNullOrEmpty(_tableName))
            throw new InvalidOperationException("table name is required: call Table()");
        if (_headersProvider is null && _clientId is null)
            throw new InvalidOperationException("authentication is required: call OAuth() or HeadersProvider()");

        return (
            new TableProperties(_tableName, descriptorProto),
            _options with { RecordType = recordType });
    }
}

/// <summary>
/// Opens a <see cref="JsonZerobusStream"/>. Obtain one from <see cref="StreamBuilder.Json"/>.
/// </summary>
public sealed class JsonStreamBuilder
{
    private readonly StreamBuilder _base;

    internal JsonStreamBuilder(StreamBuilder builder)
    {
        _base = builder;
    }

    /// <summary>Opens the JSON stream.</summary>
    /// <exception cref="InvalidOperationException">Thrown if the table or authentication is not set.</exception>
    /// <exception cref="ZerobusException">Thrown if the stream cannot be created.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the SDK has been disposed.</exception>
    public JsonZerobusStream Build() => new(_base.Build(null, RecordType.Json));

    /// <inheritdoc cref="Build"/>
    public async Task<JsonZerobusStream> BuildAsync() =>
        new(await _base.BuildAsync(null, RecordType.Json).ConfigureAwait(false));
}

/// <summary>
/// Opens a <see cref="ProtoZerobusStream"/>. Obtain one from <see cref="StreamBuilder.Proto"/>.
/// </summary>
public sealed class ProtoStreamBuilder
{
    private readonly StreamBuilder _base;
    private readonly byte[] _descriptorProto;

    internal ProtoStreamBuilder(StreamBuilder builder, byte[] descriptorProto)
    {
        _base = builder;
        _descriptorProto = descriptorProto;
    }

    /// <summary>Opens the protobuf stream.</summary>
    /// <exception cref="InvalidOperationException">Thrown if the table or authentication is not set.</exception>
    /// <exception cref="ZerobusException">Thrown if the stream cannot be created.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the SDK has been disposed.</exception>
    public ProtoZerobusStream Build() => new(_base.Build(_descriptorProto, RecordType.Proto));

    /// <inheritdoc cref="Build"/>
    public async Task<ProtoZerobusStream> BuildAsync() =>
        new(await _base.BuildAsync(_descriptorProto, RecordType.Proto).ConfigureAwait(false));
}
