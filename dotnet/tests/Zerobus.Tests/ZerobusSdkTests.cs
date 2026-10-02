using Databricks.Zerobus;
using NUnit.Framework;

namespace Databricks.Zerobus.Tests;

[TestFixture]
public class ZerobusSdkTests
{
    private const string TableName = "catalog.schema.table";

    // Serialized DescriptorProto { name: "Test" }.
    private static readonly byte[] DescriptorProto = [0x0a, 0x04, 0x54, 0x65, 0x73, 0x74];

    private sealed class CountingHeadersProvider : IHeadersProvider
    {
        private int _calls;

        public int Calls => Volatile.Read(ref _calls);

        public IDictionary<string, string> GetHeaders()
        {
            Interlocked.Increment(ref _calls);
            return new Dictionary<string, string>();
        }
    }

    private static IEnumerable<TestCaseData> StreamFactories()
    {
        static TestCaseData Factory(string name, Func<ZerobusSdk, IHeadersProvider, Task> create) =>
            new TestCaseData(create).SetName($"{name}_AfterDispose_ThrowsObjectDisposedException");

        var jsonTable = new TableProperties(TableName);
        var protoTable = new TableProperties(TableName, DescriptorProto);
        var jsonOptions = StreamConfigurationOptions.Default with { RecordType = RecordType.Json };

        yield return Factory(nameof(ZerobusSdk.CreateStream),
            (sdk, _) => { sdk.CreateStream(protoTable, "client-id", "client-secret"); return Task.CompletedTask; });
        yield return Factory(nameof(ZerobusSdk.CreateStreamAsync),
            (sdk, _) => sdk.CreateStreamAsync(protoTable, "client-id", "client-secret"));
        yield return Factory(nameof(ZerobusSdk.CreateJsonStream),
            (sdk, _) => { sdk.CreateJsonStream(TableName, "client-id", "client-secret"); return Task.CompletedTask; });
        yield return Factory(nameof(ZerobusSdk.CreateJsonStreamAsync),
            (sdk, _) => sdk.CreateJsonStreamAsync(TableName, "client-id", "client-secret"));
        yield return Factory(nameof(ZerobusSdk.CreateProtoStream),
            (sdk, _) => { sdk.CreateProtoStream(TableName, DescriptorProto, "client-id", "client-secret"); return Task.CompletedTask; });
        yield return Factory(nameof(ZerobusSdk.CreateProtoStreamAsync),
            (sdk, _) => sdk.CreateProtoStreamAsync(TableName, DescriptorProto, "client-id", "client-secret"));
        yield return Factory(nameof(ZerobusSdk.CreateStreamWithHeadersProvider),
            (sdk, provider) => { sdk.CreateStreamWithHeadersProvider(jsonTable, provider, jsonOptions); return Task.CompletedTask; });
        yield return Factory(nameof(ZerobusSdk.CreateStreamWithHeadersProviderAsync),
            (sdk, provider) => sdk.CreateStreamWithHeadersProviderAsync(jsonTable, provider, jsonOptions));
        yield return Factory(nameof(ZerobusSdk.CreateJsonStreamWithHeadersProvider),
            (sdk, provider) => { sdk.CreateJsonStreamWithHeadersProvider(TableName, provider); return Task.CompletedTask; });
        yield return Factory(nameof(ZerobusSdk.CreateJsonStreamWithHeadersProviderAsync),
            (sdk, provider) => sdk.CreateJsonStreamWithHeadersProviderAsync(TableName, provider));
        yield return Factory(nameof(ZerobusSdk.CreateProtoStreamWithHeadersProvider),
            (sdk, provider) => { sdk.CreateProtoStreamWithHeadersProvider(TableName, DescriptorProto, provider); return Task.CompletedTask; });
        yield return Factory(nameof(ZerobusSdk.CreateProtoStreamWithHeadersProviderAsync),
            (sdk, provider) => sdk.CreateProtoStreamWithHeadersProviderAsync(TableName, DescriptorProto, provider));
    }

    [TestCaseSource(nameof(StreamFactories))]
    public void CreateStreamFactory_AfterDispose_ThrowsObjectDisposedException(
        Func<ZerobusSdk, IHeadersProvider, Task> create)
    {
        var sdk = ZerobusSdk.CreateBuilder()
            .Endpoint("https://zerobus.databricks.com")
            .UnityCatalogUrl("https://workspace.databricks.com")
            .Build();
        sdk.Dispose();
        var provider = new CountingHeadersProvider();

        Assert.ThrowsAsync<ObjectDisposedException>(() => create(sdk, provider));
        Assert.That(provider.Calls, Is.Zero);
    }
}
