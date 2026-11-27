using System.Diagnostics;
using Grpc.Core;
using NUnit.Framework;

namespace Databricks.Zerobus.IntegrationTests;

[TestFixture]
[Parallelizable(ParallelScope.Children)]
public class StreamBuilderIntegrationTests : IntegrationTestBase
{
    private static StreamBuilder CreateBuilder(ZerobusSdk sdk) =>
        sdk.CreateStreamBuilder()
            .Table(TestTableName)
            .HeadersProvider(new TestHeadersProvider())
            .Recovery(false);

    [TestCase(false)]
    [TestCase(true)]
    public async Task Json_OpensWorkingStream(bool useAsync)
    {
        await using var fixture = await MockServerFixture.StartAsync();
        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("builder_json"),
            MockResponses.RecordAckResponse(0),
        ]);
        using var sdk = CreateDefaultSdk(fixture);
        var builder = CreateBuilder(sdk).Json();

        await using var stream = useAsync ? await builder.BuildAsync() : builder.Build();
        var offset = stream.IngestRecord("{\"message\":\"json\"}");
        await stream.FlushAsync();

        Assert.That(offset, Is.EqualTo(0));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Proto_OpensWorkingStream(bool useAsync)
    {
        await using var fixture = await MockServerFixture.StartAsync();
        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("builder_proto"),
            MockResponses.RecordAckResponse(0),
        ]);
        using var sdk = CreateDefaultSdk(fixture);
        var builder = CreateBuilder(sdk).Proto(TestDescriptor.CreateTestDescriptorProto());

        await using var stream = useAsync ? await builder.BuildAsync() : builder.Build();
        var offset = stream.IngestRecord("proto-payload"u8.ToArray());
        await stream.FlushAsync();

        Assert.That(offset, Is.EqualTo(0));
    }

    [Test]
    public async Task HeadersProvider_AfterOAuth_Wins()
    {
        await using var fixture = await MockServerFixture.StartAsync();
        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("builder_auth"),
        ]);
        using var sdk = CreateDefaultSdk(fixture);
        var provider = new CountingHeadersProvider(new Dictionary<string, string>
        {
            ["authorization"] = "Bearer test_token",
        });

        using var stream = CreateBuilder(sdk)
            .OAuth("id", "secret")
            .HeadersProvider(provider)
            .Json()
            .Build();

        Assert.That(provider.CallCount, Is.GreaterThan(0));
    }

    [Test]
    public async Task RecoveryDisabled_FailsFastOnRetriableError()
    {
        await using var fixture = await MockServerFixture.StartAsync();
        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.ErrorResponse(StatusCode.Unavailable, "Retriable error"),
        ]);
        using var sdk = CreateDefaultSdk(fixture);
        var sw = Stopwatch.StartNew();

        var ex = Assert.Throws<ZerobusException>(() => CreateBuilder(sdk).Json().Build());

        Assert.That(ex!.IsRetryable, Is.True);
        Assert.That(sw.ElapsedMilliseconds, Is.LessThan(1000));
    }
}
