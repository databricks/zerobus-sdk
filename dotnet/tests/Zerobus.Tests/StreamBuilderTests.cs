using Databricks.Zerobus;
using NUnit.Framework;

namespace Databricks.Zerobus.Tests;

[TestFixture]
public class StreamBuilderTests
{
    private static ZerobusSdk CreateSdk() =>
        ZerobusSdk.CreateBuilder()
            .Endpoint("https://zerobus.databricks.com")
            .UnityCatalogUrl("https://workspace.databricks.com")
            .Build();

    private sealed class NoopHeadersProvider : IHeadersProvider
    {
        public IDictionary<string, string> GetHeaders() => new Dictionary<string, string>();
    }

    [Test]
    public void Setters_ReturnSameBuilderInstance()
    {
        using var sdk = CreateSdk();
        var builder = sdk.CreateStreamBuilder();

        Assert.Multiple(() =>
        {
            Assert.That(builder.Table("c.s.t"), Is.SameAs(builder));
            Assert.That(builder.OAuth("id", "secret"), Is.SameAs(builder));
            Assert.That(builder.HeadersProvider(new NoopHeadersProvider()), Is.SameAs(builder));
            Assert.That(builder.MaxInflightRequests(1), Is.SameAs(builder));
            Assert.That(builder.Recovery(false), Is.SameAs(builder));
            Assert.That(builder.RecoveryTimeoutMs(1), Is.SameAs(builder));
            Assert.That(builder.RecoveryBackoffMs(1), Is.SameAs(builder));
            Assert.That(builder.RecoveryRetries(1), Is.SameAs(builder));
            Assert.That(builder.ServerLackOfAckTimeoutMs(1), Is.SameAs(builder));
            Assert.That(builder.FlushTimeoutMs(1), Is.SameAs(builder));
            Assert.That(builder.StreamPausedMaxWaitTimeMs(null), Is.SameAs(builder));
        });
    }

    [Test]
    public void NullArguments_Throw()
    {
        using var sdk = CreateSdk();
        var builder = sdk.CreateStreamBuilder();

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => builder.Table(null!));
            Assert.Throws<ArgumentNullException>(() => builder.OAuth(null!, "secret"));
            Assert.Throws<ArgumentNullException>(() => builder.OAuth("id", null!));
            Assert.Throws<ArgumentNullException>(() => builder.HeadersProvider(null!));
            Assert.Throws<ArgumentNullException>(() => builder.Proto(null!));
        });
    }

    [Test]
    public void Proto_EmptyDescriptor_Throws()
    {
        using var sdk = CreateSdk();

        Assert.Throws<ArgumentException>(() => sdk.CreateStreamBuilder().Proto([]));
    }

    [TestCase("")]
    [TestCase(null)]
    public void Build_WithoutTable_Throws(string? tableName)
    {
        using var sdk = CreateSdk();
        var builder = sdk.CreateStreamBuilder().OAuth("id", "secret");
        if (tableName is not null) builder.Table(tableName);

        var ex = Assert.Throws<InvalidOperationException>(() => builder.Json().Build());
        Assert.That(ex!.Message, Is.EqualTo("table name is required: call Table()"));
    }

    [Test]
    public void Build_WithoutAuth_Throws()
    {
        using var sdk = CreateSdk();
        var builder = sdk.CreateStreamBuilder().Table("c.s.t");

        var ex = Assert.Throws<InvalidOperationException>(() => builder.Json().Build());
        Assert.That(ex!.Message, Is.EqualTo("authentication is required: call OAuth() or HeadersProvider()"));
    }

    [Test]
    public void BuildAsync_WithoutAuth_Throws()
    {
        using var sdk = CreateSdk();
        var builder = sdk.CreateStreamBuilder().Table("c.s.t");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => builder.Proto([1]).BuildAsync());
        Assert.That(ex!.Message, Is.EqualTo("authentication is required: call OAuth() or HeadersProvider()"));
    }

    [Test]
    public void CreateStreamBuilder_DisposedSdk_Throws()
    {
        var sdk = CreateSdk();
        sdk.Dispose();

        Assert.Throws<ObjectDisposedException>(() => sdk.CreateStreamBuilder());
    }

    [Test]
    public void Build_AfterSdkDisposed_Throws()
    {
        var sdk = CreateSdk();
        var builder = sdk.CreateStreamBuilder().Table("c.s.t").OAuth("id", "secret").Json();
        sdk.Dispose();

        Assert.Throws<ObjectDisposedException>(() => builder.Build());
        Assert.ThrowsAsync<ObjectDisposedException>(() => builder.BuildAsync());
    }
}
