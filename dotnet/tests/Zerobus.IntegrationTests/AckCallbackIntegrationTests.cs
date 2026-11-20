using System.Collections.Concurrent;
using Databricks.Zerobus.Native;
using NUnit.Framework;

namespace Databricks.Zerobus.IntegrationTests;

/// <summary>
/// The ack callback against the mock server: acks on both create paths, and
/// the registry key a recreated stream shares with the stream it replaces.
/// </summary>
/// <remarks>
/// <see cref="AckCallbackRegistry.Count"/> is process-wide, so this fixture
/// doesn't run in parallel with other tests.
/// </remarks>
[TestFixture]
[NonParallelizable]
public class AckCallbackIntegrationTests : IntegrationTestBase
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(5);

    private sealed class ChannelCallback : IAckCallback
    {
        private readonly ConcurrentQueue<long> _acks = new();
        private readonly ConcurrentQueue<string> _errors = new();
        private readonly SemaphoreSlim _ackSignal = new(0);

        public long[] Acks => _acks.ToArray();
        public string[] Errors => _errors.ToArray();

        public void OnAck(long offset)
        {
            _acks.Enqueue(offset);
            _ackSignal.Release();
        }

        public void OnError(long offset, string errorMessage) => _errors.Enqueue(errorMessage);

        /// <summary>Waits (bounded) until <paramref name="count"/> acks arrived.</summary>
        public async Task WaitForAcksAsync(int count)
        {
            for (var i = 0; i < count; i++)
                Assert.That(await _ackSignal.WaitAsync(Bound), Is.True, $"ack {i + 1} of {count} did not arrive");
        }
    }

    // D5: acks reach the callback once each, in order, on both create paths.
    [TestCase(false)]
    [TestCase(true)]
    public async Task AckCallback_ReceivesEachAck(bool createAsync)
    {
        await using var fixture = await MockServerFixture.StartAsync();
        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
            MockResponses.RecordAckResponse(0),
            MockResponses.RecordAckResponse(1),
            MockResponses.RecordAckResponse(2),
        ]);
        using var sdk = CreateDefaultSdk(fixture);
        var callback = new ChannelCallback();
        var options = CreateDefaultOptions() with { AckCallback = callback };

        using var stream = createAsync
            ? await sdk.CreateStreamWithHeadersProviderAsync(CreateTableProperties(), new TestHeadersProvider(), options)
            : sdk.CreateStreamWithHeadersProvider(CreateTableProperties(), new TestHeadersProvider(), options);
        for (var i = 0; i < 3; i++)
            stream.IngestRecord(System.Text.Encoding.UTF8.GetBytes($"test record {i}"));
        stream.Flush();
        await callback.WaitForAcksAsync(3);
        stream.Close();

        Assert.That(callback.Acks, Is.EqualTo(new long[] { 0, 1, 2 }));
        Assert.That(callback.Errors, Is.Empty);
    }

    // D6: a recreated stream shares the key; the old stream's teardown leaves
    // it, and the new stream's last Dispose releases it.
    [TestCase(false)]
    [TestCase(true)]
    public async Task RecreateStream_SharesTheAckCallbackKey(bool disposeAsync)
    {
        await using var fixture = await MockServerFixture.StartAsync();
        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
            MockResponses.CreateStreamResponse("test_stream_2"),
            MockResponses.RecordAckResponse(0),
        ]);
        using var sdk = CreateDefaultSdk(fixture);
        var callback = new ChannelCallback();
        var options = CreateDefaultOptions() with { AckCallback = callback };
        var baseline = AckCallbackRegistry.Count;

        var original = sdk.CreateStreamWithHeadersProvider(CreateTableProperties(), new TestHeadersProvider(), options);
        original.Close();
        var recreated = sdk.RecreateStream(original);
        original.Dispose();
        Assert.That(AckCallbackRegistry.Count, Is.EqualTo(baseline + 1),
            "the recreated stream still holds the shared key");

        recreated.IngestRecord("recreated stream"u8.ToArray());
        recreated.Flush();
        await callback.WaitForAcksAsync(1);
        Assert.That(callback.Acks, Is.EqualTo(new long[] { 0 }));

        if (disposeAsync)
            await recreated.DisposeAsync();
        else
            recreated.Dispose();
        Assert.That(AckCallbackRegistry.Count, Is.EqualTo(baseline),
            "the last stream's dispose releases the key");
    }
}
