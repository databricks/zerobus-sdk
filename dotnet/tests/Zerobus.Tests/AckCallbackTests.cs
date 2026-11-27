using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using Databricks.Zerobus;
using Databricks.Zerobus.Native;
using NUnit.Framework;

namespace Databricks.Zerobus.Tests;

/// <summary>
/// The ack callback bridge: options to C, the ref-counted key registry, the
/// native trampolines, and registry cleanup after a failed create.
/// </summary>
/// <remarks>
/// <see cref="AckCallbackRegistry.Count"/> is process-wide, so these tests
/// don't run in parallel with each other.
/// </remarks>
[TestFixture]
[NonParallelizable]
public class AckCallbackTests
{
    private sealed class RecordingCallback : IAckCallback
    {
        public List<long> Acks { get; } = [];
        public List<(long Offset, string Message)> Errors { get; } = [];
        public bool ThrowOnce { get; set; }

        public void OnAck(long offset)
        {
            if (TakeThrow())
                throw new InvalidOperationException("OnAck test exception");
            Acks.Add(offset);
        }

        public void OnError(long offset, string errorMessage)
        {
            if (TakeThrow())
                throw new InvalidOperationException("OnError test exception");
            Errors.Add((offset, errorMessage));
        }

        private bool TakeThrow()
        {
            if (!ThrowOnce)
                return false;
            ThrowOnce = false;
            return true;
        }
    }

    // D1: the AckCallback key and CallbackMaxWaitTimeMs reach the C options.
    [Test]
    public void ConvertConfig_AckKey_SetsTrampolinesAndUserData()
    {
        var none = NativeInterop.ConvertConfig(StreamConfigurationOptions.Default, 0);
        Assert.That(none.AckOnAck, Is.EqualTo(IntPtr.Zero));
        Assert.That(none.AckOnError, Is.EqualTo(IntPtr.Zero));
        Assert.That(none.AckUserData, Is.EqualTo(IntPtr.Zero));

        var withKey = NativeInterop.ConvertConfig(StreamConfigurationOptions.Default, 42);
        Assert.That(withKey.AckOnAck, Is.Not.EqualTo(IntPtr.Zero));
        Assert.That(withKey.AckOnError, Is.Not.EqualTo(IntPtr.Zero));
        Assert.That(withKey.AckUserData, Is.EqualTo((IntPtr)42));
    }

    [TestCase(0UL, true, 0UL)]
    [TestCase(1234UL, true, 1234UL)]
    [TestCase(StreamConfigurationOptions.CallbackWaitForever, false, 0UL)]
    public void ConvertConfig_CallbackMaxWaitTimeMs_MapsToNative(ulong wait, bool expectedHas, ulong expectedValue)
    {
        var converted = NativeInterop.ConvertConfig(
            StreamConfigurationOptions.Default with { CallbackMaxWaitTimeMs = wait });

        Assert.That(converted.HasCallbackMaxWaitTimeMs, Is.EqualTo(expectedHas));
        if (expectedHas)
            Assert.That(converted.CallbackMaxWaitTimeMs, Is.EqualTo(expectedValue));
    }

    // D2: keys, reference counts, and a call after the last release.
    [Test]
    public void Registry_RefCountsKeysAndIgnoresReleasedKeys()
    {
        Assert.That(AckCallbackRegistry.Register(null), Is.EqualTo(0));

        var baseline = AckCallbackRegistry.Count;
        var first = new RecordingCallback();
        var second = new RecordingCallback();
        var firstKey = AckCallbackRegistry.Register(first);
        var secondKey = AckCallbackRegistry.Register(second);
        try
        {
            Assert.That(firstKey, Is.Not.EqualTo(0));
            Assert.That(secondKey, Is.Not.EqualTo(0).And.Not.EqualTo(firstKey));
            Assert.That(AckCallbackRegistry.Count, Is.EqualTo(baseline + 2));

            // A shared key survives one release.
            AckCallbackRegistry.AddRef(firstKey);
            AckCallbackRegistry.Release(firstKey);
            AckCallbackRegistry.DispatchAck(firstKey, 5);
            Assert.That(first.Acks, Is.EqualTo(new long[] { 5 }));

            // The last release removes it; later calls reach nothing.
            AckCallbackRegistry.Release(firstKey);
            Assert.That(AckCallbackRegistry.Count, Is.EqualTo(baseline + 1));
            AckCallbackRegistry.DispatchAck(firstKey, 6);
            AckCallbackRegistry.DispatchError(firstKey, 6, "late");
            Assert.That(first.Acks, Is.EqualTo(new long[] { 5 }));
            Assert.That(first.Errors, Is.Empty);

            AckCallbackRegistry.Release(0);
            AckCallbackRegistry.Release(long.MaxValue);
            Assert.That(AckCallbackRegistry.Count, Is.EqualTo(baseline + 1));
            Assert.That(second.Acks, Is.Empty);
        }
        finally
        {
            AckCallbackRegistry.Release(secondKey);
        }
        Assert.That(AckCallbackRegistry.Count, Is.EqualTo(baseline));
    }

    // D2 (DG1): the pointers Apply writes are the trampolines, which copy the
    // UTF-8 message and map a null message to "".
    [Test]
    public unsafe void Apply_InstallsTrampolinesThatReachTheCallback()
    {
        var callback = new RecordingCallback();
        var key = AckCallbackRegistry.Register(callback);
        const string message = "échec: ошибка ✓";
        var nativeMessage = Marshal.StringToCoTaskMemUTF8(message);
        try
        {
            var config = NativeInterop.ConvertConfig(StreamConfigurationOptions.Default, key);
            var onAck = (delegate* unmanaged[Cdecl]<long, IntPtr, void>)config.AckOnAck;
            var onError = (delegate* unmanaged[Cdecl]<long, IntPtr, IntPtr, void>)config.AckOnError;

            onAck(7, config.AckUserData);
            onError(8, nativeMessage, config.AckUserData);
            onError(9, IntPtr.Zero, config.AckUserData);

            Assert.That(callback.Acks, Is.EqualTo(new long[] { 7 }));
            Assert.That(callback.Errors, Is.EqualTo(new[] { (8L, message), (9L, string.Empty) }));
        }
        finally
        {
            Marshal.FreeCoTaskMem(nativeMessage);
            AckCallbackRegistry.Release(key);
        }
    }

    // D3: an exception from the callback stays in the dispatch, and the key
    // keeps working.
    [Test]
    public void Dispatch_ContainsCallbackExceptions()
    {
        var callback = new RecordingCallback();
        var key = AckCallbackRegistry.Register(callback);
        var log = new CapturingTraceListener();
        Trace.Listeners.Add(log);
        try
        {
            callback.ThrowOnce = true;
            Assert.DoesNotThrow(() => AckCallbackRegistry.DispatchAck(key, 1));
            AckCallbackRegistry.DispatchAck(key, 2);

            callback.ThrowOnce = true;
            Assert.DoesNotThrow(() => AckCallbackRegistry.DispatchError(key, 3, "first"));
            AckCallbackRegistry.DispatchError(key, 4, "second");

            Assert.That(callback.Acks, Is.EqualTo(new long[] { 2 }));
            Assert.That(callback.Errors, Is.EqualTo(new[] { (4L, "second") }));
            // Each swallowed exception is reported with its method and offset.
            var text = log.Text;
            Assert.That(text, Does.Contain("IAckCallback.OnAck(offset 1) threw"));
            Assert.That(text, Does.Contain("OnAck test exception"));
            Assert.That(text, Does.Contain("IAckCallback.OnError(offset 3) threw"));
            Assert.That(text, Does.Contain("OnError test exception"));
            Assert.That(text, Does.Not.Contain("offset 2").And.Not.Contain("offset 4"));
        }
        finally
        {
            Trace.Listeners.Remove(log);
            AckCallbackRegistry.Release(key);
        }
    }

    private sealed class CapturingTraceListener : TraceListener
    {
        private readonly StringBuilder _text = new();

        public string Text
        {
            get
            {
                lock (_text)
                    return _text.ToString();
            }
        }

        public override void Write(string? message)
        {
            lock (_text)
                _text.Append(message);
        }

        public override void WriteLine(string? message)
        {
            lock (_text)
                _text.AppendLine(message);
        }
    }

    // D4: a create that fails before any I/O (malformed descriptor) leaves the
    // registry as it was, on all four create paths.
    [Test]
    public void FailedCreate_ReleasesTheAckCallbackKey()
    {
        using var sdk = ZerobusSdk.CreateBuilder()
            .Endpoint("http://127.0.0.1:1")
            .UnityCatalogUrl("http://127.0.0.1:1")
            .DisableTls()
            .Build();
        var table = new TableProperties("catalog.schema.table", [0xff, 0xff, 0xff]);
        var options = StreamConfigurationOptions.Default with { AckCallback = new RecordingCallback() };
        var baseline = AckCallbackRegistry.Count;

        Assert.That(() => sdk.CreateStream(table, "id", "secret", options),
            Throws.InstanceOf<ZerobusException>());
        Assert.That(AckCallbackRegistry.Count, Is.EqualTo(baseline), "CreateStream");

        Assert.That(async () => await sdk.CreateStreamAsync(table, "id", "secret", options),
            Throws.InstanceOf<ZerobusException>());
        Assert.That(AckCallbackRegistry.Count, Is.EqualTo(baseline), "CreateStreamAsync");

        Assert.That(() => sdk.CreateStreamWithHeadersProvider(table, new StaticHeadersProvider(), options),
            Throws.InstanceOf<ZerobusException>());
        Assert.That(AckCallbackRegistry.Count, Is.EqualTo(baseline), "CreateStreamWithHeadersProvider");

        Assert.That(async () => await sdk.CreateStreamWithHeadersProviderAsync(table, new StaticHeadersProvider(), options),
            Throws.InstanceOf<ZerobusException>());
        Assert.That(AckCallbackRegistry.Count, Is.EqualTo(baseline), "CreateStreamWithHeadersProviderAsync");
    }

    private sealed class StaticHeadersProvider : IHeadersProvider
    {
        public IDictionary<string, string> GetHeaders() => new Dictionary<string, string>();
    }
}
