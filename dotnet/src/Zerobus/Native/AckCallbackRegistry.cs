// Bridges a managed IAckCallback to the ack_on_ack / ack_on_error fields of the
// native CStreamConfigurationOptions.

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Databricks.Zerobus.Native;

/// <summary>
/// Maps the integer key passed to the FFI as <c>ack_user_data</c> to the
/// stream's <see cref="IAckCallback"/>.
/// </summary>
/// <remarks>
/// A key, not a <see cref="GCHandle"/>, crosses the boundary: the ordinary
/// stream ack ABI has no free hook, so a callback can still arrive while or
/// after the stream is freed. A lookup that misses (the key was released) just
/// returns, where a freed GCHandle would be a use-after-free. Entries are
/// reference-counted because <c>RecreateStream</c> gives the new stream the
/// same callback and user data (the core clones them), so the key goes only
/// when the last stream using it is disposed.
/// </remarks>
internal static class AckCallbackRegistry
{
    private sealed class Entry
    {
        public Entry(IAckCallback callback) => Callback = callback;

        public IAckCallback Callback { get; }

        // Guarded by the registry lock.
        public int References = 1;
    }

    private static readonly ConcurrentDictionary<long, Entry> s_entries = new();
    private static readonly object s_lock = new();
    private static long s_nextKey;

    /// <summary>Registers <paramref name="callback"/> and returns its key, or 0 for null.</summary>
    public static long Register(IAckCallback? callback)
    {
        if (callback is null)
            return 0;
        var key = Interlocked.Increment(ref s_nextKey);
        s_entries[key] = new Entry(callback);
        return key;
    }

    /// <summary>Adds a reference for a stream that shares <paramref name="key"/>.</summary>
    public static void AddRef(long key)
    {
        if (key == 0)
            return;
        lock (s_lock)
        {
            if (s_entries.TryGetValue(key, out var entry))
                entry.References++;
        }
    }

    /// <summary>Drops one reference; the entry goes when none is left. Key 0 is a no-op.</summary>
    public static void Release(long key)
    {
        if (key == 0)
            return;
        lock (s_lock)
        {
            if (s_entries.TryGetValue(key, out var entry) && --entry.References <= 0)
                s_entries.TryRemove(key, out _);
        }
    }

    /// <summary>The number of registered callbacks (tests).</summary>
    internal static int Count => s_entries.Count;

    /// <summary>Whether <paramref name="callback"/> is still registered under any key (tests).</summary>
    internal static bool IsRegistered(IAckCallback callback) =>
        s_entries.Values.Any(entry => ReferenceEquals(entry.Callback, callback));

    /// <summary>Wires the trampolines into <paramref name="config"/> for a non-zero key.</summary>
    public static unsafe void Apply(ref CStreamConfigurationOptions config, long key)
    {
        if (key == 0)
            return;
        config.AckOnAck = (IntPtr)(delegate* unmanaged[Cdecl]<long, IntPtr, void>)&OnAckNative;
        config.AckOnError = (IntPtr)(delegate* unmanaged[Cdecl]<long, IntPtr, IntPtr, void>)&OnErrorNative;
        config.AckUserData = (IntPtr)key;
    }

    /// <summary>Delivers an ack to the callback registered under <paramref name="key"/>.</summary>
    internal static void DispatchAck(long key, long offset)
    {
        if (!s_entries.TryGetValue(key, out var entry))
            return; // The stream was disposed; drop the late callback.
        try
        {
            entry.Callback.OnAck(offset);
        }
        catch (Exception ex)
        {
            // Never let an exception unwind across the native boundary.
            LogCallbackException("OnAck", offset, ex);
        }
    }

    /// <summary>Delivers an error to the callback registered under <paramref name="key"/>.</summary>
    internal static void DispatchError(long key, long offset, string message)
    {
        if (!s_entries.TryGetValue(key, out var entry))
            return;
        try
        {
            entry.Callback.OnError(offset, message);
        }
        catch (Exception ex)
        {
            // Never let an exception unwind across the native boundary.
            LogCallbackException("OnError", offset, ex);
        }
    }

    // Reports a swallowed callback exception. Trace is built in and does nothing
    // unless the application adds a listener.
    private static void LogCallbackException(string method, long offset, Exception ex)
    {
        try
        {
            Trace.TraceError($"Zerobus: IAckCallback.{method}(offset {offset}) threw: {ex}");
        }
        catch
        {
            // A failing trace listener must not unwind across the native boundary either.
        }
    }

    [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvCdecl) })]
    private static void OnAckNative(long offset, IntPtr userData)
    {
        try
        {
            DispatchAck((long)userData, offset);
        }
        catch
        {
            // Never let an exception unwind across the native boundary.
        }
    }

    [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvCdecl) })]
    private static void OnErrorNative(long offset, IntPtr errorMessage, IntPtr userData)
    {
        try
        {
            // errorMessage is valid only during this call, so copy it first.
            var message = errorMessage == IntPtr.Zero
                ? string.Empty
                : Marshal.PtrToStringUTF8(errorMessage) ?? string.Empty;
            DispatchError((long)userData, offset, message);
        }
        catch
        {
            // Never let an exception unwind across the native boundary.
        }
    }
}
