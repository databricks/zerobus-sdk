namespace Databricks.Zerobus;

/// <summary>
/// Receives acknowledgments for records ingested on a stream (Beta). Set it on
/// <see cref="StreamConfigurationOptions.AckCallback"/>.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="OnAck"/> is called once for each ingest call that queued records,
/// with that call's offset (one call for a whole batch). Acks are delivered in
/// offset order, so an ack for offset N confirms every earlier offset too.
/// <see cref="OnError"/> is called only when the stream fails: once for each
/// record or batch still pending, with the error text. The stream is then
/// closed; recover the records with <c>GetUnackedRecords()</c> and send them on a
/// new stream. After a stream failure, <c>Close()</c> can return before pending
/// callbacks run: they may still run until <c>Dispose()</c>, and
/// <c>Dispose()</c> drops the rest. So some <see cref="OnError"/> calls may not
/// arrive; use the exception from <c>Flush()</c> and <c>GetUnackedRecords()</c>
/// as the source of truth for which records failed.
/// </para>
/// <para>
/// After <c>RecreateStream</c>, the new stream uses the same callback. Its
/// offsets restart, so replayed records are acked again with new offsets, and an
/// ack confirms only earlier offsets of the same stream. Pending
/// <see cref="OnError"/> calls of the old stream may be lost or arrive late,
/// mixed with the new stream's acks.
/// </para>
/// <para>
/// <c>Flush()</c> can return before the last <see cref="OnAck"/> has run. When
/// <c>Close()</c> returns, all callbacks have run, unless its wait for them took
/// longer than <see cref="StreamConfigurationOptions.CallbackMaxWaitTimeMs"/>
/// (default 5 s). <c>Dispose()</c> does the same, so read state that your
/// callbacks update after it. After <c>Dispose()</c> returns, a callback that
/// was about to start may still start, and one already running may still
/// finish; no other callback runs.
/// </para>
/// <para>
/// Call <c>Dispose()</c> on every stream that has a callback. The SDK holds the
/// callback strongly, so a callback that references its stream keeps an
/// undisposed stream alive, and its finalizer never runs.
/// </para>
/// <para>
/// Callbacks run on the SDK's background threads, not on the thread that
/// created the stream, so the implementation must be thread-safe. One stream's
/// callbacks run one at a time; a callback shared by several streams can run
/// concurrently. Keep them short and move longer work to your own task: a slow
/// or blocking callback holds an SDK thread and can delay <c>Flush()</c>,
/// <c>Close()</c> and other streams. Do not call stream or
/// <see cref="ZerobusSdk"/> methods (such as <c>CreateStream</c> or
/// <c>RecreateStream</c>) from a callback; do recovery on your own task. A
/// stream method called from a callback while <c>Close()</c> runs stalls
/// <c>Close()</c> until its callback wait ends (never, with
/// <see cref="StreamConfigurationOptions.CallbackWaitForever"/>).
/// An exception thrown by a callback is caught and reported with
/// <see cref="System.Diagnostics.Trace.TraceError(string)"/>, including the
/// offset; later callbacks still run.
/// </para>
/// </remarks>
public interface IAckCallback
{
    /// <summary>Called when the server acknowledges the records up to <paramref name="offset"/>.</summary>
    void OnAck(long offset);

    /// <summary>Called for each pending record or batch when the stream fails.</summary>
    /// <param name="offset">The offset of the failed record or batch.</param>
    /// <param name="errorMessage">The error text from the SDK.</param>
    void OnError(long offset, string errorMessage);
}
