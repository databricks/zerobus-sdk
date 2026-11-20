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
/// new stream.
/// </para>
/// <para>
/// <c>Flush()</c> can return before the last <see cref="OnAck"/> has run.
/// <c>Close()</c> and <c>Dispose()</c> wait for pending callbacks, for up to
/// <see cref="StreamConfigurationOptions.CallbackMaxWaitTimeMs"/>, so read state
/// that your callbacks update after them. After <c>Dispose()</c> returns, no new
/// callback starts; one already running may still finish.
/// </para>
/// <para>
/// Callbacks run on the SDK's background threads, not on the thread that
/// created the stream, so the implementation must be thread-safe. One stream's
/// callbacks run one at a time; a callback shared by several streams can run
/// concurrently. Keep them short and move longer work to your own task: a slow
/// or blocking callback holds an SDK thread and can delay <c>Flush()</c>,
/// <c>Close()</c> and other streams. Do not call stream methods from a callback.
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
