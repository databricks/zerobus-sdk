namespace Databricks.Zerobus;

/// <summary>
/// Counts in-flight asynchronous operations on a stream and signals when none remain.
/// </summary>
/// <remarks>
/// The count is updated with interlocked operations. The drained signal is handed over
/// only under a lock, after re-reading the count, so a waiter can never observe
/// "drained" while an operation is registered. Without that re-check, the last
/// operation can decrement to zero, a new operation can register, and only then does
/// the first signal "drained".
/// </remarks>
internal sealed class AsyncOperationTracker
{
    private readonly object _lock = new();
    private int _count;
    private TaskCompletionSource? _drained;

    public void Enter() => Interlocked.Increment(ref _count);

    public void Exit()
    {
        if (Interlocked.Decrement(ref _count) != 0)
            return;

        TaskCompletionSource? drained;
        lock (_lock)
        {
            // An operation that entered after the decrement will complete the waiter
            // when it exits.
            if (Volatile.Read(ref _count) != 0)
                return;

            drained = _drained;
            _drained = null;
        }

        drained?.SetResult();
    }

    /// <summary>
    /// Returns a task that completes the next time no operation is in flight.
    /// </summary>
    public Task WhenDrained()
    {
        lock (_lock)
        {
            if (Volatile.Read(ref _count) == 0)
                return Task.CompletedTask;

            _drained ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            return _drained.Task;
        }
    }
}
