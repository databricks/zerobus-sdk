using System.Diagnostics;
using Databricks.Zerobus;
using NUnit.Framework;

namespace Databricks.Zerobus.Tests;

[TestFixture]
public class AsyncOperationTrackerTests
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    [Test]
    public void WhenDrained_NoOperations_IsCompleted()
    {
        var tracker = new AsyncOperationTracker();

        Assert.That(tracker.WhenDrained().IsCompleted, Is.True);
    }

    [Test]
    public void WhenDrained_CompletesWhenLastOperationExits()
    {
        var tracker = new AsyncOperationTracker();
        tracker.Enter();
        tracker.Enter();

        var drained = tracker.WhenDrained();
        tracker.Exit();

        Assert.That(drained.IsCompleted, Is.False);

        tracker.Exit();

        Assert.That(drained.Wait(WaitTimeout), Is.True);
    }

    [Test]
    public void WhenDrained_OperationEnteringAfterLastExit_IsWaitedFor()
    {
        // Sequential form of the race the tracker prevents: the last operation exits
        // and the next one registers straight after it.
        var tracker = new AsyncOperationTracker();
        tracker.Enter();
        var previous = tracker.WhenDrained();
        tracker.Exit();
        tracker.Enter();

        var drained = tracker.WhenDrained();

        Assert.That(previous.Wait(WaitTimeout), Is.True);
        Assert.That(drained.IsCompleted, Is.False);

        tracker.Exit();

        Assert.That(drained.Wait(WaitTimeout), Is.True);
    }

    [Test]
    public void WhenDrained_UnderContention_NeverCompletedWhileOperationRegistered()
    {
        // Signalling a separate event without re-checking the count under a lock
        // fails this check within milliseconds.
        var tracker = new AsyncOperationTracker();
        var stopwatch = Stopwatch.StartNew();
        var violations = 0;

        var workers = Enumerable.Range(0, 4)
            .Select(_ => Task.Factory.StartNew(() =>
            {
                while (stopwatch.Elapsed < TimeSpan.FromSeconds(1))
                {
                    tracker.Enter();
                    if (tracker.WhenDrained().IsCompleted)
                        Interlocked.Increment(ref violations);
                    tracker.Exit();
                }
            }, TaskCreationOptions.LongRunning))
            .ToArray();

        Task.WaitAll(workers);

        Assert.That(violations, Is.Zero);
        Assert.That(tracker.WhenDrained().IsCompleted, Is.True);
    }
}
