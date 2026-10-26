"""Unhappy path: a transient mid-stream abort must recover transparently -- the SDK
reconnects, resends the unacked tail, and makes every record durable."""

from zerobus_test_runner import scenario


@scenario()  # no gate -> runs for every wired (sdk, grpc, proto)
def recovery_after_crash(s):
    # Arm a one-shot transient abort: ack through offset 2, then drop the stream.
    s.mock.inject_stream_error(after_offset=2, retryable=True)

    st = s.sdk.create_stream(schema="simple_record")
    recs = [s.sdk.ingest(st, {"id": i, "message": f"m{i}"}) for i in range(5)]  # queue only

    # The SDK's durability guarantee: waiting on the last offset blocks until recovery has
    # resent and durably acked the tail. It succeeds only if nothing was lost.
    s.sdk.await_write(recs[-1], timeout_ms=10000)
    s.sdk.close(st)

    # Server-side evidence. Ephemeral streams use a per-stream offset space, so on recovery
    # the tail is resent under a fresh stream's offsets -- an "ack == last offset" check is
    # wrong across a recovery, hence these counters instead:
    s.expect(s.mock.call_count(), at_least=2)               # the crash forced a reconnect
    s.expect(s.mock.captured_record_count(st), at_least=5)  # server received all records incl. resent tail
    s.expect(s.mock.on_completed_count(st), equals=1)       # the recovered stream closed gracefully
    s.expect(s.mock.cancel_count(st), equals=0)
