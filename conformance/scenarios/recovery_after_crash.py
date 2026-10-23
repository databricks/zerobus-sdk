"""Unhappy path: a transient mid-stream crash must be recovered transparently.

The mock acks records 0..2, then aborts the stream once with a retryable error. An SDK
with recovery enabled (the default for the Rust-core SDKs) should reconnect, resend the
unacked tail, and make every record durable.

Asserting durability here is subtle: EPHEMERAL streams use a PER-STREAM offset space, so on
recovery the SDK opens a new stream and resends the tail as that stream's offsets 0,1 (not
the original 3,4). A global "ack watermark == last logical offset" check is therefore wrong
across a recovery. Instead we assert durability the way the SDK guarantees it (waiting on the
last offset succeeds) plus server-side evidence that the fault was real and the tail was
resent (reconnect happened; the server received at least all records).
"""

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

    # Server-side evidence (robust across the per-stream offset reset):
    s.expect(s.mock.call_count(), at_least=2)               # the crash forced a reconnect
    s.expect(s.mock.captured_record_count(st), at_least=5)  # server received all records incl. resent tail
    s.expect(s.mock.on_completed_count(st), equals=1)       # the recovered stream closed gracefully
    s.expect(s.mock.cancel_count(st), equals=0)
