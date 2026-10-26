"""Baseline ingest: queue 5 records, flush once, and confirm the server received and
acked all of them and closed cleanly. Runs on every valid + supported combination."""

from zerobus_test_runner import scenario


@scenario()  # no gate: runs on every valid + supported (sdk, transport, format)
def happy_path_ingest(s):
    st = s.sdk.create_stream(schema="simple_record")
    last = None
    for i in range(5):
        last = s.sdk.ingest(st, {"id": i, "message": f"m{i}"})  # queue only, never wait here
    s.sdk.flush(st)  # wait once for all acks
    s.sdk.close(st)
    s.expect(s.mock.ack_offset(st), equals=last.offset)
    s.expect(s.mock.captured_record_count(st), equals=5)
    s.expect(s.mock.on_completed_count(st), equals=1)
    s.expect(s.mock.cancel_count(st), equals=0)
