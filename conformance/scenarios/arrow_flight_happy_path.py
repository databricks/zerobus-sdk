"""Happy-path ingestion over the Arrow Flight transport.

The arrow analog of happy_path_ingest, gated to the `arrow` format (which rides the
arrow_flight transport). Ingest 5 one-row RecordBatches, wait for the last to be durable,
close; then assert the server received all 5 records, acked up to the last offset, and
closed cleanly.
"""

from zerobus_test_runner import scenario, formats


@scenario(applies=formats("arrow"))
def arrow_flight_happy_path(s):
    st = s.sdk.create_stream(schema="simple_record")
    recs = [s.sdk.ingest(st, {"id": i, "message": f"m{i}"}) for i in range(5)]  # queue only
    # Wait on the last offset: blocks until every batch is durably acked.
    s.sdk.await_write(recs[-1], timeout_ms=10000)
    s.sdk.close(st)

    s.expect(s.mock.ack_offset(st), equals=recs[-1].offset)   # all 5 batches durable
    s.expect(s.mock.captured_record_count(st), equals=5)       # 5 rows received
    s.expect(s.mock.on_completed_count(st), equals=1)          # DoPut half-closed gracefully
    s.expect(s.mock.cancel_count(st), equals=0)
