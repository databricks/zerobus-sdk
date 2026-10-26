"""Happy-path NATIVE-BATCH ingestion over Arrow Flight.

Exercises the arrow path for the SDKs whose arrow ingest API takes a native in-memory
RecordBatch that the SDK serializes to IPC internally — rust, python, java. Ingest 5 one-row
batches, wait for the last to be durable, close; then assert the server received all 5
records, acked up to the last offset, and closed cleanly.

Scoped to the native-batch SDKs. The IPC-bytes SDKs (typescript, cpp, go-cgo) take a
pre-serialized Arrow IPC stream instead; they belong to a separate IPC-bytes arrow scenario
(TODO), so this one reports N-A for them.
"""

from zerobus_test_runner import scenario, gate


@scenario(applies=gate(formats=["arrow"], sdks=["rust", "python", "java"]))
def arrow_native_batch_happy_path(s):
    st = s.sdk.create_stream(schema="simple_record")
    recs = [s.sdk.ingest(st, {"id": i, "message": f"m{i}"}) for i in range(5)]  # queue only
    # Wait on the last offset: blocks until every batch is durably acked.
    s.sdk.await_write(recs[-1], timeout_ms=10000)
    s.sdk.close(st)

    s.expect(s.mock.ack_offset(st), equals=recs[-1].offset)   # all 5 batches durable
    s.expect(s.mock.captured_record_count(st), equals=5)       # 5 rows received
    s.expect(s.mock.on_completed_count(st), equals=1)          # DoPut half-closed gracefully
    s.expect(s.mock.cancel_count(st), equals=0)
