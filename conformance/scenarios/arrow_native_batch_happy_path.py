"""Happy-path ingest over Arrow Flight for the native-batch SDKs (rust, python, java), whose
arrow API takes an in-memory RecordBatch the SDK serializes to IPC itself. The IPC-bytes SDKs
(typescript, cpp, go-cgo) take a pre-serialized stream and belong to a separate scenario."""

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
