from zerobus_test_runner import scenario, formats, golden


@scenario(applies=formats("avro"))
def encoded_bytes_avro(s):
    st = s.sdk.create_stream(schema="simple_record")
    r = s.sdk.ingest(st, {"id": 1, "message": "hello"})
    s.sdk.flush(st)
    s.sdk.close(st)
    s.expect(s.mock.captured_record_bytes(st, r.offset),
             equals=golden("simple_record", {"id": 1, "message": "hello"}, "avro"))
