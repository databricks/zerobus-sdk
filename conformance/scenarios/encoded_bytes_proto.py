"""Ingest one record and assert the exact proto bytes captured on the wire match the
committed golden -- the SDK's own encoder is what's under test here."""

from zerobus_test_runner import scenario, formats, golden


@scenario(applies=formats("proto"))
def encoded_bytes_proto(s):
    st = s.sdk.create_stream(schema="simple_record")
    r = s.sdk.ingest(st, {"id": 1, "message": "hello"})
    s.sdk.flush(st)
    s.sdk.close(st)
    s.expect(s.mock.captured_record_bytes(st, r.offset),
             equals=golden("simple_record", {"id": 1, "message": "hello"}, "proto"))
