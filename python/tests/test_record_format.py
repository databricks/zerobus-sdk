"""Tests for RecordFormat selection on TableProperties (the ingest dispatch source of truth).

These run without fastavro — they cover format selection, the record_type guard, the
bytes-passthrough path, and naive-datetime coercion, none of which encode a dict.
"""

import json
from collections import UserDict
from datetime import datetime, timezone

import pytest

from tests.test_row_pb2 import AirQuality
from zerobus.sdk.shared.avro import AvroEncoder, _utc_naive_datetimes
from zerobus.sdk.sync import RecordType, StreamConfigurationOptions, TableProperties, ZerobusSdk


def test_json_format_default():
    assert TableProperties("catalog.schema.table").record_format == "json"


def test_proto_format_from_descriptor():
    props = TableProperties("catalog.schema.table", AirQuality.DESCRIPTOR)
    assert props.record_format == "proto"


def test_avro_format_from_schema():
    schema = json.dumps({"type": "record", "name": "R", "fields": [{"name": "id", "type": "int"}]})
    props = TableProperties("catalog.schema.table", avro_schema=schema)
    assert props.avro_schema == schema
    assert props.record_format == "avro"


def test_descriptor_and_avro_schema_mutually_exclusive():
    schema = json.dumps({"type": "record", "name": "R", "fields": []})
    with pytest.raises(ValueError):
        TableProperties("catalog.schema.table", AirQuality.DESCRIPTOR, avro_schema=schema)


def test_bytes_passthrough_does_not_parse_schema():
    """Pre-encoded bytes-like values pass through without parsing the schema (no fastavro)."""
    enc = AvroEncoder("not-json")
    for payload in (b"\x00", bytearray(b"\x00"), memoryview(b"\x00")):
        assert enc.encode(payload) == b"\x00"
    assert enc._parsed is None


def test_encode_batch_rejects_single_record():
    """A single record (str / bytes-like / mapping) is rejected, matching the Rust guard."""
    enc = AvroEncoder("not-json")
    for bad in ("x", b"x", bytearray(b"x"), memoryview(b"x"), {"id": 1}, UserDict({"id": 1})):
        with pytest.raises(TypeError, match="list of records"):
            enc.encode_batch(bad)


def test_naive_datetime_coerced_in_mapping_subclass():
    """The walker normalizes Mapping subclasses (e.g. UserDict), not just plain dict."""
    out = _utc_naive_datetimes(UserDict({"t": datetime(1970, 1, 1)}))
    assert out["t"] == datetime(1970, 1, 1, tzinfo=timezone.utc)


def test_record_type_proto_rejected_on_json_table():
    """A record_type that disagrees with the schema is rejected before connecting."""
    sdk = ZerobusSdk("https://example.zerobus.cloud.databricks.com", "https://example.cloud.databricks.com")
    opts = StreamConfigurationOptions(record_type=RecordType.PROTO, recovery=False)
    with pytest.raises(ValueError, match="conflicts with the schema"):
        sdk.create_stream("id", "secret", TableProperties("catalog.schema.table"), opts)


def test_record_type_values():
    assert int(RecordType.UNSPECIFIED) == 0
    assert int(RecordType.PROTO) == 1
    assert int(RecordType.JSON) == 2
    assert int(RecordType.AVRO) == 4


def test_record_type_defaults_to_unspecified():
    assert StreamConfigurationOptions().record_type == RecordType.UNSPECIFIED
