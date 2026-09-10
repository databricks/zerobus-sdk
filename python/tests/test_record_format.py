"""Tests for RecordFormat selection on TableProperties (the ingest dispatch source of truth)."""

from tests.test_row_pb2 import AirQuality
from zerobus.sdk.sync import RecordType, StreamConfigurationOptions, TableProperties


def test_json_format_default():
    assert TableProperties("catalog.schema.table").record_format == "json"


def test_proto_format_from_descriptor():
    props = TableProperties("catalog.schema.table", AirQuality.DESCRIPTOR)
    assert props.record_format == "proto"


def test_record_type_values():
    assert int(RecordType.UNSPECIFIED) == 0
    assert int(RecordType.PROTO) == 1
    assert int(RecordType.JSON) == 2
    assert int(RecordType.AVRO) == 4


def test_record_type_defaults_to_unspecified():
    assert StreamConfigurationOptions().record_type == RecordType.UNSPECIFIED
