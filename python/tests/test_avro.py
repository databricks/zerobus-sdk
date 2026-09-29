"""
Tests for Avro record format support.

Skipped if fastavro is not installed (it powers the dict-encoding path).
"""

import json
import time

import pytest

# fastavro powers the object-encoding path; skip these tests when it is absent.
AVRO_AVAILABLE = False
try:
    import fastavro

    from zerobus.sdk.shared.avro import AvroEncoder, _encode, _encode_batch, _parse_schema

    AVRO_AVAILABLE = True
except ImportError:
    pass

pytestmark = pytest.mark.skipif(not AVRO_AVAILABLE, reason="fastavro not installed")


class TestAvroEncoding:
    """Test Avro encoding functions."""

    def test_parse_schema(self):
        """Parse schema JSON."""
        schema_json = json.dumps({"type": "record", "name": "Test", "fields": [{"name": "id", "type": "int"}]})
        parsed = _parse_schema(schema_json)
        assert parsed is not None
        assert parsed["name"] == "Test"

    def test_encode_simple_record(self):
        """Encode a simple record."""
        schema_json = json.dumps(
            {
                "type": "record",
                "name": "Simple",
                "fields": [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}],
            }
        )
        parsed = _parse_schema(schema_json)
        record = {"id": 1, "name": "test"}
        encoded = _encode(parsed, record)
        assert isinstance(encoded, bytes)
        assert len(encoded) > 0

    def test_encode_batch(self):
        """Encode multiple records."""
        schema_json = json.dumps({"type": "record", "name": "Batch", "fields": [{"name": "value", "type": "int"}]})
        parsed = _parse_schema(schema_json)
        records = [{"value": i} for i in range(5)]
        encoded_list = _encode_batch(parsed, records)
        assert len(encoded_list) == 5
        for enc in encoded_list:
            assert isinstance(enc, bytes)

    @pytest.mark.skipif(not hasattr(time, "tzset"), reason="TZ manipulation requires tzset (Unix)")
    def test_naive_datetime_treated_as_utc(self, monkeypatch):
        """A naive datetime encodes as UTC regardless of the process timezone."""
        from datetime import datetime, timezone
        from io import BytesIO

        schema_json = json.dumps(
            {
                "type": "record",
                "name": "Ts",
                "fields": [{"name": "t", "type": {"type": "long", "logicalType": "timestamp-millis"}}],
            }
        )
        parsed = _parse_schema(schema_json)
        monkeypatch.setenv("TZ", "America/Los_Angeles")
        time.tzset()
        try:
            encoded = _encode(parsed, {"t": datetime(1970, 1, 1)})
            decoded = fastavro.schemaless_reader(BytesIO(encoded), parsed)
        finally:
            monkeypatch.delenv("TZ", raising=False)
            time.tzset()
        assert decoded["t"] == datetime(1970, 1, 1, tzinfo=timezone.utc)

    def test_encode_logical_types(self):
        """Encode records with logical types."""
        schema_json = json.dumps(
            {
                "type": "record",
                "name": "LogicalTypes",
                "fields": [
                    {"name": "date_field", "type": {"type": "int", "logicalType": "date"}},
                    {"name": "ts_millis", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                ],
            }
        )
        parsed = _parse_schema(schema_json)
        from datetime import date, datetime

        record = {"date_field": date(2025, 1, 1), "ts_millis": datetime(2025, 1, 1, 0, 0, 0)}
        encoded = _encode(parsed, record)
        assert isinstance(encoded, bytes)


class TestAvroEncodingFlow:
    """Exercise the AvroEncoder the stream wrappers use."""

    def test_encode_dict(self):
        """A dict is encoded to Avro bytes against the writer schema."""
        schema_json = json.dumps(
            {
                "type": "record",
                "name": "Event",
                "fields": [{"name": "event_id", "type": "string"}, {"name": "count", "type": "int"}],
            }
        )
        encoded = AvroEncoder(schema_json).encode({"event_id": "evt-1", "count": 42})
        assert isinstance(encoded, bytes) and len(encoded) > 0

    def test_encode_batch_dict_and_bytes(self):
        """A batch encodes dicts and passes pre-encoded bytes through unchanged."""
        schema_json = json.dumps({"type": "record", "name": "V", "fields": [{"name": "value", "type": "int"}]})
        enc = AvroEncoder(schema_json)
        pre_encoded = enc.encode({"value": 1})
        out = enc.encode_batch([{"value": 2}, pre_encoded])
        assert out[1] == pre_encoded
        assert isinstance(out[0], bytes) and out[0] != pre_encoded


class TestAvroSchemaVariations:
    """Test various Avro schema patterns."""

    def test_nested_record(self):
        """Encode nested record."""
        schema_json = json.dumps(
            {
                "type": "record",
                "name": "Parent",
                "fields": [
                    {"name": "id", "type": "int"},
                    {
                        "name": "child",
                        "type": {"type": "record", "name": "Child", "fields": [{"name": "name", "type": "string"}]},
                    },
                ],
            }
        )
        parsed = _parse_schema(schema_json)
        record = {"id": 1, "child": {"name": "child_name"}}
        encoded = _encode(parsed, record)
        assert isinstance(encoded, bytes)

    def test_optional_field(self):
        """Encode record with optional field."""
        schema_json = json.dumps(
            {
                "type": "record",
                "name": "Optional",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "optional_field", "type": ["null", "string"], "default": None},
                ],
            }
        )
        parsed = _parse_schema(schema_json)
        record = {"id": 1, "optional_field": None}
        encoded = _encode(parsed, record)
        assert isinstance(encoded, bytes)

    def test_array_field(self):
        """Encode record with array."""
        schema_json = json.dumps(
            {
                "type": "record",
                "name": "WithArray",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "tags", "type": {"type": "array", "items": "string"}},
                ],
            }
        )
        parsed = _parse_schema(schema_json)
        record = {"id": 1, "tags": ["tag1", "tag2"]}
        encoded = _encode(parsed, record)
        assert isinstance(encoded, bytes)

    def test_fixed_field(self):
        """Encode record with fixed-length bytes."""
        schema_json = json.dumps(
            {
                "type": "record",
                "name": "WithFixed",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "uuid", "type": {"type": "fixed", "size": 16, "name": "UUID"}},
                ],
            }
        )
        parsed = _parse_schema(schema_json)
        record = {"id": 1, "uuid": b"0123456789abcdef"}
        encoded = _encode(parsed, record)
        assert isinstance(encoded, bytes)
