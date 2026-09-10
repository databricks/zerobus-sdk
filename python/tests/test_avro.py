"""
Tests for Avro record format support.

Skipped if fastavro is not available or the Rust extension was built without avro feature.
"""

import json

import pytest

# Check if avro support is available
AVRO_AVAILABLE = False
try:
    import fastavro

    from zerobus.sdk.shared.avro import _encode, _encode_batch, _parse_schema
    from zerobus.sdk.sync import TableProperties, ZerobusSdk

    # Also check if the extension was built with avro feature
    try:
        test_props = TableProperties("test", avro_schema="schema")
        AVRO_AVAILABLE = True
    except TypeError:
        # avro_schema parameter not recognized - extension built without avro feature
        pass
except ImportError:
    pass

pytestmark = pytest.mark.skipif(not AVRO_AVAILABLE, reason="fastavro not installed or avro feature not compiled")


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


class TestAvroStreamIntegration:
    """Test Avro stream integration (mock/no server)."""

    def test_stream_creation_with_avro_schema(self):
        """Create stream with avro_schema property."""
        schema_json = json.dumps({"type": "record", "name": "Test", "fields": [{"name": "id", "type": "int"}]})
        props = TableProperties("test_table", avro_schema=schema_json)
        assert props.avro_schema == schema_json
        assert props.record_format == "avro"

    def test_stream_encoding_flow_dict(self):
        """Test encoding dicts on stream (integration)."""
        schema_json = json.dumps(
            {
                "type": "record",
                "name": "Event",
                "fields": [{"name": "event_id", "type": "string"}, {"name": "count", "type": "int"}],
            }
        )
        from zerobus.sdk.shared.avro import _parse_schema

        parsed = _parse_schema(schema_json)

        # Simulate encoding on stream wrapper
        record = {"event_id": "evt-1", "count": 42}
        encoded = _encode(parsed, record)
        assert isinstance(encoded, bytes)

    def test_stream_encoding_flow_bytes(self):
        """Test pre-encoded bytes on stream."""
        schema_json = json.dumps({"type": "record", "name": "Bytes", "fields": [{"name": "value", "type": "int"}]})
        from zerobus.sdk.shared.avro import _encode, _parse_schema

        parsed = _parse_schema(schema_json)
        record = {"value": 99}
        pre_encoded = _encode(parsed, record)

        # Pre-encoded bytes should pass through
        assert isinstance(pre_encoded, bytes)


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
