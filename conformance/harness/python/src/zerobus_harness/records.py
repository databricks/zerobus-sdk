"""Logical record -> SDK-native record conversion. The only place this harness
understands record bodies.

Proto pipeline: ``CreateStreamRequest.descriptor_proto_bytes`` -> :func:`descriptor_from_bytes`
-> :class:`ResolvedDescriptor` (a file descriptor for the SDK + a message class);
``IngestRecordRequest.record_json`` + message class -> :func:`record_from_json` -> a
protobuf message. The SDK serializes that message itself (it calls ``SerializeToString``).

Avro pipeline: ``CreateStreamRequest.avro_schema_json`` -> :class:`AvroRecordBuilder`;
``IngestRecordRequest.record_json`` -> :meth:`AvroRecordBuilder.__call__` -> a plain dict.
The harness never encodes Avro. The SDK's avro stream encodes the dict itself.
"""

import base64
import json

from google.protobuf import descriptor_pb2, descriptor_pool, json_format, message_factory


class ResolvedDescriptor:
    """A message type resolved from a serialized ``DescriptorProto``."""

    def __init__(self, descriptor_proto: descriptor_pb2.DescriptorProto):
        # The SDK's TableProperties takes a serialized FileDescriptorProto, so wrap the
        # single message type in a file. A fresh pool per stream avoids name collisions.
        file_proto = descriptor_pb2.FileDescriptorProto(
            name=f"{descriptor_proto.name}.proto",
            message_type=[descriptor_proto],
        )
        pool = descriptor_pool.DescriptorPool()
        pool.Add(file_proto)
        message_descriptor = pool.FindMessageTypeByName(descriptor_proto.name)
        self.file_descriptor_bytes = file_proto.SerializeToString()
        self.message_class = message_factory.GetMessageClass(message_descriptor)


def descriptor_from_bytes(data: bytes) -> ResolvedDescriptor:
    """Decode a serialized ``google.protobuf.DescriptorProto`` and resolve it.

    Raises ValueError if the bytes are not a valid DescriptorProto or name no message.
    """
    proto = descriptor_pb2.DescriptorProto()
    try:
        proto.ParseFromString(data)
    except Exception as e:  # protobuf raises DecodeError; keep the message readable
        raise ValueError(f"descriptor_proto_bytes is not a DescriptorProto: {e}") from e
    if not proto.name:
        raise ValueError("descriptor_proto_bytes has no message name")
    try:
        return ResolvedDescriptor(proto)
    except Exception as e:
        raise ValueError(f"descriptor_proto_bytes does not describe a valid message: {e}") from e


def record_from_json(message_class, record_json: str):
    """Parse a canonical protobuf-JSON object into a message of ``message_class``.

    Unknown fields are rejected (``ignore_unknown_fields=False``), so a fixture typo
    fails loudly instead of being silently dropped.
    """
    try:
        value = json.loads(record_json)
    except json.JSONDecodeError as e:
        raise ValueError(f"record_json is not valid JSON: {e}") from e
    if not isinstance(value, dict):
        raise ValueError("record_json must be a JSON object")
    try:
        return json_format.ParseDict(value, message_class(), ignore_unknown_fields=False)
    except json_format.ParseError as e:
        raise ValueError(f"record_json does not match the schema: {e}") from e


def _coerce(schema, value):
    """Map one canonical-JSON value onto the Python value the Avro writer expects.

    Handles the JSON-only conventions: bytes/fixed as base64 strings, int64 as a
    string beyond 2^53, and inline record/array/map/union nesting. Named references
    to types defined elsewhere in the schema pass through unchanged.
    """
    if isinstance(schema, list):  # union: only a single non-null branch is unambiguous
        branches = [b for b in schema if b not in ("null", {"type": "null"})]
        if value is None or len(branches) != 1:
            return value
        return _coerce(branches[0], value)
    if isinstance(schema, str):
        kind = schema
    elif isinstance(schema, dict):
        kind = schema.get("type")
    else:
        return value

    if kind == "record" and isinstance(value, dict):
        known = {f["name"]: f["type"] for f in schema["fields"]}
        unknown = sorted(set(value) - set(known))
        if unknown:
            raise ValueError(f"record_json has fields not in the Avro schema: {unknown}")
        return {k: _coerce(known[k], v) for k, v in value.items()}
    if kind == "array" and isinstance(value, list):
        return [_coerce(schema["items"], v) for v in value]
    if kind == "map" and isinstance(value, dict):
        return {k: _coerce(schema["values"], v) for k, v in value.items()}
    if kind in ("bytes", "fixed") and isinstance(value, str):
        try:
            return base64.b64decode(value, validate=True)
        except ValueError as e:
            raise ValueError(f"record_json bytes field is not valid base64: {e}") from e
    if kind in ("long", "int") and isinstance(value, str):
        try:
            return int(value)
        except ValueError as e:
            raise ValueError(f"record_json integer field is not a number: {e}") from e
    return value


class AvroRecordBuilder:
    """Turns a logical JSON record into the dict an Avro stream takes.

    Built once per stream from the writer schema JSON (``CreateStreamRequest.avro_schema_json``).
    The schema is validated up front; each record is checked against it before it is
    returned, so a bad fixture fails with a clear error rather than inside the SDK.
    """

    def __init__(self, schema_json: str):
        # Imported lazily so the proto-only path never needs fastavro.
        import fastavro
        from fastavro.validation import validate

        try:
            self._schema = json.loads(schema_json)
            self._parsed = fastavro.parse_schema(self._schema)
        except Exception as e:
            raise ValueError(f"avro_schema_json is not a valid Avro schema: {e}") from e
        if not isinstance(self._schema, dict) or self._schema.get("type") != "record":
            raise ValueError("avro_schema_json must describe a top-level record")
        self._validate = validate

    def __call__(self, record_json: str) -> dict:
        try:
            value = json.loads(record_json)
        except json.JSONDecodeError as e:
            raise ValueError(f"record_json is not valid JSON: {e}") from e
        if not isinstance(value, dict):
            raise ValueError("record_json must be a JSON object")
        record = _coerce(self._schema, value)
        try:
            self._validate(record, self._parsed, raise_errors=True)
        except Exception as e:
            raise ValueError(f"record_json does not match the Avro schema: {e}") from e
        return record
