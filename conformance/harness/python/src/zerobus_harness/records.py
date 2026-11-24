"""Logical record -> SDK-native record conversion: the only place this harness
understands record bodies. Each format builds the SDK's input (a protobuf message,
an Avro dict, or a pyarrow.RecordBatch); the SDK validates and encodes on ingest.

Conversion only. A value the SDK can judge (schema semantics, Avro record validity)
passes through unchecked so the SDK's own error is what the caller sees. A value the
harness cannot represent in the SDK's input raises ConversionError.

The same rules hold for every format:
- An object key that is not a field of the record is rejected (protobuf, Avro inline
  records, Arrow columns). Named Avro references resolve to their definition, so the
  rule applies to them too. A union with more than one non-null branch is the one
  exception: its value passes through unchecked.
- JSON null passes to the SDK as the format's own null (Avro None, Arrow null cell).
  Protobuf follows protobuf-JSON, where null means the field's default.
- An int64 (or Avro int, 32-bit) field takes a JSON integer, an integral float, or a
  decimal string. Bools and fractional numbers are rejected. An out-of-range value is
  rejected.
"""

import base64
import json

from google.protobuf import descriptor_pb2, descriptor_pool, json_format, message_factory


class ConversionError(ValueError):
    """The harness cannot represent a record or schema in the SDK's input. The RPC
    aborts with INVALID_ARGUMENT; the SDK never sees the value."""


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

    Raises ConversionError if the bytes are not a valid DescriptorProto or name no message.
    """
    proto = descriptor_pb2.DescriptorProto()
    try:
        proto.ParseFromString(data)
    except Exception as e:  # protobuf raises DecodeError; keep the message readable
        raise ConversionError(f"descriptor_proto_bytes is not a DescriptorProto: {e}") from e
    if not proto.name:
        raise ConversionError("descriptor_proto_bytes has no message name")
    try:
        return ResolvedDescriptor(proto)
    except Exception as e:
        raise ConversionError(f"descriptor_proto_bytes does not describe a valid message: {e}") from e


def _parse_object(record_json: str) -> dict:
    """Parse a record that must be a JSON object."""
    try:
        value = json.loads(record_json)
    except json.JSONDecodeError as e:
        raise ConversionError(f"record_json is not valid JSON: {e}") from e
    if not isinstance(value, dict):
        raise ConversionError("record_json must be a JSON object")
    return value


def record_from_json(message_class, record_json: str):
    """Parse a canonical protobuf-JSON object into a message of ``message_class``.

    Unknown fields are rejected (``ignore_unknown_fields=False``), at every nesting level.
    """
    value = _parse_object(record_json)
    try:
        return json_format.ParseDict(value, message_class(), ignore_unknown_fields=False)
    except json_format.ParseError as e:
        raise ConversionError(f"record_json does not match the schema: {e}") from e


def _to_int(where: str, value, bits: int) -> int:
    """Convert one JSON integer field to a Python int, or reject it.

    Accepts a JSON integer, an integral float, or a decimal string. Rejects bools,
    fractional numbers, non-numeric strings, and values outside the signed ``bits`` range.
    """
    if isinstance(value, bool):
        raise ConversionError(f"{where}: expected an integer, got {value!r}")
    if isinstance(value, int):
        number = value
    elif isinstance(value, float) and value.is_integer():
        number = int(value)
    elif isinstance(value, str):
        try:
            number = int(value, 10)
        except ValueError as e:
            raise ConversionError(f"{where}: expected an integer string, got {value!r}") from e
    else:
        raise ConversionError(f"{where}: expected an integer, got {value!r}")
    if not -(2 ** (bits - 1)) <= number < 2 ** (bits - 1):
        raise ConversionError(f"{where}: {number} is outside the int{bits} range")
    return number


def _collect_named(schema, registry: dict) -> None:
    """Index every named record, enum, and fixed type in an Avro schema by its name."""
    if isinstance(schema, list):
        for branch in schema:
            _collect_named(branch, registry)
    elif isinstance(schema, dict):
        kind = schema.get("type")
        if kind in ("record", "enum", "fixed") and "name" in schema:
            registry[schema["name"]] = schema
        if kind == "record":
            for field in schema.get("fields", []):
                _collect_named(field["type"], registry)
        elif kind == "array":
            _collect_named(schema["items"], registry)
        elif kind == "map":
            _collect_named(schema["values"], registry)
        elif isinstance(kind, (dict, list)):
            _collect_named(kind, registry)


def arrow_schema_from_ipc(data: bytes):
    """Decode the Arrow IPC schema message (``CreateStreamRequest.arrow_schema_ipc``).

    Returns a ``pyarrow.Schema``. Raises ConversionError if the bytes are not an IPC stream.
    """
    import pyarrow as pa

    try:
        return pa.ipc.open_stream(data).schema
    except Exception as e:
        raise ConversionError(f"arrow_schema_ipc is not a valid Arrow IPC stream: {e}") from e


def record_batch_from_json(schema, record_jsons):
    """Build one ``pyarrow.RecordBatch`` with a row per ``record_json``, shaped by ``schema``.

    Unknown JSON keys are rejected (a column cannot hold them); a missing key becomes null.
    Only the column types the scenarios use are handled (int64, utf8, large_utf8); others
    fail loudly.
    """
    import pyarrow as pa

    for field in schema:
        if field.type not in (pa.int64(), pa.utf8(), pa.large_utf8()):
            raise ConversionError(f"field '{field.name}': arrow type {field.type} is not supported by the harness yet")

    rows = [_arrow_row(schema, record_json) for record_json in record_jsons]
    try:
        columns = [pa.array([row[i] for row in rows], type=field.type) for i, field in enumerate(schema)]
    except OverflowError as e:
        raise ConversionError(f"record_json integer out of range: {e}") from e
    return pa.RecordBatch.from_arrays(columns, schema=schema)


def _arrow_row(schema, record_json: str) -> list:
    """Parse one ``record_json`` into schema-ordered cell values, type-checked per column."""
    import pyarrow as pa

    value = _parse_object(record_json)
    unknown = sorted(set(value) - set(schema.names))
    if unknown:
        raise ConversionError(f"record_json has unknown field '{unknown[0]}'")

    cells = []
    for field in schema:
        cell = value.get(field.name)
        if field.type == pa.int64():
            if cell is not None:
                cell = _to_int(f"field '{field.name}'", cell, 64)
        elif cell is not None and not isinstance(cell, str):
            raise ConversionError(f"field '{field.name}': expected string, got {cell!r}")
        cells.append(cell)
    return cells


class AvroRecordBuilder:
    """Turns a logical JSON record into the dict an Avro stream takes.

    Built once per stream from the writer schema JSON (``CreateStreamRequest.avro_schema_json``).
    Only the JSON-to-Python conventions are applied here; schema validity and record
    validity are the SDK's to check and report.
    """

    def __init__(self, schema_json: str):
        try:
            self._schema = json.loads(schema_json)
        except json.JSONDecodeError as e:
            raise ConversionError(f"avro_schema_json is not valid JSON: {e}") from e
        self._named = {}
        _collect_named(self._schema, self._named)

    def __call__(self, record_json: str) -> dict:
        return self._coerce(self._schema, _parse_object(record_json), "record_json")

    def _coerce(self, schema, value, where: str):
        """Map one canonical-JSON value onto the Python value the Avro writer expects."""
        if isinstance(schema, list):  # union: only a single non-null branch is unambiguous
            branches = [b for b in schema if b not in ("null", {"type": "null"})]
            if value is None or len(branches) != 1:
                return value
            return self._coerce(branches[0], value, where)
        if isinstance(schema, str):
            kind = schema
        elif isinstance(schema, dict):
            kind = schema.get("type")
        else:
            return value

        if isinstance(kind, str) and kind in self._named:  # named reference
            return self._coerce(self._named[kind], value, where)
        if isinstance(kind, (dict, list)):
            return self._coerce(kind, value, where)

        if kind == "record" and isinstance(value, dict):
            fields = {f["name"]: f["type"] for f in schema.get("fields", [])}
            unknown = sorted(set(value) - set(fields))
            if unknown:
                raise ConversionError(f"{where} has unknown field '{unknown[0]}'")
            return {k: self._coerce(fields[k], v, f"{where}.{k}") for k, v in value.items()}
        if kind == "array" and isinstance(value, list):
            return [self._coerce(schema["items"], v, f"{where}[{i}]") for i, v in enumerate(value)]
        if kind == "map" and isinstance(value, dict):
            return {k: self._coerce(schema["values"], v, f"{where}.{k}") for k, v in value.items()}
        if kind in ("bytes", "fixed") and isinstance(value, str):
            try:
                return base64.b64decode(value, validate=True)
            except ValueError as e:
                raise ConversionError(f"{where}: bytes field is not valid base64: {e}") from e
        if kind in ("long", "int") and value is not None:
            return _to_int(where, value, 64 if kind == "long" else 32)
        return value
