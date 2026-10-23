"""Independent expected-bytes encoders.

These encode a logical record with the *reference* libraries (protobuf, fastavro), never the
SDK under test. The scenario's ``golden(...)`` resolves to the output of ``expected_bytes``.
"""

from __future__ import annotations

import io
import json
from functools import lru_cache

import fastavro
from google.protobuf import descriptor_pool, json_format, message_factory

from zerobus_test_runner.data.schemas import SchemaArtifacts

GOLDEN_FORMATS = frozenset({"proto", "avro"})


class GoldenUnsupported(ValueError):
    pass


@lru_cache(maxsize=None)
def _proto_message_class(schema_name: str, fds_bytes: bytes, full_name: str):
    from google.protobuf import descriptor_pb2

    fds = descriptor_pb2.FileDescriptorSet.FromString(fds_bytes)
    pool = descriptor_pool.DescriptorPool()
    for f in fds.file:  # --include_imports emits dependencies first
        pool.Add(f)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName(full_name))


@lru_cache(maxsize=None)
def _parsed_avro(schema_name: str, avro_json: str):
    return fastavro.parse_schema(json.loads(avro_json))


def expected_bytes(schema: SchemaArtifacts, record: dict, fmt: str) -> bytes:
    """Encode ``record`` (logical JSON-object dict) into the expected wire bytes for ``fmt``."""
    if fmt == "proto":
        if "proto" not in schema.formats:
            raise GoldenUnsupported(f"schema '{schema.name}' has no proto artifact")
        cls = _proto_message_class(schema.name, schema.file_descriptor_set_bytes, schema.proto_full_name)
        msg = json_format.ParseDict(record, cls())
        return msg.SerializeToString(deterministic=True)
    if fmt == "avro":
        if "avro" not in schema.formats:
            raise GoldenUnsupported(f"schema '{schema.name}' has no avro artifact")
        buf = io.BytesIO()
        # Raw datum bytes (schemaless), the form the SDK puts on the wire for avro records.
        fastavro.schemaless_writer(buf, _parsed_avro(schema.name, schema.avro_schema_json), record)
        return buf.getvalue()
    # TODO(json/arrow): no golden encoder yet; json/arrow byte-exact goldens are not in the first slice.
    raise GoldenUnsupported(f"no golden encoder for format '{fmt}'")
