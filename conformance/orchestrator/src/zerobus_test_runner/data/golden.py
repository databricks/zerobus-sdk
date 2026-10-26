"""Frozen, committed golden wire bytes.

Golden bytes live in ``conformance/schemas/<schema>/golden/<fmt>/<key>.bin`` and are committed.
They are generated ONCE by ``scripts/regen_goldens.py`` (independent protobuf / fastavro encoders,
never the SDK under test), reviewed by a human, and then only *read* here. Assertions compare
captured SDK bytes against these files; nothing is encoded at assert time.

Keying: ``<key>`` is the SHA-1 of the canonical JSON of the logical record dict
(``sort_keys``, compact separators, UTF-8). The format is the directory. The human-readable
mapping (record, format, file, hex) is ``golden/MANIFEST.json``, rewritten by the regen script.

``encode_golden`` (the independent encoder) is imported only by the regen script and tests.
"""

from __future__ import annotations

import hashlib
import io
import json
from functools import lru_cache
from pathlib import Path

import fastavro
from google.protobuf import descriptor_pool, json_format, message_factory

from zerobus_test_runner.data.schemas import SchemaArtifacts, load_schema, schema_dir
from zerobus_test_runner.errors import InfraError

GOLDEN_FORMATS = frozenset({"proto", "avro"})
GOLDEN_DIRNAME = "golden"
MANIFEST_NAME = "MANIFEST.json"


class GoldenUnsupported(ValueError):
    pass


class GoldenMissing(InfraError):
    """A required committed golden .bin is absent. Fix by running the regeneration script."""


def canonical_record_json(record: dict) -> bytes:
    return json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def golden_key(record: dict) -> str:
    return hashlib.sha1(canonical_record_json(record)).hexdigest()


def golden_relpath(record: dict, fmt: str) -> Path:
    """Path of the golden .bin relative to ``schemas/<schema>/``."""
    return Path(GOLDEN_DIRNAME) / fmt / f"{golden_key(record)}.bin"


def golden_path(schema: str, record: dict, fmt: str) -> Path:
    return schema_dir(schema) / golden_relpath(record, fmt)


def golden_dir(schema: str) -> Path:
    return schema_dir(schema) / GOLDEN_DIRNAME


def check_golden_present(schema: str, record: dict, fmt: str) -> Path:
    """Return the golden path, or raise GoldenMissing if the committed .bin is absent."""
    if fmt not in GOLDEN_FORMATS:
        raise GoldenUnsupported(f"no golden for format '{fmt}'")
    path = golden_path(schema, record, fmt)
    if not path.is_file():
        raise GoldenMissing(
            f"missing committed golden for schema '{schema}' format '{fmt}' record "
            f"{canonical_record_json(record).decode()}: expected {path}. "
            "Run `conformance/orchestrator/.venv/bin/python conformance/orchestrator/scripts/regen_goldens.py` "
            "to generate it, then review and commit the new file."
        )
    return path


def read_golden(schema: str, record: dict, fmt: str) -> bytes:
    """The expected wire bytes, read from the committed .bin. Never encodes."""
    return check_golden_present(schema, record, fmt).read_bytes()


# --------------------------------------------------------------------------- independent encoders
# Used only by scripts/regen_goldens.py. Reference libraries, never the SDK under test.


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


def encode_golden(schema: SchemaArtifacts, record: dict, fmt: str) -> bytes:
    """Encode ``record`` into the expected wire bytes for ``fmt`` with the reference libraries."""
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


def encode_golden_for(schema_name: str, record: dict, fmt: str) -> bytes:
    return encode_golden(load_schema(schema_name), record, fmt)
