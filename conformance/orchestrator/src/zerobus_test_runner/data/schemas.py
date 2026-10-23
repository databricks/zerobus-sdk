"""Load schema artifacts from ``conformance/schemas/<name>/``.

Source files are authored (``schema.proto``, ``schema.avsc``, ``schema.arrow.json``).
``build/<name>.desc`` is a generated FileDescriptorSet; it is (re)built here when missing or
older than ``schema.proto`` so the orchestrator does not depend on a separate build step.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass
from pathlib import Path

from grpc_tools import protoc
from google.protobuf import descriptor_pb2

from zerobus_test_runner.errors import InfraError
from zerobus_test_runner.paths import SCHEMAS_DIR


@dataclass(frozen=True)
class SchemaArtifacts:
    name: str
    formats: frozenset
    # Serialized google.protobuf.DescriptorProto of the record message (CreateStream payload).
    descriptor_proto_bytes: bytes
    # Full proto name of the record message (used by the golden encoder).
    proto_full_name: str
    # Serialized FileDescriptorSet (with imports) for building message classes.
    file_descriptor_set_bytes: bytes
    avro_schema_json: str = ""
    # TODO(arrow): arrow_schema_ipc is not produced yet; no arrow scenario needs it in the first slice.
    arrow_schema_ipc: bytes = b""


def schema_dir(name: str) -> Path:
    return SCHEMAS_DIR / name


def supported_formats(name: str) -> frozenset:
    """Formats implied by which artifacts the schema ships. JSON needs no artifact."""
    d = schema_dir(name)
    if not d.is_dir():
        raise InfraError(f"unknown schema '{name}' (no directory {d})")
    found = {"json"}
    if (d / "schema.proto").is_file():
        found.add("proto")
    if (d / "schema.avsc").is_file():
        found.add("avro")
    if (d / "schema.arrow.json").is_file():
        found.add("arrow")
    return frozenset(found)


def _ensure_descriptor_set(name: str) -> Path:
    d = schema_dir(name)
    src = d / "schema.proto"
    out = d / "build" / f"{name}.desc"
    if out.is_file() and out.stat().st_mtime >= src.stat().st_mtime:
        return out
    out.parent.mkdir(parents=True, exist_ok=True)
    rc = protoc.main([
        "protoc",
        f"-I{d}",
        f"--descriptor_set_out={out}",
        "--include_imports",
        src.name,
    ])
    if rc != 0:
        raise InfraError(f"protoc failed building descriptor set for schema '{name}' (rc={rc})")
    return out


@functools.lru_cache(maxsize=None)
def load_schema(name: str) -> SchemaArtifacts:
    formats = supported_formats(name)
    descriptor_proto_bytes = b""
    proto_full_name = ""
    fds_bytes = b""
    if "proto" in formats:
        fds_bytes = _ensure_descriptor_set(name).read_bytes()
        fds = descriptor_pb2.FileDescriptorSet.FromString(fds_bytes)
        own = [f for f in fds.file if f.name == "schema.proto"]
        if len(own) != 1 or not own[0].message_type:
            raise InfraError(f"schema '{name}': schema.proto must declare at least one top-level message")
        record_msg = own[0].message_type[0]  # convention: the first message is the record
        descriptor_proto_bytes = record_msg.SerializeToString()
        pkg = own[0].package
        proto_full_name = f"{pkg}.{record_msg.name}" if pkg else record_msg.name
    avro = ""
    if "avro" in formats:
        avro = (schema_dir(name) / "schema.avsc").read_text()
    return SchemaArtifacts(
        name=name,
        formats=formats,
        descriptor_proto_bytes=descriptor_proto_bytes,
        proto_full_name=proto_full_name,
        file_descriptor_set_bytes=fds_bytes,
        avro_schema_json=avro,
    )
