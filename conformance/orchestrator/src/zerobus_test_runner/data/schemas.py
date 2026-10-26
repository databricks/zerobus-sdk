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
    # Arrow IPC-encoded schema (an IPC stream carrying just the schema), shipped to the
    # harness for the arrow_flight transport. Built from schema.arrow.json via pyarrow.
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


# Arrow type names allowed in schema.arrow.json → pyarrow types. Per the README mapping,
# Delta STRING → Arrow LargeUtf8 and int64 → Int64.
_ARROW_TYPES = {
    "int64": "int64",
    "int32": "int32",
    "large_utf8": "large_utf8",
    "utf8": "utf8",
    "string": "large_utf8",
    "bool": "bool_",
    "float64": "float64",
    "float32": "float32",
    "binary": "binary",
    "large_binary": "large_binary",
}


def _arrow_schema_ipc(name: str) -> bytes:
    """Build the Arrow schema from schema.arrow.json and serialize it as an IPC stream
    carrying just the schema (schema message + EOS, no batches)."""
    import json

    import pyarrow as pa

    spec = json.loads((schema_dir(name) / "schema.arrow.json").read_text())
    fields = []
    for f in spec["fields"]:
        factory = _ARROW_TYPES.get(f["type"])
        if factory is None:
            raise InfraError(f"schema '{name}': unknown arrow type '{f['type']}'")
        fields.append(pa.field(f["name"], getattr(pa, factory)()))
    schema = pa.schema(fields)
    sink = pa.BufferOutputStream()
    pa.ipc.new_stream(sink, schema).close()
    return sink.getvalue().to_pybytes()


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
    arrow_ipc = _arrow_schema_ipc(name) if "arrow" in formats else b""
    return SchemaArtifacts(
        name=name,
        formats=formats,
        descriptor_proto_bytes=descriptor_proto_bytes,
        proto_full_name=proto_full_name,
        file_descriptor_set_bytes=fds_bytes,
        avro_schema_json=avro,
        arrow_schema_ipc=arrow_ipc,
    )
