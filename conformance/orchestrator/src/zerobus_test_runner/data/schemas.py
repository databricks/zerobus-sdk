"""Load schema artifacts from ``conformance/schemas/<name>/``.

Source files are authored (``schema.proto``, ``schema.avsc``, ``schema.arrow.json``).
``build/<name>.desc`` is a generated FileDescriptorSet; it is (re)built here when missing or
older than ``schema.proto`` so the orchestrator does not depend on a separate build step.
"""

from __future__ import annotations

import functools
import importlib.resources
from dataclasses import dataclass
from pathlib import Path

from google.protobuf import descriptor_pb2
from grpc_tools import protoc

from zerobus_test_runner.paths import SCHEMAS_DIR
from zerobus_test_runner.scenario import ScenarioError

# The source file each non-JSON format is implied by. JSON needs no artifact. One table,
# used both to discover a schema's formats and to load them.
FORMAT_ARTIFACTS = {
    "proto": "schema.proto",
    "avro": "schema.avsc",
    "arrow": "schema.arrow.json",
}


@dataclass(frozen=True)
class SchemaArtifacts:
    name: str
    formats: frozenset
    # Serialized google.protobuf.DescriptorProto of the record message (CreateStream payload).
    descriptor_proto_bytes: bytes
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
        raise ScenarioError(f"unknown schema '{name}' (no directory {d})")
    found = {"json"}
    found |= {fmt for fmt, fname in FORMAT_ARTIFACTS.items() if (d / fname).is_file()}
    return frozenset(found)


def _descriptor_proto_bytes(name: str) -> bytes:
    """Build (if stale) the FileDescriptorSet and return the first message's DescriptorProto."""
    d = schema_dir(name)
    src = d / "schema.proto"
    out = d / "build" / f"{name}.desc"
    if not (out.is_file() and out.stat().st_mtime >= src.stat().st_mtime):
        out.parent.mkdir(parents=True, exist_ok=True)
        # grpc_tools' bundled include path carries the well-known types, so a schema that imports
        # google/protobuf/*.proto builds. protoc.main does not add it the way `-m grpc_tools.protoc` does.
        wkt = importlib.resources.files("grpc_tools") / "_proto"
        rc = protoc.main(["protoc", f"-I{d}", f"-I{wkt}", f"--descriptor_set_out={out}", "--include_imports", src.name])
        if rc != 0:
            raise ScenarioError(f"protoc failed building descriptor set for schema '{name}' (rc={rc})")
    fds = descriptor_pb2.FileDescriptorSet.FromString(out.read_bytes())
    own = [f for f in fds.file if f.name == "schema.proto"]
    if len(own) != 1 or not own[0].message_type:
        raise ScenarioError(f"schema '{name}': schema.proto must declare at least one top-level message")
    return own[0].message_type[0].SerializeToString()  # convention: the first message is the record


def _arrow_schema_ipc(name: str) -> bytes:
    """Build the Arrow schema from schema.arrow.json and serialize it as an IPC stream
    carrying just the schema (schema message + EOS, no batches)."""
    import json

    import pyarrow as pa

    spec = json.loads((schema_dir(name) / "schema.arrow.json").read_text())
    fields = []
    for f in spec["fields"]:
        try:
            dt = pa.type_for_alias(f["type"])
        except ValueError as e:
            raise ScenarioError(f"schema '{name}': unknown arrow type '{f['type']}'") from e
        fields.append(pa.field(f["name"], dt))
    sink = pa.BufferOutputStream()
    pa.ipc.new_stream(sink, pa.schema(fields)).close()
    return sink.getvalue().to_pybytes()


@functools.lru_cache(maxsize=None)
def load_schema(name: str) -> SchemaArtifacts:
    formats = supported_formats(name)
    return SchemaArtifacts(
        name=name,
        formats=formats,
        descriptor_proto_bytes=_descriptor_proto_bytes(name) if "proto" in formats else b"",
        avro_schema_json=(schema_dir(name) / "schema.avsc").read_text() if "avro" in formats else "",
        arrow_schema_ipc=_arrow_schema_ipc(name) if "arrow" in formats else b"",
    )
