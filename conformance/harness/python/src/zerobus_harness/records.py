"""Logical record -> SDK-native record conversion. The only place this harness
understands record bodies.

Pipeline: ``CreateStreamRequest.descriptor_proto_bytes`` -> :func:`descriptor_from_bytes`
-> :class:`ResolvedDescriptor` (a file descriptor for the SDK + a message class);
``IngestRecordRequest.record_json`` + message class -> :func:`record_from_json` -> a
protobuf message. The SDK serializes that message itself (it calls ``SerializeToString``).
"""

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
