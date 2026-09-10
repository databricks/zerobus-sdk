"""
Avro record format support for the Zerobus SDK.

Requires fastavro to be installed:
    pip install databricks-zerobus-ingest-sdk[avro]

Records can be passed as dicts (encoded to bytes via fastavro) or pre-encoded bytes.

Fidelity rules:
- Ambiguous unions ([int,long], [float,double], same-shape records) require
  explicit tuple: (branch_name, value).
- Decimal scale must match schema definition.
- Naive datetime.datetime objects are treated as UTC.
- UUID fields on fixed(16) receive 16 raw bytes.
- Fixed-length fields are enforced.

Example (Sync):
    >>> import json
    >>> from zerobus.sdk.sync import ZerobusSdk, TableProperties
    >>>
    >>> schema = {
    ...     "type": "record",
    ...     "name": "Event",
    ...     "fields": [
    ...         {"name": "event_id", "type": "string"},
    ...         {"name": "timestamp", "type": "long"}
    ...     ]
    ... }
    >>> sdk = ZerobusSdk(host, unity_catalog_url)
    >>> props = TableProperties("catalog.schema.table", avro_schema=json.dumps(schema))
    >>> stream = sdk.create_stream(client_id, client_secret, props)
    >>> stream.ingest_record_offset({"event_id": "evt1", "timestamp": 1234567890})
    >>> stream.flush()
    >>> stream.close()
"""

import json
from io import BytesIO
from typing import Any, Dict

_FASTAVRO_IMPORT_ERROR = (
    "fastavro is required for Avro support. " "Install with: pip install databricks-zerobus-ingest-sdk[avro]"
)


def _require_fastavro():
    """Lazy import fastavro; raise ImportError if unavailable."""
    try:
        import fastavro

        return fastavro
    except ImportError:
        raise ImportError(_FASTAVRO_IMPORT_ERROR)


def _parse_schema(schema_json: str) -> Dict[str, Any]:
    """Parse a schema JSON string and return a fastavro parsed schema."""
    fastavro = _require_fastavro()
    schema_dict = json.loads(schema_json)
    return fastavro.parse_schema(schema_dict)


def _encode(parsed_schema: Dict[str, Any], record: Dict[str, Any]) -> bytes:
    """Encode a single record to Avro bytes using a parsed schema."""
    fastavro = _require_fastavro()
    output = BytesIO()
    fastavro.schemaless_writer(output, parsed_schema, record)
    return output.getvalue()


def _encode_batch(parsed_schema: Dict[str, Any], records: list) -> list:
    """Encode a batch of records to Avro bytes."""
    return [_encode(parsed_schema, record) for record in records]


def encode_payload(parsed_schema: Dict[str, Any], payload: Any) -> bytes:
    """Coerce a single ingest payload to Avro bytes.

    Pre-encoded ``bytes``/``bytearray`` pass through unchanged; anything else is
    treated as a record and encoded against ``parsed_schema`` via fastavro.
    """
    if isinstance(payload, (bytes, bytearray)):
        return bytes(payload)
    return _encode(parsed_schema, payload)


def encode_payloads(parsed_schema: Dict[str, Any], payloads: Any) -> list:
    """Coerce a batch of ingest payloads, applying :func:`encode_payload` to each."""
    return [encode_payload(parsed_schema, p) for p in payloads]


def parse_table_schema(table_properties: Any):
    """Return the parsed fastavro schema for a stream, or ``None`` if not Avro.

    Reads ``table_properties.avro_schema`` if present; the attribute is absent on
    builds without the ``avro`` feature, in which case this returns ``None``.
    """
    schema_json = getattr(table_properties, "avro_schema", None)
    if schema_json is None:
        return None
    return _parse_schema(schema_json)
