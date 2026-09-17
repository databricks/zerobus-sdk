"""
Avro record format support for the Zerobus SDK.

Requires fastavro to be installed for the object path:
    pip install databricks-zerobus-ingest-sdk[avro]

Records can be passed as dicts (encoded to bytes via fastavro) or pre-encoded bytes.
Pre-encoded bytes pass through without importing fastavro.

fastavro value conventions:
- Union values: fastavro writes the first branch whose type matches the value;
  disambiguate an ambiguous union (e.g. ``["int", "long"]``) with an explicit
  ``(branch_name, value)`` tuple.
- Decimal scale must match the schema definition.
- Naive ``datetime`` values are treated as UTC.
- UUID fields on fixed(16) receive 16 raw bytes; fixed-length fields are enforced.

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
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Dict, Optional

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


def _utc_naive_datetimes(value: Any) -> Any:
    """Return ``value`` with any naive ``datetime`` stamped as UTC, recursing into
    mappings and sequences (the latter covers union ``(branch, value)`` pairs).

    Returns ``value`` itself when nothing changed, so records without naive
    datetimes are not copied.
    """
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
    if isinstance(value, Mapping):
        coerced = {k: _utc_naive_datetimes(v) for k, v in value.items()}
        return coerced if any(coerced[k] is not value[k] for k in value) else value
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray, memoryview)):
        items = [_utc_naive_datetimes(v) for v in value]
        if all(new is old for new, old in zip(items, value)):
            return value
        return tuple(items) if isinstance(value, tuple) else list(items)
    return value


def _encode(parsed_schema: Dict[str, Any], record: Dict[str, Any]) -> bytes:
    """Encode a single record to Avro bytes using a parsed schema."""
    fastavro = _require_fastavro()
    output = BytesIO()
    fastavro.schemaless_writer(output, parsed_schema, _utc_naive_datetimes(record))
    return output.getvalue()


def _encode_batch(parsed_schema: Dict[str, Any], records: list) -> list:
    """Encode a batch of records to Avro bytes."""
    return [_encode(parsed_schema, record) for record in records]


class AvroEncoder:
    """Encodes ingest payloads for an Avro stream.

    Pre-encoded ``bytes``/``bytearray`` pass through as-is; dict/object payloads are
    encoded with fastavro. The writer schema is parsed lazily on the first object
    payload, so a bytes-only stream never imports fastavro.
    """

    def __init__(self, schema_json: str):
        self._schema_json = schema_json
        self._parsed: Optional[Dict[str, Any]] = None

    def _schema(self) -> Dict[str, Any]:
        if self._parsed is None:
            self._parsed = _parse_schema(self._schema_json)
        return self._parsed

    def encode(self, payload: Any) -> bytes:
        if isinstance(payload, (bytes, bytearray, memoryview)):
            return bytes(payload)
        return _encode(self._schema(), payload)

    def encode_batch(self, payloads: Any) -> list:
        # A single record is not a batch; reject it as the Rust batch API does.
        if isinstance(payloads, (str, bytes, bytearray, memoryview, Mapping)):
            raise TypeError("payloads must be a list of records")
        return [self.encode(p) for p in payloads]


def make_encoder(schema_json: Optional[str]) -> Optional[AvroEncoder]:
    """An `AvroEncoder` for an Avro stream, or `None` when `schema_json` is `None`."""
    return AvroEncoder(schema_json) if schema_json is not None else None
