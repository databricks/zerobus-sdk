"""
Synchronous Avro ingestion example.

Demonstrates ingesting dict records with the Avro format, including a
`timestamp-millis` logical-type field passed as a `datetime`.
"""

import json
from datetime import datetime, timezone

from example_config import required_env

from zerobus.sdk.sync import TableProperties, ZerobusSdk

SERVER_ENDPOINT = required_env("ZEROBUS_SERVER_ENDPOINT")
WORKSPACE_URL = required_env("DATABRICKS_WORKSPACE_URL")
TABLE_NAME = required_env("ZEROBUS_TABLE_NAME")
CLIENT_ID = required_env("DATABRICKS_CLIENT_ID")
CLIENT_SECRET = required_env("DATABRICKS_CLIENT_SECRET")

# Avro schema with various field types
schema = {
    "type": "record",
    "name": "SensorReading",
    "fields": [
        {"name": "device_id", "type": "string"},
        {"name": "reading_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "temperature", "type": "double"},
        {"name": "count", "type": "long"},
    ],
}

# Initialize SDK
sdk = ZerobusSdk(host=SERVER_ENDPOINT, unity_catalog_url=WORKSPACE_URL)

# Create stream with Avro format
table_properties = TableProperties(TABLE_NAME, avro_schema=json.dumps(schema))
stream = sdk.create_stream(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    table_properties=table_properties,
)

# Pattern: ingest in a loop, then flush() once
try:
    # Ingest dicts (auto-encoded to Avro)
    for i in range(10):
        record = {
            "device_id": f"device-{i}",
            "reading_time": datetime.now(timezone.utc),
            "temperature": 20.5 + i * 0.1,
            "count": i,
        }
        stream.ingest_record_offset(record)

    # Flush all pending records
    stream.flush()
    print("✓ All records ingested and acknowledged")

finally:
    stream.close()
