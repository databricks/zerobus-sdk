"""
Synchronous Avro ingestion example.

Demonstrates ingesting records with the Avro format, including dicts (auto-encoded),
pre-encoded bytes, ambiguous unions, and logical types.
"""

import json
from datetime import datetime

from zerobus.sdk.sync import TableProperties, ZerobusSdk

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
sdk = ZerobusSdk(
    host="https://your-shard.zerobus.region.cloud.databricks.com",
    unity_catalog_url="https://your-workspace.cloud.databricks.com",
)

# Create stream with Avro format
table_properties = TableProperties("my_catalog.my_schema.my_table", avro_schema=json.dumps(schema))
stream = sdk.create_stream(
    client_id="your-client-id",
    client_secret="your-client-secret",
    table_properties=table_properties,
)

# Pattern: ingest in a loop, then flush() once
try:
    # Ingest dicts (auto-encoded to Avro)
    for i in range(10):
        record = {
            "device_id": f"device-{i}",
            "reading_time": datetime.now().timestamp() * 1000,
            "temperature": 20.5 + i * 0.1,
            "count": i,
        }
        stream.ingest_record_offset(record)

    # Flush all pending records
    stream.flush()
    print("✓ All records ingested and acknowledged")

finally:
    stream.close()
