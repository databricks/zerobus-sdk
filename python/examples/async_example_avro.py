"""
Asynchronous Avro ingestion example.

Demonstrates async ingest pattern with Avro format.
"""

import asyncio
import json
from datetime import datetime, timezone

from example_config import required_env

from zerobus.sdk.aio import TableProperties, ZerobusSdk

SERVER_ENDPOINT = required_env("ZEROBUS_SERVER_ENDPOINT")
WORKSPACE_URL = required_env("DATABRICKS_WORKSPACE_URL")
TABLE_NAME = required_env("ZEROBUS_TABLE_NAME")
CLIENT_ID = required_env("DATABRICKS_CLIENT_ID")
CLIENT_SECRET = required_env("DATABRICKS_CLIENT_SECRET")

schema = {
    "type": "record",
    "name": "Event",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "event_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "value", "type": "double"},
    ],
}


async def main():
    sdk = ZerobusSdk(host=SERVER_ENDPOINT, unity_catalog_url=WORKSPACE_URL)

    table_properties = TableProperties(TABLE_NAME, avro_schema=json.dumps(schema))
    stream = await sdk.create_stream(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        table_properties=table_properties,
    )

    try:
        # Ingest in a loop
        for i in range(10):
            record = {
                "event_id": f"evt-{i}",
                "event_time": datetime.now(timezone.utc),
                "value": 100.0 + i,
            }
            await stream.ingest_record_offset(record)

        # Flush all pending
        await stream.flush()
        print("✓ All async Avro records ingested")

    finally:
        await stream.close()


if __name__ == "__main__":
    asyncio.run(main())
