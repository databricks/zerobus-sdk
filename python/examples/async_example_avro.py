"""
Asynchronous Avro ingestion example.

Demonstrates async ingest pattern with Avro format.
"""

import asyncio
import json
from datetime import datetime

from zerobus.sdk.aio import TableProperties, ZerobusSdk

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
    sdk = ZerobusSdk(
        host="https://your-shard.zerobus.region.cloud.databricks.com",
        unity_catalog_url="https://your-workspace.cloud.databricks.com",
    )

    table_properties = TableProperties("my_catalog.my_schema.my_table", avro_schema=json.dumps(schema))
    stream = await sdk.create_stream(
        client_id="your-client-id",
        client_secret="your-client-secret",
        table_properties=table_properties,
    )

    try:
        # Ingest in a loop
        for i in range(10):
            record = {
                "event_id": f"evt-{i}",
                "event_time": int(datetime.now().timestamp() * 1000),
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
