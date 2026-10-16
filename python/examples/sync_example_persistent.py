"""
Synchronous Ingestion Example - Persistent Stream

This example creates a persistent (Eos) stream, ingests records, and then resumes the same
stream by its stream_id.

A persistent stream records its identity and committed offset on the server. After a restart,
resume() reconnects to the same stream, and delivery into Delta stays exactly-once.

Persistent streams support JSON and protobuf records. Authentication uses OAuth or a custom
headers provider.
"""

import json
import logging
import os

from zerobus.sdk.sync import ZerobusSdk

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SERVER_ENDPOINT = os.getenv("ZEROBUS_SERVER_ENDPOINT", "https://your-shard-id.zerobus.region.cloud.databricks.com")
UNITY_CATALOG_ENDPOINT = os.getenv("DATABRICKS_WORKSPACE_URL", "https://your-workspace.cloud.databricks.com")
TABLE_NAME = os.getenv("ZEROBUS_TABLE_NAME", "catalog.schema.table")
CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID", "your-oauth-client-id")
CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET", "your-oauth-client-secret")

NUM_RECORDS = 100


def new_builder(sdk):
    return sdk.persistent_stream_builder().table(TABLE_NAME).oauth(CLIENT_ID, CLIENT_SECRET).json()


def main():
    sdk = ZerobusSdk(SERVER_ENDPOINT, UNITY_CATALOG_ENDPOINT)

    # Create a new persistent stream. The server assigns its stream_id.
    stream = new_builder(sdk).build()
    stream_id = stream.stream_id
    logger.info("Created persistent stream %s", stream_id)

    try:
        # Queue the records, then flush once to confirm them.
        for i in range(NUM_RECORDS):
            stream.ingest_record_offset(json.dumps({"device_name": f"sensor-{i}", "temp": 20 + i % 10}))
        stream.flush()
    finally:
        stream.close()

    # Save the stream_id. After a restart, resume the same stream with it.
    stream = new_builder(sdk).resume(stream_id)
    logger.info("Resumed stream %s. Committed offset at resume: %s", stream_id, stream.resume_watermark)
    try:
        stream.ingest_records_offset([json.dumps({"device_name": "sensor-resumed", "temp": 21})])
        stream.flush()
    finally:
        stream.close()


if __name__ == "__main__":
    main()
