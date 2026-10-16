"""
Synchronous Ingestion Example - Persistent Stream

This example creates a persistent (Eos) stream or resumes an existing one.

On the first run, leave ZEROBUS_PERSISTENT_STREAM_ID unset. The server creates a durable
stream, and this example prints its stream_id. Save that ID. On a later run, set
ZEROBUS_PERSISTENT_STREAM_ID to the saved ID to resume the same stream from the last offset the
server committed. Delivery into Delta stays exactly-once.

Persistent streams support JSON and protobuf records. Authentication uses OAuth or a custom
headers provider.

The API is experimental because the persistent stream API in the Rust core is in development.
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
STREAM_ID = os.getenv("ZEROBUS_PERSISTENT_STREAM_ID")

NUM_RECORDS = 100


def main():
    sdk = ZerobusSdk(SERVER_ENDPOINT, UNITY_CATALOG_ENDPOINT)
    builder = sdk.persistent_stream_builder().table(TABLE_NAME).oauth(CLIENT_ID, CLIENT_SECRET).json()

    if STREAM_ID:
        stream = builder.resume(STREAM_ID)
        logger.info("Resumed persistent stream %s. Committed offset at resume: %s", STREAM_ID, stream.resume_watermark)
    else:
        stream = builder.build()
        logger.info("Created persistent stream %s. Save this ID to resume later.", stream.stream_id)

    try:
        # Queue the records, then flush once to confirm them.
        for i in range(NUM_RECORDS):
            stream.ingest_record_offset(json.dumps({"device_name": f"sensor-{i}", "temp": 20 + i % 10}))
        stream.flush()
        logger.info("All queued records are durable")
    finally:
        # Closing ends this connection only. The persistent stream stays available to resume.
        stream.close()


if __name__ == "__main__":
    main()
