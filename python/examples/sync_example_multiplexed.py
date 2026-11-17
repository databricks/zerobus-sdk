"""
Synchronous Ingestion Example - Multiplexed Stream (Beta)

This example opens a multiplexed stream: several sub-streams to one table, with records
distributed round-robin across them. Use it when one stream is the throughput bottleneck and
global ordering is not required. Records keep their order within a sub-stream, but there is no
order across sub-streams.

Each ingest call returns a message ID: an opaque int that identifies the sub-stream and the
record's position in it. Message IDs are not ordered and may be negative. Confirm durability
with flush(), not by waiting on each message ID.

Multiplexed streams support JSON, protobuf, and Avro records. Authentication uses OAuth or a
custom headers provider.
"""

import logging

# Import the generated protobuf module
import record_pb2
from example_config import required_env

from zerobus.sdk.sync import ZerobusSdk

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SERVER_ENDPOINT = required_env("ZEROBUS_SERVER_ENDPOINT")
WORKSPACE_URL = required_env("DATABRICKS_WORKSPACE_URL")
TABLE_NAME = required_env("ZEROBUS_TABLE_NAME")
CLIENT_ID = required_env("DATABRICKS_CLIENT_ID")
CLIENT_SECRET = required_env("DATABRICKS_CLIENT_SECRET")

STREAM_COUNT = 4
NUM_RECORDS = 1000
BATCH_SIZE = 100


def main():
    sdk = ZerobusSdk(SERVER_ENDPOINT, WORKSPACE_URL)
    stream = (
        sdk.stream_builder()
        .table(TABLE_NAME)
        .oauth(CLIENT_ID, CLIENT_SECRET)
        .compiled_proto(record_pb2.AirQuality.DESCRIPTOR.file.serialized_pb)
        .multiplexed(STREAM_COUNT)
        .build()
    )
    logger.info("Opened a multiplexed stream with %d sub-streams", STREAM_COUNT)

    try:
        # Queue the records, then flush once to confirm them on every sub-stream.
        for i in range(NUM_RECORDS):
            stream.ingest_record(record_pb2.AirQuality(device_name=f"sensor-{i}", temp=20 + i % 10, humidity=50))
        stream.flush()
        logger.info("%d records are durable", NUM_RECORDS)

        # A batch lands on one sub-stream, and one message ID covers it.
        for start in range(0, NUM_RECORDS, BATCH_SIZE):
            batch = [
                record_pb2.AirQuality(device_name=f"sensor-batch-{i}", temp=20 + i % 10, humidity=50)
                for i in range(start, start + BATCH_SIZE)
            ]
            stream.ingest_records(batch)
        stream.flush()
        logger.info("%d batched records are durable", NUM_RECORDS)
    finally:
        stream.close()

    # Records that were never acknowledged stay available after close, so they can be
    # ingested into a new stream. After a successful flush and close, there are none.
    logger.info("Unacknowledged records after close: %d", len(list(stream.get_unacked_records())))


if __name__ == "__main__":
    main()
