"""
Synchronous Ingestion Example - Personal Access Token Authentication

This example demonstrates streaming to Zerobus with a Databricks personal access
token (PAT) instead of an OAuth client_id/client_secret. Create the PAT with the
`zerobus` API scope in the workspace that the Zerobus endpoint belongs to, and
export it as DATABRICKS_TOKEN. The SDK sends the token as given; it does not
refresh it, so a stream fails once the token expires or is revoked.

OAuth service principals remain the recommended option for production workloads;
see sync_example_json.py.

Record Type Mode: JSON (omitting a descriptor from TableProperties selects JSON).
"""

import logging

from example_config import required_env

from zerobus import PersonalAccessToken
from zerobus.sdk.shared import StreamConfigurationOptions, TableProperties
from zerobus.sdk.sync import ZerobusSdk

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


SERVER_ENDPOINT = required_env("ZEROBUS_SERVER_ENDPOINT")
WORKSPACE_URL = required_env("DATABRICKS_WORKSPACE_URL")
TABLE_NAME = required_env("ZEROBUS_TABLE_NAME")
DATABRICKS_TOKEN = required_env("DATABRICKS_TOKEN")

NUM_RECORDS = 100


def create_sample_json_record(index):
    """Create a sample record as a dict. JSON mode accepts a dict or a JSON string."""
    return {
        "device_name": f"sensor-{index % 10}",
        "temp": 20 + (index % 15),
        "humidity": 50 + (index % 40),
    }


def main():
    sdk = ZerobusSdk(SERVER_ENDPOINT, WORKSPACE_URL)

    # repr(auth) leaves the token out, so it is safe to log.
    auth = PersonalAccessToken(DATABRICKS_TOKEN)
    logger.info("Creating stream with %r to %s", auth, TABLE_NAME)

    # No descriptor => JSON record format.
    table_properties = TableProperties(TABLE_NAME)
    options = StreamConfigurationOptions(max_inflight_records=100_000, recovery=True)

    stream = sdk.create_stream(table_properties=table_properties, options=options, auth=auth)
    try:
        # Queue records in a loop, then flush once. Never wait per record.
        for i in range(NUM_RECORDS):
            stream.ingest_record_offset(create_sample_json_record(i))
        stream.flush()
        logger.info("Ingested and acknowledged %d records", NUM_RECORDS)
    finally:
        stream.close()


if __name__ == "__main__":
    main()
