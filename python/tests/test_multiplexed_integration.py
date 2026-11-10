"""
Integration tests for ``stream_builder()`` streams against a real Zerobus server in a dev
workspace.

Skipped unless these environment variables are set:

- ``ZEROBUS_SERVER_ENDPOINT``: the Zerobus endpoint, for example
  ``https://<workspace-id>.zerobus.<region>.cloud.databricks.com``.
- ``DATABRICKS_WORKSPACE_URL``: the workspace URL, used for OAuth.
- ``ZEROBUS_TABLE_NAME``: the target table (``catalog.schema.table``) with the columns
  ``device_name STRING, temp INT, humidity BIGINT``.
- ``DATABRICKS_CLIENT_ID`` and ``DATABRICKS_CLIENT_SECRET``: OAuth client credentials of a
  service principal that can write to the table.

Every row written by one run starts with a unique ``mux-<run id>-`` prefix in
``device_name``. Run with ``-s`` to see the expected row counts and a SQL query to check the
table:

    pytest -s tests/test_multiplexed_integration.py
"""

import asyncio
import io
import json
import os
import threading
import time
import uuid

import pytest

from zerobus import _zerobus_core as core
from zerobus.sdk import aio, sync

ENDPOINT = os.getenv("ZEROBUS_SERVER_ENDPOINT")
TABLE = os.getenv("ZEROBUS_TABLE_NAME")
WORKSPACE_URL = os.getenv("DATABRICKS_WORKSPACE_URL")
CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID")
CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET")

pytestmark = pytest.mark.skipif(
    not (ENDPOINT and WORKSPACE_URL and TABLE and CLIENT_ID and CLIENT_SECRET),
    reason="set ZEROBUS_SERVER_ENDPOINT, DATABRICKS_WORKSPACE_URL, ZEROBUS_TABLE_NAME, "
    "DATABRICKS_CLIENT_ID, and DATABRICKS_CLIENT_SECRET",
)

# Bits of a message ID above the sub-stream offset, holding the sub-stream index.
STREAM_INDEX_SHIFT = 58
# Seconds to wait for ACK callbacks, which run asynchronously after flush().
CALLBACK_WAIT_SECONDS = 30

AVRO_SCHEMA = json.dumps(
    {
        "type": "record",
        "name": "AirQuality",
        "fields": [
            {"name": "device_name", "type": "string"},
            {"name": "temp", "type": "int"},
            {"name": "humidity", "type": "long"},
        ],
    }
)

RUN_PREFIX = f"mux-{uuid.uuid4().hex[:8]}-"
# Rows each test expects to find in the table, keyed by device_name prefix.
EXPECTED_ROWS = {}


class _RecordingCallback(core.AckCallback):
    """Records every ID the stream acknowledges or fails. Sub-streams may call it concurrently."""

    def __init__(self):
        super().__init__()
        self._lock = threading.Lock()
        self.acked = set()
        self.errors = []

    def on_ack(self, offset):
        with self._lock:
            if offset in self.acked:
                self.errors.append(f"duplicate ACK for {offset}")
            self.acked.add(offset)

    def on_error(self, offset, error_message):
        with self._lock:
            self.errors.append(f"{offset}: {error_message}")

    def wait_for(self, expected):
        """Wait until exactly ``expected`` has been acknowledged."""
        deadline = time.monotonic() + CALLBACK_WAIT_SECONDS
        while time.monotonic() < deadline:
            with self._lock:
                if len(self.acked) >= len(expected):
                    break
            time.sleep(0.05)
        with self._lock:
            assert self.acked == set(expected), "ACKed IDs must match the IDs the ingest calls returned"
            assert not self.errors, self.errors


@pytest.fixture(scope="module", autouse=True)
def _print_verification_query():
    yield
    if not EXPECTED_ROWS:
        return
    print(f"\n=== stream_builder() integration run {RUN_PREFIX} ===")
    print("Expected rows per device_name prefix:")
    for prefix, rows in EXPECTED_ROWS.items():
        print(f"  {prefix}%  ->  {rows}")
    print(f"  total: {sum(EXPECTED_ROWS.values())}")
    print("Check with:")
    print(
        f"  SELECT regexp_extract(device_name, '^({RUN_PREFIX}[a-z]+-)', 1) AS prefix, count(*) AS row_count "
        f"FROM {TABLE} WHERE device_name LIKE '{RUN_PREFIX}%' GROUP BY 1 ORDER BY 1;"
    )


def _sdk(module):
    return module.ZerobusSdk(ENDPOINT, WORKSPACE_URL)


def _authenticated(builder):
    return builder.table(TABLE).oauth(CLIENT_ID, CLIENT_SECRET)


def _air_quality(device_name, i):
    from tests import test_row_pb2

    return test_row_pb2.AirQuality(device_name=device_name, temp=20 + i % 10, humidity=50 + i % 10)


def _air_quality_descriptor():
    from tests import test_row_pb2

    return test_row_pb2.AirQuality.DESCRIPTOR


def _row(device_name, i):
    return {"device_name": device_name, "temp": 20 + i % 10, "humidity": 50 + i % 10}


def _stream_index(message_id):
    """The sub-stream index packed into the top bits of a message ID (an unsigned view of the int)."""
    return (message_id & 0xFFFF_FFFF_FFFF_FFFF) >> STREAM_INDEX_SHIFT


def test_ordinary_stream():
    prefix = RUN_PREFIX + "ordinary-"
    stream = _authenticated(_sdk(sync).stream_builder()).compiled_proto(_air_quality_descriptor()).build()
    try:
        for i in range(10):
            stream.ingest_record_offset(_air_quality(f"{prefix}{i}", i))
        stream.flush()
    finally:
        stream.close()
    EXPECTED_ROWS[prefix] = 10


def test_multiplexed_proto_spreads_records_and_acks_every_message_id():
    stream_count = 4
    prefix = RUN_PREFIX + "proto-"
    callback = _RecordingCallback()
    ids = set()
    rows = 0

    builder = _authenticated(_sdk(sync).stream_builder()).compiled_proto(_air_quality_descriptor())
    stream = builder.multiplexed(stream_count).ack_callback(callback).build()
    try:
        # Ingest in a loop, then flush once.
        for i in range(1000):
            ids.add(stream.ingest_record(_air_quality(f"{prefix}msg-{i}", i)))
            rows += 1
        for i in range(10):
            ids.add(stream.ingest_record(_air_quality(f"{prefix}bytes-{i}", i).SerializeToString()))
            rows += 1
        for b in range(10):
            batch = [_air_quality(f"{prefix}batch-{b}-{i}", i) for i in range(10)]
            batch_id = stream.ingest_records(batch)
            assert batch_id is not None
            ids.add(batch_id)
            rows += len(batch)
        assert stream.ingest_records([]) is None

        stream.flush()

        # Every ingest call returns a distinct ID.
        assert len(ids) == 1000 + 10 + 10
        # Each ingest call is acknowledged exactly once with the ID it returned.
        callback.wait_for(ids)
    finally:
        stream.close()

    assert stream.is_closed
    assert list(stream.get_unacked_records()) == []
    assert list(stream.get_unacked_batches()) == []
    with pytest.raises(core.ZerobusException):
        stream.ingest_record(_air_quality(prefix, 0))
    EXPECTED_ROWS[prefix] = rows


def test_negative_message_ids_from_sub_streams_32_and_above_can_be_awaited():
    stream_count = 40
    prefix = RUN_PREFIX + "wide-"
    last_id_per_stream = {}

    builder = _authenticated(_sdk(sync).stream_builder()).compiled_proto(_air_quality_descriptor())
    stream = builder.multiplexed(stream_count).build()
    try:
        for i in range(2 * stream_count):
            message_id = stream.ingest_record(_air_quality(f"{prefix}{i}", i))
            last_id_per_stream[_stream_index(message_id)] = message_id
        assert len(last_id_per_stream) == stream_count
        assert any(i < 0 for i in last_id_per_stream.values()), last_id_per_stream

        # Deliberately wait on one ID per sub-stream, including the negative ones, to exercise
        # wait_for_message_id. For bulk ingestion, flush() once instead.
        for message_id in last_id_per_stream.values():
            stream.wait_for_message_id(message_id)
        stream.flush()
    finally:
        stream.close()
    assert list(stream.get_unacked_records()) == []
    EXPECTED_ROWS[prefix] = 2 * stream_count


def test_multiplexed_json():
    prefix = RUN_PREFIX + "json-"
    stream = _authenticated(_sdk(sync).stream_builder()).json().multiplexed(2).build()
    try:
        for i in range(100):
            stream.ingest_record(json.dumps(_row(f"{prefix}string-{i}", i)))
        for i in range(10):
            stream.ingest_record(_row(f"{prefix}dict-{i}", i))  # serialized with json.dumps
        assert stream.ingest_records([json.dumps(_row(f"{prefix}batch-{i}", i)) for i in range(10)]) is not None
        stream.flush()
    finally:
        stream.close()
    assert list(stream.get_unacked_records()) == []
    EXPECTED_ROWS[prefix] = 120


def test_multiplexed_avro():
    import fastavro

    prefix = RUN_PREFIX + "avro-"
    stream = _authenticated(_sdk(sync).stream_builder()).avro(AVRO_SCHEMA).multiplexed(2).build()
    try:
        for i in range(50):
            stream.ingest_record(_row(f"{prefix}dict-{i}", i))  # encoded with fastavro
        encoded = io.BytesIO()
        fastavro.schemaless_writer(encoded, fastavro.parse_schema(json.loads(AVRO_SCHEMA)), _row(f"{prefix}bytes", 9))
        stream.ingest_record(encoded.getvalue())
        assert stream.ingest_records([_row(f"{prefix}batch-{i}", i) for i in range(10)]) is not None
        stream.flush()
    finally:
        stream.close()
    assert list(stream.get_unacked_records()) == []
    EXPECTED_ROWS[prefix] = 61


def test_async_multiplexed_proto():
    prefix = RUN_PREFIX + "async-"
    callback = _RecordingCallback()

    async def run():
        builder = _authenticated(_sdk(aio).stream_builder()).compiled_proto(_air_quality_descriptor())
        stream = await builder.multiplexed(4).ack_callback(callback).build()
        ids = set()
        try:
            for i in range(200):
                ids.add(await stream.ingest_record(_air_quality(f"{prefix}{i}", i)))
            await stream.flush()
        finally:
            await stream.close()
        assert list(await stream.get_unacked_records()) == []
        return ids

    ids = asyncio.run(run())
    callback.wait_for(ids)
    EXPECTED_ROWS[prefix] = 200
