"""
Integration tests for multiplexed streams (Beta) against a real Zerobus server in a dev
workspace. They cover what the Python binding adds on top of the Rust core: message IDs in
both directions, calls after close, errors and unacknowledged records, threads and asyncio.

Skipped unless these environment variables are set:

- ``ZEROBUS_SERVER_ENDPOINT``: the Zerobus endpoint; see "Connection URLs and Workspace ID" in
  the top-level ``README.md``.
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
import gc
import inspect
import io
import json
import os
import threading
import time
import uuid

import pytest

from zerobus import MultiplexedStreamOptions, OAuth, TableProperties
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

# Seconds to wait for ACK callbacks, which run asynchronously after flush().
CALLBACK_WAIT_SECONDS = 30
# Seconds to wait for a thread before the test reports it as stuck.
THREAD_JOIN_SECONDS = 120

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
        self.failed = []

    def on_ack(self, offset):
        with self._lock:
            if offset in self.acked:
                self.errors.append(f"duplicate ACK for {offset}")
            self.acked.add(offset)

    def on_error(self, offset, error_message):
        with self._lock:
            self.errors.append(f"{offset}: {error_message}")
            self.failed.append((offset, error_message))

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

    def wait_for_failure(self):
        """Wait until ``on_error`` has been called, and return its (ID, message) calls."""
        deadline = time.monotonic() + CALLBACK_WAIT_SECONDS
        while time.monotonic() < deadline:
            with self._lock:
                if self.failed:
                    break
            time.sleep(0.05)
        with self._lock:
            assert self.failed, "on_error was not called"
            return list(self.failed)


@pytest.fixture(scope="module", autouse=True)
def _print_verification_query():
    yield
    if not EXPECTED_ROWS:
        return
    print(f"\n=== create_multiplexed_stream() integration run {RUN_PREFIX} ===")
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


def _proto_table():
    from tests import test_row_pb2

    return TableProperties(TABLE, test_row_pb2.AirQuality.DESCRIPTOR)


def _avro_table():
    return TableProperties(TABLE, avro_schema=AVRO_SCHEMA)


def _create(sdk, table_properties, stream_count, options=None):
    """Open a multiplexed stream with the test's OAuth credentials; awaitable in the async SDK."""
    return sdk.create_multiplexed_stream(
        table_properties, stream_count, auth=OAuth(CLIENT_ID, CLIENT_SECRET), options=options
    )


def _air_quality(device_name, i):
    from tests import test_row_pb2

    return test_row_pb2.AirQuality(device_name=device_name, temp=20 + i % 10, humidity=50 + i % 10)


def _row(device_name, i):
    return {"device_name": device_name, "temp": 20 + i % 10, "humidity": 50 + i % 10}


@pytest.fixture(params=["sync", "aio"])
def module(request):
    return sync if request.param == "sync" else aio


async def _maybe(result):
    """Await ``result`` when it comes from the async API; return it as is from the sync API."""
    return await result if inspect.isawaitable(result) else result


def _run_concurrently(threads, body):
    """Run ``body(index)`` on ``threads`` threads at once and return what the threads raised."""
    failures = []
    lock = threading.Lock()
    start = threading.Event()

    def worker(index):
        start.wait()
        try:
            body(index)
        except BaseException as e:  # reported to the test thread
            with lock:
                failures.append(e)

    workers = [threading.Thread(target=worker, args=(index,), daemon=True) for index in range(threads)]
    for w in workers:
        w.start()
    start.set()
    for w in workers:
        w.join(THREAD_JOIN_SECONDS)
    assert not any(w.is_alive() for w in workers), "a thread did not finish"
    return failures


def test_negative_message_ids_from_sub_streams_32_and_above_can_be_awaited():
    prefix = RUN_PREFIX + "wide-"
    callback = _RecordingCallback()
    ids = set()

    stream = _create(_sdk(sync), _proto_table(), 40, MultiplexedStreamOptions(ack_callback=callback))
    try:
        for i in range(80):
            ids.add(stream.ingest_record(_air_quality(f"{prefix}{i}", i)))
        negative = [message_id for message_id in ids if message_id < 0]
        assert negative, ids
        ids.add(stream.ingest_records([_air_quality(f"{prefix}batch-{i}", i) for i in range(2)]))

        # Deliberately wait on every negative ID to exercise wait_for_message_id with them. For
        # bulk ingestion, flush() once instead.
        for message_id in negative:
            stream.wait_for_message_id(message_id)
        stream.flush()
        # The callback receives exactly the IDs the ingest calls returned, negative ones included.
        callback.wait_for(ids)

        # -1 belongs to sub-stream 63, which this stream does not have: the core rejects it.
        with pytest.raises(core.NonRetriableException):
            stream.wait_for_message_id(-1)
    finally:
        stream.close()
    assert list(stream.get_unacked_records()) == []
    EXPECTED_ROWS[prefix] = 82


def test_calls_after_close_are_rejected(module):
    prefix = RUN_PREFIX + "closed-"

    async def run():
        stream = await _maybe(_create(_sdk(module), _proto_table(), 4))
        try:
            # An empty batch on an open stream queues nothing.
            assert await _maybe(stream.ingest_records([])) is None
            for i in range(8):
                await _maybe(stream.ingest_record(_air_quality(f"{prefix}{i}", i)))
            await _maybe(stream.flush())
        finally:
            await _maybe(stream.close())
        assert stream.is_closed
        assert list(await _maybe(stream.get_unacked_records())) == []
        assert list(await _maybe(stream.get_unacked_batches())) == []

        # Each call fails with the core's closed-stream error, an empty batch too.
        for call in (
            lambda: stream.ingest_record(_air_quality(prefix, 0)),
            lambda: stream.ingest_records([_air_quality(prefix, 0)]),
            lambda: stream.ingest_records([]),
            stream.flush,
        ):
            with pytest.raises(core.NonRetriableException):
                await _maybe(call())

    asyncio.run(run())
    EXPECTED_ROWS[prefix] = EXPECTED_ROWS.get(prefix, 0) + 8


# A JSON record the table rejects: temp is an INT column.
def _rejected_json(prefix):
    return json.dumps({"device_name": f"{prefix}rejected", "temp": "not a number", "humidity": 1})


# Bytes that decode as no Avro datum.
UNDECODABLE = b"\xff"


def test_rejected_record_reaches_on_error_and_comes_back(module):
    prefix = RUN_PREFIX + "rejected-"
    callback = _RecordingCallback()
    record = _rejected_json(prefix)
    options = MultiplexedStreamOptions(recovery=False, ack_callback=callback)

    async def run():
        stream = await _maybe(_create(_sdk(module), TableProperties(TABLE), 2, options))
        # The valid record goes to sub-stream 0, the rejected one to sub-stream 1, so the
        # rejected record's ID is not a bare sub-stream offset. Deliberately wait for the valid
        # record, so only the rejected one stays unacknowledged.
        valid_id = await _maybe(stream.ingest_record(json.dumps(_row(f"{prefix}valid", 0))))
        await _maybe(stream.wait_for_message_id(valid_id))
        rejected_id = await _maybe(stream.ingest_record(record))
        with pytest.raises(core.ZerobusException):
            await _maybe(stream.flush())

        failures = callback.wait_for_failure()
        failed_ids = {failed_id for failed_id, _ in failures}
        assert rejected_id in failed_ids
        assert failed_ids <= {valid_id, rejected_id}
        assert all(isinstance(message, str) and message for _, message in failures)
        assert stream.is_closed
        with pytest.raises(core.ZerobusException):
            await _maybe(stream.ingest_record(record))
        with pytest.raises(core.ZerobusException):
            await _maybe(stream.close())
        assert list(await _maybe(stream.get_unacked_batches())) == [[record.encode()]]
        # A failed stream rejects an empty batch too, as the core does.
        with pytest.raises(core.ZerobusException):
            await _maybe(stream.ingest_records([]))

    asyncio.run(run())
    EXPECTED_ROWS[prefix] = 1


def _avro_bytes(row):
    import fastavro

    encoded = io.BytesIO()
    fastavro.schemaless_writer(encoded, fastavro.parse_schema(json.loads(AVRO_SCHEMA)), row)
    return encoded.getvalue()


def _decoded_avro(record):
    """An Avro record as canonical JSON, or ``"undecodable"`` for the rejected one."""
    import fastavro

    if record == UNDECODABLE:
        return "undecodable"
    row = fastavro.schemaless_reader(io.BytesIO(record), fastavro.parse_schema(json.loads(AVRO_SCHEMA)))
    return json.dumps(row, sort_keys=True)


def test_avro_records_come_back_unacknowledged():
    prefix = RUN_PREFIX + "unacked-"
    rows = [_row(f"{prefix}avro-{i}", i) for i in range(6)]
    expected = [json.dumps(r, sort_keys=True) for r in rows]
    stream = _create(_sdk(sync), _avro_table(), 1, MultiplexedStreamOptions(recovery=False))

    # The first record fails the stream. A later call may raise once the stream has failed;
    # that call and every call after it queue nothing, so their records are not expected back.
    accepted = ["undecodable"]
    stream.ingest_record(UNDECODABLE)
    for method, payload, records in (
        ("ingest_record", rows[0], expected[0:1]),
        ("ingest_record", _avro_bytes(rows[1]), expected[1:2]),
        ("ingest_records", rows[2:4], expected[2:4]),
        ("ingest_records", [_avro_bytes(r) for r in rows[4:6]], expected[4:6]),
    ):
        try:
            getattr(stream, method)(payload)
        except core.ZerobusException:
            break
        accepted.extend(records)
    if len(accepted) == 1:
        pytest.skip("the server rejected the first record before any other was queued")

    with pytest.raises(core.ZerobusException):
        stream.flush()
    with pytest.raises(core.ZerobusException):
        stream.close()
    assert sorted(_decoded_avro(r) for r in stream.get_unacked_records()) == sorted(accepted)


def test_sync_flush_lets_other_python_threads_run():
    # flush() releases the GIL while it waits, so a Python thread keeps running meanwhile.
    prefix = RUN_PREFIX + "gil-"
    beats = []
    stop = threading.Event()

    def heartbeat():
        while not stop.is_set():
            beats.append(time.monotonic())
            time.sleep(0.001)

    stream = _create(_sdk(sync), TableProperties(TABLE), 2)
    beating = threading.Thread(target=heartbeat, daemon=True)
    try:
        for i in range(2000):
            stream.ingest_record(_row(f"{prefix}{i}", i))
        beating.start()
        started = time.monotonic()
        stream.flush()
        finished = time.monotonic()
    finally:
        stop.set()
        beating.join(THREAD_JOIN_SECONDS)
        stream.close()
    EXPECTED_ROWS[prefix] = 2000
    if finished - started < 0.02:
        pytest.skip("flush() returned too fast to observe other threads")
    # One beat can come from the gap just before the native call; two need the GIL released.
    assert len([beat for beat in beats if started < beat < finished]) >= 2


def test_close_while_other_threads_ingest():
    ingest_threads = 4
    prefix = RUN_PREFIX + "closing-"
    ingested = [threading.Event() for _ in range(ingest_threads)]
    stream = _create(_sdk(sync), TableProperties(TABLE), 4)

    def body(thread):
        if thread >= ingest_threads:
            # Two threads close the stream, concurrently with each other and with the ingest
            # calls, once every ingest thread is under way.
            for event in ingested:
                assert event.wait(THREAD_JOIN_SECONDS), "an ingest thread never ingested"
            try:
                stream.close()
            except core.ZerobusException:
                pass  # the close that comes second may find the stream already closing
            return
        # Bounded so a slow close cannot let the threads queue records without limit.
        for i in range(5000):
            try:
                message_id = stream.ingest_record(_row(f"{prefix}{thread}-{i}", i))
            except core.ZerobusException:
                return
            assert isinstance(message_id, int)
            ingested[thread].set()

    try:
        assert _run_concurrently(ingest_threads + 2, body) == []
    finally:
        if not stream.is_closed:
            stream.close()
    assert stream.is_closed


def test_cancelled_flush_leaves_the_async_stream_usable():
    prefix = RUN_PREFIX + "cancel-"
    callback = _RecordingCallback()

    async def run():
        stream = await _create(_sdk(aio), _proto_table(), 2, MultiplexedStreamOptions(ack_callback=callback))
        ids = set()
        try:
            for i in range(1000):
                ids.add(await stream.ingest_record(_air_quality(f"{prefix}{i}", i)))
            flush = asyncio.create_task(stream.flush())
            await asyncio.sleep(0)  # let the flush start its native call
            flush.cancel()
            # If the flush finished before the cancel, the test has proved nothing and fails here.
            with pytest.raises(asyncio.CancelledError):
                await flush
            assert flush.cancelled()

            # The stream is still usable: it takes another record and flushes it.
            assert not stream.is_closed
            ids.add(await stream.ingest_record(_air_quality(f"{prefix}1000", 1000)))
            await asyncio.wait_for(stream.flush(), CALLBACK_WAIT_SECONDS)
        finally:
            await asyncio.wait_for(stream.close(), CALLBACK_WAIT_SECONDS)
        return ids

    ids = asyncio.run(run())
    callback.wait_for(ids)
    EXPECTED_ROWS[prefix] = 1001


def test_dropping_a_stream_without_close_leaves_the_sdk_usable():
    # Rows ingested into the dropped streams may or may not land, so they are not counted.
    prefix = RUN_PREFIX + "dropped-"
    sdk = _sdk(sync)
    stream = _create(sdk, TableProperties(TABLE), 2)
    stream.ingest_record(_row(prefix, 0))
    del stream
    gc.collect()

    # The SDK still opens and uses new streams.
    stream = _create(sdk, TableProperties(TABLE), 2)
    try:
        stream.ingest_record(_row(prefix, 1))
        stream.flush()
    finally:
        stream.close()


def test_async_stream_dropped_after_its_event_loop_leaves_the_sdk_usable():
    # Dropping inside a running loop is the same as the sync case above.
    prefix = RUN_PREFIX + "dropped-"
    sdk = _sdk(aio)

    async def open_and_keep():
        stream = await _create(sdk, TableProperties(TABLE), 2)
        await stream.ingest_record(_row(prefix, 2))
        return stream

    async def open_and_use():
        stream = await _create(sdk, TableProperties(TABLE), 2)
        try:
            await stream.ingest_record(_row(prefix, 3))
            await stream.flush()
        finally:
            await stream.close()

    stream = asyncio.run(open_and_keep())
    del stream
    gc.collect()
    asyncio.run(open_and_use())


@pytest.mark.parametrize("record_format", ["json", "avro"])
def test_async_multiplexed_batches_and_waits(record_format):
    # The async wrapper encodes JSON and Avro payloads on its own code path.
    prefix = RUN_PREFIX + "aio" + record_format + "-"
    rows = [_row(f"{prefix}{i}", i) for i in range(12)]
    if record_format == "json":
        table, singles = TableProperties(TABLE), [json.dumps(rows[0]), rows[1]]
    else:
        table, singles = _avro_table(), [rows[0], _avro_bytes(rows[1])]

    async def run():
        stream = await _create(_sdk(aio), table, 2)
        try:
            for payload in singles:
                assert isinstance(await stream.ingest_record(payload), int)
            batch_id = await stream.ingest_records(rows[2:])
            assert isinstance(batch_id, int)
            assert await stream.ingest_records([]) is None
            # Deliberately wait on the batch's ID to exercise wait_for_message_id.
            await stream.wait_for_message_id(batch_id)
            # -1 belongs to sub-stream 63, which this stream does not have.
            with pytest.raises(core.NonRetriableException):
                await asyncio.wait_for(stream.wait_for_message_id(-1), 5)
            await stream.flush()
        finally:
            await stream.close()
        assert list(await stream.get_unacked_batches()) == []

    asyncio.run(run())
    EXPECTED_ROWS[prefix] = len(rows)


class _RaisingCallback(_RecordingCallback):
    """Records each ID, then raises; the stream must keep delivering acknowledgments."""

    def on_ack(self, offset):
        super().on_ack(offset)
        raise RuntimeError("raised on purpose by the test callback")


def test_raising_ack_callback_does_not_stop_delivery():
    prefix = RUN_PREFIX + "raising-"
    callback = _RaisingCallback()
    stream = _create(_sdk(sync), TableProperties(TABLE), 2, MultiplexedStreamOptions(ack_callback=callback))
    try:
        ids = [stream.ingest_record(_row(f"{prefix}{i}", i)) for i in range(20)]
        stream.flush()
        # Every acknowledgment arrives, and none turns into an on_error call.
        callback.wait_for(ids)
    finally:
        stream.close()
    EXPECTED_ROWS[prefix] = 20
