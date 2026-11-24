"""
Integration tests for ``stream_builder()`` streams against a real Zerobus server in a dev
workspace.

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
    stream_count = 40
    prefix = RUN_PREFIX + "wide-"
    callback = _RecordingCallback()
    ids = set()
    last_id_per_stream = {}

    builder = _authenticated(_sdk(sync).stream_builder()).compiled_proto(_air_quality_descriptor())
    stream = builder.multiplexed(stream_count).ack_callback(callback).build()
    try:
        for i in range(2 * stream_count):
            message_id = stream.ingest_record(_air_quality(f"{prefix}{i}", i))
            ids.add(message_id)
            last_id_per_stream[_stream_index(message_id)] = message_id
        assert len(last_id_per_stream) == stream_count
        assert any(i < 0 for i in last_id_per_stream.values()), last_id_per_stream
        ids.add(stream.ingest_records([_air_quality(f"{prefix}batch-{i}", i) for i in range(2)]))

        # Deliberately wait on one ID per sub-stream, including the negative ones, to exercise
        # wait_for_message_id. For bulk ingestion, flush() once instead.
        for message_id in last_id_per_stream.values():
            stream.wait_for_message_id(message_id)
        stream.flush()
        # The callback receives exactly the IDs the ingest calls returned, negative ones included.
        callback.wait_for(ids)

        # -1 belongs to sub-stream 63, which this stream does not have: the core rejects it at
        # once instead of waiting.
        started = time.monotonic()
        with pytest.raises(core.NonRetriableException):
            stream.wait_for_message_id(-1)
        assert time.monotonic() - started < 5
    finally:
        stream.close()
    assert list(stream.get_unacked_records()) == []
    EXPECTED_ROWS[prefix] = 2 * stream_count + 2


def test_calls_after_close_are_rejected(module):
    prefix = RUN_PREFIX + "closed-"

    async def run():
        builder = _authenticated(_sdk(module).stream_builder()).compiled_proto(_air_quality_descriptor())
        stream = await _maybe(builder.multiplexed(4).build())
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


# Bytes that decode as neither a protobuf message nor an Avro datum.
UNDECODABLE = b"\xff"


def test_rejected_record_reaches_on_error_and_comes_back(module):
    prefix = RUN_PREFIX + "rejected-"
    callback = _RecordingCallback()
    record = _rejected_json(prefix)

    async def run():
        builder = _authenticated(_sdk(module).stream_builder()).recovery(False).json()
        stream = await _maybe(builder.multiplexed(1).ack_callback(callback).build())
        message_id = await _maybe(stream.ingest_record(record))
        with pytest.raises(core.ZerobusException):
            await _maybe(stream.flush())

        failures = callback.wait_for_failure()
        assert [failed_id for failed_id, _ in failures] == [message_id]
        assert all(isinstance(message, str) and message for _, message in failures)
        assert callback.acked == set()
        assert stream.is_closed
        with pytest.raises(core.ZerobusException):
            await _maybe(stream.ingest_record(record))
        with pytest.raises(core.ZerobusException):
            await _maybe(stream.close())
        assert list(await _maybe(stream.get_unacked_records())) == [record.encode()]
        # A failed stream rejects an empty batch too, as the core does.
        with pytest.raises(core.ZerobusException):
            await _maybe(stream.ingest_records([]))

    asyncio.run(run())


def _ingest_after_rejection(stream, calls):
    """Make each (method, payload, expected) call and return the records expected back.

    The first call ingests a record the server rejects, which fails the stream. Queuing takes far
    less than the server's round trip, so every later call is still accepted; if one raises, the
    test fails instead of silently checking fewer record shapes.
    """
    accepted = []
    for method, payload, expected in calls:
        getattr(stream, method)(payload)
        accepted.extend(expected)
    return accepted


def _assert_records_come_back(stream, accepted, decode):
    """Fail the stream, close it, and check that every accepted record comes back unacknowledged."""
    with pytest.raises(core.ZerobusException):
        stream.flush()
    try:
        stream.close()
    except core.ZerobusException:
        pass  # a multiplexed stream reports the failure again; an ordinary stream does not
    batches = list(stream.get_unacked_batches())
    assert batches
    assert all(isinstance(batch, list) and all(isinstance(r, bytes) for r in batch) for batch in batches)
    assert sorted(decode(r) for r in stream.get_unacked_records()) == sorted(accepted)


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


def test_multiplexed_avro_records_come_back_unacknowledged():
    prefix = RUN_PREFIX + "unacked-"
    rows = [_row(f"{prefix}avro-{i}", i) for i in range(6)]
    expected = [json.dumps(r, sort_keys=True) for r in rows]
    stream = _authenticated(_sdk(sync).stream_builder()).recovery(False).avro(AVRO_SCHEMA).multiplexed(1).build()
    accepted = _ingest_after_rejection(
        stream,
        [
            ("ingest_record", UNDECODABLE, ["undecodable"]),
            ("ingest_record", rows[0], expected[0:1]),
            ("ingest_record", _avro_bytes(rows[1]), expected[1:2]),
            ("ingest_records", rows[2:4], expected[2:4]),
            ("ingest_records", [_avro_bytes(r) for r in rows[4:6]], expected[4:6]),
        ],
    )
    _assert_records_come_back(stream, accepted, _decoded_avro)
    assert stream.is_closed
    with pytest.raises(core.NonRetriableException):
        stream.ingest_record(rows[0])


def test_ordinary_avro_stream_encodes_dicts(module):
    # build() wires the Avro encoder into an ordinary stream, on its own code path in each SDK.
    prefix = RUN_PREFIX + "ordinaryavro-"
    rows = [_row(f"{prefix}{i}", i) for i in range(10)]

    async def run():
        stream = await _maybe(_authenticated(_sdk(module).stream_builder()).avro(AVRO_SCHEMA).build())
        try:
            for row in rows:
                await _maybe(stream.ingest_record_offset(row))
            await _maybe(stream.flush())
        finally:
            await _maybe(stream.close())
        assert list(await _maybe(stream.get_unacked_records())) == []

    asyncio.run(run())
    EXPECTED_ROWS[prefix] = EXPECTED_ROWS.get(prefix, 0) + len(rows)


def test_ordinary_stream_ack_callback_receives_every_offset(module):
    prefix = RUN_PREFIX + "ordinaryack-"
    callback = _RecordingCallback()

    async def run():
        builder = _authenticated(_sdk(module).stream_builder()).compiled_proto(_air_quality_descriptor())
        stream = await _maybe(builder.ack_callback(callback).build())
        offsets = []
        try:
            for i in range(20):
                offsets.append(await _maybe(stream.ingest_record_offset(_air_quality(f"{prefix}{i}", i))))
            await _maybe(stream.flush())
        finally:
            await _maybe(stream.close())
        return offsets

    offsets = asyncio.run(run())
    assert len(set(offsets)) == 20
    callback.wait_for(offsets)
    EXPECTED_ROWS[prefix] = EXPECTED_ROWS.get(prefix, 0) + 20


def test_sync_flush_lets_other_python_threads_run():
    # flush() releases the GIL while it waits, so a Python thread keeps running meanwhile.
    prefix = RUN_PREFIX + "gil-"
    beats = []
    stop = threading.Event()

    def heartbeat():
        while not stop.is_set():
            beats.append(time.monotonic())
            time.sleep(0.001)

    stream = _authenticated(_sdk(sync).stream_builder()).json().multiplexed(2).build()
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
    assert finished - started >= 0.05, "flush() returned too fast to observe other threads"
    assert len([beat for beat in beats if started < beat < finished]) >= 5
    EXPECTED_ROWS[prefix] = 2000


def test_close_while_other_threads_ingest():
    ingest_threads = 4
    prefix = RUN_PREFIX + "closing-"
    ingested = [threading.Event() for _ in range(ingest_threads)]
    stream = _authenticated(_sdk(sync).stream_builder()).json().multiplexed(4).build()

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
        builder = _authenticated(_sdk(aio).stream_builder()).compiled_proto(_air_quality_descriptor())
        stream = await builder.multiplexed(2).ack_callback(callback).build()
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
        finally:
            await asyncio.wait_for(stream.close(), CALLBACK_WAIT_SECONDS)
        return ids

    ids = asyncio.run(run())
    callback.wait_for(ids)
    EXPECTED_ROWS[prefix] = 1000


def test_dropping_a_stream_without_close_leaves_the_sdk_usable():
    # Rows ingested into the dropped streams may or may not land, so they are not counted.
    prefix = RUN_PREFIX + "dropped-"
    sdk = _sdk(sync)
    stream = _authenticated(sdk.stream_builder()).json().multiplexed(2).build()
    stream.ingest_record(_row(prefix, 0))
    del stream
    gc.collect()

    # The SDK still opens and uses new streams.
    stream = _authenticated(sdk.stream_builder()).json().multiplexed(2).build()
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
        stream = await _authenticated(sdk.stream_builder()).json().multiplexed(2).build()
        await stream.ingest_record(_row(prefix, 2))
        return stream

    async def open_and_use():
        stream = await _authenticated(sdk.stream_builder()).json().multiplexed(2).build()
        try:
            await stream.ingest_record(_row(prefix, 3))
            await stream.flush()
        finally:
            await stream.close()

    stream = asyncio.run(open_and_keep())
    del stream
    gc.collect()
    asyncio.run(open_and_use())


def test_flush_timeout_reaches_the_core():
    prefix = RUN_PREFIX + "timeout-"
    stream = _authenticated(_sdk(sync).stream_builder()).json().flush_timeout_ms(1).multiplexed(2).build()
    try:
        for i in range(1000):
            stream.ingest_record(_row(f"{prefix}{i}", i))
        # 1 ms is too short for the server to acknowledge 1000 records; with the default (300 s)
        # this flush would succeed.
        with pytest.raises(core.ZerobusException):
            stream.flush()
    finally:
        try:
            stream.close()
        except core.ZerobusException:
            pass  # close() flushes with the same timeout


class _SlowCallback(core.AckCallback):
    """Takes 0.4 s per acknowledgment, so callbacks are still queued when close() starts."""

    def __init__(self):
        super().__init__()
        self.done = 0

    def on_ack(self, offset):
        time.sleep(0.4)
        self.done += 1  # callbacks of one sub-stream run one at a time

    def wait_for_all(self, expected):
        deadline = time.monotonic() + CALLBACK_WAIT_SECONDS
        while self.done < expected and time.monotonic() < deadline:
            time.sleep(0.05)
        assert self.done == expected, self.done


@pytest.mark.parametrize("wait", ["unset", None, 0])
def test_callback_max_wait_time_reaches_the_core(wait):
    # One sub-stream, so callbacks run one at a time: 20 of them take 8 s.
    prefix = RUN_PREFIX + "callbackwait-"
    callback = _SlowCallback()
    builder = _authenticated(_sdk(sync).stream_builder()).json()
    if wait != "unset":
        builder = builder.callback_max_wait_time_ms(wait)
    stream = builder.multiplexed(1).ack_callback(callback).build()
    closed = False
    try:
        for i in range(20):
            stream.ingest_record(_row(f"{prefix}{i}", i))
        stream.flush()
        assert callback.done <= 3, "the callbacks must still be queued when close() starts"
        started = time.monotonic()
        stream.close()
        closed = True
        took = time.monotonic() - started
        done = callback.done
    finally:
        if not closed:
            stream.close()
        # Callbacks still queued when close() stops waiting keep running. Let them finish before
        # the SDK is released: releasing it while a callback waits for the GIL hangs.
        callback.wait_for_all(20)
    EXPECTED_ROWS[prefix] = EXPECTED_ROWS.get(prefix, 0) + 20

    if wait == "unset":
        # The core's default wait is 5 s.
        assert 4 <= took and done < 20, (took, done)
    elif wait is None:
        # No limit: close() waits for every callback.
        assert done == 20, (took, done)
    else:
        assert took < 1 and done <= 5, (took, done)


@pytest.mark.parametrize("record_format", ["json", "avro"])
def test_async_multiplexed_batches_and_waits(record_format):
    # The async wrapper encodes JSON and Avro payloads on its own code path.
    prefix = RUN_PREFIX + "aio" + record_format + "-"
    rows = [_row(f"{prefix}{i}", i) for i in range(12)]
    if record_format == "json":
        singles = [json.dumps(rows[0]), rows[1]]
    else:
        singles = [rows[0], _avro_bytes(rows[1])]

    async def run():
        builder = _authenticated(_sdk(aio).stream_builder())
        builder = builder.json() if record_format == "json" else builder.avro(AVRO_SCHEMA)
        stream = await builder.multiplexed(2).build()
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


def test_max_ingest_payload_bytes_reaches_the_core():
    prefix = RUN_PREFIX + "payloadlimit-"
    limit = 64
    builder = _authenticated(_sdk(sync).stream_builder()).compiled_proto(_air_quality_descriptor())
    stream = builder.max_ingest_payload_bytes(limit).multiplexed(1).build()
    try:
        too_large = _air_quality(prefix + "x" * 100, 0)
        assert len(too_large.SerializeToString()) > limit
        # Checked on the client before anything is sent.
        with pytest.raises(core.NonRetriableException):
            stream.ingest_record(too_large)

        small = _air_quality(f"{prefix}0", 0)
        assert len(small.SerializeToString()) < limit
        stream.ingest_record(small)
        stream.flush()
    finally:
        stream.close()
    EXPECTED_ROWS[prefix] = 1
