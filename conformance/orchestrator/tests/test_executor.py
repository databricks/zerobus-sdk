import asyncio

import pytest

from tests.fakes import FakeHarness, FakeMock
from zerobus_test_runner.capability import Classification, State
from zerobus_test_runner.clients import MockAdminClient
from zerobus_test_runner.executor import Executor
from zerobus_test_runner.runner import Plan, RunResult, _run_one, expand, guard_rail_error
from zerobus_test_runner.scenario import Scenario, _MockRecorder, formats

PROTO_HEX = "0801120568656c6c6f"  # independent reference encoding of {"id":1,"message":"hello"}
PLAN = Plan("rust", "grpc", "proto", Classification(None))


def _run(body, harness, mock, fmt="proto", transport="grpc"):
    sc = Scenario("t", body, None)
    ex = Executor(harness=harness, mock=mock, transport=transport, fmt=fmt, server_endpoint="127.0.0.1:1")
    return asyncio.run(ex.run(sc.steps))


def _happy(s):
    st = s.sdk.create_stream(schema="simple_record")
    for i in range(5):
        s.sdk.ingest(st, {"id": i, "message": f"m{i}"})
    s.sdk.flush(st)
    s.sdk.close(st)
    s.expect(s.mock.ack_offset(st), equals=4)
    s.expect(s.mock.captured_record_count(st), equals=5)
    s.expect(s.mock.on_completed_count(st), equals=1)
    s.expect(s.mock.cancel_count(st), equals=0)


def test_happy_path_is_queue_only_then_one_flush():
    h = FakeHarness()
    m = FakeMock(ack_offset=4, captured_record_count=5, on_completed_count=1, cancel_count=0)
    checks = _run(_happy, h, m)
    names = [c[0] for c in h.calls]
    assert names == ["create_stream"] + ["ingest_record"] * 5 + ["flush", "close_stream"]
    assert "await_write_completed" not in names  # the performance rule: never wait per record
    assert all(c.passed for c in checks)


def test_expect_evaluates_to_fail_with_expected_and_actual():
    m = FakeMock(ack_offset=3, captured_record_count=5, on_completed_count=1, cancel_count=0)  # off by one
    failed = [c for c in _run(_happy, FakeHarness(), m) if not c.passed]
    assert len(failed) == 1 and failed[0].expected == 4 and failed[0].actual == 3


def test_batch_ingest_returns_one_offset_and_queries_mock_globally():
    def body(s):
        st = s.sdk.create_stream(schema="simple_record")
        r = s.sdk.ingest_many(st, [{"id": i, "message": f"m{i}"} for i in range(3)])
        s.sdk.flush(st)
        s.expect(s.mock.ack_offset(st), equals=r.offset)  # the batch's single offset
        s.expect(s.mock.captured_record_count(st), equals=3)

    h = FakeHarness()
    m = FakeMock(ack_offset=0, captured_record_count=3)  # batch gets one offset
    checks = _run(body, h, m)
    # One IngestRecords RPC carries the whole batch; cleanup closes the unclosed stream.
    assert [c[0] for c in h.calls] == ["create_stream", "ingest_records", "flush", "close_stream"]
    assert h.calls[1] == ("ingest_records", "stream-A", [{"id": i, "message": f"m{i}"} for i in range(3)])
    assert all(c.passed for c in checks)
    # Q2/B: the per-stream DSL ref collapses to the global mock query ("").
    assert ("ack_offset", {"stream_id": ""}) in m.calls


def test_offset_refs_resolve_to_harness_returned_offsets():
    def body(s):
        st = s.sdk.create_stream(schema="simple_record")
        r = s.sdk.ingest(st, {"id": 1, "message": "hello"})
        s.expect(s.mock.captured_record_bytes(st, r.offset), equals=bytes.fromhex(PROTO_HEX))

    m = FakeMock(captured_record_bytes=lambda stream_id, offset: bytes.fromhex(PROTO_HEX) if offset == 0 else b"")
    checks = _run(body, FakeHarness(), m)
    assert len(checks) == 1 and checks[0].passed
    assert ("captured_record_bytes", {"stream_id": "", "offset": 0}) in m.calls


def test_sdk_error_through_await_is_one_fail_not_two():
    def body(s):
        st = s.sdk.create_stream(schema="simple_record")
        r = s.sdk.ingest(st, {"id": 1, "message": "x"})
        s.sdk.await_write(r)
        s.expect(s.mock.ack_offset(st), equals=r.offset)

    h = FakeHarness(sdk_fault_on="await_write_completed", retryable=True, message="deadline exceeded")
    checks = _run(body, h, FakeMock(ack_offset=0))
    sdk_errors = [c for c in checks if not c.passed and c.actual == "deadline exceeded"]
    assert len(sdk_errors) == 1  # the SDK error surfaces exactly once


def test_infra_error_from_harness_becomes_error_state():
    res = asyncio.run(
        _run_one(Scenario("t", _happy, None), PLAN, FakeHarness(fail_on="ingest_record"), FakeMock(), "127.0.0.1:1")
    )
    assert res.state is State.ERROR and "UNAVAILABLE" in res.detail


def _terminal_body(s):
    s.mock.inject_stream_error(retryable=False)
    st = s.sdk.create_stream(schema="simple_record")
    s.sdk.ingest(st, {"id": 1, "message": "x"})
    s.sdk.flush(st)
    s.expect_terminal_failure(st)


@pytest.mark.parametrize(
    "harness, passes",
    [
        (FakeHarness(sdk_fault_on="flush", retryable=False), True),  # terminal SDK error -> pass
        (FakeHarness(sdk_fault_on="flush", retryable=True), False),  # retryable is not terminal
        (FakeHarness(), False),  # no error at all
    ],
)
def test_expect_terminal_failure_only_on_nonretryable_sdk_error(harness, passes):
    checks = _run(_terminal_body, harness, FakeMock())
    assert len(checks) == 1 and checks[0].passed is passes


def test_harness_crash_does_not_count_as_terminal_failure():
    # A harness crash is an infra fault (ERROR), never a satisfied expect_terminal_failure.
    res = asyncio.run(
        _run_one(Scenario("t", _terminal_body, None), PLAN, FakeHarness(fail_on="flush"), FakeMock(), "127.0.0.1:1")
    )
    assert res.state is State.ERROR


def test_actions_after_a_failure_are_skipped():
    def body(s):
        st = s.sdk.create_stream(schema="simple_record")
        s.sdk.ingest(st, {"id": 1, "message": "x"})
        s.sdk.flush(st)
        s.sdk.close(st)
        s.expect_terminal_failure(st)

    h = FakeHarness(sdk_fault_on="ingest_record", retryable=False)
    checks = _run(body, h, FakeMock())
    names = [c[0] for c in h.calls]
    assert "flush" not in names  # the flush step is skipped once the stream is dead
    assert names.count("close_stream") == 1  # the close step is skipped; cleanup closes the dead stream once
    assert len(checks) == 1 and checks[0].passed  # the terminal failure was observed


def test_open_stream_is_closed_at_end_of_run():
    def body(s):
        st = s.sdk.create_stream(schema="simple_record")
        s.sdk.ingest(st, {"id": 1, "message": "x"})
        s.sdk.flush(st)  # no explicit close
        s.expect(s.mock.ack_offset(st), equals=0)

    h = FakeHarness()
    _run(body, h, FakeMock(ack_offset=0))
    assert ("close_stream", "stream-A") in h.calls  # cleanup closed the leaked stream


def test_every_mock_query_is_a_client_method():
    queries = {n for n in dir(_MockRecorder) if not n.startswith("_") and n != "inject_stream_error"}
    assert queries <= set(dir(MockAdminClient))


def test_expansion_pins_and_enumerates_only_valid_tuples():
    sc = Scenario("t", _happy, None)
    unrestricted = expand(sc, "rust")
    assert [(p.transport, p.fmt) for p in unrestricted] == [
        ("grpc", "proto"),
        ("grpc", "json"),
        ("grpc", "avro"),
        ("arrow_flight", "arrow"),
    ]
    states = {(p.transport, p.fmt): p.classification for p in unrestricted}
    assert states[("grpc", "proto")].runs
    assert states[("arrow_flight", "arrow")].runs  # arrow is wired for rust

    pinned_invalid = expand(sc, "rust", "arrow_flight", "avro")
    assert pinned_invalid[0].classification.state is State.INVALID


def test_guard_rail_fires_for_dead_matrix_without_error():
    # rust has wired tuples; if none produced PASS/FAIL (and none ERRORed), the gate is dead.
    sc = Scenario("t", _happy, None)
    dead = [RunResult(Plan("rust", "grpc", "proto", Classification(State.NA, "x")), State.NA, "x")]
    assert guard_rail_error(sc, "rust", dead, unrestricted=True)
    assert guard_rail_error(sc, "rust", dead, unrestricted=False) is None  # restricted: no guard


def test_guard_rail_silent_on_error_matrix():
    # An all-ERROR matrix is an infra fault, already explained by its own row; not a dead gate.
    sc = Scenario("t", _happy, None)
    errored = [RunResult(Plan("rust", "grpc", "proto", Classification(None)), State.ERROR, "boom")]
    assert guard_rail_error(sc, "rust", errored, unrestricted=True) is None


def test_guard_rail_silent_for_sdk_with_no_wired_tuples():
    # go has no wired tuples, so an all-NOT_WIRED matrix is not a dead gate (Teodor's false positive).
    sc = Scenario("t", _happy, None)
    unwired = [RunResult(Plan("go", "grpc", "proto", Classification(State.NOT_WIRED, "x")), State.NOT_WIRED, "x")]
    assert guard_rail_error(sc, "go", unwired, unrestricted=True) is None
    ran = [RunResult(Plan("rust", "grpc", "proto", Classification(None)), State.PASS)]
    assert guard_rail_error(sc, "rust", ran, unrestricted=True) is None


def test_create_fault_does_not_satisfy_terminal_failure():
    # A non-retryable error from create_stream must not count as the injected terminal failure.
    checks = _run(_terminal_body, FakeHarness(sdk_fault_on="create_stream", retryable=False), FakeMock())
    labels = [c.description for c in checks]
    assert any("created" in lbl and not c.passed for lbl, c in zip(labels, checks))  # create FAIL recorded
    terminal = [c for c in checks if "fails terminally" in c.description]
    assert terminal and not terminal[0].passed  # terminal expectation not satisfied by a create error


def test_expect_on_faulted_stream_is_fail_not_error():
    # ingest fails; the later expect on r.offset cannot resolve -> FAIL with the SDK message, not ERROR.
    def body(s):
        st = s.sdk.create_stream(schema="simple_record")
        r = s.sdk.ingest(st, {"id": 1, "message": "x"})
        s.expect(s.mock.ack_offset(st), equals=r.offset)

    h = FakeHarness(sdk_fault_on="ingest_record", retryable=False, message="ingest rejected")
    res = asyncio.run(_run_one(Scenario("t", body, None), PLAN, h, FakeMock(ack_offset=0), "127.0.0.1:1"))
    assert res.state is State.FAIL  # not ERROR
    assert any("stream faulted" in c.description and c.actual == "ingest rejected" for c in res.checks)


def test_error_keeps_partial_checks():
    # A FAIL check is recorded, then a mock RPC fails -> ERROR, but the FAIL check is retained.
    from zerobus_test_runner.errors import MockError

    def body(s):
        st = s.sdk.create_stream(schema="simple_record")
        s.sdk.ingest(st, {"id": 1, "message": "x"})
        s.sdk.flush(st)
        s.expect(s.mock.captured_record_count(st), equals=1)  # FAIL (mock returns 0)
        s.expect(s.mock.ack_offset(st), equals=0)  # this query raises MockError

    class BoomOnAck(FakeMock):
        async def ack_offset(self, **kw):
            raise MockError("mock.GetAckOffset failed: UNAVAILABLE")

    m = BoomOnAck(captured_record_count=0)
    res = asyncio.run(_run_one(Scenario("t", body, None), PLAN, FakeHarness(), m, "127.0.0.1:1"))
    assert res.state is State.ERROR
    assert any(not c.passed and "captured_record_count" in c.description for c in res.checks)


def test_captured_record_bytes_missing_is_fail():
    # The mock returning None (no record captured) is a FAIL, not ERROR.
    def body(s):
        st = s.sdk.create_stream(schema="simple_record")
        r = s.sdk.ingest(st, {"id": 1, "message": "x"})
        s.expect(s.mock.captured_record_bytes(st, r.offset), equals=b"\x00")

    checks = _run(body, FakeHarness(), FakeMock(captured_record_bytes=None))
    assert len(checks) == 1 and not checks[0].passed and checks[0].actual is None
