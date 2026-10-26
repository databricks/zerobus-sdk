import asyncio

from zerobus_test_runner.capability import Classification, State
from zerobus_test_runner.paths import SCENARIOS_DIR
from zerobus_test_runner.runner import Plan, RunResult, _run_one, expand, guard_rail_error
from zerobus_test_runner.scenario import REGISTRY, Executor, Scenario, load_scenarios

from tests.fakes import FakeHarness, FakeMock

PROTO_HEX = "0801120568656c6c6f"  # independent reference encoding of {"id":1,"message":"hello"}
AVRO_HEX = "020a68656c6c6f"


def _scenarios():
    load_scenarios(SCENARIOS_DIR)
    return REGISTRY


def _run(sc, harness, mock, fmt="proto", transport="grpc"):
    ex = Executor(harness=harness, mock=mock, transport=transport, fmt=fmt, server_endpoint="127.0.0.1:1")
    return asyncio.run(ex.run(sc.steps))


def test_happy_path_is_queue_only_then_one_flush():
    sc = _scenarios()["happy_path_ingest"]
    h = FakeHarness()
    m = FakeMock(ack_offset=4, captured_record_count=5, on_completed_count=1, cancel_count=0)
    checks = _run(sc, h, m)
    names = [c[0] for c in h.calls]
    assert names == ["create_stream"] + ["ingest_record"] * 5 + ["flush", "close_stream"]
    assert "await_write_completed" not in names  # the performance rule: never wait per record
    assert [c.description.split(" == ")[0] for c in checks] == [
        "mock.ack_offset", "mock.captured_record_count", "mock.on_completed_count", "mock.cancel_count",
    ]
    assert all(c.passed for c in checks)


def test_expect_evaluates_to_fail_with_expected_and_actual():
    sc = _scenarios()["happy_path_ingest"]
    h = FakeHarness()
    m = FakeMock(ack_offset=3, captured_record_count=5, on_completed_count=1, cancel_count=0)  # off by one
    failed = [c for c in _run(sc, h, m) if not c.passed]
    assert len(failed) == 1
    assert failed[0].expected == 4 and failed[0].actual == 3


def test_offset_refs_resolve_to_harness_returned_offsets():
    sc = _scenarios()["encoded_bytes_proto"]
    h = FakeHarness()
    m = FakeMock(captured_record_bytes=lambda stream_id, offset: bytes.fromhex(PROTO_HEX) if offset == 0 else b"")
    checks = _run(sc, h, m)
    assert len(checks) == 1 and checks[0].passed
    assert m.calls[-1] == ("captured_record_bytes", {"stream_id": "stream-A", "offset": 0})


def test_golden_bytes_pass_and_mismatch_fails_on_avro():
    sc = _scenarios()["encoded_bytes_avro"]
    good = _run(sc, FakeHarness(), FakeMock(captured_record_bytes=bytes.fromhex(AVRO_HEX)), fmt="avro")
    assert good[0].passed
    bad = _run(sc, FakeHarness(), FakeMock(captured_record_bytes=bytes.fromhex(PROTO_HEX)), fmt="avro")
    assert not bad[0].passed


def test_await_error_is_an_assertion_fail_not_infra():
    def body(s):
        st = s.sdk.create_stream(schema="simple_record")
        r = s.sdk.ingest(st, {"id": 1, "message": "x"})
        s.sdk.await_write(r, timeout_ms=500)
        s.expect(s.mock.ack_offset(st), equals=r.offset)

    sc = Scenario("await_case", body, None)
    checks = _run(sc, FakeHarness(await_error="deadline exceeded"), FakeMock(ack_offset=0))
    assert not checks[0].passed and checks[0].actual == "deadline exceeded"


def test_infra_error_from_harness_becomes_error_state():
    sc = _scenarios()["happy_path_ingest"]
    plan = Plan("rust", "grpc", "proto", Classification(None))
    res = asyncio.run(_run_one(sc, plan, FakeHarness(fail_on="ingest_record"), FakeMock(), "127.0.0.1:1"))
    assert res.state is State.ERROR and "UNAVAILABLE" in res.detail


def test_expansion_pins_and_enumerates_only_valid_tuples():
    sc = _scenarios()["happy_path_ingest"]
    unrestricted = expand(sc, "rust")
    assert [(p.transport, p.fmt) for p in unrestricted] == [
        ("grpc", "proto"), ("grpc", "json"), ("grpc", "avro"), ("arrow_flight", "arrow"),
    ]
    states = {(p.transport, p.fmt): p.classification for p in unrestricted}
    assert states[("grpc", "proto")].runs
    assert states[("arrow_flight", "arrow")].runs  # arrow is wired for rust

    pinned_invalid = expand(sc, "rust", "arrow_flight", "avro")
    assert pinned_invalid[0].classification.state is State.INVALID


def test_guard_rail_fires_only_for_unrestricted_dead_matrix():
    sc = _scenarios()["encoded_bytes_proto"]
    none_ran = [RunResult(Plan("go", "grpc", "proto", Classification(None)), State.ERROR, "x")]
    assert guard_rail_error(sc, "go", none_ran, unrestricted=True)
    assert guard_rail_error(sc, "go", none_ran, unrestricted=False) is None
    ran = [RunResult(Plan("rust", "grpc", "proto", Classification(None)), State.PASS)]
    assert guard_rail_error(sc, "rust", ran, unrestricted=True) is None
