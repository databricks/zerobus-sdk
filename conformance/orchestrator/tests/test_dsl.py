import pytest

from zerobus_test_runner import formats, scenario
from zerobus_test_runner.scenario import (
    REGISTRY,
    Close,
    CreateStream,
    Expect,
    Flush,
    Gate,
    Ingest,
    RecordRef,
    Scenario,
    ScenarioError,
    StreamRef,
    gate,
    validate,
)


def _make(fn, applies=None):
    return Scenario(fn.__name__, fn, applies)


def test_recording_does_not_execute_and_keeps_order():
    calls = []

    def body(s):
        calls.append("body")  # would only be seen if the body ran
        st = s.sdk.create_stream(schema="simple_record")
        last = None
        for i in range(3):
            last = s.sdk.ingest(st, {"id": i, "message": f"m{i}"})
        s.sdk.flush(st)
        s.sdk.close(st)
        s.expect(s.mock.ack_offset(st), equals=last.offset)

    sc = _make(body)
    steps = sc.steps
    assert calls == ["body"]  # body ran exactly once, during recording
    assert [type(x) for x in steps] == [CreateStream, Ingest, Ingest, Ingest, Flush, Close, Expect]
    assert isinstance(steps[0].out, StreamRef)
    assert isinstance(steps[1].out, RecordRef)
    assert steps[-1].equals is steps[3].out.offset  # symbolic ref, resolved later
    assert validate(sc) == []


def test_formats_argument_checks():
    assert formats("proto").formats == frozenset({"proto"})
    with pytest.raises(ScenarioError):
        formats("protoo")
    with pytest.raises(ScenarioError):
        formats()


def test_scenario_decorator_rejects_bad_gate_and_registers_name():
    with pytest.raises(ScenarioError):
        scenario(applies="proto")  # not a Gate

    def unique_name_scenario_xyz(s):
        pass

    try:
        scenario()(unique_name_scenario_xyz)
        assert "unique_name_scenario_xyz" in REGISTRY
    finally:
        REGISTRY.pop("unique_name_scenario_xyz", None)


def test_refs_are_identity_based_and_owned_by_one_recording():
    def a(s):
        s.sdk.create_stream(schema="simple_record")

    def b(s):
        st = s.sdk.create_stream(schema="simple_record")
        s.sdk.ingest(st, {"id": 1, "message": "x"})

    sb = _make(b)
    foreign = sb.steps[0].out

    # Using a stream from scenario b inside scenario a's recorder must be rejected at record time.
    def bad(s):
        s.sdk.flush(foreign)

    with pytest.raises(ScenarioError):
        _make(bad).steps


def test_validator_catches_use_before_define_and_closed_ingest():
    def use_before(s):
        st = s.sdk.create_stream(schema="simple_record")
        s.sdk.close(st)
        s.sdk.ingest(st, {"id": 1, "message": "x"})  # ingest after close
        s.expect(s.mock.call_count(), equals=0)

    probs = validate(_make(use_before))
    assert any("ingest after close" in p for p in probs), probs

    def no_expect(s):
        st = s.sdk.create_stream(schema="simple_record")
        s.sdk.flush(st)

    assert any("no s.expect" in p for p in validate(_make(no_expect)))


def test_validator_flags_dead_gate():
    def dead(s):  # arrow is valid only on arrow_flight, so (grpc, arrow) can never run
        st = s.sdk.create_stream(schema="simple_record")
        s.expect(s.mock.call_count(), equals=0)
        del st

    dead_gate = Gate(formats=frozenset({"arrow"}), transports=frozenset({"grpc"}))
    probs = validate(_make(dead, applies=dead_gate))
    assert any("dead or mistyped gate" in p for p in probs), probs


def test_offset_ref_to_ingested_record_is_valid():
    def ok_then_bad(s):
        st = s.sdk.create_stream(schema="simple_record")
        r = s.sdk.ingest(st, {"id": 1, "message": "x"})
        s.sdk.flush(st)
        s.expect(s.mock.captured_record_bytes(st, r.offset), equals=b"\x00")

    # captured_record_bytes is gRPC-only, so the scenario must gate out arrow_flight.
    assert validate(_make(ok_then_bad, applies=gate(transports=["grpc"]))) == []


def test_duplicate_scenario_name_rejected():
    def first(s):
        pass

    def second(s):  # a different function defined on a different line
        pass

    first.__name__ = first.__qualname__ = "dup_name_zzz"
    second.__name__ = second.__qualname__ = "dup_name_zzz"
    scenario()(first)
    try:
        with pytest.raises(ScenarioError, match="duplicate"):
            scenario()(second)
    finally:
        REGISTRY.pop("dup_name_zzz", None)


def test_recordref_as_expected_value_rejected():
    def bad(s):
        st = s.sdk.create_stream(schema="simple_record")
        r = s.sdk.ingest(st, {"id": 1, "message": "x"})
        s.sdk.flush(st)
        s.expect(s.mock.ack_offset(st), equals=r)  # should be r.offset, not the record

    assert any("RecordRef" in p for p in validate(_make(bad)))


def test_cross_stream_offset_rejected():
    def bad(s):
        a = s.sdk.create_stream(schema="simple_record")
        b = s.sdk.create_stream(schema="simple_record")
        ra = s.sdk.ingest(a, {"id": 1, "message": "x"})
        s.sdk.flush(a)
        s.sdk.flush(b)
        s.expect(s.mock.ack_offset(b), equals=ra.offset)  # offset from stream a

    assert any("different stream" in p for p in validate(_make(bad)))


def test_double_close_and_flush_after_close_flagged():
    def bad(s):
        st = s.sdk.create_stream(schema="simple_record")
        s.sdk.close(st)
        s.sdk.close(st)
        s.sdk.flush(st)
        s.expect(s.mock.call_count(), equals=1)

    probs = validate(_make(bad))
    assert any("close after close" in p for p in probs), probs
    assert any("flush after close" in p for p in probs), probs


def test_create_stream_rejects_empty_schema():
    with pytest.raises(ScenarioError):
        _make(lambda s: s.sdk.create_stream(schema="")).steps


def test_await_write_rejects_bool_timeout():
    def bad(s):
        st = s.sdk.create_stream(schema="simple_record")
        r = s.sdk.ingest(st, {"id": 1, "message": "x"})
        s.sdk.await_write(r, timeout_ms=True)

    with pytest.raises(ScenarioError):
        _make(bad).steps


def test_gate_transports_rejects_bare_string():
    with pytest.raises(ScenarioError):
        gate(transports="grpc")


def test_gate_formats_and_sdks_reject_bare_string():
    with pytest.raises(ScenarioError):
        gate(formats="proto")
    with pytest.raises(ScenarioError):
        gate(sdks="rust")


def test_one_create_stream_per_scenario():
    def two(s):
        a = s.sdk.create_stream(schema="simple_record")
        b = s.sdk.create_stream(schema="simple_record")
        s.sdk.ingest(a, {"id": 1, "message": "x"})
        s.sdk.flush(a)
        s.expect(s.mock.captured_record_count(a), equals=1)

    assert any("only one s.sdk.create_stream" in p for p in validate(_make(two)))


def test_captured_record_bytes_rejected_for_arrow():
    def bad(s):
        st = s.sdk.create_stream(schema="simple_record")
        r = s.sdk.ingest(st, {"id": 1, "message": "x"})
        s.sdk.flush(st)
        s.expect(s.mock.captured_record_bytes(st, r.offset), equals=b"\x00")

    # Ungated: admits arrow_flight tuples, where captured_record_bytes returns nothing.
    assert any("gRPC-only" in p for p in validate(_make(bad)))


def test_offset_assert_after_retryable_inject_flagged():
    def bad(s):
        s.mock.inject_stream_error(retryable=True)
        st = s.sdk.create_stream(schema="simple_record")
        r = s.sdk.ingest(st, {"id": 1, "message": "x"})
        s.sdk.flush(st)
        s.expect(s.mock.ack_offset(st), equals=r.offset)

    assert any("assert on counts" in p for p in validate(_make(bad)))


def test_abort_count_target_usable():
    def ok(s):
        st = s.sdk.create_stream(schema="simple_record")
        s.sdk.ingest(st, {"id": 1, "message": "x"})
        s.sdk.flush(st)
        s.expect(s.mock.abort_count(st), equals=0)

    assert validate(_make(ok)) == []


def test_terminal_failure_primitive():
    def ok(s):
        s.mock.inject_stream_error(retryable=False)
        st = s.sdk.create_stream(schema="simple_record")
        s.sdk.ingest(st, {"id": 1, "message": "x"})
        s.sdk.flush(st)
        s.expect_terminal_failure(st)

    assert validate(_make(ok)) == []

    def missing(s):
        s.mock.inject_stream_error(retryable=False)
        st = s.sdk.create_stream(schema="simple_record")
        s.sdk.ingest(st, {"id": 1, "message": "x"})
        s.sdk.flush(st)
        s.expect(s.mock.call_count(), equals=1)

    assert any("expect_terminal_failure" in p for p in validate(_make(missing)))


def test_load_scenarios_handles_dataclass_future_annotations(tmp_path):
    from zerobus_test_runner.scenario import load_scenarios

    (tmp_path / "dc.py").write_text(
        "from __future__ import annotations\n"
        "from dataclasses import dataclass\n"
        "from zerobus_test_runner import scenario\n"
        "\n"
        "@dataclass\n"
        "class Helper:\n"
        "    x: int\n"
        "\n"
        "@scenario()\n"
        "def dc_scenario_xyz(s):\n"
        "    s.sdk.create_stream(schema='simple_record')\n"
        "    s.expect(s.mock.call_count(), equals=1)\n"
    )
    try:
        result = load_scenarios(tmp_path)
        assert "dc_scenario_xyz" in result
    finally:
        REGISTRY.pop("dc_scenario_xyz", None)
