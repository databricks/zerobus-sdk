import pytest

from zerobus_test_runner import formats, golden, scenario
from zerobus_test_runner.scenario import (
    REGISTRY,

    CreateStream,
    Expect,
    Flush,
    Gate,
    Ingest,
    Scenario,
    ScenarioError,
    Close,
    StreamRef,
    RecordRef,
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

def test_formats_and_golden_argument_checks():
    assert formats("proto").formats == frozenset({"proto"})
    with pytest.raises(ScenarioError):
        formats("protoo")
    with pytest.raises(ScenarioError):
        formats()
    with pytest.raises(ScenarioError):
        golden("simple_record", {"id": 1, "message": "x"}, "json")  # no golden encoder yet

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

def test_validator_flags_dead_gate_and_missing_golden():
    def dead(s):  # arrow is valid only on arrow_flight, so (grpc, arrow) can never run
        st = s.sdk.create_stream(schema="simple_record")
        s.expect(s.mock.call_count(), equals=0)
        del st

    dead_gate = Gate(formats=frozenset({"arrow"}), transports=frozenset({"grpc"}))
    probs = validate(_make(dead, applies=dead_gate))
    assert any("dead or mistyped gate" in p for p in probs), probs

    def bad_golden(s):
        st = s.sdk.create_stream(schema="simple_record")
        s.sdk.flush(st)
        # no committed golden exists for this record (only {"id":1,"message":"hello"} is frozen)
        s.expect(s.mock.call_count(), equals=golden("simple_record", {"id": 1}, "avro"))

    probs = validate(_make(bad_golden))
    assert any("unavailable" in p and "regen_goldens.py" in p for p in probs), probs

def test_offset_ref_to_ingested_record_is_valid():
    def ok_then_bad(s):
        st = s.sdk.create_stream(schema="simple_record")
        r = s.sdk.ingest(st, {"id": 1, "message": "x"})
        s.sdk.flush(st)
        s.expect(s.mock.captured_record_bytes(st, r.offset), equals=b"\x00")

    assert validate(_make(ok_then_bad)) == []
