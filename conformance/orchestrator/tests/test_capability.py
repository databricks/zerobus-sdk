from zerobus_test_runner import formats
from zerobus_test_runner.capability import State, classify, valid_tuples

PROTO_ONLY = formats("proto")
AVRO_ONLY = formats("avro")
SIMPLE = frozenset({"proto", "json", "avro"})


def test_valid_tuples_are_protocol_truth():
    assert valid_tuples() == [
        ("grpc", "proto"),
        ("grpc", "json"),
        ("grpc", "avro"),
        ("arrow_flight", "arrow"),
    ]


def test_gate_order_and_terminal_states():
    # Gate 1: avro over arrow_flight is INVALID regardless of SDK.
    assert classify("rust", "arrow_flight", "avro", None, SIMPLE).state is State.INVALID
    # Gate 2: go does not support avro.
    assert classify("go", "grpc", "avro", None, SIMPLE).state is State.UNSUPPORTED
    # Gate 3: scenario gate excludes avro.
    assert classify("rust", "grpc", "avro", PROTO_ONLY, SIMPLE).state is State.NA
    # Gate 3: schema without an arrow artifact.
    assert classify("rust", "arrow_flight", "arrow", None, SIMPLE).state is State.NA
    # Wiring: valid, supported, applicable, but harness not implemented for go.
    c = classify("go", "grpc", "proto", None, SIMPLE)
    assert c.state is State.UNSUPPORTED and "not wired" in c.reason
    # The one wired tuple runs.
    assert classify("rust", "grpc", "proto", PROTO_ONLY, SIMPLE).runs
