from zerobus_test_runner import formats
from zerobus_test_runner.capability import State, classify, is_supported, valid_tuples

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
    # A wired tuple runs.
    assert classify("rust", "grpc", "proto", PROTO_ONLY, SIMPLE).runs


def test_capability_table_matches_shipped_sdks():
    # avro ships on rust/python/typescript/java (not go/purego/cpp/dotnet).
    assert is_supported("typescript", "grpc", "avro")
    assert is_supported("java", "grpc", "avro")
    assert not is_supported("go", "grpc", "avro")
    assert not is_supported("purego", "grpc", "avro")
    assert not is_supported("dotnet", "grpc", "avro")
    # Arrow Flight ships on go/cpp too (not purego/dotnet).
    assert is_supported("go", "arrow_flight", "arrow")
    assert is_supported("cpp", "arrow_flight", "arrow")
    assert not is_supported("purego", "arrow_flight", "arrow")
    assert not is_supported("dotnet", "arrow_flight", "arrow")


def test_supported_but_unwired_reports_not_wired_not_unsupported():
    # typescript ships avro, but the harness does not wire it yet: report "not wired (TODO)",
    # not the false "does not support".
    c = classify("typescript", "grpc", "avro", None, frozenset({"avro"}))
    assert c.state is State.UNSUPPORTED and "not wired" in c.reason
    # go ships Arrow Flight but has no harness yet.
    c2 = classify("go", "arrow_flight", "arrow", None, frozenset({"arrow"}))
    assert c2.state is State.UNSUPPORTED and "not wired" in c2.reason
