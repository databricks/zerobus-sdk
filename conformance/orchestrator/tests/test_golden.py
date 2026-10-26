import pytest

from zerobus_test_runner.data.golden import (
    GoldenMissing,
    encode_golden_for,
    golden_path,
    golden_relpath,
    read_golden,
)
from zerobus_test_runner.data.schemas import schema_dir

RECORD = {"id": 1, "message": "hello"}


def test_committed_goldens_are_the_reviewed_bytes():
    # Hand-derived from the wire format: proto = field1 varint 1 + field2 len-5 "hello";
    # avro = zigzag long 1, zigzag string length 5, then the UTF-8 bytes.
    assert read_golden("simple_record", RECORD, "proto") == bytes.fromhex("08 01 12 05 68 65 6c 6c 6f")
    assert read_golden("simple_record", RECORD, "avro") == bytes.fromhex("02 0a 68 65 6c 6c 6f")


def test_golden_is_keyed_by_canonical_record_and_format():
    # Key order must not matter; the format selects the directory.
    assert golden_relpath({"message": "hello", "id": 1}, "proto") == golden_relpath(RECORD, "proto")
    assert golden_relpath(RECORD, "proto").parts[0] == "golden"
    assert golden_relpath(RECORD, "proto").parts[1] == "proto"
    assert golden_relpath(RECORD, "proto") != golden_relpath(RECORD, "avro")


def test_reader_does_not_encode_it_returns_the_file_on_disk():
    path = golden_path("simple_record", RECORD, "avro")
    assert read_golden("simple_record", RECORD, "avro") == path.read_bytes()


def test_missing_golden_names_the_file_and_the_regen_command():
    with pytest.raises(GoldenMissing) as exc:
        read_golden("simple_record", {"id": 999, "message": "absent"}, "avro")
    msg = str(exc.value)
    assert "regen_goldens.py" in msg
    assert str(schema_dir("simple_record") / "golden") in msg


def test_regenerating_from_the_encoders_reproduces_the_committed_bytes():
    # The independent encoders must still agree with the frozen constants (drift detector).
    assert encode_golden_for("simple_record", RECORD, "proto") == read_golden("simple_record", RECORD, "proto")
    assert encode_golden_for("simple_record", RECORD, "avro") == read_golden("simple_record", RECORD, "avro")
