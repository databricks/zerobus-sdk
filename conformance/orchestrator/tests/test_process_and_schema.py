import asyncio
import sys
import textwrap

import pytest

from zerobus_test_runner import process
from zerobus_test_runner.data.schemas import load_schema, supported_formats
from zerobus_test_runner.errors import InfraError


def _py(code: str):
    return [sys.executable, "-c", textwrap.dedent(code)]


def test_endpoint_line_is_parsed_and_teardown_terminates(tmp_path):
    async def go():
        argv = _py("""
            import sys, time
            print("noise before endpoint", flush=True)
            print("MOCK_ENDPOINT=127.0.0.1:4567", flush=True)
            time.sleep(60)
        """)
        p = await process.launch(argv, name="fake", endpoint_prefix=process.MOCK_PREFIX, log_dir=tmp_path)
        assert p.endpoint == "127.0.0.1:4567"
        await p.stop(shutdown=None, grace_s=0.5)  # no Shutdown RPC: SIGTERM must end it
        return p.proc.returncode

    rc = asyncio.run(go())
    assert rc is not None and rc != 0
    assert "noise before endpoint" in (tmp_path / "fake.log").read_text()


def test_teardown_escalates_to_kill_when_sigterm_is_ignored(tmp_path):
    async def go():
        argv = _py("""
            import signal, time
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            print("HARNESS_ENDPOINT=127.0.0.1:1", flush=True)
            time.sleep(60)
        """)
        p = await process.launch(argv, name="stubborn", endpoint_prefix=process.HARNESS_PREFIX, log_dir=tmp_path)
        await p.stop(shutdown=None, grace_s=0.3)
        return p.proc.returncode

    assert asyncio.run(go()) == -9


def test_process_exiting_without_endpoint_is_infra_error(tmp_path):
    async def go():
        argv = _py("import sys; print('boom', file=sys.stderr); sys.exit(3)")
        await process.launch(argv, name="dies", endpoint_prefix=process.MOCK_PREFIX, log_dir=tmp_path)

    with pytest.raises(InfraError, match="exited before printing"):
        asyncio.run(go())


def test_malformed_endpoint_is_rejected():
    with pytest.raises(InfraError):
        process.parse_endpoint("localhost")
    assert process.parse_endpoint("127.0.0.1:9") == "127.0.0.1:9"


def test_simple_record_schema_artifacts():
    s = load_schema("simple_record")
    assert {"proto", "avro", "json"} <= set(s.formats) and "arrow" not in s.formats
    assert s.proto_full_name == "zerobus.conformance.fixtures.SimpleRecord"
    assert s.descriptor_proto_bytes  # DescriptorProto, not the FileDescriptorSet
    assert '"SimpleRecord"' in s.avro_schema_json


def test_unknown_schema_is_infra_error():
    with pytest.raises(InfraError):
        supported_formats("no_such_schema")


def test_generated_stubs_import_as_package():
    from zerobus_test_runner._generated import mock_admin_pb2_grpc, test_harness_pb2_grpc

    assert hasattr(test_harness_pb2_grpc, "TestHarnessStub")
    assert hasattr(mock_admin_pb2_grpc, "MockAdminStub")
