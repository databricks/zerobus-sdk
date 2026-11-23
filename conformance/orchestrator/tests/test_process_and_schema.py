import asyncio
import sys
import textwrap

import pytest

from zerobus_test_runner import process
from zerobus_test_runner.data.schemas import load_schema, supported_formats
from zerobus_test_runner.errors import InfraError
from zerobus_test_runner.scenario import Scenario, ScenarioError


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


def test_launch_spec_reports_missing_artifact_with_build_hint(tmp_path, monkeypatch):
    import json as _json

    sdk_dir = tmp_path / "typescript"
    sdk_dir.mkdir()
    (sdk_dir / "launch.json").write_text(
        _json.dumps(
            {
                "argv": ["node", "{harness_dir}/dist/main.js"],
                "artifact": "{harness_dir}/dist/main.js",
                "build_hint": "npm run build",
            }
        )
    )
    monkeypatch.setattr(process, "HARNESS_DIR", tmp_path)

    async def go():
        await process.build_and_launch_harness("typescript", tmp_path)

    with pytest.raises(InfraError, match="npm run build"):
        asyncio.run(go())


def test_malformed_endpoint_is_rejected():
    with pytest.raises(InfraError):
        process.parse_endpoint("localhost")
    assert process.parse_endpoint("127.0.0.1:9") == "127.0.0.1:9"


def test_simple_record_schema_artifacts():
    s = load_schema("simple_record")
    assert {"proto", "avro", "json", "arrow"} <= set(s.formats)
    assert s.descriptor_proto_bytes  # DescriptorProto, not the FileDescriptorSet
    assert '"SimpleRecord"' in s.avro_schema_json
    assert s.arrow_schema_ipc  # Arrow IPC schema for the arrow_flight path


def test_schema_files_agree_on_field_names_and_order():
    import json as _json

    from google.protobuf import descriptor_pb2

    from zerobus_test_runner.data.schemas import schema_dir

    art = load_schema("simple_record")
    proto_fields = [f.name for f in descriptor_pb2.DescriptorProto.FromString(art.descriptor_proto_bytes).field]
    avro_fields = [f["name"] for f in _json.loads(art.avro_schema_json)["fields"]]
    arrow_fields = [
        f["name"] for f in _json.loads((schema_dir("simple_record") / "schema.arrow.json").read_text())["fields"]
    ]
    assert proto_fields == avro_fields == arrow_fields


def test_exit_ok_truth_table():
    from zerobus_test_runner.capability import Classification, State
    from zerobus_test_runner.runner import Plan, RunResult, exit_ok

    def results(*states):
        return [RunResult(Plan("rust", "grpc", "proto", Classification(None)), s) for s in states]

    assert exit_ok(results(State.PASS, State.NA), None) is True
    assert exit_ok(results(State.NA), None) is False  # nothing actually ran
    assert exit_ok(results(State.PASS, State.INVALID), None) is False
    assert exit_ok(results(State.PASS, State.FAIL), None) is False
    assert exit_ok(results(State.PASS), "guard tripped") is False


def test_setup_failure_marks_all_plans_error(tmp_path, monkeypatch):
    from zerobus_test_runner import runner
    from zerobus_test_runner.capability import Classification, State
    from zerobus_test_runner.runner import Plan

    async def boom(*a, **k):
        raise InfraError("no cargo on PATH")

    monkeypatch.setattr(process, "cargo_build", boom)
    plans = [Plan("rust", "grpc", "proto", Classification(None))]
    results = asyncio.run(runner._execute(Scenario("t", _trivial, None), "rust", plans, tmp_path))
    assert results and all(r.state is State.ERROR for r in results)


def _trivial(s):
    st = s.sdk.create_stream(schema="simple_record")
    s.sdk.flush(st)
    s.expect(s.mock.call_count(), equals=1)


def test_unknown_schema_is_scenario_error():
    # An unknown schema is an author mistake, not infra: ScenarioError (exit 2), not ERROR.
    with pytest.raises(ScenarioError):
        supported_formats("no_such_schema")


def test_generated_stubs_import_as_package():
    from zerobus_test_runner._generated import mock_admin_pb2_grpc, test_harness_pb2_grpc

    assert hasattr(test_harness_pb2_grpc, "TestHarnessStub")
    assert hasattr(mock_admin_pb2_grpc, "MockAdminStub")
