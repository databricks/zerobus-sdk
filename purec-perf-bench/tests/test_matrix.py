import importlib.util
import contextlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location("matrix", Path(__file__).resolve().parents[1] / "run_matrix.py")
matrix = importlib.util.module_from_spec(spec)
spec.loader.exec_module(matrix)
BASE = {"host": "localhost", "port": 1, "table": "test.default.events", "input": "fixture.jsonl"}
MEASUREMENT = {"acked_records_per_second": 100, "acked_mb_per_second": 1.25,
               "cohort_acked_records_per_second": 90, "cohort_acked_mb_per_second": 1.125,
               "cohort_cpu_ns_per_acked_record": 42}


class MatrixTests(unittest.TestCase):
    def test_rounds_interleave_all_variants_reproducibly(self):
        variants = matrix.variants_for("queues", BASE, [1, 2, 4])
        jobs = matrix.interleave(variants, 3, 7)
        self.assertEqual(jobs, matrix.interleave(variants, 3, 7))
        for i in range(3):
            self.assertEqual({job["name"] for job in jobs[i * 6:(i + 1) * 6]}, {v["name"] for v in variants})

    def test_topologies_use_every_stream(self):
        variants = matrix.variants_for("topology", BASE, [1, 2, 8])
        for variant in variants:
            options = variant["options"]
            self.assertLessEqual(options["io-threads"], options["connections"])
            self.assertLessEqual(options["connections"], options["streams"])
            self.assertLessEqual(options["streams"], options["producers"])
            self.assertEqual(options["io-threads"], 1)

    def test_coalescing_is_independent_of_batching(self):
        variants = matrix.variants_for("coalescing", dict(BASE, batch="auto"), [4])
        self.assertEqual({v["options"]["write-coalesce-bytes"] for v in variants}, {0, 65536, 262144})
        self.assertTrue(all(v["options"]["batch"] == "auto" for v in variants))

    def test_unknown_options_and_tokens_are_not_allowed(self):
        for key in ("token", "authorization", "anything"):
            with self.assertRaises(ValueError):
                matrix.command_for("bench", dict(BASE, **{key: "secret"}))

    def test_finalists_are_explicit(self):
        with self.assertRaises(ValueError):
            matrix.variants_for("finalists", BASE, [1])
        variants = matrix.variants_for("finalists", BASE, [1, 2], [{"name": "a", "options": {"queue": "atomic"}}])
        self.assertEqual(len(variants), 2)

    def test_failed_results_are_not_zero_throughput_samples(self):
        values = dict(MEASUREMENT)
        rows = [{"variant": "a", "exit_code": 0, "result": {"ok": True, "measurement": values}},
                {"variant": "a", "exit_code": 1, "result": {"ok": False}}]
        summary = matrix.summarize(rows)[0]
        self.assertEqual(summary["failed_runs"], 1)
        self.assertEqual(summary["acked_records_per_second"]["median"], 100)
        self.assertEqual(summary["acked_mb_per_second"]["median"], 1.25)
        self.assertEqual(summary["cohort_acked_mb_per_second"]["median"], 1.125)

    def test_interleaved_failure_counts_follow_variant(self):
        variants = matrix.variants_for("retention", BASE, [1])
        jobs = matrix.interleave(variants, 3, 13)
        values = dict(MEASUREMENT)
        rows = []
        for job in jobs:
            failed = job["name"] == "acked_p1" and job["repeat"] == 2
            rows.append({"variant": job["name"], "exit_code": int(failed),
                         "result": {"ok": not failed, "measurement": values}})
        summaries = {item["variant"]: item for item in matrix.summarize(rows)}
        self.assertEqual(summaries["acked_p1"]["successful_runs"], 2)
        self.assertEqual(summaries["acked_p1"]["failed_runs"], 1)
        self.assertEqual(summaries["sent_p1"]["successful_runs"], 3)

    def test_dry_run_does_not_execute_or_create_output(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = root / "config.json"
            config.write_text(json.dumps({"baseline": BASE}))
            output = root / "results.json"
            run = subprocess.run([sys.executable, str(Path(matrix.__file__)),
                                  "--bench", str(root / "no-executable"), "--config", str(config),
                                  "--stage", "queues", "--producers", "1,2", "--repeats", "2",
                                  "--output", str(output), "--dry-run"],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 universal_newlines=True, timeout=10)
            self.assertEqual(run.returncode, 0, run.stderr)
            self.assertEqual(len(json.loads(run.stdout)["runs"]), 8)
            self.assertFalse(output.exists())

    def test_old_byte_rates_are_converted_to_decimal_megabytes(self):
        result = {"schema_version": 1, "ok": True, "measurement": {
            "acked_bytes_per_second": 1_250_000,
            "cohort_acked_bytes_per_second": 1_125_000,
            "cohort_acked_bytes": 2_500_000}}
        matrix.normalize_result_units(result)
        self.assertEqual(result["schema_version"], 2)
        self.assertEqual(result["measurement"], {"acked_mb_per_second": 1.25,
                         "cohort_acked_mb_per_second": 1.125, "cohort_acked_bytes": 2_500_000})
        matrix.normalize_result_units(result)
        self.assertEqual(result["measurement"]["acked_mb_per_second"], 1.25)

    def test_pretty_array_is_readable_between_runs_and_never_overwritten(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            bench = root / "fake-bench"
            bench.touch()
            bench.chmod(0o755)
            config = root / "config.json"
            config.write_text(json.dumps({"baseline": BASE,
                "finalists": [{"name": "candidate", "options": {}}]}))
            output = root / "results.json"
            completed = []

            def run_fake(command, **kwargs):
                if "--dry-run" in command:
                    return subprocess.CompletedProcess(command, 0, "{}", "")
                persisted = json.loads(output.read_text())
                self.assertEqual(len(persisted), len(completed))
                code = int(len(completed) == 1)
                completed.append(code)
                result = {"schema_version": 2, "ok": not code, "measurement": MEASUREMENT}
                return subprocess.CompletedProcess(command, code, json.dumps(result), "")

            args = ["run_matrix", "--bench", str(bench), "--config", str(config),
                    "--stage", "finalists", "--producers", "1", "--repeats", "3",
                    "--output", str(output)]
            stdout = io.StringIO()
            with mock.patch.object(sys, "argv", args), mock.patch.object(matrix.subprocess, "run", run_fake), \
                    contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(matrix.main(), 1)
                contents = output.read_text()
                self.assertEqual(matrix.main(), 2)
            self.assertEqual(output.read_text(), contents)
            rows = json.loads(contents)
            self.assertEqual([row["exit_code"] for row in rows], [0, 1, 0])
            self.assertIn('\n    "result": {\n', contents)
            summary = json.loads(stdout.getvalue())["variants"][0]
            self.assertEqual(summary["successful_runs"], 2)
            self.assertEqual(summary["failed_runs"], 1)
            self.assertEqual(summary["acked_mb_per_second"]["median"], 1.25)


if __name__ == "__main__":
    unittest.main()
