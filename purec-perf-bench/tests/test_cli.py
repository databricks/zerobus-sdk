"""Public CLI tests: no token or network required."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

BENCH = sys.argv.pop(1)


class CommandLineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.input = Path(self.temp.name) / 'quoted"café.jsonl'
        self.input.write_bytes(b'{"id":1}\r\n\n{"id":2}')
        self.args = [BENCH, "--host", "127.0.0.1", "--port", "1", "--table", "test.default.records",
                     "--input", str(self.input), "--dry-run"]

    def run_bench(self, args):
        run = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             universal_newlines=True, timeout=10)
        return run, json.loads(run.stdout)

    def test_dry_run_preserves_configuration_and_needs_no_token(self):
        run, result = self.run_bench(self.args)
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertTrue(result["dry_run"])
        self.assertEqual(result["schema_version"], 2)
        self.assertEqual(result["corpus"]["records"], 2)
        self.assertEqual(result["corpus"]["payload_bytes"], 16)
        self.assertEqual(result["config"]["input"], str(self.input))

    def test_aggregate_capacities_are_bounded(self):
        run, result = self.run_bench(self.args + ["--producers", "3", "--streams", "3", "--queue-capacity", "100"])
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertEqual(result["config"]["queue_capacity_effective"], 96)
        self.assertEqual(result["config"]["queue_capacity_per_stream"], 32)
        self.assertLessEqual(result["config"]["buffer_bytes_effective"], result["config"]["buffer_bytes_requested"])

    def test_invalid_configurations_fail_with_json(self):
        for extra in (["--duration", "nan"], ["--producers", "0"], ["--queue", "bad"],
                      ["--port", "65536"], ["--streams", "2"], ["--queue-capacity", "1"],
                      ["--duration", "0"], ["--host", "remote.example", "--test-plaintext"],
                      ["--sample-every", "-1"], ["--unknown"]):
            with self.subTest(extra=extra):
                run, result = self.run_bench(self.args + list(extra))
                self.assertNotEqual(run.returncode, 0)
                self.assertFalse(result["ok"])
                self.assertEqual(result["schema_version"], 2)

    def test_nul_and_empty_input_rejected(self):
        for contents in (b"", b"\n\r\n", b"{}\x00\n"):
            self.input.write_bytes(contents)
            run, result = self.run_bench(self.args)
            self.assertNotEqual(run.returncode, 0)
            self.assertFalse(result["ok"])

    def test_missing_token_fails_before_connecting(self):
        args = self.args[:-1] + ["--token-env", "ZBPERF_TEST_UNSET_TOKEN"]
        env = dict(os.environ)
        env.pop("ZBPERF_TEST_UNSET_TOKEN", None)
        run = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             universal_newlines=True, env=env, timeout=10)
        self.assertNotEqual(run.returncode, 0)
        self.assertFalse(json.loads(run.stdout)["ok"])


if __name__ == "__main__":
    unittest.main()
