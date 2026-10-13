"""Wrapper checks with fake build/token/benchmark commands; no network access."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

SOURCE = Path(__file__).resolve().parents[1]


@unittest.skipUnless(shutil.which("jq") and shutil.which("bash"), "wrapper requires jq and Bash")
class RunWrapperTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="zbperf wrapper ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bench = self.root / "purec-perf-bench"
        (self.bench / "build").mkdir(parents=True)
        (self.root / "purec-perf").mkdir()
        self.bin = self.root / "bin"
        self.bin.mkdir()
        for name in ("run.sh", "run_matrix.py"):
            shutil.copy2(SOURCE / name, self.bench / name)
        self.log = self.root / "build.jsonl"
        self.token_log = self.root / "minted-table"
        self.executable(self.bin / "cmake", '''#!/usr/bin/env python3
import json, os, sys
with open(os.environ["WRAPPER_BUILD_LOG"], "a") as output:
    output.write(json.dumps(sys.argv[1:]) + "\\n")
''')
        (self.bench / "mint_token.sh").write_text(
            'printf "%s" "$1" > "$WRAPPER_TOKEN_LOG"\nexport ZB_TOKEN=fixture-token\n')
        self.executable(self.bench / "build" / "zbperf-bench", '''#!/usr/bin/env python3
import json, os, sys
if "--dry-run" not in sys.argv:
    assert os.environ.get("ZB_TOKEN") == "fixture-token"
print(json.dumps({"schema_version": 2, "ok": True, "measurement": {
    "acked_records_per_second": 1, "acked_mb_per_second": 0.001024,
    "cohort_acked_records_per_second": 1, "cohort_acked_mb_per_second": 0.001024,
    "cohort_cpu_ns_per_acked_record": 10}}))
''')
        self.config = self.root / "config with spaces.json"
        self.data = {
            "baseline": {"host": "fixture.example", "port": 443,
                         "table": "test.default.records", "input": "records with spaces.jsonl"},
            "finalists": [{"name": "baseline", "options": {}}],
        }
        self.env = dict(os.environ, PATH=str(self.bin) + os.pathsep + os.environ["PATH"],
                        WRAPPER_BUILD_LOG=str(self.log), WRAPPER_TOKEN_LOG=str(self.token_log))

    def executable(self, path, text):
        path.write_text(text)
        path.chmod(0o755)

    def run_wrapper(self, *args):
        self.config.write_text(json.dumps(self.data))
        return subprocess.run([str(self.bench / "run.sh"), self.config.name, *args],
                              cwd=self.root, env=self.env, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, universal_newlines=True, timeout=15)

    def test_defaults_build_mint_and_run_once_with_unique_results(self):
        for _ in range(2):
            run = self.run_wrapper()
            self.assertEqual(run.returncode, 0, run.stderr)
        outputs = list((self.bench / "results").glob("*.json"))
        self.assertEqual(len(outputs), 2)
        for output in outputs:
            rows = json.loads(output.read_text())
            self.assertEqual(len(rows), 1)
            self.assertIn('\n    "result": {\n', output.read_text())
            self.assertEqual(rows[0]["stage"], "finalists")
            self.assertEqual(rows[0]["variant"], "baseline_p1")
        self.assertEqual(self.token_log.read_text(), "test.default.records")
        builds = [json.loads(line) for line in self.log.read_text().splitlines()]
        self.assertEqual(len(builds), 10)
        self.assertTrue(all("-DCMAKE_EXPORT_COMPILE_COMMANDS=ON" in args
                            for args in builds if "-S" in args))
        self.assertTrue(any(arg.endswith("/install/lib/cmake/zbperf")
                            for args in builds for arg in args if arg.startswith("-Dzbperf_DIR=")))

    def test_forwarded_flags_override_defaults(self):
        output = self.root / "custom results.json"
        run = self.run_wrapper("--stage", "queues", "--producers", "1,2",
                               "--repeats", "2", "--output", str(output))
        self.assertEqual(run.returncode, 0, run.stderr)
        rows = json.loads(output.read_text())
        self.assertEqual(len(rows), 8)
        self.assertTrue(all(row["stage"] == "queues" for row in rows))

    def test_dry_run_and_help_have_no_side_effects(self):
        for flag in ("--d", "--dr", "--dry", "--dry-", "--dry-r", "--dry-ru", "--dry-run", "--help"):
            run = self.run_wrapper(flag)
            self.assertEqual(run.returncode, 0, run.stderr)
            self.assertFalse(self.log.exists())
            self.assertFalse(self.token_log.exists())
            self.assertFalse((self.bench / "results").exists())

    def test_different_targets_and_token_sources_rejected_before_build(self):
        for overrides in ({"table": "test.default.other"}, {"host": "other.example"},
                          {"port": 8443}, {"token-env": "OTHER_TOKEN"},
                          {"test-plaintext": True}):
            with self.subTest(overrides=overrides):
                self.data["finalists"][0]["options"] = overrides
                run = self.run_wrapper()
                self.assertNotEqual(run.returncode, 0)
                self.assertFalse(self.log.exists())
                self.assertFalse(self.token_log.exists())

    def test_invalid_matrix_and_reserved_flags_rejected_before_build(self):
        for args in (("--repeats", "0"), ("--config", "another.json"),
                     ("--bench=other",), ("--unknown",)):
            run = self.run_wrapper(*args)
            self.assertNotEqual(run.returncode, 0)
            self.assertFalse(self.log.exists())
            self.assertFalse(self.token_log.exists())


if __name__ == "__main__":
    unittest.main()
