"""Filesystem layout of the conformance tree, resolved from this package's location."""

from pathlib import Path

# .../conformance/orchestrator/src/zerobus_test_runner/paths.py
CONFORMANCE_DIR = Path(__file__).resolve().parents[3]
REPO_ROOT = CONFORMANCE_DIR.parent
SCENARIOS_DIR = CONFORMANCE_DIR / "scenarios"
SCHEMAS_DIR = CONFORMANCE_DIR / "schemas"
MOCK_MANIFEST = CONFORMANCE_DIR / "mock_server" / "Cargo.toml"
HARNESS_DIR = CONFORMANCE_DIR / "harness"
RUNS_DIR = CONFORMANCE_DIR / ".runs"
