#!/usr/bin/env python3
"""Regenerate every committed golden .bin from the independent encoders.

Run after adding or changing a ``golden(...)`` call or a schema, then review the diff and commit:

    conformance/orchestrator/.venv/bin/python conformance/orchestrator/scripts/regen_goldens.py

The required set is discovered by recording every scenario and collecting each ``golden()`` used in
an ``s.expect(...)``, so the committed files cannot drift from the scenarios. For each schema it
writes ``schemas/<schema>/golden/<fmt>/<sha1>.bin`` and a human-readable
``schemas/<schema>/golden/MANIFEST.json`` (record, file, hex). Stale ``.bin`` files that no scenario
references any more are deleted.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ORCH_SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(ORCH_SRC))

from zerobus_test_runner.data.golden import (  # noqa: E402
    GOLDEN_FORMATS,
    MANIFEST_NAME,
    encode_golden_for,
    golden_dir,
    golden_key,
)
from zerobus_test_runner.paths import SCENARIOS_DIR  # noqa: E402
from zerobus_test_runner.scenario import Expect, GoldenRef, load_scenarios, REGISTRY  # noqa: E402


def required_goldens() -> dict[tuple[str, str, str], dict]:
    """(schema, fmt, key) -> record, for every golden() referenced by an expectation."""
    load_scenarios(SCENARIOS_DIR)
    required: dict[tuple[str, str, str], dict] = {}
    for sc in REGISTRY.values():
        for step in sc.steps:
            if not isinstance(step, Expect):
                continue
            for eq in (step.equals, step.at_least):
                if isinstance(eq, GoldenRef):
                    key = (eq.schema, eq.fmt, golden_key(eq.record))
                    required[key] = eq.record
    return required


def main() -> int:
    required = required_goldens()
    if not required:
        print("no golden() references found in scenarios; nothing to do")
        return 0

    by_schema: dict[str, dict[tuple[str, str], dict]] = {}
    for (schema, fmt, key), record in required.items():
        if fmt not in GOLDEN_FORMATS:
            raise SystemExit(f"golden() for unsupported format '{fmt}' in scenario data")
        by_schema.setdefault(schema, {})[(fmt, key)] = record

    for schema, entries in sorted(by_schema.items()):
        out_dir = golden_dir(schema)
        manifest_rows = []
        wanted_files: set[Path] = set()
        for (fmt, key), record in sorted(entries.items()):
            data = encode_golden_for(schema, record, fmt)
            path = out_dir / fmt / f"{key}.bin"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            wanted_files.add(path.resolve())
            manifest_rows.append({
                "file": f"{fmt}/{key}.bin",
                "format": fmt,
                "record": record,
                "bytes": len(data),
                "hex": data.hex(" "),
            })
            print(f"wrote {path.relative_to(SCENARIOS_DIR.parent)} ({len(data)} bytes)")

        # Prune .bin files that no scenario references any more.
        for stale in out_dir.glob("*/*.bin"):
            if stale.resolve() not in wanted_files:
                stale.unlink()
                print(f"removed stale {stale.relative_to(SCENARIOS_DIR.parent)}")

        (out_dir / MANIFEST_NAME).write_text(json.dumps(manifest_rows, indent=2, ensure_ascii=False) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
