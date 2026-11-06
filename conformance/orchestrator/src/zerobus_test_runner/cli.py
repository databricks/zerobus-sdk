"""Command-line entry point: ``conformance/test run --sdk <sdk> --scenario <name> [--transport T] [--format F]``."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from typing import Optional

from zerobus_test_runner.capability import CAPABILITIES, FORMATS, TRANSPORTS
from zerobus_test_runner.errors import InfraError
from zerobus_test_runner.paths import SCENARIOS_DIR
from zerobus_test_runner.runner import exit_ok, render_summary, run_invocation
from zerobus_test_runner.scenario import REGISTRY, ScenarioError, load_scenarios


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="conformance/test", description="Zerobus SDK Layer-1 conformance runner")
    sub = parser.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("run", help="run one scenario against one SDK")
    run.add_argument("--sdk", required=True, choices=sorted(CAPABILITIES))
    run.add_argument("--scenario", required=True, help="scenario name (function name in conformance/scenarios/)")
    run.add_argument("--transport", choices=TRANSPORTS, help="pin the transport (default: all valid)")
    run.add_argument("--format", dest="fmt", choices=FORMATS, help="pin the record format (default: all valid)")
    run.add_argument("-v", "--verbose", action="store_true", help="log orchestrator progress to stderr")
    return parser


async def _run(args: argparse.Namespace) -> int:
    try:
        scenarios = load_scenarios(SCENARIOS_DIR)
    except ScenarioError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    sc = scenarios.get(args.scenario)
    if sc is None:
        known = ", ".join(sorted(REGISTRY)) or "(none)"
        print(f"error: unknown scenario '{args.scenario}'. known: {known}", file=sys.stderr)
        return 2
    try:
        results, guard = await run_invocation(sc, args.sdk, args.transport, args.fmt)
    except (ScenarioError, InfraError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    print(render_summary(sc.name, args.sdk, results, guard))
    return 0 if exit_ok(results, guard) else 1


def main(argv: Optional[list] = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING, format="%(levelname)s %(message)s")
    if args.cmd == "run":
        return asyncio.run(_run(args))
    return 2
