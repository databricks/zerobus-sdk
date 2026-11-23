"""Matrix expansion, per-run orchestration and result summary.

One invocation = one scenario x one SDK x (optionally pinned) transport/format. The mock and the
harness are launched once per invocation; the mock is ``Reset`` before every run.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from zerobus_test_runner import process
from zerobus_test_runner.capability import FORMATS, TRANSPORTS, Classification, State, classify, is_valid, is_wired
from zerobus_test_runner.clients import HarnessClient, MockAdminClient
from zerobus_test_runner.data.schemas import supported_formats
from zerobus_test_runner.executor import Check, Executor, show_value
from zerobus_test_runner.paths import MOCK_MANIFEST, RUNS_DIR
from zerobus_test_runner.scenario import Scenario, ScenarioError, static_admitted_tuples, validate_or_raise

log = logging.getLogger("zerobus_test_runner")


def schema_formats_for(sc: Scenario) -> frozenset:
    """Formats every schema the scenario uses ships artifacts for (a schema implies its formats).

    The intersection across all of a scenario's schemas; all formats when it names none.
    """
    names = sc.schema_names()
    if not names:
        return frozenset(FORMATS)
    formats = supported_formats(names[0])
    for name in names[1:]:
        formats &= supported_formats(name)
    return formats


@dataclass(frozen=True)
class Plan:
    sdk: str
    transport: str
    fmt: str
    classification: Classification


@dataclass
class RunResult:
    plan: Plan
    state: State
    detail: str = ""
    checks: list = field(default_factory=list)
    seconds: float = 0.0


def expand(
    sc: Scenario,
    sdk: str,
    transport: Optional[str] = None,
    fmt: Optional[str] = None,
) -> list[Plan]:
    """Expand the requested dimensions into plans. Omitted dimensions expand over valid combos only.

    An explicitly pinned invalid tuple is reported INVALID; an invalid tuple produced by expansion
    is never enumerated.
    """
    schema_formats = schema_formats_for(sc)
    transports = [transport] if transport else list(TRANSPORTS)
    fmts = [fmt] if fmt else list(FORMATS)
    pinned = transport is not None and fmt is not None
    plans = []
    for t in transports:
        for f in fmts:
            if not pinned and not is_valid(t, f):
                continue
            plans.append(Plan(sdk, t, f, classify(sdk, t, f, sc.applies, schema_formats)))
    return plans


def guard_rail_error(sc: Scenario, sdk: str, results: list[RunResult], unrestricted: bool) -> Optional[str]:
    """A scenario that admits *wired* tuples for this SDK but reaches zero PASS/FAIL is a dead or
    mistyped gate. Only on an unrestricted expansion (no --transport/--format). Skipped when any
    result ERRORed, since an infra fault is already explained by its own row. Unwired tuples do not
    count: a tuple with no harness yet is NOT_WIRED, not an author mistake.
    """
    if not unrestricted or any(r.state is State.ERROR for r in results):
        return None
    admitted = [t for t in static_admitted_tuples(sc) if t[0] == sdk and is_wired(*t)]
    if admitted and not any(r.state in (State.PASS, State.FAIL) for r in results):
        return (
            f"guard-rail: scenario '{sc.name}' admits {len(admitted)} wired tuple(s) for {sdk} "
            f"but none reached PASS or FAIL"
        )
    return None


async def run_invocation(
    sc: Scenario,
    sdk: str,
    transport: Optional[str] = None,
    fmt: Optional[str] = None,
    runs_root: Path = RUNS_DIR,
) -> tuple[list[RunResult], Optional[str], Optional[Path]]:
    validate_or_raise(sc)
    plans = expand(sc, sdk, transport, fmt)
    results = [
        RunResult(p, p.classification.state, p.classification.reason) for p in plans if not p.classification.runs
    ]
    runnable = [p for p in plans if p.classification.runs]
    run_dir: Optional[Path] = None
    if runnable:
        # %f (microseconds) so two runs of the same scenario/sdk in the same second do not share a
        # directory or append into each other's logs.
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
        run_dir = runs_root / f"{stamp}-{sc.name}-{sdk}"
        results += await _execute(sc, sdk, runnable, run_dir)
    guard = guard_rail_error(sc, sdk, results, unrestricted=(transport is None and fmt is None))
    results.sort(key=lambda r: (TRANSPORTS.index(r.plan.transport), FORMATS.index(r.plan.fmt)))
    return results, guard, run_dir


async def _execute(sc: Scenario, sdk: str, runnable: list[Plan], run_dir: Path) -> list[RunResult]:
    def all_error(msg: str) -> list[RunResult]:
        return [RunResult(p, State.ERROR, msg) for p in runnable]

    harness_proc = mock_proc = None
    try:
        mock_bin = await process.cargo_build(MOCK_MANIFEST, run_dir / "build-mock.log")
        mock_proc = await process.launch(
            [str(mock_bin)], name="mock", endpoint_prefix=process.MOCK_PREFIX, log_dir=run_dir
        )
        harness_proc = await process.build_and_launch_harness(sdk, run_dir)
    except Exception as e:  # build/launch fault: all plans ERROR, and tear down whatever started
        log.error("setup failed: %s", e)
        await _teardown(harness_proc, None, mock_proc, None)
        return all_error(f"setup failed: {type(e).__name__}: {e}")
    except BaseException:  # cancellation during setup: tear down whatever started, then propagate
        await _teardown(harness_proc, None, mock_proc, None)
        raise

    harness = HarnessClient(harness_proc.endpoint)
    mock = MockAdminClient(mock_proc.endpoint)
    results: list[RunResult] = []
    try:
        for plan in runnable:
            results.append(await _run_one(sc, plan, harness, mock, mock_proc.endpoint))
    finally:
        await _teardown(harness_proc, harness, mock_proc, mock)
    return results


async def _run_one(sc: Scenario, plan: Plan, harness, mock, server_endpoint: str) -> RunResult:
    t0 = time.monotonic()
    ex = Executor(
        harness=harness,
        mock=mock,
        transport=plan.transport,
        fmt=plan.fmt,
        server_endpoint=server_endpoint,
    )
    try:
        await mock.reset()
        checks: list[Check] = await ex.run(sc.steps)
    except ScenarioError:
        raise  # author error (e.g. bad schema content): abort the invocation -> exit 2
    except Exception as e:  # harness/mock/infra fault: ERROR, but keep any checks already recorded
        detail = f"{type(e).__name__}: {e}"
        return RunResult(plan, State.ERROR, detail, list(ex.checks), seconds=time.monotonic() - t0)
    failed = [c for c in checks if not c.passed]
    state = State.FAIL if failed else State.PASS
    detail = f"{len(checks)} checks" if not failed else f"{len(failed)} of {len(checks)} checks failed"
    return RunResult(plan, state, detail, checks, time.monotonic() - t0)


async def _teardown(harness_proc, harness, mock_proc, mock) -> None:
    # Harness first (it is a client of the mock), then the mock. Every step runs even if an earlier
    # one fails or is cancelled, so one failure never leaves the other process/channel alive. A
    # cancellation is held and re-raised after all steps so the invariant holds on cancel too.
    cancelled: Optional[BaseException] = None

    async def _quiet(coro) -> None:
        nonlocal cancelled
        try:
            await coro
        except asyncio.CancelledError as e:
            cancelled = e
        except Exception as e:
            log.warning("teardown step failed: %s", e)

    if harness_proc is not None:
        await _quiet(harness_proc.stop(shutdown=harness.shutdown if harness else None))
    if harness is not None:
        await _quiet(harness.aclose())
    if mock_proc is not None:
        await _quiet(mock_proc.stop(shutdown=mock.shutdown if mock else None))
    if mock is not None:
        await _quiet(mock.aclose())
    if cancelled is not None:
        raise cancelled


def render_summary(
    sc_name: str, sdk: str, results: list[RunResult], guard: Optional[str], run_dir: Optional[Path] = None
) -> str:
    lines = [f"scenario={sc_name} sdk={sdk}", ""]
    lines.append(f"  {'STATE':<12} {'TRANSPORT':<13} {'FORMAT':<7} DETAIL")
    for r in results:
        lines.append(f"  {r.state.value:<12} {r.plan.transport:<13} {r.plan.fmt:<7} {r.detail}")
        if r.state in (State.FAIL, State.ERROR):  # show failing checks for ERROR too (partial results)
            for c in r.checks:
                if not c.passed:
                    lines.append(
                        f"      x {c.description}  expected={show_value(c.expected)} actual={show_value(c.actual)}"
                    )
    counts = Counter(r.state for r in results)
    totals = " ".join(f"{s.value}={counts[s]}" for s in State)
    lines += ["", f"totals: {totals}"]
    if run_dir is not None and any(r.state == State.ERROR for r in results):
        lines.append(f"logs: {run_dir}  ({process.MOCK_LOG}, {process.harness_log(sdk)}, build-*.log)")
    if guard:
        lines.append(f"guard: {guard}")
    return "\n".join(lines)


def exit_ok(results: list[RunResult], guard: Optional[str]) -> bool:
    bad = any(r.state in (State.FAIL, State.ERROR, State.INVALID) for r in results)
    passed = any(r.state == State.PASS for r in results)
    return passed and not bad and guard is None
