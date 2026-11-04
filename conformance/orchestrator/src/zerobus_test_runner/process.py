"""Build, launch, endpoint-parse and tear down the mock and harness subprocesses.

Startup protocol: each binary prints exactly one ``*_ENDPOINT=host:port`` line to stdout,
logs go to stderr, and both exit on the ``Shutdown`` RPC or SIGTERM/SIGINT.

Only the Rust harness is cargo-built and launched here. Each other SDK adds its own launch
recipe in the harness module that implements it.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections import deque
from pathlib import Path
from typing import Awaitable, Callable, Optional

from zerobus_test_runner.errors import InfraError
from zerobus_test_runner.paths import CONFORMANCE_DIR, HARNESS_DIR, REPO_ROOT, RUST_HARNESS_MANIFEST

MOCK_PREFIX = "MOCK_ENDPOINT="
HARNESS_PREFIX = "HARNESS_ENDPOINT="
STARTUP_TIMEOUT_S = 60.0
GRACE_S = 5.0
_STREAM_LIMIT = 1 << 20  # long log lines must not kill the pump

# Log file names, defined once so launch() and the runner's summary footer agree.
MOCK_LOG = "mock.log"


def harness_log(sdk: str) -> str:
    return f"harness-{sdk}.log"


async def cargo_build(manifest: Path, log_path: Path) -> Path:
    """``cargo build`` the crate and return the path of its single binary artifact."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    # json-render-diagnostics keeps the artifact JSON on stdout and renders compile errors
    # to stderr, so a failed build logs the actual errors rather than just "could not compile".
    argv = ["cargo", "build", "--manifest-path", str(manifest), "--message-format=json-render-diagnostics"]
    try:
        proc = await asyncio.create_subprocess_exec(
            *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
    except FileNotFoundError as e:
        raise InfraError(f"cannot run 'cargo' (is the Rust toolchain installed?): {e}") from e
    out, err = await proc.communicate()
    log_path.write_bytes(err)
    if proc.returncode != 0:
        raise InfraError(f"cargo build failed for {manifest} (log: {log_path})")
    bins = []
    for line in out.decode(errors="replace").splitlines():
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        if msg.get("reason") != "compiler-artifact" or not msg.get("executable"):
            continue
        if "bin" in msg.get("target", {}).get("kind", []):
            bins.append(msg["executable"])
    if len(set(bins)) != 1:
        raise InfraError(f"expected exactly one binary from {manifest}, found {sorted(set(bins))}")
    return Path(bins[0])


def _load_launch_spec(sdk: str) -> tuple:
    """Read conformance/harness/<sdk>/launch.json. Each harness ships its own launch recipe
    so it is reviewed with the harness it starts. Placeholders {harness_dir}, {conformance_dir},
    {repo_root} are substituted; interpreters (node, java, dotnet) are expected on PATH."""
    path = HARNESS_DIR / sdk / "launch.json"
    if not path.is_file():
        raise InfraError(f"no harness for sdk '{sdk}' (missing {path})")
    spec = json.loads(path.read_text())
    subs = {"harness_dir": str(HARNESS_DIR / sdk), "conformance_dir": str(CONFORMANCE_DIR), "repo_root": str(REPO_ROOT)}
    argv = [tok.format(**subs) for tok in spec["argv"]]
    artifact = spec["artifact"].format(**subs) if spec.get("artifact") else None
    env = {k: v.format(**subs) for k, v in spec.get("env", {}).items()}
    return argv, artifact, spec.get("build_hint", ""), env


async def build_and_launch_harness(sdk: str, run_dir: Path) -> "ManagedProcess":
    """Build and launch the SDK's harness. Rust is cargo-built here; every other SDK ships a
    launch.json recipe in its harness directory."""
    if sdk == "rust":
        bin_ = await cargo_build(RUST_HARNESS_MANIFEST, run_dir / "build-harness-rust.log")
        return await launch([str(bin_)], name="harness-rust", endpoint_prefix=HARNESS_PREFIX, log_dir=run_dir)
    argv, artifact, build_hint, env = _load_launch_spec(sdk)
    if artifact is not None and not Path(artifact).exists():
        raise InfraError(f"{sdk} harness not built ({artifact} missing). Build with: {build_hint}")
    try:
        return await launch(argv, name=f"harness-{sdk}", endpoint_prefix=HARNESS_PREFIX, log_dir=run_dir, env=env)
    except InfraError as e:
        # A spawn/startup failure (e.g. the interpreter or venv is missing) should still point at
        # how to build the harness, since a module launch has no artifact to pre-check above.
        if not build_hint:
            raise
        raise InfraError(f"{e}. Build with: {build_hint}") from e


def parse_endpoint(value: str) -> str:
    host, sep, port = value.rpartition(":")
    if not sep or not host or not port.isdigit():
        raise InfraError(f"malformed endpoint '{value}', expected host:port")
    return value


class ManagedProcess:
    """A running child with its stdout/stderr pumped into a log file."""

    def __init__(self, name: str, proc: asyncio.subprocess.Process, endpoint: str, logf, pumps, tail):
        self.name = name
        self.proc = proc
        self.endpoint = endpoint
        self._logf = logf
        self._pumps = pumps
        self._tail = tail

    async def stop(self, shutdown: Optional[Callable[[], Awaitable[None]]] = None, grace_s: float = GRACE_S) -> None:
        """Shutdown RPC, then SIGTERM after ``grace_s``, then SIGKILL after another ``grace_s``.

        The pumps are always gathered and the log file always closed, even if an earlier step
        raises or the task is cancelled, so no child/pump/log handle leaks on any exit path.
        """
        try:
            if self.proc.returncode is None and shutdown is not None:
                try:
                    await asyncio.wait_for(shutdown(), grace_s)
                except Exception:  # teardown is best-effort; escalate below
                    pass
            if not await self._wait(grace_s):
                self._signal(self.proc.terminate)
                if not await self._wait(grace_s):
                    self._signal(self.proc.kill)
                    await self.proc.wait()
        finally:
            # A cancel before SIGTERM would leave the child running; the pumps only end on its
            # EOF, so kill it first or the gather below hangs.
            if self.proc.returncode is None:
                self._signal(self.proc.kill)
                await self.proc.wait()
            await asyncio.gather(*self._pumps, return_exceptions=True)
            try:
                self._logf.close()
            except OSError:
                pass

    def _signal(self, send) -> None:
        if self.proc.returncode is None:
            try:
                send()
            except ProcessLookupError:
                pass  # exited between the check and the signal

    async def _wait(self, timeout: float) -> bool:
        try:
            await asyncio.wait_for(self.proc.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False


async def launch(
    argv: list,
    *,
    name: str,
    endpoint_prefix: str,
    log_dir: Path,
    startup_timeout: float = STARTUP_TIMEOUT_S,
    env: Optional[dict] = None,
) -> ManagedProcess:
    """Start ``argv``, wait for the endpoint line on stdout, and keep pumping output to logs.

    ``env`` entries are merged onto the current environment.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    logf = (log_dir / f"{name}.log").open("a", buffering=1)
    run_env = {**os.environ, **env} if env else None
    try:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=_STREAM_LIMIT,
            env=run_env,
        )
    except OSError as e:  # FileNotFoundError, PermissionError, ENOEXEC, ...: never leak the log handle
        logf.close()
        raise InfraError(f"{name}: cannot launch {argv[0]!r}: {e}") from e
    loop = asyncio.get_running_loop()
    endpoint_fut: asyncio.Future = loop.create_future()
    tail: deque = deque(maxlen=20)
    pumps = [
        asyncio.create_task(_pump(proc.stdout, logf, name, endpoint_prefix, endpoint_fut, tail)),
        asyncio.create_task(_pump(proc.stderr, logf, f"{name}:err", None, None, tail)),
    ]
    try:
        endpoint = await asyncio.wait_for(endpoint_fut, startup_timeout)
    except BaseException as e:  # timeout, endpoint error, OR cancellation: release the child, pumps, log
        if proc.returncode is None:
            try:
                proc.kill()
            except ProcessLookupError:
                pass  # already exited (the usual cause of the endpoint error)
        await proc.wait()
        await asyncio.gather(*pumps, return_exceptions=True)
        logf.close()
        if isinstance(e, (asyncio.TimeoutError, InfraError)):
            reason = "timed out waiting for endpoint" if isinstance(e, asyncio.TimeoutError) else str(e)
            raise InfraError(f"{name}: {reason}; last output: {' | '.join(tail)[-800:]}") from e
        raise  # cancellation or anything else: cleaned up, propagate unchanged
    return ManagedProcess(name, proc, endpoint, logf, pumps, tail)


def _write_log(logf, text: str) -> None:
    # Never let a log-sink failure (e.g. a full disk) kill the pump: if we stop reading, the pipe
    # fills and the child blocks. Drop the line and keep draining.
    try:
        logf.write(text)
    except OSError:
        pass


async def _pump(stream, logf, tag, prefix, endpoint_fut, tail) -> None:
    try:
        while True:
            try:
                raw = await stream.readline()
            except ValueError:
                # A line longer than _STREAM_LIMIT; readline() has already consumed past it,
                # so logging a marker and continuing keeps the pump draining. The rest of the
                # long line is logged as one more line.
                _write_log(logf, f"[{tag}] <line exceeded {_STREAM_LIMIT} bytes; truncated>\n")
                continue
            if not raw:
                break
            line = raw.decode(errors="replace").rstrip("\r\n")
            _write_log(logf, f"[{tag}] {line}\n")
            tail.append(line)
            if prefix and endpoint_fut is not None and not endpoint_fut.done() and line.startswith(prefix):
                try:
                    endpoint_fut.set_result(parse_endpoint(line[len(prefix) :].strip()))
                except InfraError as e:
                    endpoint_fut.set_exception(e)
    finally:
        if endpoint_fut is not None and not endpoint_fut.done():
            endpoint_fut.set_exception(InfraError("process exited before printing its endpoint line"))
