"""Build, launch, endpoint-parse and tear down the mock and harness subprocesses.

Startup protocol: each binary prints exactly one ``*_ENDPOINT=host:port`` line to stdout,
logs go to stderr, and both exit on the ``Shutdown`` RPC or SIGTERM/SIGINT.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable, Optional

from zerobus_test_runner.errors import InfraError
from zerobus_test_runner.paths import HARNESS_DIR, REPO_ROOT

MOCK_PREFIX = "MOCK_ENDPOINT="
HARNESS_PREFIX = "HARNESS_ENDPOINT="
STARTUP_TIMEOUT_S = 60.0
GRACE_S = 5.0
_STREAM_LIMIT = 1 << 20  # long log lines must not kill the pump


def harness_manifest(sdk: str) -> Path:
    """Only the Rust harness exists in the first slice. Other SDKs fail with a clear error."""
    manifest = HARNESS_DIR / sdk / "Cargo.toml"
    if not manifest.is_file():
        raise InfraError(f"no harness for sdk '{sdk}' yet (expected {manifest})")
    return manifest


async def cargo_build(manifest: Path, log_path: Path, bin_name: Optional[str] = None) -> Path:
    """``cargo build`` the crate and return the path of its single binary artifact."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    argv = ["cargo", "build", "--manifest-path", str(manifest), "--message-format=json"]
    proc = await asyncio.create_subprocess_exec(
        *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
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
        if "bin" not in msg.get("target", {}).get("kind", []):
            continue
        if bin_name is None or msg["target"]["name"] == bin_name:
            bins.append(msg["executable"])
    if len(set(bins)) != 1:
        raise InfraError(f"expected exactly one binary from {manifest}, found {sorted(set(bins))}")
    return Path(bins[0])


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


# Per-SDK harness launch recipes. The harness binaries/artifacts are built out of band (see
# each SDK's build steps under conformance/harness/<sdk>/); the orchestrator only
# launches the already-built artifact, except Rust which it cargo-builds (fast + cached).
# Tool locations are overridable via env vars for portability across machines/CI.
_PY_HARNESS = REPO_ROOT / "python" / ".venv" / "bin" / "python"
_JAVA = _env("ZB_JAVA", "/usr/lib/jvm/java-17-openjdk-amd64/bin/java")
_DOTNET = _env("ZB_DOTNET", str(Path.home() / ".dotnet" / "dotnet"))
_NODE = _env("ZB_NODE", "node")

_JAVA_JAR = HARNESS_DIR / "java" / "target" / "zerobus-testing-harness-java.jar"
_TS_JS = HARNESS_DIR / "typescript" / "dist" / "main.js"
_DOTNET_DLL = HARNESS_DIR / "dotnet" / "bin" / "Debug" / "net8.0" / "zerobus-testing-harness-dotnet.dll"
_CPP_BIN = HARNESS_DIR / "cpp" / "build" / "zerobus-testing-harness-cpp"
_PUREGO_BIN = HARNESS_DIR / "purego" / "bin" / "zerobus-testing-harness-purego"


@dataclass(frozen=True)
class LaunchSpec:
    argv: list
    env: dict
    artifact: Optional[Path]  # file that must exist before launch (None = skip the check)
    build_hint: str


_LAUNCH: dict = {
    "python": LaunchSpec(
        [str(_PY_HARNESS), "-m", "zerobus_harness"],
        {"PYTHONPATH": str(HARNESS_DIR / "python" / "src")},
        None,  # it's a module, not a file; let python -m report a clear import error
        "cd python && .venv/bin/python -m maturin develop --features testing",
    ),
    "java": LaunchSpec(
        [_JAVA, "-jar", str(_JAVA_JAR)], {}, _JAVA_JAR,
        "build zerobus-jni --features testing, stage the .so, mvn install the SDK, then mvn package in conformance/harness/java",
    ),
    "typescript": LaunchSpec(
        [_NODE, str(_TS_JS)], {}, _TS_JS,
        "cd typescript && npm install && npm run build; then cd conformance/harness/typescript && npm install && npm run build",
    ),
    "dotnet": LaunchSpec(
        [_DOTNET, str(_DOTNET_DLL)], {}, _DOTNET_DLL,
        "cd conformance/harness/dotnet && ~/.dotnet/dotnet build",
    ),
    "cpp": LaunchSpec(
        [str(_CPP_BIN)], {}, _CPP_BIN,
        "needs gRPC C++ dev (libgrpc++-dev, protobuf-compiler-grpc); cmake -S conformance/harness/cpp -B conformance/harness/cpp/build -DProtobuf_PROTOC_EXECUTABLE=/usr/bin/protoc && cmake --build conformance/harness/cpp/build",
    ),
    "purego": LaunchSpec(
        [str(_PUREGO_BIN)], {}, _PUREGO_BIN,
        "cd conformance/harness/purego && go build -o bin/zerobus-testing-harness-purego .",
    ),
}


async def build_and_launch_harness(sdk: str, run_dir: Path) -> "ManagedProcess":
    """Rust is cargo-built here; other SDKs are launched from their pre-built artifact."""
    if sdk == "rust":
        bin_ = await cargo_build(harness_manifest(sdk), run_dir / f"build-{sdk}.log")
        return await launch([str(bin_)], name="harness-rust", endpoint_prefix=HARNESS_PREFIX, log_dir=run_dir)
    spec = _LAUNCH.get(sdk)
    if spec is None:
        raise InfraError(f"no harness recipe for sdk '{sdk}' yet")
    if spec.artifact is not None and not spec.artifact.exists():
        raise InfraError(f"{sdk} harness not built ({spec.artifact} missing). Build with: {spec.build_hint}")
    return await launch(
        spec.argv, name=f"harness-{sdk}", endpoint_prefix=HARNESS_PREFIX, log_dir=run_dir, env=spec.env
    )


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
        """Shutdown RPC, then SIGTERM after ``grace_s``, then SIGKILL after another ``grace_s``."""
        if self.proc.returncode is None and shutdown is not None:
            try:
                await asyncio.wait_for(shutdown(), grace_s)
            except Exception:  # teardown is best-effort; escalate below
                pass
        if not await self._wait(grace_s):
            self.proc.terminate()
            if not await self._wait(grace_s):
                self.proc.kill()
                await self.proc.wait()
        await asyncio.gather(*self._pumps, return_exceptions=True)
        self._logf.close()

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

    ``env`` entries are merged onto the current environment (e.g. PYTHONPATH for the py harness).
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
    except FileNotFoundError as e:
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
    except (asyncio.TimeoutError, InfraError) as e:
        proc.kill()
        await proc.wait()
        await asyncio.gather(*pumps, return_exceptions=True)
        logf.close()
        reason = "timed out waiting for endpoint" if isinstance(e, asyncio.TimeoutError) else str(e)
        raise InfraError(f"{name}: {reason}; last output: {' | '.join(tail)[-800:]}") from e
    return ManagedProcess(name, proc, endpoint, logf, pumps, tail)


async def _pump(stream, logf, tag, prefix, endpoint_fut, tail) -> None:
    try:
        while True:
            raw = await stream.readline()
            if not raw:
                break
            line = raw.decode(errors="replace").rstrip("\r\n")
            logf.write(f"[{tag}] {line}\n")
            tail.append(line)
            if prefix and endpoint_fut is not None and not endpoint_fut.done() and line.startswith(prefix):
                try:
                    endpoint_fut.set_result(parse_endpoint(line[len(prefix):].strip()))
                except InfraError as e:
                    endpoint_fut.set_exception(e)
    finally:
        if endpoint_fut is not None and not endpoint_fut.done():
            endpoint_fut.set_exception(InfraError("process exited before printing its endpoint line"))
