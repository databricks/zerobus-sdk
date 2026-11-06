"""Gates 1 (validity), 2 (capability), 3 (applicability) and wiring.

Pure functions, no I/O. The runner calls classify() to decide, per (sdk, transport, format)
tuple, whether it runs or lands in a terminal state.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Iterable, Optional

if TYPE_CHECKING:
    from zerobus_test_runner.scenario import Gate

TRANSPORTS = ("grpc", "arrow_flight")
FORMATS = ("proto", "json", "avro", "arrow")

# Gate 1: protocol truth, SDK-independent. Anything not listed is INVALID.
VALID_FORMATS_BY_TRANSPORT = {
    "grpc": frozenset({"proto", "json", "avro"}),
    "arrow_flight": frozenset({"arrow"}),
}

# Gate 2: static capability table (cross product of each SDK's transports and formats).


@dataclass(frozen=True)
class Capability:
    transports: frozenset
    formats: frozenset


# avro only on rust, python.
CAPABILITIES = {
    "rust": Capability(frozenset({"grpc", "arrow_flight"}), frozenset({"proto", "json", "avro", "arrow"})),
    "python": Capability(frozenset({"grpc", "arrow_flight"}), frozenset({"proto", "json", "avro", "arrow"})),
    "go": Capability(frozenset({"grpc"}), frozenset({"proto", "json"})),
    "typescript": Capability(frozenset({"grpc", "arrow_flight"}), frozenset({"proto", "json", "arrow"})),
    "java": Capability(frozenset({"grpc", "arrow_flight"}), frozenset({"proto", "json", "arrow"})),
    "cpp": Capability(frozenset({"grpc", "arrow_flight"}), frozenset({"proto", "json", "arrow"})),
    "dotnet": Capability(frozenset({"grpc"}), frozenset({"proto", "json"})),
    "purego": Capability(frozenset({"grpc"}), frozenset({"proto", "json"})),
}

# Tuples whose harness is actually implemented; anything else passing gates 1-3 is reported
# UNSUPPORTED (never faked). grpc+proto is wired for all SDKs; the rest are partial.
WIRED = frozenset(
    {(sdk, "grpc", "proto") for sdk in ("rust", "python", "typescript", "java", "cpp", "dotnet", "purego")}
    # avro wired only where supported (rust, python)
    | {("rust", "grpc", "avro"), ("python", "grpc", "avro")}
    # arrow_flight wired for rust/python/java/typescript (cpp excluded; purego/dotnet unsupported)
    | {(sdk, "arrow_flight", "arrow") for sdk in ("rust", "python", "java", "typescript")}
)


class State(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    ERROR = "ERROR"
    NA = "N-A"
    UNSUPPORTED = "UNSUPPORTED"
    INVALID = "INVALID"


@dataclass(frozen=True)
class Classification:
    """state is None means the tuple should be executed."""

    state: Optional[State]
    reason: str = ""

    @property
    def runs(self) -> bool:
        return self.state is None


def is_valid(transport: str, fmt: str) -> bool:
    return fmt in VALID_FORMATS_BY_TRANSPORT.get(transport, frozenset())


def is_supported(sdk: str, transport: str, fmt: str) -> bool:
    cap = CAPABILITIES.get(sdk)
    return cap is not None and transport in cap.transports and fmt in cap.formats


def valid_tuples() -> list[tuple[str, str]]:
    """Every valid (transport, format) pair, in a stable order."""
    return [(t, f) for t in TRANSPORTS for f in FORMATS if is_valid(t, f)]


def admits(
    sdk: str,
    transport: str,
    fmt: str,
    gate: "Optional[Gate]",
    schema_formats: Iterable[str],
) -> Optional[Classification]:
    """Gates 1-3. Returns a terminal Classification, or None if the tuple passes them."""
    if not is_valid(transport, fmt):
        return Classification(State.INVALID, f"{fmt} is not a valid record encoding for {transport}")
    if not is_supported(sdk, transport, fmt):
        return Classification(State.UNSUPPORTED, f"{sdk} does not support {transport}/{fmt}")
    if gate is not None and not gate.matches(sdk, transport, fmt):
        return Classification(State.NA, "scenario gate excludes this tuple")
    if fmt not in set(schema_formats):
        return Classification(State.NA, f"schema has no {fmt} artifact")
    return None


def classify(
    sdk: str,
    transport: str,
    fmt: str,
    gate: "Optional[Gate]",
    schema_formats: Iterable[str],
) -> Classification:
    """Full placement for one scenario: gates 1-3, then wiring (first slice)."""
    terminal = admits(sdk, transport, fmt, gate, schema_formats)
    if terminal is not None:
        return terminal
    if (sdk, transport, fmt) not in WIRED:
        return Classification(State.UNSUPPORTED, f"not wired in this slice (TODO): {sdk}/{transport}/{fmt}")
    return Classification(None)
