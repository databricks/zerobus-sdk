"""Exception types shared across the orchestrator.

Three failure origins get three treatments. An SDK-under-test error is in-band data
(an SdkError in the response), asserted on by the scenario, never an exception here. A
harness or mock RPC failure, or a build/process fault, is an exception that maps to the
ERROR state. Author mistakes raise ScenarioError (see scenario.py) and never run.
"""


class InfraError(RuntimeError):
    """A build, process, or timeout fault. Maps to the ERROR state, never an assertion FAIL."""


class HarnessError(InfraError):
    """The harness process failed an RPC (gRPC status), as opposed to the SDK reporting
    an error in-band. Attributes the ERROR to the harness side."""


class MockError(InfraError):
    """The mock admin plane failed an RPC (gRPC status). Attributes the ERROR to the mock side."""
