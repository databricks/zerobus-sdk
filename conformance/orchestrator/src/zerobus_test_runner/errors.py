"""Exception types shared across the orchestrator."""


class InfraError(RuntimeError):
    """Infrastructure failure (process died, RPC transport error, build failed).

    Maps to the ERROR state, never to an assertion FAIL.
    """
