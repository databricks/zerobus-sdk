"""Layer-1 conformance orchestrator.

Public surface for scenario authors is re-exported here: ``scenario``, ``formats``,
``golden``. Importing this package executes nothing; scenarios only record steps.
"""

from zerobus_test_runner.scenario import formats, golden, scenario

__all__ = ["scenario", "formats", "golden"]
