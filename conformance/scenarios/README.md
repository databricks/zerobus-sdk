# Scenarios

A scenario is a static, ordered list of steps (SDK actions and mock assertions) written with the Python builder DSL, one scenario per file. The DSL surface is defined in `../CONTRACTS.md` ("Python builder DSL surface") and the design in `../DESIGN.md` §5. Applicability comes from the `applies=` gate (for example `formats("proto")`) or is left unset to run everywhere; the schema also implies which formats a scenario can use. The orchestrator combines these with validity and per-SDK capability to place each scenario in the 3-gate matrix.
