# driftwatch documentation

| Document | What it covers |
|---|---|
| [PRD.md](PRD.md) | The full technical product requirements. The authority for every decision below. |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Components, data flow, and the concurrency model. |
| [CORRECTNESS.md](CORRECTNESS.md) | Why naive diffing produces false positives, and the six mechanisms that fix it. |
| [DECISIONS.md](DECISIONS.md) | ADR log. Every technology choice and every deviation from the PRD. |
| [DISCOVERIES.md](DISCOVERIES.md) | Things that did not behave as expected, with evidence. |
| [KNOWN_GAPS.md](KNOWN_GAPS.md) | Limitations, stated plainly. |
| [TESTING.md](TESTING.md) | How to run each test level and how to add a fault scenario. |
| [OPERATIONS.md](OPERATIONS.md) | Runbook: what each alert means and what to check first. |
| [METRICS.md](METRICS.md) | Every exported metric, its labels, and its cardinality bound. |
| [ADDING_A_SOURCE.md](ADDING_A_SOURCE.md) | Implementing the `Source` interface, with a worked example. |
| [ADDING_A_PROJECTION.md](ADDING_A_PROJECTION.md) | Implementing the `Projection` interface, with a worked example. |
| [evidence/](evidence/) | Captured terminal output backing every claim in the README. |

Start with [CORRECTNESS.md](CORRECTNESS.md). It is the part of this project
worth reading.
