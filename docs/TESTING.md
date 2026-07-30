# Testing

_Not yet written. Lands in Phase 9._

Will cover, per PRD §16 and §21.5:

- The test levels and what belongs at each (§16.1), and why e2e is deliberately
  the smallest of them.
- Running each level: unit, property, fault matrix, integration, interop,
  controller, e2e, soak.
- The table-driven test convention (§16.3).
- Time in tests: the injected clock, and why `time.Sleep` is prohibited (§16.4).
- Goroutine leak detection with `goleak` in every `TestMain` (§16.5).
- The bounded-resource checklist (§19.2).
- How to add a row to the fault scenario matrix (§15).

Until then, PRD §16 is the reference.
