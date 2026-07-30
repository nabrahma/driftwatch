# Architecture

_Not yet written. Lands in Phase 9._

Will cover, per PRD §21.5:

- The component diagram (§6.1) and what each component is responsible for.
- End-to-end data flow, source frame to reported finding (§6.2).
- The concurrency model (§6.3) — one goroutine per role, and why the applier is
  single-threaded.
- The failure and degradation policy (§6.4).

Until then, PRD §6 is the reference.
