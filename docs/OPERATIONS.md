# Operations runbook

_Not yet written. Lands in Phase 9._

Will contain one section per alert, per PRD §21.5: what the alert means, what to
check first, the likely causes ranked, and how to confirm each one.

Two entries are already known to be needed:

- **Debugging in-cluster.** The runtime image is distroless with no shell
  (ADR-0005). Use an ephemeral debug container. Worth knowing before an
  incident, not during one.
- **Drift alert during a target outage.** driftwatch does not report divergence
  when the target is unreachable (§6.4, §23 A5). If a drift alert and a Redis
  incident coincide, the drift is real and predates the outage.

Until then, PRD §12.2 lists the alerting rules.
