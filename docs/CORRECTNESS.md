# Correctness

_Not yet written. Lands in Phase 9._

This is the document that explains the substance of the project to a human
reader: why comparing an event-derived oracle against a target store naively
produces an avalanche of false positives, and the six mechanisms that make the
comparison sound.

Will cover, per PRD §5 and §21.2:

- The eight reasons the naive design fails (F1-F8).
- Sequence numbers and trust state — how driftwatch knows when *it* is the
  untrustworthy party.
- The settlement window, and why it uses local receive time rather than
  publisher timestamps.
- Two-phase confirmation.
- Version-fenced comparison.
- Bootstrap modes, and what each one does and does not guarantee.
- TTL and eviction handling.
- The fourteen correctness invariants (I1-I14) and the property tests that
  prove them.

Until then, PRD §5 is the reference.
