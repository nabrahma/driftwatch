# Adding a projection

_Not yet written. Lands in Phase 9._

Will be a worked example of implementing the `Projection` interface
(`pkg/projection`), per PRD §21.5, covering:

- The interface contract: a pure fold, no I/O, no clock, no randomness.
- Declaring commutativity, and what the property tests assert for each case.
- `KeyOwnership()` and how partitioning by publisher narrows the blast radius of
  a sequence gap (§5.2).
- The reference implementation pattern used for differential property testing.

Until then, `pkg/projection/scalar.go` is the simplest reference.
