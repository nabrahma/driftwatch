# Adding a source

_Not yet written. Lands in Phase 9._

Will be a worked example of implementing the `Source` interface (`pkg/source`),
per PRD §21.5, covering:

- The interface contract and the `RawMessage` shape (§6.2).
- Bounded buffering, and why overflow drops rather than blocks.
- Reconnection, backoff, and why a reconnect marks keys `Suspect` unless the
  source guarantees replay (§6.4).
- Registering the source and the tests a new source must pass.

Until then, `pkg/source/memory.go` is the reference implementation to copy.
