# Known gaps

Limitations driftwatch has, stated plainly.

Two kinds of entry belong here (PRD §3.2, §21.5):

1. **Deliberate scope boundaries** — things not built on purpose, with the
   reasoning and a sketch of how they would be done.
2. **Real blind spots** — cases driftwatch genuinely cannot see, and what an
   operator should watch instead.

Every `t.Skip()` in the test suite must be linked to an entry here (§1.1.2).
A skipped test with no entry is a lie.

An honest blind spot that is documented is acceptable. An undocumented one is
not.

---

## G-001 — The oracle uses ~640 MiB per million keys, against a 512 MiB budget

**Kind:** Real limitation. Success criterion S5 (§3.3) budgets 512 MiB for
tracking 1,000,000 keys. The measured figure at the end of Phase 1 is
**640.5 MiB**, about 25% over.

**Measured by:** `BenchmarkOracleMemory1M`, on a Ryzen 7 6800HS.
Evidence: `docs/evidence/phase1-benchmarks.txt`.

**Where it goes.** Roughly 670 bytes per key, of which the largest single item
is the per-key history ring. Each `HistoryEntry` embeds a full `event.Event`,
which is about 200 bytes once the raw payload is dropped — two `time.Time`
values at 24 bytes each and five string headers at 16 apiece account for most of
it. The rest is the entry itself, the key string, and the Go map overhead for
the shard map and the settlement index.

Phase 1 already removed the two large avoidable costs, which is why the number
is 640 MiB and not 5.2 GiB: the ring now grows lazily instead of preallocating
sixteen slots for keys that have only ever seen one event, and the resulting
value is stored by reference rather than cloned into the ring on every apply.

**What an operator should do meanwhile.** Size `MaxTrackedKeys` against ~700
bytes per key rather than against S5's figure, and watch
`driftwatch_oracle_evictions_total` — the oracle degrades coverage rather than
exceeding its bound, so the failure mode is partial findings, not a crash.

**How it would be closed.** Store a compacted history record instead of a full
`Event`: `explain` needs the sequence number, epoch, publisher, operation, key,
member, both timestamps and the verdict, but not the topic, the TTL pointer, the
value bytes, or the raw payload. That is roughly 150 bytes per key back, which
brings a single-event-per-key workload inside the budget. It is deferred rather
than done now because `pkg/explain` (M13, Phase 5) defines which fields are
actually needed, and compacting against a guess would mean doing it twice.

Reducing the default `RingSize` would also work, and is the wrong fix: it buys
memory by throwing away the history that makes `explain` worth having, and it
does not help the workload measured here, where each key sees a single event.

**Not a gap:** the bound itself holds. Invariant I8 is proven by
`TestProp_MemoryBounded`, and the oracle never exceeds `MaxTrackedKeys`.
