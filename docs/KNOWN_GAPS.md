# Known gaps

Limitations driftwatch has, stated plainly.

Two kinds of entry belong here (PRD §3.2, §21.5):

1. **Deliberate scope boundaries**, things not built on purpose, with the
   reasoning and a sketch of how they would be done.
2. **Real blind spots**, cases driftwatch genuinely cannot see, and what an
   operator should watch instead.

Every `t.Skip()` in the test suite must be linked to an entry here (§1.1.2).
A skipped test with no entry is a lie.

An honest blind spot that is documented is acceptable. An undocumented one is
not.

---

## G-001, The oracle uses ~640 MiB per million keys, against a 512 MiB budget

**Kind:** Real limitation. Success criterion S5 (§3.3) budgets 512 MiB for
tracking 1,000,000 keys. The measured figure at the end of Phase 1 is
**640.5 MiB**, about 25% over.

**Measured by:** `BenchmarkOracleMemory1M`, on a Ryzen 7 6800HS.
Evidence: `docs/evidence/phase1-benchmarks.txt`.

**Where it goes.** Roughly 670 bytes per key, of which the largest single item
is the per-key history ring. Each `HistoryEntry` embeds a full `event.Event`,
which is about 200 bytes once the raw payload is dropped, two `time.Time`
values at 24 bytes each and five string headers at 16 apiece account for most of
it. The rest is the entry itself, the key string, and the Go map overhead for
the shard map and the settlement index.

Phase 1 already removed the two large avoidable costs, which is why the number
is 640 MiB and not 5.2 GiB: the ring now grows lazily instead of preallocating
sixteen slots for keys that have only ever seen one event, and the resulting
value is stored by reference rather than cloned into the ring on every apply.

**What an operator should do meanwhile.** Size `MaxTrackedKeys` against ~700
bytes per key rather than against S5's figure, and watch
`driftwatch_oracle_evictions_total`, the oracle degrades coverage rather than
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

### Update, Phase 8, the 640 MiB figure was measured before the rings filled

The benchmark above writes one event per key, so every history ring holds a
single entry. That is the best case, and the sentence "it does not help the
workload measured here, where each key sees a single event" was the clue.

The Phase 8 soak measured the other end. 500,000 keys at 5,000 events/sec, real
Redis, `ringSize: 16`, `retainRaw: false`:

```text
t=1m   keys=300,098  rss=384 MiB
t=2m   keys=500,000  rss=873 MiB
t=3m   keys=500,000  rss=1,289 MiB
t=4m   keys=500,000  rss=1,676 MiB
t=5m   keys=500,000  rss=2,215 MiB
t=6m   keys=500,000  rss=2,462 MiB
```

The key count is at its ceiling from t=2m and memory keeps climbing, because
each key's ring only fills after that key has been touched sixteen times, 
`ringSize × keys / rate`, which is 1,600 seconds at these parameters. At t=6m
the rings are roughly a third full and the process is already at 2.4 GiB.
Extrapolating the remaining fill puts the steady state near **8 GiB at 500,000
keys**, or about 16 KiB per key rather than 670 bytes.

So the real shape of this gap is: **~670 bytes per key with one event of
history, and roughly 16 KiB per key once sixteen events have accumulated.** S5's
512 MiB budget is met only at the former. The run was stopped at six minutes
rather than allowed to exhaust the machine, so the 8 GiB figure is an
extrapolation from six measurements and not itself measured.

The remedy is the one already named above, a compacted history record instead
of a full `Event`, and it is now worth considerably more than the 150 bytes per
key estimated in Phase 1, because it applies to every ring slot rather than to
one. `pkg/explain` has since defined which fields are actually needed, so the
work is no longer blocked on a guess.

**What an operator should do meanwhile,** revised: size against ~16 KiB per key
for a workload where keys are touched repeatedly, or lower `policy.ringSize`.
Dropping it from 16 to 4 cuts the ring cost by three quarters and still leaves
`driftwatch explain` the four most recent events for a key, which is usually
enough to see a pattern.

See `docs/DISCOVERIES.md` D-022. Evidence:
`docs/evidence/S2-soak-capacity-500k-keys.txt`.

---

## G-002, Reordering at the very start of a publisher's stream cannot be detected

**Kind:** Real blind spot, inherent rather than unimplemented.

The reorder buffer added in Phase 6 (see D-014) restores sequence order within a
bounded window, so an adjacent pair delivered out of order folds into the same
oracle state a correctly-ordered stream would. It works by knowing which
sequence number it expects next.

It has nothing to expect from the first event a publisher sends. A stream whose
first two events arrive as seq 2 then seq 1 is indistinguishable, at the moment
seq 2 arrives, from a stream that legitimately begins at seq 2, which is the
normal case, because driftwatch attaches to a publisher that has been running
for hours and adopts whatever sequence it first sees as a baseline (§5.2).

**What it costs.** For a non-commutative projection, if the very first two
events driftwatch sees from a publisher are reordered *and* they touch the same
key *and* their order matters, `add m` then `remove m`, not `add m1` then
`add m2`, the oracle can hold a value the store does not. That key stays wrong
until a later event overwrites it.

**What an operator should watch.** Nothing specific: the window is one pair of
events at attach time, per publisher, per restart. If it matters, bootstrap
`Strict` closes it completely, driftwatch asserts nothing until a publisher
retransmits its state, which supersedes anything mis-folded at attach.

**How it would be closed.** It cannot be, from the subscriber side, without the
publisher declaring where its stream starts. An explicit `snapshotBegin` at
startup does exactly that, which is what bootstrap `Strict` requires and why
that mode exists.

**Asserted, not assumed:** `test/faults: TestFault07_AdjacentPairReorderedOnDriftwatch`
seeds a stream before reordering the pair, with a comment saying why. The
limitation is in the test rather than hidden by it.

---

## G-003, The e2e suite is green once, not the five consecutive runs §22 asks for

**Status at the v0.1.0 cut:** `make e2e` passes. **34 of 34 specs, 0 failed, 0
skipped, in 9m14s**, on 2026-08-03. That is the first fully green run the suite
has had, up from 20/5 at the start of Phase 9.

What it is not is five of them.

```text
Ran 34 of 34 Specs in 553.842 seconds
SUCCESS! -- 34 Passed | 0 Failed | 0 Pending | 0 Skipped
```

### What is still open

§22 asks for “8 e2e scenarios pass; suite under 8 minutes; 5 consecutive clean
runs”. One of those three is met.

| §22 criterion | State |
|---|---|
| 8 scenarios pass | Met. All eight, all 34 specs |
| 5 consecutive clean runs | **1 of 5.** No run has yet been repeated without a change in between |
| Suite under 8 minutes | **9m14s.** Over by 74 seconds |

The suite is also only measured on a 16-core developer machine and a 2-core
GitHub runner, which have produced materially different failures from the same
commit — see [D-028](DISCOVERIES.md). A green run on one says less about the
other than it looks like it does.

### Why the last four scenarios took so long

Every one of the six failures resolved in the final pass was a scenario
measuring something other than what it claimed to, and they fell into three
kinds worth naming, because the same mistakes are easy to repeat.

**A workload that forbade the assertion.** E3 asserts that driftwatch marks
keys suspect when its own subscription is cut. Its publisher only ever emitted
`add` of the same member to the same key, so every event after a key’s first
was a no-op on the store and on the oracle alike. A missed event could not
change anything, so the divergence the scenario asserts on was impossible to
produce. It had been failing for a reason that had nothing to do with the code
under test. See [D-027](DISCOVERIES.md).

**Sizing that depended on luck.** Four scenarios ran with a key cycle shorter
than their settlement window — 1.5 to 1.9 seconds against 3 — and passed because
a uniform random key draw leaves about a fifth of the keyspace quiet at any
instant by chance. Walking the keyspace in order, so the `keys/rate` arithmetic
every sizing comment already assumed was actually true, removed the luck and
left the arithmetic, which had always said nothing could settle.

**A gate that gated nothing.** E3 waited on `trackedKeys` before cutting the
subscription, to guarantee the partition landed on keys the oracle knew.
Bootstrap is `Adopt`, so a check created against a store the publisher has
already filled reads the whole keyspace out of Redis in seconds and satisfies
that without a single event. Whether it did depended on how long the fixture
took to come up — which is why the scenario passed alone and failed in a full
suite. Both E1 and E3 now wait on `eventsApplied`.

None of the three fails cleanly. All three read as a defect in whatever the
assertion happened to be about.

### What it did find

The run that closed this gap also produced [D-029](DISCOVERIES.md), which is a
real product defect and not a test one: a healthy check publishes
`TargetAvailable=False` and goes `Degraded` every `extraScanInterval` — five
minutes under the shipped default — because the extras pass reports target
health it never measured.

