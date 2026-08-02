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

## G-003, The e2e suite is at 23 of 27 specs; four scenarios have unverified fixes

**Status at the v0.1.0 cut:** `make e2e` is not green.

The last measured run was **23 specs passing, 4 failing** across the eight
scenarios, up from 20/5 at the start of Phase 9. E4, E5, E6 and E8 pass
outright.

The four remaining failures all have diagnosed causes and committed fixes; what
they do not have is a run confirming those fixes, because each cycle is eleven
minutes and the fixes were made after the last one.

| Scenario | Last measured | Cause | Fix, unverified |
|---|---|---|---|
| E1 HappyPath | coverage 0.8982 against a 0.90 bar | Coverage is measured against tracked keys, which grows until the whole keyspace has been touched once, so the oracle must *fill* as well as settle before the assertion runs | 40,000 keys at 800/sec, a 50s cycle, populated 50s in |
| E2 DroppedEventDetected | drift 0 | Not re-measured since the keyspace changed | 20,000 keys, a 50s cycle, so a skipped key stays wrong long enough to confirm |
| E3 SelfLossReportsSuspect | suspect 0 | §5.2 suspicion decays per key on the next event; at 2,000 keys and 600/sec the keyspace healed every 3.3s, faster than a sweep could look | 20,000 keys, a 33s cycle |
| E7 PublisherRestart | restarts_total 0 | **Not fully diagnosed.** See below |

E1, E2 and E3 are all the same mistake made three times independently: **a
scenario whose workload heals faster than the mechanism under test can observe
proves nothing, and fails in a way that reads as the mechanism being broken.**
The arithmetic is now written at each parameter rather than left implicit.

### E7 is the one that is genuinely open

The scenario deletes the publisher pod. The replacement comes up correctly, and
driftwatch never receives from it, `lastSeenSeconds` climbing past 92 while the
new pod emits 800 events/sec.

That much is [D-025](DISCOVERIES.md), and D-025's fix, an idle deadline on the
receive loop, is implemented, unit-tested against a stub socket that goes
silent, and confirmed to reach the socket from the spec through all four hops
(`TestCheck_TheIdleTimeoutReachesTheSource`). The effective-config dump from the
failing run shows `idleTimeout: 1m0s`, and the CRD carries the field.

**It nonetheless did not visibly fire in the cluster.** After 92 seconds of
silence against a 60-second deadline there is exactly one reconnect in the
manager log, the one at startup. Either the deadline is not firing, or the
session ends and the reconnect finds nothing to receive from, and the diagnostics
captured so far do not distinguish those.

So D-025 is a real bug with a fix that is right in principle and not yet proven
in situ. That is a weaker claim than the discovery entry implies on its own, and
this is where it is recorded.

### What this blocks

§22's "8 e2e scenarios pass; suite under 8 minutes; 5 consecutive clean runs" is
not satisfied and is not close: the suite currently takes about eleven minutes,
and no consecutive clean runs have been achieved.

The suite is also sized for the 16-core machine it was developed on rather than
the 2-core runner §14.5 specifies, which is a separate piece of work.
