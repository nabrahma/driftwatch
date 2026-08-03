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

## G-003, The e2e suite meets §22, at nine minutes against an eight-minute budget

**Status at the v0.1.0 cut:** `make e2e` passes, five consecutive times, on an
unchanged tree.

```text
run 1: exit=0 wall=557s | 34 Passed | 0 Failed | 0 Pending | 0 Skipped
run 2: exit=0 wall=565s | 34 Passed | 0 Failed | 0 Pending | 0 Skipped
run 3: exit=0 wall=596s | 34 Passed | 0 Failed | 0 Pending | 0 Skipped
run 4: exit=0 wall=590s | 34 Passed | 0 Failed | 0 Pending | 0 Skipped
run 5: exit=0 wall=564s | 34 Passed | 0 Failed | 0 Pending | 0 Skipped
```

Full capture, including the cache check that makes it evidence rather than five
identical assertions:
[`S-E2E-five-consecutive-clean-runs.txt`](evidence/S-E2E-five-consecutive-clean-runs.txt).

| §22 criterion | State |
|---|---|
| 8 scenarios pass | Met. All eight, all 34 specs |
| 5 consecutive clean runs | Met |
| Suite under 8 minutes | **Not met. 9m17s to 9m56s** |

### The eight minutes

The budget is not met and will not be, and it is worth saying why rather than
quietly moving it.

§14.5's eight minutes was written before the scenarios were sized. Sizing them
established three constraints nobody had stated: a key cycle has to be longer
than the settlement window or nothing settles; the oracle has to fill before
coverage means anything, which costs a full cycle; and a fault has to stay
observable for longer than detection takes. Those forced roughly 20-second
cycles, and the cycles are not negotiable in the way the budget is — shortening
them puts the scenarios back to proving nothing.

The runner is not the reason. These are 16-core numbers; a 2-core GitHub runner
was at 9m04s before the cycles were lengthened at all. Cluster creation, two
image builds and eight fixture bring-ups dominate the total and none of them get
faster with cores. That is also where the time should come from: reusing one
Redis across scenarios that do not destroy it, or overlapping fixture bring-up
with the image build. Neither is done.

### What five runs cost, and why the number is five

Six attempts. Each of the first five died on a different real defect, none of
which a single green run would have surfaced:

| What failed | What it was |
|---|---|
| The materializer exited when E7 rescheduled the publisher | [D-030](DISCOVERIES.md) — D-011's DNS bug, in the harness |
| Four of five "clean runs" | Go test-cache replays; `-count=1` was missing from every target that stands up infrastructure |
| E7's convergence assertion | Treated the first observed zero as permanent; convergence is a limit |
| The fixture's deployment wait | The one setup step never retried, in a file whose header says all setup may be |
| Pausing a check | Discarded the oracle. A real product bug — see below |
| The image build | Could not survive a DNS failure resolving the base image manifest |

The pause one was not a test problem. `policy.paused` was read once at
construction, so the only way to apply it was to replace the runner — and
replacing a runner discards its oracle, which is the single thing pause exists
to keep. `Adopt` bootstrap hid it: the rebuilt check re-read the whole keyspace
from Redis and `trackedKeys` came straight back, so the scenario asserting
"pauses without discarding the oracle" passed over an oracle that had been
discarded and re-adopted from the very store it audits. It is now applied to the
live check, and the scenario checks `eventsApplied`, which adoption cannot fake.

Three of the six are the same shape: **a scenario asserting something its own
fixture could not deliver, passing anyway.** A green run says nothing about
faults the workload cannot express, and neither the test nor the fixture says
which those are.

---

## G-004, One §16.8 benchmark target is not met: batch reads allocate 7 per key against a target of 5

**Status at the v0.1.0 cut:** nine of §16.8's ten targets are met and enforced by
`hack/verify-benchmarks.sh` in CI. This is the tenth.

| | |
|---|---|
| Target | fewer than 5 allocations per key |
| Measured | **7.04** per key, against a real Redis |
| Where | `BenchmarkGetMany500Real`, `pkg/sweeper`, integration tag |

### The number that was there before was worse and meant less

`BenchmarkGetMany500` runs against miniredis, which is a Redis server written in
Go running in the same process. Its RESP parsing and reply construction are
counted alongside the client's, so it reports about **19 allocations per key** —
a figure that is mostly not driftwatch's and that no amount of work on
driftwatch would move.

§16.8 describes this benchmark as "dominated by network", which is the tell: the
target was written about a real server, where the server's allocations happen in
another process entirely. `BenchmarkGetMany500Real` measures it there.

### Where the seven go

Roughly six of them are inside go-redis, per command in the pipeline: a
`StringCmd`, its argument slice, and the reply string. The seventh is
driftwatch's, converting that reply string into the `[]byte` an `event.Value`
holds.

Getting under five means not using go-redis's command layer — issuing raw
commands against a preallocated argument buffer and decoding replies directly.
That is a rewrite against a library boundary rather than a tuning exercise, and
it trades away the reconnect, cluster and sentinel handling that comes with the
client.

### What was done instead

The read path no longer copies every result twice. `readChunk` used to allocate
its own slice per batch which was then appended into the caller's, copying every
`Read` — a struct carrying a byte slice and a member map — a second time. Each
chunk now fills its own window of the final slice directly.

Measured on the same benchmark, 500 keys per batch:

```text
before   1996492 ns/op   3993 ns/key   222106 B/op   3522 allocs/op
after    1818762 ns/op   3638 ns/key   189281 B/op   3521 allocs/op
```

**15% less memory and 9% less time per key**, in the path S6 measures. It does
not move the per-key allocation count, because what it removed was per batch
rather than per key.

### Why this is recorded rather than asserted

`hack/verify-benchmarks.sh` deliberately does not assert this target. Asserting
the miniredis number would look like enforcement while measuring the wrong
thing, and relaxing the target to fit would be moving the goalposts quietly.

Regressions are still caught: the benchstat gate fails on **any** increase in
allocations against the committed baseline, so 7.04 cannot drift to 8 unnoticed.
What it will not do is tell anyone this reached 5.

