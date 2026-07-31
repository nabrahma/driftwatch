# Discoveries

Things that turned out not to work the way they were expected to.

This file is a primary deliverable (PRD §1.1.7, §21.3), not a changelog. An
entry earns its place by being something a competent engineer would have got
wrong, with the evidence that proves it.

One entry per finding, **newest first**, in this form:

```markdown
## D-000 — One-line statement of the surprising behaviour

**Found:** Phase N, while doing X.

**What happened:** What was expected, what actually happened.

**Why it matters:** The consequence for driftwatch if it had gone unnoticed.

**Fix:** What changed as a result.

**Evidence:** `docs/evidence/D-000-slug.txt`

**Regression test:** `pkg/foo: TestSomething`
```

Rules, from §23 A11: every entry describes something that actually happened, and
every entry links to a real captured file in `docs/evidence/`. Nothing is
written here in anticipation.

---

## D-009 — A confirmed finding is a claim about one oracle version, and nothing was withdrawing it

**Found:** Phase 3, the first run of the I11 property test.

**What happened:** the sweeper confirms a finding, stores it, and clears it when
a later sweep finds the key agreeing again. That is the resolution path §9 M10
asks for, it is tested directly, and it works.

`TestProp_NoKeyIsEverBothInFlightAndReported` shrank a random interleaving to
seven steps and showed it is not enough:

```text
apply("a", "x")     the oracle learns key a
advance 1s
advance 1s          a is settled
sweep               target has no a  -> candidate raised
advance 1s
sweep + confirm     still missing    -> CONFIRMED
apply("a", "x")     one more event
```

The confirmation was correct when it happened. The last event put the key back
inside its settlement window, and nothing touched the finding — so between that
event and the next sweep, key `a` was simultaneously in the in-flight set and
reported as divergent. That is invariant I11, and it is the exact false positive
the settlement window exists to prevent.

**Why it matters:** the window is only worth having if it covers every path that
can report a key, and I had been thinking of it as a filter applied at one point
in time — the sweep. It is not. A finding survives between sweeps, so it is a
standing claim, and a standing claim has to be withdrawn by anything that
invalidates it. An event arriving is such a thing, and the sweeper is never told
an event arrived.

The size of the hole depends on the sweep interval: with the default 30s sweep
and a 5s window, a key that receives an event just after being confirmed spends
up to 30 seconds reported as drift while it is legitimately in flight. Every
per-key test passed, because the only way to see it is to interleave an event
with a confirmation and look in between.

**Fix:** a finding records the oracle version it was raised against, so the test
is exact rather than heuristic: if the oracle has moved past that version, the
claim is about an expectation that no longer exists and is withdrawn. The next
sweep establishes a fresh one on the new value, with its own two reads.

The withdrawal happens lazily, in `liveEpisodes`, on every read of `Confirmed()`
or `Episodes()`. Doing it in the sweep would leave the same gap in miniature —
correct only just after a sweep — and there is no event the sweeper could hang
it on instead.

It is counted as `ConfirmedSuperseded`, apart from `DriftResolved`. Withdrawing
a claim because the question changed is not the target having been repaired, and
merging the two would make the repair rate rise with the event rate.

This is the same rule the fence applies within a sweep and the confirm loop
applies to a waiting candidate (§5.5, I12). It was implemented in both of those
places and missing from the third, which is what a property test is for.

**Evidence:** `docs/evidence/D-009-superseded-finding.txt`

**Regression test:** `pkg/sweeper: TestProp_NoKeyIsEverBothInFlightAndReported`,
`TestSweeper_ANewEventWithdrawsAConfirmedFinding`

---

## D-008 — Discarding timed-out probes shrinks the settlement window 12x, and only during an outage

**Found:** Phase 3, implementing the convergence estimator (M11).

**What happened:** the settlement window W is the p99 of measured
event-to-target convergence, times a safety factor. Probes that never converge
within `MaxPollDelay` have to be accounted for somehow, and discarding them is
the obvious choice: nothing was measured, the key may have been deleted, the
read may have been unlucky. Recording a number that was never observed feels
like fabricating data.

I measured what each choice does to W. 10,000 probes, `MaxPollDelay` 2s, safety
factor 3.

Under **uniform degradation** — the whole materializer slowing together, which
is how one naturally models "it got slow" — discarding barely matters:

```text
scenario           timeout%   W kept   W discarded   too small by
healthy                0.0%    478ms         478ms           1.0x
loaded                 0.1%   2.859s        2.807s           1.0x
struggling             5.6%       6s          5.4s           1.1x
badly degraded        22.8%       6s        5.815s           1.0x
```

At 23% timeouts the two answers are within 3% of each other. On this evidence
the decision does not matter and discarding is fine.

It is the wrong model. Materializers do not degrade uniformly; a shard goes
down, a replica disconnects, a consumer group stalls on one partition. Most
keys stay fast and a minority stop converging entirely:

```text
scenario                timeout%   W kept   W discarded   too small by
0.5% of keys wedged         0.4%    513ms         480ms           1.1x
1%   of keys wedged         1.1%       6s         477ms          12.6x
2%   of keys wedged         2.3%       6s         469ms          12.8x
5%   of keys wedged         5.5%       6s         480ms          12.5x
```

**W discarded does not move at all.** 0.5% wedged or 5% wedged, it sits at
~480ms, because the surviving observations are the healthy keys and the healthy
keys did not change. The estimator reports the system is fine while a twentieth
of the keyspace is not converging.

The two tables differ because of where the 99th percentile falls. Timeouts only
reach the p99 rank once they exceed 1% of observations — hence nothing at 0.5%
and a cliff immediately after. Under uniform degradation the keys just below the
timeout threshold are nearly as slow as the ones just above, so removing the top
1% costs almost nothing. Under a partial outage the distribution is bimodal:
removing the timeouts removes the entire failure mode, and what is left is a
clean measurement of the half of the system that was never broken.

**Why it matters:** the error points the wrong way and is self-reinforcing. W
exists to absorb materializer slowness. Discarding timeouts makes W insensitive
to the one failure it exists to absorb, and the worse the outage gets, the more
of the tail is discarded. W is then ~12x smaller than the measurement supports,
and every key slow enough to exceed it is reported as drift — during an
incident, when the operator is already reading the output and deciding what to
trust. §23 A7's whole argument is that a tool which cries wolf under load gets
ignored, and this is the mechanism that would have made it do so.

The reason this is worth an entry is the first table. A reasonable engineer
models "slow" as everything slowing together, measures a 1.0x difference,
concludes the decision is unimportant, and discards — and is wrong for a reason
the measurement they took cannot show them.

**Fix:** a timed-out probe is recorded as an observation of `MaxPollDelay`
rather than discarded (`window.recordTimeout`). It is a floor, not a
measurement: the probe took *at least* that long, so W derived from it is
conservative in the safe direction. `Stats.TimedOut` reports the count
separately so a p99 that is really a wall of timeouts is legible rather than
hidden, and `Stats.Clamped` reports when the measured p99 wants more than
`MaxWindow` allows — past that point driftwatch is knowingly running with a
window it has measured to be too small.

**Evidence:** `docs/evidence/D-008-timeout-bias.txt`

**Regression test:** `pkg/lag: TestEstimator_TimedOutProbesAreRecordedNotDiscarded`,
`TestProp_TimedOutObservationsNeverLowerThePercentile`

---

## D-007 — The `<5 allocs/key` budget for batched reads is below the client's own floor

**Found:** Phase 2, writing `BenchmarkGetMany500`.

**What happened:** PRD §16.8 budgets fewer than five allocations per key for a
batched read. driftwatch measured 19. Before optimizing, I measured what the
mandated client costs on its own: a bare go-redis pipeline of 500 `GET`s, with
no driftwatch code in the path at all, allocates **16.03 per key**. Adding the
string-to-bytes copy that `event.Value` requires takes it to 17.03.

The budget is a third of the floor. It is not reachable with go-redis and a
pipelined `GET`, which are both things §8.2 and §9 M8 specify.

`MGET` costs 8.04 per key — half — but it is a different command that M8 does
not specify and that cannot span cluster slots, so it is not a drop-in.

**Why it matters:** less for the number than for what a wrong budget does to a
test. An assertion that cannot pass gets one of two treatments: it is deleted,
or it is quietly loosened until it passes, at which point it is measuring
nothing and nobody notices when the real regression arrives. Both outcomes are
worse than not having the test.

**Fix:** measure the thing driftwatch controls. `TestGetMany500AllocationBudget`
now measures the bare-client floor and driftwatch's `GetMany` in the same run
and asserts on the *difference*, budgeted at four allocations per key. The
current overhead is **1.01 per key** — the reply conversion into `event.Value`
and the per-key `Read` slot.

Measuring both in one run has a second benefit: a go-redis upgrade that moves
the floor in either direction changes both numbers together, so the assertion
keeps testing driftwatch rather than tracking its dependency.

**Evidence:** `docs/evidence/D-007-getmany-alloc-floor.txt`

**Regression test:** `pkg/target: TestGetMany500AllocationBudget`

---

## D-006 — A `FLUSHDB` mid-`SCAN` does not loop forever; it does something quieter and worse

**Found:** Phase 2, investigating the trap PRD §9 M8 warns about.

**What happened:** M8 says a `SCAN` cursor invalidated by a `FLUSHDB`
mid-iteration causes Redis to restart the cursor at 0, and that the loop must be
detected and aborted "rather than spinning forever". I built the detection, then
went to reproduce the behaviour it was defending against.

It does not reproduce. Ten thousand keys, `COUNT 100`, the keyspace destroyed
at various points — including flushed and refilled on every single call — and
every case terminated normally on both Redis 6.2 and 7.2. No cursor was ever
returned twice.

What actually happens is that the scan **finishes early and reports success**.
Flushing after the first call ended the iteration after 2 calls having seen 100
of the 10,000 keys: one percent of the keyspace, cursor 0, no error. From the
caller's side that is indistinguishable from a keyspace that genuinely contains
a hundred keys.

**Why it matters:** the danger the PRD anticipated is loud — a hung sweep is
obvious. The danger that is actually there is silent, and it points the wrong
way. driftwatch scans the target to find `extra_in_target` keys, and a scan
that silently returns 1% of the keyspace does not manufacture extras; it
*misses* them. So the failure mode is under-reporting, which is the safe
direction, and it is safe by accident rather than by design.

The reason it stays safe is §5.5's conservative extras rule: a key is only
reported as extra if it is still absent from the oracle on a re-read a full
settlement window later. A truncated scan produces fewer candidates, never more.
That rule was written to handle the non-atomicity of `SCAN`, and it turns out to
cover this too.

**Fix:** the loop detection stays, with its threshold raised from one repeat to
three. Since Redis does not do this, the detection now guards against a
Redis-compatible server that implements the cursor differently — Valkey, KeyDB,
Dragonfly, a managed emulation — rather than against Redis itself, and aborting
a legitimate scan on a single unexpected repeat would be a worse failure than
the one being prevented. The 1,000,000-call cap remains as the backstop.

The early-termination behaviour is documented on `redisIterator` so the next
person does not go looking for the hang either.

**Evidence:** `docs/evidence/D-006-scan-flushdb.txt`

**Regression test:** `pkg/target: TestRedisIntegration_ScanSurvivesAFlushMidIteration`
(build tag `integration`, runs against both versions)

---

## D-005 — `INFO` with several sections works on Redis 7 and fails on Redis 6

**Found:** Phase 2, writing `Health`.

**What happened:** `Health` needs fields from the `stats`, `memory`,
`replication` and `server` sections, so it asked for them in one call:
`INFO stats memory replication server`. That works against Redis 7. Against
Redis 6.2 it fails with `ERR syntax error`, because Redis 6 accepts at most one
section argument — multi-section `INFO` arrived in Redis 7.0.

Worth noting where the two fakes disagree: miniredis rejects the same call with
`ERR wrong number of arguments for 'info' command`. Same outcome, different
message, and neither matches the other. Anything that matched on the error text
rather than on the failure would have passed against the fake and broken against
the server.

**Why it matters:** it is a total failure of `Health` on the older version, and
`Health` is what tells the sweeper the store is reachable. A driftwatch that
cannot read health against Redis 6 reports the target as unavailable, which by
§6.4 suppresses divergence reporting entirely — the tool would run, look fine,
and detect nothing.

It also fails in the least helpful place. `INFO` is the first thing `Health`
calls, so the error is the operator's first experience of pointing driftwatch at
a Redis 6 instance.

**Fix:** call bare `INFO`, which returns every default section on both versions
and is a superset of what `Health` needs. The parser reads fields by name and
ignores section boundaries entirely, so it does not care which sections arrive
or in what order — which also means it survives fields moving between sections
in a future version, the other half of the same problem.

**Evidence:** `docs/evidence/D-005-info-sections.txt`

**Regression test:** `pkg/target: TestRedisIntegration_HealthParsesBothVersions`

---

## D-004 — A strict read-only allowlist refuses the client library's own handshake

**Found:** Phase 2, wiring the `redis.Hook` that enforces read-only access.

**What happened:** the allowlist in PRD §5.8 I13 names twelve commands, all of
them keyspace reads. Enforcing exactly that list made every single read fail
with `mutating command attempted on a read-only target`.

There are two layers of this, and I hit the second one first. Enforcing the I13
list verbatim refuses `HELLO`, the RESP handshake, so the connection dies before
a single key is read. My implementation already permitted `HELLO` — it is
obviously not a write — so what actually bit me was the next one along:
`CLIENT MAINT_NOTIFICATIONS`, which go-redis v9.17 issues during connection
setup. It sends `CLIENT SETINFO` too.

driftwatch asks for none of them and cannot decline them; they are how the
client establishes a connection. Fixing the first refusal just reveals the next,
which is what makes this worth writing down rather than patching quietly: the
set is not guessable, it is discovered one failure at a time, and it changes
with the client version.

**Why it matters:** the failure is a self-inflicted denial of service with a
maximally misleading error message. driftwatch cannot read anything at all, and
the message says a *write* was attempted — sending whoever is debugging it to
look for a mutation in a codebase whose entire design is that it never mutates.
It would have been found in Phase 7 against a real cluster, after the cause had
been buried under six phases of other work.

**Fix:** two allowlists rather than one. `readOnlyCommands` holds the data-plane
reads, matched on the verb. `connectionCommands` holds the connection-scoped
commands a client issues on driftwatch's behalf, matched in full including the
subcommand — so `CLIENT SETINFO` is permitted while `CLIENT KILL` is not, and
`CLUSTER SLOTS` is permitted while `CLUSTER RESET` is not. Nothing in the second
list can read or modify a byte of keyspace data.

The general lesson is worth keeping: an allowlist has to cover the commands the
library sends, not only the commands the application calls, and the two sets are
discovered at different times.

**Evidence:** `docs/evidence/D-004-client-handshake.txt`

**Regression test:** `pkg/target: TestRedis_RecordingTargetSeesTheCommandsTheClientSends`,
`TestRedis_TheHookRejectsAWriteEvenWhenCalledDirectly`

---

## D-003 — Enforcing a global key budget per shard silently loses ~0.3% of the capacity you configured

**Found:** Phase 1, while benchmarking the oracle at a million keys.

**What happened:** The oracle bounds memory with `MaxTrackedKeys`, and enforces
it as a per-shard budget so that eviction stays shard-local and no code path
ever needs two shard locks. Seeding an oracle configured for exactly 1,000,000
keys with exactly 1,000,000 distinct keys left **996,885** tracked. 3,115 keys
had been evicted while the oracle was, globally, not full.

The cause is balls-into-bins, not a bug in the eviction path. Hashing a million
keys across 64 shards gives a fair share of 15,625, but the observed loads range
from 15,306 to 15,880. Every key that lands on an over-subscribed shard is
evicted while under-subscribed shards sit idle. The effect is worse with more
shards and fewer keys, because the relative deviation grows as the bins get
smaller — 0.31% at 64 shards and a million keys, 1.91% at 256 shards and a
hundred thousand.

**Why it matters:** Two reasons, and the second is the one that would have hurt.

The obvious one is that an operator who sizes `MaxTrackedKeys` to their exact
keyspace silently gets less coverage than they asked for. `coverage_ratio` would
show it, but only if someone looked.

The subtle one is what it does to a *test*. The natural benchmark asserts a
million keys go in and a million keys are tracked, and that assertion fails —
which reads as an eviction bug. Chasing a correctness bug that is really a
statistical property of sharding is the expensive kind of wrong turn.

**Fix:** No change to the eviction design; the alternative — a global atomic
counter with cross-shard eviction — needs two shard locks at the exact moment
the applier is saturated, and trades a bounded, measured 0.3% for a lock
ordering that has to be got right forever.

Instead the effect is documented where it is configured, on
`Config.MaxTrackedKeys`, with the recommendation to allow a few percent of
headroom. `TestOracle_PerShardBudgetsCostSomeCapacityToHashImbalance` pins the
loss so that a change to the hash or the sharding shows up as a number rather
than as a surprise.

**Evidence:** `docs/evidence/D-003-shard-budget-imbalance.txt`

**Regression test:**
`pkg/oracle: TestOracle_PerShardBudgetsCostSomeCapacityToHashImbalance`

---

## D-002 — A JSON sequence number above 2^53 is silently corrupted by any decoder that goes through float64

**Found:** Phase 1, while implementing the json codec.

**What happened:** `encoding/json` is safe here *only* if the destination is a
typed `uint64` field — then it parses the digits directly and
`{"seq":9007199254740993}` survives. Decode the same payload into
`map[string]any` and the number arrives as `float64`, which has 53 bits of
mantissa, so it comes back as `9007199254740992`. Off by one, no error.

The trap is that M3 requires the JSON field names to be **configurable**, so a
foreign producer can be read without a code change. A fixed Go struct with
`json:"seq"` tags cannot express that. The natural implementation is therefore
`map[string]any` or `map[string]json.RawMessage` — and the first of those is
exactly the unsafe one. The requirement that makes the codec useful is the
requirement that steers you into the bug.

**Why it matters:** The sequence number is the single value driftwatch uses to
decide whether an event was lost. An off-by-one in `seq` manufactures a gap that
never happened, which marks keys `Suspect` and suppresses real findings, or
hides a gap that did happen. Either way driftwatch reports confidently on state
it has silently misread — the exact failure mode it exists to detect in other
systems.

2^53 is 9.007e15. A publisher emitting 100k events/sec reaches it in about
2,850 years, so this is not reachable by counting. It is reachable immediately
by any producer that derives `seq` from a timestamp in nanoseconds (1.78e18
today), from a Snowflake-style ID, or from a hash — all of which are ordinary
choices.

**Fix:** `pkg/codec` uses a hand-written scanner that parses `seq` and `epoch`
from their raw digits with `strconv.ParseUint`, never through `float64`. A
number written with a fraction or an exponent is rejected outright with
`ErrMalformed` rather than rounded, because a producer sending `1e300` as a
sequence number is misconfigured and should be told so. Numeric strings are
accepted, since sending large integers as strings is what a careful producer
does.

**Evidence:** `docs/evidence/D-002-json-float64-seq.txt`

**Regression test:** `pkg/codec: TestJSON_Decode/a_seq_above_2^53_survives...`,
`.../a_seq_written_as_a_float_is_rejected...`

---

## D-001 — Go's `time.RFC3339` layout rejects the lowercase `t` and `z` that RFC 3339 permits

**Found:** Phase 1, while writing an allocation-free RFC3339 parser for the json
codec.

**What happened:** The codec has two timestamp paths: a byte-level fast path for
the common case, and a fallback through `time.Parse` for escaped strings. A
differential test asserting the two agree failed on `2026-07-30t11:02:31z`. The
hand-written parser accepted it — RFC 3339 §5.6 says the separators "MAY be
lower case" — but `time.Parse(time.RFC3339, ...)` rejects it, because Go's
layout string matches `T` and `Z` literally.

**Why it matters:** Not because of the lowercase form itself, which is rare.
Because the fast path was a *superset* of the fallback. The two paths are
selected by whether the payload happens to contain a backslash anywhere, so the
same timestamp would have decoded successfully in one event and failed in
another for a reason entirely unrelated to the timestamp. That class of bug —
two code paths that agree on everything you thought to test — is why the
differential test was written before the optimization was trusted.

**Fix:** The fast path matches Go's behavior exactly, rejecting the lowercase
forms so that it can only ever be faster than the fallback, never more
permissive. The constraint is stated on `parseRFC3339` so the next person to
optimize it knows it is load-bearing rather than an oversight.

**Evidence:** `docs/evidence/D-001-rfc3339-lowercase.txt`

**Regression test:** `pkg/codec: TestJSON_RFC3339FastPathAgreesWithTimeParse`,
`TestJSON_RFC3339RejectionsFallThroughAndAreReportedHonestly`
