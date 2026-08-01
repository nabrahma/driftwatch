# How driftwatch avoids being wrong

This document explains why the obvious way to detect drift does not work, and
what driftwatch does instead. It assumes no knowledge of the codebase.

The short version: a detector that reports things which are not wrong is worse
than no detector at all, because it trains the person reading it to ignore it.
Most of the engineering in this project is spent not on finding divergence but
on refusing to claim it.

---

## The problem

Some systems keep a derived copy of state. An event stream says what happened; a
separate process reads that stream and writes the result into a store. A
KV-cache index is the case this was built for: inference replicas publish "I now
hold block X", a materializer folds those into a Redis set per block saying
which replicas have it, and a router reads Redis to decide where to send a
request.

Nothing checks that Redis still matches the events. If the materializer drops a
message, mishandles a delete, or falls behind and never catches up, the index is
quietly wrong. Requests go to replicas that no longer hold the block. There is
no error, no alert and no log line — the system is behaving correctly according
to a store that is lying to it.

driftwatch subscribes to the same event stream, independently folds it into what
the store *should* contain, and compares.

---

## The naive approach, and why it fails

The obvious design is three lines of pseudocode:

```text
for each event:      oracle[key] = fold(oracle[key], event)
every 30 seconds:    for each key: if oracle[key] != store[key]: report(key)
```

This does not work. Not "needs tuning" — it produces so many false reports that
the output is meaningless.

### A worked example

Take a modest deployment: **10,000 keys**, **2,000 events/sec**, and a
materializer whose write latency is **p50 = 20ms, p99 = 400ms**. Those are
healthy numbers for a Redis-backed consumer.

The sweep reads all 10,000 keys and compares each against the oracle. The
question is: how many keys were written by an event driftwatch has already
folded but the materializer has not yet applied?

At 2,000 events/sec, during the 400ms the slowest 1% of writes take, **800
events are in flight**. Those events touch up to 800 distinct keys, and every
one of them shows the oracle ahead of the store.

**800 false positives per sweep out of 10,000 keys — an 8% false-positive
rate**, every 30 seconds. None of them are drift. All of them are a materializer
being normally, healthily, a few hundred milliseconds behind.

It gets worse: they are not the same 800 keys each time. They rotate, so an
operator watching a list of divergent keys sees it churn completely between
sweeps — which is what a real, spreading corruption would look like.

Sweeping less often does not help, because the in-flight window is set by write
latency rather than by how frequently you look. Comparing only keys that "look
settled" by their event timestamp does not help either, for reasons in
[Whose clock?](#whose-clock) below.

### The eight failure modes

The worked example is only the first.

| # | Failure | What the naive detector concludes |
|---|---|---|
| **F1** | Materializer lag — the store has not caught up yet | Drift, on every key currently in flight |
| **F2** | driftwatch itself missed events | Drift, on every key it can no longer vouch for |
| **F3** | Out-of-order delivery | Permanent drift for order-dependent folds |
| **F4** | The store evicted keys under memory pressure | Mass "missing" drift, when the store did what it was configured to do |
| **F5** | Publisher clock skew | Keys judged settled that are not, or never judged settled at all |
| **F6** | driftwatch restarted with no history | The entire pre-existing keyspace reported as drift |
| **F7** | TTLs expiring keys the oracle still holds | Drift on every expiry |
| **F8** | Comparison against a value the oracle has since superseded | Drift on a key that was correct at both instants |

**F2 is the one that matters most**, and the one a naive design gets exactly
backwards. When driftwatch loses events — a reconnect, a full buffer, a network
blip — its oracle goes stale while the store stays correct. Compare them and
driftwatch reports the store as wrong. The tool blames the system for the tool's
own failure, at precisely the moment it is least able to tell the difference.

---

## The six mechanisms

Each defeats specific failure modes. None is optional.

### 1. The settlement window — defeats F1

A key is compared only once it has gone unchanged for **W** seconds. Anything
touched more recently is *in flight*: driftwatch knows an event is on its way to
the store and declines to draw a conclusion.

W is derived from measurement rather than guessed. driftwatch tracks the delay
between the oracle learning a value and the store holding it, takes the 99th
percentile and multiplies by a safety factor (default 3). If p99 convergence is
400ms, W is 1.2 seconds.

Applied to the worked example: with W = 1.2s the 800 in-flight keys are excluded
from comparison. The false-positive rate goes from 8% to approximately zero, and
the cost is that genuine drift on a hot key takes an extra 1.2 seconds to
surface.

**The relationship that matters is W > p99 convergence.** While it holds, a key
that is merely behind is never compared. If the two cross — because the
materializer slowed and W hit its configured ceiling — false positives resume
immediately. That is why the Grafana dashboard plots them on one axis and why
`DriftwatchSettlementWindowAtMax` is an alert rather than a log line.

#### Whose clock?

Settlement uses **driftwatch's local receive time**, never the publisher's
timestamp. This is what defeats F5.

If settlement used publisher timestamps, a replica whose clock ran five minutes
fast would stamp its events in the future. Those keys would appear to have
settled long ago, and driftwatch would compare them the instant they arrived —
guaranteeing a false positive on every event from that publisher. A replica five
minutes slow would produce keys that never settled at all.

Publisher clock skew is still measured and exported, because it is worth
knowing. It simply cannot affect what gets compared, or when.

### 2. Sequence tracking and trust states — defeats F2

Every event carries a publisher identity, an epoch and a monotonic sequence
number. driftwatch tracks, per publisher, which sequence numbers it has actually
seen.

When a gap appears — 500 arrives after 498 — driftwatch knows it missed 499. It
does not know which key that event touched, so every key that publisher could
have written becomes **Suspect**.

| State | Meaning | Reported as |
|---|---|---|
| **Complete** | Derived from an unbroken event stream | Confirmed divergence — alert on this |
| **Suspect** | A gap means part of the history is missing | Suspect divergence — never alert |
| **Adopted** | Read from the store at startup, never verified | Not asserted on at all |

Findings on Suspect keys are counted separately and never merge into the
alertable number. This is the honesty mechanism, and it is the difference
between a tool that says "the store is wrong" and one that says "I cannot
currently vouch for these keys".

A sequence *reset* — the number going back to 1 — is read as a publisher restart
rather than as several hundred thousand missing events, provided the epoch
moved. Getting that wrong marks the whole keyspace suspect every time a
publisher is rescheduled.

### 3. Two-phase confirmation — defeats residual F1

A key seen to disagree once is a **candidate**, not a finding. It is re-read
after a further settlement window and reported only if it still disagrees.

This catches whatever the window's estimate missed — a convergence distribution
that shifted, or one unusually slow write. The first read sees a disagreement,
the second sees it resolved, and the candidate is recorded as a **transient**
that never reaches the alertable count.

A healthy pipeline produces transients constantly. Their *absence* alongside
real traffic is more suspicious than their presence: it usually means W is so
wide that nothing is being compared while it still moves.

### 4. Version fencing — defeats F8

Between the first read and the confirming read, an event may arrive that changes
the oracle's expectation for that key. The candidate was raised against a value
the oracle has since superseded.

Every oracle entry carries a version that increments on change. A candidate
records the version it was raised against, and confirmation checks it first: if
the version moved, the finding is *withdrawn* rather than confirmed, and the key
— now in flight again — is reconsidered on a later sweep.

Without this, a key updated at exactly the wrong moment produces a confirmed
finding describing a disagreement that was never simultaneously true.

### 5. Bootstrap modes — defeat F6

At startup the store already holds contents driftwatch has seen no events for.
Three answers, because different deployments need different ones:

| Mode | Behaviour | Cost |
|---|---|---|
| **Adopt** | Read the store once as a baseline; never assert on those keys | Coverage of the pre-existing keyspace is zero until events touch it |
| **Wait** | Believe nothing; track only what events prove | Useless until the stream has touched enough of the keyspace |
| **Strict** | Assert nothing until a publisher retransmits its full state | Needs a producer that can snapshot on demand |

Adopt is the default because it is the only one useful within seconds of
starting. What it must not do — and what an earlier version of this code did —
is then claim to have *verified* those keys. Comparing an adopted key against
the store proves only that the store agrees with itself, so adopted keys are
excluded from both the comparison and the coverage ratio.

### 6. Eviction and expiry correlation — defeats F4 and F7

A key missing from the store looks identical whether the materializer failed to
write it or the store evicted it under memory pressure. The remedies are
opposite: one is a bug to investigate, the other a capacity setting to change.

driftwatch reads the store's own eviction and expiry counters during each sweep.
A sweep finding mass absence while those counters climb has an explanation that
is not drift, and `driftwatch explain` says `TARGET_EVICTION_LIKELY` rather than
leaving the operator to guess.

For TTLs, `expiryPolicy` picks a position: `Strict` reports every absence
(correct for an index with no TTLs), `Ignore` suppresses absences once the
oracle's copy is older than an assumed TTL, and `Model` expects events to carry
TTLs so the oracle can expire keys itself.

---

## Ordering, and the folds that care

F3 is defeated by a bounded reorder buffer — but only for the folds that need
one.

A **commutative** fold gives the same result whatever order events arrive in.
Set membership is commutative: adding A then B is the same set as B then A. For
these, reordering loses ordering but not information, and no correction is
needed.

A **non-commutative** fold does not. Last-write-wins scalars, and counters that
accept absolute values, both converge to different answers under different
orderings. For these, an event arriving ahead of its predecessor is held — for a
bounded window, up to a bounded count — until either the predecessor arrives or
the wait expires. When it expires the hole is a real gap, and sequence tracking
says so.

The bound in both directions is the point. Holding forever turns one lost
message into a permanently stalled publisher; releasing immediately produces a
permanently wrong oracle.

---

## What remains undetectable

These are limits of the approach, not gaps in the implementation. A tool that
did not state them would be overselling itself.

**Drift that predates driftwatch, under Adopt.** Keys read at startup are taken
on trust. If the store was already wrong when driftwatch attached, the oracle is
wrong in the same way and the two agree perfectly. Only an event touching a key
promotes it to something driftwatch can vouch for. `Strict` closes this, at the
cost of asserting nothing until a full retransmission.

**Reordering at the very start of a stream.** Sequence tracking establishes its
baseline from the first event it sees from a publisher. If the first two arrive
out of order there is nothing to be out of order *relative to* — the one seen
first simply becomes the baseline. This is G-002 in `KNOWN_GAPS.md`.

**Divergence inside the settlement window.** By construction. A key that is
wrong and then rewritten correctly within W is never compared in its wrong
state. A deliberate trade: catching it would mean surrendering the mechanism
that defeats F1.

**Whether the event stream itself is correct.** driftwatch compares the store
against the events. If a publisher emits wrong events, the store and the oracle
agree on the wrong answer and no drift is reported. It audits consistency
between two representations, not the truth of either.

**Keys outside the configured pattern.** A `keyPattern` narrower than the real
keyspace leaves the remainder unaudited; wider, and everything else is reported
as an extra. The coverage ratio makes the first case visible.

**Anything at all, while the store is unreachable.** No comparison runs, so no
new findings appear and the counts freeze at their last known values. This is
deliberate — absence of data is not evidence of divergence — and it means a
drift alert coinciding with a store outage should be read as the drift having
come first.

---

## The invariants

Fourteen properties that must hold for the above to be sound. Each has a
property test running 10,000 randomised cases.

| ID | Invariant |
|---|---|
| **I1** | Applying an event twice yields the same oracle state as applying it once. |
| **I2** | For a commutative projection, any permutation of the same event set yields identical final state. |
| **I3** | For a non-commutative projection, applying events in sequence order yields the canonical state, and out-of-order delivery converges to it once all events are delivered. |
| **I4** | A sequence gap is never missed: if any event is withheld, the gap set contains its sequence number. |
| **I5** | No event is ever double-counted in `driftwatch_events_received_total`. |
| **I6** | The differ reports empty if and only if oracle and target agree for all settled, Complete-trust keys. |
| **I7** | Confirmed divergence implies the target genuinely disagreed at two points separated by at least W. |
| **I8** | Oracle memory is bounded: tracked keys never exceed `maxTrackedKeys`, and the per-key ring never exceeds `ringSize`. |
| **I9** | Gap-set interval count never exceeds `maxGapIntervals`. |
| **I10** | After a full snapshot cycle, no key is Suspect. |
| **I11** | A key is never simultaneously in the in-flight set and reported as divergent. |
| **I12** | A comparison is never performed against a value the oracle has already superseded. |
| **I13** | Sweep is read-only: no target write command is ever issued. |
| **I14** | Shutdown drains cleanly: no goroutine outlives `Close()` by more than the shutdown grace period. |

I13 is enforced structurally rather than by review. The Redis client carries a
hook refusing any command outside a read-only allowlist, and the test double
fails the test on any mutating verb. A tool that audits a store must not be able
to change it, and "we were careful" is not an enforcement mechanism.

---

## Where this is exercised

Reasoning about correctness is not the same as testing it. The claims above are
backed by:

- **60 fault scenarios** in `test/faults/`, one per row of a matrix covering
  every failure mode above, each named for the row it implements. Runs in about
  four seconds against a fake clock; 20 consecutive runs with no flakes
  ([evidence](evidence/fault-matrix-20-runs-no-flake.txt)).
- **The honesty test**,
  `TestFaults_DriftwatchOwnLoss_ReportsSuspectNotConfirmed` — faults on
  driftwatch's own stream must produce suspect keys, never confirmed findings.
- **The e2e honesty test**, E3, which severs driftwatch's subscription with a
  network proxy while the store keeps being written correctly, and asserts
  `suspectDivergentKeys > 0` with `divergentKeys == 0`.
- **A 60-minute soak** at 1,500 events/sec across 150,000 keys: zero false
  positives for the entire run
  ([evidence](evidence/S2-soak-60min-zero-drift.txt)).

The soak's midpoint fault is worth singling out. Surviving an hour proves the
process does not crash. Breaking something deliberately at minute thirty and
watching it be found within one minute — and resolve in the next — proves the
process is still *working*, which is a different claim and the one that matters.
