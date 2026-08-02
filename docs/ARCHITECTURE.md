# Architecture

driftwatch is one long pipeline with a comparison bolted onto the middle of it.
Events go in one end, an in-memory expectation forms, and a sweeper periodically
asks a datastore whether it agrees.

This document is about where the boundaries fall and why. For *what* the
comparison guarantees, read [CORRECTNESS.md](CORRECTNESS.md); this is the
structural half.

## The shape

```text
                       ┌───────────────────────────────────────────────────┐
                       │                  driftwatch                        │
  ZMQ / NATS ─────────►│ ┌─────────┐   ┌────────┐   ┌────────────────┐    │
  (raw frames)         │ │ Source  │──►│ Codec  │──►│ Ingest pipeline │    │
                       │ └─────────┘   └────────┘   └───────┬────────┘    │
                       │                             ┌───────▼────────┐    │
                       │                             │ SeqTracker      │    │
                       │                             │ (gaps, epochs)  │    │
                       │                             └───────┬────────┘    │
                       │                             ┌───────▼────────┐    │
                       │                             │  Projection     │    │
                       │                             │  (pure fold)    │    │
                       │                             └───────┬────────┘    │
                       │                       ┌─────────────▼───────────┐ │
                       │                       │        Oracle           │ │
                       │                       │  keys, versions, trust, │ │
                       │                       │  per-key event ring     │ │
                       │                       └──────┬──────────┬───────┘ │
                       │            ┌─────────────────▼───┐   ┌──▼──────┐  │
                       │            │      Sweeper        │   │ Explain │  │
                       │            │ settle → diff →     │   │ engine  │  │
                       │            │ confirm             │   └─────────┘  │
                       │            └──────┬──────────────┘                │
                       │            ┌──────▼───────┐    ┌──────────────┐   │
   Redis ◄─────read────│            │ Target       │    │ LagEstimator │   │
                       │            │ adapter      │    │ (probes)     │   │
                       │            └──────┬───────┘    └──────┬───────┘   │
                       │            ┌──────▼───────────────────▼───────┐   │
                       │            │        Reporter / Metrics        │   │
                       │            └──────┬───────────────────┬───────┘   │
                       └───────────────────┼───────────────────┼───────────┘
                                    /metrics (Prom)      DriftCheck.status
                                     ┌─────▼─────┐      ┌──────▼───────┐
                                     │  Grafana  │      │ K8s API      │
                                     └───────────┘      │ (controller) │
                                                        └──────────────┘
```

## The packages, and what each is not allowed to know

The dependency graph is a DAG, and its direction matters more than its shape.
The rule is that **nothing below the oracle knows what a datastore is, and
nothing above the projection knows what a transport is.**

| Package | Owns | Deliberately does not know |
|---|---|---|
| `pkg/event` | The `Event` type, `Op`, `Value`, trust states | Everything. It is the vocabulary and imports nothing of ours. |
| `pkg/clock` | `Clock`, `FakeClock`, tickers and timers | Anything about driftwatch. A general utility. |
| `pkg/source` | Reading frames off a wire; reconnection; loss signals | What a frame means. It hands over bytes and a receipt time. |
| `pkg/codec` | Turning bytes into an `Event` | Where the bytes came from, or what happens next. |
| `pkg/seqtrack` | Per-publisher sequence, epochs, gaps, high-water marks | Keys, values, projections, stores. |
| `pkg/projection` | Folding one event onto one previous value | I/O of any kind. Pure functions only. |
| `pkg/oracle` | The expectation: keys, versions, trust, event rings | That a target exists. It never reads anything. |
| `pkg/target` | Reading a datastore, read-only | Why anyone wants the data. |
| `pkg/differ` | Comparing one oracle entry against one target read | How either was obtained. |
| `pkg/lag` | Measuring propagation, deriving W | The sweeper's schedule. |
| `pkg/sweeper` | Settle → read → diff → confirm | Transports, codecs, Kubernetes. |
| `pkg/explain` | Rendering one key's history into a diagnosis | How to fetch any of it. |
| `pkg/metrics` | The registry, the label allow-list, cardinality | What the numbers mean. |
| `pkg/check` | Composition. The only package that wires the others together | — |
| `internal/controller` | Reconciling `DriftCheck` objects onto running checks | The pipeline's internals. |
| `internal/cli` | `watch`, `diff`, `explain`, `replay` | Same. |

`pkg/check` is the only package importing both `pkg/source` and `pkg/target`.
That is the point: every other package can be reasoned about — and tested —
without a network, a container, or a clock that moves on its own.

## The data flow, in ten steps

1. **Source** reads a frame and records a local receipt time. That timestamp is
   the only one driftwatch trusts for elapsed-time decisions, because a
   publisher's clock can be wrong by more than the settlement window.
2. **Codec** decodes it into an `Event`. Failures are typed sentinels, so the
   pipeline can tell a version mismatch from corruption from an oversized frame
   and count them apart.
3. **SeqTracker** records sequence and epoch, detecting gaps and restarts. This
   is where driftwatch learns whether its *own* view is complete.
4. **Reorder buffer** briefly holds out-of-order events, so a late arrival is
   not mistaken for a hole. A hole is only a gap once nothing will fill it.
5. **Projection** folds the event onto the key's previous value. Pure: no I/O,
   no clock, no logging. This is the piece users extend.
6. **Oracle** stores the result, bumps that key's version, appends to its event
   ring, and stamps a trust state taken from the tracker.
7. **Sweeper** selects keys settled for at least W and reads them from the
   target in batches, fencing each against the version it was selected at.
8. **Differ** categorizes each disagreement: missing, extra, member mismatch,
   version skew, evicted, expired, wrong type.
9. **Confirmation** re-reads each candidate after a delay, dropping it if the
   oracle version moved or the key became suspect in the meantime.
10. **Reporter** writes metrics, `DriftCheck.status` and Kubernetes Events.

## Three decisions that shape everything else

### The oracle never reads the target

It would be convenient to let the oracle fetch a key it does not have. It would
also destroy the guarantee: an oracle that can read the store is an oracle that
can agree with the store for reasons having nothing to do with the event stream,
and the entire value of this tool is that the two answers are reached
independently.

So `pkg/oracle` does not import `pkg/target`, and cannot. The comparison happens
one level up, in `pkg/sweeper`, which holds both.

### Projections are pure, and the fold is the extension point

A projection is `(previous value, event) → mutation`. No I/O, no clock, no state
of its own. Everything that varies between deployments — what a key looks like,
whether members accumulate or replace, whether order matters — lives there, and
nothing else changes to support a new one.

The purity is load-bearing for testing: the fault matrix drives sixty scenarios
through the whole pipeline in-process on a fake clock, in under two minutes,
because nothing below `pkg/check` can block on anything.

### The clock is an interface, everywhere

Every package that measures elapsed time takes a `clock.Clock`. The settlement
window, the confirmation delay, the sweep interval, the reconnection backoff and
the lag estimator all read it, and in tests they all read a fake one.

This is why `go test ./...` finishes in under three minutes while exercising
sixty-minute behaviours, and why `hack/verify-no-sleep.sh` can forbid
`time.Sleep` in tests outright: there is always a better option.

The fake clock has one sharp edge worth knowing before writing a test with it.
A tick carries the deadline it was *scheduled* for, not the clock's new value,
and a tick the consumer has not yet drained is dropped rather than queued —
exactly like `time.Ticker`. So a single `Advance(3s)` across a 1s ticker does
not reliably deliver three ticks. See `advanceUntil` in
`pkg/check/check_test.go`, which exists because that cost an afternoon.

## Where the state lives

Four things survive a single event, and each has a cap:

- **The oracle's key map**, bounded by `maxTrackedKeys`, evicting per shard.
- **Each key's event ring**, bounded by `ringSize`, overwriting oldest. This is
  what `explain` reads, and what makes memory depend on ring depth rather than
  on key count ([G-001](KNOWN_GAPS.md)).
- **Per-publisher sequence state**, bounded by `maxPublishers`.
- **The confirmation queue**, bounded by `maxConfirmQueue`, dropping and
  counting rather than growing.

Everything else is per-sweep or per-call. The full audit, with the enforcement
site for each of eighteen collections, is in
[BOUNDED_RESOURCES.md](BOUNDED_RESOURCES.md).

## Concurrency

One check runs a small, fixed number of goroutines — thirteen throughout the
sixty-minute soak, at both t=0 and t=60m — communicating over two buffered
channels:

```text
  source ──raw bytes──► [raw chan] ──► applier ──► oracle
                                          │
  sweeper (own goroutine, own ticker) ────┘  reads the oracle under shard locks
  lag estimator (own ticker)
  confirm cycle (own ticker)
```

**The applier is single-threaded, and that is a correctness requirement rather
than a simplification.** seqtrack, the projection and the oracle each assume they
see one publisher's stream in order, and each is wrong in a different way when
they do not: seqtrack reports phantom gaps, a non-commutative projection folds to
the wrong value, and the oracle's version counter stops meaning "how many events
have touched this key". Parallelising it would need per-publisher sharding all
the way down, which is a large change for throughput that has not been the
bottleneck in any measurement taken so far.

The oracle is sharded, and eviction is deliberately shard-local so that no code
path ever needs two shard locks. That costs about 0.3% of configured capacity to
hash imbalance ([D-003](DISCOVERIES.md)) and buys a lock ordering that cannot be
got wrong later.

The sweeper takes a `TryLock` rather than a lock: a sweep overrunning its
interval is skipped and counted, never queued behind the running one. Stacking
them would turn a slow sweeper into an unbounded queue of sweeps, which is a
memory leak in the process auditing someone else's memory.

## Failure and degradation

The table §6.4 specifies, and what each row is protecting against. The theme is
that **driftwatch degrades to saying less, never to saying something wrong.**

| Condition | Behaviour | Why not something else |
|---|---|---|
| Source disconnects | Reconnect with exponential backoff and jitter, 100ms → 30s. On reconnect, mark keys suspect unless the transport guarantees replay. | The events missed during the gap are unknowable, so every key they *could* have touched is now unverifiable. Continuing to assert on them is how a checker blames a store for its own loss. |
| Target unreachable | Sweeps fail fast, `TargetUnavailable=True`, **no divergence reported**. | Absence of data is not evidence of drift (§23 A5). Reporting it would produce a wall of findings proportional to someone else's outage. |
| Decode failures exceed 10%/min | `CodecMismatch=True`, keep running. | Almost always a misconfigured field mapping. Stopping would remove the one signal saying so. |
| Oracle hits `maxTrackedKeys` | Evict approximately-oldest per shard, `OracleSaturated=True`, and reduce `coverage_ratio` accordingly. Never OOM. | A saturated oracle has a permanently partial view, so the phase stays Degraded rather than returning to Watching — a clean report over 60% of a keyspace must not read like a clean report. |
| Sweep exceeds its interval | Skip the next tick, count it, log at WARN. | Overlapping sweeps are an unbounded queue. |
| Confirm queue full | Drop and count. | Under mass divergence the magnitude matters more than the per-key list, and the list is what costs memory. |
| Panic in any goroutine | Recover at the goroutine boundary, log with stack, count it, cancel that check's context. | One check's panic must not take down the other forty-nine in the same manager. |

Two conditions are **sticky**, and deliberately so:

- **`OracleSaturated`** — every report after saturation covers only part of the
  store, so a later clean sweep does not clear it.
- **`SourceFailed`** — a source that has stopped delivering makes every
  subsequent sweep look perfectly clean, because the oracle stops changing and
  the target stops being written to. That is the most dangerous reading this
  tool can produce, and it must never be reported as Watching.

## The controller

`internal/controller` maps `DriftCheck` objects onto running checks through a
registry keyed by namespace/name. A spec change stops the old runner before
starting the new one, under a per-key mutex, because `MaxConcurrentReconciles`
is above one and two reconciles of the same object really can overlap. Getting
that wrong leaves orphaned runners that no reconcile will ever find again, each
holding an oracle and a connection pool and writing metrics under the same label.

Leader election is per-check rather than per-process, so N manager replicas
spread the checks between them rather than idling.

## Extending it

- A new transport: [ADDING_A_SOURCE.md](ADDING_A_SOURCE.md)
- A new fold: [ADDING_A_PROJECTION.md](ADDING_A_PROJECTION.md)
- A new datastore: implement `target.Target`. The same shape as the source
  guide, with a read-only allowlist as the one extra obligation — see
  [D-004](DISCOVERIES.md) for the trap in enforcing that strictly.
