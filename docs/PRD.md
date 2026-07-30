# driftwatch — Technical Product Requirements Document

**Version:** 1.0
**Status:** Ready for implementation
**Target implementer:** Autonomous coding agent (Claude Opus 5 / Claude Code)
**Language:** Go 1.23+
**License:** Apache-2.0

---

## 0. What this project is, in plain words

### 0.1 The one-sentence version

**driftwatch is a tool that catches a specific silent bug: when a system's "index" quietly stops matching reality.**

### 0.2 The real-world example

Imagine a city library system with 12 branches.

- Every physical **book** lives at exactly one branch (or sometimes several branches have copies).
- There is one central **catalog computer** that tells you *"Book #4471 is at the Riverside branch."*
- Branches don't update the catalog directly. Instead, whenever a book moves, the branch **shouts an announcement over a city-wide PA system**: *"Riverside just received Book #4471!"* or *"Northside just discarded Book #2210!"*
- A little program sits and listens to the PA system, and every time it hears an announcement, it updates the central catalog.

This works beautifully — until it doesn't.

The PA system is a **broadcast**. Nobody confirms receipt. So:

- If the listening program is busy for two seconds, it **misses announcements entirely**. They're gone. Nobody knows.
- If two announcements arrive out of order (*"discarded"* before *"received"*), the catalog ends up with the **wrong final answer**.
- If a branch's computer reboots and starts numbering its announcements from 1 again, the listener gets **confused about what's new**.
- If someone hand-edits the catalog directly, no announcement was ever made, so the catalog and the shelves **silently disagree forever**.

Here's the killer part: **nothing breaks loudly.** The catalog still returns an answer. The website still works. Every dashboard is green. It's just that some percentage of the time, the catalog sends a patron to Riverside for a book that's actually at Northside. The patron walks over, doesn't find it, and the system just looks... mysteriously bad. Slow. Unreliable. Nobody can reproduce it.

**driftwatch is the auditor you hire to fix this.**

It does three things:

1. **Listens to the same PA system, independently**, and keeps its own private notebook of what the catalog *should* say. (This notebook is called the **oracle**.)
2. **Periodically walks the shelves** — reads the actual catalog — and compares it against the notebook.
3. **Tells you precisely what disagrees**, how long it's been wrong, and — for any specific book you ask about — replays the exact sequence of announcements it heard so you can see which one got lost or arrived in the wrong order.

Plus it exposes all of this as metrics and a dashboard, so instead of "the system feels flaky," you get a number: *"1,204 catalog entries are wrong, and they've been wrong for 8 minutes."*

### 0.3 Where this pattern shows up in real software

The library is an analogy. This exact architecture is *everywhere*:

| Real system | "Books" | "PA system" | "Catalog" |
|---|---|---|---|
| LLM inference routers (vLLM, kthena, llm-d) | KV-cache blocks on GPU replicas | ZeroMQ pub/sub events from each replica | Redis index of `block_hash → replica` |
| Kubernetes service networking | Pod endpoints | watch stream from the API server | kube-proxy / CNI dataplane rules |
| CDN edge caches | Cached objects | invalidation event bus | edge routing table |
| Database read replicas | Rows | replication log | replica's materialized tables |
| Search infrastructure | Documents | change-data-capture stream | Elasticsearch index |
| Payment ledgers | Settled transactions | webhook stream from processors | internal ledger balances |

In every one of these, the same failure exists and is famously hard to debug. driftwatch is a **general-purpose detector** for the whole class.

### 0.4 What driftwatch is NOT

- It is **not** the thing that writes to the catalog. It never mutates the target store. It is read-only and side-effect free by design.
- It is **not** a replacement for the event bus or the cache. It watches them.
- It is **not** tied to any one domain. It has no knowledge of LLMs, payments, or search.
- It is **not** a repair tool. It detects and reports. Repair is out of scope (see §3.2).

### 0.5 Why this is worth building

Three reasons, in order of importance to the author:

1. **It is genuinely useful and doesn't exist.** There is no general-purpose divergence detector for pub/sub-materialized state. People write one-off scripts per system.
2. **It exercises exactly the skills that distributed-systems maintainers care about**: Go, Kubernetes operators, Kind-based end-to-end testing, Redis, ZeroMQ, Prometheus/Grafana observability, and — most of all — *reasoning correctly about lag, ordering, and partial failure*.
3. **The hard part is intellectually real.** Naively diffing two stores produces an avalanche of false positives, because the target legitimately lags the event stream. Solving that properly (§5) is the substance of the project and the thing worth talking about in an interview.

---

## 1. How the implementing agent should work

This section is a **binding working agreement**. Read it before writing any code.

### 1.1 Core rules

1. **Test-first, always.** For every module, write the test file before the implementation file. A module is not "done" until its tests pass with `-race`.
2. **No skipped tests. No `t.Skip()` without a linked TODO issue in `docs/KNOWN_GAPS.md`.** A skipped test is a lie.
3. **Never mock the thing under test.** Mock only the boundaries (network, clock, store).
4. **The clock is always injected.** No direct `time.Now()` calls anywhere except in `main()` and in the clock implementation itself. This is non-negotiable — the entire test strategy depends on controllable time.
5. **Commit at every green checkpoint**, with conventional-commit messages (`feat:`, `fix:`, `test:`, `docs:`, `refactor:`, `chore:`). Small commits. One logical change each.
6. **Never leave a broken `main` branch.** If a phase can't complete, commit the working subset and record the gap.
7. **Maintain `docs/DISCOVERIES.md` continuously.** Every time something surprises you — a library behaves unexpectedly, a Redis command has a subtlety, a ZMQ socket drops silently — write it down immediately with the reproducing evidence. This file is a primary deliverable, not an afterthought. See §21.3.
8. **Maintain `docs/evidence/`.** Every significant claim in the README must map to a real captured log or output file in this directory. See §21.4.
9. **Do not add dependencies not listed in §8.4** without recording the decision in `docs/DECISIONS.md` with rationale and alternatives considered.
10. **Prefer boring, obvious code.** This project's value is in its correctness reasoning and its test suite, not in clever Go. If a reviewer has to think hard about *how* the code works, rewrite it.

### 1.2 Definition of "done" for any module

A module is done when **all** of the following hold:

- [ ] All exported symbols have doc comments beginning with the symbol name.
- [ ] Unit tests exist and pass, including table-driven cases for every documented edge case.
- [ ] Property tests exist where invariants are stated (§16.2).
- [ ] `go test -race ./...` passes.
- [ ] `go vet ./...` passes.
- [ ] `golangci-lint run` passes with the project's `.golangci.yml`.
- [ ] `goleak` verification is wired into the package's `TestMain`.
- [ ] Package coverage meets the target in §16.9.
- [ ] Any surprise encountered is recorded in `docs/DISCOVERIES.md`.

### 1.3 Order of work

Follow the phase plan in §20 **strictly in order**. Each phase has explicit exit criteria. Do not begin phase N+1 until phase N's exit criteria are met and committed. The phases are ordered so that every phase produces something demonstrable, which means if the project is abandoned mid-way it still looks complete rather than half-built.

### 1.4 When blocked

If genuinely blocked (a library doesn't do what's needed, a design in this PRD turns out to be wrong):

1. Write the problem into `docs/DECISIONS.md` under a new heading.
2. State the two or three options with tradeoffs.
3. Pick the one that preserves testability, and proceed.
4. Do **not** silently deviate from the PRD without recording it.

### 1.5 What "resume-ready" means here

The end state is not just working code. It is a repository that a Kubernetes maintainer can open and, within 90 seconds, conclude *"this person understands distributed systems and testing."* Concretely that requires:

- A green CI badge.
- A README with a real architecture diagram, real measured numbers, and a **Key Discoveries** section.
- `docs/evidence/` with actual terminal output backing every claim.
- `make e2e` that works from a clean clone on a machine with Docker.
- A Grafana dashboard screenshot showing drift being detected and recovering.
- Zero "TODO" or "FIXME" in the main code path.
- No unbacked superlatives anywhere. Never write "production-grade," "enterprise," or "institutional." Write the measured number instead.

---

## 2. Problem statement

### 2.1 The system class

Consider any system with this shape:

```
┌──────────────┐   events    ┌──────────────┐   writes    ┌──────────────┐
│  Producers   │ ──────────► │ Materializer │ ──────────► │ Target store │
│ (N replicas) │  pub/sub    │  (consumer)  │             │   (Redis)    │
└──────────────┘             └──────────────┘             └──────────────┘
                                                                  │
                                                                  │ reads
                                                                  ▼
                                                          ┌──────────────┐
                                                          │   Consumer   │
                                                          │  (a router,  │
                                                          │  a scheduler)│
                                                          └──────────────┘
```

Producers emit state-change events over a **lossy, unordered, at-most-once** broadcast channel. A materializer consumes them and maintains derived state in a target store. Some downstream consumer reads that store to make decisions.

### 2.2 Why this shape is fragile

| Property of the channel | Consequence |
|---|---|
| **At-most-once** (ZMQ PUB drops to slow subscribers at the high-water mark) | Events vanish with no error anywhere |
| **No ordering guarantee across publishers** | Independent producers' events interleave arbitrarily |
| **No ordering guarantee even within a publisher** across reconnects | A reconnect can replay or skip |
| **No end-to-end acknowledgement** | The producer cannot know its event was applied |
| **Producers restart independently** | Sequence numbers reset; identity may or may not be stable |
| **Target store has independent failure modes** | Eviction under `maxmemory`, TTL expiry, `FLUSHDB`, failover to a stale replica |
| **Target store is writable by others** | Out-of-band mutation leaves no event trace |

Any one of these produces **silent, permanent divergence**. The system continues to serve requests. Correctness degrades. No alert fires, because from the perspective of every individual component, nothing failed.

### 2.3 Why existing tools don't catch it

- **Liveness/readiness probes** check that processes are up. Every process *is* up.
- **Consumer lag metrics** (Kafka-style) don't exist for fire-and-forget pub/sub, and even where they do, lag ≠ correctness. Zero lag with a dropped message is still wrong.
- **Store-level metrics** (Redis `INFO`, keyspace size) show a plausible-looking number. They cannot know what the number *should* be.
- **Application metrics** show degraded outcomes (cache hit rate down, latency up) but cannot localize the cause, and the degradation is often within normal variance.
- **Distributed tracing** follows requests, not background state convergence.

The missing capability is: *an independent computation of what the store should contain, compared against what it does contain.*

### 2.4 The concrete motivating case

In LLM inference serving (vLLM, llm-d, Volcano's kthena), each model replica holds prefix KV-cache blocks in GPU memory. Replicas publish block-ownership events over ZeroMQ. A materializer maintains a Redis index of `block_hash → {replica}`. The router reads this index to score which replica can serve a request with the least prefill work.

When the index drifts:
- The router sends a request to a replica that no longer holds the block → full prefill → higher time-to-first-token.
- Or it *fails to send* to a replica that does hold it → wasted cache.

Observable symptom: **cache hit rate lower than it should be, and p99 TTFT worse than expected.** There is no error and no obvious place to look. This is exactly the class of bug that eats weeks.

driftwatch makes that bug a number on a dashboard.

---

## 3. Goals and non-goals

### 3.1 Goals

**G1 — Detect divergence between an event-derived oracle and a target store, with a false-positive rate low enough to alert on.**
This is the primary goal and the hardest one. See §5.

**G2 — Attribute divergence to a cause.** Distinguish, at minimum: dropped events (sequence gaps), reordering, duplicate delivery, out-of-band target mutation, target eviction/expiry, and publisher restart.

**G3 — Explain a single key's history.** Given a key, replay every event driftwatch observed for it, with timestamps, sequence numbers, and the resulting oracle state after each — so a human can see exactly where things went wrong.

**G4 — Run as a Kubernetes-native operator** with a declarative `DriftCheck` custom resource, so a check is configuration rather than code.

**G5 — Expose everything as Prometheus metrics** with bounded cardinality, plus a ready-to-import Grafana dashboard.

**G6 — Be provably correct under adversarial conditions.** A fault-injection harness that can drop, reorder, duplicate, delay, and partition; property-based tests over generated event orderings; a Kind-based end-to-end suite that exercises the whole real path.

**G7 — Support pluggable sources, projections, codecs, and targets** so the tool is genuinely general rather than a one-off.

**G8 — Be operationally boring.** Bounded memory, no unbounded queues, deterministic cleanup, graceful shutdown, no goroutine leaks, safe under `-race`.

### 3.2 Non-goals (explicit — do not build these)

**NG1 — Repair.** driftwatch never writes to the target store. Auto-repair requires domain knowledge driftwatch doesn't have, and a detector that can also mutate is a detector nobody will deploy. Read-only is a feature.

**NG2 — Being the materializer.** driftwatch does not replace the consumer that writes to the target. It observes alongside it.

**NG3 — Exactly-once delivery.** driftwatch cannot fix a lossy channel. It detects and quantifies the loss.

**NG4 — Distributed oracle / horizontal sharding of a single check.** One `DriftCheck` is handled by one process. Multiple checks can be spread across replicas via leader election per check, but a single keyspace is not sharded across processes. This bounds scope; note it in the README as a known limitation with a sketch of how it would be done.

**NG5 — A web UI.** The CLI and the Grafana dashboard are the interfaces. No React, no dashboard server.

**NG6 — Supporting every event bus.** ZeroMQ and NATS plus an in-memory and a file-replay source. Kafka is explicitly deferred (it has consumer groups and offsets, which makes it a materially different and *easier* problem).

**NG7 — Multi-tenancy, authn/authz for the tool's own API.** It has no API beyond metrics and a CLI.

### 3.3 Success criteria

The project succeeds if all of these are true:

| # | Criterion | How it is verified |
|---|---|---|
| S1 | Detects a single dropped event within 2× the settlement window | E2E test `TestDropSingleEvent` |
| S2 | Zero false positives over a 30-minute steady-state soak at ≥5,000 events/sec | Soak test, `driftwatch_divergent_keys == 0` for the full window |
| S3 | Correctly classifies all 30+ fault scenarios in §15.3 | Fault matrix test suite, one test per row |
| S4 | `explain` output identifies the exact missing sequence number for a dropped-event scenario | E2E test with golden-file assertion |
| S5 | Memory stays under 512 MiB tracking 1,000,000 keys | Benchmark `BenchmarkOracle1M` + soak RSS assertion |
| S6 | Full sweep of 1,000,000 Redis keys completes in under 10 seconds | Benchmark `BenchmarkFullSweep1M` |
| S7 | `make e2e` passes from a clean clone on a machine with only Docker + Go installed | CI job `e2e` on a fresh runner |
| S8 | Wire-compatible with libzmq publishers | Interop test with a `pyzmq` publisher, §16.6 |
| S9 | Operator survives CRD create/update/delete mid-sweep with no leaked goroutines | envtest + goleak |
| S10 | No goroutine leaks, no data races, across the entire suite | `-race` + goleak in every `TestMain` |

---

## 4. Concepts and terminology

These terms are used precisely throughout. The implementation must use the same names.

| Term | Definition |
|---|---|
| **Event** | An immutable record of a state change emitted by a producer. Carries publisher identity, sequence number, timestamp, operation, and key. |
| **Publisher** | A logical producer of events, identified by a stable string. Has its own independent sequence-number space. |
| **Sequence number (seq)** | A monotonically increasing `uint64` per publisher. Gaps indicate loss. Resets indicate restart. |
| **Source** | The transport driftwatch reads events from (ZMQ, NATS, memory, file). |
| **Codec** | The decoder that turns raw bytes from a source into an `Event`. |
| **Projection** | A deterministic function that folds a stream of events into derived state. Pure; no I/O. |
| **Oracle** | driftwatch's independently-computed expectation of what the target should contain, produced by applying a projection to the observed event stream. |
| **Target** | The external store being audited (Redis). Read-only from driftwatch's perspective. |
| **Sweep** | One pass comparing oracle state against target state. |
| **Settlement window (W)** | The grace period after a key's most recent event during which divergence is *not* reported, because the real materializer may legitimately not have applied it yet. |
| **In-flight key** | A key whose most recent observed event is newer than W. Excluded from divergence counts; counted separately. |
| **Settled key** | A key whose most recent observed event is older than W. Eligible for divergence assertion. |
| **Candidate divergence** | A settled key where oracle ≠ target on first read. Not yet reported. |
| **Confirmed divergence** | A candidate that still diverges after a targeted re-read one full W later. This is what gets reported and alerted on. |
| **Divergence category** | One of: `missing_in_target`, `extra_in_target`, `value_mismatch`, `ttl_mismatch`, `member_mismatch`. |
| **Bootstrap mode** | How driftwatch handles state that existed before it started watching: `Adopt`, `Strict`, or `Wait`. See §5.6. |
| **High-water mark (HWM)** | The highest sequence number observed per publisher. |
| **Seq gap** | A missing sequence number between the previous observed seq and the current one for a publisher. Direct evidence of event loss. |
| **Trust state** | Per-key flag: `Complete` (no gaps affecting this key) or `Suspect` (a gap was observed that might have affected it). Suspect keys are reported separately. |
| **Convergence time** | Elapsed time from an event's observation to the moment the target reflects it. Measured as a histogram; used to auto-tune W. |
| **Epoch** | A publisher's incarnation. Incremented on detected restart (seq reset). Used to avoid treating a restart as a massive gap. |

---

## 5. The central correctness problem

**Read this section twice before writing code.** Everything else in the project is plumbing. This is the part that makes driftwatch work or makes it a false-positive generator that nobody deploys.

### 5.1 Why the naive design fails

The obvious implementation:

```
loop:
  oracle  := applyAllEventsSeenSoFar()
  target  := readEverythingFromRedis()
  report(diff(oracle, target))
```

This produces a torrent of false positives. Here is every reason:

**F1 — Legitimate lag.** driftwatch and the real materializer both consume the same broadcast. driftwatch is a lightweight in-memory fold; the materializer does a network round-trip to Redis. driftwatch is *always ahead*. Every key with a recent event will appear "missing in target" even though nothing is wrong.

**F2 — The sweep is not atomic.** Redis `SCAN` is a cursor over a mutating keyspace. A full sweep of a million keys takes seconds. Keys written during the sweep may or may not appear. The "target snapshot" is a smear across time, not a point-in-time view. Comparing a smear against a point-in-time oracle is meaningless.

**F3 — The oracle moves during the sweep.** Events keep arriving while sweeping. If you compare key K at sweep-time T against an oracle that has since advanced, you compare against the wrong version.

**F4 — driftwatch started late.** If driftwatch attaches to a running system, the target already contains state derived from events driftwatch never saw. Every single one of those keys looks like `extra_in_target`.

**F5 — Wall-clock skew.** Producers stamp events with their own clocks. Comparing a producer's timestamp against driftwatch's `time.Now()` to decide "is this settled?" is unsound when clocks differ by more than W.

**F6 — Reordering within the settlement window.** Two events for the same key arriving out of order transiently produce a wrong oracle state that self-corrects. Sampling during that transient reports a phantom divergence.

**F7 — Target-side TTL and eviction.** Redis may expire or evict a key with no event. Whether that's a bug depends on whether the projection models TTL. driftwatch must not report a correctly-expired key as drift.

**F8 — Lossy channel means the oracle is *also* wrong.** driftwatch's own subscription drops events. When oracle and target disagree, it is not automatically the target that's wrong. driftwatch must know when *it* is the untrustworthy party.

F8 is the subtle one and the reason sequence numbers are mandatory rather than optional.

### 5.2 Mechanism 1 — Sequence numbers and trust state

**Requirement: every event carries `(publisher, epoch, seq)` where `seq` is monotonic per `(publisher, epoch)`.**

If the source system does not provide this, driftwatch cannot distinguish "the target is wrong" from "I missed an event," and its output degrades from *assertion* to *hint*. This must be stated loudly in the README and enforced in config: a `DriftCheck` on a source without sequence numbers runs in **advisory mode**, which reports divergence as `driftwatch_advisory_divergent_keys` and never fires the alerting condition.

Per-publisher tracking:

```go
type PublisherState struct {
    ID          string
    Epoch       uint64
    HWM         uint64        // highest seq observed in this epoch
    Gaps        *GapSet       // observed missing ranges, coalesced
    FirstSeen   time.Time
    LastSeen    time.Time
    EventCount  uint64
    RestartCount uint64
}
```

**Gap detection algorithm:**

```
on event e from publisher p:
  st := state[p.ID]

  if st is new:
      st.Epoch = e.Epoch; st.HWM = e.Seq
      mark bootstrap-origin for this publisher
      return Accept

  if e.Epoch > st.Epoch:
      // publisher restarted, declared explicitly
      st.Epoch = e.Epoch; st.HWM = e.Seq
      st.RestartCount++
      emit RestartDetected(p, oldEpoch, newEpoch)
      return AcceptAfterRestart

  if e.Epoch < st.Epoch:
      // stale event from a previous incarnation, arrived late
      return DropStaleEpoch

  // same epoch
  switch:
    case e.Seq == st.HWM + 1:  st.HWM = e.Seq; return Accept
    case e.Seq <= st.HWM:
         if st.Gaps.Contains(e.Seq):
             st.Gaps.Fill(e.Seq); return AcceptLateFill
         else:
             return DropDuplicate          // already seen
    case e.Seq > st.HWM + 1:
         st.Gaps.Add(st.HWM+1, e.Seq-1)    // record the hole
         st.HWM = e.Seq
         emit SeqGap(p, from, to)
         return AcceptWithGap
```

**Implicit restart detection** (for producers that reset `seq` without bumping `epoch`): if `e.Seq < st.HWM - restartHeuristicThreshold` **and** `e.Seq` is small (below `restartSeqCeiling`, default 100), treat it as an implicit restart, bump the internal epoch, and record `implicit_restart` in metrics. Both thresholds are configurable. Log at WARN with the observed values, because guessing here is exactly the kind of thing that should be visible.

**GapSet** must be an interval set (not a per-seq bitmap) so that a 10-million-event gap costs one entry, and must be capped: if gap intervals exceed `maxGapIntervals` (default 1024), coalesce aggressively and set a `GapsTruncated` flag. Unbounded gap tracking is a memory-exhaustion vector under a flapping publisher.

**Trust state per key:** when a gap is recorded for publisher `p`, every key that `p` could have touched becomes `Suspect`. driftwatch cannot know which keys those were — that information was in the lost events. Therefore:

- If the projection declares **key-space partitioning by publisher** (i.e. publisher `p` only ever writes keys matching a declared pattern), only keys in that partition become `Suspect`. This is a projection-level capability: `Projection.KeyOwnership() OwnershipModel`.
- Otherwise, **all keys become `Suspect`** until a full snapshot event resets trust.

`Suspect` keys are diffed and reported under `driftwatch_suspect_divergent_keys`, separately from `driftwatch_divergent_keys`. Only the latter feeds alerting. This single distinction is what keeps driftwatch honest: *it never claims the target is broken when it knows its own view is incomplete.*

**Snapshot events** (`OpSnapshotBegin` / `OpSnapshotEnd`) allow a publisher to resynchronize. On `SnapshotEnd`, the publisher's gaps are cleared and its keys return to `Complete`. Producers that support periodic snapshots make driftwatch dramatically more useful; document this as a recommendation.

### 5.3 Mechanism 2 — The settlement window

**Definition:** a key `k` is **settled** at time `t` if `t - lastEventObservedAt(k) > W`.

Only settled keys are eligible for divergence assertion. In-flight keys are counted in `driftwatch_inflight_keys` and skipped.

Note carefully: `lastEventObservedAt` is driftwatch's **local monotonic receive time**, not the publisher's timestamp. This sidesteps F5 (clock skew) entirely. Publisher timestamps are retained for `explain` output and for skew *measurement*, but never used in settlement decisions. Record observed skew as `driftwatch_publisher_clock_skew_seconds{publisher}` — it is useful diagnostic output and costs nothing.

**Choosing W.** Static W is fine for a first implementation but brittle. Implement both:

- **Static:** `spec.policy.settlementWindow`, default `5s`.
- **Adaptive:** `W = max(minW, p99(convergenceTime) × safetyFactor)` where `safetyFactor` defaults to 3, `minW` to 1s, and there is a hard `maxW` of 120s. Recompute every sweep from a sliding window of the last 10,000 convergence observations.

**Measuring convergence time** is the input to adaptive W and requires care. Method: maintain a small sampled set of keys (default 200, rotating) marked as *probes*. When an event arrives for a probe key, record the observation time and poll that single key in the target at increasing intervals (10ms, 20ms, 40ms… capped at W×2) until the target reflects the expected value. The elapsed time is one convergence observation. This costs a bounded, tiny number of extra Redis reads (`GET`/`SMEMBERS` on 200 keys) and gives a real distribution rather than a guess.

Emit `driftwatch_convergence_seconds` as a histogram with buckets `[.001 .0025 .005 .01 .025 .05 .1 .25 .5 1 2.5 5 10]`.

**Edge case: a key that receives events continuously never settles.** A hot key updated every 100ms with W=5s is permanently in-flight and never checked. This is a real blind spot. Mitigation: track `driftwatch_never_settled_keys` and, for keys in-flight longer than `neverSettledThreshold` (default 10×W), perform a **stability-window check**: if the oracle value for that key has been *unchanged* for W despite events arriving (i.e. idempotent repeats), treat it as settled. Log the remaining permanently-unsettleable keys explicitly — an honest blind spot documented is fine; an undocumented one is not.

### 5.4 Mechanism 3 — Two-phase confirmation

A single disagreeing read is never reported. The sequence is:

```
Phase 1 (sweep):        settled key k, oracle(k) ≠ target(k)  →  candidate
Phase 2 (confirm):      wait W, capture oracleVersion(k)
                        re-read only k from target
                        if still ≠ and oracle(k) unchanged since candidate  →  CONFIRMED
                        if oracle(k) changed                                →  discard, re-queue
                        if now equal                                        →  discard, record
                                                                               transient_resolved
```

This eliminates F1, F3, and F6 almost entirely, at the cost of one extra targeted read per candidate. Since confirmed divergence should be rare in a healthy system, the cost is negligible. In an unhealthy system the confirm queue is bounded (`maxConfirmQueue`, default 10,000) and overflow increments `driftwatch_confirm_queue_dropped_total` — under mass divergence you don't need to individually confirm every key, you need to know the magnitude.

Record `driftwatch_transient_divergence_total{reason}` for discarded candidates. A rising transient rate with zero confirmed drift is itself a useful signal: it means the materializer is slow relative to W.

### 5.5 Mechanism 4 — Version-fenced comparison

To defeat F3, every oracle key carries a version counter incremented on each applied event:

```go
type OracleEntry struct {
    Key         string
    Version     uint64        // bumped on every apply
    Value       Value         // scalar or member-set
    LastEventAt time.Time     // monotonic, local
    LastSeq     uint64
    LastPublisher string
    Trust       TrustState
}
```

Sweep comparison procedure per key:

```
v1     := oracle.Version(k)
tval   := target.Read(k)
v2     := oracle.Version(k)
if v1 != v2 { requeue(k); return }     // oracle moved mid-read; comparison invalid
compare(oracle.Value(k) @ v1, tval)
```

This is a lightweight optimistic-read fence. It is not a substitute for the settlement window (which handles the materializer's lag) — it handles *driftwatch's own* concurrency.

To defeat F2 (non-atomic sweep), the sweep must be **oracle-driven, not target-driven** wherever possible:

- **Primary direction (oracle → target):** iterate settled oracle keys and read each from the target. This is exact, because the oracle is a local data structure that can be iterated under a read lock with version fencing.
- **Secondary direction (target → oracle):** to find `extra_in_target`, a `SCAN` of the target keyspace is unavoidable. Handle F2 by treating extras conservatively: a key found in the target but not the oracle is only reported if (a) it is still present on a re-read after W, **and** (b) it is not in the oracle at that later time either. Newly-appeared keys mid-scan therefore self-resolve.

Run the two directions at different cadences: `oracle→target` every `sweepInterval` (default 30s), `target→oracle` every `extraScanInterval` (default 5m, since extras are usually a slower-moving problem and the scan is the expensive half).

### 5.6 Mechanism 5 — Bootstrap modes

Defeats F4. Three modes, configured per `DriftCheck`:

**`Adopt` (default).** At startup, perform one full target scan and load it into the oracle as baseline, marking every adopted key `Trust: Adopted`. From then on, apply events normally. Adopted keys are never reported as `extra_in_target`, but *are* checked once an event touches them (at which point they transition to `Complete` or `Suspect` per normal rules).
- Pro: works immediately against a running system.
- Con: cannot detect pre-existing drift. Document this clearly — `Adopt` mode's guarantee is "no *new* drift since I started."

**`Strict`.** Refuse to assert until a full `OpSnapshotBegin`/`OpSnapshotEnd` cycle has been received from every known publisher. Until then, status is `Phase: AwaitingSnapshot` and no divergence is reported.
- Pro: the strongest guarantee — detects pre-existing drift.
- Con: requires producer cooperation.

**`Wait`.** Start with an empty oracle. Only assert on keys that have received at least one event since startup. Ignore everything else forever (until it receives an event).
- Pro: no producer cooperation needed, no false positives from pre-existing state.
- Con: coverage grows slowly; keys that never change are never checked.

Expose which mode is active and the resulting coverage: `driftwatch_oracle_keys{trust="adopted|complete|suspect"}` and `driftwatch_coverage_ratio` = (asserted keys) / (total target keys).

### 5.7 Mechanism 6 — TTL and eviction handling

Three configurable target-expiry policies, since whether an expired key is drift is domain-dependent:

- **`Ignore`** — a key absent from the target that the oracle expects is *not* reported if the target's keyspace has TTLs enabled and the key's oracle age exceeds the configured `assumedTTL`. Blunt but useful.
- **`Model`** — the projection tracks TTL from events (`Event.TTL`) and the oracle expires keys itself. Divergence is then meaningful in both directions. Requires producers to emit TTL.
- **`Strict`** — any absence is drift. Correct when the target has no TTLs, which is the common case for an index.

Redis eviction under `maxmemory` is different from expiry and worth detecting separately: read `INFO stats` → `evicted_keys` before and after each sweep, and if it increased, annotate the sweep result with `evictionSuspected: true` and expose `driftwatch_target_evictions_observed_total`. A sweep that finds mass `missing_in_target` concurrent with rising evictions has an obvious explanation, and saying so in the output saves the operator an hour.

### 5.8 Correctness invariants (these become property tests)

These are the assertions the test suite must prove. Each maps to a property test in §16.2.

| ID | Invariant |
|---|---|
| **I1** | Applying an event twice yields the same oracle state as applying it once (idempotence via seq dedup). |
| **I2** | For a commutative projection, any permutation of the same event set yields identical final oracle state. |
| **I3** | For a non-commutative projection, applying events in seq order yields the canonical state, and out-of-order delivery converges to it once all events are delivered. |
| **I4** | A sequence gap is never missed: if any event is withheld, `GapSet` contains its seq. |
| **I5** | No event is ever double-counted in `driftwatch_events_received_total`. |
| **I6** | The differ reports empty iff oracle and target are equal for all settled, `Complete`-trust keys. |
| **I7** | Confirmed divergence implies the target genuinely disagreed at two points separated by ≥ W. |
| **I8** | Oracle memory is bounded: tracked keys never exceed `maxTrackedKeys`, and the per-key event ring never exceeds `ringSize`. |
| **I9** | GapSet interval count never exceeds `maxGapIntervals`. |
| **I10** | After a full snapshot cycle, no key is `Suspect`. |
| **I11** | A key is never simultaneously in the in-flight set and reported as divergent. |
| **I12** | Version fencing: a comparison is never performed against a value the oracle has already superseded. |
| **I13** | Sweep is read-only: no target write command is ever issued. (Enforced by a command-recording fake target that fails the test on any mutating verb.) |
| **I14** | Shutdown drains cleanly: no goroutine outlives `Close()` by more than `shutdownGrace`. |

I13 deserves emphasis: implement a `RecordingTarget` wrapper used in *every* test that fails immediately if any command outside a strict read-only allowlist (`GET SMEMBERS SCAN TYPE TTL PTTL EXISTS HGETALL INFO STRLEN SCARD MEMORY`) is issued. This makes NG1 structurally enforced rather than merely intended.

---

## 6. Architecture

### 6.1 Component diagram

```
                       ┌───────────────────────────────────────────────────┐
                       │                  driftwatch                        │
                       │                                                    │
  ZMQ / NATS ─────────►│ ┌─────────┐   ┌────────┐   ┌────────────────┐    │
  (raw frames)         │ │ Source  │──►│ Codec  │──►│ Ingest pipeline │    │
                       │ └─────────┘   └────────┘   └───────┬────────┘    │
                       │                                     │             │
                       │                             ┌───────▼────────┐    │
                       │                             │ SeqTracker      │    │
                       │                             │ (gaps, epochs)  │    │
                       │                             └───────┬────────┘    │
                       │                                     │             │
                       │                             ┌───────▼────────┐    │
                       │                             │  Projection     │    │
                       │                             │  (pure fold)    │    │
                       │                             └───────┬────────┘    │
                       │                                     │             │
                       │                       ┌─────────────▼───────────┐ │
                       │                       │        Oracle           │ │
                       │                       │  keys, versions, trust, │ │
                       │                       │  per-key event ring     │ │
                       │                       └──────┬──────────┬───────┘ │
                       │                              │          │         │
                       │            ┌─────────────────▼───┐   ┌──▼──────┐  │
                       │            │      Sweeper        │   │ Explain │  │
                       │            │ settle → diff →     │   │ engine  │  │
                       │            │ confirm             │   └─────────┘  │
                       │            └──────┬──────────────┘                │
                       │                   │                               │
                       │            ┌──────▼───────┐    ┌──────────────┐   │
   Redis ◄─────read────│            │ Target       │    │ LagEstimator │   │
                       │            │ adapter      │    │ (probes)     │   │
                       │            └──────┬───────┘    └──────┬───────┘   │
                       │                   │                   │           │
                       │            ┌──────▼───────────────────▼───────┐   │
                       │            │        Reporter / Metrics        │   │
                       │            └──────┬───────────────────┬───────┘   │
                       └───────────────────┼───────────────────┼───────────┘
                                           │                   │
                                    /metrics (Prom)      DriftCheck.status
                                           │                   │
                                     ┌─────▼─────┐      ┌──────▼───────┐
                                     │  Grafana  │      │ K8s API      │
                                     └───────────┘      │ (controller) │
                                                        └──────────────┘
```

### 6.2 Data flow, end to end

1. **Source** yields `RawMessage{Topic, Payload, ReceivedAt}` on a channel. Bounded buffer; on overflow, drop-newest and increment `driftwatch_ingest_dropped_total{reason="buffer_full"}`. Dropping is correct here — blocking would make driftwatch a slow subscriber and cause the upstream ZMQ socket to drop instead, which is worse because it's invisible.
2. **Codec** decodes to `Event`. Decode failures increment `driftwatch_events_dropped_total{reason="decode_error"}` and log the first N failures per minute with a truncated hex dump (rate-limited; never log full payloads, which may contain sensitive data).
3. **SeqTracker** classifies (Accept / AcceptWithGap / AcceptLateFill / DropDuplicate / DropStaleEpoch / AcceptAfterRestart), updates publisher state, emits gap and restart signals.
4. **Projection** folds accepted events into oracle mutations. Pure function — takes current entry + event, returns new entry or a delete instruction. No I/O, no clock, no randomness.
5. **Oracle** applies the mutation, bumps `Version`, updates `LastEventAt` (monotonic local), appends to the per-key event ring, updates trust state, and maintains the settled/in-flight index.
6. **Sweeper** runs on a ticker: iterates settled keys, reads from **Target**, diffs with version fencing, pushes candidates to the confirm queue. A second, slower ticker runs the target→oracle extras scan.
7. **Confirmer** drains the queue after W, re-reads individual keys, promotes to confirmed or discards.
8. **Reporter** updates Prometheus metrics, writes structured logs, and — when running as an operator — patches `DriftCheck.status`.
9. **LagEstimator** independently polls probe keys to build the convergence-time histogram, which feeds adaptive W.
10. **Explain engine** serves `driftwatch explain <key>` by reading the per-key event ring plus a live target read.

### 6.3 Concurrency model

Deliberately simple. One goroutine per role, communicating over channels:

| Goroutine | Count | Responsibility |
|---|---|---|
| `sourceReader` | 1 per source endpoint | read frames, push to raw channel |
| `decoder` | `min(4, GOMAXPROCS)` | decode raw → Event, push to event channel |
| `applier` | **exactly 1** | SeqTracker + Projection + Oracle mutation |
| `sweeper` | 1 | oracle→target sweep |
| `extraScanner` | 1 | target→oracle scan |
| `confirmer` | 1 | drain confirm queue |
| `lagProbe` | 1 | convergence measurement |
| `metricsServer` | 1 | HTTP `/metrics`, `/healthz`, `/readyz` |

**The applier is single-threaded by design.** This makes ordering, sequence tracking, and version bumping trivially correct without locks on the hot path, and it is fast enough: a fold over an in-memory map at 100k+ events/sec on one core. Do not parallelize it. If throughput becomes a problem, shard by key hash into N independent appliers each owning a disjoint keyspace — but only after benchmarks prove the need, and record the decision.

The oracle is read by the sweeper, extraScanner, confirmer, and explain engine concurrently with the applier's writes. Use a `sync.RWMutex` per shard (default 64 shards by key hash) rather than one global lock. Version fencing (§5.5) handles the read-then-compare race that a lock alone cannot.

**All goroutines take a `context.Context` and exit on cancellation.** Every one is registered with an `errgroup`. `Close()` cancels, waits with `shutdownGrace` (default 10s), and returns an error listing any goroutine that failed to exit. `goleak` in `TestMain` for every package enforces this.

### 6.4 Failure and degradation policy

| Condition | Behaviour |
|---|---|
| Source disconnects | Reconnect with exponential backoff + jitter (100ms → 30s cap). Increment `driftwatch_source_reconnects_total`. On reconnect, treat as potential gap: mark all keys `Suspect` unless the source guarantees replay. |
| Target unreachable | Sweeps fail fast, increment `driftwatch_target_errors_total`, set condition `TargetUnavailable=True`, do **not** report divergence (absence of data is not evidence of drift). Retry next interval. |
| Decode failures exceed `maxDecodeErrorRate` (default 10% over 1 min) | Set condition `CodecMismatch=True`; likely misconfiguration. Keep running. |
| Oracle hits `maxTrackedKeys` | Evict by oldest `LastEventAt` (approximate LRU via a per-shard clock), increment `driftwatch_oracle_evictions_total`, set condition `OracleSaturated=True`, and reduce `coverage_ratio` accordingly. Never OOM. |
| Sweep exceeds `sweepInterval` | Skip the next tick rather than overlapping. Increment `driftwatch_sweeps_skipped_total`. Log at WARN with duration. |
| Confirm queue full | Drop and count. Magnitude matters more than per-key detail under mass divergence. |
| Panic in any goroutine | Recover at the goroutine boundary, log with stack, increment `driftwatch_panics_total`, and cancel the check's context so the operator restarts it cleanly. Never let a panic take down other checks in a multi-check process. |

---

## 7. Repository layout

Create exactly this structure. Deviations must be recorded in `docs/DECISIONS.md`.

```
driftwatch/
├── .github/
│   └── workflows/
│       ├── ci.yaml                    # lint, unit, race, coverage
│       ├── e2e.yaml                   # Kind-based e2e
│       ├── soak.yaml                  # nightly 1h soak
│       └── release.yaml               # goreleaser, multi-arch, SBOM
├── api/
│   └── v1alpha1/
│       ├── driftcheck_types.go        # CRD Go types
│       ├── driftcheck_webhook.go      # defaulting + validating webhook
│       ├── groupversion_info.go
│       └── zz_generated.deepcopy.go   # generated
├── cmd/
│   ├── driftwatch/
│   │   └── main.go                    # CLI entrypoint (cobra)
│   └── driftwatch-manager/
│       └── main.go                    # operator entrypoint
├── internal/
│   ├── controller/
│   │   ├── driftcheck_controller.go
│   │   ├── driftcheck_controller_test.go
│   │   ├── runner.go                  # owns one Check lifecycle
│   │   └── runner_test.go
│   ├── cli/
│   │   ├── root.go
│   │   ├── watch.go
│   │   ├── diff.go
│   │   ├── explain.go
│   │   ├── replay.go
│   │   ├── inject.go
│   │   └── version.go
│   └── buildinfo/
│       └── buildinfo.go               # version, commit, date (ldflags)
├── pkg/
│   ├── event/
│   │   ├── event.go                   # Event, Op, Value types
│   │   ├── event_test.go
│   │   └── testdata/                  # golden payloads
│   ├── codec/
│   │   ├── codec.go                   # Codec interface + registry
│   │   ├── json.go
│   │   ├── json_test.go
│   │   ├── msgpack.go
│   │   ├── msgpack_test.go
│   │   ├── template.go                # user-defined field mapping
│   │   ├── template_test.go
│   │   └── fuzz_test.go
│   ├── source/
│   │   ├── source.go                  # Source interface + registry
│   │   ├── memory.go                  # in-process, for tests
│   │   ├── memory_test.go
│   │   ├── file.go                    # replay from JSONL
│   │   ├── file_test.go
│   │   ├── zmq.go
│   │   ├── zmq_test.go
│   │   ├── zmq_interop_test.go        # //go:build interop
│   │   ├── nats.go
│   │   └── nats_test.go
│   ├── seqtrack/
│   │   ├── seqtrack.go
│   │   ├── gapset.go
│   │   ├── gapset_test.go
│   │   ├── seqtrack_test.go
│   │   └── seqtrack_property_test.go
│   ├── projection/
│   │   ├── projection.go              # Projection interface + registry
│   │   ├── keyset.go                  # set-ownership projection
│   │   ├── keyset_test.go
│   │   ├── scalar.go                  # last-write-wins scalar
│   │   ├── scalar_test.go
│   │   ├── counter.go                 # additive counter
│   │   ├── counter_test.go
│   │   ├── reference.go               # naive reference impl for property tests
│   │   └── projection_property_test.go
│   ├── oracle/
│   │   ├── oracle.go
│   │   ├── entry.go
│   │   ├── shard.go
│   │   ├── ring.go                    # bounded per-key event ring
│   │   ├── ring_test.go
│   │   ├── settle.go                  # settled / in-flight index
│   │   ├── settle_test.go
│   │   ├── evict.go
│   │   ├── evict_test.go
│   │   ├── oracle_test.go
│   │   ├── oracle_property_test.go
│   │   └── oracle_bench_test.go
│   ├── target/
│   │   ├── target.go                  # Target interface + registry
│   │   ├── redis.go
│   │   ├── redis_test.go              # miniredis
│   │   ├── redis_integration_test.go  # //go:build integration (testcontainers)
│   │   ├── memory.go                  # in-memory fake
│   │   ├── recording.go               # read-only enforcement wrapper
│   │   ├── recording_test.go
│   │   └── target_bench_test.go
│   ├── differ/
│   │   ├── differ.go
│   │   ├── report.go
│   │   ├── categorize.go
│   │   ├── differ_test.go
│   │   └── differ_property_test.go
│   ├── sweeper/
│   │   ├── sweeper.go
│   │   ├── confirm.go
│   │   ├── extras.go
│   │   ├── sweeper_test.go
│   │   └── confirm_test.go
│   ├── lag/
│   │   ├── estimator.go
│   │   ├── probe.go
│   │   ├── estimator_test.go
│   │   └── adaptive.go
│   ├── check/
│   │   ├── check.go                   # assembles a full Check from spec
│   │   ├── config.go
│   │   ├── config_test.go
│   │   └── check_test.go
│   ├── metrics/
│   │   ├── metrics.go
│   │   ├── metrics_test.go            # asserts names + cardinality
│   │   └── registry.go
│   ├── explain/
│   │   ├── explain.go
│   │   ├── render.go                  # text + json output
│   │   ├── explain_test.go
│   │   └── testdata/                  # golden files
│   ├── clock/
│   │   ├── clock.go                   # Clock interface, real + fake
│   │   └── clock_test.go
│   └── logging/
│       └── logging.go                 # logr/zap setup, redaction
├── test/
│   ├── harness/
│   │   ├── harness.go                 # in-process full-stack harness
│   │   ├── faultinjector/
│   │   │   ├── injector.go            # Source middleware
│   │   │   ├── drop.go
│   │   │   ├── reorder.go
│   │   │   ├── duplicate.go
│   │   │   ├── delay.go
│   │   │   ├── partition.go
│   │   │   ├── corrupt.go
│   │   │   └── injector_test.go
│   │   ├── publisher/
│   │   │   └── publisher.go           # synthetic event producer
│   │   ├── materializer/
│   │   │   └── materializer.go        # reference consumer that writes to target
│   │   └── scenario/
│   │       ├── scenario.go            # declarative scenario DSL
│   │       └── scenario_test.go
│   ├── faults/                        # the fault matrix, one test per row
│   │   ├── drop_test.go
│   │   ├── reorder_test.go
│   │   ├── duplicate_test.go
│   │   ├── delay_test.go
│   │   ├── restart_test.go
│   │   ├── clockskew_test.go
│   │   ├── target_mutation_test.go
│   │   ├── target_flush_test.go
│   │   ├── target_evict_test.go
│   │   ├── target_failover_test.go
│   │   ├── partition_test.go
│   │   ├── malformed_test.go
│   │   ├── saturation_test.go
│   │   └── bootstrap_test.go
│   ├── e2e/
│   │   ├── suite_test.go              # ginkgo bootstrap
│   │   ├── kind.go                    # cluster lifecycle
│   │   ├── fixtures.go                # deploy redis, publisher, driftwatch
│   │   ├── diagnostics.go             # failure dump collection
│   │   ├── happy_test.go
│   │   ├── drop_test.go
│   │   ├── explain_test.go
│   │   ├── operator_test.go
│   │   ├── chaos_test.go
│   │   └── manifests/
│   │       ├── kind-config.yaml
│   │       ├── redis.yaml
│   │       ├── publisher.yaml
│   │       ├── materializer.yaml
│   │       └── driftcheck.yaml
│   ├── soak/
│   │   └── soak_test.go
│   └── interop/
│       ├── publisher.py               # pyzmq publisher for wire-compat test
│       └── README.md
├── config/                            # kustomize, kubebuilder-style
│   ├── crd/
│   ├── rbac/
│   ├── manager/
│   ├── webhook/
│   ├── prometheus/
│   └── samples/
├── deploy/
│   ├── helm/driftwatch/
│   └── grafana/
│       └── driftwatch-dashboard.json
├── docs/
│   ├── README.md                      # docs index
│   ├── ARCHITECTURE.md
│   ├── CORRECTNESS.md                 # the §5 reasoning, written for humans
│   ├── DISCOVERIES.md                 # ⚠ primary deliverable
│   ├── DECISIONS.md                   # ADR log
│   ├── KNOWN_GAPS.md
│   ├── TESTING.md
│   ├── OPERATIONS.md                  # runbook: what each alert means
│   ├── ADDING_A_SOURCE.md
│   ├── ADDING_A_PROJECTION.md
│   ├── METRICS.md                     # generated from code
│   └── evidence/                      # ⚠ captured terminal output
│       └── README.md                  # index: file → claim it proves
├── hack/
│   ├── boilerplate.go.txt
│   ├── verify-metrics-docs.sh
│   ├── verify-codegen.sh
│   └── install-tools.sh
├── .golangci.yml
├── .goreleaser.yaml
├── Dockerfile
├── Makefile
├── PROJECT                            # kubebuilder metadata
├── go.mod
├── go.sum
├── LICENSE
├── CONTRIBUTING.md
└── README.md
```

---

## 8. Technology decisions

Each decision must also be recorded in `docs/DECISIONS.md` in ADR form (context / options / decision / consequences).

### 8.1 ZeroMQ binding: pure Go, not cgo

**Decision: use `github.com/go-zeromq/zmq4` (pure Go), not `github.com/pebbe/zmq4` (cgo + libzmq).**

Rationale:
- No cgo means `CGO_ENABLED=0` static binaries, trivial multi-arch cross-compilation, `FROM scratch`/distroless images, and no libzmq version-matching pain in CI or Kind node images.
- ZMTP 3.1 is a documented wire protocol; the pure-Go implementation is wire-compatible with libzmq for PUB/SUB.

**Risk and required mitigation:** wire compatibility must be *proven*, not assumed. §16.6 mandates an interop test where a real `pyzmq` (libzmq-backed) publisher feeds the Go subscriber, run in CI under a build tag. If a compatibility gap is found (particularly around subscription-prefix filtering or multipart framing), record it in `DISCOVERIES.md` — that finding alone is worth the README space.

**Known behaviour to handle explicitly:** ZMQ PUB sockets drop messages for slow subscribers once the high-water mark is reached, silently. Set `SUB` receive HWM explicitly and document the value. driftwatch's own ingest buffer must be larger than the socket HWM so that the drop, when it happens, happens in driftwatch's own countable buffer rather than invisibly in the socket. This is a real design subtlety — put it in `DISCOVERIES.md`.

### 8.2 Redis client: `go-redis/v9`

`github.com/redis/go-redis/v9`. Mature, context-aware, supports cluster and sentinel, has a `Pipeline` and `Scan` iterator. Use `SCAN` with `COUNT` tuning, never `KEYS`. Use pipelining for batched key reads in the sweep (default batch 500).

### 8.3 Kubernetes: kubebuilder + controller-runtime

Scaffold with kubebuilder v4. `sigs.k8s.io/controller-runtime`. Use `envtest` for controller tests (fast, no cluster) and Kind only for full e2e.

### 8.4 Dependency list

Pin all versions in `go.mod`. Do not add anything outside this list without an ADR.

| Dependency | Purpose |
|---|---|
| `github.com/go-zeromq/zmq4` | ZMQ source (pure Go) |
| `github.com/redis/go-redis/v9` | Redis target |
| `github.com/nats-io/nats.go` | NATS source |
| `sigs.k8s.io/controller-runtime` | operator framework |
| `k8s.io/apimachinery`, `k8s.io/client-go` | K8s types/clients |
| `github.com/spf13/cobra`, `github.com/spf13/pflag` | CLI |
| `github.com/prometheus/client_golang` | metrics |
| `github.com/go-logr/logr` + `go.uber.org/zap` | structured logging |
| `github.com/vmihailenco/msgpack/v5` | msgpack codec |
| `github.com/cespare/xxhash/v2` | key sharding hash (fast, non-crypto) |
| `github.com/stretchr/testify` | assertions |
| `pgregory.net/rapid` | property-based testing |
| `go.uber.org/goleak` | goroutine leak detection |
| `github.com/alicebob/miniredis/v2` | fast in-process Redis fake |
| `github.com/testcontainers/testcontainers-go` | real Redis in integration tests |
| `github.com/onsi/ginkgo/v2` + `github.com/onsi/gomega` | e2e suite |
| `sigs.k8s.io/kind` (tool, not lib) | e2e cluster |
| `github.com/Shopify/toxiproxy/v2` (client) | network fault injection in e2e |

**Explicitly rejected:**
- `pebbe/zmq4` — cgo (see §8.1).
- Any ORM or query builder — there is no SQL.
- `viper` — config comes from flags, env, and the CRD; viper's precedence magic is not worth the dependency.
- OpenTelemetry tracing — Prometheus metrics are sufficient for this tool's purpose; adding OTel is scope creep. Note it as a possible future extension in `KNOWN_GAPS.md`.

### 8.5 Go version and build

- Go 1.23 minimum (for `range`-over-func iterators in the oracle, and `slices`/`maps` stdlib).
- `CGO_ENABLED=0` always.
- Version info injected via ldflags into `internal/buildinfo`.
- Container: multi-stage, final stage `gcr.io/distroless/static:nonroot`, non-root UID 65532, read-only root filesystem, no shell.

---

## 9. Module specifications

Each module below gives: **responsibility**, **exact Go interface**, **required behaviours**, **edge cases that must be handled**, and **required tests**. The interfaces are normative — implement these signatures.

---

### M1 — `pkg/clock`

**Responsibility.** Inject time so every other module is testable. Build this first; nothing else can be tested properly without it.

```go
package clock

// Clock abstracts time for testability.
type Clock interface {
    // Now returns the current wall-clock time. Use only for display and
    // for timestamps written to output, never for elapsed-time decisions.
    Now() time.Time

    // Since returns elapsed time using a monotonic source.
    Since(t time.Time) time.Duration

    // NewTicker returns a ticker driven by this clock.
    NewTicker(d time.Duration) Ticker

    // NewTimer returns a timer driven by this clock.
    NewTimer(d time.Duration) Timer

    // Sleep blocks for d, respecting ctx cancellation.
    Sleep(ctx context.Context, d time.Duration) error
}

type Ticker interface {
    C() <-chan time.Time
    Stop()
    Reset(d time.Duration)
}

type Timer interface {
    C() <-chan time.Time
    Stop() bool
    Reset(d time.Duration) bool
}

// Real returns a Clock backed by the time package.
func Real() Clock

// Fake returns a manually-advanced Clock for tests.
func Fake(start time.Time) FakeClock

type FakeClock interface {
    Clock
    // Advance moves time forward and fires any due tickers/timers
    // synchronously before returning.
    Advance(d time.Duration)
    // BlockUntil waits until n goroutines are waiting on this clock.
    // Prevents flaky tests that advance before a waiter has registered.
    BlockUntil(n int)
}
```

**Required behaviours.**
- `Fake.Advance` must fire due timers/tickers **synchronously and deterministically** before returning, so tests never need `time.Sleep`.
- `Fake.BlockUntil(n)` is essential: without it, `Advance` can fire before the code under test has registered its waiter, producing intermittent failures.
- A ticker whose channel is not drained must drop ticks (matching `time.Ticker` semantics), not block.

**Edge cases.** `Advance(0)`; `Advance` past multiple ticks of the same ticker (must fire the correct count per `time.Ticker` semantics — coalesce to one pending tick); `Stop` during `Advance`; `Reset` to a shorter interval mid-flight; concurrent `Advance` calls (serialize with a mutex).

**Tests.** Table-driven for each edge case; a test proving `Advance` is deterministic across 1,000 runs of a multi-ticker scenario; `goleak` in `TestMain`.

---

### M2 — `pkg/event`

**Responsibility.** The core immutable data types. No logic beyond validation and cheap accessors.

```go
package event

type Op uint8

const (
    OpUnknown Op = iota
    OpSet                 // scalar assignment: Key = Value
    OpDelete              // remove Key entirely
    OpAdd                 // add Member to the set at Key
    OpRemove              // remove Member from the set at Key
    OpIncr                // add Delta to the counter at Key
    OpSnapshotBegin       // publisher begins a full resync
    OpSnapshotEnd         // publisher completes a full resync
    OpHeartbeat           // liveness only; advances seq, touches no key
)

func (o Op) String() string
func (o Op) TouchesKey() bool   // false for snapshot markers and heartbeat
func ParseOp(s string) (Op, error)

// Event is an immutable observed state change.
// Constructed only via New* helpers so invariants hold.
type Event struct {
    Publisher string
    Epoch     uint64
    Seq       uint64

    // PublishedAt is the producer's wall clock. Diagnostic only:
    // never used for settlement decisions (clock skew).
    PublishedAt time.Time

    // ObservedAt is driftwatch's local receive time, monotonic.
    // This is the authoritative time for all elapsed-time logic.
    ObservedAt time.Time

    Topic  string
    Op     Op
    Key    string
    Member string
    Value  []byte
    Delta  int64
    TTL    *time.Duration

    // Raw is the original wire bytes, retained only if
    // retainRaw is enabled (memory cost). Used by `explain`.
    Raw []byte
}

// Validate returns an error if the event violates a structural invariant
// for its Op (e.g. OpAdd without a Member).
func (e *Event) Validate() error

// Fingerprint returns a stable identity for dedup: publisher/epoch/seq.
func (e *Event) Fingerprint() Fingerprint

type Fingerprint struct{ Publisher string; Epoch, Seq uint64 }

// Value is the oracle-side representation of a key's state.
type Value struct {
    Kind    ValueKind          // ValueScalar | ValueSet | ValueCounter | ValueAbsent
    Scalar  []byte
    Members map[string]struct{}
    Counter int64
}

func (v Value) Equal(other Value) bool
func (v Value) Clone() Value
func (v Value) IsAbsent() bool
func (v Value) String() string       // truncated, safe for logs
```

**Required behaviours.**
- `Event` is treated as immutable after construction. Never mutate a received event; projections return new values.
- `Validate` rules: `OpSet` requires non-nil `Value`; `OpAdd`/`OpRemove` require non-empty `Member`; `OpIncr` requires `Delta != 0`; `OpDelete` requires only `Key`; snapshot markers and `OpHeartbeat` must have empty `Key`; `Publisher` must be non-empty for all ops; `Seq` may be 0 only as the first event of an epoch.
- `Value.Equal` must treat `nil` and empty `Members` map as equal (a set that has had all members removed equals an absent set — **or does it?** This is a real design decision: choose "empty set == absent" for Redis compatibility, because Redis deletes a set key when its last member is removed. Document this in a code comment and in `DISCOVERIES.md`, because it is exactly the kind of subtlety that causes false positives.)
- `Value.String()` must truncate to 64 bytes and hex-encode non-UTF8 bytes. It must never be used for comparison.

**Edge cases.** Empty key (`""`) — legal in Redis, must be supported; binary (non-UTF8) keys and members; keys longer than 512 MB (reject at codec level with a bounded max, default 4 KiB, configurable); very large member sets (`OpAdd` producing a set of 1M members — see M7 for the bound); `TTL` of zero vs nil (zero means "expire immediately", nil means "no TTL"); negative `Delta`.

**Tests.** Validation table covering every Op × every missing-field combination; `Value.Equal` symmetry and reflexivity property test; `Clone` deep-copy verification (mutate the clone, assert original unchanged); golden-file round-trip for `String()` with binary input.

---

### M3 — `pkg/codec`

**Responsibility.** Decode raw wire bytes into `Event`. Pluggable, because real producers have their own formats.

```go
package codec

// Codec decodes wire bytes into an Event.
// Implementations must be safe for concurrent use.
type Codec interface {
    // Name returns the registry name (e.g. "json").
    Name() string

    // Decode parses payload into dst. It must not retain payload
    // unless retainRaw is set; callers may reuse the buffer.
    Decode(payload []byte, topic string, dst *event.Event) error
}

// Registry maps names to constructors.
func Register(name string, ctor Constructor)
func New(name string, cfg map[string]string) (Codec, error)
func Names() []string

type Constructor func(cfg map[string]string) (Codec, error)
```

**Built-in codecs.**

1. **`json`** (default). Field names configurable via cfg so foreign formats work without code:
   `publisherField`, `epochField`, `seqField`, `timestampField`, `opField`, `keyField`, `memberField`, `valueField`, `ttlField`, `deltaField`, plus `opMapping` (e.g. `"BLOCK_STORED=add,BLOCK_EVICTED=remove"`). Timestamp parsing must accept RFC3339, RFC3339Nano, Unix seconds, Unix millis, and Unix nanos — auto-detected by magnitude, with the detection rule documented.

2. **`msgpack`** — same configurable field mapping, msgpack encoding.

3. **`template`** — for line-oriented or delimited formats: a regex with named capture groups mapped to fields. Slow; documented as a compatibility escape hatch, not for high throughput.

**Required behaviours.**
- Decoding must never panic on arbitrary input. Enforced by fuzzing.
- Payloads larger than `maxPayloadBytes` (default 1 MiB) are rejected with a typed error, not truncated.
- Unknown `op` values produce `ErrUnknownOp` (a sentinel), which the pipeline counts separately from malformed input — an unknown op is likely a version mismatch, not corruption, and deserves a different diagnosis.
- Errors must be typed sentinels (`ErrMalformed`, `ErrUnknownOp`, `ErrTooLarge`, `ErrMissingField`) so the pipeline can categorize.
- Zero allocations on the happy path where feasible: reuse a `sync.Pool` of decode scratch buffers. Prove with a benchmark asserting `allocs/op` below a threshold.

**Edge cases.** Empty payload; payload that is valid JSON but not an object; deeply nested JSON (depth-limit it — a JSON bomb is a real DoS vector); duplicate JSON keys; numeric seq sent as a string; seq sent as a float (must reject, since float64 loses precision above 2^53 — this is a genuine correctness trap and belongs in `DISCOVERIES.md`); null fields; unicode escapes; NaN/Inf in numeric fields; timestamp of 0.

**Tests.**
- Table-driven decode tests with a `testdata/` golden payload per case.
- **Fuzz test** (`FuzzDecodeJSON`) seeded with the golden corpus; must run in CI for 60s and never panic. Any crash found gets committed to `testdata/fuzz/` as a regression case.
- Benchmark with allocation assertion.
- Timestamp auto-detection table covering all five accepted formats plus ambiguous boundary values.

---

### M4 — `pkg/source`

**Responsibility.** Read raw frames from a transport. Reconnect. Never block the pipeline.

```go
package source

type RawMessage struct {
    Topic      string
    Payload    []byte
    ObservedAt time.Time     // set by the source on receipt
}

// Source delivers raw messages until ctx is cancelled or Close is called.
type Source interface {
    Name() string

    // Run reads from the transport and sends to out until ctx is done.
    // Run must return only after all its goroutines have exited.
    // Implementations must not close out; the caller owns it.
    Run(ctx context.Context, out chan<- RawMessage) error

    // Stats returns transport-level counters for diagnostics.
    Stats() Stats

    // Close releases transport resources. Idempotent.
    Close() error
}

type Stats struct {
    Connected      bool
    Reconnects     uint64
    FramesReceived uint64
    BytesReceived  uint64
    LastFrameAt    time.Time
    LastError      string
}

func Register(name string, ctor Constructor)
func New(name string, cfg Config, clk clock.Clock) (Source, error)
```

**Implementations.**

**`memory`** — in-process channel. Used by every unit test and by the fault injector. Must support `Publish(RawMessage)` and a `Backlog()` accessor.

**`file`** — replay newline-delimited JSON from a file or stdin. Config: `path`, `speed` (`asFastAsPossible` | `realtime` | a multiplier), `loop`. This is how a captured production event stream gets replayed against a new projection — a genuinely useful feature and the backbone of `driftwatch replay`.

**`zmq`** — SUB socket. Config: `endpoints` (list), `topics` (subscription prefixes; empty means subscribe-all), `recvHWM` (default 100000), `connectTimeout`, `reconnectIntervalMax`.
Required behaviours:
- Connect to all endpoints on one SUB socket (ZMQ supports multiple connects per socket).
- Set `SUBSCRIBE` for each topic prefix; if none given, subscribe to `""`.
- Handle multipart frames: by convention frame 0 is the topic, frame 1 the payload. If a single-frame message arrives, treat the whole frame as payload with topic `""`. **Both conventions exist in the wild — support both and document it.**
- Reconnect with exponential backoff + full jitter on any receive error.
- **On reconnect, signal a possible gap** via a `GapSignal` channel so the pipeline can mark keys `Suspect`. This is essential and easy to forget.
- Explicitly set and report `recvHWM`, and document the interaction with the ingest buffer (§8.1).

**`nats`** — core NATS subscription (not JetStream; JetStream has durable consumers and would be a different, easier problem). Config: `url`, `subjects`, `queueGroup` (must be empty — a queue group would split events across replicas and break the oracle; **validate and reject a non-empty queue group with a clear error message**, since this is a plausible and catastrophic misconfiguration).

**Edge cases.** Endpoint unresolvable at startup (must not fail startup; retry forever, report `Connected: false`, and let readiness reflect it); endpoint resolves but refuses connection; connection drops mid-frame; zero-length payload; frame larger than `maxPayloadBytes`; all endpoints down then one recovers; DNS re-resolution after a pod restart changes the IP (must re-resolve on reconnect, not cache the first IP — a classic Kubernetes bug worth a `DISCOVERIES.md` entry); `Close()` called twice; `Close()` called during `Run`; context cancelled while blocked in a socket receive (must unblock within `shutdownGrace` — this requires a receive timeout on the socket rather than an indefinite block).

**Tests.** For `memory` and `file`, full unit coverage. For `zmq`, a test that spins up a real in-process pure-Go PUB socket and asserts delivery, multipart handling, topic filtering, reconnect-after-close, and HWM drop behaviour under a deliberately slow consumer. Interop test per §16.6. A test asserting `Run` returns within `shutdownGrace` of context cancellation while blocked in receive.

---

### M5 — `pkg/seqtrack`

**Responsibility.** Per-publisher sequence tracking, gap detection, epoch/restart detection, duplicate rejection. This is where §5.2 lives.

```go
package seqtrack

type Verdict uint8

const (
    Accept Verdict = iota
    AcceptWithGap
    AcceptLateFill
    AcceptAfterRestart
    AcceptFirstSeen
    DropDuplicate
    DropStaleEpoch
)

func (v Verdict) Accepted() bool

type Tracker struct{ /* ... */ }

type Config struct {
    MaxPublishers          int           // default 1024, bounds memory
    MaxGapIntervals        int           // default 1024 per publisher
    ImplicitRestartDelta   uint64        // default 1000
    ImplicitRestartCeiling uint64        // default 100
    Clock                  clock.Clock
}

func New(cfg Config) *Tracker

// Observe classifies an event and updates publisher state.
// Returns the verdict plus any gap that was newly detected.
func (t *Tracker) Observe(e *event.Event) (Verdict, *Gap)

type Gap struct {
    Publisher string
    Epoch     uint64
    From, To  uint64      // inclusive
    DetectedAt time.Time
}

// Publishers returns a snapshot of all publisher states.
func (t *Tracker) Publishers() []PublisherState

// Trust reports whether the tracker's view is complete for a publisher.
func (t *Tracker) Trust(publisher string) TrustState

// ClearGaps resets gap state for a publisher, called on snapshot completion.
func (t *Tracker) ClearGaps(publisher string)

// Reset drops all state (used on source reconnect when replay is unavailable).
func (t *Tracker) Reset()
```

**`GapSet` (separate file, separately tested).** An interval set over `uint64`.

```go
type GapSet struct{ /* sorted, coalesced intervals */ }

func NewGapSet(maxIntervals int) *GapSet
func (g *GapSet) Add(from, to uint64)          // coalesces adjacent/overlapping
func (g *GapSet) Fill(seq uint64)              // removes seq, splitting if interior
func (g *GapSet) Contains(seq uint64) bool
func (g *GapSet) Count() uint64                // total missing seqs
func (g *GapSet) Intervals() []Interval
func (g *GapSet) Truncated() bool              // maxIntervals was exceeded
func (g *GapSet) Clear()
```

**Required behaviours.** Exactly the algorithm in §5.2. Plus:
- `MaxPublishers` exceeded → evict the publisher with the oldest `LastSeen`, increment a counter, and log at WARN. Never grow unbounded.
- On `maxGapIntervals` exceeded: merge the two closest intervals repeatedly until under the cap, set `Truncated`, and never lose the *count* of missing seqs even when interval detail is lost.
- `Fill` on an interior seq splits one interval into two, which can push the count over the cap — handle this (it's the non-obvious case).

**Edge cases.** First event with `Seq == 0`; first event with `Seq == 1<<63` (a publisher that starts high); seq wraparound at `MaxUint64` (treat as an implicit restart and document); epoch going backwards then forwards again (out-of-order across a restart); two publishers with the same ID but different epochs arriving interleaved; an event with `Seq == HWM` exactly (duplicate of the newest); `Fill` of a seq not in any gap (no-op, must not corrupt); `Add(from, to)` where `from > to` (programmer error — panic in dev, but reachable from malformed input, so validate at the codec boundary and assert here).

**Tests.**
- Full table over every state-transition pair in the §5.2 algorithm.
- `GapSet` unit tests: add/coalesce/split/fill/truncate, each with explicit expected interval lists.
- **Property tests (I4):** generate a random event sequence, withhold a random subset, feed the rest in random order, assert `GapSet.Count()` equals exactly the number withheld and `Contains` is true for each withheld seq. Run with `rapid` at 1,000+ iterations.
- **Property test (I9):** interval count never exceeds the cap under any generated input.
- **Property test:** duplicates are never accepted twice (I1/I5).
- Fuzz `Observe` with random events; must never panic.

---

### M6 — `pkg/projection`

**Responsibility.** The pure fold from events to expected state. **Must contain zero I/O, zero clock access, and zero randomness** — this purity is what makes property testing possible.

```go
package projection

// Mutation is the result of applying one event to one key.
type Mutation struct {
    Key    string
    Action Action        // ActionUpsert | ActionDelete | ActionNone
    Value  event.Value   // meaningful for ActionUpsert
    TTL    *time.Duration
}

// Projection folds events into expected target state.
// Implementations MUST be pure: same (prev, event) always yields
// the same Mutation, with no side effects.
type Projection interface {
    Name() string

    // Apply computes the new state for the key the event touches.
    // prev is the current value (event.Value with Kind ValueAbsent if none).
    // Returning ActionNone means the event does not affect target state.
    Apply(prev event.Value, e *event.Event) (Mutation, error)

    // Commutative reports whether event order affects the final state.
    // If false, the oracle must order by seq before applying.
    Commutative() bool

    // KeyOwnership describes whether publishers own disjoint keyspaces.
    // Used to scope Suspect-marking after a gap (§5.2).
    KeyOwnership() OwnershipModel

    // TargetShape describes how the value maps onto the target store,
    // so the Target adapter knows which read command to issue.
    TargetShape() Shape       // ShapeScalar | ShapeSet | ShapeCounter
}

type OwnershipModel struct {
    Partitioned bool
    // KeyPattern, if Partitioned, is a template that expands to the
    // key prefix a given publisher may write, e.g. "replica:{{.Publisher}}:*".
    KeyPattern string
}

func Register(name string, ctor Constructor)
func New(name string, cfg map[string]string) (Projection, error)
```

**Built-in projections.**

**`keysetOwnership`** — the KV-cache-index shape and the flagship case. Maintains `key → set of members`. `OpAdd` adds, `OpRemove` removes, `OpDelete` clears, `OpSet` replaces the whole set from a delimited value. Config: `keyTemplate` (default `"{{.Key}}"`), `memberTemplate` (default `"{{.Member}}"`), `setDelimiter` for `OpSet`. Commutative: **false** — `add` then `remove` differs from `remove` then `add`. Shape: `ShapeSet`.
Critical behaviour: when the last member is removed, emit `ActionDelete`, not an upsert with an empty set — because Redis deletes empty set keys. Getting this wrong produces a permanent false `extra_in_target`/`missing_in_target` pair on every key that empties. **This is the single most likely bug in the project. Write the test first.**

**`scalar`** — last-write-wins `key → bytes`. `OpSet` upserts, `OpDelete` deletes, `OpAdd`/`OpRemove`/`OpIncr` are errors. Commutative: **false**. Shape: `ShapeScalar`.

**`counter`** — `key → int64` with `OpIncr` applying `Delta`. Commutative: **true** (addition commutes). `OpSet` sets absolutely; `OpDelete` deletes. Shape: `ShapeCounter`.
Note the subtlety: a counter is commutative *only* if every event is `OpIncr`. A mix of `OpIncr` and `OpSet` is not commutative. Therefore `Commutative()` must return false unless config declares `incrOnly: true`. Document this; it is a good example of why the flag is per-projection-instance, not per-type.

**`reference`** (test-only, in `reference.go`). A deliberately naive, obviously-correct implementation using a plain map and no optimizations, used as the differential-testing oracle in property tests. If the optimized projection and the reference ever disagree on any generated input, that is a bug in the optimized one.

**Required behaviours.**
- Template expansion is compiled once at construction, never per event (benchmark this).
- An `Apply` that returns an error must not be silently dropped: the pipeline counts `driftwatch_projection_errors_total{projection,reason}` and logs the first N per minute.
- `Apply` must handle `prev.Kind` mismatching the projection's shape (e.g. a scalar projection receiving a set-valued prev) by returning a typed error rather than panicking. This happens in practice when a `DriftCheck`'s projection is changed without clearing the oracle.

**Edge cases.** `OpAdd` of a member already present (idempotent, no version bump needed — but bump anyway for simplicity and document why); `OpRemove` of a member not present (no-op); `OpRemove` on an absent key (no-op, must not create the key); `OpDelete` on an absent key (no-op); `OpIncr` on an absent key (creates it at `Delta`); `OpIncr` overflow (saturate at `MaxInt64`, count `driftwatch_counter_overflows_total`); a member set exceeding `maxMembersPerKey` (default 100,000 — reject the add, count it, mark the key as `Truncated`, and never grow unbounded); template expansion producing an empty key; template referencing a field the event doesn't have.

**Tests.**
- Table-driven per projection covering every Op × every prev-state combination, including all the edge cases above.
- **The empty-set-becomes-delete test, written first and named explicitly** (`TestKeysetOwnership_LastMemberRemoval_YieldsDelete`).
- **Property test (I2):** for `counter` with `incrOnly`, all permutations of a generated event set yield identical state.
- **Property test (I3):** for `keysetOwnership`, applying in seq order matches the reference implementation; applying in shuffled order and then sorting-and-replaying converges to the same state.
- **Differential property test:** optimized vs `reference` implementation over 10,000 generated event sequences.
- Purity test: call `Apply` 100 times with identical inputs, assert byte-identical outputs and that `prev` is unmodified.
- Benchmark: `Apply` throughput and `allocs/op`, with a regression threshold.

---

### M7 — `pkg/oracle`

**Responsibility.** Hold the expected state. Sharded, bounded, versioned, with a per-key event ring for `explain` and a settled/in-flight index.

```go
package oracle

type Oracle struct{ /* ... */ }

type Config struct {
    Shards           int              // default 64
    MaxTrackedKeys   int              // default 1_000_000
    RingSize         int              // per-key event history, default 16
    RetainRaw        bool             // keep Event.Raw in the ring
    SettlementWindow time.Duration    // may be updated at runtime
    Clock            clock.Clock
}

func New(cfg Config) *Oracle

// Apply mutates the oracle. Called ONLY from the single applier goroutine.
func (o *Oracle) Apply(m projection.Mutation, e *event.Event, trust TrustState) ApplyResult

type ApplyResult struct {
    Key        string
    Version    uint64
    Created    bool
    Deleted    bool
    Evicted    string        // non-empty if a key was evicted to make room
}

// Get returns a snapshot copy of the entry. Safe for concurrent use.
func (o *Oracle) Get(key string) (Entry, bool)

// Version returns just the version, cheaply. Used for fencing.
func (o *Oracle) Version(key string) (uint64, bool)

// SettledKeys returns an iterator over keys settled as of now.
// The iterator holds no lock between yields; callers must use
// version fencing (§5.5) when comparing.
func (o *Oracle) SettledKeys(now time.Time) func(yield func(string) bool)

// Counts returns cardinalities for metrics.
func (o *Oracle) Counts(now time.Time) Counts

type Counts struct {
    Total     int
    Settled   int
    InFlight  int
    ByTrust   map[TrustState]int
    Truncated int
}

// History returns the per-key event ring, oldest first.
func (o *Oracle) History(key string) []HistoryEntry

type HistoryEntry struct {
    Event      event.Event
    Verdict    seqtrack.Verdict
    ResultValue event.Value
    Version    uint64
    AppliedAt  time.Time
}

// AdoptSnapshot loads baseline state from the target (bootstrap Adopt mode).
func (o *Oracle) AdoptSnapshot(entries map[string]event.Value, at time.Time)

// SetSettlementWindow updates W at runtime (adaptive mode).
func (o *Oracle) SetSettlementWindow(d time.Duration)

// MarkSuspect flags keys as untrustworthy after a gap.
// pattern == "" means all keys.
func (o *Oracle) MarkSuspect(pattern string, reason string)

// ClearSuspect returns keys to Complete after a snapshot cycle.
func (o *Oracle) ClearSuspect(pattern string)
```

```go
type TrustState uint8

const (
    TrustComplete TrustState = iota  // no known gaps affecting this key
    TrustSuspect                     // a gap may have affected it
    TrustAdopted                     // loaded from target at bootstrap, never event-confirmed
)

type Entry struct {
    Key         string
    Value       event.Value
    Version     uint64
    Trust       TrustState
    LastEventAt time.Time     // monotonic local
    LastSeq     uint64
    LastEpoch   uint64
    LastPublisher string
    TTL         *time.Duration
    Truncated   bool          // member set hit the cap
    CreatedAt   time.Time
}
```

**Required behaviours.**
- **Sharding** by `xxhash(key) % Shards`, each shard with its own `RWMutex`. Never take two shard locks at once (no cross-key operations exist, so this is easy to guarantee — assert it in review).
- **Version** increments on every applied mutation, monotonically, per key, never reused. A deleted-then-recreated key continues from the previous version, not from zero — otherwise fencing breaks across a delete.
- **Settled/in-flight index.** Do not scan all keys to compute this. Maintain a per-shard min-heap (or a coarse time-bucketed ring of key sets, which is cheaper) ordered by `LastEventAt`, so `SettledKeys` yields in O(settled) and `Counts` is O(1)-ish. Naive full scans at 1M keys every 30s is 
  a performance bug that will show up in the benchmark.
- **Bounded memory.** On reaching `MaxTrackedKeys`, evict the key with the oldest `LastEventAt` from the fullest shard (approximate global LRU — exact global LRU across shards requires cross-shard coordination and is not worth it; document the approximation). Count evictions and expose the resulting coverage loss.
- **Per-key ring** is a fixed-size circular buffer of `RingSize` entries. It must never grow. If `RetainRaw` is false, `Event.Raw` is nilled before storing, which is the difference between ~200 bytes and ~2 KB per history entry at 16 entries per key — at 1M keys that is 3 GB vs 300 MB. This matters and must be the default.
- `Get` returns a **deep copy** (member maps cloned). Returning a reference into the shard is a data race waiting to happen.

**Edge cases.** Apply to a key at exactly `MaxTrackedKeys` (evict then insert, must not evict the key being inserted); delete of a non-existent key; `AdoptSnapshot` with more entries than `MaxTrackedKeys`; `SetSettlementWindow` to a smaller value (keys become settled immediately — must not break the index); `SetSettlementWindow` to zero (everything settled; legal, used in tests); `History` on an evicted key (returns empty, must not error); concurrent `Get` during `Apply` on the same key (the version-fencing test must exercise this under `-race` with 100 goroutines); a key whose value is a 100,000-member set (memory and clone cost — benchmark it); `MarkSuspect("")` on 1M keys (must not take a global lock for seconds — do it per shard, and consider a generation counter instead of touching every entry: a global `suspectGeneration` incremented once, with per-entry comparison, turns an O(n) write into O(1). Prefer this. It is the kind of optimization that shows systems thinking.)

**Tests.**
- Unit tests for every method and edge case above.
- **Property test (I8):** under any generated sequence of applies, tracked keys never exceed `MaxTrackedKeys` and ring length never exceeds `RingSize`.
- **Property test (I12):** a concurrent reader using version fencing never observes a torn or superseded value. Implement as a `-race` test with one applier and 50 readers over 10,000 events.
- **Property test:** version is strictly monotonic per key across creates, deletes, and recreates.
- Benchmarks: `BenchmarkApply`, `BenchmarkGet`, `BenchmarkSettledKeys1M`, `BenchmarkOracle1M` (memory: assert RSS via `runtime.ReadMemStats` under the S5 budget), `BenchmarkMarkSuspectAll1M` (must be sub-millisecond with the generation-counter approach).
- `goleak` in `TestMain`.

---

### M8 — `pkg/target`

**Responsibility.** Read actual state from the external store. **Read-only, structurally enforced.**

```go
package target

// Target reads state from the audited store. Implementations MUST NOT
// issue any mutating command. This is enforced in tests by RecordingTarget.
type Target interface {
    Name() string

    // Get reads one key, shaped per the projection.
    Get(ctx context.Context, key string, shape projection.Shape) (event.Value, error)

    // GetMany reads a batch, pipelined. Order of results matches keys.
    // A missing key yields a Value with Kind ValueAbsent, not an error.
    GetMany(ctx context.Context, keys []string, shape projection.Shape) ([]event.Value, error)

    // Scan iterates the keyspace matching pattern. Must use a
    // non-blocking cursor (Redis SCAN), never KEYS.
    Scan(ctx context.Context, pattern string, batch int) Iterator

    // TTL returns the remaining TTL, or nil if none, or ErrNotFound.
    TTL(ctx context.Context, key string) (*time.Duration, error)

    // Health returns store-level diagnostics used to explain sweeps.
    Health(ctx context.Context) (Health, error)

    Close() error
}

type Iterator interface {
    Next(ctx context.Context) bool
    Keys() []string          // current batch
    Err() error
    Close() error
}

type Health struct {
    Reachable        bool
    EvictedKeys      uint64      // Redis INFO stats
    ExpiredKeys      uint64
    UsedMemoryBytes  uint64
    MaxMemoryBytes   uint64
    KeyspaceSize     int64
    Role             string      // master | replica
    ReplicationLagMs int64
    Version          string
}
```

**`redis` implementation.**
- `go-redis/v9`, supporting standalone, sentinel, and cluster (cluster changes `SCAN` semantics — you must scan each master; handle it or explicitly reject cluster mode in v1 with a clear error. Prefer: support it, because it's a real deployment and the `ClusterClient.ForEachMaster` helper makes it tractable).
- `GetMany` pipelines with a configurable batch size (default 500). For `ShapeSet` use `SMEMBERS`; for `ShapeScalar` use `GET`; for `ShapeCounter` use `GET` + integer parse.
- **`WRONGTYPE` handling:** if the target key holds a different type than the shape expects, that is itself a form of drift (`value_mismatch` with reason `type_mismatch`), not an error to swallow. Surface it.
- `Scan` uses `SCAN ... MATCH ... COUNT`, honouring the cursor contract. Document explicitly that `SCAN` guarantees only that keys present for the whole iteration are returned at least once — keys may be returned more than once, and keys added or removed mid-scan may or may not appear. Deduplicate within a scan. This guarantee is precisely why §5.5 treats extras conservatively.
- `Health` parses `INFO stats`, `INFO memory`, `INFO replication`, and `DBSIZE`.
- Read-only enforcement: build the client with a command hook (`redis.Hook`) that returns an error for any command outside the allowlist. Belt and braces alongside `RecordingTarget`.
- **Replica reads:** if `Health.Role == "replica"`, log at WARN once and set a condition, because a replica can serve stale data and produce phantom drift. Optionally refuse (`policy.requirePrimary: true`).

**`memory` implementation.** A plain map guarded by a mutex, with an injectable `Latency` and `FailureRate` so the sweeper's error paths can be tested deterministically. Also supports `SimulateEvict(n)` and `SimulateFlush()` for fault tests.

**`recording` wrapper.** Wraps any Target, records every method call, and **fails the test immediately** if a mutating operation is attempted. Used in every test. This is the structural enforcement of NG1/I13.

**Edge cases.** Key absent (must return `ValueAbsent`, not an error — this distinction drives everything downstream); empty set (Redis returns an empty array, which must map to `ValueAbsent` per the M2 decision); `WRONGTYPE`; key expires between `SCAN` and `GET` (must be treated as absent, then handled by two-phase confirm); connection lost mid-pipeline (partial results must be discarded entirely, not partially applied); `SCAN` cursor invalidated by a `FLUSHDB` mid-iteration (Redis restarts the cursor at 0 — detect the loop and abort the scan with a typed error rather than spinning forever; **this is a real trap worth a `DISCOVERIES.md` entry**); a keyspace with 10M keys (scan must be interruptible by context and must not accumulate all keys in memory); `MEMORY USAGE` unavailable on old Redis; auth failure; TLS; a key containing the pattern metacharacters `*?[]`.

**Tests.**
- `miniredis` unit tests for every method and edge case (fast, runs everywhere).
- **Integration tests against real Redis** via testcontainers, build-tagged `integration`, covering: `SCAN` over 100k keys, pipelining, `WRONGTYPE`, cluster mode, sentinel failover, TTL, `INFO` parsing across Redis 6/7 (version differences in `INFO` output are a genuine source of parsing bugs — test both).
- `RecordingTarget` test proving it catches an attempted write.
- A test that the redis Hook rejects `SET` even if called directly.
- Benchmarks: `BenchmarkGetMany500`, `BenchmarkScan1M`.

---

### M9 — `pkg/differ`

**Responsibility.** Compare oracle entries against target values and categorize disagreements. Pure.

```go
package differ

type Category uint8

const (
    CatMissingInTarget Category = iota   // oracle has it, target doesn't
    CatExtraInTarget                     // target has it, oracle doesn't
    CatValueMismatch                     // both have it, scalars differ
    CatMemberMismatch                    // both have it, set membership differs
    CatTypeMismatch                      // target holds an unexpected type
    CatTTLMismatch                       // TTL differs beyond tolerance
    CatCounterMismatch
)

type Finding struct {
    Key           string
    Category      Category
    Trust         oracle.TrustState
    OracleValue   event.Value
    TargetValue   event.Value
    MissingMembers []string      // in oracle, absent from target
    ExtraMembers   []string      // in target, absent from oracle
    OracleVersion uint64
    LastEventAt   time.Time
    LastSeq       uint64
    LastPublisher string
    FirstSeenAt   time.Time      // when this finding first appeared
    Confirmed     bool
}

// Compare produces a Finding, or nil if the values agree.
func Compare(key string, oe oracle.Entry, tv event.Value, opts Options) *Finding

type Options struct {
    TTLTolerance   time.Duration    // default 5s
    ExpiryPolicy   ExpiryPolicy     // Ignore | Model | Strict
    AssumedTTL     time.Duration    // for ExpiryPolicy Ignore
    MaxMembersReported int          // truncate member diffs, default 20
}

// Report aggregates findings for one sweep.
type Report struct {
    StartedAt, FinishedAt time.Time
    KeysCompared          int
    KeysSkippedInFlight   int
    KeysSkippedSuspect    int
    Findings              []Finding
    ByCategory            map[Category]int
    ByTrust               map[oracle.TrustState]int
    EvictionSuspected     bool
    TargetHealth          target.Health
    Truncated             bool          // finding list hit a cap
}

func (r *Report) Summary() string        // one-line, for logs
func (r *Report) Text() string           // human-readable multi-line
func (r *Report) JSON() ([]byte, error)
```

**Required behaviours.**
- Member diffs must be **truncated** at `MaxMembersReported` with a count of the remainder. A key with 100,000 divergent members must not produce a 100,000-line report.
- The `Findings` slice must be capped (`maxFindings`, default 10,000) with `Truncated` set. Mass divergence must not OOM the reporter.
- TTL comparison uses a tolerance, because a TTL read at time T is inherently a moving target.
- `CatExtraInTarget` requires special handling per §5.5 — the differ produces the finding, but the *sweeper* decides whether to report it based on the conservative extras rules.

**Edge cases.** Both absent (return nil — not a finding); oracle absent + target present + `TrustAdopted` (suppress); scalar of length 0 vs absent (per M2 decision, these differ — document which is which); sets differing only in order (must be equal); counter mismatch of exactly 1 (real, report it — don't add a fudge factor); TTL present in oracle but not target under each `ExpiryPolicy`; a `Finding` for a key whose oracle entry was evicted between compare and report (tolerate; the sweeper re-queues).

**Tests.** A comprehensive table: every `(oracleKind, targetKind, oracleValue, targetValue, trust, expiryPolicy)` combination that is reachable, with the expected `Category` or nil. **Property test (I6):** `Compare` returns nil iff `oe.Value.Equal(tv)` for `TrustComplete` settled keys — generated over random values including binary and large sets. Golden-file tests for `Text()` and `JSON()` output.

---

### M10 — `pkg/sweeper`

**Responsibility.** Orchestrate §5.3, §5.4, §5.5. This is where the correctness mechanisms are composed.

```go
package sweeper

type Sweeper struct{ /* ... */ }

type Config struct {
    Oracle            *oracle.Oracle
    Target            target.Target
    Shape             projection.Shape
    DifferOptions     differ.Options
    Clock             clock.Clock
    Metrics           *metrics.Metrics

    SweepInterval     time.Duration   // default 30s
    ExtraScanInterval time.Duration   // default 5m
    ExtraScanPattern  string
    SettlementWindow  func() time.Duration   // dynamic, from lag estimator
    ReadBatchSize     int             // default 500
    MaxConfirmQueue   int             // default 10000
    RequirePrimary    bool
}

func New(cfg Config) *Sweeper

// Run drives both sweep loops until ctx is done.
func (s *Sweeper) Run(ctx context.Context) error

// SweepOnce performs one oracle→target pass synchronously.
// Exposed for the `diff` CLI and for deterministic tests.
func (s *Sweeper) SweepOnce(ctx context.Context) (*differ.Report, error)

// ScanExtrasOnce performs one target→oracle pass.
func (s *Sweeper) ScanExtrasOnce(ctx context.Context) (*differ.Report, error)

// Confirmed returns currently-confirmed findings, keyed by key.
func (s *Sweeper) Confirmed() map[string]differ.Finding

// Subscribe returns a channel of confirmed findings for the reporter.
func (s *Sweeper) Subscribe() <-chan differ.Finding
```

**`SweepOnce` algorithm — implement exactly this.**

```
1.  health := target.Health(ctx)
    if !health.Reachable: record error, return ErrTargetUnavailable  (report NOTHING)
    if RequirePrimary && health.Role != "master": return ErrNotPrimary
    evictedBefore := health.EvictedKeys

2.  now := clock.Now();  W := SettlementWindow()
    batch := []string{};  versions := map[string]uint64{}

3.  for key := range oracle.SettledKeys(now):
        e, ok := oracle.Get(key)
        if !ok: continue                          // evicted mid-iteration
        if e.Trust == TrustAdopted: continue      // never event-confirmed
        versions[key] = e.Version
        batch = append(batch, key)
        if len(batch) == ReadBatchSize:
            processBatch(batch, versions)
            batch, versions = reset
    processBatch(remaining)

4.  processBatch(keys, versions):
        vals := target.GetMany(ctx, keys, shape)
        for i, key := range keys:
            v2, ok := oracle.Version(key)
            if !ok || v2 != versions[key]:
                requeueNextSweep(key); continue    // FENCE FAILED
            e, _ := oracle.Get(key)
            if f := differ.Compare(key, e, vals[i], opts); f != nil:
                enqueueConfirm(f, e.Version)

5.  healthAfter := target.Health(ctx)
    report.EvictionSuspected = healthAfter.EvictedKeys > evictedBefore

6.  emit metrics, return report
```

**Confirmation loop (`confirm.go`).**

```
for candidate := range confirmQueue:
    wait until clock.Now() - candidate.EnqueuedAt >= W    (batched, not per-item sleep)
    v, ok := oracle.Version(candidate.Key)
    if !ok:                    discard, count "key_evicted"
    if v != candidate.Version: discard, count "oracle_advanced", requeue for next sweep
    tv := target.Get(ctx, candidate.Key, shape)
    e, _ := oracle.Get(candidate.Key)
    if f := differ.Compare(...); f == nil:
        discard, count transient_divergence_total{reason="resolved"}
    else:
        f.Confirmed = true
        if existing, ok := confirmed[key]: f.FirstSeenAt = existing.FirstSeenAt
        confirmed[key] = f
        publish to Subscribe() channel
```

**Extras scan (`extras.go`).** Two-pass, per §5.5:
```
pass1 := set of keys from target.Scan(pattern) that are NOT in oracle
        (dedup within the scan; SCAN may repeat keys)
record pass1 with timestamp
wait W
pass2 := for each k in pass1:  still in target AND still not in oracle?
report only the intersection as CatExtraInTarget
```
Bound `pass1` at `maxExtrasTracked` (default 100,000); beyond that, report the magnitude only and set `Truncated`.

**Required behaviours.**
- **Non-overlapping sweeps.** Use a guard flag; if a tick arrives while sweeping, skip and count.
- **Context cancellation must abort a sweep promptly**, including mid-`Scan`.
- **A confirmed finding that later resolves must be removed** from `Confirmed()` and counted as recovered. Otherwise `driftwatch_divergent_keys` never returns to zero and the alert never clears — which makes the whole tool useless. Implement resolution detection in the next sweep: any previously-confirmed key that now compares equal is removed and `driftwatch_drift_resolved_total` incremented. Also track and export `driftwatch_drift_duration_seconds` for how long the current drift episode has lasted.
- **Never report anything when the target is unreachable.** Absence of data is not evidence of drift. This is a correctness requirement, not a nicety.

**Edge cases.** Zero settled keys (valid; report with `KeysCompared: 0`); every key in-flight; oracle empty; target empty but oracle full (mass `missing_in_target` — should correlate with `EvictionSuspected` or a `FLUSHDB`; the report must say so); `W` changing mid-sweep (capture it once at sweep start); confirm queue full; a key confirmed divergent that is then evicted from the oracle (drop it from `Confirmed`, count separately); `SweepOnce` called concurrently with `Run` (must be safe — serialize on the same guard).

**Tests.**
- Deterministic unit tests using `memory` target, `memory` source, and `FakeClock` — **no real sleeps anywhere in this package's tests.**
- Test each numbered step's failure mode: unreachable target, fence failure, batch partial failure.
- Test that a transient divergence within W is never reported.
- Test that a real divergence *is* reported after exactly one confirm cycle.
- Test resolution: inject drift, confirm it, repair the target, assert the finding is removed and `drift_resolved_total` incremented.
- Test the extras two-pass logic including a key that appears mid-scan and then resolves.
- **Property test (I7):** any confirmed finding implies two disagreeing reads separated by ≥ W.
- **Property test (I11):** no key is ever both in-flight and reported.
- `RecordingTarget` asserts read-only across every test in this package.

---

### M11 — `pkg/lag`

**Responsibility.** Measure event→target convergence latency; drive adaptive `W`.

```go
package lag

type Estimator struct{ /* ... */ }

type Config struct {
    Oracle       *oracle.Oracle
    Target       target.Target
    Shape        projection.Shape
    Clock        clock.Clock
    Metrics      *metrics.Metrics

    ProbeCount     int             // default 200
    ProbeRotation  time.Duration   // default 1m
    MaxPollDelay   time.Duration   // give up after this, default 60s
    WindowSize     int             // observations retained, default 10000

    MinWindow      time.Duration   // default 1s
    MaxWindow      time.Duration   // default 120s
    SafetyFactor   float64         // default 3.0
    Static         *time.Duration  // if set, adaptive is disabled
}

func New(cfg Config) *Estimator

// Run drives probing until ctx is done.
func (e *Estimator) Run(ctx context.Context) error

// Observe is called by the applier for probe keys.
func (e *Estimator) Observe(key string, version uint64, at time.Time)

// SettlementWindow returns the current W. Safe for concurrent use.
func (e *Estimator) SettlementWindow() time.Duration

// Stats exposes the distribution for status reporting.
func (e *Estimator) Stats() Stats

type Stats struct {
    P50, P90, P99, Max time.Duration
    Observations       int
    TimedOut           int
    CurrentWindow      time.Duration
    Adaptive           bool
}
```

**Required behaviours.**
- Probe keys are selected by sampling from the oracle's settled set, rotating every `ProbeRotation` so hot and cold keys are both represented. Selection must be cheap — reservoir-sample during the sweep rather than scanning separately.
- Polling backoff: 10ms, 20ms, 40ms, … capped at 1s, giving up at `MaxPollDelay`. A timed-out probe is recorded as `MaxPollDelay` in the distribution (not discarded — discarding timeouts biases the estimate optimistically, which would shrink W and cause false positives. This is a statistics trap worth noting in `DISCOVERIES.md`.)
- `SettlementWindow()` must be lock-free on the read path (`atomic.Int64` of nanoseconds).
- **Hysteresis:** W must not oscillate. Only change W if the new computed value differs by more than 20%, and rate-limit changes to once per minute. Log every change at INFO with old and new values.
- If `Static` is set, `Run` is a no-op that still records the histogram (measurement is useful even when not used for control).

**Edge cases.** No probe keys available (empty oracle) — must not busy-loop; every probe times out (target is broken — W should hit `MaxWindow` and a condition should fire, not grow unbounded); a probe key deleted mid-poll (treat as converged if the target also shows absent); fewer than 100 observations (do not adapt yet; use `MinWindow` floor and report `Adaptive: false` until enough data); the histogram's p99 exceeding `MaxWindow/SafetyFactor` (clamp, log, set a condition — the materializer is slower than driftwatch can meaningfully audit).

**Tests.** Fake clock throughout. Test: convergence measurement accuracy against a `memory` target with injected latency; timeout accounting; hysteresis (assert W does not change for a 10% shift and does for a 30% shift); rate limiting; clamping at min and max; the "fewer than 100 observations" gate. Property test: `SettlementWindow()` always returns a value in `[MinWindow, MaxWindow]` under any generated observation stream.

---

### M12 — `pkg/metrics`

**Responsibility.** All Prometheus instrumentation, with bounded cardinality.

**Cardinality rules (hard requirements).**
- **Never** use a key name, member, or value as a label. Ever. A single violation makes driftwatch a Prometheus DoS.
- `publisher` is allowed as a label but must be bounded: if distinct publishers exceed `maxPublisherLabels` (default 100), collapse further ones into `publisher="__other__"` and log once.
- `check` (the `DriftCheck` name) is allowed; bounded by CRD count.
- `reason` and `category` labels must come from a **closed enum** defined in code. Never from user input or error strings.

**Metric definitions.** Implement exactly these names; `docs/METRICS.md` is generated from this registry by `hack/verify-metrics-docs.sh`, and CI fails if they drift out of sync.

```
# --- ingest ---
driftwatch_events_received_total{check,publisher,op}                  counter
driftwatch_events_dropped_total{check,publisher,reason}               counter
    reason: decode_error | unknown_op | too_large | buffer_full |
            duplicate | stale_epoch | validation_error
driftwatch_ingest_queue_depth{check,stage}                            gauge
    stage: raw | decoded
driftwatch_bytes_received_total{check}                                counter

# --- sequence integrity ---
driftwatch_seq_gaps_total{check,publisher}                            counter
driftwatch_seq_missing_events{check,publisher}                        gauge
driftwatch_publisher_restarts_total{check,publisher,kind}             counter
    kind: explicit | implicit
driftwatch_publisher_clock_skew_seconds{check,publisher}              gauge
driftwatch_publishers_tracked{check}                                  gauge
driftwatch_gapset_truncated{check,publisher}                          gauge

# --- oracle ---
driftwatch_oracle_keys{check,trust}                                   gauge
    trust: complete | suspect | adopted
driftwatch_oracle_settled_keys{check}                                 gauge
driftwatch_oracle_inflight_keys{check}                                gauge
driftwatch_oracle_never_settled_keys{check}                           gauge
driftwatch_oracle_evictions_total{check}                              counter
driftwatch_oracle_apply_duration_seconds{check}                       histogram
driftwatch_projection_errors_total{check,projection,reason}           counter

# --- target ---
driftwatch_target_reachable{check}                                    gauge (0|1)
driftwatch_target_errors_total{check,op}                              counter
driftwatch_target_read_duration_seconds{check,op}                     histogram
driftwatch_target_keyspace_size{check}                                gauge
driftwatch_target_evictions_observed_total{check}                     counter
driftwatch_target_expirations_observed_total{check}                    counter
driftwatch_target_role{check,role}                                    gauge (0|1)

# --- divergence (the headline metrics) ---
driftwatch_divergent_keys{check,category}                             gauge
driftwatch_suspect_divergent_keys{check,category}                     gauge
driftwatch_advisory_divergent_keys{check,category}                    gauge
driftwatch_drift_episodes_total{check,category}                       counter
driftwatch_drift_resolved_total{check,category}                       counter
driftwatch_drift_duration_seconds{check}                              gauge
driftwatch_transient_divergence_total{check,reason}                   counter
    reason: resolved | oracle_advanced | key_evicted | fence_failed
driftwatch_confirm_queue_depth{check}                                 gauge
driftwatch_confirm_queue_dropped_total{check}                         counter

# --- sweeps ---
driftwatch_sweeps_total{check,kind,result}                            counter
    kind: oracle_to_target | target_to_oracle
    result: success | target_unavailable | error | aborted
driftwatch_sweeps_skipped_total{check,kind}                           counter
driftwatch_sweep_duration_seconds{check,kind}                         histogram
driftwatch_sweep_keys_compared{check}                                 gauge
driftwatch_coverage_ratio{check}                                      gauge

# --- lag ---
driftwatch_convergence_seconds{check}                                 histogram
driftwatch_settlement_window_seconds{check}                           gauge
driftwatch_lag_probe_timeouts_total{check}                            counter

# --- source ---
driftwatch_source_connected{check,endpoint_index}                     gauge (0|1)
driftwatch_source_reconnects_total{check}                             counter

# --- process ---
driftwatch_build_info{version,commit,goversion}                       gauge (=1)
driftwatch_checks_active                                              gauge
driftwatch_panics_total{check,component}                              counter
```

**Histogram buckets.** Latency histograms: `[.0005 .001 .0025 .005 .01 .025 .05 .1 .25 .5 1 2.5 5 10 30]`. Sweep duration: `[.1 .25 .5 1 2.5 5 10 30 60 120 300]`. Convergence: `[.001 .0025 .005 .01 .025 .05 .1 .25 .5 1 2.5 5 10 30 60]`.

**Tests.**
- A test that enumerates the registry and asserts the exact set of metric names, so an accidental rename breaks CI.
- A **cardinality test**: feed 10,000 distinct keys and 500 distinct publishers, then assert total time series is under a fixed budget (e.g. 500). This is the test that prevents the catastrophic mistake.
- A test that `publisher="__other__"` collapsing engages at the limit.
- `hack/verify-metrics-docs.sh` run in CI, diffing generated `docs/METRICS.md` against the committed file.

---

### M13 — `pkg/explain`

**Responsibility.** Answer "what happened to this key?" This is the module that makes driftwatch a debugging tool rather than just an alarm, and it is the feature most worth demonstrating.

```go
package explain

type Explanation struct {
    Key             string
    GeneratedAt     time.Time

    OracleValue     event.Value
    OracleVersion   uint64
    OracleTrust     oracle.TrustState
    LastEventAt     time.Time

    TargetValue     event.Value
    TargetTTL       *time.Duration
    TargetReadAt    time.Time
    TargetReachable bool

    Verdict         Verdict
    Diagnosis       []Diagnosis

    History         []Step
    PublisherStates []seqtrack.PublisherState
    SettlementWindow time.Duration
    Settled         bool
}

type Verdict uint8
const (
    VerdictAgree Verdict = iota
    VerdictDiverged
    VerdictInFlight
    VerdictSuspect
    VerdictUnknownKey
    VerdictTargetUnavailable
)

type Step struct {
    Index       int
    Event       event.Event
    Verdict     seqtrack.Verdict
    ValueAfter  event.Value
    Version     uint64
    AppliedAt   time.Time
    DeltaFromPrev time.Duration
    Note        string          // e.g. "gap before this event: seq 41-43 missing"
}

// Diagnosis is a plain-language hypothesis with supporting evidence.
type Diagnosis struct {
    Code       string     // stable identifier, e.g. "SEQ_GAP_BEFORE_LAST_EVENT"
    Confidence Confidence // High | Medium | Low
    Statement  string     // human sentence
    Evidence   []string   // concrete observations backing it
}

func Explain(ctx context.Context, in Input) (*Explanation, error)

type Input struct {
    Key      string
    Oracle   *oracle.Oracle
    Target   target.Target
    SeqTrack *seqtrack.Tracker
    Shape    projection.Shape
    Window   time.Duration
    Clock    clock.Clock
}

func (e *Explanation) Text() string
func (e *Explanation) JSON() ([]byte, error)
```

**Diagnosis rules — implement each as a named, individually-tested rule.**

| Code | Fires when | Statement (template) |
|---|---|---|
| `AGREE` | oracle == target | Oracle and target agree at version {v}. |
| `IN_FLIGHT` | not settled | Last event was {d} ago, inside the {W} settlement window; disagreement here is expected. |
| `SEQ_GAP_AFFECTING_PUBLISHER` | the key's last publisher has gaps | Publisher {p} has {n} missing sequence numbers ({ranges}); driftwatch's own view may be incomplete, so this disagreement may be driftwatch's fault, not the target's. |
| `MISSING_IN_TARGET_NO_GAPS` | oracle has it, target doesn't, trust Complete | Target is missing this key. driftwatch observed a complete event sequence for publisher {p} (seq {a}..{b}, no gaps), so the materializer most likely dropped or failed to apply seq {b}. |
| `EXTRA_IN_TARGET` | target has it, oracle doesn't | Target holds this key but no event ever created it. Either it predates driftwatch (bootstrap mode {m}) or it was written out-of-band. |
| `PUBLISHER_RESTARTED` | restart within the key's history | Publisher {p} restarted at {t} ({kind}); events around that boundary may have been lost. |
| `TARGET_EVICTION_LIKELY` | missing + evictions rising | Redis reported {n} evictions since the last sweep and is at {pct}% of maxmemory; this key was probably evicted. |
| `TTL_EXPIRED` | missing + oracle TTL elapsed | The oracle's TTL for this key expired {d} ago; absence in the target is expected. |
| `TYPE_MISMATCH` | WRONGTYPE | Target holds type {actual} but the {proj} projection expects {expected}; the projection may be misconfigured for this keyspace. |
| `CLOCK_SKEW_HIGH` | skew > W | Publisher {p}'s clock differs from driftwatch's by {d}, which exceeds the settlement window; publisher timestamps in this output are unreliable (settlement itself is unaffected — it uses local receive time). |
| `MEMBER_SUBSET` | target members ⊂ oracle members | Target is missing {n} of {m} members ({sample}); consistent with dropped `add` events rather than a wholesale failure. |
| `MEMBER_SUPERSET` | oracle members ⊂ target members | Target holds {n} extra members ({sample}); consistent with dropped `remove` events. |
| `HISTORY_TRUNCATED` | ring is full | Only the last {n} events are retained; earlier history is unavailable. |
| `NO_HISTORY` | key not in oracle at all | driftwatch has never observed an event for this key. |

**Text output format.** Must be genuinely readable — this is the screenshot that goes in the README. Sketch:

```
KEY  block:9f3a2c1e
─────────────────────────────────────────────────────────────────────
VERDICT   DIVERGED (confirmed)                    settled 47s ago

ORACLE    set{replica-0, replica-2}     version 18   trust complete
TARGET    set{replica-0}                read 12ms ago
DIFF      missing in target: replica-2

DIAGNOSIS
  [high]  Target is missing member "replica-2". driftwatch observed a
          complete event sequence for publisher replica-2 (seq 8801..8847,
          no gaps), so the materializer most likely failed to apply seq 8842.
  [low]   Redis reported 0 evictions since the last sweep, so eviction
          is unlikely.

HISTORY (last 6 of 18)
  #13  8839  replica-0  add     replica-0   → {replica-0}           v15  -4m12s
  #14  8841  replica-2  add     replica-2   → {replica-0,replica-2} v16  -3m58s
  #15  8842  replica-2  remove  replica-2   → {replica-0}           v17  -2m01s
  #16  8847  replica-2  add     replica-2   → {replica-0,replica-2} v18  -47s
       ⚠ target still reflects the state as of #15

PUBLISHERS
  replica-0   epoch 3  hwm 12043  gaps 0            last seen 2s ago
  replica-2   epoch 1  hwm  8847  gaps 0            last seen 47s ago
```

**Edge cases.** Key not in oracle and not in target (`VerdictUnknownKey`); target unreachable (still show oracle state and history — a partial answer is far better than an error); history empty but key present (adopted at bootstrap); binary key (must render hex-escaped and must be accepted as a CLI argument — support `--key-hex`); a key with 100,000 members (truncate the display, show counts); multiple diagnoses firing at once (order by confidence, then by code).

**Tests.** One test per diagnosis rule, each constructing the exact state that should trigger it and asserting the `Code` appears with the right confidence. Golden-file tests for `Text()` covering: agree, in-flight, missing-with-gaps, missing-without-gaps, extra, member-subset, type-mismatch, truncated-history, unknown-key. Golden files live in `pkg/explain/testdata/`. A test asserting `Text()` never exceeds 120 columns.

---

### M14 — `pkg/check`

**Responsibility.** Assemble a complete, running check from a spec. The composition root.

```go
package check

// Check is one running audit: source → codec → seqtrack → projection
// → oracle → sweeper → reporter.
type Check struct{ /* ... */ }

func New(spec Spec, deps Deps) (*Check, error)

// Run blocks until ctx is done or a fatal error occurs.
// All goroutines are joined before Run returns.
func (c *Check) Run(ctx context.Context) error

// Status returns a snapshot for CRD status and the CLI.
func (c *Check) Status() Status

// Explain proxies to the explain engine.
func (c *Check) Explain(ctx context.Context, key string) (*explain.Explanation, error)

// SweepNow triggers an out-of-band sweep.
func (c *Check) SweepNow(ctx context.Context) (*differ.Report, error)

// Close shuts down and releases resources. Idempotent.
func (c *Check) Close() error

type Deps struct {
    Clock   clock.Clock
    Logger  logr.Logger
    Metrics *metrics.Metrics
}
```

**Required behaviours.**
- Construct in dependency order and **validate the whole spec before starting anything**. A misconfigured check must fail at construction with a precise error naming the offending field, not fail halfway up and leave sockets open.
- Wire the source's gap-signal channel to `oracle.MarkSuspect`.
- Wire the seqtrack gap output to both metrics and `MarkSuspect` (scoped by `KeyOwnership`).
- Run every goroutine under one `errgroup.WithContext`; the first fatal error cancels all.
- `Close()` must be safe to call after a failed `New` (partial construction cleanup) and safe to call twice.
- Implement **bootstrap** per §5.6 before starting the sweeper: in `Adopt` mode, perform the full target scan into the oracle and only then start sweeping. Report `Phase: Bootstrapping` throughout.

**Edge cases.** Spec with an unknown source/codec/projection/target name (clear error listing valid names); source that fails to connect at startup (must still start — the check is `Phase: Degraded`, not a construction failure, because the endpoint may come up later); bootstrap scan that fails (retry with backoff, stay in `Bootstrapping`); bootstrap scan that finds more keys than `MaxTrackedKeys` (adopt as many as fit, set `OracleSaturated`, log the shortfall explicitly); `Close` during bootstrap.

**Tests.** Construction table: every invalid spec variant → expected error. A full in-process integration test using memory source + memory target + fake clock that drives a complete scenario (events → oracle → sweep → clean report → inject drift → confirmed finding → repair → resolved) with zero real time elapsed. This single test is the best proof the whole system composes correctly; make it the flagship test and name it `TestCheck_EndToEnd_InProcess`.

---

## 10. The `DriftCheck` custom resource

### 10.1 Full spec

```yaml
apiVersion: driftwatch.io/v1alpha1
kind: DriftCheck
metadata:
  name: kvcache-index
  namespace: inference
spec:
  # ---------- SOURCE ----------
  source:
    type: zmq                          # zmq | nats | file | memory
    zmq:
      endpoints:                       # required, min 1
        - tcp://vllm-0.vllm.inference.svc:5557
        - tcp://vllm-1.vllm.inference.svc:5557
      topics: ["kv_events"]            # empty = subscribe all
      recvHWM: 100000
      connectTimeout: 5s
      reconnectIntervalMax: 30s
      multipart: auto                  # auto | topicThenPayload | singleFrame
    nats:
      url: nats://nats.default.svc:4222
      subjects: ["kv.events.>"]
      credentialsSecretRef:
        name: nats-creds
        key: creds
    file:
      path: /data/events.jsonl
      speed: realtime                  # realtime | fast | "2.0"
      loop: false
    ingestBufferSize: 200000           # must exceed recvHWM

  # ---------- CODEC ----------
  codec:
    type: json                         # json | msgpack | template
    maxPayloadBytes: 1048576
    retainRaw: false
    fieldMapping:
      publisher: replica_id
      epoch: incarnation
      seq: event_id
      timestamp: ts
      op: event_type
      key: block_hash
      member: replica_id
      value: payload
      ttl: ttl_seconds
    opMapping:
      BLOCK_STORED: add
      BLOCK_EVICTED: remove
      ALL_BLOCKS_CLEARED: delete
      SNAPSHOT_START: snapshotBegin
      SNAPSHOT_END: snapshotEnd
      PING: heartbeat

  # ---------- PROJECTION ----------
  projection:
    type: keysetOwnership              # keysetOwnership | scalar | counter
    keyTemplate: "block:{{.Key}}"
    memberTemplate: "{{.Member}}"
    maxMembersPerKey: 100000
    incrOnly: false                    # counter projection only
    ownership:
      partitioned: false
      keyPattern: ""                   # e.g. "replica:{{.Publisher}}:*"

  # ---------- TARGET ----------
  target:
    type: redis
    redis:
      mode: standalone                 # standalone | sentinel | cluster
      addr: redis.inference.svc:6379
      addrs: []                        # cluster/sentinel
      masterName: ""                   # sentinel
      db: 0
      username: ""
      passwordSecretRef:
        name: redis-creds
        key: password
      tls:
        enabled: false
        insecureSkipVerify: false
        caSecretRef: {name: "", key: ""}
      keyPattern: "block:*"            # for the extras scan
      readBatchSize: 500
      scanCount: 1000
      dialTimeout: 5s
      readTimeout: 3s
      poolSize: 10

  # ---------- POLICY ----------
  policy:
    settlementWindow:
      mode: adaptive                   # static | adaptive
      static: 5s
      min: 1s
      max: 120s
      safetyFactor: "3.0"
    sweepInterval: 30s
    extraScanInterval: 5m
    bootstrap: Adopt                   # Adopt | Strict | Wait
    expiryPolicy: Strict               # Ignore | Model | Strict
    assumedTTL: 0s
    ttlTolerance: 5s
    requirePrimary: false
    maxTrackedKeys: 1000000
    ringSize: 16
    maxConfirmQueue: 10000
    maxFindings: 10000
    maxExtrasTracked: 100000
    maxPublishers: 1024
    oracleShards: 64
    neverSettledThreshold: 10          # multiples of W
    paused: false                      # stop sweeping, keep ingesting

  # ---------- ALERTING ----------
  alert:
    divergentKeysThreshold: 10
    divergentRatioThreshold: "0.001"   # fraction of tracked keys
    forDuration: 60s
    includeSuspect: false

status:
  phase: Watching                      # Pending | Bootstrapping | Watching
                                       # | Degraded | Paused | Failed
  observedGeneration: 4
  message: "steady state"

  divergentKeys: 0
  suspectDivergentKeys: 0
  divergenceByCategory:
    missingInTarget: 0
    extraInTarget: 0
    valueMismatch: 0
    memberMismatch: 0
  driftDurationSeconds: 0

  trackedKeys: 120433
  settledKeys: 120401
  inFlightKeys: 32
  coverageRatio: "0.982"

  settlementWindowSeconds: "0.36"
  convergenceP99Seconds: "0.118"

  publishers:
    - id: replica-0
      epoch: 3
      highWaterMark: 984221
      missingEvents: 0
      restarts: 2
      lastSeenSeconds: 1
      clockSkewSeconds: "0.004"
    - id: replica-1
      epoch: 1
      highWaterMark: 771903
      missingEvents: 14
      restarts: 0
      lastSeenSeconds: 2
      clockSkewSeconds: "-0.012"

  lastSweepTime: "2026-07-30T11:02:31Z"
  lastSweepDurationSeconds: "1.83"
  lastSweepKeysCompared: 120401
  sweepsSkipped: 0

  targetReachable: true
  targetRole: master
  targetKeyspaceSize: 120455

  conditions:
    - type: Ready
      status: "True"
      reason: Watching
      lastTransitionTime: "2026-07-30T10:14:02Z"
    - type: SourceConnected
      status: "True"
      reason: AllEndpointsConnected
    - type: TargetAvailable
      status: "True"
      reason: Reachable
    - type: DriftDetected
      status: "False"
      reason: NoDivergence
    - type: OracleSaturated
      status: "False"
      reason: WithinLimit
    - type: SequenceIntegrity
      status: "False"
      reason: GapsObserved
      message: "publisher replica-1: 14 missing events"
```

### 10.2 Validation rules (webhook + CRD schema)

Enforce all of these. Each gets a unit test in `api/v1alpha1/driftcheck_webhook_test.go`.

| Rule | Error |
|---|---|
| `source.type` must be a registered name | `unknown source type %q, valid: [...]` |
| zmq requires ≥1 endpoint, each parseable as a ZMQ URI | `source.zmq.endpoints[0]: invalid endpoint` |
| `ingestBufferSize` must be ≥ `recvHWM` | `policy: ingestBufferSize (%d) must be >= source.zmq.recvHWM (%d), otherwise event loss occurs invisibly in the socket` |
| nats `queueGroup` must be empty | `source.nats.queueGroup must be empty: a queue group would distribute events across replicas and corrupt the oracle` |
| `codec.type` registered; `fieldMapping` keys must be known field names | `codec.fieldMapping: unknown field %q` |
| `opMapping` values must be valid ops | `codec.opMapping[%q]: unknown op %q` |
| `projection.type` registered | as above |
| counter projection with `incrOnly: false` and events other than incr → warn, not error | condition `ProjectionNotCommutative` |
| `target.redis.mode: cluster` requires `addrs`, forbids `addr` | field-specific |
| `keyPattern` must be a valid Redis glob | `target.redis.keyPattern: invalid glob` |
| `settlementWindow.min` ≤ `static` ≤ `max` | ordering error |
| `safetyFactor` must be ≥ 1.0 | `policy.settlementWindow.safetyFactor must be >= 1.0` |
| `sweepInterval` must be > `settlementWindow.max` × 0 and ≥ 1s | bound error |
| `sweepInterval` should be ≥ 2× `settlementWindow` → warn | condition `SweepIntervalTight` |
| `maxTrackedKeys` between 1,000 and 100,000,000 | bound error |
| `oracleShards` a power of two between 1 and 1024 | bound error |
| `ringSize` between 1 and 1024 | bound error |
| `bootstrap: Strict` requires the codec to map `snapshotBegin`/`snapshotEnd` | `policy.bootstrap=Strict requires codec.opMapping to define snapshotBegin and snapshotEnd` |
| `expiryPolicy: Ignore` requires `assumedTTL > 0` | field error |
| secret refs must exist in the same namespace | `target.redis.passwordSecretRef: secret %q not found` (validated by controller, not webhook — a webhook must not depend on other objects existing) |
| immutable after creation: `projection.type`, `target.type` | `field is immutable; delete and recreate the DriftCheck` |

Defaulting webhook fills every optional field so `status` reflects effective config and users can `kubectl get driftcheck -o yaml` to see what is actually running.

### 10.3 Controller behaviour (`internal/controller`)

```go
type DriftCheckReconciler struct {
    client.Client
    Scheme  *runtime.Scheme
    Clock   clock.Clock
    Metrics *metrics.Metrics
    Runners *RunnerRegistry     // name/namespace → *check.Check
}
```

**Reconcile logic.**

1. Fetch the `DriftCheck`. If not found → stop and remove any runner for that key.
2. If `DeletionTimestamp` set → stop the runner, wait for clean shutdown, remove the finalizer, return.
3. Ensure the finalizer `driftwatch.io/cleanup` is present.
4. Resolve secret references. On failure: set `Ready=False, reason=SecretMissing`, requeue after 30s.
5. Compute a **spec hash**. If a runner exists with the same hash, only refresh status and return.
6. If a runner exists with a different hash: stop it cleanly, then start a new one. Log the transition. **A spec change must never leave two runners for the same check** — hold a per-key mutex in the registry.
7. Start the runner in a goroutine tied to the manager's context. Set `Phase: Bootstrapping`.
8. Update `status` from `check.Status()`, including all conditions.
9. Requeue after `statusRefreshInterval` (default 15s) so status stays fresh without watch churn.

**Requirements.**
- **Leader election enabled** (`--leader-elect`), so exactly one manager runs the checks. On leadership loss, all runners stop.
- **Status updates must use `Status().Patch` with optimistic concurrency**, retrying on conflict. Never `Update` the whole object from a stale copy.
- Emit Kubernetes **Events** for: bootstrap complete, drift detected (with count), drift resolved, source disconnected/reconnected, target unavailable, oracle saturated, publisher restart detected. Events are what an operator sees first in `kubectl describe`.
- **RBAC: least privilege.** `driftchecks` get/list/watch/update/patch, `driftchecks/status` update/patch, `driftchecks/finalizers` update, `secrets` get (namespaced, and document that a `Role` per namespace is preferable to a `ClusterRole` for secrets), `events` create/patch, plus leader-election `leases`. **No pod/node/deployment access whatsoever.** Generate with kubebuilder markers and commit the generated `config/rbac/role.yaml`.
- Multiple `DriftCheck`s run concurrently in one manager, each isolated: a panic or fatal error in one must not affect others (per-runner recover, per-runner context).

**Edge cases.** Two `DriftCheck`s with identical source and target (legal — different projections auditing the same data; must not interfere, and metrics must be separable by the `check` label); a check deleted while its bootstrap scan is running (context cancellation must abort the scan promptly); a spec updated 10 times in 10 seconds (debounce: coalesce by comparing hash, and rate-limit restarts to once per 5s per check); manager shutdown with 50 running checks (all must stop within `shutdownGrace`; parallelize the shutdown); a `DriftCheck` in a namespace the manager can't read secrets from (clear condition, no crash-loop).

**Tests.**
- **envtest** suite: create/update/delete lifecycle, finalizer behaviour, status patching under conflict, spec-change restart, secret resolution failure and recovery, event emission (assert on the fake recorder), and `goleak` after teardown.
- A test proving no two runners exist for one key under a rapid update storm (20 sequential updates, assert registry size 1 and exactly the final hash).
- A test that deleting a check mid-bootstrap terminates within 2s.

---

## 11. CLI specification

Binary: `driftwatch`. Framework: cobra. Every command must work **without Kubernetes** — the CLI reads a YAML file with the same schema as `DriftCheck.spec`. This matters: it makes the tool usable and testable standalone, and it makes the demo easy.

### `driftwatch watch -f check.yaml`
Runs a check in the foreground. Serves `/metrics` on `--metrics-addr` (default `:9090`). Prints a live status line every `--status-interval` (default 10s) and a full report on each sweep at `-v 1`.
Flags: `--metrics-addr`, `--status-interval`, `--log-level`, `--log-format` (`console|json`), `--once` (single sweep then exit), `--timeout`.
Exit codes: `0` clean shutdown; `1` fatal error; `2` config invalid; `3` (with `--fail-on-drift`) drift confirmed.

### `driftwatch diff -f check.yaml`
Bootstraps, waits `--warmup` (default 2× W) to fill the oracle, performs one sweep plus one extras scan, prints the report, exits. `--output text|json`. Exit code 3 if drift found. **This is the command for CI pipelines**: assert your cache index is consistent as a test.

### `driftwatch explain -f check.yaml --key block:9f3a`
Runs a check in the background, waits for the key to be observed (or `--wait` timeout), prints the explanation. `--key-hex` for binary keys. `--output text|json`. Also supports `--from-running http://localhost:9090` to query a live `watch` process via a small internal HTTP endpoint (`/explain?key=...`) — needed for the Kubernetes case where you can't easily start a second subscriber.

### `driftwatch replay -f check.yaml --events events.jsonl`
Deterministic offline replay: reads events from a file, applies them, then diffs against a target (which may be `memory` seeded from `--target-snapshot snapshot.json`). Fully hermetic — no network. This is how a captured incident gets reproduced, and it is the backbone of regression testing. `--speed`, `--stop-at-seq`, `--dump-oracle out.json`.

### `driftwatch inject -f check.yaml --scenario drop-burst`
Test-only helper (built with a `dev` tag, excluded from release binaries): publishes a synthetic event stream through the fault injector so a human can watch drift appear on the dashboard. Scenarios named identically to the fault matrix rows in §15.3. `--list-scenarios`, `--duration`, `--rate`.

### `driftwatch version`
Prints version, commit, build date, Go version. `--output json`.

**CLI requirements.** All commands honour `SIGINT`/`SIGTERM` with graceful shutdown. Errors go to stderr, data to stdout, so output is pipeable. `--output json` must emit a single well-formed JSON document (no interleaved logs). No colour when not a TTY. `--help` for every command includes a runnable example.

**Tests.** Golden-file tests for each command's output using the memory source/target and fake clock. A test per exit code. A test that `--output json` produces parseable JSON with logs at `--log-level=debug` (proving stream separation).

---

## 12. Observability deliverables

### 12.1 Grafana dashboard

`deploy/grafana/driftwatch-dashboard.json`. Must be importable with a `DS_PROMETHEUS` datasource variable, and templated by a `check` variable. Rows and panels:

**Row 1 — Verdict (the "is it broken" row)**
- Stat: `sum(driftwatch_divergent_keys{check=~"$check"})` — big number, green at 0, red above threshold.
- Stat: `driftwatch_drift_duration_seconds` — how long the current episode has lasted.
- Stat: `driftwatch_coverage_ratio` — what fraction of the keyspace is actually being asserted on. **This panel is what stops the dashboard from lying**: 0 divergence at 3% coverage is meaningless.
- Timeseries: `driftwatch_divergent_keys` by `category`.

**Row 2 — Sequence integrity**
- Timeseries: `rate(driftwatch_seq_gaps_total[5m])` by publisher.
- Timeseries: `driftwatch_seq_missing_events` by publisher.
- Table: publisher, epoch, HWM, missing, restarts, skew — from the gauge series.

**Row 3 — Lag and settlement**
- Heatmap: `driftwatch_convergence_seconds_bucket`.
- Timeseries: p50/p90/p99 convergence overlaid with `driftwatch_settlement_window_seconds`. **The visual point: W must sit above p99.** If they cross, false positives are coming.
- Timeseries: `driftwatch_transient_divergence_total` rate by reason.

**Row 4 — Oracle and target**
- Timeseries: `driftwatch_oracle_keys` stacked by trust.
- Timeseries: `driftwatch_target_keyspace_size` vs `driftwatch_oracle_keys` total.
- Timeseries: `driftwatch_oracle_inflight_keys`, `driftwatch_oracle_never_settled_keys`.
- Timeseries: `rate(driftwatch_target_evictions_observed_total[5m])`.

**Row 5 — Throughput and health**
- Timeseries: `rate(driftwatch_events_received_total[1m])` by op.
- Timeseries: `rate(driftwatch_events_dropped_total[1m])` by reason.
- Timeseries: `driftwatch_ingest_queue_depth` by stage.
- Timeseries: sweep duration p99 vs `sweepInterval`; `driftwatch_sweeps_skipped_total`.

Include the JSON in the repo and a **screenshot** in `docs/evidence/` taken during a fault-injection run showing drift appearing and then resolving. That screenshot is one of the highest-value artifacts in the whole project.

### 12.2 PrometheusRule

`config/prometheus/rules.yaml`. Alerts:

| Alert | Expression (sketch) | For | Severity |
|---|---|---|---|
| `DriftwatchDriftDetected` | `driftwatch_divergent_keys > 0` | 5m | warning |
| `DriftwatchDriftSevere` | `driftwatch_divergent_keys / (driftwatch_oracle_keys{trust="complete"} > 0) > 0.01` | 5m | critical |
| `DriftwatchEventLoss` | `rate(driftwatch_seq_gaps_total[10m]) > 0` | 10m | warning |
| `DriftwatchTargetUnavailable` | `driftwatch_target_reachable == 0` | 2m | critical |
| `DriftwatchSourceDisconnected` | `min(driftwatch_source_connected) == 0` | 2m | warning |
| `DriftwatchLowCoverage` | `driftwatch_coverage_ratio < 0.8` | 15m | info |
| `DriftwatchOracleSaturated` | `rate(driftwatch_oracle_evictions_total[5m]) > 0` | 5m | warning |
| `DriftwatchSettlementWindowAtMax` | `driftwatch_settlement_window_seconds >= 120` | 10m | warning |
| `DriftwatchSweepsSkipped` | `rate(driftwatch_sweeps_skipped_total[10m]) > 0` | 10m | warning |
| `DriftwatchIngestBackpressure` | `rate(driftwatch_events_dropped_total{reason="buffer_full"}[5m]) > 0` | 5m | critical |

Each alert must have `summary`, `description`, and a `runbook_url` pointing at an anchor in `docs/OPERATIONS.md`. Write that runbook — one section per alert saying what it means, what to check, and what the likely causes are. A runbook is cheap to write and is exactly the kind of thing that signals operational maturity.

### 12.3 Logging

- `logr` over `zap`. `--log-format console|json`.
- Levels: error, warn, info, debug (`-v 1`), trace (`-v 2`).
- **Every log line carries `check` (name/namespace) as a field.** Multi-check processes are unreadable otherwise.
- **Rate-limit repetitive logs**: decode errors, projection errors, and target errors must use a token-bucket sampler (first 10, then 1 per 10s per unique reason). An unbounded error log is its own outage.
- **Never log**: full event payloads, Redis passwords, target values. Log key names only at `-v 2`, and provide `--redact-keys` to hash them for environments where key names are sensitive. Implement a `logging.Redact(string) string` helper and use it consistently.
- On startup, log the effective configuration once with secrets replaced by `[REDACTED]`. This single line saves enormous debugging time.

---

## 13. Fault injection framework

Lives in `test/harness/faultinjector`. It is a `Source` **middleware** — it wraps any `Source` and perturbs the stream. This design means every fault scenario can be tested in-process with a fake clock, with no cluster and no flakiness.

```go
package faultinjector

// Injector wraps a Source and applies faults to the stream.
type Injector struct{ /* ... */ }

func Wrap(inner source.Source, clk clock.Clock, faults ...Fault) *Injector

// Fault transforms a stream of messages. Deterministic given a seed.
type Fault interface {
    Name() string
    // Apply may drop (return false), modify, delay, duplicate, or reorder.
    Apply(msg source.RawMessage, emit func(source.RawMessage)) (keep bool)
    // Reset clears internal state between scenarios.
    Reset()
}
```

**Fault implementations.**

| Fault | Constructor | Behaviour |
|---|---|---|
| Drop | `Drop(rate float64, seed int64)` | drops each message with probability `rate` |
| DropBurst | `DropBurst(after, count int)` | drops `count` consecutive messages after the first `after` |
| DropRange | `DropSeqRange(from, to uint64)` | drops exactly the messages with seq in `[from,to]` — **the deterministic drop used by most tests** |
| Reorder | `Reorder(window int, seed int64)` | buffers `window` messages and emits in shuffled order |
| ReorderSwap | `ReorderSwap(a, b uint64)` | swaps exactly two seqs — deterministic |
| Duplicate | `Duplicate(rate float64, delay time.Duration, seed int64)` | re-emits a copy after `delay` |
| Delay | `Delay(min, max time.Duration, seed int64)` | holds each message a jittered interval |
| DelayPublisher | `DelayPublisher(pub string, d time.Duration)` | delays only one publisher's stream |
| Partition | `Partition(start, duration time.Duration)` | drops everything for a window, then resumes |
| Corrupt | `Corrupt(rate float64, seed int64)` | flips random bytes in the payload |
| Truncate | `Truncate(rate float64, seed int64)` | cuts the payload short |
| Oversize | `Oversize(atMsg int, bytes int)` | emits one absurdly large message |
| ClockSkew | `ClockSkew(pub string, offset time.Duration)` | rewrites one publisher's timestamps |
| SeqReset | `SeqReset(atMsg int)` | rewrites subsequent seqs to restart from 1 (implicit restart) |
| EpochBump | `EpochBump(atMsg int)` | bumps epoch and resets seq (explicit restart) |
| Interleave | `Interleave(pubs int)` | multiplexes N synthetic publishers |

**Determinism requirement.** Every fault takes an explicit seed and, given the same seed and the same input stream, must produce the identical output stream. **A test that fails once in fifty runs is worse than no test.** Add a meta-test `TestFaults_Deterministic` that runs each fault twice with the same seed over 10,000 messages and asserts byte-identical output.

**Composability.** Faults chain: `Wrap(src, clk, DropSeqRange(100,110), Reorder(8, 42), Delay(1*ms, 50*ms, 7))`. Order of application is the order given; document that clearly, because `Drop` then `Reorder` differs from `Reorder` then `Drop`.

**Target-side faults** live on the `memory` target and the e2e Redis:
- `memory.SimulateFlush()`, `SimulateEvict(n)`, `SimulateLatency(d)`, `SimulateErrorRate(r)`, `SimulateWrongType(key)`, `SimulateOutOfBandWrite(key, val)`.
- In e2e, real Redis: `FLUSHDB`, `CONFIG SET maxmemory` to force eviction, `DEBUG SLEEP`, kill the container, sentinel failover, and **toxiproxy** for latency/partition between driftwatch and Redis.

**The scenario DSL** (`test/harness/scenario`) makes fault tests declarative and readable:

```go
scenario.New(t).
    WithProjection("keysetOwnership").
    WithPublishers(3).
    WithKeys(1000).
    WithFaults(faultinjector.DropSeqRange(500, 500)).
    WithSettlementWindow(1 * time.Second).
    Run(func(s *scenario.Session) {
        s.PublishEvents(1000)              // synthetic, deterministic
        s.RunMaterializer()                // reference consumer writes to target
        s.AdvanceClock(5 * time.Second)    // fake clock; no real waiting
        r := s.Sweep()
        s.RequireDivergentKeys(r, 1)
        s.RequireCategory(r, differ.CatMemberMismatch, 1)
        e := s.Explain(s.KeyForSeq(500))
        s.RequireDiagnosis(e, "MISSING_IN_TARGET_NO_GAPS")
    })
```

Note the crucial detail: the **materializer is part of the harness**, not part of driftwatch. `test/harness/materializer` is a simple, obviously-correct reference consumer that reads the *unperturbed* stream and writes to the target. The fault injector sits only in front of *driftwatch's* subscription in some tests and in front of the *materializer's* subscription in others. This distinction is what lets you test both directions:

- Faults on the **materializer's** stream → the target is genuinely wrong → driftwatch must report **confirmed divergence**.
- Faults on **driftwatch's** stream → driftwatch's oracle is wrong → driftwatch must report **suspect** divergence, never confirmed. This is the test for §5.2's honesty requirement, and it is the test most projects would forget. Write it early and name it `TestFaults_DriftwatchOwnLoss_ReportsSuspectNotConfirmed`.

---

## 14. The Kind-based end-to-end suite

### 14.1 Goals

Prove the *real* path: real ZMQ over TCP between pods, real Redis, real operator reconciling a real CRD, real Prometheus scrape. Everything else is tested faster elsewhere; e2e exists to catch integration and packaging mistakes that unit tests structurally cannot.

Keep it **small and reliable**: 6–8 scenarios. A 40-scenario e2e suite that takes 25 minutes and flakes weekly is worse than 6 that take 5 minutes and never flake.

### 14.2 Cluster and fixtures

`test/e2e/manifests/kind-config.yaml`: single-node cluster, `containerd` config to allow the locally-built image, port mappings for Prometheus if needed.

Lifecycle in `test/e2e/kind.go`:
1. `kind create cluster --name driftwatch-e2e --config ...` (skipped if `DRIFTWATCH_E2E_REUSE_CLUSTER=1`, which makes local iteration bearable).
2. `docker build` the driftwatch image, `kind load docker-image`.
3. Apply CRDs, RBAC, and the manager deployment.
4. Deploy fixtures: Redis (single pod, no persistence, `maxmemory` configurable), the synthetic publisher (a small Go binary in the same image, `driftwatch inject`-driven), and the reference materializer.
5. Wait for readiness with explicit conditions and generous-but-bounded timeouts.

**Ginkgo structure.** `SynchronizedBeforeSuite` for cluster setup; one `Describe` per scenario; `AfterEach` collects diagnostics on failure.

### 14.3 Diagnostics on failure (this is an explicit deliverable)

`test/e2e/diagnostics.go` — on any failure, dump to `test/e2e/_artifacts/<test-name>/`:

- `kubectl get driftcheck -o yaml` (full status, all conditions)
- `kubectl describe driftcheck` (events)
- Manager pod logs, current and previous
- Publisher and materializer pod logs
- Redis: `INFO all`, `DBSIZE`, `SCAN` of the first 200 keys with values
- `/metrics` scraped from the manager
- `driftwatch explain` for the first 5 divergent keys
- `kubectl get events --sort-by=.lastTimestamp -A`
- `kubectl get pods -o wide`, node describe
- The full `DriftCheck` spec that was applied

Upload as a CI artifact. **A failing e2e test that dumps nothing costs an hour per occurrence.** Write the diagnostics collector before writing the second e2e test — you will need it immediately.

### 14.4 Scenarios

| # | Name | What it proves |
|---|---|---|
| E1 | `HappyPath` | Full real path with no faults reaches and holds `divergentKeys == 0` for 60s at 2,000 events/sec. Status, conditions, and metrics all correct. |
| E2 | `DroppedEventDetected` | Publisher deliberately skips a seq range destined for the materializer; driftwatch reports confirmed `memberMismatch`, a Kubernetes Event fires, the metric rises, and `explain` names the exact missing seq. |
| E3 | `SelfLossReportsSuspect` | driftwatch's own subscription is partitioned (toxiproxy) so it misses events; asserts `suspectDivergentKeys > 0` and `divergentKeys == 0`. The honesty test. |
| E4 | `TargetFlushAndRecover` | `FLUSHDB` mid-run → mass `missingInTarget` confirmed, `evictionSuspected` reasoning in the report; then the materializer re-syncs and drift resolves to 0 with `drift_resolved_total` incrementing. |
| E5 | `RedisEviction` | `maxmemory` set low → real evictions → driftwatch correlates and the `explain` output produces `TARGET_EVICTION_LIKELY`. |
| E6 | `OperatorLifecycle` | Create, update (`sweepInterval` and `settlementWindow` changed), pause, resume, delete — all mid-traffic. Exactly one runner throughout; no goroutine leak in the manager (assert via `/debug/pprof/goroutine?debug=1` count before and after). |
| E7 | `PublisherRestart` | Publisher pod is deleted and rescheduled; seq resets; driftwatch detects the restart, does not report a spurious 900,000-event gap, and converges. |
| E8 | `MultiCheck` | Two `DriftCheck`s on the same Redis with different projections and key patterns; metrics separate cleanly by `check` label; a failure injected into one does not affect the other. |

### 14.5 Reliability requirements

- **No `time.Sleep` for synchronization.** Poll with `Eventually` and explicit conditions. Timeouts are per-assertion and generous (30–90s) but bounded.
- **Deterministic cleanup**: `AfterEach` deletes the `DriftCheck` and waits for the finalizer to clear; `AfterSuite` deletes the cluster unless `DRIFTWATCH_E2E_KEEP=1`.
- **Namespace per scenario**, generated name, so scenarios cannot interfere.
- **Retry only cluster-setup steps**, never assertions. Retrying an assertion hides a real bug.
- Total wall time target: **under 8 minutes** on a 2-core GitHub runner.
- `make e2e` must work from a clean clone with only Docker and Go installed. Test this by actually doing it in a fresh container before declaring the phase done.

---

## 15. The fault scenario matrix

This is the specification of correctness under failure. **Every row is one test in `test/faults/`.** The "Expected" column is the assertion; if the implementation does something else, the implementation is wrong.

Column key:
- **Fault on** — whose event stream is perturbed: `M` = the materializer's (so the target becomes genuinely wrong), `D` = driftwatch's (so the oracle becomes wrong), `T` = the target store directly.
- **Expected** — required observable behaviour.

### 15.1 Event-stream faults

| # | Scenario | Fault on | Expected |
|---|---|---|---|
| 1 | Single event dropped | M | 1 confirmed finding within 2×W. Category matches the op (`memberMismatch` for add/remove, `missingInTarget` for a create). `explain` → `MISSING_IN_TARGET_NO_GAPS` naming the seq. |
| 2 | Single event dropped | D | `suspectDivergentKeys` = 1, `divergentKeys` = 0. `seq_gaps_total` = 1. `explain` → `SEQ_GAP_AFFECTING_PUBLISHER`. **Never a confirmed finding.** |
| 3 | Burst of 100 consecutive events dropped | M | ≤100 confirmed findings (fewer if events touched the same keys). No crash. Report not truncated at this size. |
| 4 | Burst of 100 dropped | D | All affected keys `Suspect`. `seq_missing_events` = 100. Confirmed count stays 0. |
| 5 | Sustained 5% drop for 60s | M | Confirmed count rises monotonically; `drift_duration_seconds` grows; no memory growth beyond bounds; sweep duration stays under `sweepInterval`. |
| 6 | Adjacent pair reordered (add,remove → remove,add) | M | Final target state is wrong (member present when it should be absent) → confirmed `memberMismatch`. This is the case that proves ordering matters. |
| 7 | Adjacent pair reordered | D | Oracle transiently wrong, then correct once both applied. **Zero findings**, because reordering loses no information — only ordering. Assert this explicitly: it's a common false-positive source. |
| 8 | Window-8 shuffle over 10,000 events | D | Zero confirmed findings after settlement. Oracle final state equals the reference implementation's. |
| 9 | Duplicate delivery, immediate | D | `events_dropped_total{reason="duplicate"}` increments. Oracle state unchanged (idempotent). Zero findings. |
| 10 | Duplicate delivery, delayed 30s (after settlement) | D | Same as #9. The late duplicate must not re-bump the version or reset `LastEventAt`, or the key would falsely become in-flight. **Assert `LastEventAt` unchanged.** |
| 11 | Uniform delay of 2s, W=1s (static) | D | Steady false-positive stream initially → then, in adaptive mode, W grows above p99 and findings drop to 0. In static mode, findings persist. **Test both, and assert adaptive recovers.** |
| 12 | One publisher delayed 10s, others normal | D | Only that publisher's keys are affected; `clock_skew` unaffected (this is receive delay, not skew). Adaptive W accommodates. |
| 13 | Partition of driftwatch's source for 30s | D | `source_connected` → 0, then 1. On reconnect, **all keys marked Suspect** (no replay guarantee). `source_reconnects_total` = 1. Confirmed count stays 0 throughout. |
| 14 | Partition of the materializer's source for 30s | M | Confirmed findings for every event in the window; resolves if the materializer re-syncs, persists if not. |
| 15 | Corrupt payload, 1% | D | `events_dropped_total{reason="decode_error"}` rises. No panic. `CodecMismatch` condition **does not** fire at 1% (below the 10% threshold). |
| 16 | Corrupt payload, 50% | D | `CodecMismatch=True`. Still no panic. Process stays up. |
| 17 | Truncated payload | D | Same as #15 with `decode_error`. |
| 18 | Oversized payload (2 MiB, max 1 MiB) | D | `events_dropped_total{reason="too_large"}` = 1. No allocation of 2 MiB (assert via memstats delta — the payload must be rejected before full buffering where the transport allows). |
| 19 | Unknown op code | D | `events_dropped_total{reason="unknown_op"}` = 1, distinct from `decode_error`. |
| 20 | Explicit publisher restart (epoch bump, seq → 0) | D | `publisher_restarts_total{kind="explicit"}` = 1. **No gap recorded.** HWM resets. Keys not marked Suspect (a declared restart with a snapshot is clean; without a snapshot, mark Suspect and assert that). |
| 21 | Implicit restart (seq → 1, no epoch change) | D | `publisher_restarts_total{kind="implicit"}` = 1. Detected via the heuristic. `seq_gaps_total` **does not** jump by 900,000. This is the test that catches the naive implementation. |
| 22 | Stale event from a previous epoch arrives late | D | `events_dropped_total{reason="stale_epoch"}` = 1. Oracle unchanged. |
| 23 | Publisher clock 5 minutes ahead | D | `clock_skew_seconds` ≈ +300. **Settlement behaviour identical to no-skew** (proves local-time settlement). `explain` shows `CLOCK_SKEW_HIGH`. |
| 24 | Publisher clock 5 minutes behind | D | Mirror of #23, skew ≈ −300. |
| 25 | Two publishers writing the same key concurrently | M/D | Last-seq-wins per publisher is not globally meaningful; assert driftwatch does **not** report drift when the target reflects either valid interleaving. Document this as an inherent limitation: for `keysetOwnership`, adds/removes from different publishers commute per-member, so this is well-defined; for `scalar`, it is not, and the projection must declare it. Add a `MultiWriterUnsafe` condition when a scalar projection sees the same key from ≥2 publishers. |
| 26 | Heartbeat-only stream (no key events) | D | Seq advances, no keys created, no findings, `oracle_keys` = 0. Must not divide by zero in `coverage_ratio`. |
| 27 | 1,000 publishers (exceeds `maxPublishers`=1024? no — test at 1,500) | D | Oldest publishers evicted, counter increments, no unbounded memory. Metrics collapse to `publisher="__other__"` beyond 100. |

### 15.2 Target-store faults

| # | Scenario | Fault on | Expected |
|---|---|---|---|
| 28 | `FLUSHDB` mid-run | T | Mass confirmed `missingInTarget`. `Report.EvictionSuspected` false (flush isn't eviction) but `target_keyspace_size` drop is visible. `findings` capped at `maxFindings` with `Truncated: true`. No OOM. |
| 29 | Redis eviction under `maxmemory` | T | Confirmed `missingInTarget` **plus** `evictions_observed_total` rising **plus** `explain` → `TARGET_EVICTION_LIKELY`. |
| 30 | TTL expiry, `expiryPolicy: Strict` | T | Confirmed `missingInTarget`. |
| 31 | TTL expiry, `expiryPolicy: Ignore` + `assumedTTL` | T | **No finding.** |
| 32 | TTL expiry, `expiryPolicy: Model` with TTL in events | T | No finding; oracle expired it too. Assert oracle key count drops. |
| 33 | Out-of-band write (third party sets a key) | T | Confirmed `valueMismatch` or `extraInTarget`. `explain` → `EXTRA_IN_TARGET`. |
| 34 | Out-of-band delete | T | Confirmed `missingInTarget` with no seq gap → `MISSING_IN_TARGET_NO_GAPS`. |
| 35 | `WRONGTYPE` (key holds a list where a set is expected) | T | `CatTypeMismatch`, `explain` → `TYPE_MISMATCH`. No error thrown to the operator; it's drift, not a bug. |
| 36 | Redis unreachable for 60s | T | `target_reachable` = 0, `TargetAvailable=False`, `sweeps_total{result="target_unavailable"}` rising, **`divergentKeys` unchanged (not zeroed, not inflated)**. On recovery, normal sweeps resume. |
| 37 | Redis high latency (toxiproxy 2s) | T | Sweep duration rises; `sweeps_skipped_total` may rise; no false findings; context deadline respected. |
| 38 | Redis restart without persistence | T | Equivalent to #28 plus a reconnect. Client must reconnect automatically. |
| 39 | Sentinel failover to a replica with lag | T | If `requirePrimary: true` → `sweeps_total{result="error"}`, no findings. If false → possible transient findings that resolve; assert they are classified `transient_divergence_total`, not confirmed, because two-phase confirm rides out the lag if lag < W. |
| 40 | `SCAN` cursor reset by concurrent `FLUSHDB` | T | Scan aborts with a typed error rather than looping forever. `sweeps_total{kind="target_to_oracle",result="aborted"}` = 1. **Bounded runtime asserted with a hard test timeout.** |
| 41 | Key added to target mid-`SCAN` | T | Must **not** be reported as `extraInTarget` (two-pass extras rule). Explicitly assert. |
| 42 | Key removed from target mid-`SCAN` | T | Must not be reported. |
| 43 | Empty target, full oracle | T | Mass confirmed `missingInTarget`. |
| 44 | Full target, empty oracle, bootstrap `Adopt` | T | Zero findings (all adopted). `coverage_ratio` reflects that adopted keys aren't asserted. |
| 45 | Full target, empty oracle, bootstrap `Wait` | T | Zero findings, `coverage_ratio` ≈ 0, rising as events arrive. |
| 46 | Full target, empty oracle, bootstrap `Strict` | T | `Phase: AwaitingSnapshot`, zero findings until a snapshot cycle completes. |

### 15.3 driftwatch-internal faults

| # | Scenario | Expected |
|---|---|---|
| 47 | Ingest buffer overflow (publish 10× faster than decode) | `events_dropped_total{reason="buffer_full"}` rises. `IngestBackpressure` alert condition. Keys marked Suspect (dropped events are lost events). **No blocking**, no unbounded memory. |
| 48 | `maxTrackedKeys` reached | `oracle_evictions_total` rises, `OracleSaturated=True`, `coverage_ratio` drops, memory bounded. Evicted keys produce no findings. |
| 49 | Confirm queue full | `confirm_queue_dropped_total` rises. Already-confirmed findings are retained. |
| 50 | Sweep exceeds `sweepInterval` | `sweeps_skipped_total` rises, no overlapping sweeps (assert with a counter of concurrent entries). |
| 51 | Hot key never settles (event every 100ms, W=5s) | `never_settled_keys` = 1 initially; then the stability-window check settles it. Assert it is eventually compared. |
| 52 | Adaptive W hits `max` | `settlement_window_seconds` = 120, clamped, condition set, no unbounded growth. |
| 53 | Zero observations for lag estimator | W = `min`, `Adaptive: false` in status, no panic, no busy-loop (assert CPU via a bounded iteration count). |
| 54 | Context cancelled mid-sweep | Sweep aborts within 1s, `sweeps_total{result="aborted"}`, all goroutines exit, goleak clean. |
| 55 | Panic injected in the applier | Recovered at the goroutine boundary, `panics_total` = 1, check's context cancelled, other checks unaffected (test with 2 checks). |
| 56 | Projection returns an error for 100% of events | `projection_errors_total` rises, rate-limited logging, oracle stays empty, no findings, process stays up. |
| 57 | `Close()` called twice | No panic, no double-close of channels. |
| 58 | `Close()` during bootstrap | Bootstrap scan aborts, clean shutdown within `shutdownGrace`. |
| 59 | Spec change from `keysetOwnership` to `scalar` (immutable field) | Webhook rejects. If bypassed, `Apply` returns shape-mismatch errors rather than panicking. |
| 60 | 50 concurrent checks in one manager | All run, metrics separable, memory linear in checks, shutdown of all within `shutdownGrace`. |

**Every row above must have a corresponding named test.** Add a meta-test `TestFaultMatrix_Coverage` that reflects over the test names in `test/faults/` and fails if any matrix row number lacks a test named `TestFault<NN>_<Name>`. This makes the matrix self-enforcing rather than aspirational — a documentation table that CI verifies is worth ten that it doesn't.

---

## 16. Test strategy

### 16.1 Levels and what belongs where

| Level | Location | Runtime | Deps | What it covers |
|---|---|---|---|---|
| Unit | `pkg/*/[!_]*_test.go` | < 10s total | none | every function, every edge case |
| Property | `pkg/*/*_property_test.go` | < 60s | rapid | the invariants in §5.8 |
| Fuzz | `pkg/codec/fuzz_test.go` | 60s in CI | none | never panic on arbitrary input |
| Integration | `pkg/target/*_integration_test.go` | < 90s | Docker | real Redis behaviour |
| Fault | `test/faults/*_test.go` | < 120s | none (fake clock, in-process) | the §15 matrix |
| Controller | `internal/controller/*_test.go` | < 90s | envtest | reconcile lifecycle |
| E2E | `test/e2e/` | < 8min | Kind + Docker | the real path |
| Soak | `test/soak/` | 60min nightly | Docker | leaks, stability |
| Interop | `test/interop/` | < 60s | Python + libzmq | ZMQ wire compat |
| Benchmark | `*_bench_test.go` | < 120s | none | performance regressions |

**Build tags**: `integration`, `e2e`, `soak`, `interop`. Default `go test ./...` runs only unit + property + fault tests and must complete in **under 3 minutes**. This matters more than it sounds: a slow default test command stops being run.

### 16.2 Property tests (the invariants from §5.8)

Use `pgregory.net/rapid`. Each property test must:
- Define a generator for the input domain (events, values, orderings) as a reusable `gen*.go` helper.
- Run at least 1,000 iterations locally and 10,000 in CI (`-rapid.checks`).
- On failure, rapid's shrinking must produce a minimal reproducer; **commit that reproducer as an explicit table-driven regression test** rather than relying on rapid to rediscover it.

Required generators in `pkg/testgen/`:
```go
func Key(t *rapid.T) string                    // incl. empty, binary, long, glob metachars
func Member(t *rapid.T) string
func Op(t *rapid.T) event.Op
func Event(t *rapid.T, pub string, seq uint64) event.Event
func EventStream(t *rapid.T, publishers, count int) []event.Event   // valid seqs
func Permutation(t *rapid.T, evs []event.Event) []event.Event
func WithdrawSubset(t *rapid.T, evs []event.Event) (kept, withheld []event.Event)
func Value(t *rapid.T, kind event.ValueKind) event.Value
```

Map each invariant to its test:

| Invariant | Test |
|---|---|
| I1 idempotence | `pkg/projection: TestProp_ApplyTwiceEqualsOnce` |
| I2 commutativity | `pkg/projection: TestProp_CommutativePermutationInvariant` |
| I3 convergence | `pkg/projection: TestProp_ConvergesToReference` |
| I4 gap detection | `pkg/seqtrack: TestProp_WithheldSeqsAlwaysDetected` |
| I5 no double count | `pkg/seqtrack: TestProp_DuplicatesNeverAcceptedTwice` |
| I6 differ soundness | `pkg/differ: TestProp_EmptyIffEqual` |
| I7 confirm implies 2 reads | `pkg/sweeper: TestProp_ConfirmedImpliesTwoDisagreements` |
| I8 oracle bounded | `pkg/oracle: TestProp_MemoryBounded` |
| I9 gapset bounded | `pkg/seqtrack: TestProp_GapSetIntervalsBounded` |
| I10 snapshot clears suspect | `pkg/oracle: TestProp_SnapshotClearsSuspect` |
| I11 never both states | `pkg/sweeper: TestProp_NeverInflightAndDivergent` |
| I12 version fencing | `pkg/oracle: TestProp_FencedReadNeverTorn` |
| I13 read-only | enforced by `RecordingTarget` in every package |
| I14 clean shutdown | `goleak` in every `TestMain` |

### 16.3 Table-driven test convention

Every unit test uses this shape, and the `name` must describe the *behaviour*, not the input:

```go
func TestKeysetOwnership_Apply(t *testing.T) {
    tests := []struct {
        name    string
        prev    event.Value
        event   event.Event
        want    projection.Mutation
        wantErr error
    }{
        {
            name:  "removing the last member yields a delete, matching Redis empty-set semantics",
            prev:  setOf("replica-0"),
            event: removeEvent("k", "replica-0"),
            want:  projection.Mutation{Key: "k", Action: projection.ActionDelete},
        },
        // ...
    }
    for _, tc := range tests {
        t.Run(tc.name, func(t *testing.T) { /* ... */ })
    }
}
```

### 16.4 Time in tests

**Zero `time.Sleep` in unit, property, fault, or controller tests.** Every one uses `clock.Fake`. Add a CI check: `hack/verify-no-sleep.sh` greps for `time.Sleep` outside `test/e2e/` and `test/soak/` and fails the build. This is a small script that permanently prevents the most common source of flaky tests.

E2E and soak may use `Eventually`/`Consistently` with polling, never bare sleeps.

### 16.5 Goroutine leak detection

Every package with goroutines gets:

```go
func TestMain(m *testing.M) {
    goleak.VerifyTestMain(m,
        goleak.IgnoreTopFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).reaper"),
        // add ignores ONLY for third-party goroutines, with a comment
        // explaining why each is safe. Never ignore our own.
    )
}
```

Any ignore for driftwatch's own code is a bug to fix, not an ignore to add.

### 16.6 ZMQ interop test (proves §8.1)

`test/interop/`, build tag `interop`, CI job with Python + libzmq installed.

1. `publisher.py` uses `pyzmq` (real libzmq) to bind a PUB socket and emit 10,000 events with known seqs across 3 topics, including multipart and single-frame variants, and payloads with binary content.
2. The Go test subscribes with `go-zeromq/zmq4`, collects, and asserts: all 10,000 received (allowing for the documented startup race — use a synchronization handshake, not a sleep), topic filtering worked, multipart framing parsed correctly, binary payloads byte-identical.
3. Reverse direction too: Go PUB → `pyzmq` SUB, verified by the Python process writing results to a file the Go test reads.

**Whatever this test reveals goes in `docs/DISCOVERIES.md`.** The ZMQ subscribe-before-connect race ("slow joiner") is real and will bite; documenting it with the reproduction is exactly the kind of finding that makes the repo credible.

### 16.7 Soak test

`test/soak/soak_test.go`, tag `soak`, nightly CI, 60 minutes:
- 3 publishers, 5,000 events/sec, 500,000 distinct keys, real Redis, real materializer.
- Assertions sampled every minute:
  - `divergentKeys == 0` for the entire run (this is success criterion S2).
  - RSS growth over the final 45 minutes < 5% (allowing warmup).
  - Goroutine count stable within ±5 after warmup.
  - No `panics_total`.
  - Sweep p99 duration < `sweepInterval`.
  - `coverage_ratio > 0.95`.
- At the 30-minute mark, inject a deliberate 10-event drop and assert detection and then resolution — proving the tool still *works* after 30 minutes, not just that it doesn't crash.
- Dump `pprof` heap and goroutine profiles at start, middle, and end as CI artifacts.

### 16.8 Benchmarks and regression gating

Required benchmarks:

| Benchmark | Target |
|---|---|
| `BenchmarkCodecJSONDecode` | > 500k events/sec/core, < 3 allocs/op |
| `BenchmarkSeqTrackObserve` | > 5M ops/sec/core, 0 allocs/op steady state |
| `BenchmarkProjectionApply` | > 2M ops/sec/core |
| `BenchmarkOracleApply` | > 500k ops/sec/core |
| `BenchmarkOracleGet` | > 2M ops/sec/core |
| `BenchmarkSettledKeys1M` | < 50ms to iterate 1M settled keys |
| `BenchmarkMarkSuspectAll1M` | < 1ms (generation-counter approach) |
| `BenchmarkGetMany500` | dominated by network; assert < 5 allocs/key |
| `BenchmarkFullSweep1M` | < 10s against real Redis (S6) |
| `BenchmarkOracleMemory1M` | < 512 MiB RSS (S5) |

Gate with `benchstat` in CI against committed baselines in `docs/benchmarks/`: fail if any benchmark regresses > 20% or allocations increase at all. Commit the baseline and update it deliberately with a commit message explaining the change.

### 16.9 Coverage targets

| Package | Minimum |
|---|---|
| `pkg/event`, `pkg/codec`, `pkg/seqtrack`, `pkg/projection`, `pkg/differ` | 95% |
| `pkg/oracle`, `pkg/sweeper`, `pkg/lag`, `pkg/explain` | 90% |
| `pkg/target`, `pkg/source` | 85% (network paths) |
| `pkg/check`, `pkg/metrics`, `pkg/clock` | 90% |
| `internal/controller` | 80% |
| `internal/cli` | 70% |
| **overall** | **≥ 88%** |

CI fails below target. Coverage is a floor, not a goal — a 95%-covered module with no property tests is worse than an 85%-covered one with them. Do not chase coverage by testing getters.

---

## 17. CI/CD

### 17.1 `.github/workflows/ci.yaml`

Jobs, all on `ubuntu-latest`, triggered on push and PR:

| Job | Steps |
|---|---|
| `lint` | `golangci-lint run --timeout 5m`; `gofumpt -l -d .` (fail if output); `go vet ./...`; `hack/verify-codegen.sh` (deepcopy/CRD manifests up to date); `hack/verify-metrics-docs.sh`; `hack/verify-no-sleep.sh`; `hack/verify-fault-matrix.sh` |
| `unit` | `go test -race -covermode=atomic -coverprofile=cover.out ./pkg/... ./internal/... ./api/...`; enforce per-package thresholds with a small script; upload coverage |
| `property` | `go test -race -run 'TestProp_' -rapid.checks=10000 ./pkg/...` (separate job because it's slow) |
| `fault` | `go test -race ./test/faults/...` |
| `fuzz` | `go test -run='^$' -fuzz=Fuzz -fuzztime=60s ./pkg/codec/` |
| `integration` | Redis service container; `go test -tags=integration -race ./pkg/target/...` |
| `interop` | `apt-get install libzmq3-dev python3-zmq`; `go test -tags=interop ./test/interop/...` |
| `controller` | `setup-envtest`; `go test -race ./internal/controller/...` |
| `build` | cross-compile `linux/amd64`, `linux/arm64`, `darwin/arm64`; build and scan the container image with `trivy` (fail on HIGH/CRITICAL) and `govulncheck ./...` |
| `bench` | `go test -run='^$' -bench=. -benchmem ./... | tee bench.txt`; `benchstat baseline.txt bench.txt`; fail on regression |

All jobs use Go module and build caching. Total CI wall time target: **under 10 minutes** for the required set.

### 17.2 `.github/workflows/e2e.yaml`

Separate workflow (slower, and should be able to fail independently on infra flakes without blocking a docs PR):
- `helm/kind-action` to create the cluster.
- Build and load the image.
- `go test -tags=e2e -timeout=20m ./test/e2e/...`
- **Always** upload `test/e2e/_artifacts/` on failure.
- Runs on PRs to `main` and on push to `main`.

### 17.3 `.github/workflows/soak.yaml`
Nightly cron. 90-minute timeout. Uploads pprof profiles and the final metrics dump.

### 17.4 `.github/workflows/release.yaml`
On tag `v*`: `goreleaser` → multi-arch binaries + checksums + SBOM (`syft`), multi-arch container image to GHCR, signed with `cosign`, Helm chart packaged and published, release notes generated from conventional commits.

### 17.5 `Makefile`

Targets (every one must actually work — test them):

```
make help                  # default; self-documenting via ## comments
make build                 # both binaries into bin/
make install-tools         # controller-gen, golangci-lint, kind, envtest, ginkgo, benchstat
make generate              # deepcopy
make manifests             # CRDs + RBAC
make lint fmt vet
make test                  # unit + property + fault, race, coverage
make test-unit test-property test-fault
make test-integration      # needs Docker
make test-interop          # needs python3-zmq
make test-controller       # envtest
make bench benchstat
make fuzz FUZZTIME=60s
make e2e                   # THE headline target: kind up, build, load, run, tear down
make e2e-keep              # leave the cluster for debugging
make e2e-reuse             # reuse an existing cluster (fast iteration)
make soak DURATION=60m
make docker-build docker-push
make deploy undeploy       # kustomize to the current kubecontext
make demo                  # docker-compose: redis + publisher + materializer + driftwatch
                           # + prometheus + grafana with the dashboard pre-loaded
make verify                # everything CI runs, locally
make clean
```

**`make demo` is a required deliverable, not a nicety.** One command that brings up the whole stack with Grafana at localhost:3000 showing the dashboard, plus `make demo-inject-drift` that triggers a fault so a visitor can watch the number go red and then recover. This is how you show the project in 60 seconds — in an interview, in a PR description, in a README GIF. Build it in Phase 8 and make sure it works from a clean clone.

---

## 18. Security and hardening

| Requirement | Implementation |
|---|---|
| No secrets in logs | `logging.Redact`; config logged once with `[REDACTED]`; a unit test asserting a password never appears in captured log output |
| No secrets in status/events | Controller must never copy secret values into `status` or Events; unit-tested |
| Read-only against the target | `RecordingTarget` in tests + a `redis.Hook` command allowlist at runtime |
| Least-privilege RBAC | Only the verbs in §10.3; `Role` per namespace for secrets where possible; committed generated RBAC |
| Non-root container | distroless `nonroot`, UID 65532, `readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`, all capabilities dropped, `seccompProfile: RuntimeDefault` |
| No shell in the image | distroless static |
| Resource limits in manifests | requests/limits set in the sample deployment and Helm defaults; documented sizing guidance keyed to `maxTrackedKeys` |
| Bounded resource use | every queue, map, and buffer has an explicit cap (audited in §19.2) |
| TLS to Redis and NATS | supported with CA from a secret; `insecureSkipVerify` allowed but logged at WARN on every startup |
| Input validation | payload size cap, JSON depth cap, key length cap, member count cap — all enforced at the boundary |
| Dependency scanning | `govulncheck` + `trivy` in CI, failing on HIGH/CRITICAL |
| Supply chain | SBOM + cosign signatures on release |
| No network egress beyond configured endpoints | documented; no telemetry, no phone-home, no update check |
| Fuzzing | codec fuzzed in CI; corpus committed |

**Explicit note for the README:** driftwatch reads potentially sensitive key names and values. The `--redact-keys` flag hashes key names in all output, and `retainRaw: false` (the default) means payloads are never stored. State this plainly — operators will ask.

---

## 19. Performance requirements

### 19.1 Targets

| Dimension | Target | Verified by |
|---|---|---|
| Ingest throughput | ≥ 50,000 events/sec sustained, single check, 4 cores | soak + benchmark |
| Ingest latency (receive → oracle applied) | p99 < 5ms | benchmark with timestamps |
| Sweep of 1M keys | < 10s against local Redis | `BenchmarkFullSweep1M` |
| Memory, 1M keys, `ringSize=16`, `retainRaw=false` | < 512 MiB RSS | `BenchmarkOracleMemory1M` + soak |
| Memory, 10M keys | < 5 GiB, documented as the practical ceiling | soak variant |
| Redis read load added | < 5% of the materializer's write QPS at default settings | measured in soak, documented |
| CPU, steady state at 5k events/sec | < 0.5 core | soak |
| Startup to `Watching` (Adopt, 1M keys) | < 30s | e2e timing assertion |

### 19.2 Bounded-resource audit

Every one of these must have an explicit cap, a metric, and a test proving the cap holds. Maintain this as a checklist in `docs/TESTING.md`:

| Resource | Cap |
|---|---|
| raw ingest channel | `ingestBufferSize` |
| decoded event channel | `ingestBufferSize / 2` |
| oracle keys | `maxTrackedKeys` |
| per-key event ring | `ringSize` |
| members per key | `maxMembersPerKey` |
| publishers tracked | `maxPublishers` |
| gap intervals per publisher | `maxGapIntervals` |
| confirm queue | `maxConfirmQueue` |
| findings per report | `maxFindings` |
| extras tracked | `maxExtrasTracked` |
| lag observations | `WindowSize` |
| Prometheus label values (publisher) | `maxPublisherLabels` |
| payload size | `maxPayloadBytes` |
| JSON nesting depth | `maxJSONDepth` (default 32) |
| key length | `maxKeyBytes` (default 4096) |
| log volume per reason | token bucket |

**There must be no unbounded collection anywhere in the codebase.** Add a review checklist item and, in Phase 8, do an explicit audit pass reading every `make(map` and `append` in the codebase to confirm each is bounded or provably short-lived. Record the audit result in `docs/DISCOVERIES.md`.

---

## 20. Phased delivery plan

Nine phases. **Do them in order.** Each ends with a commit, green CI, and something demonstrable. The ordering is chosen so that abandoning after any phase still leaves a coherent project.

---

### Phase 0 — Scaffold (target: 0.5 day)

**Build:** `go mod init`; the full directory tree from §7 (empty files with package declarations are fine); `Makefile` with `help build lint test fmt vet clean`; `.golangci.yml` (enable `errcheck govet staticcheck unused gosimple ineffassign misspell revive gocritic bodyclose contextcheck errorlint nilerr prealloc unconvert`); `.github/workflows/ci.yaml` with lint + unit jobs; `LICENSE` (Apache-2.0); `CONTRIBUTING.md`; skeleton `README.md`; `docs/` skeleton with empty `DISCOVERIES.md`, `DECISIONS.md`, `KNOWN_GAPS.md`; `hack/install-tools.sh`.

**Exit criteria:**
- [ ] `make lint test` passes on an empty codebase.
- [ ] CI green on the first push.
- [ ] `docs/DECISIONS.md` contains the ADRs for §8.1–§8.5 written up properly.

---

### Phase 1 — Core domain (target: 2 days)

**Build:** `pkg/clock` (M1) → `pkg/event` (M2) → `pkg/codec` (M3, json only) → `pkg/seqtrack` (M5) → `pkg/projection` (M6, all three + reference) → `pkg/oracle` (M7). Plus `pkg/testgen` generators.

This is the intellectual core and it is all pure, in-memory, and fast to test. Do not touch the network in this phase.

**Exit criteria:**
- [ ] All of M1, M2, M3(json), M5, M6, M7 complete per §9 including every listed edge case.
- [ ] Property tests I1, I2, I3, I4, I5, I8, I9, I12 passing at 10,000 rapid checks.
- [ ] `TestKeysetOwnership_LastMemberRemoval_YieldsDelete` passing.
- [ ] Fuzz test for the json codec running clean for 60s.
- [ ] Coverage ≥ 95% on `event`, `codec`, `seqtrack`, `projection`; ≥ 90% on `oracle`.
- [ ] `BenchmarkOracleApply`, `BenchmarkProjectionApply`, `BenchmarkSeqTrackObserve` meeting §16.8 targets; baselines committed.
- [ ] `-race` clean; goleak in every `TestMain`.
- [ ] `make test` under 90 seconds.

**Demo at end of phase:** a Go test that feeds 100,000 synthetic events through codec → seqtrack → projection → oracle and prints the resulting state and gap report.

---

### Phase 2 — Target and differ (target: 1.5 days)

**Build:** `pkg/target` (M8: interface, `memory`, `recording`, `redis`) → `pkg/differ` (M9).

**Exit criteria:**
- [ ] `memory` and `recording` targets complete; `RecordingTarget` proven to catch an attempted write.
- [ ] `redis` target complete with miniredis unit tests for every method.
- [ ] Integration tests (tag `integration`) passing against real Redis 6 and 7 via testcontainers, including `SCAN` over 100k keys, `WRONGTYPE`, TTL, `INFO` parsing on both versions, and cluster mode.
- [ ] `differ` complete with the full comparison table and property test I6.
- [ ] The `SCAN`-cursor-reset-on-`FLUSHDB` behaviour investigated, handled, and written up in `DISCOVERIES.md`.
- [ ] `BenchmarkGetMany500`, `BenchmarkScan1M` committed.

**Demo:** `go test` that seeds miniredis, builds an oracle from events, diffs, and prints a `Report.Text()`.

---

### Phase 3 — Sweeper: the correctness mechanisms (target: 2.5 days)

**Build:** `pkg/lag` (M11) → `pkg/sweeper` (M10: sweep, confirm, extras).

This phase implements §5.3–§5.5 and is the highest-risk part of the project. Take the time. Write the tests first.

**Exit criteria:**
- [ ] Settlement window (static and adaptive), two-phase confirmation, version fencing, and the two-pass extras scan all implemented exactly as specified.
- [ ] Drift resolution implemented and tested — a repaired key **must** leave `Confirmed()` and increment `drift_resolved_total`.
- [ ] Property tests I7, I11 passing.
- [ ] Every §15.3 internal-fault row (47–54) tested.
- [ ] All sweeper tests use `FakeClock` with zero real sleeps.
- [ ] `hack/verify-no-sleep.sh` in CI and passing.

**Demo:** a fully in-process test that injects drift, confirms it, repairs it, and shows resolution — with zero wall-clock time elapsed.

---

### Phase 4 — Sources and the fault injector (target: 2 days)

**Build:** `pkg/source` (M4: `memory`, `file`, `zmq`, `nats`) → `test/harness/faultinjector` (§13) → `test/harness/publisher` and `test/harness/materializer` → `test/harness/scenario` DSL.

**Exit criteria:**
- [ ] All four sources complete, including reconnect with gap signalling and the DNS re-resolution fix.
- [ ] `zmq` tested against an in-process pure-Go PUB socket: delivery, multipart both conventions, topic filtering, reconnect, HWM drop under a slow consumer.
- [ ] `TestFaults_Deterministic` passing for every fault.
- [ ] Scenario DSL working and readable.
- [ ] The NATS queue-group validation rejects with the specified message.

**Demo:** `driftwatch inject --scenario drop-burst` printing observed drift to stdout.

---

### Phase 5 — Composition, metrics, CLI (target: 2 days)

**Build:** `pkg/metrics` (M12) → `pkg/check` (M14) → `pkg/explain` (M13) → `internal/cli` (§11) → `cmd/driftwatch`.

**Exit criteria:**
- [ ] `TestCheck_EndToEnd_InProcess` passing — the flagship composition test.
- [ ] Metric name test and cardinality test passing; `docs/METRICS.md` generated and CI-verified.
- [ ] `explain` complete with every diagnosis rule individually tested and golden-file output tests.
- [ ] All CLI commands working with golden-file tests and correct exit codes.
- [ ] `driftwatch watch -f examples/local.yaml` works against a local `docker run redis`.
- [ ] `driftwatch replay` fully hermetic (no network) and deterministic.

**Demo:** the `explain` text output from §M13 reproduced against a real injected fault. **Capture it to `docs/evidence/explain-dropped-event.txt`.**

---

### Phase 6 — The full fault matrix (target: 2 days)

**Build:** `test/faults/` — every row of §15.1 and §15.2 not already covered.

**Exit criteria:**
- [ ] All 60 matrix rows have a passing named test.
- [ ] `hack/verify-fault-matrix.sh` in CI, passing.
- [ ] `TestFaults_DriftwatchOwnLoss_ReportsSuspectNotConfirmed` passing — the honesty test.
- [ ] Full fault suite runs in under 120s with no flakes across 20 consecutive runs (**actually run it 20 times and record the result in `docs/evidence/`**).

**Demo:** `make test-fault` output showing 60 green tests, captured to evidence.

---

### Phase 7 — Kubernetes (target: 2.5 days)

**Build:** `api/v1alpha1` (CRD types, deepcopy, webhooks) → `internal/controller` → `cmd/driftwatch-manager` → `config/` kustomize → `deploy/helm`.

**Exit criteria:**
- [ ] CRD applies cleanly; `kubectl explain driftcheck.spec` shows descriptions for every field (means every field has a doc comment).
- [ ] Every §10.2 validation rule implemented with a webhook unit test.
- [ ] envtest suite covering the full lifecycle, the rapid-update-storm single-runner test, and mid-bootstrap deletion.
- [ ] RBAC generated, committed, and minimal.
- [ ] Helm chart lints and templates for dev and prod values.
- [ ] `make deploy` works against a Kind cluster.
- [ ] goleak clean after manager teardown.

---

### Phase 8 — E2E, dashboard, demo (target: 2.5 days)

**Build:** `test/e2e` (§14) → `deploy/grafana/driftwatch-dashboard.json` → `config/prometheus/rules.yaml` → `docs/OPERATIONS.md` runbook → `make demo`.

**Exit criteria:**
- [ ] All 8 e2e scenarios passing.
- [ ] Diagnostics collector working — verified by deliberately breaking a test and confirming the artifact dump is complete and useful.
- [ ] E2E suite under 8 minutes, passing 5 consecutive runs.
- [ ] `make e2e` works **from a clean clone in a fresh container** with only Docker and Go. Verify this literally.
- [ ] Grafana dashboard imports and every panel renders with data.
- [ ] `make demo` + `make demo-inject-drift` works; screenshot captured showing drift rising and resolving.
- [ ] Runbook section for every alert.
- [ ] Interop test passing; findings in `DISCOVERIES.md`.
- [ ] Soak test written and one full 60-minute run passing, with pprof artifacts in `docs/evidence/`.

---

### Phase 9 — Polish for presentation (target: 1.5 days)

This phase is not optional. It is the phase that converts working code into a repository that gets someone selected.

**Build:**
- [ ] **README rewrite** per §21.1. Architecture diagram, the plain-English problem statement, real measured numbers, Key Discoveries, quick start that actually works, honest limitations.
- [ ] **`docs/CORRECTNESS.md`** — §5 rewritten as prose for a human reader. This document is the single strongest artifact in the repo for demonstrating systems thinking. Take it seriously.
- [ ] **`docs/DISCOVERIES.md`** finalized: at least 8 real findings, each with reproduction and evidence link.
- [ ] **`docs/evidence/README.md`** — the index table mapping each evidence file to the claim it proves.
- [ ] **Bounded-resource audit** (§19.2) performed and recorded.
- [ ] **Benchmark results table** in the README with the machine spec stated.
- [ ] A short **demo GIF or asciinema** cast of `make demo` → inject drift → `explain`.
- [ ] `docs/ADDING_A_SOURCE.md` and `docs/ADDING_A_PROJECTION.md` — extension guides, which also prove the abstractions are real.
- [ ] `docs/KNOWN_GAPS.md` — honest list: no sharded oracle (NG4), no Kafka source, no repair, no OTel tracing, e2e coverage limits.
- [ ] Grep the entire repo for "production-grade", "enterprise", "institutional", "blazing", "robust" and delete every instance. Replace with a number or nothing.
- [ ] Grep for `TODO`, `FIXME`, `XXX`, `panic(` in non-test code; resolve or justify each.
- [ ] Tag `v0.1.0`; release workflow produces binaries, image, SBOM, signatures.

**Total: approximately 19 working days.** Compressible to ~12 by trimming Phase 6 to the highest-value 30 matrix rows and Phase 7's Helm chart, but **do not trim Phases 1, 3, or 9.** Phase 1 is the foundation, Phase 3 is the substance, and Phase 9 is the entire reason the project exists as a signalling artifact.

---

## 21. Documentation deliverables

### 21.1 README structure (write it last, in this order)

```markdown
# driftwatch

> One sentence: what it detects.

[CI badge] [Go report] [license] [release]

## The problem
(4–6 sentences. The library analogy, compressed. Then one concrete
software example. No jargon in the first paragraph.)

## What driftwatch does
(3 bullets: independent oracle, periodic comparison, per-key explanation.)

## Quick start
(A copy-pasteable block that works. `make demo` and what you'll see.
Then the 8-line YAML for a real check.)

## Example output
(The `explain` text block. This is the single most persuasive thing
in the README — put it high.)

## How it avoids false positives
(6–8 sentences summarizing §5: settlement window, two-phase confirm,
version fencing, sequence-gap trust states. Link to docs/CORRECTNESS.md.
This section is what a maintainer reads to decide whether the author
actually thought about the problem.)

## Architecture
(The §6.1 diagram, then the 10-step data flow compressed to 10 lines.)

## Metrics and dashboard
(Table of the 8 headline metrics. Dashboard screenshot.)

## Configuration
(The DriftCheck spec, abbreviated, with a link to the full reference.)

## Measured performance
(Real numbers with the machine spec. No estimates.)

## Key Discoveries
(6–10 findings from DISCOVERIES.md, one paragraph each, each linking
to its evidence file. THIS IS THE HIGHEST-VALUE SECTION IN THE README.)

## Testing
(The §16.1 table. Then: "N unit tests, M property tests at 10k checks
each, 60 fault scenarios, 8 Kind-based e2e scenarios, 60-minute soak."
Real numbers only.)

## Limitations
(Honest. From KNOWN_GAPS.md. A limitations section increases trust;
its absence decreases it.)

## Contributing / License
```

### 21.2 `docs/CORRECTNESS.md`

Rewrite §5 as prose for a reader who has not read this PRD. Structure: the naive approach → why it fails (with a worked numeric example showing the false-positive rate) → each mechanism and what it defeats → what remains undetectable and why. End with the invariant table.

This document is worth more than any single code file for the purpose of getting selected. A maintainer reading it learns in five minutes that the author reasons carefully about distributed state. Write it deliberately, not as a dump.

### 21.3 `docs/DISCOVERIES.md` — format

Maintain this from Phase 0 onward. One entry per finding, newest first:

```markdown
## D-007 — Redis returns an empty array for a set with no members, and deletes the key

**Found:** Phase 2, while implementing the keysetOwnership projection.

**What happened:** The projection emitted an upsert with an empty member set
when the last member was removed. The target had no key at all, because Redis
deletes a set key when its final member is removed via SREM. Every key that
ever emptied produced a permanent false `missing_in_target`.

**Why it matters:** This would have made driftwatch report drift on every
key that legitimately emptied — the most common transition in a KV-cache
ownership index. In a 1M-key workload the false-positive rate would have
been high enough to make the tool unusable.

**Fix:** `Value.Equal` treats an empty member set as equal to absent, and
the projection emits `ActionDelete` rather than an empty upsert.

**Evidence:** `docs/evidence/D-007-redis-empty-set.txt`

**Regression test:** `pkg/projection: TestKeysetOwnership_LastMemberRemoval_YieldsDelete`
```

**Target: at least 8 real entries.** Candidates the implementation will almost certainly hit — record whichever actually occur, and do not invent ones that don't:

- Redis empty-set deletion semantics (above).
- ZMQ slow-joiner: a SUB socket connected after PUB starts misses early messages, with no error. Requires a synchronization handshake in tests.
- ZMQ PUB drops silently at the HWM; the interaction with driftwatch's own buffer sizing.
- Multipart vs single-frame ZMQ conventions both existing in the wild.
- JSON numbers above 2^53 losing precision when decoded as `float64` — a sequence number silently corrupted.
- Redis `SCAN` cursor semantics: keys may repeat; `FLUSHDB` resets the cursor and can cause an infinite loop.
- `INFO` output differences between Redis 6 and 7 breaking a naive parser.
- Discarding timed-out lag probes biasing the p99 estimate downward, shrinking W, causing false positives.
- Kubernetes DNS re-resolution: caching the first resolved IP breaks reconnection after a pod reschedule.
- Go's `time.Ticker` coalescing missed ticks, which changes fake-clock test expectations.
- `MarkSuspect` on 1M keys taking seconds with a naive per-entry write; the generation-counter fix.
- goleak catching a leaked goroutine in a specific dependency, and why the ignore is safe.

### 21.4 `docs/evidence/` convention

One file per claim, named `<id>-<slug>.<ext>`. `docs/evidence/README.md` is an index table:

| File | Claim it proves | Produced by |
|---|---|---|
| `S2-soak-60min-zero-drift.txt` | Zero false positives over 60 min at 5k events/sec | `make soak` |
| `S6-sweep-1m-keys.txt` | 1M-key sweep in under 10s | `BenchmarkFullSweep1M` |
| `explain-dropped-event.txt` | `explain` identifies the exact missing seq | e2e E2 |
| `fault-matrix-60-green.txt` | All 60 fault scenarios pass | `make test-fault` |
| `fault-matrix-20-runs-no-flake.txt` | No flakes across 20 runs | `hack/repeat-tests.sh 20` |
| `e2e-8-scenarios.txt` | Full Kind e2e suite green | `make e2e` |
| `dashboard-drift-detected.png` | Dashboard shows drift rise and resolution | `make demo` |
| `D-007-redis-empty-set.txt` | Redis empty-set behaviour | manual repro |
| `interop-pyzmq-10k.txt` | Wire compatibility with libzmq | `make test-interop` |
| `memory-1m-keys.txt` | Under 512 MiB at 1M keys | `BenchmarkOracleMemory1M` |
| `pprof-soak-heap.pb.gz` | No heap growth over 60 min | `make soak` |
| `coverage-summary.txt` | Coverage by package | `make test` |

**Capture these as you go.** Reconstructing evidence at the end is painful and the output will be less convincing.

### 21.5 Other required docs

- `docs/ARCHITECTURE.md` — the diagram, component responsibilities, concurrency model, why the applier is single-threaded.
- `docs/TESTING.md` — how to run each level, the bounded-resource checklist, how to add a fault scenario.
- `docs/OPERATIONS.md` — a runbook section per alert: what it means, what to check first, likely causes, how to confirm.
- `docs/ADDING_A_SOURCE.md`, `docs/ADDING_A_PROJECTION.md` — with a worked example each.
- `docs/METRICS.md` — generated, CI-verified.
- `docs/DECISIONS.md` — ADRs.
- `docs/KNOWN_GAPS.md` — honest limitations.
- `CONTRIBUTING.md` — how to build, test conventions, commit format, the "no `time.Sleep`" rule.

---

## 22. Master definition of done

The project is complete when every box is checked. Do not declare completion early.

**Functionality**
- [ ] All modules M1–M14 implemented per §9.
- [ ] `DriftCheck` CRD with defaulting and validating webhooks; every §10.2 rule enforced.
- [ ] Controller with leader election, finalizers, status patching, events.
- [ ] All 6 CLI commands working with documented exit codes.
- [ ] All 3 bootstrap modes, all 3 expiry policies, static and adaptive W.
- [ ] Sources: memory, file, zmq, nats. Targets: memory, redis (standalone/sentinel/cluster). Codecs: json, msgpack, template. Projections: keysetOwnership, scalar, counter.

**Correctness**
- [ ] All 14 invariants (§5.8) have passing property tests.
- [ ] All 60 fault matrix rows (§15) have passing named tests; `verify-fault-matrix.sh` green.
- [ ] `TestFaults_DriftwatchOwnLoss_ReportsSuspectNotConfirmed` passing.
- [ ] `TestCheck_EndToEnd_InProcess` passing.
- [ ] Read-only enforced structurally (`RecordingTarget` + redis hook), with a test proving each.

**Testing**
- [ ] Coverage meets every §16.9 target; overall ≥ 88%.
- [ ] `-race` clean across the entire suite.
- [ ] goleak in every package's `TestMain`, no ignores for own code.
- [ ] Zero `time.Sleep` outside `test/e2e` and `test/soak`; verified by script in CI.
- [ ] Fuzz corpus committed; 60s fuzz clean in CI.
- [ ] Integration tests pass against Redis 6 and 7.
- [ ] Interop test passes against libzmq.
- [ ] 8 e2e scenarios pass; suite under 8 minutes; 5 consecutive clean runs.
- [ ] 60-minute soak passes all assertions.
- [ ] Fault suite runs 20× with zero flakes; evidence captured.
- [ ] All benchmarks meet §16.8 targets; baselines committed; benchstat gate in CI.

**Operations**
- [ ] Grafana dashboard imports; every panel renders.
- [ ] PrometheusRule with 10 alerts, each with a runbook anchor.
- [ ] `docs/OPERATIONS.md` runbook complete.
- [ ] Helm chart lints and installs.
- [ ] `make demo` works from a clean clone.
- [ ] `make e2e` works from a clean clone in a fresh container.
- [ ] Container: distroless, non-root, read-only rootfs, no shell.
- [ ] Release workflow produces multi-arch binaries, image, SBOM, cosign signature.

**Documentation**
- [ ] README per §21.1, with real numbers and no superlatives.
- [ ] `docs/CORRECTNESS.md` written as prose.
- [ ] `docs/DISCOVERIES.md` with ≥ 8 real findings, each with evidence and a regression test.
- [ ] `docs/evidence/` with ≥ 12 files and an index table.
- [ ] `kubectl explain driftcheck.spec` shows descriptions for every field.
- [ ] `docs/METRICS.md` generated and CI-verified.
- [ ] `docs/KNOWN_GAPS.md` honest and specific.
- [ ] Extension guides for sources and projections.
- [ ] Demo GIF or asciinema cast.

**Hygiene**
- [ ] `golangci-lint` clean.
- [ ] `govulncheck` and `trivy` clean at HIGH/CRITICAL.
- [ ] No `TODO`/`FIXME`/`XXX` in non-test code.
- [ ] No `panic()` in non-test code except documented programmer-error assertions.
- [ ] Bounded-resource audit (§19.2) complete and recorded.
- [ ] Conventional commits throughout; history is readable.
- [ ] `v0.1.0` tagged.

---

## 23. Anti-patterns — how this project would fail

Read this before starting and again at the end of each phase.

**A1 — Skipping the settlement window and two-phase confirm "for now."** The tool will report thousands of false positives, and every subsequent decision will be made on the basis of noisy output. This is the failure mode that kills the project. Build §5 in Phase 3, before the fault matrix, before Kubernetes.

**A2 — Using `time.Sleep` in tests.** The suite becomes slow and flaky, then people stop running it, then it rots. Inject the clock from Phase 1. The `verify-no-sleep.sh` check exists to make this impossible to backslide on.

**A3 — Labeling metrics with key names.** One `prometheus.Labels{"key": key}` turns driftwatch into a cardinality bomb that takes down the monitoring system it's supposed to inform. The cardinality test in §M12 exists specifically to catch this.

**A4 — Unbounded collections.** A `map[string]X` that grows with the keyspace, a slice of all findings, a per-key list of all events. Every one is an OOM in production. §19.2 is a checklist, not a suggestion.

**A5 — Reporting drift when the target is unreachable.** Absence of data is not evidence of divergence. This mistake makes the tool actively harmful during an outage — it fires a drift alert during a Redis incident, sending the operator down the wrong path.

**A6 — Not implementing drift *resolution*.** If a confirmed finding never clears, `divergent_keys` never returns to zero and the alert is permanently firing. A detector that can't tell you the problem is over is a detector people silence. This is easy to forget because tests naturally focus on detection.

**A7 — Claiming the target is wrong when driftwatch's own view has gaps.** The `Suspect` trust state is what makes driftwatch trustworthy. Without it, the tool is confidently wrong whenever its own subscription drops, which is exactly when it matters most.

**A8 — Building breadth instead of depth.** Adding Kafka, etcd, PostgreSQL, and a web UI makes the repo look like a survey. The value is in one deep, correct path. Resist. `KNOWN_GAPS.md` is where extensions go to be acknowledged without being built.

**A9 — Writing the README last, in a hurry.** The README is what gets read. Budget Phase 9 properly. A brilliant codebase behind a thin README gets skimmed and closed.

**A10 — Superlatives instead of numbers.** "Production-grade, blazing-fast, enterprise-ready" reads as unearned to exactly the audience you're trying to reach. "1M-key sweep in 8.3s on a 4-core M1; zero false positives over a 60-minute 5k events/sec soak" reads as real. Always the second.

**A11 — Fake evidence.** Do not write a `DISCOVERIES.md` entry for something that didn't happen, or paste invented benchmark numbers. Anyone who tries to reproduce will find out, and the cost is total. Every claim traces to a real captured file.

**A12 — Parallelizing the applier early.** It will introduce ordering bugs that take days to find, for throughput nobody needs. Single-threaded applier until a benchmark proves otherwise.

**A13 — Testing implementation instead of behaviour.** Tests that assert internal call sequences break on every refactor and prove nothing. Assert on observable outputs: oracle state, report contents, metric values, exit codes.

**A14 — Letting e2e become the primary test level.** E2E is slow and its failures are hard to localize. Every behaviour that *can* be tested in-process with a fake clock *must* be. E2E exists only for integration and packaging.

---

## 24. Mapping to the LFX application

This section exists because the project has a specific purpose beyond being good software.

### 24.1 Skill-to-artifact mapping

The Volcano/Kthena "KVCache-aware scheduler E2E test suite" project lists: *Go, Kubernetes, Kind-based E2E testing, Redis, ZMQ, observability/metrics, and distributed-system debugging.* Every one maps to a concrete artifact:

| Listed skill | Artifact in this repo |
|---|---|
| Go | ~15k lines of idiomatic Go; property tests; benchmarks with allocation gates |
| Kubernetes | `DriftCheck` CRD, controller-runtime operator, webhooks, RBAC, Helm chart, envtest suite |
| Kind-based E2E testing | `test/e2e/` — 8 scenarios, deterministic cleanup, diagnostic artifact collection |
| Redis | `pkg/target/redis.go` — SCAN semantics, pipelining, cluster/sentinel, INFO parsing, eviction correlation |
| ZMQ | `pkg/source/zmq.go` — pure-Go SUB, HWM behaviour, multipart conventions, libzmq interop test |
| Observability / metrics | 40+ Prometheus metrics with a cardinality test, Grafana dashboard, 10 alerts, a runbook |
| Distributed-system debugging | `explain` with 14 diagnosis rules; `docs/CORRECTNESS.md`; the 60-row fault matrix |

The second project (*inPlace rolling update*: `go/kubernetes/markdown`) is covered by the operator work plus `docs/CORRECTNESS.md` as evidence of design-document writing — and separately by the PDB-blindness KEP discussed earlier.

### 24.2 The sentence to use in the application

Do not say "I built a mini kthena." Say:

> I built driftwatch, a general-purpose divergence detector for pub/sub-materialized caches: an independent oracle folded from a ZeroMQ event stream, compared against Redis with a settlement window and two-phase confirmation to eliminate false positives from materializer lag. It includes a 60-scenario fault-injection matrix, property-based convergence tests, and an 8-scenario Kind e2e suite with diagnostic collection.
>
> The KVCache-aware suite in issue #1328 is the same problem shape with vLLM block-ownership events. The reusable pieces the issue asks for — cache-state injection, observation, and assertion utilities — are exactly what I built as `test/harness/faultinjector` and the scenario DSL, and I'd bring that structure to `test/e2e/router/`.

That is a contributor describing transferable work, not an applicant who read the issue and cloned it.

### 24.3 Resume bullets

Phrase with their vocabulary, and lead with the number:

- *Built driftwatch (Go): detects silent divergence between ZeroMQ event streams and Redis-materialized indexes. Zero false positives across a 60-minute soak at 5,000 events/sec; 1M-key consistency sweep in under 10s.*
- *Designed a settlement-window and two-phase-confirmation protocol that eliminates false positives caused by consumer lag; documented the correctness argument and the 14 invariants it rests on.*
- *Wrote a 60-scenario fault-injection harness (event drop/reorder/duplicate/delay, publisher restart, Redis eviction and failover, network partition) plus property-based convergence tests over generated event orderings.*
- *Shipped a Kubernetes operator (controller-runtime, CRD, validating webhook, least-privilege RBAC) and an 8-scenario Kind-based E2E suite with deterministic cleanup and automatic diagnostic artifact collection.*
- *Instrumented 40+ Prometheus metrics with a cardinality-regression test, a Grafana dashboard, and 10 alerts each backed by a runbook section.*

### 24.4 What still matters more than this project

Stated plainly, because it would be dishonest to omit it: **2–3 merged PRs in `volcano-sh/kthena` outweigh this entire repository** in the selection decision. Build driftwatch in the background; spend the foreground time reading kthena's `test/e2e/router/` framework, fixing a flaky test, filling a doc gap, and asking one informed question on issue #1328. The project makes the resume credible. The PRs make the mentor recognize the name.

---

## 25. Appendices

### 25.1 Sample event payloads (commit as `pkg/codec/testdata/`)

Canonical driftwatch JSON format:
```json
{"publisher":"replica-2","epoch":1,"seq":8847,"ts":"2026-07-30T11:02:31.412Z",
 "op":"add","key":"9f3a2c1e","member":"replica-2"}
```

A foreign format requiring `fieldMapping` and `opMapping` (the realistic case):
```json
{"replica_id":"vllm-2","incarnation":1,"event_id":8847,
 "ts":1785412951412,"event_type":"BLOCK_STORED","block_hash":"9f3a2c1e"}
```

Snapshot cycle:
```json
{"publisher":"replica-2","epoch":1,"seq":9000,"op":"snapshotBegin"}
{"publisher":"replica-2","epoch":1,"seq":9001,"op":"add","key":"a1","member":"replica-2"}
{"publisher":"replica-2","epoch":1,"seq":9002,"op":"add","key":"a2","member":"replica-2"}
{"publisher":"replica-2","epoch":1,"seq":9003,"op":"snapshotEnd"}
```

Adversarial payloads for the fuzz corpus: empty; `{}`; `null`; `[]`; `{"seq":1e300}`; `{"seq":"9007199254740993"}`; `{"seq":9007199254740993}` (above 2^53); 1 MiB of nested arrays; invalid UTF-8 in `key`; duplicate JSON keys; `{"op":"ADD"}` (wrong case); `{"ttl":-1}`.

### 25.2 Minimal local config (`examples/local.yaml`)

```yaml
source:
  type: memory
codec:
  type: json
projection:
  type: keysetOwnership
  keyTemplate: "block:{{.Key}}"
target:
  type: redis
  redis:
    addr: localhost:6379
    keyPattern: "block:*"
policy:
  settlementWindow: {mode: static, static: 2s}
  sweepInterval: 10s
  bootstrap: Wait
```

### 25.3 Glossary quick reference

See §4 for full definitions. The five terms that matter most: **oracle** (driftwatch's independent expectation), **settlement window** (grace period for materializer lag), **two-phase confirmation** (re-read before reporting), **trust state** (whether driftwatch's own view is complete), **version fencing** (optimistic read guard against oracle mutation mid-comparison).

### 25.4 Naming note

If `driftwatch` is taken on pkg.go.dev or feels wrong, alternatives that keep the same framing: `skew`, `driftguard`, `reconcile-watch`, `oracled`, `statediff`. Pick one in Phase 0 and never change it — a rename mid-project scatters the git history and breaks every import path.

### 25.5 First commands to run

```bash
mkdir driftwatch && cd driftwatch
git init
go mod init github.com/<you>/driftwatch
# then Phase 0, §20
```

---

**End of PRD.**

Implementation begins at Phase 0. Read §1 (working agreement), §5 (correctness), and §23 (anti-patterns) before writing any code.
