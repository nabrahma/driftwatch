# Bounded-resource audit

PRD §19.2 requires that every collection in driftwatch has an explicit cap, a
metric, and a test proving the cap holds — and states the rule plainly: **there
must be no unbounded collection anywhere in the codebase.**

This document is the audit. It was performed by reading every `make(map`,
`make(chan`, `make([]` and `append(` in non-test code and classifying each.

**Result: no unbounded collection found.** Every long-lived collection is
bounded by a configured cap; every other allocation is provably short-lived.

Audited at the Phase 9 commit. Reproduce the inventory with:

```sh
grep -rn 'make(map\|make(chan\|make(\[\]\|append(' --include='*.go' \
  pkg/ internal/ api/ cmd/ | grep -v _test.go | wc -l
```

## What was audited

| Construct | Sites (non-test) |
|---|---|
| `make(map` | 32 |
| map literals | 79 |
| `make([]` | 87 |
| `append(` | 137 |
| `make(chan` | 17 |

Counting sites is not the audit. The question for each is only ever: **can this
grow without a bound as the process runs?** That splits every site into two
groups, and only the first needs a cap.

## Long-lived collections, and what bounds each

A collection is long-lived if it is a struct field or package variable that
survives the call that filled it. These are the ones that can leak.

| Collection | Where | Bound | Enforced by |
|---|---|---|---|
| Oracle key map | `pkg/oracle/shard.go` | `policy.maxTrackedKeys` | Eviction on insert; `driftwatch_oracle_evictions_total` |
| Per-key event ring | `pkg/oracle/ring.go` | `policy.ringSize` | Fixed-size ring, overwrites oldest |
| Members per key | `pkg/projection/keyset.go` | `policy.maxMembersPerKey` | Key marked truncated past the cap |
| Publisher sequence state | `pkg/seqtrack/seqtrack.go` | `policy.maxPublishers` | Oldest publisher evicted |
| Gap intervals per publisher | `pkg/seqtrack/gapset.go` | `maxGapIntervals` | Coalesced; set marked truncated |
| Confirmation queue | `pkg/sweeper/confirm.go` | `policy.maxConfirmQueue` | Candidate dropped; `driftwatch_confirm_queue_dropped_total` |
| Confirmed episodes | `pkg/sweeper/sweeper.go` | `policy.maxConfirmQueue` | Cannot exceed what the queue admitted |
| Extras tracked | `pkg/sweeper/extras.go` | `policy.maxExtrasTracked` | First pass stops at the cap |
| Findings per report | `pkg/differ/report.go` | `policy.maxFindings` | `Report.Truncated` set; counts stay complete |
| Lag observations | `pkg/lag/estimator.go` | `WindowSize` | Ring buffer |
| Publisher label values | `pkg/metrics/metrics.go` | `maxPublisherLabels` | Collapsed into `__other__` |
| Log sampler reasons | `pkg/logging/logging.go` | `maxReasons` | Oldest reason evicted |
| Raw ingest channel | `pkg/check/check.go` | `policy.ingestBufferSize` | Buffered channel; overflow counted as `buffer_full` |
| Decoded event channel | `pkg/check/check.go` | `ingestBufferSize / 2` | Buffered channel |
| Reorder buffer | `pkg/check/reorder.go` | `defaultMaxHeldPerPublisher` (1024) | Drains on fill |
| Clock skew per publisher | `pkg/check/check.go` | `policy.maxPublishers` | Explicit length check before insert |
| Runner registry | `internal/controller/runner.go` | Number of DriftChecks | `delete` on stop; per-key locks refcounted and dropped |
| Event transition state | `internal/controller/events.go` | Number of DriftChecks | `delete` on finalize |

Three of these deserve a note, because each was a real defect before it was a
bound.

**`confirmedCats` in `pkg/check` is rebuilt, not accumulated.** It maps each
confirmed key to its category so that `publishEpisodes` can tell a new episode
from a continuing one. An earlier reading of the code suggested it only ever
grew; it does not — each sweep replaces the whole map with one built from
`Sweeper.Confirmed()`, which is itself bounded by the confirmation queue.

**The clock-skew map checks its own length before inserting.** A stream of
one-off publisher identities — which a misconfigured producer emits readily —
would otherwise grow it without limit. It is capped at `policy.maxPublishers`,
the same bound seqtrack uses, so the two cannot disagree about how many
publishers exist.

**The ingest channel is sized differently per source type.** §10.2 requires it
to exceed the socket high-water mark so that loss is countable rather than
invisible, which is right for a transport that can drop — and costs 12.8 MB per
check for one that cannot. `ingestBufferFor` gives zmq and nats the configured
size and caps everything else at 4,096. See D-016.

## Short-lived allocations

Everything else. These are bounded by the call that makes them and cannot
survive it:

- **Per-batch slices and maps in the sweeper.** `versions := make(map[string]uint64,
  s.cfg.ReadBatchSize)` is rebuilt per batch and bounded by `readBatchSize`.
- **Snapshot copies.** `Confirmed()`, `Publishers()`, `Status()` and the
  diagnostics helpers each build a fresh copy sized from the collection they
  read, hand it to the caller, and keep no reference.
- **Rendering buffers.** Every `strings.Builder`, every `append` building a log
  line, a metric label set or a YAML document.
- **Decoder scratch.** `pkg/codec` allocates per payload, bounded by
  `codec.maxPayloadBytes`, which is enforced at the boundary before any buffer
  is sized from it.

## Channels

Seventeen `make(chan` sites. Every one is either:

- **Buffered with a configured size** — the two ingest channels above.
- **Buffered with a fixed small size** — the source's gap-signal channel, sized
  1 and dropping rather than blocking, because the pipeline needs to learn that
  a gap happened rather than how many times.
- **Unbuffered and used for a single handoff** — `done`, `bootstrapped`, the
  errgroup's internal channels. An unbuffered channel holds nothing.

## The bound that is documented rather than enforced

`policy.maxTrackedKeys` bounds the oracle's *key count*, not its memory. Each
key's cost depends on how many events its ring holds, which is why the
measured figure is ~670 bytes per key with one event of history and roughly
16 KiB per key with sixteen. The bound holds — the oracle never exceeds
`maxTrackedKeys` and `TestProp_MemoryBounded` proves it — but sizing against
the key count alone will under-provision by more than an order of magnitude on
a workload that touches keys repeatedly.

This is `docs/KNOWN_GAPS.md` G-001, quantified in D-022, and it is the one place
where "bounded" and "small" are not the same statement.

## How this stays true

- `TestProp_MemoryBounded` (§5.8 I8) proves the oracle never exceeds its cap.
- `TestMetrics_CardinalityStaysBoundedUnderTenThousandKeys` proves the label
  budget holds at 10,000 keys and 500 publishers: 329 series against a 500
  budget.
- §15 rows 55–60 drive each bound to its limit and assert degradation rather
  than failure.
- `TestFault60_FiftyConcurrentChecksInOneManager` asserts a per-check memory
  ceiling, so the allocation D-016 removed cannot creep back.
