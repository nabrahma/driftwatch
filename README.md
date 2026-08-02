# driftwatch

> Detects when a datastore has silently stopped matching the event stream that built it.

[![CI](https://github.com/nabrahma/driftwatch/actions/workflows/ci.yaml/badge.svg)](https://github.com/nabrahma/driftwatch/actions/workflows/ci.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/nabrahma/driftwatch)](https://goreportcard.com/report/github.com/nabrahma/driftwatch)
[![Go Reference](https://pkg.go.dev/badge/github.com/nabrahma/driftwatch.svg)](https://pkg.go.dev/github.com/nabrahma/driftwatch)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

## The problem

A city library keeps one central catalogue saying where each book is. Branches
never edit it directly — when a book moves, the branch announces it over a
city-wide PA system, and a small program listens and updates the catalogue.

The PA system is a broadcast. Nobody confirms receipt. So if the listener is busy
for two seconds it misses announcements entirely, and they are simply gone. If
two announcements arrive out of order, the catalogue settles on the wrong answer.
Nothing breaks loudly: the catalogue still answers, the website still works,
every dashboard is green. Some percentage of the time a reader is quietly sent to
the wrong branch.

The same shape is everywhere in software. In LLM inference serving, model
replicas publish KV-cache block ownership over ZeroMQ, a materializer maintains a
Redis index of `block_hash → replica`, and a router reads that index to decide
where to send a request. When one message is dropped, the index is wrong in a way
no health check, error rate or queue-depth graph can see. The publisher
published. The subscriber acknowledged. Redis returned OK. The symptom is a
slightly worse cache hit rate and a slightly worse p99 — no error, no obvious
place to look, and weeks of debugging.

driftwatch subscribes to the same event stream, folds it independently into its
own answer, and periodically compares that answer against what the store actually
holds. When they disagree it names the key, the category, and the sequence number
of the event to go looking for.

## What driftwatch does

- **Keeps an independent oracle.** It subscribes to the same stream as the
  materializer and folds events through a pure projection function. It shares no
  memory, no connection and no code path with the thing it is checking.
- **Compares carefully.** Every sweep reads the store, diffs it against the
  oracle, and re-checks each disagreement after a settlement window before
  reporting it. A disagreement that heals was propagation lag, not drift.
- **Explains one key.** `driftwatch explain --key <k>` prints the oracle's value,
  the store's value, every event that produced the oracle's value with sequence
  numbers, and a ranked diagnosis of what most likely went wrong.

It never writes to the target store. Read-only is a feature, not a limitation: a
detector that can also mutate is a detector nobody deploys.

## Quick start

The demo brings up a publisher, a materializer, Redis, driftwatch, Prometheus and
Grafana — then breaks one of them on request:

```sh
git clone https://github.com/nabrahma/driftwatch && cd driftwatch
make demo                   # six containers; ~60s for the oracle to fill
make demo-inject-drift      # deletes ~400 keys from Redis behind driftwatch's back
open http://localhost:3000  # Grafana, on the driftwatch dashboard
make demo-down
```

A captured run is in
[docs/evidence/demo-drift-detected-and-resolved.txt](docs/evidence/demo-drift-detected-and-resolved.txt):
392 keys deleted, 360 confirmed divergent within 7 seconds, decaying back to zero
over the following three minutes as the stream naturally rewrote them. Coverage
never dropped below 0.997.

A real check is a Kubernetes object:

```yaml
apiVersion: driftwatch.io/v1alpha1
kind: DriftCheck
metadata:
  name: kvcache
  namespace: inference
spec:
  source:
    type: zmq
    zmq:
      endpoints: ["tcp://vllm-0.vllm.inference.svc.cluster.local:5557"]
  projection:
    type: keysetOwnership
    keyTemplate: "block:{{.Key}}"
  target:
    type: redis
    redis:
      addr: redis.inference.svc.cluster.local:6379
      keyPattern: "block:*"
```

Everything else has a default, and the defaulting webhook writes them back to the
object — so `kubectl get driftcheck kvcache -o yaml` shows the whole effective
configuration rather than the eight lines you typed.

> **Use fully-qualified service names.** A DriftCheck's endpoints are resolved by
> the manager pod, not from the object's own namespace, and getting this wrong
> fails as a DNS timeout rather than as a configuration error. See
> [D-024](docs/DISCOVERIES.md#d-024--a-driftchecks-endpoints-resolve-from-the-manager-not-from-itself).

## Example output

`driftwatch explain` against a real Redis, on a key the materializer dropped. Full
capture with reproduction steps:
[docs/evidence/explain-dropped-event.txt](docs/evidence/explain-dropped-event.txt).

```text
KEY  block:9f3a2c1e
──────────────────────────────────────────────────────────────────────
VERDICT   DIVERGED                                    settled 2.1s ago

ORACLE    set(2)[replica-0 replica-2]        version 2   trust complete
TARGET    absent                             read just now
DIFF      missing in target: replica-0, replica-2

DIAGNOSIS
  [high]   Target is missing this key. driftwatch observed a complete event sequence for publisher
           replica-0 (seq 8801..8847, no gaps), so the materializer most likely dropped or failed
           to apply seq 8847.
           - oracle expects set(2)[replica-0 replica-2] at version 2
           - target holds nothing, read just now
           - 2 events for this key in the retained history
           - the store reports 0 evictions in total, so eviction is unlikely

HISTORY (last 2)
  #0   8801     replica-0   add      replica-0 → {replica-0}            v1    -2.1s
  #1   8847     replica-0   add      replica-2 → {replica-0,replica-2}  v2    -2.1s

PUBLISHERS
  replica-0     epoch 1    hwm 8847      gaps 0        last seen 2.1s ago
  replica-1     epoch 1    hwm 2         gaps 0        last seen 2.1s ago
```

The load-bearing line is the diagnosis. driftwatch does not say the key is missing
and stop there. It says it observed replica-0's sequence from 8801 to 8847 with no
gaps — so its *own* view was complete, which is what makes the target the party
that lost something, and names seq 8847 as the event to search the materializer's
logs for.

## How it avoids false positives

Naively diffing an event-derived oracle against a store produces an avalanche of
false positives. driftwatch is always slightly ahead of the real materializer; a
keyspace scan is a smear across time rather than a snapshot; the oracle keeps
moving while that scan runs; and driftwatch's own subscription drops events too —
so when the two disagree it is not automatically the store that is wrong.

The arithmetic is unforgiving. At 10,000 keys, 2,000 events/sec and a p99
propagation of 400ms, roughly 800 keys are legitimately in flight at any instant:
**an 8% false-positive rate per sweep**, which is enough noise to get the tool
muted within a week. Six mechanisms prevent it:

1. **A settlement window.** A key is eligible for comparison only after it has
   been quiet for W, and W is derived from a measured p99 of propagation lag
   rather than configured by guess.
2. **Two-phase confirmation.** A disagreement is only a *candidate*. It is re-read
   after a delay and reported only if it persists.
3. **Version fencing.** Each confirmation carries the oracle version it was raised
   against. If a new event for that key arrives first, the finding is withdrawn
   rather than reported ([D-009](docs/DISCOVERIES.md)).
4. **Sequence-gap trust states.** If driftwatch misses events, its own answer is
   incomplete. Affected keys drop from `complete` to `partial` trust, and
   disagreements on them are reported as **suspect** — never as confirmed drift.
5. **Eviction and expiry awareness.** A key the store evicted under memory
   pressure is not a materializer bug, and is categorized separately.
6. **Bootstrap honesty.** Until the oracle has run long enough to have a
   defensible answer it reports a coverage ratio below 1 and declines to confirm.

The uncomfortable consequence of mechanism 4 is that driftwatch's own losses must
surface as its own uncertainty rather than as the store's fault. That is tested
directly: e2e scenario E3 severs driftwatch's subscription with a toxiproxy fault
and asserts `suspectDivergentKeys > 0` while `divergentKeys == 0`. Getting those
two the wrong way round is the single most damaging thing this tool could do.

[docs/CORRECTNESS.md](docs/CORRECTNESS.md) works through the arithmetic, the eight
failure modes each mechanism addresses, the fourteen invariants held under
property test, and — importantly — what stays undetectable.

## Architecture

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

End to end, an event takes ten steps:

1. The **source** reads a frame off the wire, recording a local receipt time.
2. The **codec** decodes it into an `Event` — publisher, sequence, epoch, key, op, payload.
3. The **sequence tracker** records seq/epoch, detecting gaps and publisher restarts.
4. The **reorder buffer** briefly holds out-of-order events, so a late arrival is not a gap.
5. The **projection** folds the event into a new value for its key. Pure; no I/O.
6. The **oracle** stores it, bumps that key's version, appends to its event ring, and sets trust from the tracker's gap state.
7. The **sweeper** selects keys settled for at least W and reads them from the target in batches.
8. The **differ** categorizes each disagreement: missing, extra, member mismatch, version skew, evicted, expired.
9. **Confirmation** re-reads each candidate after a delay, fenced against the oracle version it was raised at.
10. The **reporter** writes metrics, `DriftCheck.status`, and Kubernetes Events.

Package boundaries and why they fall where they do:
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Metrics and dashboard

Forty-eight metrics. These eight are the ones to alert on.

| Metric | Type | What it tells you |
|---|---|---|
| `driftwatch_coverage_ratio` | gauge | Fraction of the keyspace driftwatch can currently answer for. **Read this before anything else** — every number below is scoped to it. |
| `driftwatch_divergent_keys` | gauge | Keys confirmed to disagree, with driftwatch's own view complete. |
| `driftwatch_suspect_divergent_keys` | gauge | Keys that disagree where driftwatch missed events and cannot attribute the fault. |
| `driftwatch_seq_missing_events` | gauge | Events driftwatch knows it did not see, per publisher. |
| `driftwatch_events_dropped_total` | counter | Events dropped inside driftwatch, by reason. |
| `driftwatch_sweep_duration_seconds` | histogram | Sweep latency. A p99 approaching the sweep interval means sweeps are overlapping. |
| `driftwatch_convergence_seconds` | histogram | How long a divergence took to heal. Its p99 is what the settlement window must exceed. |
| `driftwatch_target_reachable` | gauge | Whether the store answered at all. Zero here invalidates everything above. |

The Grafana dashboard in [deploy/grafana/](deploy/grafana/) has five rows and 18
panels. Row 1 leads with coverage, for the reason in the table. Row 3 overlays the
settlement window on the convergence p99, so you can see at a glance whether W is
still wide enough for the workload. Ten PrometheusRule alerts ship with the chart
in [deploy/helm/driftwatch/templates/prometheusrule.yaml](deploy/helm/driftwatch/templates/prometheusrule.yaml).

`hack/dashboardcheck` runs in CI and fails if any panel references a metric that
is not registered — a dashboard that has silently stopped showing data is the
same class of failure driftwatch exists to catch.

Full reference: [docs/METRICS.md](docs/METRICS.md). Runbook:
[docs/OPERATIONS.md](docs/OPERATIONS.md).

## Configuration

```yaml
spec:
  source:                          # zmq | nats | file | synthetic
    type: zmq
    zmq:
      endpoints: [...]             # subscribe-before-connect; DNS re-resolved per attempt
      topics: ["kv-events"]        # NOTE: ZMQ subscription is a prefix match
  codec: json
  projection:
    type: keysetOwnership          # keysetOwnership | scalarLatest | counterSum
    keyTemplate: "block:{{.Key}}"  # the oracle key, which may differ from the event key
  target:
    type: redis
    redis:
      addr: redis.inference.svc.cluster.local:6379
      keyPattern: "block:*"
      readOnly: true               # refuses any command outside the allowlist
  settlement:
    mode: adaptive                 # adaptive | fixed
    window: 5s                     # a floor when adaptive; exact when fixed
  sweep:
    interval: 30s
    readBatchSize: 500
  limits:
    maxTrackedKeys: 1000000        # see the memory caveat under Limitations
```

Every field, its default and the reason for that default lives on the type itself
in [api/v1alpha1/driftcheck_types.go](api/v1alpha1/driftcheck_types.go), and those
comments are generated into the CRD — so `kubectl explain driftcheck.spec.sweep`
is the reference, and it cannot drift from the code. A fuller sample with every
field spelled out is in
[config/samples/](config/samples/driftwatch.io_v1alpha1_driftcheck.yaml).

## Measured performance

AMD Ryzen 7 6800HS, windows/amd64, go1.26.4. Raw output in
[docs/benchmarks/](docs/benchmarks/); reproduce with `make bench`.

| Operation | Result | Allocations |
|---|---|---|
| JSON event decode | 715 ns/op (178 MB/s) | 2 |
| JSON event decode, parallel | 92 ns/op | 2 |
| Sequence tracker observe | 28 ns/op | 0 |
| Projection apply (keyset) | 346 ns/op | 2 |
| Projection apply (counter) | 23 ns/op | 0 |
| Oracle apply | 360 ns/op | 0 |
| Oracle get | 364 ns/op | 2 |
| Mark all suspect, 1M keys | 1.03 µs | 0 |
| Iterate settled keys, 1M | 44 ms | — |
| Redis batched read (500) | 23.8 µs/key | 19/key |
| Full keyspace scan, 1M keys | 983 ms | bounded window, not the keyspace |
| **Full sweep, 1M keys, real Redis** | **5.68 s** | 9/key |
| Oracle memory, 1M keys | 656 MiB | — |

Two of those are success criteria. A full sweep of a million keys in **5.68 s**
against the 10 s bar is [S6](docs/evidence/S6-sweep-1m-keys.txt). Marking a
million keys suspect in **1.03 µs** is why the oracle uses a generation counter
rather than touching every key.

One is a miss: 656 MiB per million keys against a 512 MiB budget. It is recorded
as [G-001](docs/KNOWN_GAPS.md) rather than quietly rounded down.

Under the 60-minute soak — 3 publishers, 1,500 events/sec, 150,000 distinct keys,
real Redis 7.4, real materializer — driftwatch **applied 5,388,510 events and
dropped 0**. Goroutines held at 13 for the entire hour. RSS grew 3.9% between the
27-minute mark, where the per-key event rings finish filling, and the end. A fault
injected at t=31m was detected in that same sample and resolved by t=32m. Capture:
[docs/evidence/S2-soak-60min-zero-drift.txt](docs/evidence/S2-soak-60min-zero-drift.txt).

## Key discoveries

Twenty-four findings are recorded in [docs/DISCOVERIES.md](docs/DISCOVERIES.md),
each with a reproduction, an evidence file and a regression test. These are the
ones that changed the design.

**A JSON sequence number above 2^53 is silently corrupted.** Any decoder that
routes through `float64` — which is what Go's `encoding/json` does for an
`interface{}` — rounds a 64-bit sequence number and does not error. A publisher
using nanosecond timestamps as sequence numbers crosses 2^53 immediately, and the
corruption then looks like a reordering bug rather than a decoding bug. driftwatch
decodes sequence numbers through `json.Number`. Because the rounded value is still
*plausible*, no test using small sequence numbers would ever have caught it.
[D-002](docs/DISCOVERIES.md) · [evidence](docs/evidence/D-002-json-float64-seq.txt)

**Discarding timed-out probes shrinks the settlement window 12x, exactly during an
outage.** The lag estimator discarded probes that timed out, on the reasoning that
a timeout is not a measurement. But timeouts are not randomly distributed: they
cluster precisely when propagation is slowest. Dropping them makes the surviving
sample look fast, which narrows W — so the settlement window tightens at the
moment it most needs to widen, and driftwatch emits a flood of false positives in
the middle of someone else's incident. Timed-out probes are now recorded at the
timeout value. [D-008](docs/DISCOVERIES.md) ·
[evidence](docs/evidence/D-008-timeout-bias.txt)

**A confirmed finding is a claim about one oracle version, and nothing was
withdrawing it.** Confirmation re-reads a candidate after a delay. If a new event
for that key arrived in between, the original finding was made against a version
that no longer exists — and it was still being reported. On any workload that
rewrites keys, this produces drift reports for keys that are entirely correct.
Findings now carry the version they were raised at and are withdrawn if it moves.
[D-009](docs/DISCOVERIES.md) · [evidence](docs/evidence/D-009-superseded-finding.txt)

**Caching the first DNS resolution turns a pod reschedule into permanent
silence.** The ZMQ source resolved its endpoint once and reconnected to that
cached IP forever. When the publisher pod is rescheduled it gets a new IP; the
source then reconnects successfully to nothing, reports itself healthy, and
driftwatch goes quiet without a single error. Every reconnection attempt now
re-resolves. [D-011](docs/DISCOVERIES.md) ·
[evidence](docs/evidence/D-011-dns-reresolution.txt)

**Fifty idle checks held 640 MB, essentially all of it an empty channel.** §10.2
requires the ingest buffer to exceed the socket high-water mark so that loss is
countable rather than invisible. That is right for a transport that can drop, and
costs 12.8 MB per check for one that cannot. The buffer is now sized per source
type. [D-016](docs/DISCOVERIES.md) ·
[evidence](docs/evidence/D-016-idle-check-memory.txt)

**Three metrics were declared, documented, exported — and never written.** They
appeared in `/metrics` at zero, which is indistinguishable from "nothing is
happening" and strictly worse than being absent, because an alert built on one
would never fire. A CI check now asserts every registered metric has a write site.
[D-015](docs/DISCOVERIES.md) ·
[evidence](docs/evidence/D-015-declared-and-unwritten-metrics.txt)

**The oracle's memory does not level off when the key count does.**
`maxTrackedKeys` bounds the *count*, not the bytes: each key's cost depends on how
full its event ring is, and rings keep filling long after the last new key
arrives. A soak that looked like a leak for 27 minutes was a ring buffer reaching
steady state. Sizing against key count under-provisions by more than an order of
magnitude. This is a shipped limitation, not a fixed bug.
[D-022](docs/DISCOVERIES.md) · [G-001](docs/KNOWN_GAPS.md)

**A soak that "detected nothing" was injecting a fault that changed nothing.** The
fault skipped an `SADD`. `SADD` is idempotent and the workload cycles the same
members, so the next pass rewrote the member and the store was never actually
wrong. The test was correct; the fault was a no-op. A negative result from a fault
injector is worth nothing until you have proven the injector does damage.
[D-021](docs/DISCOVERIES.md)

**A `FLUSHDB` mid-`SCAN` does not loop forever; it does something quieter and
worse.** The cursor does not spin — it completes, having returned a fraction of
the keyspace, with no error and no signal that anything was missed. A checker that
trusts that scan concludes the store is missing everything it did not see.
[D-006](docs/DISCOVERIES.md) · [evidence](docs/evidence/D-006-scan-flushdb.txt)

**Enforcing a global key budget per shard silently loses ~0.3% of the capacity you
configured.** Hash imbalance across 64 shards evicts keys from over-subscribed
shards while others sit idle. The cost is real but small; the expensive part is
that the natural benchmark asserts a million in, a million tracked — and fails,
reading as an eviction bug. That prediction was tested a second time when
`BenchmarkFullSweep1M` was written months later and walked straight into it.
[D-003](docs/DISCOVERIES.md) · [evidence](docs/evidence/D-003-shard-budget-imbalance.txt)

## Testing

| Level | Location | Runtime | Deps |
|---|---|---|---|
| Unit | `pkg/*/*_test.go` | < 10s | none |
| Property | `pkg/*/*_property_test.go` | < 60s | rapid |
| Fuzz | `pkg/codec/fuzz_test.go` | 60s in CI | none |
| Integration | `pkg/target/*_integration_test.go` | < 90s | Docker |
| Fault | `test/faults/` | < 120s | none (fake clock, in-process) |
| Controller | `internal/controller/` | < 90s | envtest |
| E2E | `test/e2e/` | < 8min | Kind + Docker |
| Soak | `test/soak/` | 60min nightly | Docker |
| Interop | `test/interop/` | < 60s | Python + libzmq |
| Benchmark | `*_bench_test.go` | < 120s | none |

**675 unit tests, 49 property tests at 10,000 cases each, 60 fault scenarios, 8
Kind-based e2e scenarios, a 60-minute soak, and a ZMQ interop test against real
libzmq in both directions.** `go test ./...` runs the unit, property and fault
levels and finishes in under three minutes; everything heavier sits behind a build
tag, because a slow default test command stops being run.

Two scripted checks exist because the alternative was worse:

- `hack/verify-no-sleep.sh` — no `time.Sleep` may be used for synchronization in
  any test. Tests that wait, poll on the condition they are waiting for.
- `hack/verify-no-superlatives.sh` — a word list banned repository-wide. Each of
  those words is a claim with no number behind it, and a reader who notices one
  stops trusting the numbers that *are* there.

The fault matrix ran 20 consecutive times with zero flakes:
[docs/evidence/fault-matrix-20-runs-no-flake.txt](docs/evidence/fault-matrix-20-runs-no-flake.txt).
More in [docs/TESTING.md](docs/TESTING.md);
[docs/evidence/](docs/evidence/README.md) indexes every captured artifact.

## Limitations

Deliberate scope boundaries, then the things that are genuinely missing. Full list
with reproductions in [docs/KNOWN_GAPS.md](docs/KNOWN_GAPS.md) and
[docs/CORRECTNESS.md](docs/CORRECTNESS.md).

**By design:**

- **No repair.** driftwatch never writes to the target. Auto-repair needs domain
  knowledge it does not have.
- **Not a replacement for the materializer.** It observes alongside the consumer
  that writes to the target; it does not become it.
- **It cannot fix a lossy channel**, only detect and quantify the loss.
- **One check runs in one process.** Checks spread across replicas via per-check
  leader election, but a single keyspace is not sharded.
- **Kafka is out of scope.** Consumer groups and offsets make it a materially
  different and easier problem.

**Currently:**

- **Memory is bounded by key count, not by bytes** — ~656 MiB per million keys
  with one event of history, and roughly 16 KiB per key with a full ring. Size
  against your ring depth, not your key count. ([G-001](docs/KNOWN_GAPS.md))
- **Reordering at the very start of a publisher's stream is undetectable.** With
  no prior high-water mark there is nothing to compare against.
  ([G-002](docs/KNOWN_GAPS.md))
- **Redis sentinel and cluster modes are routed but not tested.** They go through
  `NewUniversalClient` without a dedicated integration test. Standalone is tested
  against both Redis 6 and 7.
- **A deterministic, total materializer bug is invisible.** If it applies every
  event to the wrong key consistently, oracle and store disagree everywhere, which
  reads as a configuration error — correctly, but it cannot tell you which.
- **driftwatch cannot check itself.** If its own subscription is lossy it says so —
  coverage drops and findings become suspect — but it cannot distinguish "the
  store is wrong" from "I missed the event that would have made it right." That
  distinction is the entire reason the suspect category exists.

## Contributing

`make verify` runs everything CI runs. Read
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) first, and see
[CONTRIBUTING.md](CONTRIBUTING.md) — the working agreement there is binding, not
advisory.

If you find something that surprised you, add an entry to
[docs/DISCOVERIES.md](docs/DISCOVERIES.md). That file is the most useful thing in
this repository.

## License

Apache 2.0. See [LICENSE](LICENSE).
