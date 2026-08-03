# Evidence

Captured output backing every claim made in the README and in
[DISCOVERIES.md](../DISCOVERIES.md).

One file per claim, named `<id>-<slug>.<ext>`. Nothing is reconstructed after the
fact; output is captured when it is produced (PRD §21.4). If a claim in the README
has no row here, the claim comes out.

Every row below names the command that produced it, so a reader can re-run it
rather than take the file's word for it.

## Discoveries

Each of these is the captured output for one entry in
[DISCOVERIES.md](../DISCOVERIES.md).

| File | Claim it proves | Produced by |
|---|---|---|
| `D-001-rfc3339-lowercase.txt` | Go's `time.RFC3339` rejects timestamps RFC 3339 permits | `pkg/event` |
| `D-002-json-float64-seq.txt` | A sequence number above 2^53 is corrupted by any float64 decoder | `pkg/codec` |
| `D-003-shard-budget-imbalance.txt` | A per-shard key budget loses ~0.3% of the configured capacity | `pkg/oracle` |
| `D-004-client-handshake.txt` | A strict read-only allowlist refuses go-redis' own handshake | `pkg/target` |
| `D-005-info-sections.txt` | `INFO` with several sections works on Redis 7 and fails on Redis 6 | `pkg/target` |
| `D-006-scan-flushdb.txt` | A `FLUSHDB` mid-`SCAN` silently truncates the iteration | `pkg/target` |
| `D-007-getmany-alloc-floor.txt` | The allocation floor of a pipelined batch read | `pkg/target` |
| `D-008-timeout-bias.txt` | Discarding timed-out probes shrinks W 12x during an outage | `pkg/lag` |
| `D-009-superseded-finding.txt` | A confirmed finding is a claim about one oracle version | `pkg/sweeper` |
| `D-010-sub-hwm-noop.txt` | The pure-Go ZMQ binding accepts a SUB high-water mark and ignores it | `pkg/source` |
| `D-011-dns-reresolution.txt` | Caching the first DNS resolution breaks reconnection after a reschedule | `pkg/source` |
| `D-012-publisher-label-budget.txt` | §12's publisher label default and its cardinality budget are incompatible | `pkg/metrics` |
| `D-013-projection-key-template.txt` | A key template made the applier fetch the wrong previous value | `pkg/check` |
| `D-014-commutative-unconsumed.txt` | `Commutative()` was declared by every projection and read by nothing | `pkg/check` |
| `D-015-declared-and-unwritten-metrics.txt` | Three §12 metrics were exported and never written | `pkg/check` |
| `D-016-idle-check-memory.txt` | Fifty idle checks held 640 MB of empty channel | `pkg/check` |
| `D-024-namespace-resolution.txt` | A bare service name in a DriftCheck resolves from the manager's namespace | `test/e2e` |
| `D-025-silent-subscriber.txt` | A SUB socket whose publisher is replaced never reconnects, and driftwatch reports itself healthy while deaf | `pkg/source`, `test/e2e` |
| `D-026-stale-manager-image.txt` | `make e2e-reuse` applied an unchanged manifest and kept a 17-hour-old manager running | `test/e2e` |
| `D-027-workload-forbade-the-assertion.txt` | E3 asserted on a divergence its own idempotent workload made impossible to produce | `test/e2e` |
| `D-028-ticker-pacing-loses-rate.txt` | A per-event ticker published at 24% of its requested rate on a loaded two-core node | `test/e2e` |

Seven discoveries, D-017 through D-023, carry their reproduction inline in
[DISCOVERIES.md](../DISCOVERIES.md) rather than as a separate capture, because
each is a code-level finding whose evidence is a named regression test rather
than a terminal transcript. D-021 and D-022 are additionally visible in the soak
captures below.

## Success criteria

The numbered criteria from PRD §2.

| File | Claim it proves | Produced by |
|---|---|---|
| `S2-soak-60min-zero-drift.txt` | 60 minutes, 5,388,510 events, 0 dropped, 0 false positives in steady state | `make soak` |
| `S6-sweep-1m-keys.txt` | A full sweep of 1,000,000 real Redis keys in 5.68s, against a 10s bar | `make bench-sweep` |
| `S2-soak-capacity-500k-keys.txt` | Why the soak runs at 150k keys and not §16.7's 500k, the memory arithmetic | `make soak` |

### Soak profiles

Captured by `test/soak/soak_test.go` at the start, midpoint and end of the
60-minute run. They are the evidence for "no goroutine leak, no unbounded heap
growth", a claim that a table of RSS samples alone cannot settle.

| File | What it shows |
|---|---|
| `S2-soak-goroutine-start.pprof` | Goroutine profile at t=0 |
| `S2-soak-goroutine-middle.pprof` | Goroutine profile at t=30m, immediately after the injected fault |
| `S2-soak-goroutine-end.pprof` | Goroutine profile at t=60m, same 13 goroutines as t=0 |
| `S2-soak-heap-start.pprof` | Heap profile at t=0 |
| `S2-soak-heap-middle.pprof` | Heap profile at t=30m, while the per-key rings were still filling |
| `S2-soak-heap-end.pprof` | Heap profile at t=60m, after the rings reached steady state |

Read them with:

```sh
go tool pprof -top docs/evidence/S2-soak-goroutine-end.pprof
go tool pprof -base docs/evidence/S2-soak-heap-middle.pprof \
                    docs/evidence/S2-soak-heap-end.pprof
```

The `-base` comparison is the one that matters: it is what distinguishes D-022's
ring-buffer fill from an actual leak.

## Test levels

| File | Claim it proves | Produced by |
|---|---|---|
| `fault-matrix-60-rows.txt` | Every row of the §15 fault matrix has a passing named test | `make test-fault` |
| `fault-matrix-20-runs-no-flake.txt` | All 60 rows, 20 consecutive runs, zero flakes | `make test-fault-repeat` |
| `interop-libzmq-both-directions.txt` | driftwatch's pure-Go ZMQ is wire-compatible with real libzmq, both directions | `make test-interop` |
| `phase7-controller-suite.txt` | The controller suite against envtest | `make test-controller` |
| `phase7-crd-validation.txt` | The CRD rejects the invalid specs it is supposed to reject | `make test-controller` |
| `phase7-kubectl-explain.txt` | Every field's documentation reaches the CRD, so `kubectl explain` works | `make manifests` |

## End to end

| File | Claim it proves | Produced by |
|---|---|---|
| `explain-dropped-event.txt` | `driftwatch explain` names the event the materializer did not apply | `internal/cli` |
| `phase5-redis-demo.txt` | `driftwatch watch -f examples/local.yaml` against a real Redis | `internal/cli` |
| `phase7-live-check.txt` | A DriftCheck reconciled by the real manager, detecting real drift | `internal/controller` |
| `demo-drift-detected-and-resolved.txt` | `make demo` detects 360 deleted keys in 7s and watches them heal | `make demo` |
| `dashboard-drift-detected.png` | The dashboard mid-fault: 350 confirmed divergent keys at 100% coverage | `make demo` + `make demo-inject-drift` |

## What is deliberately not here

**The e2e diagnostics dumps** (`test/e2e/_artifacts/`). Regenerated on every
failing run, and they contain full container logs. D-024's load-bearing lines are
transcribed into `D-024-namespace-resolution.txt` with a reproduction that does
not need an e2e run at all.

**A demo GIF or asciinema cast.** The dashboard screenshot above covers the
static case. A recording would show the number rising and then decaying back to
zero on its own, which is the half that argues the tool is usable day to day,
and it is still outstanding.

Note that the screenshot is a supplement rather than the guarantee.
`hack/dashboardcheck` runs in CI and asserts every panel resolves to a
registered metric, which is the stronger check: an image goes stale silently
and the check does not.
