# Testing

driftwatch has ten test levels, and the interesting thing about the shape is
that **e2e is deliberately the smallest of them.**

That inversion is on purpose. A checker whose evidence comes mostly from a Kind
cluster is a checker whose failures take twenty minutes to reproduce and whose
bugs are found by someone else. The fault matrix, sixty named failure scenarios
driven through the entire pipeline in-process on a fake clock, runs in under two
minutes and is the specification of correctness under failure. e2e exists to
prove the wiring is real, not to find logic bugs.

## The levels

| Level | Location | Runtime | Deps | Command |
|---|---|---|---|---|
| Unit | `pkg/*/*_test.go` | < 10s | none | `make test-unit` |
| Property | `pkg/*/*_property_test.go` | < 60s | rapid | `make test-property` |
| Fuzz | `pkg/codec/fuzz_test.go` | 60s in CI | none | `make fuzz` |
| Integration | `pkg/target/*_integration_test.go` | < 90s | Docker | `make test-integration` |
| Fault | `test/faults/` | < 120s | none | `make test-fault` |
| Controller | `internal/controller/`, `api/` | < 90s | envtest | `make test-controller` |
| E2E | `test/e2e/` | < 8min | Kind + Docker | `make e2e` |
| Soak | `test/soak/` | 60min | Docker | `make soak` |
| Interop | `test/interop/` | < 60s | Python + libzmq | `make test-interop` |
| Benchmark | `*_bench_test.go` | < 120s | none | `make bench` |

`go test ./...` runs unit, property and fault, and finishes in under three
minutes. Everything heavier is behind a build tag, `integration`, `e2e`,
`interop`, `soak`, because a slow default test command stops being run, and a
test suite nobody runs is worse than none: it looks like coverage.

**675 unit tests, 49 property tests at 10,000 cases each, 60 fault scenarios, 8
Kind-based e2e scenarios, a 60-minute soak, and a ZMQ interop test against real
libzmq in both directions.**

## Time in tests

Every package that measures elapsed time takes a `clock.Clock`, and in tests it
takes a fake one. `hack/verify-no-sleep.sh` runs in CI and **fails the build on
any `time.Sleep` used for synchronization**, across 205 Go files. Only
`test/e2e` and `test/soak` are exempt, because there the thing being waited on is
a real cluster.

The reasoning is not stylistic. A sleep is a guess about how long something takes
on the machine you happened to write it on. It is too short on CI and too long
everywhere, and when it fails it fails intermittently, which costs more than
every other test problem combined. Anything worth sleeping for is worth asserting
on.

The replacements:

```go
// Waiting for something a goroutine will do:
require.Eventually(t, func() bool { return c.Status().EventsApplied == 6 },
    eventuallyFor, eventuallyPoll, "the applier never drained the source")

// Waiting for elapsed time to matter:
clk.Advance(window + time.Second)

// Waiting for a waiter to register before advancing past it:
clk.BlockUntil(3)
```

### The fake clock's sharp edge

Worth reading before writing a test that advances a clock past a ticker.

**A tick carries the deadline it was scheduled for, not the clock's new value.
A tick the consumer has not yet drained is dropped rather than queued**, exactly
like `time.Ticker`.

So a single `Advance(3s)` across a 1s ticker fires three ticks, delivers whichever
one or two the consuming goroutine happened to be ready for, and drops the rest.
If the only one delivered carries T+1s and the thing you are waiting for is not
due until T+2s, it never happens, and the test hangs on a condition that can no
longer become true.

Two ways out, both used here:

- **Size the interval so one tick suffices.** `pkg/sweeper/run_test.go` sets
  `ConfirmInterval` longer than W, so a single advance produces exactly one tick
  and that tick is already past the deadline.
- **Step one interval at a time.** `advanceUntil` in `pkg/check/check_test.go`
  advances by exactly one interval and re-checks, so a dropped tick is retried by
  the next step rather than lost.

Also: `Advance` guarantees the value is in the waiter's channel before it
returns. It does **not** guarantee the goroutine woken by that value has run.
Assert on something the woken goroutine writes, never on a variable it sets.

## Goroutine leaks

`goleak.VerifyTestMain` is in **every** package. A leaked goroutine in a process
designed to run for weeks is not a test-hygiene issue; it is the bug.

Ignores are permitted only for third-party goroutines started at package init,
and each one carries a comment saying which and why. The ones present:

- `go-redis`'s process-wide time cache and connection-pool reaper.
- `go-zeromq`'s connection reaper and per-connection readers.
- `klog`'s flush daemon.
- `go-winio`'s IO completion processor, Windows only, reached through
  testcontainers.

**No ignore is ever for one of driftwatch's own goroutines.** One of those
outliving a test is a bug to fix, not an entry to add.

## Property tests

49 of them, at 10,000 cases each, covering the §5.8 invariants. The pattern worth
copying is **differential testing against a naive reference**:

```go
func TestProp_OracleMatchesTheNaiveFold(t *testing.T) {
    rapid.Check(t, func(t *rapid.T) {
        events := genEvents(t)

        want := naiveFold(events)          // slow, obvious, in the test file
        got := applyAll(t, realOracle(), events)

        require.Equal(t, want, got)
    })
}
```

Write the naive version as slowly and stupidly as possible, then optimize the
real one freely. This is what makes the generation-counter suspicion mechanism
safe to have written at all: `BenchmarkMarkSuspectAll1M` marks a million keys
suspect in 1.03 µs by not touching any of them, and a property test proves it
means the same thing as the version that does.

## The fault matrix

`test/faults/` is sixty named scenarios, one per row of §15, every way the
system around driftwatch can misbehave, and what driftwatch must report when it
does. It is the closest thing here to a specification.

Two rules, both enforced in CI:

- **`hack/verify-fault-matrix.sh` fails if a row has no test named for it.** A
  row without a test is a claim without evidence.
- **No `t.Skip` anywhere in `test/faults/`.** A skipped row looks covered from
  the outside and is not.

The whole matrix ran 20 consecutive times with zero flakes:
[docs/evidence/fault-matrix-20-runs-no-flake.txt](evidence/fault-matrix-20-runs-no-flake.txt).

### Adding a row

1. Add it to §15's table in [DESIGN.md](DESIGN.md) with an ID and expected behaviour.
2. Write `TestFaultNN_ShortDescription` in the matching file under
   `test/faults/`.
3. Drive it through a real check with a fake clock, the harness in
   `test/faults/harness.go` builds one over a memory source and memory target.
4. Assert on the **observable**: the status, the metric, the condition. Not on
   internal state.
5. `make test-fault` must still finish inside 120 seconds.

## Coverage

§16.9's floors are enforced by `hack/verify-coverage.sh`, which runs in CI and
fails per package. It recomputes each package's figure from statement counts in
the profile rather than averaging per-function percentages. That would weight a
one-line getter the same as a ninety-line applier.

Current: **91.5% overall**, every package over its floor.

Coverage is a floor, not a goal. §16.9 says it outright: a 95%-covered package
with no property tests is worse than an 85%-covered one with them. Nothing here
rewards going above the floor, and nothing should be tested only to move the
number.

## Fuzzing

`FuzzDecodeJSON` asserts the one property that matters against untrusted input:
`Decode` never panics. A decoder that panics on a malformed frame takes down the
auditor, and does so at exactly the moment the system it is auditing is
misbehaving.

CI runs 60 seconds per push. The committed corpus in
`pkg/codec/testdata/fuzz/` is run as ordinary seeds by every `go test`, so those
six inputs are exercised constantly rather than only when someone remembers
`-fuzz`. Their provenance and selection criteria are in that directory's README.

A crash the fuzzer finds is written there by the toolchain and becomes a
permanent regression case. CI uploads it as an artifact, because "the fuzzer
found something" without the input is not a bug report.

## The e2e suite

Eight scenarios, one namespace each with a generated name, torn down after. The
two worth knowing about:

- **E2 (DroppedEventDetected)**: the materializer skips a sequence range.
  driftwatch's own view is complete, so it must report *confirmed* divergence.
- **E3 (SelfLossReportsSuspect)**: toxiproxy severs driftwatch's own
  subscription. The same underlying disagreement must come out as **suspect**,
  with `divergentKeys == 0`.

They are adjacent on purpose. Getting them the wrong way round is the single most
damaging thing this tool can do, and one test cannot catch it.

`test/e2e/diagnostics.go` writes nineteen files per failed scenario, the object,
its events, every container's log, Redis's INFO and DBSIZE, the metrics, the
pprof profiles, `driftwatch explain` for an affected key. It was written *second*,
before the second scenario existed, because a failing e2e test that dumps nothing
costs an hour per occurrence. Nothing in it may fail the test: every step records
its own error and continues.

Debugging flags:

```sh
make e2e-keep    # leave the cluster up after the run
make e2e-reuse   # reuse an existing cluster, for fast iteration
make e2e-break   # deliberately break a scenario, to see the diagnostics
```

## Interop

`test/interop/` runs driftwatch's pure-Go ZMQ against real libzmq in both
directions, Python publisher to Go subscriber, and Go publisher to Python
subscriber. It found the thing no unit test could: **ZMQ subscription is a prefix
match**, so subscribing to `kv-events` also receives `kv-events-debug`, and two
of three topics matching a prefix yields exactly two thirds of the events. That
looks like 33% packet loss and is not.

Synchronization between the two processes is a handshake, never a sleep, a
ready-file in one direction, because an earlier REQ/REP attempt put a second
socket pair between two processes that were already failing to talk over the
first.

## The soak

60 minutes, 3 publishers, 1,500 events/sec, 150,000 distinct keys, real Redis,
real materializer, with a fault injected at the halfway mark. It asserts no
goroutine growth, no unbounded heap growth, and that the injected fault is both
detected and resolved.

Two things it taught, both recorded rather than quietly worked around:

- **RSS climbs for the first 27 minutes and then flattens.** That is not a leak;
  it is the per-key event rings reaching steady state, which takes
  `ringSize × keys / rate`. `ringFillTime()` computes it, so a run shorter than
  that reports a leak that is not one ([D-022](DISCOVERIES.md)).
- **A fault that changes nothing proves nothing.** The first injector skipped an
  `SADD`; `SADD` is idempotent and the workload cycles the same members, so the
  store was never actually wrong. `requireFaultIsObservable()` now fails early
  rather than reporting a clean soak ([D-021](DISCOVERIES.md)).

## Running everything

```sh
make verify          # everything CI runs, locally
```

which is lint, vet, manifest verification, helm lint, the unit suite with
coverage floors, the repository's own rules (no sleeps, no superlatives, metrics
documented, fault matrix complete), the dashboard check, and the fault matrix.

The heavy levels are separate and each needs something: Docker for integration
and soak, envtest binaries for the controller suite, Kind for e2e, and
python3-zmq for interop.

### Which of them CI actually runs

A test level that exists and is never run is worth what an unrun test is worth,
so this is the honest mapping rather than the aspirational one.

| Level | Workflow | Job |
|---|---|---|
| Unit, property, race, coverage floors | `ci.yaml` | `unit` |
| Fault matrix | `ci.yaml` | `faults` |
| Fuzz, 60s | `ci.yaml` | `fuzz` |
| Controller, envtest | `ci.yaml` | `controller` |
| Benchmarks and the §16.8 gates | `ci.yaml` | `bench` |
| Integration, real Redis 6 and 7 | `e2e.yaml` | `integration` |
| End to end, Kind | `e2e.yaml` | `e2e` |
| Interop, real libzmq | `e2e.yaml` | `interop` |
| Soak, 60 minutes | not run in CI | by hand, `make soak` |

The soak is deliberate: an hour of runner time on every push buys less than
running it before a release does. It is the one level whose evidence can go
stale without CI noticing, so `docs/evidence/S2-soak-*` carries the date it was
captured and should be re-run when the pipeline changes.

The `bench` job enforces §16.8 in two parts. `hack/verify-benchmarks.sh` asserts
the absolute targets, which hold on any machine and back the numbers in the
README. `hack/verify-bench-regression.sh` runs benchstat against the committed
baseline and fails on any allocation increase, or on a time regression past 20%
that benchstat finds significant across six repetitions. Allocations are
enforced strictly because allocs/op is counted rather than timed and does not
move under load; times are read through a confidence interval because on a
shared runner they very much do.
