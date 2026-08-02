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

## D-026 — `make e2e-reuse` silently tested a seventeen-hour-old binary

**Found:** Phase 9, three e2e runs into fixing D-025, when a fix that was
definitely in the source kept not appearing in the cluster.

**What happened:** The suite's reuse path does this:

```go
kubectl apply -f manifests/manager.yaml
kubectl rollout status deployment/driftwatch-manager
```

and the apply is a no-op. The manifest never changes — the image tag is fixed
at `driftwatch/manager:e2e` and the pull policy is `Never` — so the Deployment
spec is byte-identical to what is already there, Kubernetes correctly decides
there is nothing to do, and the running pod stays exactly where it was.

`kind load docker-image` had put the newly built layers into the node's
containerd. A pod that is already running does not restart because of that.

The evidence was in the diagnostics all along, in the one file nobody reads:

```text
$ kubectl get pods -o wide          # 15-node.txt
NAMESPACE           NAME                                  AGE
driftwatch-system   driftwatch-manager-8b7f69798-hgsn9    17h
```

Seventeen hours, across four suite runs and three separate fixes.

**Why it matters:** `make e2e-reuse` is the documented fast-iteration path —
the one someone reaches for precisely when they are debugging. It presents a
stale binary's behaviour as the current one, and the failure mode is the most
expensive possible: **a fix that did not work.**

So the search moves on. The next hypothesis is wrong too, because it is also
tested against the old binary, and so is the one after that. Three of the
conclusions drawn earlier in this session — that E1's coverage bug had survived
its fix, that E2 was still undetected, that the sizing change to E5 had not
taken — were all artifacts of this and all wrong.

There is no signal anywhere that this is happening. The build succeeds, the
load succeeds, the rollout status succeeds because the existing pod is
healthy, and the suite runs green up to the assertion that was going to fail
anyway.

**Fix:** always `kubectl rollout restart` after loading the images, before
waiting on the rollout. It costs a few seconds on a path that already spends
minutes building, and it removes the category rather than the instance.

The general lesson is worth more than the fix: **a cache keyed on something
that does not change is a cache that never invalidates.** The tag was constant
by design, so that `imagePullPolicy: Never` would work; that same constancy is
what made the apply a no-op.

**Evidence:** `docs/evidence/D-026-stale-manager-image.txt`

**Regression test:** none that is worth having. Asserting the manager pod's
age or UID changed would test kubectl rather than driftwatch, and the fix is
one unconditional command whose absence is visible in the function. The
protection here is the comment at the call site, which says why the rollout is
not redundant.

---

## D-025 — A SUB socket whose publisher is replaced never reconnects, and driftwatch reports itself healthy while deaf

**Found:** Phase 9, running the e2e suite after the coverage work. E7
(PublisherRestart) failed on "the publisher restarted and nothing recorded it",
and the reason turned out to have nothing to do with restart detection.

**What happened:** E7 deletes the publisher pod and waits for the Deployment to
reschedule it. The replacement came up correctly — same identity `replica-0`, a
higher epoch, sequence restarting at 1:

```text
$ kubectl get pods
publisher-695946dd8b-9gfkx   1/1   Running   0   97s   10.244.0.12

$ kubectl logs publisher-695946dd8b-9gfkx
publishing as replica-0 epoch 1785611184 on tcp://0.0.0.0:5557 at 800/s
```

driftwatch's status, at the same moment:

```yaml
publishers:
- id: replica-0
  epoch: 1785611178        # the OLD incarnation
  highWaterMark: 3916
  lastSeenSeconds: "92.544"
```

Ninety-two seconds with no events, from a publisher that was emitting eight
hundred a second. And the manager log had nothing to say about it: one
reconnect at startup, then silence. No error, no retry, no re-resolution.

The cause is in the receive loop. driftwatch's ZMQ session ends when `Recv`
returns an error, and **a SUB socket whose peer disappears does not return an
error — it blocks, waiting for a peer that is never coming back.** So the
session never ended, `Run` never retried, and D-011's per-attempt DNS
re-resolution never got a chance to run, because there was never another
attempt.

**Why it matters:** this is the failure this entire project exists to make
visible, occurring inside the tool.

A deaf driftwatch does not look broken. It looks *clean*. The oracle stops
changing, the store stops being written to by anything driftwatch can see, and
the two frozen answers agree perfectly:

- `driftwatch_divergent_keys` — 0
- `driftwatch_coverage_ratio` — 1.0
- `driftwatch_target_reachable` — 1
- `SourceConnected` — True
- phase — `Watching`

Every alert in §12.2 stays silent. An operator looking at the dashboard sees a
system with no drift in it, and every panel is green, and the tool has not
observed anything for an hour. The one signal that would have shown it —
`lastSeenSeconds` climbing — is a status field nobody watches.

And the trigger is not exotic. Any pod reschedule does it: a node drain, a
rolling update, an OOM kill, an eviction. On a busy cluster this is a
weekly event.

**Fix:** an idle deadline on the receive loop. If no frame arrives within
`source.zmq.idleTimeout` (default 60s), the session ends with `ErrIdle`, and
`Run` treats it as any other failed session — back off, re-resolve, reconnect,
and signal possible loss so the affected keys become suspect.

The default is on, because the failure it prevents is silent. A stream that is
legitimately quieter than sixty seconds should raise the value rather than
disable it: firing early costs one reconnect and a round of suspicion that the
next event clears, and firing late costs silence.

The deliberate consequence is that a genuinely idle publisher now produces
periodic reconnects and suspicion. That is the right trade — §5.2's suspicion
decays per key as events arrive, and the alternative is a tool that goes deaf
without saying so.

Worth noting what did *not* catch this. The unit tests drive reconnection by
making `Recv` return an error, which is the case that already worked. goleak
saw nothing, because no goroutine leaked — one was parked forever, which is
different. The 60-minute soak passed, because nothing in it replaces the
publisher. It took an e2e scenario that deletes a pod, and it presented as an
unrelated assertion about a metric.

**Evidence:** `docs/evidence/D-025-silent-subscriber.txt`

**Regression test:**
`pkg/source: TestZMQ_ASilentSocketEndsTheSessionRatherThanBlockingForever`, and
`pkg/check: TestCheck_TheIdleTimeoutReachesTheSource` for the four-hop config
path.

**Status:** the diagnosis above is measured and the fix is unit-tested, but it
has **not** yet been confirmed end to end — E7 still fails, and the deadline
did not visibly fire in the cluster. See `docs/KNOWN_GAPS.md` G-003 before
relying on this being closed.

---

## D-024 — A DriftCheck's endpoints resolve from the manager, not from itself

**Found:** Phase 8, the first full run of the e2e suite — all eight scenarios
failing identically.

**What happened:** Every scenario timed out waiting for its check to leave
Bootstrapping. Redis held 3,000 keys, the publisher had emitted tens of
thousands of events, and driftwatch had applied zero.

The diagnostics dump answered it in two lines:

```text
redis: connection pool: failed to dial after 2 attempts:
  dial tcp: lookup redis: i/o timeout
the source may have missed messages; ... "detail": "resolving publisher:
  lookup publisher on 10.96.0.10:53: server misbehaving"
```

The DriftCheck said `addr: redis:6379` and
`endpoints: ["tcp://publisher:5557"]`. Both services exist — in the scenario's
own namespace. The manager runs in `driftwatch-system`, and a bare service name
resolves through the *resolving pod's* search domain, so the manager was looking
for a Service called `redis` in `driftwatch-system` and correctly not finding
one.

**Why it matters:** this is not a test bug that happens to look like a product
one. It is a trap sitting in the deployment `config/default` ships.

The manager is cluster-scoped by default: one operator in `driftwatch-system`
reconciling DriftChecks in every namespace. An operator writing a DriftCheck in
their own namespace will naturally write `addr: redis:6379`, because that is
what every other manifest in that namespace says and what works from every pod
in it. It will not work, and the way it fails is the problem — not a clean
"service not found" but a DNS timeout, so the check sits in Bootstrapping
retrying a scan against a Redis that is up and healthy one namespace away. The
status says `Bootstrapping`, `targetReachable: false`, and nothing anywhere says
"you wrote a name I cannot resolve from where I am".

Eight scenarios failing at once is what made this cheap to find. One would have
looked like a scenario bug.

**Fix:** The suite qualifies every endpoint the manager consumes —
`redis.<namespace>.svc.cluster.local:6379` — and the fixture exposes them as
methods rather than constants so the namespace is impossible to omit. The short
forms are kept, separately named, for the materializer and the throwaway curl
pod, which really are in the namespace.

Nothing changed in driftwatch itself, and that is a decision rather than an
oversight: resolving a bare name against the *object's* namespace instead of the
process's would mean driftwatch second-guessing DNS, and would break the
legitimate case of a check pointing at a store outside the cluster entirely. The
right fix is documentation and, eventually, a warning condition when a
single-label host fails to resolve. `docs/OPERATIONS.md` and the sample manifests
already use fully-qualified names; this makes the reason explicit.

**Evidence:** `docs/evidence/D-024-namespace-resolution.txt` — the load-bearing
lines from the failing run's diagnostics dump (the manager log, and
`07-redis-dbsize.txt` showing 3,000 keys present while `01-driftcheck.yaml`
showed `trackedKeys: 0`), plus a four-command reproduction that needs no e2e
run. The dump itself lives under `test/e2e/_artifacts/`, which is gitignored
because it is regenerated on every failing run and holds container logs.

**Regression test:** All eight e2e scenarios. Any of them reverting to a bare
service name fails within ninety seconds.

---

## D-023 — pure-Go zmq4 is wire compatible with libzmq, and the slow joiner is real

**Found:** Phase 8, writing the §16.6 interop test.

**What happened:** ADR-0001 chose `github.com/go-zeromq/zmq4`, a pure-Go ZMTP
implementation, over a cgo binding to libzmq. That buys static binaries,
cross-compilation and a distroless image, and it costs a guarantee: wire
compatibility with the libzmq publishers driftwatch will actually be pointed at
becomes a claim rather than something the linker enforces.

Measured against real libzmq 4.3.5 through pyzmq 27.1.0, over TCP loopback, the
claim holds in both directions:

```text
libzmq PUB -> Go SUB   6,667 of 10,000 delivered (correct: prefix filtering)
Go PUB -> libzmq SUB   5,000 of 5,000 delivered, contiguous
binary payloads        byte-identical in both directions
framing                topic-then-payload and single-frame both parsed
```

The interesting part is not that it works. It is the two ways the test was wrong
first.

**The slow joiner is not a theory.** A PUB socket discards every message it has
no subscriber for, silently and without buffering, and connecting a SUB socket
is asynchronous — the TCP connect returns, the ZMTP handshake completes, and the
subscription itself travels as a later frame the publisher processes some time
after that. A publisher that starts emitting the instant after `bind()` loses an
unpredictable prefix. The conventional fix is `sleep(0.1)`, which is a guess
that fails on a loaded CI runner and fails in a way that looks exactly like
message loss in the library under test.

The test therefore has the subscriber announce itself and the publisher wait for
that announcement. In the libzmq→Go direction that is a REQ/REP handshake. In
the Go→libzmq direction it started as one too, and had to be replaced: when
nothing arrived, there was no way to tell whether the PUB/SUB pair or the
REQ/REP pair had failed, because two independent socket pairs were between two
processes that were not talking. A file the subscriber creates and the publisher
polls for has no handshake of its own to get wrong. It is still a real
synchronisation, not a sleep.

**6,667 of 10,000 is the correct answer, and the first assertion called it a
bug.** The publisher emits across three topics; the subscriber asks for
`kv-events`; ZMQ subscription is a *prefix* match, so `kv-events-secondary`
arrives too and `other-events` does not. Two thirds of ten thousand is 6,667.
The first version of the test asserted that the received sequence numbers were
contiguous, and reported ~3,300 "gaps" — every one of them a message correctly
filtered out.

That is the more dangerous of the two, because the failure was loud and precise
and pointed at the wrong thing. An assertion that "obviously" holds — a stream
should have no holes in it — quietly stopped being true the moment filtering
entered the picture. The fix asserts the exact expected set of sequence numbers
rather than contiguity, which is also strictly stronger: it catches a filter
that dropped the right *number* of the wrong messages.

**Why it matters:** driftwatch's entire input path depends on this library
reading what vLLM's libzmq-backed publishers emit. Without this test that is an
assumption; ADR-0001 would be a bet rather than a decision. It also means the
prefix-matching behaviour is pinned: an operator who sets `topics: ["kv"]`
expecting an exact match will receive `kv-events`, `kv-cache` and anything else
starting with those two letters, and that is ZMQ's semantics rather than
driftwatch's to change.

**Fix:** Nothing in driftwatch. Both defects were in the test, and both are now
documented in it at the point where somebody would otherwise make the same
mistake again.

**Evidence:** `docs/evidence/interop-libzmq-both-directions.txt`

**Regression test:** `test/interop: TestInterop_LibzmqPublisherToGoSubscriber`
and `TestInterop_GoPublisherToLibzmqSubscriber`, behind the `interop` build tag,
run by the `interop` job in `.github/workflows/e2e.yaml`.

---

## D-022 — The oracle's memory does not level off when the key count does

**Found:** Phase 8, the first soak run failing its RSS assertion three times.

**What happened:** §16.7 asserts RSS growth under 5% over the steady-state
window, allowing for warmup. Every early run failed it, and not marginally: a
four-minute run at 50,000 keys went from 230 MiB to 627 MiB, +173%, with the key
count flat at 50,000 from the forty-second mark onwards.

Nothing was leaking. The heap profile named it immediately:

```text
127.31MB 56.19%  oracle.(*ring).push
 47.01MB 20.75%  projection.cloneMembers
```

The oracle keeps the last `ringSize` events per key for `driftwatch explain`.
The key count reaches its ceiling as soon as the workload has touched every key
once — but each key's *ring* only fills after that key has been touched sixteen
times. Memory therefore keeps climbing long after the thing everybody watches
has gone flat.

The time to steady state is `ringSize × keys / rate`, and it is not small. At
§16.7's own parameters — 500,000 keys, 5,000 events/sec — it is
16 × 500,000 / 5,000 = 1,600 seconds. **Twenty-seven minutes of a sixty-minute
soak is warmup**, and §16.7's "final 45 minutes" window starts fifteen minutes
before the oracle has finished growing.

**Why it matters:** Two things, and the second is worse than the first.

The soak as specified cannot pass at the parameters it specifies, and would have
been "fixed" by whoever hit it next — most likely by widening the threshold from
5% to something that accommodated the growth, which would have discarded the
assertion's entire value. The failure looks exactly like a leak.

The capacity picture is also worse than §19.1 assumes. Measured at 50,000 keys
with rings roughly a third full, the ring costs about 530 bytes per retained
event. At §19.1's stated case — 1,000,000 keys, `ringSize: 16`,
`retainRaw: false` — full rings alone come to roughly 8 GB, against a stated
budget of 512 MiB. `docs/KNOWN_GAPS.md` G-001 already recorded 640 MiB at 1M
keys; that measurement was taken before the rings had filled, so it was
measuring the same warmup this discovery is about. G-001 is updated accordingly.

**Fix:** The test now computes its own warmup from `ringSize × keys / rate`
rather than taking a fixed fraction of the run, and refuses to assert on a run
too short for the rings to fill — with a message that says so rather than
reporting a leak.

The memory itself is not fixed here. It is a real capacity limit and belongs in
G-001 with a number attached, not in a quiet threshold change.

**Evidence:** `docs/evidence/S2-soak-heap-middle.pprof`, and the RSS column in
`docs/evidence/S2-soak-60min-zero-drift.txt`.

**Regression test:** `test/soak: TestSoak` — `Config.ringFillTime` and the
`require.Less(t, warmup, len(samples))` guard mean a run that cannot see steady
state says so instead of failing the memory assertion.

---

## D-021 — A soak that "detected nothing" was injecting a fault that changed nothing

**Found:** Phase 8, the first soak run reaching its midpoint.

**What happened:** §16.7 asks for a deliberate 10-event drop at the halfway mark,
detected and then resolved, to prove the tool still works after half an hour
rather than merely still running. The obvious implementation is for the
materializer to skip those ten events. It was, and driftwatch reported nothing.

driftwatch was right. The workload emits `add key member` and the materializer
applies `SADD`, which is idempotent — and the workload cycles each key through
the same three publishers forever, so by the midpoint every member is already in
every set. Skipping one more `add` leaves the store holding exactly what the
oracle expects. There was no divergence to find.

The second attempt made the fault real by removing the member instead, and the
next run detected 8 of 10 within one sample and resolved them in the next.

The 8 rather than 10 is the other half of the finding, and it is correct: two of
the ten keys were touched again by the workload between the sweep that raised
them and the read that would have confirmed them, so two-phase confirmation
classified them as transient. They had stopped being divergent before anybody
could have acted on them.

**Why it matters:** A test that injects a fault and asserts detection is only
worth having if the fault is observable. This one asserted the most important
property in §16.7 — that detection still works late in a long run — and would
have passed the moment somebody weakened it to "detected or not", or failed
forever while looking like a detection bug in driftwatch rather than a modelling
bug in the test.

It also produced a second trap immediately. Reducing the key count to make a
short validation run cheap made the fault invisible again for a different
reason: at 20,000 keys and 5,000 events/sec each key comes round every four
seconds, so the removed member was written back long before a 30-second sweep
could confirm it was gone. The fault is only observable when
`keys / rate > sweepInterval + settlementWindow.max`. §16.7's parameters give
100 seconds against 90; the margin is thinner than it looks.

**Fix:** The drop removes the member rather than skipping the write, and
`Config.requireFaultIsObservable` fails before the run starts if the parameters
cannot see it, naming the arithmetic.

**Evidence:** `docs/evidence/S2-soak-60min-zero-drift.txt` — the drift column
going non-zero at the midpoint and back to zero one sample later.

**Regression test:** `test/soak: TestSoak` — `requireFaultIsObservable`, plus the
existing `require.NotZero(t, detectedAt)`.

---

## D-020 — The extras scan overwrote the one gauge that stops the dashboard lying

**Found:** Phase 8, watching the demo's own dashboard for thirty seconds.

**What happened:** `driftwatch_coverage_ratio` dropped to zero and came back, on
a period that turned out to be exactly `policy.extraScanInterval`:

```text
10:02:55  coverage=0.9905
10:03:01  coverage=0.0000     <- extras scan
10:03:06  coverage=0.9910
10:03:29  coverage=0.0000     <- extras scan
10:03:35  coverage=0.9905
```

Both halves of §5.5's comparison reach the metrics through one `OnReport`
callback, and `recordSweepMetrics` did not distinguish them. The target→oracle
pass walks the *store* looking for keys no event created, so its `KeysCompared`
is a count of store keys — and coverage, which means "what fraction of the
oracle did the last sweep verify", was being recomputed from it every time.

The same fall-through counted each extras scan as
`sweeps_total{kind="oracle_to_target"}` and mixed its duration into that
histogram, so both the sweep count and the sweep p99 were measuring two
different operations at once. The p99 that `DriftwatchSweepsSkipped` and the
row-5 dashboard panel are built on was a blend of a keyspace walk and a
comparison.

**Why it matters:** Of every gauge this could have hit, it hit the one whose
entire purpose is to stop the dashboard overstating its own verdict. §12.1 says
so in as many words: zero divergence at 3% coverage is meaningless, and this
panel is what makes that visible. An operator watching it would have seen it
flash red on a timer and learned to disregard it — which is the failure mode the
panel exists to prevent, arriving through the panel itself.

Nothing in the unit suite could have caught it. Every test that drives an extras
scan calls `ScanExtras` directly and asserts on the report it returns; none of
them asserted on a gauge that a *different* operation had set earlier. The bug
only exists in the interleaving, and the interleaving only happens in a process
that runs both on their own timers for longer than `extraScanInterval`.

**Fix:** `differ.Report` gained a `Pass` field, set by the sweeper for each half.
`recordSweepMetrics` routes a target→oracle report to `recordExtrasScanMetrics`,
which records the scan's own result, duration and observed target health and
touches nothing else — so every gauge it has no opinion about keeps the value
the last real sweep gave it, which is the honest answer.

**Evidence:** `docs/evidence/demo-drift-detected-and-resolved.txt` — the coverage
column holding between 0.9969 and 0.9999 across a full drift episode, including
several extras-scan boundaries.

**Regression test:**
`pkg/check: TestCheck_ExtrasScanDoesNotClobberSweepMetrics`.

---

## D-019 — The manager panicked at startup on a registry every test built differently

**Found:** Phase 7, the first time the real image ran in a Kind cluster.

**What happened:** `cmd/driftwatch-manager` puts driftwatch's metrics on
controller-runtime's registry, so that one scrape of one port returns both the
check metrics and the controller's. Alongside that it registered the Go and
process collectors, which is what every Prometheus program does.

controller-runtime registers those two itself. The second registration is not a
duplicate-metric warning; `MustRegister` panics. The manager died before its
first reconcile, in a binary that had just passed every unit test, the whole
envtest suite and `go vet`:

```text
panic: duplicate metrics collector registration attempted
  main.buildMetrics cmd/driftwatch-manager/main.go:245
```

**Why it matters:** Nothing in the test suite could have caught it. The unit
tests build a fresh `prometheus.NewRegistry()`, and the envtest suite builds a
manager without ever calling `buildMetrics` — both are testing a registry that
does not exist in production. The only place the two registrations meet is the
real entrypoint against controller-runtime's package-level registry, and the
only way to reach that is to run the binary.

It also fails in the least useful way: a `CrashLoopBackOff` with a panic about
metrics, on a deployment whose actual problem has nothing to do with metrics.

**Fix:** Drop the two registrations. controller-runtime already provides them,
so the metrics an operator gets are unchanged.

The broader conclusion is about the phase rather than the bug: §20 Phase 7 makes
`make deploy` against Kind an exit criterion, and this is why. Three other
defects in this phase were found the same way and nowhere else — the image's
pull policy, the webhook's missing certificate, and the Prometheus operator CRDs
in the default overlay. All four are startup failures, which is the class of bug
a test suite is structurally worst at reaching.

**Evidence:** `docs/evidence/phase7-live-check.txt` — the manager running
afterwards, with the status and events it produces.

**Regression test:** None that is honest. A test asserting `buildMetrics` does
not panic would pass against a registry the test itself made, which is exactly
the mistake that caused this. The CI `image` job runs
`docker run --rm driftwatch:ci --version`, which starts the real binary and is
the cheapest thing that would actually have caught it.

---

## D-018 — Defaults do not reach a field the operator did not mention

**Found:** Phase 7, applying `config/samples/` to Kind and reading it back.

**What happened:** Every field in the CRD carries a `+kubebuilder:default`, and
§10.2 asks that `kubectl get driftcheck -o yaml` show the configuration that is
actually running rather than the sparse thing the operator typed. Applying the
minimal sample and reading it back gave defaults for `source`, `projection` and
`target` — and nothing at all for `codec`, `policy` or `alert`.

Structural-schema defaulting descends into a field only if that field is
present. `policy` was absent from the submitted YAML, so the API server never
looked inside it, and none of the twenty defaults on its children applied.

**Why it matters:** The check still ran correctly — `check.ApplyDefaults` fills
the same values at construction — so nothing failed. What broke was the thing
the status block exists for. An operator reading the object saw no
`sweepInterval`, no `settlementWindow`, no `maxTrackedKeys`, and had no way to
tell whether the check was using the documented defaults or something else. The
one honest source of what a check is doing had a hole in it, silently.

It is also the difference between the CRD being useful on its own and needing
the webhook. Without this, `kubectl apply -f config/crd/` gives a schema whose
defaults mostly do not fire.

**Fix:** `+kubebuilder:default={}` on `codec`, `policy`, `alert` and
`policy.settlementWindow`. An empty object is enough to make the API server
descend, and the children default from there.

**Evidence:** `docs/evidence/phase7-live-check.txt` — the effective
configuration read back from a cluster with no webhook installed, showing all
six blocks filled in.

**Regression test:** `api/v1alpha1: TestWebhook_DefaultingFillsEveryOptionalField`
covers the webhook's half. The schema's half is covered by
`hack/verify-crd-docs.sh`, which regenerates the CRD and diffs it against the
committed one, so the markers cannot be dropped without CI noticing.

---

## D-017 — Cancelling the leader-elected runnables does not order them

**Found:** Phase 7, running the manager test under `-race`.

**What happened:** §10.3 requires that all runners stop when the manager stops
leading, and `RunnerStopper` implements it: a leader-elected runnable that waits
on its context and then calls `StopAll`. The test creates two checks, cancels the
manager's context, waits for `Start` to return, and asserts the registry is
empty.

It was not. Four runnables had been built and only two closed, and the registry
still held two.

controller-runtime cancels every leader-elected runnable together rather than in
any order, so the stopper's `StopAll` ran while a reconcile was still in flight.
That reconcile then called `Ensure` and started a runner — after the only thing
that would ever have stopped it had already finished.

**Why it matters:** The runner left behind has no path back to it. The manager
is gone, so no further reconcile arrives; the process may hold the lease no
longer, so another replica is auditing the same store. Two oracles sweep one
target and both write metrics under the same `check` label, which presents as a
divergent-key count alternating between two values — the same symptom §10.3's
per-key mutex exists to prevent, arriving through a completely different door.

Under `-race` it reproduced roughly one run in four. Without the race detector
it did not reproduce at all in ten runs, which is how it would have shipped.

**Fix:** `RunnerRegistry.Shutdown` latches the registry closed before it
enumerates, and `Ensure` re-checks that latch under the same per-key lock it
started the runner under. Either the re-check sees the latch and undoes its own
start, or the latch came afterwards — in which case the entry was already in the
map when `Shutdown` enumerated. `StopAll` alone cannot close that window,
whatever order the runnables are cancelled in.

**Evidence:** `docs/evidence/phase7-controller-suite.txt`

**Regression test:** `internal/controller: TestRegistry_ShutdownRefusesLateStarts`
and `TestRegistry_ShutdownRacingWithEnsureLeavesNothingRunning` — the second runs
eight concurrent `Ensure` calls against a `Shutdown`, which is the interleaving
that produced it.

---

## D-016 — Fifty idle checks held 640 MB, essentially all of it an empty channel

**Found:** Phase 6, writing §15 row 60.

**What happened:** The row requires memory to be linear in the number of checks
and small enough that a manager can hold a realistic number of them. Fifty
checks that had each ingested one event measured 12.9 MB apiece.

The whole of it was one allocation. `pkg/check` sizes the channel between the
source and the applier from `policy.ingestBufferSize`, whose §10.1 default is
200,000, and a `source.RawMessage` is 64 bytes: 12.8 MB reserved at
construction, per check, before a single message arrives.

**Why it matters:** The default is correct for what it was written for. §10.2
requires the ingest buffer to exceed the socket's high-water mark so that loss
happens in the channel, where driftwatch counts it and can mark the affected
keys suspect, rather than inside the transport where it is invisible. That
argument is sound — and it only applies to a transport that can drop.

A file source blocks its reader. A memory source is in-process. Neither can lose
a message however far behind the applier falls, and both were paying 12.8 MB for
a buffer sized against a socket they do not have. In the operator, which §15 row
60 says must run fifty checks in one process, that is 640 MB of channel holding
nothing.

**Fix:** `ingestBufferFor` in `pkg/check`. A zmq or nats source keeps the
configured size, because for those the §10.2 argument holds; everything else is
capped at 4,096. Per-check memory went from 12.9 MB to 387 KiB, and what remains
is the oracle's shards and settlement index, which is real state.

**Evidence:** `docs/evidence/D-016-idle-check-memory.txt`

**Regression test:** `test/faults: TestFault60_FiftyConcurrentChecksInOneManager`
— it asserts a per-check ceiling, so the allocation cannot creep back.

---

## D-015 — Three metrics were declared, documented, exported and never written

**Found:** Phase 6, writing §15 rows 18, 19, 23, 24 and 36.

**What happened:** `driftwatch_publisher_clock_skew_seconds`,
`driftwatch_events_dropped_total{reason="unknown_op"}` and
`{reason="too_large"}` were all registered and all documented, and nothing in
the codebase ever set them. `driftwatch_target_reachable` had the same shape:
it was set only from a successful sweep's report, so a sweep that could not
reach the store left it holding its previous value.

Nothing caught it. `hack/verify-metrics-docs.sh` checks that the documentation
matches the declarations, and the name test checks that the registry matches a
hand-written list — both were satisfied, because both are about names. A metric
registered and never written exports no series at all, which on a dashboard is
indistinguishable from one correctly reporting zero.

The drop reasons are the sharpest case. `pkg/codec` already distinguishes
`ErrMalformed`, `ErrUnknownOp` and `ErrTooLarge`, with a comment saying why the
three are separate; `pkg/check` reported all of them as `decode_error`.

**Why it matters:** Each of the four is the metric an operator would reach for
in exactly the situation it was silent in. `target_reachable` read 1 throughout
an outage. `clock_skew_seconds` read nothing while a publisher's clock drifted.
And the collapsed drop reasons send someone to the serializer when the real
answer is that a producer started emitting an event type nobody configured.

**Why the name tests were not enough:** they assert the contract's shape. §15
asserts its behaviour — each of these was found by the row that names the value,
not the metric.

**Fix:** `decodeReason` maps the codec's typed errors onto the §12 reasons;
`recordSkew` measures the publisher offset and feeds both the metric and the
status block; `recordSweepMetrics` sets reachability on the failure path.
`publishGauges` is also now called after every sweep, so a process that only
sweeps out of band — which is what `driftwatch diff` and `watch --once` do —
exports its state gauges more than once.

**Evidence:** `docs/evidence/D-015-declared-and-unwritten-metrics.txt`

**Regression test:** `test/faults`: rows 18, 19, 23, 24 and 36.

---

## D-014 — `Commutative()` was declared by every projection and read by nothing

**Found:** Phase 6, writing §15 row 7.

**What happened:** §9 M6 defines the method and states the obligation plainly —
"If false, the oracle must order by seq before applying". All three projections
report false. Nothing anywhere ordered by sequence number; the method was
declared four times and called zero times.

Row 7 requires that reordering driftwatch's stream produce zero findings, on the
grounds that reordering loses ordering and not information. It failed.

A publisher emits `add block:a replica-0` then `remove block:a replica-0`. The
materializer applies them in order and ends with the key gone. driftwatch
receives them swapped, applies the remove against a key that does not exist yet
— a no-op — then the add, and ends holding a member the store does not have.

**Why it matters:** Neither side is broken. The store is correct, driftwatch's
expectation is not, and driftwatch reports the difference as drift. It is a
false positive it manufactured entirely on its own, on a transport doing
something PUB/SUB does routinely, and it never resolves: the oracle stays wrong
until some later event happens to overwrite that key.

This is the failure mode the whole project is built to avoid, arriving through
the one mechanism nobody had implemented.

**Fix:** `pkg/check/reorder.go`. A per-publisher buffer holds an event that
arrives ahead of its predecessor and releases on whichever comes first: the
predecessor arriving, a two-second window expiring, or the buffer filling at
1,024 events. Bounded in both directions on purpose — holding forever would turn
one lost message into a permanently stalled publisher, and releasing immediately
is what produced the bug. When the wait times out the hole is a real gap and
seqtrack records it as one, just later and with far fewer false alarms.

Two consequences worth knowing. Gap detection is now deferred by up to the
reorder window, which is strictly more accurate. And an undecodable frame leaves
a hole nothing can fill — its sequence number was in the part that would not
parse — so the events behind it wait out the window; §15 rows 15 and 16 pin that.

**Evidence:** `docs/evidence/D-014-commutative-unconsumed.txt`

**Regression test:** `test/faults: TestFault07_AdjacentPairReorderedOnDriftwatch`,
and `TestFault08_WindowShuffleOverTenThousandEvents`, which shuffles 10,000
events within a sliding window of eight and compares the oracle against
`pkg/projection`'s independent reference fold.

---

## D-013 — A key template makes the oracle key and the event key different, and the applier used the wrong one

**Found:** Phase 5, on the first run of `TestCheck_EndToEnd_InProcess`.

**What happened:** With `keyTemplate: "block:{{.Key}}"` configured, six events —
three blocks, each added by two replicas — produced an oracle holding one member
per key instead of two. The version was 2, so both events had been applied. Only
the first member of each key was missing.

`Projection.Apply` takes the key's previous value, so the caller has to fetch it
first. But the store key is the template's output (`block:0`) and the event
carries the raw key (`0`), and the applier looked up the raw one. It missed every
time, handed `Apply` an absent value, and every event overwrote instead of
accumulating.

Nothing errored. The pipeline ran, the oracle filled, and the report was
confidently wrong about every single key.

**Why it matters:** The direction of the wrongness is what makes this bad. An
oracle holding fewer members than it should reports `extra_in_target` — the
target has a member no event created — so driftwatch would have accused the store
of holding data nobody wrote, on every key, in the exact configuration §25.2 ships
as the example. The failure is also invisible without a key template, which is
why every unit test in the repository passed: the harness projections were all
configured with the default `{{.Key}}`, where the raw key and the store key
happen to be the same string.

**Fix:** `Projection` grew a `TargetKey(e *event.Event) (string, error)`, resolved
through the same template `Apply` uses. `pkg/check` resolves the store key before
fetching the previous value; the scenario harness, which had the same latent bug,
was fixed with it.

**Evidence:** `docs/evidence/D-013-projection-key-template.txt`

**Regression test:** `pkg/check: TestCheck_EndToEnd_InProcess` — the flagship
composition test, which is what found it. It is the only test in the repository
that runs a key template through the whole pipeline, and that is precisely why
§9 M14 asks for it.

---

## D-012 — §12's default publisher label limit and its cardinality budget cannot both be satisfied

**Found:** Phase 5, writing the cardinality test M12 requires.

**What happened:** The test — 10,000 keys and 500 publishers, asserting the
registry stays under 500 time series — failed at 629 on its first run, with the
`maxPublisherLabels` default of 100 that §9 M12 specifies.

The two numbers are in the same section and are not compatible. §12 defines seven
metrics carrying the `publisher` label, six of which a plain ingest workload
touches, and each costs `limit + 1` series once the `__other__` aggregate is
counted. At a limit of 100 that is 6 x 101 = 606 series before any other metric
exists, which is 21% over the whole budget.

Measured across four limits:

```text
maxPublisherLabels=25   -> 156 time series (budget 500) OK
maxPublisherLabels=50   -> 306 time series (budget 500) OK
maxPublisherLabels=75   -> 456 time series (budget 500) OK
maxPublisherLabels=100  -> 606 time series (budget 500) OVER BUDGET
```

**Why it matters:** The budget is the number worth keeping. driftwatch runs
beside the store it audits, frequently one replica per node, so its series count
is multiplied by the fleet before it reaches Prometheus. A tool that costs a
hundred series per replica is the monitoring incident it was deployed to detect,
and it gets uninstalled after the first one.

The limit, by contrast, only decides how many publishers keep an individual graph.
`driftwatch_publishers_tracked` still reports the true count once labels collapse,
and per-publisher detail beyond fifty publishers belongs in `driftwatch explain`
and the logs rather than in a label.

**Fix:** `DefaultMaxPublisherLabels` is 50, deviating from §9 M12's 100, with the
arithmetic recorded in ADR-0008. The full-registry cardinality test is the guard:
adding an eighth publisher-labelled metric, or raising the limit back to 100,
moves a number that test asserts on, which forces the decision to be made
deliberately rather than discovered in production.

**Evidence:** `docs/evidence/D-012-publisher-label-budget.txt`

**Regression test:** `pkg/metrics: TestMetrics_CardinalityStaysBoundedUnderTenThousandKeys`
and `TestMetrics_CardinalityStaysBoundedWithEveryMetricExercised`.

---

## D-011 — Caching the first DNS resolution turns a pod reschedule into permanent silence

**Found:** Phase 4, implementing the ZMQ source's reconnect loop.

**What happened:** nothing, which is the point. §9 M4 lists this among its edge
cases and it is worth writing down because the failure it describes is entirely
invisible and the code that causes it looks like an optimization.

The shape of it: a subscriber resolves `tcp://publisher.default.svc:5555` once
at startup, keeps the address, and reconnects to it forever. The publisher pod
is rescheduled, comes back on a different IP, and the DNS record updates. The
subscriber goes on dialling an address nothing is listening on.

What makes it nasty is how it presents. There is no error to alert on — the
reconnect loop is working exactly as designed, retrying with backoff against an
endpoint that refuses. `Connected` reports false, which is also what a
subscriber waiting for a publisher that has not started yet reports. Events stop
arriving, and driftwatch marks keys `Suspect` and stops asserting, so the
observable symptom is a monitoring tool that has quietly stopped monitoring.
Nothing distinguishes it from a quiet publisher except noticing that it has been
quiet for a suspiciously long time.

**Why it matters:** less for the bug than for how easy it is to write. Resolving
once and reusing the result is the obvious thing to do — it is one syscall
instead of many, the address does not normally change, and every reconnect after
the first is measurably cheaper. In a static deployment it is correct. On
Kubernetes, where a pod's IP is expected to change and the service record is the
only stable name, it is a time bomb that goes off during the next node drain.

**Fix:** `resolveAll` runs on every connection attempt, not once at
construction, and there is no cached address anywhere in the source. Literal IPs
and non-TCP transports pass straight through, so the extra lookup only happens
where there is a name to look up. A host resolving to several addresses has all
of them dialled, which is what a headless service needs.

`TestZMQ_ReResolvesDNSOnEveryReconnect` drives a resolver that returns a
different address on each call and asserts the second attempt dialled the second
address. It is a test of an absence — that nothing cached — which is exactly the
kind of property that rots silently without one.

**Evidence:** `docs/evidence/D-011-dns-reresolution.txt`

**Regression test:** `pkg/source: TestZMQ_ReResolvesDNSOnEveryReconnect`,
`TestZMQ_AnUnresolvableEndpointRetriesForeverRatherThanFailing`

---

## D-010 — The pure-Go ZMQ binding accepts a subscriber high-water mark and ignores it

**Found:** Phase 4, implementing §8.1's ingest-buffer sizing rule.

**What happened:** §8.1 sets out a specific defence. ZMQ PUB sockets drop for
slow subscribers silently, so driftwatch should set `ZMQ_RCVHWM` explicitly and
size its own ingest buffer *larger* than it — that way, when loss happens, it
happens in driftwatch's own countable buffer rather than invisibly in the
socket. The reasoning is sound and I built to it.

The mechanism it assumes does not exist in `go-zeromq/zmq4`. A SUB socket has no
receive high-water mark. `SetOption` stores the property in a map and returns
nil:

```go
// zmq4@v0.17.0/socket.go:373
func (sck *socket) SetOption(name string, value interface{}) error {
    // FIXME(sbinet) different socket types support different options.
    sck.props[name] = value
    return nil
}
```

Nothing in the receive path reads it. HWM *is* implemented — on the PUB socket,
where it drops at the publisher (`pub.go:299`) — so the option name is real, the
call succeeds, and the code looks correct while doing nothing at all.

What the subscriber does instead is worse than dropping. The reader is a fixed
ten-message channel (`msgio.go:44`, `const qrsize = 10`), not configurable, and
past it the connection read blocks. So a slow driftwatch does not lose frames;
it applies TCP backpressure all the way to the publisher and slows down the
system it is supposed to be observing without touching.

**Why it matters:** the §8.1 mitigation is untestable as written against this
binding, and worse, it silently appears to work. Setting the option returns no
error. A reviewer reads the line, matches it against the PRD, and moves on. The
failure only shows up in production, as either unbounded memory or — the part
that would be genuinely hard to diagnose — a publisher mysteriously slowing down
whenever the monitoring is running.

It is also exactly the class of finding §8.1 asked for in advance: it chose the
pure-Go binding over cgo and required any wire or behavioural gap to be recorded
rather than assumed away.

**Fix:** enforce the bound in driftwatch, where it can be counted. `RecvHWM`
sizes the ingest channel; a frame that cannot be handed over is dropped,
counted in `Stats.Dropped`, and raises a `GapHighWaterMark` signal so the
pipeline marks the affected keys `Suspect`. The receive loop never blocks on a
full pipeline, which is what keeps the backpressure off the publisher.

`SetOption(OptionHWM, …)` is still called, with a comment saying it is a no-op
on this binding and pointing here — if the upstream implements it, the intent is
already expressed, and until then nobody has to rediscover why it is missing.

§8.1's guarantee survives intact: loss is bounded, counted and visible. Only the
layer enforcing it moved.

**Evidence:** `docs/evidence/D-010-sub-hwm-noop.txt`

**Regression test:** `pkg/source: TestZMQ_DropsAtTheHighWaterMarkRatherThanGrowing`

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
