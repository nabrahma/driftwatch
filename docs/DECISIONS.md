# Architecture Decision Records

Every technology choice, every deviation from `docs/PRD.md`, and every decision
taken while blocked (PRD §1.4) is recorded here. Newest last.

Each record states the **context** that forced a choice, the **options
considered** with their real trade-offs, the **decision**, and the
**consequences**, including the ones we would rather not have.

Status values: `Accepted`, `Superseded by ADR-NNNN`, `Reversed`.

---

## ADR-0001, ZeroMQ binding: pure Go, not cgo

**Status:** Accepted
**Date:** 2026-07-30
**Phase:** 0
**PRD reference:** §8.1

### Context

driftwatch's primary motivating source (§2.4) is a ZeroMQ PUB/SUB stream: LLM
inference replicas publishing KV-cache block ownership events. Subscribing to
that stream from Go requires either a binding to the C library `libzmq`, or a
native Go implementation of the ZMTP wire protocol.

This choice is unusually load-bearing for a project of this shape, because it
propagates into every part of the delivery story:

- **Container image.** driftwatch ships as a distroless, non-root, read-only
  rootfs image (§8.5). A cgo binary needs a libc and the `libzmq.so` family in
  the final image, which rules out `FROM scratch` and complicates distroless.
- **Cross-compilation.** Release builds target `linux/amd64`, `linux/arm64` and
  `darwin/arm64` (§17.1 `build` job). With cgo that is three cross-toolchains
  plus three cross-built copies of libzmq, or three native runners.
- **CI and Kind.** The e2e suite (§14) loads driftwatch images into Kind nodes.
  A libzmq dependency means matching the library version across the CI runner,
  the build image and the node image, and a mismatch surfaces as a runtime
  symbol error rather than a build failure.
- **The race detector.** `go test -race` needs cgo. With a cgo dependency in the
  build graph, a broken or mismatched C toolchain breaks the *test* suite too,
  not just the build, and the test suite is this project's primary output.

### Options considered

**Option A, `github.com/pebbe/zmq4` (cgo binding to libzmq).**

- *For:* wraps the reference implementation, so wire compatibility with other
  libzmq peers is not in question. Complete socket-option surface. Long track
  record.
- *Against:* requires `CGO_ENABLED=1` everywhere, a C toolchain in every build
  and test environment, and libzmq present at runtime. Cross-compilation becomes
  a per-architecture toolchain problem. Distroless/scratch images are off the
  table without vendoring shared objects. Static linking against libzmq is
  possible but adds a build step that has to be maintained per platform.

**Option B, `github.com/go-zeromq/zmq4` (pure Go ZMTP implementation).**

- *For:* `CGO_ENABLED=0` throughout. `GOOS`/`GOARCH` cross-compilation is a
  single `go build`. `FROM scratch` and distroless images work with no runtime
  dependency. No C toolchain in CI. `-race` depends only on the Go toolchain.
- *Against:* it is a reimplementation of a wire protocol, so compatibility with
  real libzmq publishers is a *claim*, not a given. The socket-option surface is
  narrower than libzmq's. It has fewer production deployment-years behind it.
  A protocol-level incompatibility would be discovered late and would be
  expensive.

**Option C, do not support ZeroMQ; ship NATS, memory and file sources only.**

- *For:* removes the problem entirely. NATS has a good pure-Go client.
- *Against:* abandons the motivating case (§2.4). The systems this tool exists
  to audit publish over ZeroMQ. A divergence detector that cannot attach to the
  real stream is an exercise, not a tool.

### Decision

**Use `github.com/go-zeromq/zmq4` (Option B).**

The properties Option B buys, static binaries, trivial cross-compilation,
distroless images, no C toolchain in CI, `-race` that depends only on Go, are
exactly the properties that make the rest of this project's delivery plan
(§14 Kind e2e, §17 CI under ten minutes, §17.4 multi-arch release) tractable.
Option A trades all of them for a compatibility guarantee.

That guarantee is the only real argument for Option A, and it can be bought
another way: by **testing it**. So the decision is conditional on a mitigation
that is not optional.

**Required mitigation (binding):** §16.6 mandates an interop test in which a
real `pyzmq` publisher, libzmq-backed, feeds the Go subscriber, run in CI
under the `interop` build tag. Wire compatibility is asserted by a test on every
push, not assumed from a README. The areas most likely to break are
subscription-prefix filtering and multipart framing conventions; the interop
test must exercise both explicitly. If a gap is found, it goes in
`docs/DISCOVERIES.md` with the reproduction.

**Also binding:** ZMQ PUB sockets drop messages for slow subscribers once the
high-water mark is reached, and they do it silently. The SUB receive HWM must be
set explicitly rather than left at the library default, and driftwatch's own
ingest buffer must be **larger** than the socket HWM, so that when a drop
happens it happens in driftwatch's own countable buffer
(`driftwatch_ingest_dropped_total`) rather than invisibly inside the socket.
A detector that silently loses its own input is worse than no detector, because
it reports the target as correct when it has simply stopped looking.

### Consequences

- `CGO_ENABLED=0` is the default for all builds; the Makefile exports it. Only
  `go test -race` overrides it, and it needs no C *library*, just a compiler.
- The interop test is now a load-bearing part of the suite. If it is deleted or
  allowed to rot, this ADR's justification collapses. It must run in CI, not
  only locally.
- If a compatibility gap turns out to be unfixable, the fallback is Option A
  behind a build tag (`zmq_cgo`), keeping pure Go as the default. This would be
  a new ADR superseding this one. It is the reason the source layer is behind
  the `Source` interface (§7 `pkg/source`) rather than called directly.
- We accept a narrower socket-option surface. If a needed option is missing,
  that is a finding for `docs/DISCOVERIES.md`, not a silent workaround.
- `github.com/pebbe/zmq4` is explicitly rejected and must not appear in
  `go.mod`.

---

## ADR-0002, Redis client: `go-redis/v9`

**Status:** Accepted
**Date:** 2026-07-30
**Phase:** 0
**PRD reference:** §8.2

### Context

The target store adapter (§7 `pkg/target`) reads Redis and nothing else at
first. The access pattern is unusual for an application: no writes ever (NG1),
large `SCAN` traversals of up to a million keys (§19.1 S6), heavily pipelined
batched reads during a sweep, and `INFO stats` parsing to detect eviction
(§5.7). It must work against standalone, Sentinel and Cluster deployments, and
every call must be cancellable via `context.Context` because a sweep can be
abandoned mid-flight when a `DriftCheck` is deleted (§S9).

### Options considered

**Option A, `github.com/redis/go-redis/v9`.** Context-aware across the whole
API. Standalone, Sentinel and Cluster clients share one interface. `Pipeline`
and a `SCAN` iterator are first-class. Widely deployed, actively maintained,
and the client most Kubernetes-adjacent Go projects already use.

**Option B, `github.com/redis/rueidis`.** Measurably faster: RESP3, auto
request pipelining, and opt-in client-side caching. The performance headroom is
attractive against the §19.1 sweep targets.

Its headline feature is disqualifying here, though. **Client-side caching is
precisely wrong for driftwatch.** The tool's entire job is to report what the
target store *actually contains right now*. A client that can serve a read from
a local cache, even a correctly invalidated one, introduces a second possible
explanation for every finding, and turns "the target disagrees" into "the target
disagrees, or my client's cache is stale." That is exactly the class of doubt
driftwatch exists to eliminate. The feature can be left off, but building a
correctness tool on a client whose default posture is caching means every future
reader has to verify it is still off.

**Option C, `github.com/gomodule/redigo`.** Stable and minimal, but the
connection-pool API predates `context.Context` in places, and Cluster support is
not built in. Cancellation would have to be bolted on.

### Decision

**Use `github.com/redis/go-redis/v9` (Option A).**

Correctness properties beat throughput here. Uniform context support gives clean
cancellation, one interface covers all three deployment topologies, and there is
no caching layer between driftwatch and the truth it is measuring.

Usage rules that follow from this and are binding on `pkg/target`:

- **`SCAN` with a tuned `COUNT`, never `KEYS`.** `KEYS` blocks the server for
  the length of the traversal, and running it against a production index would
  make driftwatch an outage rather than a detector.
- **Pipeline batched key reads** during the sweep, default batch 500 (§8.2).
- **Read-only enforcement is structural, not conventional.** The
  `RecordingTarget` wrapper (§5.8 I13) fails any test that issues a command
  outside the allowlist `GET SMEMBERS SCAN TYPE TTL PTTL EXISTS HGETALL INFO
  STRLEN SCARD MEMORY`. NG1 is enforced by the test suite, not by care.

### Consequences

- We give up rueidis's throughput. If a benchmark (§16.8) later shows the client
  is the bottleneck in `BenchmarkFullSweep1M`. That is grounds for a new ADR, 
  with the measurement attached, and with client-side caching explicitly off.
- `INFO` output differs between Redis 6 and 7. The parser must be tested against
  both (§20 Phase 2 exit criteria) rather than written against whichever version
  is on the development machine.
- `SCAN` gives weak guarantees: keys may be returned more than once, and the
  cursor's meaning is not stable across a `FLUSHDB`. The differ must be
  idempotent over repeated keys, and the extras scan must tolerate a cursor
  reset without looping forever. This is called out as a Phase 2 investigation
  item and belongs in `docs/DISCOVERIES.md` once characterized.

---

## ADR-0003, Kubernetes: kubebuilder and controller-runtime

**Status:** Accepted
**Date:** 2026-07-30
**Phase:** 0
**PRD reference:** §8.3

### Context

G4 makes a drift check *configuration* rather than code: a `DriftCheck` custom
resource (§10) that a controller reconciles into a running check, with status
and conditions reported back. That needs CRD types, deepcopy generation, CRD and
RBAC manifests, a defaulting/validating webhook (§10.2), leader election
(NG4), and a test story that does not require a real cluster for every run.

### Options considered

**Option A, kubebuilder v4 with `sigs.k8s.io/controller-runtime`.** The
scaffolding, layout and code generation that the Kubernetes project itself uses.
`controller-gen` produces deepcopy functions, CRD schemas and RBAC from Go
markers, so the types stay the single source of truth. `envtest` runs a real
API server and etcd without a cluster, which keeps controller tests in the
seconds range and off the e2e critical path (§23 A14).

**Option B, Operator SDK.** Builds on controller-runtime and adds Ansible/Helm
operator modes plus OLM packaging. For a Go-only operator it is a layer over
Option A that adds surface without adding capability here.

**Option C, hand-written informers on `client-go`.** Maximum control and one
fewer framework to learn. It also means writing workqueue plumbing, rate
limiting, leader election and status patching by hand, all solved problems, all
easy to get subtly wrong, and none of them the interesting part of this project.
The interesting part is §5.

### Decision

**Use kubebuilder v4 scaffolding with `sigs.k8s.io/controller-runtime`
(Option A). Test controllers with `envtest`; reserve Kind for full e2e only.**

The layout it produces (`api/v1alpha1`, `internal/controller`, `config/`) is the
layout in PRD §7, and it is the layout a Kubernetes reviewer can navigate
without being told where anything is, which is an explicit goal (§1.5).

The envtest/Kind split matters as much as the framework choice. envtest gives a
real API server for reconciliation, watch and status semantics in seconds. Kind
is reserved for what genuinely needs a cluster: images, networking, RBAC as
actually applied, and the full ZMQ → materializer → Redis → driftwatch path.

### Consequences

- Generated code (`zz_generated.deepcopy.go`) and generated manifests
  (`config/crd`, `config/rbac`) are committed, so `hack/verify-codegen.sh` must
  fail CI when they drift from the markers. Until Phase 6 that script skips
  explicitly (ADR-0007) rather than passing silently.
- controller-runtime pins compatible `k8s.io/apimachinery` and `k8s.io/client-go`
  versions. Kubernetes minor upgrades become a coordinated bump, not an
  independent one.
- The webhook adds a certificate requirement in-cluster. The e2e fixtures must
  provision certs, and the Helm chart must expose that configuration.
- `internal/controller` is capped at 80% coverage in §16.9, lower than the
  domain packages, because framework-driven paths are dominated by wiring.
  Coverage there is not the measure of quality; the envtest assertions and the
  goleak check on CRD create/update/delete mid-sweep (S9) are.

---

## ADR-0004, The dependency list is closed

**Status:** Accepted
**Date:** 2026-07-30
**Phase:** 0
**PRD reference:** §8.4

### Context

driftwatch is a read-only auditing tool that people are expected to run beside
production systems. Its dependency graph is part of its operational surface: it
is what has to be vulnerability-scanned (§17.1 `build` job runs `trivy` and
`govulncheck`), reviewed and upgraded. Go makes adding a dependency a one-line
change, which is exactly why the decision to add one needs friction.

There is a second reason specific to this project. PRD §23 A8 identifies
breadth-instead-of-depth as a failure mode: a repository that supports Kafka,
etcd, PostgreSQL and a web UI reads as a survey rather than as one correct path.
Most breadth arrives as a dependency. Making the list closed makes that failure
mode visible at the point it would happen.

### Options considered

**Option A, closed list; additions require an ADR.** Every addition is a
recorded, argued decision. Costs a few minutes when the addition is obvious.

**Option B, open list; review dependencies periodically.** Lower friction.
In practice periodic review does not happen, and the graph is discovered at
audit time.

**Option C, vendor everything.** Reproducibility and offline builds, at the
cost of a large repository diff on every bump, which obscures review.

### Decision

**Option A.** The dependency list in PRD §8.4 is the complete allowed set. All
versions are pinned in `go.mod`. Adding anything outside that list requires a
new ADR in this file stating what it does, what it was chosen over, and what it
pulls in transitively.

The rejections in §8.4 are adopted as part of this decision:

- **`github.com/pebbe/zmq4`**: cgo. See ADR-0001.
- **Any ORM or query builder**: there is no SQL in this project.
- **`viper`**: configuration arrives from three places: CLI flags, environment,
  and the `DriftCheck` spec. Those precedences are few enough to write
  explicitly, and writing them explicitly means an operator can read the code
  and know which one wins. Viper's implicit precedence is a debugging cost paid
  later in exchange for setup convenience now.
- **OpenTelemetry tracing**: tracing follows requests; driftwatch's subject is
  background state convergence (§2.3). Prometheus metrics and the per-key event
  ring answer the questions this tool exists to answer. Adding OTel is scope
  creep. It should be listed in `docs/KNOWN_GAPS.md` as a possible future
  extension when that file starts taking entries.

### Consequences

- Some genuinely convenient library will be excluded and something slightly more
  verbose will be written by hand. That is the intended trade.
- `go.mod` stays small enough that `govulncheck` and `trivy` findings are
  actionable rather than background noise.
- This ADR file is the audit trail for the dependency graph. If it disagrees
  with `go.mod`, `go.mod` is the bug.

---

## ADR-0005, Go 1.23 minimum, `CGO_ENABLED=0`, distroless runtime

**Status:** Accepted
**Date:** 2026-07-30
**Phase:** 0
**PRD reference:** §8.5

### Context

Four coupled build decisions: the minimum Go version, whether cgo is enabled,
how version information reaches the binary, and what the container image is
built `FROM`.

### Options considered

**Minimum Go version.** Go 1.22 is more conservative and broadens the range of
distribution toolchains that can build the project. Go 1.23 is required by two
things the design actually uses: range-over-func iterators, which let the oracle
expose iteration over settled keys without materializing a slice per sweep, a
slice of a million keys per sweep is an allocation the §19 budget does not have
,  and the `slices`/`maps` stdlib packages, which remove a category of
hand-written helpers. A newer minimum was rejected because nothing needs it and
each bump narrows who can build the project.

**cgo.** `CGO_ENABLED=1` would be forced by a cgo ZMQ binding, which ADR-0001
already rejected. With that gone, nothing in the dependency list needs it. The
race detector needs a C *compiler* at test time, but no C *library*, and that
requirement does not propagate to release artifacts.

**Version information.** Either `runtime/debug.ReadBuildInfo` (VCS stamping, no
build flags) or `-ldflags -X` into a dedicated package. Build-info stamping is
unavailable or partial in some build contexts, notably when building from a
non-VCS source tree, and it cannot carry a release version distinct from a tag.
`-ldflags` is explicit and works identically in `make build`, goreleaser and the
Dockerfile.

**Base image.** `alpine` gives a shell for debugging at the cost of a package
manager, a libc and a larger attack surface. `gcr.io/distroless/static:nonroot`
has no shell, no package manager and no libc, which a `CGO_ENABLED=0` binary
does not need.

### Decision

- **Go 1.23 minimum**: declared as `go 1.23.0` in `go.mod`. CI builds on the
  declared floor so that the minimum is a tested claim rather than an assertion.
- **`CGO_ENABLED=0` for every build.** The Makefile exports it globally; only
  the `test` target overrides it, because `-race` requires cgo.
- **Version, commit and date injected via `-ldflags -X`** into
  `internal/buildinfo`, defaulting to `dev`/`unknown` for local builds.
- **Multi-stage container, final stage `gcr.io/distroless/static:nonroot`,**
  non-root UID 65532, read-only root filesystem, no shell.

### Consequences

- Release builds are a single `go build` per `GOOS`/`GOARCH` pair with no
  cross-toolchain, which is what makes the multi-arch release job (§17.4)
  small enough to maintain.
- There is no shell in the runtime image, so in-container debugging is done with
  an ephemeral debug container or `kubectl cp`. This is a deliberate trade and
  belongs in `docs/OPERATIONS.md` so it is not discovered during an incident.
- `go test -race` needs a working 64-bit C compiler on the developer's machine.
  This is a local toolchain requirement only; it does not reach any artifact.
  Documented in `CONTRIBUTING.md`.
- Any future dependency that requires cgo conflicts with this ADR and needs one
  that supersedes it.

---

## ADR-0006, Project name and Go module path

**Status:** Accepted
**Date:** 2026-07-30
**Phase:** 0
**PRD reference:** §25.4

### Context

PRD §25.4 requires the name to be fixed in Phase 0 and never changed, because a
rename scatters git history and rewrites every import path. The GitHub
repository was created as `github.com/nabrahma/Driftwatch`, with a capital `D`.
Go module paths are case-sensitive; the module proxy encodes uppercase letters
as `!d`, so the module path and the repository path should agree exactly.

### Options considered

**Option A, module path `github.com/nabrahma/driftwatch`, repository renamed to
lowercase.** Matches Go convention (module paths are lowercase by near-universal
practice), matches how the PRD spells the name throughout, and produces import
lines that read naturally. Requires renaming the GitHub repository; GitHub keeps
a redirect from the old name.

**Option B, module path `github.com/nabrahma/Driftwatch`.** Matches the
repository exactly as it exists today with no action required. Every import line
in the project then carries a capital letter, and the proxy path is `!driftwatch`.

**Option C, a different name from §25.4's alternatives (`skew`, `driftguard`,
`statediff`).** Only worth considering if `driftwatch` were taken or wrong.
It is neither.

### Decision

**Option A: the module path is `github.com/nabrahma/driftwatch`, all lowercase,
and the GitHub repository should be renamed to `driftwatch` to match.**

The cost of the rename is one click plus a GitHub-maintained redirect, and it is
at its lowest right now, Phase 0, before any published import path exists.
The cost of Option B is a capital letter in every import statement in the
repository, permanently, read by exactly the audience §1.5 is written for.

### Consequences

- The repository must be renamed on GitHub before the first push, or `go get`
  against the canonical lowercase path will not resolve. This is a prerequisite
  for the Phase 0 exit criterion "CI green on the first push".
- The name is now fixed. Per §25.4 it does not change again.
- Binaries are `driftwatch` and `driftwatch-manager`; the metric prefix is
  `driftwatch_`; the CRD group will follow the same spelling.

---

## ADR-0007, Phase 0 scaffold: what is stubbed and what is deferred

**Status:** Accepted
**Date:** 2026-07-30
**Phase:** 0
**PRD reference:** §7, §17.1, §20 Phase 0

### Context

PRD §7 specifies the repository layout exactly and requires deviations to be
recorded here. Phase 0 creates that tree before there is code to put in it, so
some files can be created honestly as empty package declarations and some
cannot: a `Dockerfile` that does not build, a `.goreleaser.yaml` that does not
release, or a Grafana dashboard JSON that renders nothing are not scaffolding,
they are files that look finished and are not.

The same tension exists in CI. §17.1's lint job runs four `hack/verify-*.sh`
scripts, three of which check things that do not exist until Phases 4, 5 and 6.
Phase 0's exit criteria require CI green on the first push.

### Options considered

**Option A, create every file in §7 now, with placeholder content.** The tree
matches the PRD exactly. It also means a `Dockerfile` that fails to build and a
dashboard JSON that is a stub, both of which read as broken rather than as
pending, and neither of which any check would catch.

**Option B, create only what Phase 0 builds; add the rest in the phase that
fills it.** Nothing in the tree is misleading. The tree does not match §7 until
Phase 8, and the difference has to be recorded, which is what this ADR is.

**Option C, create every file, and add a CI check that fails on placeholders.**
The check is itself work that Phase 0 does not need, and it exists only to guard
against a problem Option B does not have.

### Decision

**Option B, with the difference recorded here.**

Created in Phase 0: the full directory tree; every Go file as a package
declaration with a package doc comment; `go.mod`; `Makefile`; `.golangci.yml`;
`.github/workflows/ci.yaml`; `LICENSE`; `CONTRIBUTING.md`; a skeleton
`README.md`; the `docs/` skeleton; `hack/install-tools.sh`,
`hack/boilerplate.go.txt` and the four `hack/verify-*.sh` scripts.

Deferred to the phase that gives them content, each with its directory already
present:

| Path | Lands in |
|---|---|
| `Dockerfile` | Phase 8 (packaging) |
| `.goreleaser.yaml` | Phase 8 (release) |
| `PROJECT` (kubebuilder metadata) | Phase 6, written by kubebuilder |
| `config/{crd,rbac,manager,webhook,prometheus,samples}` contents | Phase 6, generated |
| `deploy/helm/driftwatch/` | Phase 8 |
| `deploy/grafana/driftwatch-dashboard.json` | Phase 8 |
| `test/e2e/manifests/*.yaml` | Phase 7 |
| `test/interop/publisher.py` | Phase 7 |
| `.github/workflows/{e2e,soak,release}.yaml` | Phases 7, 8 |

The three later-phase workflows are deliberately absent rather than stubbed:
GitHub Actions runs any workflow file it finds, so a stub is a red X on the
repository from the first push.

**Skip-guarded verify scripts.** `hack/verify-codegen.sh`,
`hack/verify-metrics-docs.sh` and `hack/verify-fault-matrix.sh` are wired into
CI now, and each detects whether its subject exists yet. If not, it prints an
explicit `SKIP... (Phase N)` line and exits 0. If the subject *does* exist and
the check is still unimplemented, it **fails**. That inversion is the point: the
scripts cannot rot into silent passes, because the moment the thing they guard
appears, CI goes red until the check is written.

`hack/verify-no-sleep.sh` is implemented for real now, not skip-guarded, because
§23 A2 wants the `time.Sleep` prohibition enforced from the first test written
in Phase 1, not retrofitted after the habit forms. It exempts `test/e2e` and
`test/soak`, where elapsed time is the subject rather than an accident.

**Coverage thresholds.** §17.1's unit job is specified to enforce the §16.9
per-package minimums. With no code, every package is at "no statements" and any
threshold script would either fail on everything or be written to pass on
nothing. The `unit` job runs the race-and-coverage command and uploads
`cover.out`; threshold enforcement is added in Phase 1 together with the first
covered package, where it can be tested against real numbers.

**One addition to §7:** `hack/verify-fault-matrix.sh` is required by §17.1 but
missing from §7's tree listing. It is created.

**One deviation inside `test/e2e`:** §7 lists `kind.go`, `fixtures.go` and
`diagnostics.go` alongside the `_test.go` files. The `_test.go` files carry the
`e2e` build tag so they never run in the unit suite. If the non-test files
carried it too, every file in the package would be excluded by default and
`go build ./...` would fail with "build constraints exclude all Go files". So
the tag is on the test files only, and `kind.go` carries the package doc
comment.

### Consequences

- The repository tree does not match §7 exactly until Phase 8. This ADR is the
  record of the difference, and the table above is the checklist for closing it.
- CI is green from the first push while genuinely running everything that has
  something to check, and the skip lines in the log say plainly what is not yet
  covered rather than implying it is.
- Each deferred file is now a phase exit criterion. If Phase 8 lands without a
  `Dockerfile`, the table above is the thing that catches it.

---

## ADR-0008, Publisher label limit lowered to 50 to fit the cardinality budget

**Status:** Accepted
**Date:** 2026-07-31
**Phase:** 5
**PRD reference:** §9 M12, §12

### Context

§9 M12 gives two numbers for the same mechanism, and they are incompatible.

The `publisher` label is allowed but must be bounded: "if distinct publishers
exceed `maxPublisherLabels` (default 100), collapse further ones into
`publisher="__other__"`". The same section then requires a cardinality test that
feeds 10,000 keys and 500 publishers and asserts "total time series is under a
fixed budget (e.g. 500)".

§12 defines seven metrics carrying the `publisher` label. Six of them are touched
by an ordinary ingest workload, and each costs `limit + 1` series once the
aggregate is counted. At a limit of 100 that is 606 series before any other
metric is counted at all, 21% over the whole budget, from the publisher label
alone.

Measured, at four limits:

| `maxPublisherLabels` | time series | within budget |
|---|---|---|
| 25 | 156 | yes |
| 50 | 306 | yes |
| 75 | 456 | yes |
| 100 | 606 | **no** |

### Options

**A. Keep the limit at 100 and raise the budget.** Honest about the arithmetic,
but the budget is the number with real consequences. driftwatch is deployed
beside the store it audits, often one replica per node, so its series count is
multiplied by the fleet before it reaches Prometheus. A tool costing a hundred
series per replica becomes the monitoring incident it was deployed to detect.

**B. Drop some of the publisher-labelled metrics.** §12 names them explicitly
and per-publisher rates are the point of most of them: `seq_gaps_total` without a
publisher label cannot tell an operator which replica is lossy, which is the
question it exists to answer.

**C. Lower the default limit so both numbers hold.** Costs individual graphs for
publishers 51 through 100 in a fleet that large, and nothing else:
`driftwatch_publishers_tracked` still reports the true count when labels have
collapsed, and per-publisher detail past that point belongs in
`driftwatch explain` and the logs rather than in a metric label.

### Decision

**C.** `metrics.DefaultMaxPublisherLabels` is 50. The budget of 500 is enforced
by `TestMetrics_CardinalityStaysBoundedUnderTenThousandKeys` exactly as §9 M12
specifies it, and the whole registry has a second stated ceiling of 700 in
`TestMetrics_CardinalityStaysBoundedWithEveryMetricExercised`.

The limit remains configurable through `metrics.Options.MaxPublisherLabels`, so a
deployment that genuinely wants per-publisher detail on a hundred replicas can
have it deliberately.

### Consequences

- A fleet of more than fifty publishers loses individual time series for the
  excess, aggregated under `publisher="__other__"` and logged once.
- The full-registry cardinality test is the standing guard on the relationship.
  Adding an eighth publisher-labelled metric, or raising the default back to 100,
  moves a number that test asserts on, which forces the trade-off to be made
  deliberately rather than discovered by a Prometheus that fell over.
- Recorded in `docs/DISCOVERIES.md` D-012 with the measurement that produced it.

---

## ADR-0009, `Projection` grows a `TargetKey` method

**Status:** Accepted
**Date:** 2026-07-31
**Phase:** 5
**PRD reference:** §9 M6, §9 M14

### Context

§9 M6 defines the projection interface as `Apply(prev event.Value, e *event.Event)`,
where `prev` is the key's current value. That signature requires the caller to
fetch `prev` before calling, and gives it no way to know which key to fetch.

The store key is the projection's `keyTemplate` applied to the event, and the
event carries a raw key which the template rewrites. With the §25.2 example
configuration, `keyTemplate: "block:{{.Key}}"`, an event carrying key `9f3a`
becomes `block:9f3a` in the store.

The composition root looked up the raw key, missed on every event, and handed
`Apply` an absent previous value, so every event overwrote rather than
accumulated. See D-013 for the evidence.

### Options

**A. Call `Apply` twice: once with an absent value to learn the key, then again
with the real one.** Works because the projections are pure, and doubles the cost
of the hottest path in the system to work around an interface gap.

**B. Expand the key template in the composition root.** Duplicates projection
logic outside the projection, and gets it wrong the moment a projection derives
its key from something other than the template.

**C. Add `TargetKey(e *event.Event) (string, error)` to the interface.** One more
method on three implementations, each a single line delegating to the same
expander `Apply` uses.

### Decision

**C.** The addition is recorded here because §9 M6 specifies the interface and
§1.1.9 requires deviations to be written down.

### Consequences

- Anyone adding a projection implements one more method, and the compiler says so.
- The gap is closed at the type level rather than by a comment asking callers to
  remember, which is what let it go unnoticed through four phases.
- `pkg/check` and the scenario harness both resolve the store key before the
  lookup; the harness had the same latent bug, invisible only because none of its
  projections were configured with a key template.

---

## ADR-0010, Phase 6 additions: a reorder buffer, an AwaitingSnapshot phase, and one extra test file

**Status:** Accepted
**Date:** 2026-08-01
**Phase:** 6
**PRD reference:** §7, §9 M6, §10.1, §15

### Context

Writing the sixty rows of §15 turned up four places where the PRD's own
statements were not consistent with each other or with the code. Each needed a
decision rather than a test that quietly asserted less.

**1. `Commutative()` was declared and never consumed.** §9 M6 gives every
projection a `Commutative` method and says that when it reports false "the
oracle must order by seq before applying". Nothing did. §15 row 7 requires that
reordering driftwatch's stream produces zero findings, and without ordering it
produces a permanently wrong oracle.

**2. §15 row 46 names a phase §10.1 does not list.** The phase enumeration is
`Pending | Bootstrapping | Watching | Degraded | Paused | Failed`; row 46
expects `Phase: AwaitingSnapshot`.

**3. §10.2 blocks bootstrap `Strict` on the canonical codec.** The rule is that
`Strict` requires `codec.opMapping` to define `snapshotBegin` and
`snapshotEnd`. But every registered codec already recognises driftwatch's own
operation names, so with no `opMapping` at all a snapshot cycle is recognised
perfectly well, and the rule made row 46 unconfigurable.

**4. §7 lists fourteen files for `test/faults/` and the matrix has sixty rows.**
Three of them, two publishers on one key, a heartbeat-only stream, fifteen
hundred publishers, are about the publisher population rather than about a
fault applied to a stream.

### Decision

**1. A bounded reorder buffer in the ingest path** (`pkg/check/reorder.go`), on
by default for any non-commutative projection, with a two-second window
configurable as `policy.reorderWindow`. An event that arrives ahead of its
predecessor waits, and stops waiting on whichever comes first: the predecessor
arriving, the window expiring, or the per-publisher buffer filling. When the
wait times out the hole is a real gap and seqtrack records it as one.

The alternative, leaving it out and testing row 7 with a pair that happens to
commute, would have left the tool wrong on any transport that reorders, which
is all of them.

**2. `PhaseAwaitingSnapshot` is added**, deviating from §10.1's list. §15 row 46
is the more specific requirement and the more useful behaviour: a check that
reported `Watching` while deliberately asserting nothing would read as a clean
bill of health. `Status.AwaitingSnapshot` carries the same fact independently of
the run loop, which is what the CRD condition in Phase 7 will map onto.

**3. §10.2's `Strict` rule now applies only when an `opMapping` is configured.**
The rule's purpose is "you must be able to recognise a snapshot cycle". With no
mapping the canonical names are in force and that is already true; with a
mapping, the operator has taught driftwatch a foreign vocabulary and has to
teach it the snapshot markers too, or `Strict` would wait forever for a cycle it
cannot see. The error is unchanged for that case.

**4. `test/faults/publisher_test.go` is added**, holding rows 25 to 27. Filing
them under `restart_test.go` or `clockskew_test.go` would put them where nobody
would look. §7's file list predates the matrix having sixty rows.

### Consequences

- Out-of-order delivery no longer corrupts the oracle, at the cost of up to
  `reorderWindow` of added latency for the events that were out of order, and
  a gap is now declared one window later than it used to be, which is strictly
  more accurate.
- An undecodable frame leaves a hole nothing can fill, so the events behind it
  wait out the window. Bounded, and the price of not treating every reordered
  pair as loss. Tested by §15 rows 15 and 16.
- Phase 7's CRD status enum gains a value, and its conditions gain
  `AwaitingSnapshot` and `MultiWriterUnsafe`.
- `hack/verify-fault-matrix.sh` enforces the sixty rows from outside the
  compiler and `TestFaultMatrix_Coverage` from inside, so neither a renamed test
  nor a deleted one can leave a row silently uncovered.

---

## ADR-0011, Phase 7: the webhook delegates, the schema defaults, and the operator opts in

**Status:** Accepted
**Date:** 2026-08-01
**Phase:** 7
**PRD reference:** §10.1, §10.2, §10.3, §18

### Context

Four decisions in Phase 7 went against the obvious reading of the PRD, or
against what kubebuilder's scaffolding would have produced. Each is written down
because the alternative is defensible and somebody will otherwise change it back.

**1. The validating webhook reimplements nothing.** §10.2 lists twenty rules and
asks for a unit test per rule in `api/v1alpha1/driftcheck_webhook_test.go`, which
reads as an instruction to write the rules there. Every one of them is already
implemented in `pkg/check`, because §11 requires the CLI to reject the same files
without a cluster.

Two implementations of one rule set diverge, and which one an operator hits
depends on whether they ran `driftwatch watch -f` or `kubectl apply`. So the
webhook converts the CRD spec to a `check.Spec` and delegates, translating field
paths into the API's form. It adds exactly two things that have no meaning
outside Kubernetes: parsing the string-encoded decimals the API convention
requires instead of floats, and comparing against a stored object for the
immutability rule.

The twenty tests are still there, each asserting the exact sentence, the rule is
what is under test, not where it lives.

**2. `codec`, `policy`, `alert` and `settlementWindow` default to `{}`.**
Structural-schema defaulting does not descend into an absent field, so a spec
that omitted `policy` came back from the API server with no `policy` at all and
none of its twenty defaults applied. §10.2 asks that
`kubectl get driftcheck -o yaml` show what is actually running; without the
empty-object default it showed the sparse thing the operator typed. See
docs/DISCOVERIES.md D-018.

The cost is that these four blocks are always present in the stored object, even
when the operator set nothing in them. That is the point.

**3. Leader-election leases are not in the generated ClusterRole.** §10.3 lists
`leases` alongside the rest of the RBAC, and kubebuilder's scaffold puts them in
the same ClusterRole. The manager needs exactly one lease, in its own namespace.
Cluster-wide lease write would let a compromised driftwatch evict the leader of
every other operator in the cluster, a far more interesting capability to
obtain than read access to an event stream.

So the marker is removed and `config/rbac/leader_election_role.yaml` grants it as
a namespaced Role. The generated `role.yaml` is correspondingly smaller, which
is the whole idea of committing it: what it contains is what the manager can do.

**4. The webhook and the Prometheus resources are opt-in, and `config/default`
has neither.** Both need something the cluster may not have, a certificate
source, and the Prometheus operator's CRDs, and a base that assumes them fails
on exactly the clusters people try first. `kubectl apply -k config/default`
against a bare Kind cluster has to work, or the first five minutes are spent
debugging driftwatch's packaging rather than driftwatch.

The manager therefore runs with `--enable-webhooks=false` in that base, and the
CRD schema carries the enums, ranges and per-field defaults so a malformed spec
is still rejected without admission. What is lost is §10.2's cross-field
validation, and `NOTES.txt` and the base's own comments say so plainly, the
`ingestBufferSize >= recvHWM` rule in particular, whose whole point is that
violating it is silent.

### Consequences

- A rule added to `pkg/check` is enforced by the webhook with no change here,
  and a rule that only makes sense in the cluster has to be added deliberately.
- The four defaulted blocks always appear in a stored `DriftCheck`. Anything
  diffing specs must expect them.
- A Helm install and a kustomize install grant the same permissions, and
  `hack/verify-helm-rbac.sh` fails CI if they stop agreeing, the chart cannot
  include a file from outside itself, so its copy of the rules is real
  duplication that needs guarding.
- Production installs should turn the webhook on. `values-prod.yaml` does, with
  cert-manager.

---

## ADR-0012, The Go floor moves from 1.23 to 1.25, because 1.23 cannot be patched

**Status:** Accepted
**Date:** 2026-08-02
**Phase:** 9
**PRD reference:** §8.5, §18
**Supersedes:** the Go-version half of ADR-0005

### Context

Wiring `govulncheck` into CI, a §22 box that had never been ticked, turned up
four vulnerabilities reachable from code this project actually calls. One of
them is reachable from the shipped manager binary:

```text
Vulnerability #4: GO-2026-4918
  Infinite loop in HTTP/2 transport when given bad SETTINGS_MAX_FRAME_SIZE
  Module: golang.org/x/net
    Found in: golang.org/x/net@v0.38.0
    Fixed in: golang.org/x/net@v0.53.0
    Example trace:
      cmd/driftwatch-manager/main.go:223:21: driftwatch.run calls
        manager.controllerManager.Start, which eventually calls
        http2.Transport.NewClientConn
```

That is not a test-only path. controller-runtime talks to the API server over
HTTP/2, so every driftwatch-manager pod holds a connection through the affected
transport for its entire life.

The problem is that it cannot be fixed on Go 1.23:

| Module | Fixed in | That version declares |
|---|---|---|
| `golang.org/x/net` | v0.53.0 | `go 1.25.0` |
| `golang.org/x/net` | v0.55.0 | `go 1.25.0` |
| `golang.org/x/text` | v0.39.0 | `go 1.25.0` |

There is no intermediate version. Every release carrying the fix also carries
the newer language floor.

### Options considered

**Hold 1.23 and document the exposure.** ADR-0005's reasoning is still sound on
its own terms, each bump narrows who can build the project, and 1.23 was chosen
because range-over-func and `slices`/`maps` are what the design needs and nothing
more. Holding it would mean a `KNOWN_GAPS.md` entry describing a reachable DoS in
the manager's connection to the API server, and a CI scanner set to report rather
than fail.

Rejected. A monitoring tool whose failure mode is going quiet is exactly the kind
of thing that must not hang: driftwatch reporting nothing is indistinguishable
from driftwatch reporting no drift, which is the failure this whole project
exists to make visible. Accepting a hang in the manager's watch to preserve a
build-compatibility preference gets the priority backwards.

**Bump only the release build.** Keep `go.mod` at 1.23 so the source still
compiles on older toolchains, and build the published binaries and image with
1.25. Rejected because it makes `go install` a trap: a user on Go 1.23 would get
a binary containing the vulnerability, from a repository whose CI badge says the
supply chain is clean, and nothing would tell them.

**Bump the floor.** Accepted.

### Decision

- **Go 1.25 minimum**: declared as `go 1.25.0` in `go.mod`. CI, both Dockerfiles,
  the release workflow and CONTRIBUTING.md all move together, so the floor stays
  a tested claim.
- **`golang.org/x/net` at v0.56.0 and `golang.org/x/text` at v0.39.0**: the
  lowest pair that resolves together and fixes all three module vulnerabilities.
- **`govulncheck` fails CI**: rather than reporting. A scanner set to
  `continue-on-error` is worse than no scanner: the badge claims the supply chain
  is checked while nothing checks it.

### Consequences

- Anyone on Go 1.23 or 1.24 can no longer build from source. This is a real cost
  and the reason ADR-0005 argued against bumping; it is paid here because the
  alternative is shipping a known hang.
- ADR-0005's other three decisions, `CGO_ENABLED=0`, ldflags version injection,
  distroless runtime, are unaffected and still stand.
- One vulnerability remains in every scan: `GO-2026-5856` in `crypto/tls`, fixed
  in go1.26.5. That is a property of the toolchain running the build rather than
  of anything declared here, and it resolves when the builder's patch release
  moves. It is not something `go.mod` can express.
- The `-race` requirement on a 64-bit cgo toolchain is unchanged, which is why
  the Makefile now takes `RACE=` for contributors whose local compiler cannot
  provide one.
