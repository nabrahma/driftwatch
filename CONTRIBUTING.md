# Contributing to driftwatch

## Prerequisites

- **Go 1.25 or newer.** 1.25 is the declared minimum and CI builds on it.
- **A 64-bit C compiler**: for `go test -race` only. Nothing that ships needs
  cgo, `CGO_ENABLED=0` is the default for every build (`docs/DECISIONS.md`
  ADR-0005), but the race detector does.
- **GNU make and a POSIX shell.** On Windows use Git Bash or WSL; the Makefile
  recipes are `sh`, not `cmd`.
- **Docker**: for the integration and e2e suites. Not needed for `make test`.

```sh
make install-tools    # golangci-lint and gofumpt, pinned, into $(go env GOPATH)/bin
make lint test
```

### Windows: `-race` and the C compiler

If `make test` fails with `cc1.exe: sorry, unimplemented: 64-bit mode not
compiled in`, a 32-bit MinGW `gcc` is earlier in your `PATH` than a 64-bit one.
Either reorder `PATH` so the mingw-w64 toolchain wins, or point Go at it
directly:

```sh
export CC=x86_64-w64-mingw32-gcc
```

## The working agreement

These are not style preferences. They come from PRD §1 and §23, and each one
exists because the alternative has a specific failure mode.

**Test first.** The test file is written before the implementation file. A
module is not done until its tests pass under `-race`.

**No skipped tests.** A `t.Skip()` requires a linked entry in
`docs/KNOWN_GAPS.md`. A skipped test with no entry is a lie about coverage.

**Never mock the thing under test.** Mock the boundaries only: network, clock,
store.

**The clock is always injected.** No `time.Now()` outside `main()` and the clock
implementation itself. No `time.Sleep` in tests, `hack/verify-no-sleep.sh`
enforces this in CI. A suite that sleeps is slow, then flaky, then unrun, then
rotten.

**Never label a metric with a key name.** One `prometheus.Labels{"key": key}`
turns driftwatch into a cardinality bomb that takes down the monitoring system
it exists to inform.

**No unbounded collections.** A map that grows with the keyspace, a slice of all
findings, a per-key list of every event, each one is an out-of-memory kill in
production. Every collection has a documented bound.

**Prefer boring, obvious code.** The value of this project is its correctness
reasoning and its test suite, not clever Go. If a reviewer has to think hard
about *how* the code works, rewrite it.

**Record surprises immediately.** When something does not behave as expected, 
a library, a Redis command, a socket, it goes in `docs/DISCOVERIES.md` with the
reproduction, while you still have it. Reconstructing evidence later is painful
and the result is less convincing.

**Never invent evidence.** No entry describes something that did not happen; no
benchmark number is pasted without the run that produced it. Every claim traces
to a real file in `docs/evidence/`.

**No unbacked superlatives.** The marketing adjectives that promise maturity or
speed without a number behind them do not appear anywhere in this repository, 
`hack/verify-no-superlatives.sh` enforces the list and CI runs it. Write the
measured number instead, or write nothing.

## Dependencies

The allowed dependency list is closed (PRD §8.4, ADR-0004). Adding anything
outside it requires a new ADR in `docs/DECISIONS.md` stating what it does, what
it was chosen over, and what it pulls in transitively.

## Deviating from the PRD

If the PRD turns out to be wrong, do not silently work around it. Write the
problem into `docs/DECISIONS.md` as a new ADR, state the two or three options
with their trade-offs, pick the one that preserves testability, and proceed.

## Commits

Conventional commits, one logical change each, committed at every green
checkpoint:

```
feat:     a new capability
fix:      a bug fix
test:     tests only
docs:     documentation only
refactor: no behaviour change
chore:    build, CI, tooling
perf:     a measured performance change
```

Scope is the package where useful: `feat(oracle): version-fenced comparison`.

Do not leave `main` broken. If a phase cannot complete, commit the working
subset and record the gap in `docs/KNOWN_GAPS.md`.

## Before opening a pull request

```sh
make lint
make vet
make test
```

`make lint` runs `golangci-lint` with the config in `.golangci.yml` and checks
formatting with `gofumpt`. The enabled linter set is fixed by the PRD; changing
it requires an ADR.
