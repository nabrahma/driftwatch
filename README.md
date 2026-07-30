# driftwatch

> Detects when a system's index quietly stops matching reality.

**Status: in development.** Phase 0 of 9 (scaffold) is complete. There is no
working tool yet. This README is a skeleton — it is written properly in Phase 9,
against measured numbers, per [PRD §21.1](docs/PRD.md).

Nothing below claims a capability that exists today.

---

## The problem

Imagine a city library with twelve branches. One central catalog says where each
book is. Branches never update the catalog directly — when a book moves, the
branch announces it over a city-wide PA system, and a small program listens and
updates the catalog.

The PA system is a broadcast. Nobody confirms receipt. So if the listener is
busy for two seconds it misses announcements entirely, and they are simply gone.
If two announcements arrive out of order the catalog settles on the wrong
answer. If someone edits the catalog by hand, no announcement was ever made and
the catalog and the shelves disagree forever.

Here is what makes it hard: **nothing breaks loudly.** The catalog still returns
an answer, the website still works, every dashboard is green. Some percentage of
the time a patron is simply sent to the wrong branch.

The same shape is everywhere in software. In LLM inference serving, model
replicas publish KV-cache block ownership over ZeroMQ, a materializer maintains
a Redis index of `block_hash → replica`, and a router reads that index to decide
where to send a request. When the index drifts, the router sends work to a
replica that no longer holds the block. The symptom is a slightly worse cache
hit rate and a slightly worse p99 — no error, no obvious place to look, and
weeks of debugging.

## What driftwatch does

- **Keeps an independent oracle.** It subscribes to the same event stream and
  folds it into its own expectation of what the target store should contain.
- **Compares, carefully.** It periodically diffs that expectation against the
  real store — with a settlement window, two-phase confirmation and version
  fencing, so legitimate lag is not reported as drift.
- **Explains a single key.** For any key, it replays every event it observed,
  with sequence numbers and the resulting state after each, so you can see which
  event was lost or arrived out of order.

It never writes to the target store. Read-only is a feature, not a limitation.

## Quick start

_Not yet available. Lands in Phase 8 as `make demo`._

## Example output

_Not yet available. Lands in Phase 5 (`driftwatch explain`)._

## How it avoids false positives

This is the substance of the project. Naively diffing an event-derived oracle
against a target store produces an avalanche of false positives: driftwatch is
always ahead of the real materializer, the store scan is a smear across time
rather than a snapshot, the oracle keeps moving while the scan runs, and
driftwatch's own subscription drops events too — so when the two disagree, it is
not automatically the store that is wrong.

Six mechanisms make the comparison sound: per-publisher sequence numbers with an
explicit trust state, a settlement window measured in local receive time,
two-phase confirmation before anything is reported, version-fenced comparison,
bootstrap modes for attaching to a running system, and explicit TTL and eviction
handling.

The one that matters most is the trust state: driftwatch tracks when *its own*
view has gaps, and never claims the target is broken while it knows it is
missing events.

See [docs/CORRECTNESS.md](docs/CORRECTNESS.md) — written in Phase 9. Until then,
[PRD §5](docs/PRD.md) is the reference.

## Architecture

_Diagram and data flow land in Phase 9. See [PRD §6](docs/PRD.md)._

## Metrics and dashboard

_Land in Phases 5 and 8._

## Configuration

_The `DriftCheck` custom resource lands in Phase 6. See [PRD §10](docs/PRD.md)._

## Measured performance

_No numbers yet. This section will contain measurements with the machine spec
they were taken on, and nothing else._

## Key discoveries

_See [docs/DISCOVERIES.md](docs/DISCOVERIES.md). No entries yet._

## Testing

_No tests yet. Phase 1 begins the domain packages._

Running what exists:

```sh
make install-tools
make lint test
```

## Limitations

Deliberate scope boundaries, from [PRD §3.2](docs/PRD.md):

- **No repair.** driftwatch never writes to the target store. Auto-repair needs
  domain knowledge it does not have, and a detector that can also mutate is a
  detector nobody deploys.
- **Not a replacement for the materializer.** It observes alongside the consumer
  that writes to the target; it does not become it.
- **It cannot fix a lossy channel**, only detect and quantify the loss.
- **One check runs in one process.** Multiple checks spread across replicas via
  per-check leader election, but a single keyspace is not sharded.
- **No web UI.** The CLI and a Grafana dashboard are the interfaces.
- **ZeroMQ and NATS**, plus in-memory and file-replay sources. Kafka is
  deliberately out of scope — consumer groups and offsets make it a materially
  different and easier problem.

Full list, as it grows: [docs/KNOWN_GAPS.md](docs/KNOWN_GAPS.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). The working agreement there is binding,
not advisory.

## License

Apache 2.0. See [LICENSE](LICENSE).
