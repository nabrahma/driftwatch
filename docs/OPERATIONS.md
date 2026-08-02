# Operations runbook

One section per alert in `config/prometheus/rules.yaml`. Every `runbook_url` in
that file anchors here.

These entries are deliberately short. The full treatment PRD §21.5 asks for —
ranked causes with a confirmation step for each — lands in Phase 9. What is here
exists because Phase 7 shipped the alerts, and an alert whose runbook link 404s
at three in the morning is worse than one with no link at all.

## Two things that change how everything below reads

**driftwatch reports two numbers and they are not interchangeable.**
`divergentKeys` is what driftwatch will stand behind: confirmed across two reads
separated by a settlement window. `suspectDivergentKeys` is divergence on keys
whose event stream driftwatch knows it partly missed — that number measures
driftwatch's own event loss, not the store's correctness. Nothing here pages on
it, and nothing should.

**A drift alert during a target outage is real and predates the outage.**
driftwatch reports no new findings while it cannot read the store (§6.4, §23 A5),
so the counts you can see are frozen at the last thing it actually knew. If a
drift alert and a Redis incident coincide, the drift came first.

## Debugging in-cluster

The image is distroless: no shell, no package manager, nothing to `exec` into.
That is deliberate (ADR-0005, §18) and worth knowing before an incident.

```sh
# An ephemeral container alongside the manager, sharing its process namespace.
kubectl -n driftwatch-system debug -it deploy/driftwatch-manager \
  --image=busybox:1.36 --target=manager

# The CLI ships in the same image, so a debug container built from it has
# `driftwatch explain` already in place.
kubectl -n driftwatch-system debug -it deploy/driftwatch-manager \
  --image=ghcr.io/nabrahma/driftwatch:latest --target=manager
```

Most questions are answered without any of that:

```sh
kubectl get driftchecks -A
kubectl -n <ns> describe driftcheck <name>     # conditions and events
kubectl -n driftwatch-system logs -l app.kubernetes.io/name=driftwatch -f
```

---

## DriftwatchDriftDetected

**Means:** `driftwatch_divergent_keys > 0` for five minutes. Two reads a
settlement window apart both found the target disagreeing with what the event
stream says it should hold.

**Check first:** `kubectl -n <ns> describe driftcheck <name>` — the
`divergenceByCategory` breakdown usually names the cause before you open
anything else. `missing` on every key points at the materializer;
`member_subset` points at partial application; `extra` points at something
writing to the store that is not on the stream.

**Then:** `driftwatch explain --key <key>` for the per-key event history and the
diagnosis. It names which of the §13 rules fired and with what confidence.

**Likely causes:** a materializer that stopped or is failing a subset of writes;
a second writer to the same keyspace; a deploy that changed the event format
without changing `codec.fieldMapping`.

## DriftwatchDriftSevere

**Means:** over 1% of tracked keys have diverged. A ratio rather than a count,
because a threshold that suits ten thousand keys is meaningless at ten million.

**Check first:** whether `coverageRatio` is high. Severe divergence under low
coverage is a much weaker statement than severe divergence under full coverage.

**Likely causes:** at this scale it is systemic rather than per-key — a
materializer that stopped entirely, a failover to a stale replica (check
`status.targetRole`, and consider `policy.requirePrimary`), or a `FLUSHDB`.

## DriftwatchEventLoss

**Means:** gaps in a publisher's sequence numbers. This is **not** drift. It is
driftwatch saying its own view of the stream is incomplete, so the affected keys
are marked suspect and excluded from the alertable count.

**Check first:** `status.publishers` — which publisher, and how many events. The
`SequenceIntegrity` condition names the worst one.

**Likely causes:** the ingest buffer overflowing (see
`DriftwatchIngestBackpressure`); a flapping subscription (`sourceReconnects`
climbing); a publisher restarting without declaring a new epoch; genuine
transport loss under load.

**Note:** the audit now has a hole in it, which is more urgent than it looks.
driftwatch cannot vouch for part of the keyspace until the affected publisher
retransmits.

## DriftwatchIngestBackpressure

**Means:** events are being dropped because the applier cannot keep up and the
ingest buffer is full. Critical, because it is self-inflicted: every dropped
event makes more of the keyspace suspect, so the audit degrades under exactly
the load it is most needed at.

**Check first:** `driftwatch_ingest_queue_depth` against
`policy.ingestBufferSize`, and the manager's CPU against its limit.

**Fix:** raise `policy.ingestBufferSize`, give the manager more CPU, or split
the check. If `source.zmq.recvHWM` was raised without raising the buffer, the
webhook would have rejected it — unless the webhook is not installed, which is
worth checking first.

## DriftwatchOracleSaturated

**Means:** the oracle is evicting keys to stay inside `policy.maxTrackedKeys`.
Every finding now covers only part of the store, and every clean report is a
statement about a subset.

**Check first:** `status.trackedKeys` against `policy.maxTrackedKeys`, and
`status.targetKeyspaceSize` against both.

**Fix:** raise `maxTrackedKeys` — budget roughly 700 bytes per key, and raise the
memory limit with it — or narrow `target.redis.keyPattern` to the keyspace this
check actually owns. A pattern matching more than the check is responsible for
is the more common cause.

## DriftwatchLowCoverage

**Means:** fewer than 80% of tracked keys are being compared. Informational.

**Check first:** whether the uncompared keys are in flight (`inFlightKeys`, which
means the settlement window is wide relative to the write rate) or adopted
(`bootstrap: Adopt` never asserts on keys it read at startup).

**Why it matters anyway:** it is the number that says how much a clean report is
worth. A low divergence count under low coverage is not the reassurance it looks
like.

## DriftwatchTargetUnavailable

**Means:** the store cannot be read. No comparison is running.

**Check first:** the store itself. driftwatch is the messenger here.

**While it is firing:** the divergence counts on the object are frozen, and every
other driftwatch alert means nothing. driftwatch deliberately does not report an
unreachable store as a store full of missing keys.

## DriftwatchSourceDisconnected

**Means:** the event subscription is down. The oracle is no longer being updated,
so it drifts further from the truth for as long as this lasts.

**Check first:** `status.conditions[SourceConnected].message` carries the
transport's last error. Then whether the publisher pods moved — DNS is
re-resolved on every reconnect, so a rescheduled pod is found again, but only
once it is up.

**Note:** findings raised during a disconnection are about driftwatch's stale
copy, not about the store. Treat a drift alert that starts here with suspicion.

## DriftwatchSettlementWindowAtMax

**Means:** the adaptive window has widened to `policy.settlementWindow.max`.
Measured convergence is slower than driftwatch was configured to tolerate, and
real drift now takes at least this long to surface.

**Check first:** `status.convergenceP99Seconds`. Near the ceiling means the
materializer has genuinely slowed; far below it means something transient pushed
the window up and it will come back down.

**Fix:** either the materializer, or `settlementWindow.max` if the ceiling was
set for a system that no longer exists.

## DriftwatchSweepsSkipped

**Means:** a sweep was still running when the next was due. Detection latency is
now the sweep duration rather than `policy.sweepInterval`, and the gap widens as
the keyspace grows.

**Check first:** `status.lastSweepDurationSeconds` against
`policy.sweepInterval`.

**Fix:** raise `sweepInterval` to something the sweep can finish inside, raise
`target.redis.readBatchSize` so it finishes faster, or accept that this keyspace
has outgrown one check and split it by key pattern.

---

## Capturing the dashboard screenshot

The README shows the dashboard during an injected fault. Here is how that image
is produced, so it can be refreshed when the panels change.

### 1. Bring the stack up

```sh
make demo
```

Six containers: Redis, a publisher, a materializer, driftwatch, Prometheus and
Grafana. The command waits until all of them are healthy, then prints the URLs.

### 2. Let the oracle fill

Wait about 60 seconds. driftwatch has to see each key at least once before its
expectation means anything, and a screenshot taken too early shows a coverage
ratio still climbing rather than a steady state.

Open <http://localhost:3000>. Grafana is configured for anonymous admin with the
login form disabled, so it opens directly on the dashboard. No credentials.

Wait until row one shows coverage above 0.99 and divergent keys at zero. That is
the "before" state, and it is worth confirming rather than assuming: a
screenshot of drift against a half-filled oracle proves nothing.

### 3. Break it

```sh
make demo-inject-drift
```

This deletes 400 keys straight out of Redis, behind driftwatch's back. Pass a
different count as the first argument if you want a larger fault.

### 4. Take the shot within about 90 seconds

The drift is real and it heals. The publisher keeps running, so every deleted
key is rewritten correctly within a few minutes and the count falls back to
zero. That recovery is a feature and it is also a deadline for the screenshot.

What the image should show:

- **Row 1, "Verdict"** is the one that matters. Coverage ratio high, divergent
  keys visibly risen. This row leads the dashboard because a divergence count
  means nothing without the coverage it was measured over.
- Enough of **row 2** ("Sequence integrity") to show gaps at zero. That is what
  says the drift is the store's fault and not driftwatch's, which is the whole
  distinction the tool exists to draw.

Capture the browser viewport, not the whole screen. Two rows is enough; the
remaining three are detail and shrink the readable part of the image.

### 5. Save it

```text
docs/evidence/dashboard-drift-detected.png
```

The README already references that exact path. Once the file exists, remove the
`SCREENSHOT SLOT` comment markers around the image in README.md.

Add a row to `docs/evidence/README.md` naming what the image shows and the
command that produced it, the same as every other artifact in that directory.

### 6. Tear it down

```sh
make demo-down
```

Removes the containers and their volumes.

### If the graphs stay empty

Check that Prometheus is scraping: <http://localhost:9090/targets> should show
the driftwatch target as UP. If it is not, `make demo-logs` will show why.
Raw metrics are at <http://localhost:9091/metrics>.
