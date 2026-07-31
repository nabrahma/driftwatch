# Evidence

Captured output backing every claim made in the README.

One file per claim, named `<id>-<slug>.<ext>`. Nothing is reconstructed after the
fact; output is captured when it is produced (PRD §21.4). If a claim in the
README has no row here, the claim comes out.

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
| `explain-dropped-event.txt` | `driftwatch explain` names the event the materializer did not apply | `internal/cli` |
| `phase5-redis-demo.txt` | `driftwatch watch -f examples/local.yaml` against a real Redis | `internal/cli` |
