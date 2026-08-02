# driftwatch metrics

<!-- Generated from pkg/metrics/registry.go. Do not edit by hand. -->

Regenerate with `hack/verify-metrics-docs.sh --write`; CI runs the same
script without the flag and fails if this file has drifted.

Every metric carries a `check` label naming the `DriftCheck` it belongs to,
except the process-level ones. No metric is ever labeled with a key name,
a member or a value: the keyspace is unbounded by construction, which is
what makes it worth auditing and what would make it catastrophic as a
label. The `publisher` label is bounded. Past the configured limit,
further publishers collapse into `publisher="__other__"`.

## Ingest

### `driftwatch_events_received_total`

**counter**. Events accepted from the source and decoded, by publisher and operation.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `publisher` | publisher id, bounded by `maxPublisherLabels` (default 50), then `__other__` |
| `op` | `unknown` `set` `delete` `add` `remove` `incr` `snapshot_begin` `snapshot_end` `heartbeat` |

### `driftwatch_events_dropped_total`

**counter**. Events driftwatch did not apply, by reason. Every increment is a hole in driftwatch's own view.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `publisher` | publisher id, bounded by `maxPublisherLabels` (default 50), then `__other__` |
| `reason` | `decode_error` `unknown_op` `too_large` `buffer_full` `duplicate` `stale_epoch` `validation_error` |

### `driftwatch_ingest_queue_depth`

**gauge**. Messages buffered between the source and the applier.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `stage` | `raw` `decoded` |

### `driftwatch_bytes_received_total`

**counter**. Payload bytes read off the transport.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |

## Sequence integrity

### `driftwatch_seq_gaps_total`

**counter**. Sequence gaps observed, by publisher. A gap means driftwatch missed events and cannot vouch for the affected keys.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `publisher` | publisher id, bounded by `maxPublisherLabels` (default 50), then `__other__` |

### `driftwatch_seq_missing_events`

**gauge**. Sequence numbers currently unaccounted for, by publisher.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `publisher` | publisher id, bounded by `maxPublisherLabels` (default 50), then `__other__` |

### `driftwatch_seq_epoch`

**gauge**. The incarnation each publisher currently declares. A change is a restart.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `publisher` | publisher id, bounded by `maxPublisherLabels` (default 50), then `__other__` |

### `driftwatch_seq_high_water_mark`

**gauge**. Highest sequence number seen from each publisher, within its current epoch.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `publisher` | publisher id, bounded by `maxPublisherLabels` (default 50), then `__other__` |

### `driftwatch_publisher_restarts_total`

**counter**. Publisher restarts, explicit (epoch bump) or implicit (sequence reset without one).

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `publisher` | publisher id, bounded by `maxPublisherLabels` (default 50), then `__other__` |
| `kind` | `explicit` `implicit` |

### `driftwatch_publisher_clock_skew_seconds`

**gauge**. Publisher wall clock minus driftwatch's, in seconds. Diagnostic only: settlement uses local receive time.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `publisher` | publisher id, bounded by `maxPublisherLabels` (default 50), then `__other__` |

### `driftwatch_publishers_tracked`

**gauge**. Publishers with sequence state.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |

### `driftwatch_gapset_truncated`

**gauge**. 1 when a publisher's gap interval list hit its bound, so the missing-event count is a floor.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `publisher` | publisher id, bounded by `maxPublisherLabels` (default 50), then `__other__` |

## Oracle

### `driftwatch_oracle_keys`

**gauge**. Keys tracked by the oracle, by trust state.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `trust` | `complete` `suspect` `adopted` |

### `driftwatch_oracle_settled_keys`

**gauge**. Keys whose last event is older than the settlement window, and so are eligible for comparison.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |

### `driftwatch_oracle_inflight_keys`

**gauge**. Keys changed within the settlement window. Disagreement on these is expected, not drift.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |

### `driftwatch_oracle_never_settled_keys`

**gauge**. Keys rescued by the stability window after staying in flight for a multiple of W.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |

### `driftwatch_oracle_evictions_total`

**counter**. Keys the oracle dropped to stay within maxTrackedKeys. Non-zero means coverage is incomplete.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |

### `driftwatch_oracle_apply_duration_seconds`

**histogram**. Time to fold one event into the oracle.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |

### `driftwatch_projection_errors_total`

**counter**. Events a projection refused, by reason.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `projection` | the registered projection name |
| `reason` | `unsupported_op` `invalid_event` `member_limit` `counter_saturated` `template_error` |

## Target

### `driftwatch_target_reachable`

**gauge**. 1 when the last health probe reached the store. While this is 0 driftwatch reports no findings at all.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |

### `driftwatch_target_errors_total`

**counter**. Failed store operations, by operation.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `op` | `get` `get_many` `scan` `ttl` `health` |

### `driftwatch_target_read_duration_seconds`

**histogram**. Store read latency, by operation.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `op` | `get` `get_many` `scan` `ttl` `health` |

### `driftwatch_target_keyspace_size`

**gauge**. Keys the store reports holding.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |

### `driftwatch_target_evictions_observed_total`

**counter**. Evictions the store reported. A sweep that finds mass absence while this is rising has an explanation that is not drift.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |

### `driftwatch_target_expirations_observed_total`

**counter**. Key expirations the store reported.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |

### `driftwatch_target_role`

**gauge**. 1 for the store's current replication role. Reads from a replica can be legitimately stale.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `role` | `master` `replica` `unknown` |

## Divergence

### `driftwatch_divergent_keys`

**gauge**. Confirmed divergent keys on which driftwatch is a reliable witness. This is the metric to alert on.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `category` | `missing_in_target` `extra_in_target` `value_mismatch` `member_mismatch` `type_mismatch` `ttl_mismatch` `counter_mismatch` |

### `driftwatch_suspect_divergent_keys`

**gauge**. Divergent keys whose events driftwatch knows it partly missed. Never alert on this: it measures driftwatch, not the store.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `category` | `missing_in_target` `extra_in_target` `value_mismatch` `member_mismatch` `type_mismatch` `ttl_mismatch` `counter_mismatch` |

### `driftwatch_advisory_divergent_keys`

**gauge**. Divergent keys adopted at bootstrap rather than derived from events.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `category` | `missing_in_target` `extra_in_target` `value_mismatch` `member_mismatch` `type_mismatch` `ttl_mismatch` `counter_mismatch` |

### `driftwatch_drift_episodes_total`

**counter**. Divergences that survived two-phase confirmation.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `category` | `missing_in_target` `extra_in_target` `value_mismatch` `member_mismatch` `type_mismatch` `ttl_mismatch` `counter_mismatch` |

### `driftwatch_drift_resolved_total`

**counter**. Confirmed divergences that later agreed again. A confirmed finding is a claim about one oracle version, and this is how it is withdrawn.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `category` | `missing_in_target` `extra_in_target` `value_mismatch` `member_mismatch` `type_mismatch` `ttl_mismatch` `counter_mismatch` |

### `driftwatch_drift_duration_seconds`

**gauge**. Age of the oldest unresolved drift episode.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |

### `driftwatch_transient_divergence_total`

**counter**. Candidates that stopped disagreeing before confirmation. A healthy pipeline produces these constantly: they are the false positives the §5 mechanisms suppressed.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `reason` | `resolved` `oracle_advanced` `key_evicted` `fence_failed` |

### `driftwatch_confirm_queue_depth`

**gauge**. Candidates awaiting their second read.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |

### `driftwatch_confirm_queue_dropped_total`

**counter**. Candidates discarded because the confirm queue was full. Under mass divergence the magnitude matters more than the per-key detail.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |

## Sweeps

### `driftwatch_sweeps_total`

**counter**. Sweeps run, by direction and outcome.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `kind` | `oracle_to_target` `target_to_oracle` |
| `result` | `success` `target_unavailable` `error` `aborted` |

### `driftwatch_sweeps_skipped_total`

**counter**. Sweeps skipped because the previous one was still running.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `kind` | `oracle_to_target` `target_to_oracle` |

### `driftwatch_sweep_duration_seconds`

**histogram**. Wall time of one sweep.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `kind` | `oracle_to_target` `target_to_oracle` |

### `driftwatch_sweep_keys_compared`

**gauge**. Keys compared in the last sweep.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |

### `driftwatch_coverage_ratio`

**gauge**. Fraction of tracked keys the last sweep actually compared. A high divergence count under a low coverage ratio is not what it looks like.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |

## Lag

### `driftwatch_convergence_seconds`

**histogram**. Delay between the oracle learning a value and the target holding it. The settlement window is derived from this distribution's p99.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |

### `driftwatch_settlement_window_seconds`

**gauge**. The settlement window currently in force.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |

### `driftwatch_lag_probe_timeouts_total`

**counter**. Convergence probes that never converged. Recorded at the maximum poll delay rather than discarded, because discarding them biases the window down exactly during an outage.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |

## Source

### `driftwatch_source_connected`

**gauge**. 1 per source endpoint that is currently connected.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `endpoint_index` | the endpoint's position in the source's endpoint list |

### `driftwatch_source_reconnects_total`

**counter**. Transport reconnects. Each one is a window in which messages were missed with no way to tell how many.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |

## Process

### `driftwatch_build_info`

**gauge**. Always 1, labeled with the build this process is running.

| Label | Values |
|---|---|
| `version` | fixed for the lifetime of the process |
| `commit` | fixed for the lifetime of the process |
| `goversion` | fixed for the lifetime of the process |

### `driftwatch_checks_active`

**gauge**. Checks currently running in this process.

No labels.

### `driftwatch_panics_total`

**counter**. Panics recovered, by component. Any increment is a bug.

| Label | Values |
|---|---|
| `check` | the DriftCheck name, bounded by how many checks are configured |
| `component` | `ingest` `applier` `sweeper` `lag` `source` `bootstrap` |

## Histogram buckets

**Latency**: `500µs` `1ms` `2.5ms` `5ms` `10ms` `25ms` `50ms` `100ms` `250ms` `500ms` `1s` `2.5s` `5s` `10s` `30s`

**Sweep duration**: `100ms` `250ms` `500ms` `1s` `2.5s` `5s` `10s` `30s` `1m0s` `2m0s` `5m0s`

**Convergence**: `1ms` `2.5ms` `5ms` `10ms` `25ms` `50ms` `100ms` `250ms` `500ms` `1s` `2.5s` `5s` `10s` `30s` `1m0s`

