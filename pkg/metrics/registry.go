package metrics

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Kind is the Prometheus metric type of a definition.
type Kind uint8

// The metric kinds driftwatch declares. No summaries: their quantiles cannot be
// aggregated across replicas, and every latency question here is asked across
// replicas.
const (
	KindCounter Kind = iota
	KindGauge
	KindHistogram
)

var kindNames = [...]string{
	KindCounter:   "counter",
	KindGauge:     "gauge",
	KindHistogram: "histogram",
}

// String returns the Prometheus name of the kind.
func (k Kind) String() string {
	if int(k) >= len(kindNames) {
		return fmt.Sprintf("Kind(%d)", uint8(k))
	}
	return kindNames[k]
}

// Bucket sets from §12 M12. Named rather than repeated so a change lands on
// every metric that shares a shape.
var (
	// LatencyBuckets covers a read that should be sub-millisecond and a read
	// that has gone badly wrong, which is the range worth distinguishing.
	LatencyBuckets = []float64{
		.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30,
	}
	// SweepBuckets covers a sweep of a hundred keys and a sweep of a million.
	SweepBuckets = []float64{.1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300}
	// ConvergenceBuckets is the materializer's oracle-to-target lag, which
	// drives the settlement window and so needs resolution at the fast end.
	ConvergenceBuckets = []float64{
		.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60,
	}
)

// Def declares one metric.
//
// This table is the single source of truth: the collectors are built from it,
// docs/METRICS.md is generated from it, and hack/verify-metrics-docs.sh fails
// CI when the two drift apart. A metric that is not here does not exist.
type Def struct {
	Name   string
	Kind   Kind
	Help   string
	Labels []string
	// Buckets is set for histograms only.
	Buckets []float64
	// Enums documents the closed value set of a label. Every label whose values
	// driftwatch chooses rather than an operator must appear here.
	Enums map[string][]string
	// Section groups the metric in the generated documentation.
	Section string
}

// The label names used across the metric set. They are constants so that a
// typo is a compile error rather than a second, near-identical time series.
const (
	LabelCheck         = "check"
	LabelPublisher     = "publisher"
	LabelOp            = "op"
	LabelReason        = "reason"
	LabelStage         = "stage"
	LabelKind          = "kind"
	LabelTrust         = "trust"
	LabelProjection    = "projection"
	LabelCategory      = "category"
	LabelResult        = "result"
	LabelRole          = "role"
	LabelEndpointIndex = "endpoint_index"
	LabelComponent     = "component"
	LabelVersion       = "version"
	LabelCommit        = "commit"
	LabelGoVersion     = "goversion"
)

// AllowedLabels is the complete set of labels any driftwatch metric may carry.
//
// An allow-list rather than a list of banned names, and the difference is the
// whole point. §9 M12's rule is that a key name, a member or a value must never
// become a label, because a keyspace is unbounded by construction — that is
// what makes it worth auditing, and what makes it a Prometheus outage caused by
// the tool deployed to detect one. A deny-list only stops the spellings someone
// thought of; this stops every label that was not deliberately added here,
// which includes all the ones nobody thought of.
//
// Adding an entry is therefore a decision about cardinality, not a formality.
// Every name below is bounded by something concrete: a closed enum, the
// configured check count, the publisher limiter, or the process itself.
var AllowedLabels = map[string]string{
	LabelCheck:         "one per configured DriftCheck",
	LabelPublisher:     "bounded by maxPublisherLabels, then aggregated",
	LabelOp:            "closed enum",
	LabelReason:        "closed enum",
	LabelStage:         "closed enum",
	LabelKind:          "closed enum",
	LabelTrust:         "closed enum",
	LabelProjection:    "one per registered projection",
	LabelCategory:      "closed enum",
	LabelResult:        "closed enum",
	LabelRole:          "closed enum",
	LabelEndpointIndex: "one per configured source endpoint",
	LabelComponent:     "closed enum",
	LabelVersion:       "fixed for the process lifetime",
	LabelCommit:        "fixed for the process lifetime",
	LabelGoVersion:     "fixed for the process lifetime",
}

// The sections, in the order §12 lists them and the order METRICS.md renders
// them.
const (
	sectionIngest     = "Ingest"
	sectionSequence   = "Sequence integrity"
	sectionOracle     = "Oracle"
	sectionTarget     = "Target"
	sectionDivergence = "Divergence"
	sectionSweeps     = "Sweeps"
	sectionLag        = "Lag"
	sectionSource     = "Source"
	sectionProcess    = "Process"
)

var sectionOrder = []string{
	sectionIngest, sectionSequence, sectionOracle, sectionTarget,
	sectionDivergence, sectionSweeps, sectionLag, sectionSource, sectionProcess,
}

// defs is the metric set from §12, in the order it is listed there.
//
//nolint:lll // the help strings read better on one line than wrapped
var defs = []Def{
	// --- ingest ---
	{
		Name: "driftwatch_events_received_total", Kind: KindCounter, Section: sectionIngest,
		Help:   "Events accepted from the source and decoded, by publisher and operation.",
		Labels: []string{LabelCheck, LabelPublisher, LabelOp},
		Enums:  map[string][]string{LabelOp: opValues()},
	},
	{
		Name: "driftwatch_events_dropped_total", Kind: KindCounter, Section: sectionIngest,
		Help:   "Events driftwatch did not apply, by reason. Every increment is a hole in driftwatch's own view.",
		Labels: []string{LabelCheck, LabelPublisher, LabelReason},
		Enums:  map[string][]string{LabelReason: dropReasonValues()},
	},
	{
		Name: "driftwatch_ingest_queue_depth", Kind: KindGauge, Section: sectionIngest,
		Help:   "Messages buffered between the source and the applier.",
		Labels: []string{LabelCheck, LabelStage},
		Enums:  map[string][]string{LabelStage: stageValues()},
	},
	{
		Name: "driftwatch_bytes_received_total", Kind: KindCounter, Section: sectionIngest,
		Help:   "Payload bytes read off the transport.",
		Labels: []string{LabelCheck},
	},

	// --- sequence integrity ---
	{
		Name: "driftwatch_seq_gaps_total", Kind: KindCounter, Section: sectionSequence,
		Help:   "Sequence gaps observed, by publisher. A gap means driftwatch missed events and cannot vouch for the affected keys.",
		Labels: []string{LabelCheck, LabelPublisher},
	},
	{
		Name: "driftwatch_seq_missing_events", Kind: KindGauge, Section: sectionSequence,
		Help:   "Sequence numbers currently unaccounted for, by publisher.",
		Labels: []string{LabelCheck, LabelPublisher},
	},
	{
		Name: "driftwatch_publisher_restarts_total", Kind: KindCounter, Section: sectionSequence,
		Help:   "Publisher restarts, explicit (epoch bump) or implicit (sequence reset without one).",
		Labels: []string{LabelCheck, LabelPublisher, LabelKind},
		Enums:  map[string][]string{LabelKind: restartKindValues()},
	},
	{
		Name: "driftwatch_publisher_clock_skew_seconds", Kind: KindGauge, Section: sectionSequence,
		Help:   "Publisher wall clock minus driftwatch's, in seconds. Diagnostic only: settlement uses local receive time.",
		Labels: []string{LabelCheck, LabelPublisher},
	},
	{
		Name: "driftwatch_publishers_tracked", Kind: KindGauge, Section: sectionSequence,
		Help:   "Publishers with sequence state.",
		Labels: []string{LabelCheck},
	},
	{
		Name: "driftwatch_gapset_truncated", Kind: KindGauge, Section: sectionSequence,
		Help:   "1 when a publisher's gap interval list hit its bound, so the missing-event count is a floor.",
		Labels: []string{LabelCheck, LabelPublisher},
	},

	// --- oracle ---
	{
		Name: "driftwatch_oracle_keys", Kind: KindGauge, Section: sectionOracle,
		Help:   "Keys tracked by the oracle, by trust state.",
		Labels: []string{LabelCheck, LabelTrust},
		Enums:  map[string][]string{LabelTrust: trustValues()},
	},
	{
		Name: "driftwatch_oracle_settled_keys", Kind: KindGauge, Section: sectionOracle,
		Help:   "Keys whose last event is older than the settlement window, and so are eligible for comparison.",
		Labels: []string{LabelCheck},
	},
	{
		Name: "driftwatch_oracle_inflight_keys", Kind: KindGauge, Section: sectionOracle,
		Help:   "Keys changed within the settlement window. Disagreement on these is expected, not drift.",
		Labels: []string{LabelCheck},
	},
	{
		Name: "driftwatch_oracle_never_settled_keys", Kind: KindGauge, Section: sectionOracle,
		Help:   "Keys rescued by the stability window after staying in flight for a multiple of W.",
		Labels: []string{LabelCheck},
	},
	{
		Name: "driftwatch_oracle_evictions_total", Kind: KindCounter, Section: sectionOracle,
		Help:   "Keys the oracle dropped to stay within maxTrackedKeys. Non-zero means coverage is incomplete.",
		Labels: []string{LabelCheck},
	},
	{
		Name: "driftwatch_oracle_apply_duration_seconds", Kind: KindHistogram, Section: sectionOracle,
		Help:    "Time to fold one event into the oracle.",
		Labels:  []string{LabelCheck},
		Buckets: LatencyBuckets,
	},
	{
		Name: "driftwatch_projection_errors_total", Kind: KindCounter, Section: sectionOracle,
		Help:   "Events a projection refused, by reason.",
		Labels: []string{LabelCheck, LabelProjection, LabelReason},
		Enums:  map[string][]string{LabelReason: projectionErrorValues()},
	},

	// --- target ---
	{
		Name: "driftwatch_target_reachable", Kind: KindGauge, Section: sectionTarget,
		Help:   "1 when the last health probe reached the store. While this is 0 driftwatch reports no findings at all.",
		Labels: []string{LabelCheck},
	},
	{
		Name: "driftwatch_target_errors_total", Kind: KindCounter, Section: sectionTarget,
		Help:   "Failed store operations, by operation.",
		Labels: []string{LabelCheck, LabelOp},
		Enums:  map[string][]string{LabelOp: targetOpValues()},
	},
	{
		Name: "driftwatch_target_read_duration_seconds", Kind: KindHistogram, Section: sectionTarget,
		Help:    "Store read latency, by operation.",
		Labels:  []string{LabelCheck, LabelOp},
		Enums:   map[string][]string{LabelOp: targetOpValues()},
		Buckets: LatencyBuckets,
	},
	{
		Name: "driftwatch_target_keyspace_size", Kind: KindGauge, Section: sectionTarget,
		Help:   "Keys the store reports holding.",
		Labels: []string{LabelCheck},
	},
	{
		Name: "driftwatch_target_evictions_observed_total", Kind: KindCounter, Section: sectionTarget,
		Help:   "Evictions the store reported. A sweep that finds mass absence while this is rising has an explanation that is not drift.",
		Labels: []string{LabelCheck},
	},
	{
		Name: "driftwatch_target_expirations_observed_total", Kind: KindCounter, Section: sectionTarget,
		Help:   "Key expirations the store reported.",
		Labels: []string{LabelCheck},
	},
	{
		Name: "driftwatch_target_role", Kind: KindGauge, Section: sectionTarget,
		Help:   "1 for the store's current replication role. Reads from a replica can be legitimately stale.",
		Labels: []string{LabelCheck, LabelRole},
		Enums:  map[string][]string{LabelRole: roleValues()},
	},

	// --- divergence ---
	{
		Name: "driftwatch_divergent_keys", Kind: KindGauge, Section: sectionDivergence,
		Help:   "Confirmed divergent keys on which driftwatch is a reliable witness. This is the metric to alert on.",
		Labels: []string{LabelCheck, LabelCategory},
		Enums:  map[string][]string{LabelCategory: categoryValues()},
	},
	{
		Name: "driftwatch_suspect_divergent_keys", Kind: KindGauge, Section: sectionDivergence,
		Help:   "Divergent keys whose events driftwatch knows it partly missed. Never alert on this: it measures driftwatch, not the store.",
		Labels: []string{LabelCheck, LabelCategory},
		Enums:  map[string][]string{LabelCategory: categoryValues()},
	},
	{
		Name: "driftwatch_advisory_divergent_keys", Kind: KindGauge, Section: sectionDivergence,
		Help:   "Divergent keys adopted at bootstrap rather than derived from events.",
		Labels: []string{LabelCheck, LabelCategory},
		Enums:  map[string][]string{LabelCategory: categoryValues()},
	},
	{
		Name: "driftwatch_drift_episodes_total", Kind: KindCounter, Section: sectionDivergence,
		Help:   "Divergences that survived two-phase confirmation.",
		Labels: []string{LabelCheck, LabelCategory},
		Enums:  map[string][]string{LabelCategory: categoryValues()},
	},
	{
		Name: "driftwatch_drift_resolved_total", Kind: KindCounter, Section: sectionDivergence,
		Help:   "Confirmed divergences that later agreed again. A confirmed finding is a claim about one oracle version, and this is how it is withdrawn.",
		Labels: []string{LabelCheck, LabelCategory},
		Enums:  map[string][]string{LabelCategory: categoryValues()},
	},
	{
		Name: "driftwatch_drift_duration_seconds", Kind: KindGauge, Section: sectionDivergence,
		Help:   "Age of the oldest unresolved drift episode.",
		Labels: []string{LabelCheck},
	},
	{
		Name: "driftwatch_transient_divergence_total", Kind: KindCounter, Section: sectionDivergence,
		Help:   "Candidates that stopped disagreeing before confirmation. A healthy pipeline produces these constantly: they are the false positives the §5 mechanisms suppressed.",
		Labels: []string{LabelCheck, LabelReason},
		Enums:  map[string][]string{LabelReason: transientReasonValues()},
	},
	{
		Name: "driftwatch_confirm_queue_depth", Kind: KindGauge, Section: sectionDivergence,
		Help:   "Candidates awaiting their second read.",
		Labels: []string{LabelCheck},
	},
	{
		Name: "driftwatch_confirm_queue_dropped_total", Kind: KindCounter, Section: sectionDivergence,
		Help:   "Candidates discarded because the confirm queue was full. Under mass divergence the magnitude matters more than the per-key detail.",
		Labels: []string{LabelCheck},
	},

	// --- sweeps ---
	{
		Name: "driftwatch_sweeps_total", Kind: KindCounter, Section: sectionSweeps,
		Help:   "Sweeps run, by direction and outcome.",
		Labels: []string{LabelCheck, LabelKind, LabelResult},
		Enums: map[string][]string{
			LabelKind:   sweepKindValues(),
			LabelResult: sweepResultValues(),
		},
	},
	{
		Name: "driftwatch_sweeps_skipped_total", Kind: KindCounter, Section: sectionSweeps,
		Help:   "Sweeps skipped because the previous one was still running.",
		Labels: []string{LabelCheck, LabelKind},
		Enums:  map[string][]string{LabelKind: sweepKindValues()},
	},
	{
		Name: "driftwatch_sweep_duration_seconds", Kind: KindHistogram, Section: sectionSweeps,
		Help:    "Wall time of one sweep.",
		Labels:  []string{LabelCheck, LabelKind},
		Enums:   map[string][]string{LabelKind: sweepKindValues()},
		Buckets: SweepBuckets,
	},
	{
		Name: "driftwatch_sweep_keys_compared", Kind: KindGauge, Section: sectionSweeps,
		Help:   "Keys compared in the last sweep.",
		Labels: []string{LabelCheck},
	},
	{
		Name: "driftwatch_coverage_ratio", Kind: KindGauge, Section: sectionSweeps,
		Help:   "Fraction of tracked keys the last sweep actually compared. A high divergence count under a low coverage ratio is not what it looks like.",
		Labels: []string{LabelCheck},
	},

	// --- lag ---
	{
		Name: "driftwatch_convergence_seconds", Kind: KindHistogram, Section: sectionLag,
		Help:    "Delay between the oracle learning a value and the target holding it. The settlement window is derived from this distribution's p99.",
		Labels:  []string{LabelCheck},
		Buckets: ConvergenceBuckets,
	},
	{
		Name: "driftwatch_settlement_window_seconds", Kind: KindGauge, Section: sectionLag,
		Help:   "The settlement window currently in force.",
		Labels: []string{LabelCheck},
	},
	{
		Name: "driftwatch_lag_probe_timeouts_total", Kind: KindCounter, Section: sectionLag,
		Help:   "Convergence probes that never converged. Recorded at the maximum poll delay rather than discarded, because discarding them biases the window down exactly during an outage.",
		Labels: []string{LabelCheck},
	},

	// --- source ---
	{
		Name: "driftwatch_source_connected", Kind: KindGauge, Section: sectionSource,
		Help:   "1 per source endpoint that is currently connected.",
		Labels: []string{LabelCheck, LabelEndpointIndex},
	},
	{
		Name: "driftwatch_source_reconnects_total", Kind: KindCounter, Section: sectionSource,
		Help:   "Transport reconnects. Each one is a window in which messages were missed with no way to tell how many.",
		Labels: []string{LabelCheck},
	},

	// --- process ---
	{
		Name: "driftwatch_build_info", Kind: KindGauge, Section: sectionProcess,
		Help:   "Always 1, labeled with the build this process is running.",
		Labels: []string{LabelVersion, LabelCommit, LabelGoVersion},
	},
	{
		Name: "driftwatch_checks_active", Kind: KindGauge, Section: sectionProcess,
		Help:   "Checks currently running in this process.",
		Labels: nil,
	},
	{
		Name: "driftwatch_panics_total", Kind: KindCounter, Section: sectionProcess,
		Help:   "Panics recovered, by component. Any increment is a bug.",
		Labels: []string{LabelCheck, LabelComponent},
		Enums:  map[string][]string{LabelComponent: componentValues()},
	},
}

// Defs returns the metric definitions in declaration order.
func Defs() []Def {
	out := make([]Def, len(defs))
	copy(out, defs)
	return out
}

// Names returns every declared metric name, sorted.
func Names() []string {
	out := make([]string, 0, len(defs))
	for i := range defs {
		out = append(out, defs[i].Name)
	}
	sort.Strings(out)
	return out
}

// Markdown renders docs/METRICS.md from the definitions.
//
// Generating rather than hand-writing is the point: a metric renamed in code
// and not in the docs is how an operator ends up debugging a dashboard that
// queries a series nobody exports any more.
func Markdown() string {
	var b strings.Builder

	b.WriteString("# driftwatch metrics\n\n")
	b.WriteString("<!-- Generated from pkg/metrics/registry.go. Do not edit by hand. -->\n\n")
	b.WriteString("Regenerate with `hack/verify-metrics-docs.sh --write`; CI runs the same\n")
	b.WriteString("script without the flag and fails if this file has drifted.\n\n")

	b.WriteString("Every metric carries a `check` label naming the `DriftCheck` it belongs to,\n")
	b.WriteString("except the process-level ones. No metric is ever labeled with a key name,\n")
	b.WriteString("a member or a value: the keyspace is unbounded by construction, which is\n")
	b.WriteString("what makes it worth auditing and what would make it catastrophic as a\n")
	b.WriteString("label. The `publisher` label is bounded — past the configured limit,\n")
	b.WriteString("further publishers collapse into `publisher=\"" + OtherPublisher + "\"`.\n\n")

	for _, section := range sectionOrder {
		b.WriteString("## " + section + "\n\n")

		for i := range defs {
			d := &defs[i]
			if d.Section == section {
				writeDef(&b, d)
			}
		}
	}

	b.WriteString("## Histogram buckets\n\n")
	writeBuckets(&b, "Latency", LatencyBuckets)
	writeBuckets(&b, "Sweep duration", SweepBuckets)
	writeBuckets(&b, "Convergence", ConvergenceBuckets)

	return b.String()
}

func writeDef(b *strings.Builder, d *Def) {
	fmt.Fprintf(b, "### `%s`\n\n", d.Name)
	fmt.Fprintf(b, "**%s** — %s\n\n", d.Kind, d.Help)

	if len(d.Labels) == 0 {
		b.WriteString("No labels.\n\n")
		return
	}

	b.WriteString("| Label | Values |\n|---|---|\n")
	for _, label := range d.Labels {
		fmt.Fprintf(b, "| `%s` | %s |\n", label, labelValues(d, label))
	}
	b.WriteString("\n")
}

func labelValues(d *Def, label string) string {
	if values := d.Enums[label]; len(values) > 0 {
		return "`" + strings.Join(values, "` `") + "`"
	}

	switch label {
	case LabelCheck:
		return "the DriftCheck name, bounded by how many checks are configured"
	case LabelPublisher:
		return fmt.Sprintf(
			"publisher id, bounded by `maxPublisherLabels` (default %d), then `%s`",
			DefaultMaxPublisherLabels, OtherPublisher)
	case LabelEndpointIndex:
		return "the endpoint's position in the source's endpoint list"
	case LabelProjection:
		return "the registered projection name"
	case LabelVersion, LabelCommit, LabelGoVersion:
		return "fixed for the lifetime of the process"
	default:
		return "bounded, see the metric's help text"
	}
}

func writeBuckets(b *strings.Builder, name string, buckets []float64) {
	parts := make([]string, 0, len(buckets))
	for _, v := range buckets {
		parts = append(parts, time.Duration(v*float64(time.Second)).String())
	}
	fmt.Fprintf(b, "**%s**: `%s`\n\n", name, strings.Join(parts, "` `"))
}
