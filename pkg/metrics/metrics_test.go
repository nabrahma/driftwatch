package metrics_test

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/nabrahma/driftwatch/pkg/metrics"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// wantMetricNames is the metric set from §12, written out by hand.
//
// Hand-written on purpose. Generating it from the same table the collectors
// come from would make the test tautological; typed out, it is a second,
// independent statement of the contract, and a rename in registry.go has to
// come here too or CI fails. Dashboards, recording rules and alerts are all
// keyed on these strings, and every one of them breaks silently on a rename.
var wantMetricNames = []string{
	// ingest
	"driftwatch_events_received_total",
	"driftwatch_events_dropped_total",
	"driftwatch_ingest_queue_depth",
	"driftwatch_bytes_received_total",

	// sequence integrity
	"driftwatch_seq_gaps_total",
	"driftwatch_seq_missing_events",
	"driftwatch_seq_epoch",
	"driftwatch_seq_high_water_mark",
	"driftwatch_publisher_restarts_total",
	"driftwatch_publisher_clock_skew_seconds",
	"driftwatch_publishers_tracked",
	"driftwatch_gapset_truncated",

	// oracle
	"driftwatch_oracle_keys",
	"driftwatch_oracle_settled_keys",
	"driftwatch_oracle_inflight_keys",
	"driftwatch_oracle_never_settled_keys",
	"driftwatch_oracle_evictions_total",
	"driftwatch_oracle_apply_duration_seconds",
	"driftwatch_projection_errors_total",

	// target
	"driftwatch_target_reachable",
	"driftwatch_target_errors_total",
	"driftwatch_target_read_duration_seconds",
	"driftwatch_target_keyspace_size",
	"driftwatch_target_evictions_observed_total",
	"driftwatch_target_expirations_observed_total",
	"driftwatch_target_role",

	// divergence
	"driftwatch_divergent_keys",
	"driftwatch_suspect_divergent_keys",
	"driftwatch_advisory_divergent_keys",
	"driftwatch_drift_episodes_total",
	"driftwatch_drift_resolved_total",
	"driftwatch_drift_duration_seconds",
	"driftwatch_transient_divergence_total",
	"driftwatch_confirm_queue_depth",
	"driftwatch_confirm_queue_dropped_total",

	// sweeps
	"driftwatch_sweeps_total",
	"driftwatch_sweeps_skipped_total",
	"driftwatch_sweep_duration_seconds",
	"driftwatch_sweep_keys_compared",
	"driftwatch_coverage_ratio",

	// lag
	"driftwatch_convergence_seconds",
	"driftwatch_settlement_window_seconds",
	"driftwatch_lag_probe_timeouts_total",

	// source
	"driftwatch_source_connected",
	"driftwatch_source_reconnects_total",

	// process
	"driftwatch_build_info",
	"driftwatch_checks_active",
	"driftwatch_panics_total",
}

func TestMetrics_RegistryExportsExactlyTheDocumentedNames(t *testing.T) {
	m := metrics.New(metrics.Options{})

	got := m.RegisteredNames()

	assert.ElementsMatch(t, wantMetricNames, got,
		"the registry and §12 have drifted apart; every dashboard and alert "+
			"is keyed on these names, so a rename is a breaking change")
}

func TestDefs_EveryLabelIsOnTheBoundedAllowList(t *testing.T) {
	// The rule with no exceptions (§9 M12), enforced positively. A key name as
	// a label is a Prometheus outage caused by the tool deployed to detect one,
	// and the way to make that unavailable rather than discouraged is to refuse
	// every label that was not deliberately admitted — including the spellings
	// nobody thought to ban.
	for _, d := range metrics.Defs() {
		for _, label := range d.Labels {
			bound, ok := metrics.AllowedLabels[label]
			assert.True(t, ok,
				"%s carries the label %q, which is not on the bounded allow-list; "+
					"add it to AllowedLabels with what bounds it, or do not label with it",
				d.Name, label)
			assert.NotEmpty(t, bound, "%q is admitted without stating what bounds it", label)
		}
	}
}

func TestDefs_EveryDefinitionIsWellFormed(t *testing.T) {
	seen := map[string]bool{}

	for _, d := range metrics.Defs() {
		t.Run(d.Name, func(t *testing.T) {
			assert.True(t, strings.HasPrefix(d.Name, "driftwatch_"),
				"every metric shares one prefix so an operator can find them all")
			assert.False(t, seen[d.Name], "declared twice")
			seen[d.Name] = true

			assert.NotEmpty(t, d.Help, "a metric with no help text is undocumented")
			assert.True(t, strings.HasSuffix(d.Help, "."), "help text is a sentence")
			assert.NotEmpty(t, d.Section, "every metric belongs to a documented section")

			if d.Kind == metrics.KindCounter {
				assert.True(t, strings.HasSuffix(d.Name, "_total"),
					"Prometheus convention: counters end in _total")
			}
			if d.Kind == metrics.KindHistogram {
				assert.NotEmpty(t, d.Buckets, "a histogram without buckets gets the defaults, which fit nothing here")
			} else {
				assert.Empty(t, d.Buckets, "buckets on a non-histogram are ignored, which is worse than an error")
			}

			for label := range d.Enums {
				assert.Contains(t, d.Labels, label, "documents an enum for a label it does not have")
			}
		})
	}
}

func TestDefs_ClosedEnumLabelsAreDocumented(t *testing.T) {
	// §9 M12: reason and category must come from a closed enum defined in code.
	// The enum is what makes the label bounded, so a metric carrying one of
	// these labels without declaring its values is unbounded by omission.
	closed := []string{
		metrics.LabelReason, metrics.LabelCategory, metrics.LabelStage,
		metrics.LabelKind, metrics.LabelTrust, metrics.LabelResult,
		metrics.LabelRole, metrics.LabelComponent, metrics.LabelOp,
	}

	for _, d := range metrics.Defs() {
		for _, label := range d.Labels {
			for _, c := range closed {
				if label == c {
					assert.NotEmpty(t, d.Enums[label],
						"%s carries the closed label %q but does not declare its values",
						d.Name, label)
				}
			}
		}
	}
}

// countSeries counts the time series the registry would expose.
//
// A histogram is one Metric to the client library and many series to
// Prometheus — one per bucket, plus the implicit +Inf, plus _sum and _count —
// and the number that matters for a cardinality budget is the second one.
func countSeries(t *testing.T, reg *prometheus.Registry) int {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)

	total := 0
	for _, mf := range families {
		for _, metric := range mf.GetMetric() {
			switch {
			case metric.GetHistogram() != nil:
				total += len(metric.GetHistogram().GetBucket()) + 3
			case metric.GetSummary() != nil:
				total += len(metric.GetSummary().GetQuantile()) + 2
			default:
				total++
			}
		}
	}
	return total
}

// cardinalityBudget is the ceiling from §9 M12.
//
// It is not an aspiration. driftwatch is deployed alongside the thing it
// audits, often one replica per node, so its series count is multiplied by the
// fleet before it reaches Prometheus. A tool that costs a hundred series per
// replica is a tool that gets uninstalled after the first ingestion incident.
const cardinalityBudget = 500

func TestMetrics_CardinalityStaysBoundedUnderTenThousandKeys(t *testing.T) {
	// The test that prevents the catastrophic mistake (§9 M12).
	//
	// It works by being impossible to write wrongly: nothing in this package's
	// API accepts a key, so the 10,000 keys below can only ever be counted, not
	// labeled. If somebody adds a key-labeled metric, this test is where the
	// series count explodes and CI stops them.
	const (
		keys       = 10_000
		publishers = 500
	)

	reg := prometheus.NewRegistry()
	m := metrics.New(metrics.Options{Registry: reg})
	c := m.ForCheck("inference/kvcache-index")

	categories := metrics.Categories()

	for i := 0; i < keys; i++ {
		key := "block:" + strconv.Itoa(i) // 10,000 distinct keys, none of them a label
		publisher := "replica-" + strconv.Itoa(i%publishers)

		c.EventReceived(publisher, metrics.OpAdd)
		c.AddBytesReceived(int64(len(key)))

		// Everything a per-key finding does. The key is carried in the report,
		// never in a label.
		cat := categories[i%len(categories)]
		c.SetDivergentKeys(cat, i%7)
		c.SetSuspectDivergentKeys(cat, i%3)
		c.DriftEpisode(cat)
	}

	for i := 0; i < publishers; i++ {
		publisher := "replica-" + strconv.Itoa(i)

		c.SeqGap(publisher)
		c.SetMissingEvents(publisher, uint64(i))
		c.SetClockSkew(publisher, time.Duration(i)*time.Millisecond)
		c.SetGapsetTruncated(publisher, i%2 == 0)
		c.PublisherRestart(publisher, metrics.RestartImplicit)
	}
	c.SetPublishersTracked(publishers)

	got := countSeries(t, reg)
	t.Logf("%d keys and %d publishers produced %d time series (budget %d, %d publisher labels admitted)",
		keys, publishers, got, cardinalityBudget, c.AdmittedPublishers())

	assert.Less(t, got, cardinalityBudget,
		"driftwatch must not become the monitoring incident it was deployed to detect")
	assert.Equal(t, metrics.DefaultMaxPublisherLabels, c.AdmittedPublishers(),
		"the publisher label stopped at its bound")
}

func TestMetrics_CardinalityStaysBoundedWithEveryMetricExercised(t *testing.T) {
	// The previous test drives the ingest path, which is where unbounded
	// cardinality actually comes from. This one touches every metric in the
	// set, including the histograms, so the whole registry has a stated ceiling
	// rather than only the part under test.
	//
	// It is also the guard on DefaultMaxPublisherLabels. Add an eighth
	// publisher-labeled metric, or raise the limit back to the 100 §9 M12
	// suggests, and this number moves — which forces the decision to be made
	// deliberately rather than discovered by a Prometheus that fell over.
	const fullRegistryBudget = 700

	reg := prometheus.NewRegistry()
	m := metrics.New(metrics.Options{Registry: reg})
	exerciseEveryMetric(m, m.ForCheck("inference/kvcache-index"), 500)

	got := countSeries(t, reg)
	t.Logf("every metric exercised with 500 publishers: %d time series (budget %d)",
		got, fullRegistryBudget)

	assert.Less(t, got, fullRegistryBudget)
}

func TestMetrics_PublisherLabelCollapsesAtTheLimit(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(metrics.Options{Registry: reg, MaxPublisherLabels: 4})
	c := m.ForCheck("check")

	for i := 0; i < 50; i++ {
		c.EventReceived("replica-"+strconv.Itoa(i), metrics.OpSet)
	}

	labels := publisherLabelsOf(t, reg, "driftwatch_events_received_total")

	assert.Len(t, labels, 5, "four named publishers plus the aggregate")
	assert.Contains(t, labels, metrics.OtherPublisher)
	assert.Equal(t, 46.0, labels[metrics.OtherPublisher],
		"every publisher past the limit is still counted, just not named")
	assert.Equal(t, 1.0, labels["replica-0"], "an admitted publisher keeps its own series")
}

func TestMetrics_AnAdmittedPublisherKeepsItsSeriesAfterTheLimit(t *testing.T) {
	// The limit must not be a cliff that reassigns publishers already being
	// graphed. Whoever got in first stays in, or a dashboard silently swaps
	// which replica it is showing.
	reg := prometheus.NewRegistry()
	m := metrics.New(metrics.Options{Registry: reg, MaxPublisherLabels: 2})
	c := m.ForCheck("check")

	c.EventReceived("replica-0", metrics.OpSet)
	c.EventReceived("replica-1", metrics.OpSet)
	for i := 2; i < 20; i++ {
		c.EventReceived("replica-"+strconv.Itoa(i), metrics.OpSet)
	}
	c.EventReceived("replica-0", metrics.OpSet)

	labels := publisherLabelsOf(t, reg, "driftwatch_events_received_total")

	assert.Equal(t, 2.0, labels["replica-0"])
	assert.Equal(t, 1.0, labels["replica-1"])
}

func TestMetrics_AnEmptyPublisherNameIsNeverItsOwnSeries(t *testing.T) {
	// A malformed event with no publisher must not burn a label slot, or one
	// bad producer can push every real publisher into the aggregate.
	reg := prometheus.NewRegistry()
	m := metrics.New(metrics.Options{Registry: reg, MaxPublisherLabels: 2})
	c := m.ForCheck("check")

	for i := 0; i < 100; i++ {
		c.EventDropped("", metrics.DropDecodeError)
	}
	c.EventReceived("replica-0", metrics.OpSet)
	c.EventReceived("replica-1", metrics.OpSet)

	assert.Equal(t, 2, c.AdmittedPublishers())
}

func TestMetrics_UnknownLabelValuesNormalizeIntoTheEnum(t *testing.T) {
	// The rule that keeps `reason` closed. Passing an error string through as a
	// label is how a bounded metric becomes unbounded, so an unrecognized value
	// lands on a known one rather than minting a series.
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"op", string(metrics.Op("dial tcp 10.0.0.1:5557: refused").Normalize()), "unknown"},
		{"drop reason", string(metrics.DropReason("EOF").Normalize()), "validation_error"},
		{"category", string(metrics.Category("???").Normalize()), "value_mismatch"},
		{"sweep result", string(metrics.SweepResult("context deadline").Normalize()), "error"},
		{"trust", string(metrics.Trust("").Normalize()), "suspect"},
		{"role", string(metrics.Role("sentinel").Normalize()), "unknown"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.got)
		})
	}
}

func TestMetrics_KnownLabelValuesSurviveNormalization(t *testing.T) {
	assert.Equal(t, metrics.OpAdd, metrics.OpAdd.Normalize())
	assert.Equal(t, metrics.CatMemberMismatch, metrics.CatMemberMismatch.Normalize())
	assert.Equal(t, metrics.TransientFenceFailed, metrics.TransientFenceFailed.Normalize())
	assert.Equal(t, metrics.SweepTargetToOracle, metrics.SweepTargetToOracle.Normalize())
}

func TestMetrics_TotalsBecomeCounterIncrements(t *testing.T) {
	// Several counters mirror a running total another package owns. The first
	// observation establishes a floor rather than adding the whole history,
	// which would otherwise show a restarted process as a huge burst.
	reg := prometheus.NewRegistry()
	m := metrics.New(metrics.Options{Registry: reg})
	c := m.ForCheck("check")

	c.SetOracleEvictionsTotal(1000)
	assert.Equal(t, 0.0, valueOf(t, reg, "driftwatch_oracle_evictions_total"),
		"the first total is a baseline, not 1000 evictions that just happened")

	c.SetOracleEvictionsTotal(1005)
	assert.Equal(t, 5.0, valueOf(t, reg, "driftwatch_oracle_evictions_total"))

	c.SetOracleEvictionsTotal(1005)
	assert.Equal(t, 5.0, valueOf(t, reg, "driftwatch_oracle_evictions_total"))
}

func TestMetrics_ATotalThatGoesBackwardsDoesNotAddANegative(t *testing.T) {
	// Redis restarting resets its eviction counter. A counter that tried to
	// track the drop would panic; one that added the new total would invent
	// evictions that never happened.
	reg := prometheus.NewRegistry()
	m := metrics.New(metrics.Options{Registry: reg})
	c := m.ForCheck("check")

	c.SetTargetEvictionsObserved(500)
	c.SetTargetEvictionsObserved(600)
	c.SetTargetEvictionsObserved(3) // the store restarted
	c.SetTargetEvictionsObserved(9)

	assert.Equal(t, 106.0, valueOf(t, reg, "driftwatch_target_evictions_observed_total"),
		"100 before the reset and 6 after, with nothing invented in between")
}

func TestMetrics_RoleIsExclusive(t *testing.T) {
	// Two roles reading 1 at once would make a failover look like the store
	// being both things, and the alert on replica reads would never clear.
	reg := prometheus.NewRegistry()
	m := metrics.New(metrics.Options{Registry: reg})
	c := m.ForCheck("check")

	c.SetTargetRole(metrics.RoleMaster)
	c.SetTargetRole(metrics.RoleReplica)

	roles := labelsOf(t, reg, "driftwatch_target_role", metrics.LabelRole)
	assert.Equal(t, 0.0, roles["master"])
	assert.Equal(t, 1.0, roles["replica"])
	assert.Equal(t, 0.0, roles["unknown"])
}

func TestMetrics_ChecksAreLabelledApart(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(metrics.Options{Registry: reg})

	m.ForCheck("a").EventReceived("replica-0", metrics.OpSet)
	m.ForCheck("b").EventReceived("replica-0", metrics.OpSet)
	m.ForCheck("b").EventReceived("replica-0", metrics.OpSet)

	checks := labelsOf(t, reg, "driftwatch_events_received_total", metrics.LabelCheck)
	assert.Equal(t, 1.0, checks["a"])
	assert.Equal(t, 2.0, checks["b"])
}

func TestMetrics_PublisherLimitsAreIndependentPerCheck(t *testing.T) {
	// One noisy check must not exhaust another's label budget.
	reg := prometheus.NewRegistry()
	m := metrics.New(metrics.Options{Registry: reg, MaxPublisherLabels: 2})

	noisy := m.ForCheck("noisy")
	for i := 0; i < 50; i++ {
		noisy.EventReceived("replica-"+strconv.Itoa(i), metrics.OpSet)
	}

	quiet := m.ForCheck("quiet")
	quiet.EventReceived("replica-99", metrics.OpSet)

	assert.Equal(t, 1, quiet.AdmittedPublishers())
}

func TestMetrics_HandlerServesTheRegistry(t *testing.T) {
	m := metrics.New(metrics.Options{})
	m.ForCheck("check").SetSettlementWindow(2 * time.Second)

	assert.NotNil(t, m.Handler())
	assert.Equal(t, 2.0, valueOf(t, m.Registry(), "driftwatch_settlement_window_seconds"))
}

func TestMetrics_BuildInfoIsAlwaysOne(t *testing.T) {
	m := metrics.New(metrics.Options{})
	m.SetBuildInfo("v0.5.0", "abc1234", "go1.23.0")

	assert.Equal(t, 1.0, valueOf(t, m.Registry(), "driftwatch_build_info"))
}

func TestMarkdown_MatchesTheCommittedDocumentation(t *testing.T) {
	// The same check hack/verify-metrics-docs.sh runs in CI, expressed as a Go
	// test so it also fails on a machine with no bash.
	committed, err := os.ReadFile("../../docs/METRICS.md")
	require.NoError(t, err, "docs/METRICS.md is generated; run hack/verify-metrics-docs.sh --write")

	assert.Equal(t, metrics.Markdown(), strings.ReplaceAll(string(committed), "\r\n", "\n"),
		"docs/METRICS.md is stale; run hack/verify-metrics-docs.sh --write")
}

func TestMarkdown_DocumentsEveryMetric(t *testing.T) {
	doc := metrics.Markdown()

	for _, name := range wantMetricNames {
		assert.Contains(t, doc, "### `"+name+"`", "%s is undocumented", name)
	}
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

// exerciseEveryMetric touches every metric in the set once, so a cardinality
// assertion covers the whole registry rather than the part a test remembered.
func exerciseEveryMetric(m *metrics.Metrics, c *metrics.CheckMetrics, publishers int) {
	m.SetBuildInfo("v0.5.0", "abc1234", "go1.23.0")
	m.SetChecksActive(1)

	for i := 0; i < publishers; i++ {
		p := "replica-" + strconv.Itoa(i)
		c.EventReceived(p, metrics.OpAdd)
		c.EventDropped(p, metrics.DropDecodeError)
		c.SeqGap(p)
		c.SetMissingEvents(p, 3)
		c.PublisherRestart(p, metrics.RestartExplicit)
		c.SetClockSkew(p, time.Millisecond)
		c.SetGapsetTruncated(p, false)
	}

	c.SetQueueDepth(metrics.StageRaw, 10)
	c.SetQueueDepth(metrics.StageDecoded, 2)
	c.AddBytesReceived(4096)
	c.SetPublishersTracked(publishers)

	c.SetOracleKeys(metrics.TrustComplete, 100)
	c.SetOracleKeys(metrics.TrustSuspect, 1)
	c.SetOracleKeys(metrics.TrustAdopted, 2)
	c.SetSettledKeys(98)
	c.SetInflightKeys(3)
	c.SetNeverSettledKeys(1)
	c.SetOracleEvictionsTotal(7)
	c.ObserveApplyDuration(time.Millisecond)
	c.ProjectionError("keysetOwnership", metrics.ProjectionMemberLimit)

	c.SetTargetReachable(true)
	c.SetTargetKeyspaceSize(1234)
	c.SetTargetEvictionsObserved(5)
	c.SetTargetExpirationsObserved(9)
	c.SetTargetRole(metrics.RoleMaster)
	for _, op := range []metrics.TargetOp{
		metrics.TargetGet, metrics.TargetGetMany, metrics.TargetScan,
		metrics.TargetTTL, metrics.TargetHealth,
	} {
		c.TargetError(op)
		c.ObserveTargetRead(op, time.Millisecond)
	}

	for _, cat := range metrics.Categories() {
		c.SetDivergentKeys(cat, 1)
		c.SetSuspectDivergentKeys(cat, 1)
		c.SetAdvisoryDivergentKeys(cat, 1)
		c.DriftEpisode(cat)
		c.DriftResolved(cat)
	}
	c.SetDriftDuration(time.Minute)
	for _, r := range []metrics.TransientReason{
		metrics.TransientResolved, metrics.TransientOracleAdvanced,
		metrics.TransientKeyEvicted, metrics.TransientFenceFailed,
	} {
		c.TransientDivergence(r)
	}
	c.SetConfirmQueueDepth(4)
	c.SetConfirmQueueDroppedTotal(1)

	for _, kind := range []metrics.SweepKind{
		metrics.SweepOracleToTarget, metrics.SweepTargetToOracle,
	} {
		for _, result := range []metrics.SweepResult{
			metrics.SweepSuccess, metrics.SweepTargetUnavailable,
			metrics.SweepError, metrics.SweepAborted,
		} {
			c.Sweep(kind, result)
		}
		c.SweepSkipped(kind)
		c.ObserveSweepDuration(kind, time.Second)
	}
	c.SetSweepKeysCompared(98)
	c.SetCoverageRatio(0.98)

	c.ObserveConvergence(50 * time.Millisecond)
	c.SetSettlementWindow(2 * time.Second)
	c.SetLagProbeTimeoutsTotal(2)

	c.SetSourceConnected(0, true)
	c.SetSourceConnected(1, false)
	c.SetSourceReconnectsTotal(3)

	c.Panic(metrics.ComponentSweeper)
}

// valueOf returns the single sample of a metric family.
func valueOf(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)

	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		require.Len(t, mf.GetMetric(), 1, "%s has more than one series", name)
		return sampleValue(mf.GetMetric()[0])
	}

	t.Fatalf("metric %q was never exported", name)
	return 0
}

// labelsOf returns each series of a metric family keyed by one label's value.
func labelsOf(t *testing.T, reg *prometheus.Registry, name, label string) map[string]float64 {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)

	out := map[string]float64{}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, metric := range mf.GetMetric() {
			for _, pair := range metric.GetLabel() {
				if pair.GetName() == label {
					out[pair.GetValue()] = sampleValue(metric)
				}
			}
		}
	}

	require.NotEmpty(t, out, "metric %q was never exported with label %q", name, label)
	return out
}

func publisherLabelsOf(t *testing.T, reg *prometheus.Registry, name string) map[string]float64 {
	t.Helper()
	return labelsOf(t, reg, name, metrics.LabelPublisher)
}

// sampleValue reads a scalar sample whatever kind the metric turned out to be,
// so assertions above do not each have to branch on counter versus gauge.
func sampleValue(m *dto.Metric) float64 {
	switch {
	case m.GetCounter() != nil:
		return m.GetCounter().GetValue()
	case m.GetGauge() != nil:
		return m.GetGauge().GetValue()
	case m.GetUntyped() != nil:
		return m.GetUntyped().GetValue()
	default:
		return 0
	}
}

func TestMetrics_ForgetCheckRemovesEverySeriesForAStoppedCheck(t *testing.T) {
	// A deleted DriftCheck's series must not outlive it, and the reason is the
	// third-order one: every gauge freezes at its final value. A check deleted
	// while it had drift keeps exporting that number forever, so the §12.2 alert
	// on it keeps firing about an object that no longer exists, and the only way
	// to clear it is a manager restart — which discards every other check's
	// history at the same time.
	reg := prometheus.NewRegistry()
	m := metrics.New(metrics.Options{Registry: reg})

	doomed := m.ForCheck("inference/doomed")
	survivor := m.ForCheck("inference/survivor")

	// A spread of shapes: a plain counter, one with a second label, a gauge, and
	// a histogram — because ForgetCheck has to reach all four maps.
	for _, c := range []*metrics.CheckMetrics{doomed, survivor} {
		c.EventReceived("replica-0", metrics.Op("add"))
		c.EventDropped("replica-0", metrics.DropDecodeError)
		c.SetDivergentKeys(metrics.CatMissingInTarget, 7)
		c.ObserveSweepDuration(metrics.SweepOracleToTarget, time.Second)
	}

	require.Positive(t, seriesFor(t, reg, "inference/doomed"),
		"the fixture did not produce any series to forget")
	before := seriesFor(t, reg, "inference/survivor")
	require.Positive(t, before)

	m.ForgetCheck("inference/doomed")

	assert.Zero(t, seriesFor(t, reg, "inference/doomed"),
		"a stopped check's series must not outlive it; a frozen "+
			"divergent_keys gauge alerts forever about an object that is gone")
	assert.Equal(t, before, seriesFor(t, reg, "inference/survivor"),
		"forgetting one check must not disturb another's series")
}

func TestMetrics_ForgetCheckIsSafeForACheckThatNeverRan(t *testing.T) {
	// The registry's stop path calls this unconditionally, including for a
	// runner that failed to start. It must not panic.
	reg := prometheus.NewRegistry()
	m := metrics.New(metrics.Options{Registry: reg})

	assert.NotPanics(t, func() { m.ForgetCheck("inference/never-existed") })
}

// seriesFor counts the exported series carrying a given check label.
func seriesFor(t *testing.T, reg *prometheus.Registry, check string) int {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)

	n := 0
	for _, f := range families {
		for _, sample := range f.GetMetric() {
			for _, l := range sample.GetLabel() {
				if l.GetName() == metrics.LabelCheck && l.GetValue() == check {
					n++
				}
			}
		}
	}
	return n
}
