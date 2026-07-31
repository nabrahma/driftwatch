// Package metrics declares the Prometheus instrumentation, with bounded
// cardinality (§12, M12).
//
// The whole design of this package answers one question: what stops driftwatch
// from becoming the outage it was deployed to detect? A monitoring tool that
// labels a metric with a key name turns an unbounded keyspace into unbounded
// time series, and the Prometheus that dies of it is the same one holding every
// other alert in the cluster.
//
// Three things enforce the bound, in decreasing order of how much they matter:
//
//   - Nothing in this package accepts a key, a member or a value. Not as a
//     label, not as an argument. The API makes the mistake unavailable rather
//     than discouraged, and TestDefs_EveryLabelIsOnTheBoundedAllowList checks it.
//   - Every label value is a constant of a defined type (see enums.go), so an
//     error string cannot become a label without a visible conversion — and an
//     unrecognized value normalizes back into the enum before it is used.
//   - The one genuinely open label, `publisher`, is capped. Past the limit
//     further publishers collapse into a single `__other__` series and the
//     collapse is logged once.
//
// The cardinality test is what turns those from claims into facts: 10,000
// distinct keys and 500 distinct publishers, and the registry stays under 500
// series.
package metrics

import (
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// DefaultMaxPublisherLabels is the default bound on distinct publisher label
// values.
//
// §9 M12 gives two numbers that turn out to be incompatible: a default of 100
// publisher labels, and a total budget of 500 time series. Seven metrics in
// §12 carry the publisher label, so 100 admitted publishers costs 7 x 101 =
// 707 series before a single other metric is counted, and the measured figure
// for a plain ingest workload was 629. The budget is the number worth keeping —
// driftwatch runs alongside the store it audits, often one replica per node, so
// its series count is multiplied by the fleet before it reaches Prometheus.
//
// 50 leaves the same workload at 329 series, roughly a third of the budget,
// with the rest available to the sweep and target histograms. Per-publisher
// detail beyond fifty publishers belongs in `driftwatch explain` and the logs
// rather than in a metric label, and driftwatch_publishers_tracked still
// reports the true count when the labels have collapsed.
//
// See docs/DECISIONS.md ADR-0008 and docs/DISCOVERIES.md D-012.
const DefaultMaxPublisherLabels = 50

// OtherPublisher is the label value every publisher past the limit collapses
// into.
//
// The double underscore follows Prometheus' own convention for a value the
// system supplied rather than the target, so an operator reading a graph can
// tell at a glance that the series is an aggregate and not a pod called
// "other".
const OtherPublisher = "__other__"

// Options configures a Metrics.
type Options struct {
	// Registry is where the collectors are registered. A nil Registry gets a
	// fresh one, which is what tests want; the CLI passes its own so that Go
	// runtime and process collectors can sit alongside.
	Registry *prometheus.Registry
	// Logger records the one line emitted when the publisher label limit is
	// reached. The zero value is a working no-op.
	Logger logr.Logger
	// MaxPublisherLabels bounds distinct publisher label values. Zero uses
	// DefaultMaxPublisherLabels; a negative value disables the publisher label
	// entirely, collapsing every publisher into OtherPublisher.
	MaxPublisherLabels int
}

// Metrics owns the collectors for a process.
type Metrics struct {
	reg    *prometheus.Registry
	log    logr.Logger
	maxPub int

	counters   map[string]*prometheus.CounterVec
	gauges     map[string]*prometheus.GaugeVec
	histograms map[string]*prometheus.HistogramVec

	// lastTotals backs addDelta. See its comment for why counters mirroring a
	// total owned elsewhere need this.
	deltaMu    sync.Mutex
	lastTotals map[string]uint64
}

// New builds and registers the metric set.
//
// It panics on a duplicate registration, which can only happen if this package
// declares the same name twice — a bug this package can and should fail loudly
// on, rather than silently exporting one of the two.
func New(opts Options) *Metrics {
	reg := opts.Registry
	if reg == nil {
		reg = prometheus.NewRegistry()
	}

	maxPub := opts.MaxPublisherLabels
	if maxPub == 0 {
		maxPub = DefaultMaxPublisherLabels
	}
	if maxPub < 0 {
		maxPub = 0
	}

	m := &Metrics{
		reg:        reg,
		log:        opts.Logger,
		maxPub:     maxPub,
		counters:   map[string]*prometheus.CounterVec{},
		gauges:     map[string]*prometheus.GaugeVec{},
		histograms: map[string]*prometheus.HistogramVec{},
		lastTotals: map[string]uint64{},
	}

	for i := range defs {
		m.register(&defs[i])
	}
	return m
}

func (m *Metrics) register(d *Def) {
	switch d.Kind {
	case KindCounter:
		c := prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: d.Name, Help: d.Help}, d.Labels)
		m.reg.MustRegister(c)
		m.counters[d.Name] = c

	case KindGauge:
		g := prometheus.NewGaugeVec(
			prometheus.GaugeOpts{Name: d.Name, Help: d.Help}, d.Labels)
		m.reg.MustRegister(g)
		m.gauges[d.Name] = g

	case KindHistogram:
		h := prometheus.NewHistogramVec(
			prometheus.HistogramOpts{Name: d.Name, Help: d.Help, Buckets: d.Buckets}, d.Labels)
		m.reg.MustRegister(h)
		m.histograms[d.Name] = h
	}
}

// Registry returns the underlying Prometheus registry.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// RegisteredNames returns the metric names actually registered, read back out
// of the collectors rather than out of the definition table.
//
// Reading them back is the point. A test comparing the definition table against
// itself proves nothing; comparing what Prometheus will really export against a
// hand-written list is what makes an accidental rename fail CI instead of
// silently breaking every dashboard that queries the old name.
func (m *Metrics) RegisteredNames() []string {
	collectors := make([]prometheus.Collector, 0,
		len(m.counters)+len(m.gauges)+len(m.histograms))
	for _, c := range m.counters {
		collectors = append(collectors, c)
	}
	for _, g := range m.gauges {
		collectors = append(collectors, g)
	}
	for _, h := range m.histograms {
		collectors = append(collectors, h)
	}

	out := make([]string, 0, len(collectors))
	for _, c := range collectors {
		ch := make(chan *prometheus.Desc, 8)
		go func(c prometheus.Collector) {
			c.Describe(ch)
			close(ch)
		}(c)
		for d := range ch {
			if name := fqNameOf(d); name != "" {
				out = append(out, name)
			}
		}
	}

	sort.Strings(out)
	return out
}

// fqNameOf extracts a metric's fully-qualified name from its description.
//
// prometheus.Desc keeps fqName unexported and offers no accessor, so its
// String form is the only way to read it back. The format is
// `Desc{fqName: "…", help: …}` and a metric name cannot contain a quote, which
// makes the extraction unambiguous even though it is textual.
func fqNameOf(d *prometheus.Desc) string {
	const prefix = `fqName: "`

	s := d.String()
	i := strings.Index(s, prefix)
	if i < 0 {
		return ""
	}
	rest := s[i+len(prefix):]
	j := strings.IndexByte(rest, '"')
	if j < 0 {
		return ""
	}
	return rest[:j]
}

// Handler returns an HTTP handler serving the registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{Registry: m.reg})
}

// SetBuildInfo publishes the build as a constant-1 gauge.
func (m *Metrics) SetBuildInfo(version, commit, goVersion string) {
	m.gauges["driftwatch_build_info"].WithLabelValues(version, commit, goVersion).Set(1)
}

// SetChecksActive records how many checks are running in this process.
func (m *Metrics) SetChecksActive(n int) {
	m.gauges["driftwatch_checks_active"].WithLabelValues().Set(float64(n))
}

// ForCheck returns the per-check view of the metric set.
//
// Currying the check label here rather than passing it at every call site is
// not only convenience: it makes it impossible for one check's metrics to be
// recorded under another's name, which in a multi-check process is a bug that
// looks exactly like drift.
func (m *Metrics) ForCheck(name string) *CheckMetrics {
	return &CheckMetrics{
		m:         m,
		check:     name,
		publisher: newPublisherLimiter(m.maxPub, name, m.log),
	}
}

// CheckMetrics is the metric surface for one running check.
//
// Every method is safe for concurrent use and cheap enough to call on the hot
// path; the label lookups go through prometheus' own sharded map.
type CheckMetrics struct {
	m         *Metrics
	check     string
	publisher *publisherLimiter
}

func (c *CheckMetrics) counter(name string, labels ...string) prometheus.Counter {
	return c.m.counters[name].WithLabelValues(append([]string{c.check}, labels...)...)
}

func (c *CheckMetrics) gauge(name string, labels ...string) prometheus.Gauge {
	return c.m.gauges[name].WithLabelValues(append([]string{c.check}, labels...)...)
}

func (c *CheckMetrics) histogram(name string, labels ...string) prometheus.Observer {
	return c.m.histograms[name].WithLabelValues(append([]string{c.check}, labels...)...)
}

// ---------------------------------------------------------------------------
// Ingest.
// ---------------------------------------------------------------------------

// EventReceived counts one decoded event.
func (c *CheckMetrics) EventReceived(publisher string, op Op) {
	c.counter("driftwatch_events_received_total",
		c.publisher.label(publisher), string(op.Normalize())).Inc()
}

// EventDropped counts one event that never reached the oracle.
func (c *CheckMetrics) EventDropped(publisher string, reason DropReason) {
	c.counter("driftwatch_events_dropped_total",
		c.publisher.label(publisher), string(reason.Normalize())).Inc()
}

// SetQueueDepth records how much is buffered at one pipeline stage.
func (c *CheckMetrics) SetQueueDepth(stage Stage, n int) {
	c.gauge("driftwatch_ingest_queue_depth", string(stage.Normalize())).Set(float64(n))
}

// AddBytesReceived counts payload bytes read off the transport.
func (c *CheckMetrics) AddBytesReceived(n int64) {
	c.counter("driftwatch_bytes_received_total").Add(float64(n))
}

// ---------------------------------------------------------------------------
// Sequence integrity.
// ---------------------------------------------------------------------------

// SeqGap counts one observed sequence gap.
func (c *CheckMetrics) SeqGap(publisher string) {
	c.counter("driftwatch_seq_gaps_total", c.publisher.label(publisher)).Inc()
}

// SetMissingEvents records how many of a publisher's sequence numbers are
// unaccounted for.
func (c *CheckMetrics) SetMissingEvents(publisher string, n uint64) {
	c.gauge("driftwatch_seq_missing_events", c.publisher.label(publisher)).Set(float64(n))
}

// PublisherRestart counts one restart.
func (c *CheckMetrics) PublisherRestart(publisher string, kind RestartKind) {
	c.counter("driftwatch_publisher_restarts_total",
		c.publisher.label(publisher), string(kind.Normalize())).Inc()
}

// SetClockSkew records a publisher's clock offset from driftwatch's.
func (c *CheckMetrics) SetClockSkew(publisher string, d time.Duration) {
	c.gauge("driftwatch_publisher_clock_skew_seconds",
		c.publisher.label(publisher)).Set(d.Seconds())
}

// SetPublishersTracked records how many publishers have sequence state.
//
// This is the count seqtrack actually holds, not the number of publisher label
// values, and the two differ once the label limit engages. That is deliberate:
// an operator needs to know the real number, and this is the metric that still
// tells the truth when the labels have collapsed.
func (c *CheckMetrics) SetPublishersTracked(n int) {
	c.gauge("driftwatch_publishers_tracked").Set(float64(n))
}

// SetGapsetTruncated records whether a publisher's gap list hit its bound.
func (c *CheckMetrics) SetGapsetTruncated(publisher string, truncated bool) {
	c.gauge("driftwatch_gapset_truncated", c.publisher.label(publisher)).Set(boolValue(truncated))
}

// ---------------------------------------------------------------------------
// Oracle.
// ---------------------------------------------------------------------------

// SetOracleKeys records the tracked key count for one trust state.
func (c *CheckMetrics) SetOracleKeys(trust Trust, n int) {
	c.gauge("driftwatch_oracle_keys", string(trust.Normalize())).Set(float64(n))
}

// SetSettledKeys records how many keys are eligible for comparison.
func (c *CheckMetrics) SetSettledKeys(n int) {
	c.gauge("driftwatch_oracle_settled_keys").Set(float64(n))
}

// SetInflightKeys records how many keys changed inside the settlement window.
func (c *CheckMetrics) SetInflightKeys(n int) {
	c.gauge("driftwatch_oracle_inflight_keys").Set(float64(n))
}

// SetNeverSettledKeys records how many keys the stability window rescued.
func (c *CheckMetrics) SetNeverSettledKeys(n int) {
	c.gauge("driftwatch_oracle_never_settled_keys").Set(float64(n))
}

// SetOracleEvictionsTotal publishes a monotonic eviction total.
func (c *CheckMetrics) SetOracleEvictionsTotal(total uint64) {
	c.m.addDelta(c.counter("driftwatch_oracle_evictions_total"), c.check+"|oracle_evictions", total)
}

// ObserveApplyDuration records the time to fold one event.
func (c *CheckMetrics) ObserveApplyDuration(d time.Duration) {
	c.histogram("driftwatch_oracle_apply_duration_seconds").Observe(d.Seconds())
}

// ProjectionError counts one event a projection refused.
func (c *CheckMetrics) ProjectionError(projection string, reason ProjectionErrorReason) {
	c.counter("driftwatch_projection_errors_total",
		projection, string(reason.Normalize())).Inc()
}

// ---------------------------------------------------------------------------
// Target.
// ---------------------------------------------------------------------------

// SetTargetReachable records whether the last health probe reached the store.
func (c *CheckMetrics) SetTargetReachable(reachable bool) {
	c.gauge("driftwatch_target_reachable").Set(boolValue(reachable))
}

// TargetError counts one failed store operation.
func (c *CheckMetrics) TargetError(op TargetOp) {
	c.counter("driftwatch_target_errors_total", string(op.Normalize())).Inc()
}

// ObserveTargetRead records store read latency.
func (c *CheckMetrics) ObserveTargetRead(op TargetOp, d time.Duration) {
	c.histogram("driftwatch_target_read_duration_seconds", string(op.Normalize())).Observe(d.Seconds())
}

// SetTargetKeyspaceSize records how many keys the store reports holding.
func (c *CheckMetrics) SetTargetKeyspaceSize(n int64) {
	c.gauge("driftwatch_target_keyspace_size").Set(float64(n))
}

// SetTargetEvictionsObserved publishes the store's monotonic eviction counter.
func (c *CheckMetrics) SetTargetEvictionsObserved(total uint64) {
	c.m.addDelta(c.counter("driftwatch_target_evictions_observed_total"),
		c.check+"|target_evictions", total)
}

// SetTargetExpirationsObserved publishes the store's monotonic expiry counter.
func (c *CheckMetrics) SetTargetExpirationsObserved(total uint64) {
	c.m.addDelta(c.counter("driftwatch_target_expirations_observed_total"),
		c.check+"|target_expirations", total)
}

// SetTargetRole publishes the store's replication role, zeroing the others so
// that a failover does not leave two roles reading 1 at once.
func (c *CheckMetrics) SetTargetRole(role Role) {
	role = role.Normalize()
	for _, r := range roleValues() {
		c.gauge("driftwatch_target_role", r).Set(boolValue(Role(r) == role))
	}
}

// ---------------------------------------------------------------------------
// Divergence.
// ---------------------------------------------------------------------------

// SetDivergentKeys records confirmed divergence driftwatch stands behind.
func (c *CheckMetrics) SetDivergentKeys(cat Category, n int) {
	c.gauge("driftwatch_divergent_keys", string(cat.Normalize())).Set(float64(n))
}

// SetSuspectDivergentKeys records divergence on keys driftwatch knows it may
// have an incomplete view of.
func (c *CheckMetrics) SetSuspectDivergentKeys(cat Category, n int) {
	c.gauge("driftwatch_suspect_divergent_keys", string(cat.Normalize())).Set(float64(n))
}

// SetAdvisoryDivergentKeys records divergence on keys adopted at bootstrap.
func (c *CheckMetrics) SetAdvisoryDivergentKeys(cat Category, n int) {
	c.gauge("driftwatch_advisory_divergent_keys", string(cat.Normalize())).Set(float64(n))
}

// DriftEpisode counts one confirmed divergence.
func (c *CheckMetrics) DriftEpisode(cat Category) {
	c.counter("driftwatch_drift_episodes_total", string(cat.Normalize())).Inc()
}

// DriftResolved counts one confirmed divergence that later agreed again.
func (c *CheckMetrics) DriftResolved(cat Category) {
	c.counter("driftwatch_drift_resolved_total", string(cat.Normalize())).Inc()
}

// SetDriftDuration records the age of the oldest unresolved episode.
func (c *CheckMetrics) SetDriftDuration(d time.Duration) {
	c.gauge("driftwatch_drift_duration_seconds").Set(d.Seconds())
}

// TransientDivergence counts one candidate that resolved before confirmation.
func (c *CheckMetrics) TransientDivergence(reason TransientReason) {
	c.counter("driftwatch_transient_divergence_total", string(reason.Normalize())).Inc()
}

// SetConfirmQueueDepth records how many candidates await a second read.
func (c *CheckMetrics) SetConfirmQueueDepth(n int) {
	c.gauge("driftwatch_confirm_queue_depth").Set(float64(n))
}

// SetConfirmQueueDroppedTotal publishes the monotonic drop count.
func (c *CheckMetrics) SetConfirmQueueDroppedTotal(total uint64) {
	c.m.addDelta(c.counter("driftwatch_confirm_queue_dropped_total"),
		c.check+"|confirm_dropped", total)
}

// ---------------------------------------------------------------------------
// Sweeps.
// ---------------------------------------------------------------------------

// Sweep counts one completed sweep.
func (c *CheckMetrics) Sweep(kind SweepKind, result SweepResult) {
	c.counter("driftwatch_sweeps_total",
		string(kind.Normalize()), string(result.Normalize())).Inc()
}

// SweepSkipped counts one sweep skipped because the previous one was running.
func (c *CheckMetrics) SweepSkipped(kind SweepKind) {
	c.counter("driftwatch_sweeps_skipped_total", string(kind.Normalize())).Inc()
}

// ObserveSweepDuration records how long a sweep took.
func (c *CheckMetrics) ObserveSweepDuration(kind SweepKind, d time.Duration) {
	c.histogram("driftwatch_sweep_duration_seconds", string(kind.Normalize())).Observe(d.Seconds())
}

// SetSweepKeysCompared records how many keys the last sweep compared.
func (c *CheckMetrics) SetSweepKeysCompared(n int) {
	c.gauge("driftwatch_sweep_keys_compared").Set(float64(n))
}

// SetCoverageRatio records the fraction of tracked keys the last sweep read.
func (c *CheckMetrics) SetCoverageRatio(ratio float64) {
	c.gauge("driftwatch_coverage_ratio").Set(ratio)
}

// ---------------------------------------------------------------------------
// Lag.
// ---------------------------------------------------------------------------

// ObserveConvergence records one measured oracle-to-target delay.
func (c *CheckMetrics) ObserveConvergence(d time.Duration) {
	c.histogram("driftwatch_convergence_seconds").Observe(d.Seconds())
}

// SetSettlementWindow records the window currently in force.
func (c *CheckMetrics) SetSettlementWindow(d time.Duration) {
	c.gauge("driftwatch_settlement_window_seconds").Set(d.Seconds())
}

// SetLagProbeTimeoutsTotal publishes the monotonic probe timeout count.
func (c *CheckMetrics) SetLagProbeTimeoutsTotal(total uint64) {
	c.m.addDelta(c.counter("driftwatch_lag_probe_timeouts_total"),
		c.check+"|lag_timeouts", total)
}

// ---------------------------------------------------------------------------
// Source.
// ---------------------------------------------------------------------------

// SetSourceConnected records whether one endpoint is connected.
func (c *CheckMetrics) SetSourceConnected(endpointIndex int, connected bool) {
	c.gauge("driftwatch_source_connected",
		strconv.Itoa(endpointIndex)).Set(boolValue(connected))
}

// SetSourceReconnectsTotal publishes the monotonic reconnect count.
func (c *CheckMetrics) SetSourceReconnectsTotal(total uint64) {
	c.m.addDelta(c.counter("driftwatch_source_reconnects_total"),
		c.check+"|source_reconnects", total)
}

// ---------------------------------------------------------------------------
// Process.
// ---------------------------------------------------------------------------

// Panic counts one recovered panic.
func (c *CheckMetrics) Panic(component Component) {
	c.counter("driftwatch_panics_total", string(component.Normalize())).Inc()
}

// ---------------------------------------------------------------------------
// Monotonic-total helper.
// ---------------------------------------------------------------------------

// addDelta advances a counter to match a total the caller keeps.
//
// Several counters here mirror a running total owned by another package —
// oracle evictions, the store's own eviction counter, source reconnects. Those
// packages expose a total rather than a delta, and a Prometheus counter can
// only be added to. Tracking the last value seen turns one into the other.
//
// A total that goes backwards (the store restarted, the counter reset) adds
// nothing rather than a negative, and rebases so the next increment counts from
// the new floor. Prometheus handles a counter reset perfectly well when the
// series itself restarts; what it cannot handle is an increment that lies.
func (m *Metrics) addDelta(c prometheus.Counter, key string, total uint64) {
	m.deltaMu.Lock()
	last, seen := m.lastTotals[key]
	m.lastTotals[key] = total
	m.deltaMu.Unlock()

	if !seen || total < last {
		return
	}
	if d := total - last; d > 0 {
		c.Add(float64(d))
	}
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// publisherLimiter bounds the distinct values of the `publisher` label.
//
// §9 M12 allows the label but requires it be bounded, and this is the mechanism.
// A publisher already admitted keeps its own series; one arriving after the
// limit is reached collapses into OtherPublisher, and the collapse is logged
// exactly once — an operator needs to know their per-publisher graphs have gone
// incomplete, and needs it said once rather than once per event.
type publisherLimiter struct {
	mu       sync.RWMutex
	limit    int
	check    string
	log      logr.Logger
	admitted map[string]struct{}
	warned   bool
}

func newPublisherLimiter(limit int, check string, log logr.Logger) *publisherLimiter {
	return &publisherLimiter{
		limit:    limit,
		check:    check,
		log:      log,
		admitted: make(map[string]struct{}, limit),
	}
}

func (p *publisherLimiter) label(publisher string) string {
	if publisher == "" {
		return OtherPublisher
	}

	p.mu.RLock()
	_, ok := p.admitted[publisher]
	full := len(p.admitted) >= p.limit
	p.mu.RUnlock()

	if ok {
		return publisher
	}
	if full {
		return OtherPublisher
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Re-check under the write lock: two goroutines can both have read a
	// not-full map, and admitting on both would put the map one over the limit.
	if _, ok := p.admitted[publisher]; ok {
		return publisher
	}
	if len(p.admitted) >= p.limit {
		if !p.warned {
			p.warned = true
			p.log.Info("publisher label limit reached; further publishers are aggregated",
				"check", p.check, "limit", p.limit, "collapsedInto", OtherPublisher)
		}
		return OtherPublisher
	}

	p.admitted[publisher] = struct{}{}
	return publisher
}

// Admitted returns how many publishers have their own label value.
func (p *publisherLimiter) Admitted() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.admitted)
}

// AdmittedPublishers returns how many publishers have their own label value.
// Diagnostics and tests only.
func (c *CheckMetrics) AdmittedPublishers() int { return c.publisher.Admitted() }
