// Package scenario provides the declarative fault-scenario DSL (§13).
//
// A fault test has a lot of moving parts — a publisher, two subscriptions, an
// injector, a check, a materializer, a target and a clock — and wiring them by
// hand in every test buries the one line that says what the test is about. The
// DSL exists so that line survives:
//
//	scenario.New(t).
//	    WithProjection("keysetOwnership").
//	    WithPublishers(3).
//	    WithKeys(1000).
//	    WithFaults(faultinjector.DropSeqRange(500, 500)).
//	    WithSettlementWindow(time.Second).
//	    Run(func(s *scenario.Session) {
//	        s.PublishEvents(1000)
//	        s.RunMaterializer()
//	        s.AdvanceClock(5 * time.Second)
//	        r := s.Sweep()
//	        s.RequireDivergentKeys(r, 1)
//	    })
//
// # It drives the real check
//
// Every message goes through check.Ingest — the same applier Run uses in
// production — rather than through a copy of the pipeline living here. That is
// not a stylistic preference. The harness used to wire the pieces up itself,
// and the duplicate diverged from the real thing in a way that made a whole
// class of bug invisible to every test in the repository until the composition
// test in Phase 5 found it (D-013). A fault matrix that tests a replica of the
// product proves nothing about the product.
//
// Messages are applied synchronously on the test's goroutine rather than
// through Run's channel, which makes every row of §15 deterministic: no
// polling, no Eventually, no ordering that depends on the scheduler.
//
// # Which stream the faults perturb
//
// This is the decision the DSL exists to make explicit, and §13 is emphatic
// about it. FaultsOnDriftwatch (the default) perturbs the stream driftwatch
// subscribes to, leaving the materializer's view perfect: the target is right
// and driftwatch's oracle is wrong, so any disagreement is driftwatch's own
// fault and must be reported as suspect, never confirmed.
// FaultsOnMaterializer perturbs the other side: the target is genuinely wrong
// and driftwatch must confirm it.
//
// Both directions run the same publisher through the same events. Only the
// placement of the injector differs, which is what makes the pair a controlled
// experiment rather than two unrelated tests.
package scenario

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/codec"
	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/pkg/metrics"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/source"
	"github.com/nabrahma/driftwatch/pkg/sweeper"
	"github.com/nabrahma/driftwatch/pkg/target"
	"github.com/nabrahma/driftwatch/test/harness/faultinjector"
	"github.com/nabrahma/driftwatch/test/harness/materializer"
	"github.com/nabrahma/driftwatch/test/harness/publisher"
)

// FaultTarget names which subscription the injector sits in front of.
type FaultTarget int

const (
	// FaultsOnDriftwatch perturbs driftwatch's own stream. The target stays
	// correct, so findings must be suspect and never confirmed.
	FaultsOnDriftwatch FaultTarget = iota
	// FaultsOnMaterializer perturbs the materializer's stream. The target
	// becomes genuinely wrong, so findings must be confirmed.
	FaultsOnMaterializer
)

func epoch() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }

// Epoch returns the instant every scenario's clock starts at, so a test can do
// its own arithmetic against it.
func Epoch() time.Time { return epoch() }

// drainBudget bounds how long the injector may take to push a scenario's events
// through its fault chain. Generous, because it must cover a run under -race
// and coverage instrumentation at once; it bounds a stall, not a property.
const drainBudget = 3 * time.Minute

// Builder accumulates a scenario's configuration.
type Builder struct {
	t *testing.T

	spec       check.Spec
	publishers int
	keys       int
	seed       int64
	shape      projection.Shape
	shapeSet   bool

	faults      []faultinjector.Fault
	faultTarget FaultTarget

	seedTarget    func(*target.MemoryTarget)
	maxPubLabs    int
	targetFailure float64
}

// New starts a scenario: a memory source, a memory target, a scalar projection
// and a five-second static settlement window.
func New(t *testing.T) *Builder {
	t.Helper()

	b := &Builder{
		t:          t,
		publishers: 1,
		keys:       100,
		seed:       1,
	}

	b.spec = check.Spec{
		Name:       "faults",
		Namespace:  "test",
		Source:     check.SourceSpec{Type: "memory"},
		Codec:      check.CodecSpec{Type: "json"},
		Projection: check.ProjectionSpec{Type: "scalar"},
		Target:     check.TargetSpec{Type: "memory"},
		Policy: check.PolicySpec{
			SettlementWindow: check.SettlementWindowSpec{
				Mode:   check.WindowStatic,
				Static: check.Duration(5 * time.Second),
			},
			SweepInterval: check.Duration(30 * time.Second),
			Bootstrap:     check.BootstrapWait,
		},
	}
	return b
}

// WithProjection selects the projection under test.
func (b *Builder) WithProjection(name string, cfg ...map[string]string) *Builder {
	b.spec.Projection.Type = name

	if len(cfg) > 0 {
		for k, v := range cfg[0] {
			switch k {
			case "keyTemplate":
				b.spec.Projection.KeyTemplate = v
			case "memberTemplate":
				b.spec.Projection.MemberTemplate = v
			default:
				if b.spec.Projection.Extra == nil {
					b.spec.Projection.Extra = map[string]string{}
				}
				b.spec.Projection.Extra[k] = v
			}
		}
	}
	return b
}

// WithPublishers sets how many publisher identities emit.
func (b *Builder) WithPublishers(n int) *Builder { b.publishers = n; return b }

// WithKeys sets the size of the key space.
func (b *Builder) WithKeys(n int) *Builder { b.keys = n; return b }

// WithSeed makes the event stream reproducible.
func (b *Builder) WithSeed(seed int64) *Builder { b.seed = seed; return b }

// WithShape overrides the shape the synthetic publisher emits, for the cases
// where the events under test are not the ones the projection expects.
func (b *Builder) WithShape(s projection.Shape) *Builder {
	b.shape, b.shapeSet = s, true
	return b
}

// WithSettlementWindow sets a static W.
func (b *Builder) WithSettlementWindow(d time.Duration) *Builder {
	b.spec.Policy.SettlementWindow = check.SettlementWindowSpec{
		Mode:   check.WindowStatic,
		Static: check.Duration(d),
	}
	return b
}

// WithAdaptiveWindow puts W under the lag estimator's control.
func (b *Builder) WithAdaptiveWindow(minW, maxW time.Duration, safety float64) *Builder {
	b.spec.Policy.SettlementWindow = check.SettlementWindowSpec{
		Mode:         check.WindowAdaptive,
		Static:       check.Duration(minW),
		Min:          check.Duration(minW),
		Max:          check.Duration(maxW),
		SafetyFactor: check.Decimal(safety),
	}
	return b
}

// WithPolicy edits the policy directly, for the fields that do not deserve a
// setter of their own.
func (b *Builder) WithPolicy(fn func(*check.PolicySpec)) *Builder {
	fn(&b.spec.Policy)
	return b
}

// WithCodec edits the codec configuration.
func (b *Builder) WithCodec(fn func(*check.CodecSpec)) *Builder {
	fn(&b.spec.Codec)
	return b
}

// WithSource edits the source configuration.
func (b *Builder) WithSource(fn func(*check.SourceSpec)) *Builder {
	fn(&b.spec.Source)
	return b
}

// WithFaults installs the faults, applied in the order given.
func (b *Builder) WithFaults(faults ...faultinjector.Fault) *Builder {
	b.faults = faults
	return b
}

// WithFaultsOn chooses which subscription the faults perturb.
func (b *Builder) WithFaultsOn(which FaultTarget) *Builder { b.faultTarget = which; return b }

// WithSeededTarget fills the store before the check starts, which is how a
// bootstrap scenario gets a keyspace that predates the subscription.
func (b *Builder) WithSeededTarget(fn func(*target.MemoryTarget)) *Builder {
	b.seedTarget = fn
	return b
}

// WithMaxPublisherLabels bounds the publisher metric label, for the row that
// asserts the collapse.
func (b *Builder) WithMaxPublisherLabels(n int) *Builder { b.maxPubLabs = n; return b }

// WithTargetFailureRate makes a fraction of store reads fail, which is how an
// unreachable or a flaky store is expressed in process.
func (b *Builder) WithTargetFailureRate(rate float64) *Builder {
	b.targetFailure = rate
	return b
}

// Session is a running scenario.
type Session struct {
	t   *testing.T
	clk clock.FakeClock

	chk *check.Check
	reg *prometheus.Registry

	pub   *publisher.Publisher
	mat   *materializer.Materializer
	mem   *target.MemoryTarget
	rec   *target.RecordingTarget
	codec codec.Codec
	shape projection.Shape

	faults      []faultinjector.Fault
	faultTarget FaultTarget

	window time.Duration
}

// Run builds the scenario and hands it to fn.
func (b *Builder) Run(fn func(s *Session)) {
	b.t.Helper()

	clk := clock.Fake(epoch())

	reg := prometheus.NewRegistry()
	met := metrics.New(metrics.Options{Registry: reg, MaxPublisherLabels: b.maxPubLabs})

	spec := b.spec
	if b.targetFailure > 0 {
		spec.Target.Settings = map[string]string{}
		if b.targetFailure > 0 {
			spec.Target.Settings["failureRate"] =
				strconv.FormatFloat(b.targetFailure, 'f', -1, 64)
		}
	}

	spec.ApplyDefaults()
	require.NoError(b.t, spec.Validate(), "the scenario built an invalid spec")

	chk, err := check.New(spec, check.Deps{Clock: clk, Metrics: met})
	require.NoError(b.t, err)
	b.t.Cleanup(func() { require.NoError(b.t, chk.Close()) })

	mem, ok := chk.Target().(*target.MemoryTarget)
	require.True(b.t, ok, "the scenario configures a memory target")

	// The recording wrapper is what enforces I13: the instant driftwatch issues
	// a mutating command the test fails. The seeding below goes through its
	// fixture scope, which is how a test writes to the store it audits without
	// tripping the check that driftwatch never does.
	rec := target.Recording(b.t, mem)
	if b.seedTarget != nil {
		rec.Fixture(func() { b.seedTarget(mem) })
	}

	proj, err := projection.New(spec.Projection.Type, spec.ProjectionConfig())
	require.NoError(b.t, err)

	shape := proj.TargetShape()
	if b.shapeSet {
		shape = b.shape
	}

	dec, err := codec.New(spec.Codec.Type, spec.CodecConfig())
	require.NoError(b.t, err)

	mat, err := materializer.New(materializer.Config{
		Store:      materializer.NewMemoryStore(mem, rec),
		Codec:      dec,
		Shape:      proj.TargetShape(),
		Projection: proj,
	})
	require.NoError(b.t, err)

	pub := publisher.New(publisher.Config{
		Publishers: b.publishers,
		Keys:       b.keys,
		Shape:      shape,
		Seed:       b.seed,
		Clock:      clk,
	})

	s := &Session{
		t: b.t, clk: clk, chk: chk, reg: reg,
		pub: pub, mat: mat, mem: mem, rec: rec, codec: dec, shape: shape,
		faults: b.faults, faultTarget: b.faultTarget,
		window: spec.EffectiveWindow(),
	}

	s.bootstrap()
	fn(s)
}

// bootstrap runs the check's bootstrap synchronously, so a scenario starts in
// the state a running check would be in without needing Run's goroutines.
func (s *Session) bootstrap() {
	s.t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.chk.Run(ctx) }()

	select {
	case <-s.chk.Bootstrapped():
	case <-time.After(30 * time.Second):
		s.t.Fatal("bootstrap never completed")
	}

	// Stop Run again: everything from here is driven synchronously through
	// Ingest and SweepNow, which is what makes each row of §15 deterministic.
	// Leaving the loops running would put a sweep ticker and a confirm ticker
	// on the same fake clock the test advances, and every assertion would then
	// depend on which goroutine woke first.
	cancel()

	select {
	case err := <-done:
		require.NoError(s.t, err)
	case <-time.After(30 * time.Second):
		s.t.Fatal("the check did not stop")
	}
}

// ---------------------------------------------------------------------------
// Driving the pipeline.
// ---------------------------------------------------------------------------

// PublishEvents emits n events and feeds both consumers.
//
// The two subscriptions see the same messages except where the injector sits.
// Running the faults over a copy of the stream rather than over the publisher
// means the unperturbed side is genuinely unperturbed, which is what makes the
// suspect-versus-confirmed distinction meaningful.
func (s *Session) PublishEvents(n int) {
	s.t.Helper()
	s.Deliver(s.pub.Emit(n))
}

// Deliver feeds an explicit stream through both consumers, for the rows whose
// events the synthetic publisher cannot express.
func (s *Session) Deliver(published []source.RawMessage) {
	s.t.Helper()

	driftwatchStream := published
	materializerStream := published

	if len(s.faults) > 0 {
		perturbed := s.perturb(published)
		if s.faultTarget == FaultsOnDriftwatch {
			driftwatchStream = perturbed
		} else {
			materializerStream = perturbed
		}
	}

	for _, msg := range materializerStream {
		s.mat.Apply(msg)
	}
	for _, msg := range driftwatchStream {
		s.chk.Ingest(msg)
	}
}

// Ingest feeds one message to driftwatch only, leaving the target untouched.
func (s *Session) Ingest(msg source.RawMessage) {
	s.t.Helper()
	s.chk.Ingest(msg)
}

// Materialize applies one message to the target only, leaving driftwatch's view
// untouched.
func (s *Session) Materialize(msg source.RawMessage) {
	s.t.Helper()
	s.mat.Apply(msg)
}

// Message builds a raw message carrying a JSON event, stamped with the current
// clock reading.
func (s *Session) Message(payload string) source.RawMessage {
	return source.RawMessage{Payload: []byte(payload), ObservedAt: s.clk.Now()}
}

// Emit returns n messages from the synthetic publisher without delivering them,
// so a test can perturb or withhold specific ones by hand.
func (s *Session) Emit(n int) []source.RawMessage { return s.pub.Emit(n) }

// perturb runs a stream through the configured faults.
//
// It uses a memory source and a real Injector rather than calling the faults
// directly, so that timed faults get the same treatment they would in a live
// pipeline and the scenario tests the injector too.
func (s *Session) perturb(msgs []source.RawMessage) []source.RawMessage {
	s.t.Helper()

	src := source.NewMemory(s.clk, source.WithCapacity(len(msgs)+16))
	for _, msg := range msgs {
		require.True(s.t, src.Publish(msg))
	}

	inj := faultinjector.Wrap(src, s.clk, s.faults...)
	out := make(chan source.RawMessage, 8*len(msgs)+64)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- inj.Run(ctx, out) }()

	require.Eventually(s.t, func() bool { return inj.Stats().Received == uint64(len(msgs)) },
		drainBudget, time.Millisecond, "the injector never drained the stream")

	// Let anything a timed fault is holding come due and leave in one sorted
	// release, before the close sends the remainder out through the flush
	// instead. See Injector.HasPending.
	if inj.HasPending() {
		s.clk.Advance(time.Hour)
		require.Eventually(s.t, func() bool { return !inj.HasPending() },
			drainBudget, time.Millisecond, "held messages were never released")
	}
	require.NoError(s.t, src.Close())

	select {
	case err := <-done:
		require.NoError(s.t, err)
	case <-time.After(20 * time.Second):
		s.t.Fatal("the injector did not finish")
	}

	close(out)
	got := make([]source.RawMessage, 0, len(out))
	for msg := range out {
		got = append(got, msg)
	}
	return got
}

// RunMaterializer is a no-op kept for the DSL's readability.
//
// The materializer applies events as they are published rather than in a later
// pass, because a reference consumer that ran afterwards would make every
// scenario's timing a fiction. The call site still reads better with the line
// in it, and §13's example includes it.
func (s *Session) RunMaterializer() {}

// AdvanceClock moves the fake clock, which is how a scenario waits.
func (s *Session) AdvanceClock(d time.Duration) { s.clk.Advance(d) }

// Settle advances until every published event has settled.
//
// It advances past the publisher's clock rather than past the scenario's,
// because the two are not the same: the synthetic publisher stamps consecutive
// events a millisecond apart, so after a few hundred events its timestamps are
// ahead of the fake clock the test is advancing. Settling by W alone leaves the
// most recent events still in flight, every sweep compares nothing, and a test
// that asserted "no findings" would pass for entirely the wrong reason.
func (s *Session) Settle() {
	deadline := s.pub.Now().Add(s.window + time.Second)

	if d := deadline.Sub(s.clk.Now()); d > 0 {
		s.clk.Advance(d)
		return
	}
	s.clk.Advance(s.window + time.Second)
}

// Window returns the settlement window in force.
func (s *Session) Window() time.Duration { return s.window }

// Now returns the current clock reading.
func (s *Session) Now() time.Time { return s.clk.Now() }

// Sweep runs one oracle-to-target pass and returns the report.
func (s *Session) Sweep() *differ.Report {
	s.t.Helper()

	// Everything here runs on one goroutine, so the flush that the applier
	// loop would do on its ticker has to happen explicitly. Without it a sweep
	// could compare against an oracle missing an update still held in the
	// reorder buffer.
	s.chk.FlushReorder()

	rep, err := s.chk.SweepNow(context.Background())
	require.NoError(s.t, err)
	return rep
}

// TrySweep runs a sweep that is allowed to fail, for the rows where the store
// is unreachable and the error is the assertion.
func (s *Session) TrySweep() (*differ.Report, error) {
	s.t.Helper()

	s.chk.FlushReorder()
	return s.chk.SweepNow(context.Background())
}

// ScanExtras runs one target-to-oracle pass. Two calls a window apart are
// needed before anything is reported (§5.5).
func (s *Session) ScanExtras() *differ.Report {
	s.t.Helper()

	s.chk.FlushReorder()

	rep, err := s.chk.ScanExtras(context.Background())
	require.NoError(s.t, err)
	return rep
}

// ScanExtrasTwice runs both passes with a settlement window between them, which
// is the complete extras comparison.
func (s *Session) ScanExtrasTwice() *differ.Report {
	s.t.Helper()

	s.ScanExtras()
	s.Settle()
	return s.ScanExtras()
}

// SignalSourceGap tells the check its transport may have missed messages,
// which is what a reconnect means on a stream with no replay.
func (s *Session) SignalSourceGap(reason source.GapReason, detail string) {
	s.chk.SignalGap(source.GapSignal{
		Source: "scenario",
		Reason: reason,
		At:     s.clk.Now(),
		Detail: detail,
	})
}

// Observe runs one round of the lag estimator, which is how a scenario feeds
// the adaptive settlement window without waiting for a goroutine to notice the
// clock moved.
func (s *Session) Observe() { s.chk.PollLag(context.Background()) }

// Confirm runs a confirmation cycle for every candidate whose window elapsed.
func (s *Session) Confirm() int {
	s.t.Helper()
	return s.chk.ConfirmDue(context.Background())
}

// SweepAndConfirm runs the complete comparison the matrix means by "within
// 2xW": settle, sweep to raise candidates, settle again, confirm, and report.
//
// The leading settle is not optional. A sweep run the instant after publishing
// compares nothing at all, because every key it would look at is still inside
// its settlement window — so the cycle would raise no candidates, confirm
// nothing, and hand back a clean report that means only that it looked too
// early.
func (s *Session) SweepAndConfirm() *differ.Report {
	s.t.Helper()

	s.Settle()
	s.Sweep()

	s.Settle()
	s.Confirm()

	return s.Sweep()
}

// ---------------------------------------------------------------------------
// Reaching into the running check.
// ---------------------------------------------------------------------------

// Check exposes the composition root.
func (s *Session) Check() *check.Check { return s.chk }

// Oracle exposes the oracle for assertions the DSL does not wrap.
func (s *Session) Oracle() *oracle.Oracle { return s.chk.Oracle() }

// Target exposes the store for assertions the DSL does not wrap.
func (s *Session) Target() *target.MemoryTarget { return s.mem }

// Sweeper exposes the sweeper for assertions the DSL does not wrap.
func (s *Session) Sweeper() *sweeper.Sweeper { return s.chk.Sweeper() }

// Materializer exposes the reference consumer.
func (s *Session) Materializer() *materializer.Materializer { return s.mat }

// Gaps returns how many possible-loss events the pipeline noticed: sequence
// gaps, restarts, undecodable frames and source-level gap signals.
func (s *Session) Gaps() uint64 { return s.chk.Status().GapSignals }

// Status returns the check's status snapshot.
func (s *Session) Status() check.Status { return s.chk.Status() }

// Confirmed returns the currently-confirmed findings.
func (s *Session) Confirmed() map[string]differ.Finding { return s.chk.Sweeper().Confirmed() }

// Explain answers "what happened to this key?".
func (s *Session) Explain(key string) *ExplainResult {
	s.t.Helper()

	exp, err := s.chk.Explain(context.Background(), key)
	require.NoError(s.t, err)
	return &ExplainResult{t: s.t, exp: exp}
}

// KeyForSeq returns the key the message at a global position touched.
func (s *Session) KeyForSeq(seq uint64) string { return s.pub.KeyForSeq(seq) }

// Fixture runs fn with the read-only enforcement suspended, which is how a
// scenario writes to the store it audits — the target-store faults in §15.2 all
// need it, and driftwatch itself must never use it.
func (s *Session) Fixture(fn func()) { s.rec.Fixture(fn) }

// WriteOutOfBand puts a scalar into the store with no event to explain it.
func (s *Session) WriteOutOfBand(values map[string][]byte) {
	s.Fixture(func() { s.mem.Seed(values) })
}

// WriteSetOutOfBand puts a member set into the store with no event behind it.
func (s *Session) WriteSetOutOfBand(values map[string][]string) {
	s.Fixture(func() { s.mem.SeedSets(values) })
}

// DeleteOutOfBand removes keys from the store with no event to explain it.
func (s *Session) DeleteOutOfBand(keys ...string) {
	s.Fixture(func() { s.mem.Remove(keys...) })
}

// FlushTarget empties the store, standing in for a FLUSHDB or a restart with no
// persistence.
func (s *Session) FlushTarget() {
	s.Fixture(func() { s.mem.SimulateFlush() })
}

// EvictFromTarget removes n keys the way a store under memory pressure does,
// and reports which ones went.
func (s *Session) EvictFromTarget(n int) []string {
	var evicted []string
	s.Fixture(func() { evicted = s.mem.SimulateEvict(n) })
	return evicted
}

// ExpireInTarget gives a key a deadline, so a TTL can elapse on the fake clock.
func (s *Session) ExpireInTarget(key string, at time.Time) {
	s.Fixture(func() { s.mem.SetExpiry(key, at) })
}

// SetTargetHealth replaces what the store reports about itself: its role, its
// eviction counters, its memory pressure.
//
//nolint:gocritic // hugeParam: Health by value matches pkg/target's own API.
func (s *Session) SetTargetHealth(h target.Health) {
	s.Fixture(func() { s.mem.SetHealth(h) })
}

// ---------------------------------------------------------------------------
// Metrics.
// ---------------------------------------------------------------------------

// Metric returns the sum of every series in a metric family, which is what most
// assertions want: "did this counter move".
func (s *Session) Metric(name string) float64 {
	s.t.Helper()
	return s.metricWhere(name, nil)
}

// MetricWith returns the sum of the series matching the given labels.
func (s *Session) MetricWith(name string, labels map[string]string) float64 {
	s.t.Helper()
	return s.metricWhere(name, labels)
}

func (s *Session) metricWhere(name string, labels map[string]string) float64 {
	s.t.Helper()

	families, err := s.reg.Gather()
	require.NoError(s.t, err)

	total := 0.0
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if matchesLabels(m, labels) {
				total += sampleValue(m)
			}
		}
	}
	return total
}

// MetricSeries returns how many distinct series a metric family has, for the
// rows that are about cardinality rather than value.
func (s *Session) MetricSeries(name string) int {
	s.t.Helper()

	families, err := s.reg.Gather()
	require.NoError(s.t, err)

	for _, mf := range families {
		if mf.GetName() == name {
			return len(mf.GetMetric())
		}
	}
	return 0
}

// MetricLabelValues returns the distinct values a label takes in a family.
func (s *Session) MetricLabelValues(name, label string) map[string]float64 {
	s.t.Helper()

	families, err := s.reg.Gather()
	require.NoError(s.t, err)

	out := map[string]float64{}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, pair := range m.GetLabel() {
				if pair.GetName() == label {
					out[pair.GetValue()] += sampleValue(m)
				}
			}
		}
	}
	return out
}

func matchesLabels(m *dto.Metric, labels map[string]string) bool {
	for want, value := range labels {
		found := false
		for _, pair := range m.GetLabel() {
			if pair.GetName() == want && pair.GetValue() == value {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func sampleValue(m *dto.Metric) float64 {
	switch {
	case m.GetCounter() != nil:
		return m.GetCounter().GetValue()
	case m.GetGauge() != nil:
		return m.GetGauge().GetValue()
	case m.GetHistogram() != nil:
		return float64(m.GetHistogram().GetSampleCount())
	default:
		return 0
	}
}

// ---------------------------------------------------------------------------
// Assertions.
// ---------------------------------------------------------------------------

// RequireDivergentKeys asserts the report names exactly n keys.
func (s *Session) RequireDivergentKeys(r *differ.Report, n int) {
	s.t.Helper()
	require.Len(s.t, r.Findings, n,
		"expected %d divergent keys, got %d: %s", n, len(r.Findings), r.Summary())
}

// RequireCategory asserts how many findings carry a category.
func (s *Session) RequireCategory(r *differ.Report, cat differ.Category, n int) {
	s.t.Helper()
	assert.Equal(s.t, n, r.ByCategory[cat],
		"expected %d findings of category %s, got %d", n, cat, r.ByCategory[cat])
}

// RequireNoFindings asserts a clean report, quoting it when it is not.
func (s *Session) RequireNoFindings(r *differ.Report) {
	s.t.Helper()
	assert.Zero(s.t, r.Total(), "expected a clean report, got: %s", r.Summary())
}

// RequireNoConfirmedDrift asserts driftwatch confirmed nothing.
//
// This is the assertion for a scenario whose faults were on driftwatch's own
// stream. Confirming there would mean claiming the target is wrong on the
// strength of events driftwatch knows it did not receive.
func (s *Session) RequireNoConfirmedDrift() {
	s.t.Helper()
	assert.Empty(s.t, s.Confirmed(),
		"driftwatch confirmed drift from a stream it knows it lost events from")
}

// RequireAllFindingsSuspect asserts every finding is on a key driftwatch does
// not trust itself about, so none of them is alertable.
func (s *Session) RequireAllFindingsSuspect(r *differ.Report) {
	s.t.Helper()

	assert.Zero(s.t, r.Alertable(),
		"findings from a lossy subscription must never be alertable (§23 A7)")
	assert.Equal(s.t, len(r.Findings), r.ByTrust[oracle.TrustSuspect],
		"every finding should be on a suspect key")
}

// RequireTrust asserts a key's trust state.
func (s *Session) RequireTrust(key string, want oracle.TrustState, why ...string) {
	s.t.Helper()

	entry, ok := s.Oracle().Get(key)
	require.True(s.t, ok, "the oracle has no entry for %q", key)

	reason := "trust state of " + key
	if len(why) > 0 {
		reason = why[0]
	}
	assert.Equal(s.t, want, entry.Trust, reason)
}

// RequireOracleValue asserts what the oracle expects for a key.
func (s *Session) RequireOracleValue(key, want string) {
	s.t.Helper()

	entry, ok := s.Oracle().Get(key)
	require.True(s.t, ok, "the oracle has no entry for %q", key)
	assert.Equal(s.t, want, entry.Value.String(), "oracle value of %q", key)
}

// ExplainResult wraps an explanation with assertions, so a row that names a
// diagnosis code reads as one line.
type ExplainResult struct {
	t   *testing.T
	exp interface {
		Has(string) bool
		Text() string
	}
}

// RequireDiagnosis asserts a diagnosis code fired.
func (e *ExplainResult) RequireDiagnosis(code string) *ExplainResult {
	e.t.Helper()
	assert.True(e.t, e.exp.Has(code),
		"expected the %s diagnosis; got:\n%s", code, e.exp.Text())
	return e
}

// RequireNoDiagnosis asserts a diagnosis code did not fire.
func (e *ExplainResult) RequireNoDiagnosis(code string) *ExplainResult {
	e.t.Helper()
	assert.False(e.t, e.exp.Has(code),
		"the %s diagnosis fired and should not have; got:\n%s", code, e.exp.Text())
	return e
}

// RequireText asserts the rendered explanation contains a substring.
func (e *ExplainResult) RequireText(substr string) *ExplainResult {
	e.t.Helper()
	assert.Contains(e.t, e.exp.Text(), substr)
	return e
}

// Text returns the rendered explanation.
func (e *ExplainResult) Text() string { return e.exp.Text() }

// ---------------------------------------------------------------------------
// Event construction, for rows the synthetic publisher cannot express.
// ---------------------------------------------------------------------------

// Event builds one JSON event in the canonical driftwatch format.
type Event struct {
	Publisher string
	Epoch     uint64
	Seq       uint64
	Op        string
	Key       string
	Member    string
	Value     string
	Delta     int64
	TS        string
	// TTL is the lifetime the event declares, in Go duration syntax. Only the
	// Model expiry policy reads it.
	TTL string
}

// JSON renders the event as the codec expects it.
//
//nolint:gocritic // hugeParam: an Event literal at the call site is the point.
func (e Event) JSON() string {
	var b strings.Builder

	fmt.Fprintf(&b, `{"publisher":%q,"epoch":%d,"seq":%d,"op":%q`,
		e.Publisher, e.Epoch, e.Seq, e.Op)

	if e.Key != "" {
		fmt.Fprintf(&b, `,"key":%q`, e.Key)
	}
	if e.Member != "" {
		fmt.Fprintf(&b, `,"member":%q`, e.Member)
	}
	if e.Value != "" {
		fmt.Fprintf(&b, `,"value":%q`, e.Value)
	}
	if e.Delta != 0 {
		fmt.Fprintf(&b, `,"delta":%d`, e.Delta)
	}
	if e.TTL != "" {
		fmt.Fprintf(&b, `,"ttl":%q`, e.TTL)
	}
	if e.TS != "" {
		fmt.Fprintf(&b, `,"ts":%q`, e.TS)
	}

	b.WriteByte('}')
	return b.String()
}

// Msg renders the event as a raw message stamped now.
//
//nolint:gocritic // hugeParam: see Event.JSON.
func (s *Session) Msg(e Event) source.RawMessage {
	if e.Publisher == "" {
		e.Publisher = "replica-0"
	}
	if e.Epoch == 0 {
		e.Epoch = 1
	}
	return s.Message(e.JSON())
}
