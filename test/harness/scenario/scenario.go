// Package scenario provides the declarative fault-scenario DSL (§13).
//
// A fault test has a lot of moving parts — a publisher, two subscriptions, an
// injector, an oracle, a materializer, a target, a sweeper and a clock — and
// wiring them by hand in every test buries the one line that says what the test
// is about. The DSL exists so that line survives:
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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/codec"
	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/seqtrack"
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

// Builder accumulates a scenario's configuration.
type Builder struct {
	t *testing.T

	projectionName string
	projectionCfg  map[string]string
	publishers     int
	keys           int
	seed           int64
	window         time.Duration
	faults         []faultinjector.Fault
	faultTarget    FaultTarget
	differOptions  differ.Options
}

// New starts a scenario.
func New(t *testing.T) *Builder {
	t.Helper()

	return &Builder{
		t:              t,
		projectionName: "scalar",
		publishers:     1,
		keys:           100,
		seed:           1,
		window:         5 * time.Second,
	}
}

// WithProjection selects the projection under test.
func (b *Builder) WithProjection(name string, cfg ...map[string]string) *Builder {
	b.projectionName = name
	if len(cfg) > 0 {
		b.projectionCfg = cfg[0]
	}
	return b
}

// WithPublishers sets how many publisher identities emit.
func (b *Builder) WithPublishers(n int) *Builder { b.publishers = n; return b }

// WithKeys sets the size of the key space.
func (b *Builder) WithKeys(n int) *Builder { b.keys = n; return b }

// WithSeed makes the event stream reproducible.
func (b *Builder) WithSeed(seed int64) *Builder { b.seed = seed; return b }

// WithSettlementWindow sets W.
func (b *Builder) WithSettlementWindow(d time.Duration) *Builder { b.window = d; return b }

// WithFaults installs the faults, applied in the order given.
func (b *Builder) WithFaults(faults ...faultinjector.Fault) *Builder {
	b.faults = faults
	return b
}

// WithFaultsOn chooses which subscription the faults perturb.
func (b *Builder) WithFaultsOn(which FaultTarget) *Builder { b.faultTarget = which; return b }

// WithDifferOptions sets the expiry policy and reporting limits.
func (b *Builder) WithDifferOptions(opts differ.Options) *Builder {
	b.differOptions = opts
	return b
}

// Session is a running scenario.
type Session struct {
	t   *testing.T
	clk clock.FakeClock

	pub  *publisher.Publisher
	proj projection.Projection

	orc     *oracle.Oracle
	tracker *seqtrack.Tracker
	mem     *target.MemoryTarget
	rec     *target.RecordingTarget
	swp     *sweeper.Sweeper
	mat     *materializer.Materializer
	codec   codec.Codec

	faults      []faultinjector.Fault
	faultTarget FaultTarget

	// gaps counts possible-loss signals the pipeline acted on.
	gaps int
}

// Run builds the scenario and hands it to fn.
func (b *Builder) Run(fn func(s *Session)) {
	b.t.Helper()

	clk := clock.Fake(epoch())

	proj, err := projection.New(b.projectionName, b.projectionCfg)
	require.NoError(b.t, err)

	dec, err := codec.New("json", nil)
	require.NoError(b.t, err)

	mem := target.NewMemory(target.WithClock(clk))
	rec := target.Recording(b.t, mem)

	orc := oracle.New(oracle.Config{Clock: clk, SettlementWindow: b.window})
	tracker := seqtrack.New(seqtrack.Config{Clock: clk})

	swp := sweeper.New(sweeper.Config{
		Oracle:           orc,
		Target:           rec,
		Shape:            proj.TargetShape(),
		Clock:            clk,
		SettlementWindow: func() time.Duration { return b.window },
		DifferOptions:    b.differOptions,
	})
	b.t.Cleanup(func() { require.NoError(b.t, swp.Close()) })

	mat, err := materializer.New(materializer.Config{
		Store: materializer.NewMemoryStore(mem, rec),
		Codec: dec,
		Shape: proj.TargetShape(),
	})
	require.NoError(b.t, err)

	pub := publisher.New(publisher.Config{
		Publishers: b.publishers,
		Keys:       b.keys,
		Shape:      proj.TargetShape(),
		Seed:       b.seed,
		Clock:      clk,
	})

	s := &Session{
		t: b.t, clk: clk, pub: pub, proj: proj,
		orc: orc, tracker: tracker, mem: mem, rec: rec, swp: swp,
		mat: mat, codec: dec,
		faults: b.faults, faultTarget: b.faultTarget,
	}
	fn(s)
}

// PublishEvents emits n events and feeds both consumers.
//
// The two subscriptions see the same messages except where the injector sits.
// Running the faults over a copy of the stream rather than over the publisher
// means the unperturbed side is genuinely unperturbed, which is what makes the
// suspect-versus-confirmed distinction meaningful.
func (s *Session) PublishEvents(n int) {
	s.t.Helper()

	published := s.pub.Emit(n)

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
		s.ingest(msg)
	}
}

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
		20*time.Second, time.Millisecond, "the injector never drained the stream")

	// Let anything a timed fault is holding come due and leave in one sorted
	// release, before the close sends the remainder out through the flush
	// instead. See Injector.HasPending.
	if inj.HasPending() {
		s.clk.Advance(time.Hour)
		require.Eventually(s.t, func() bool { return !inj.HasPending() },
			20*time.Second, time.Millisecond, "held messages were never released")
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

// ingest runs one message through driftwatch's pipeline: decode, sequence
// tracking, projection, oracle.
func (s *Session) ingest(msg source.RawMessage) {
	s.t.Helper()

	var e event.Event
	e.ObservedAt = msg.ObservedAt

	if err := s.codec.Decode(msg.Payload, msg.Topic, &e); err != nil {
		// A message driftwatch cannot decode is a message it did not see. That
		// is loss, and loss it knows about, so the keys it might have touched
		// become suspect — the same treatment a detected gap gets.
		s.orc.MarkSuspect("", "undecodable payload")
		s.gaps++
		return
	}

	verdict, _ := s.tracker.Observe(&e)
	if verdict == seqtrack.DropDuplicate {
		return
	}

	// A gap or an unannounced restart means events were lost, and driftwatch
	// cannot know which keys they touched — that information was in the lost
	// events. §5.2's answer is to widen suspicion to whatever the projection's
	// ownership model allows: a partitioned keyspace loses trust only in the
	// affected publisher's partition, and everything else loses trust
	// everywhere.
	//
	// This is the step that decides whether driftwatch is honest. Without it
	// the oracle keeps its stale expectation at full confidence and reports the
	// store as wrong on the strength of events it never received.
	if verdict == seqtrack.AcceptWithGap || verdict == seqtrack.AcceptAfterRestart {
		s.gaps++
		s.orc.MarkSuspect(s.suspectPattern(e.Publisher), "sequence gap on "+e.Publisher)
	}

	prev, _ := s.orc.Get(e.Key)
	mutation, err := s.proj.Apply(prev.Value, &e)
	if err != nil {
		return
	}
	s.orc.Apply(mutation, &e, verdict, s.tracker.Trust(e.Publisher))
}

// suspectPattern returns the keyspace a publisher's loss casts doubt over.
//
// An empty pattern means every key, which is the right answer whenever the
// projection does not promise that publishers own disjoint keyspaces. It is a
// blunt instrument and deliberately so: suppressing findings too widely costs
// coverage, and suppressing them too narrowly costs correctness.
func (s *Session) suspectPattern(publisher string) string {
	ownership := s.proj.KeyOwnership()
	if !ownership.Partitioned || ownership.KeyPattern == "" {
		return ""
	}
	return strings.ReplaceAll(ownership.KeyPattern, "{{.Publisher}}", publisher)
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

// Sweep runs one oracle-to-target pass and returns the report.
func (s *Session) Sweep() *differ.Report {
	s.t.Helper()

	rep, err := s.swp.SweepOnce(context.Background())
	require.NoError(s.t, err)
	return rep
}

// Confirm runs a confirmation cycle for every candidate whose window elapsed.
func (s *Session) Confirm() int {
	s.t.Helper()
	return s.swp.ConfirmDue(context.Background(), s.clk.Now())
}

// SweepAndConfirm runs the full two-phase cycle: sweep, wait a window, confirm.
//
// Most scenarios want this rather than the pieces, because a finding that has
// not been through confirmation is not something driftwatch would report.
func (s *Session) SweepAndConfirm(window time.Duration) *differ.Report {
	s.t.Helper()

	s.Sweep()
	s.AdvanceClock(window)
	s.Confirm()

	return s.Sweep()
}

// KeyForSeq returns the key the message at a global position touched.
func (s *Session) KeyForSeq(seq uint64) string { return s.pub.KeyForSeq(seq) }

// Confirmed returns the currently-confirmed findings.
func (s *Session) Confirmed() map[string]differ.Finding { return s.swp.Confirmed() }

// Oracle exposes the oracle for assertions the DSL does not wrap.
func (s *Session) Oracle() *oracle.Oracle { return s.orc }

// Target exposes the store for assertions the DSL does not wrap.
func (s *Session) Target() *target.MemoryTarget { return s.mem }

// Sweeper exposes the sweeper for assertions the DSL does not wrap.
func (s *Session) Sweeper() *sweeper.Sweeper { return s.swp }

// Materializer exposes the reference consumer.
func (s *Session) Materializer() *materializer.Materializer { return s.mat }

// Gaps returns how many possible-loss events the pipeline noticed.
func (s *Session) Gaps() int { return s.gaps }

// ---------------------------------------------------------------------------
// Assertions.
// ---------------------------------------------------------------------------

// RequireDivergentKeys asserts the report names exactly n keys.
func (s *Session) RequireDivergentKeys(r *differ.Report, n int) {
	s.t.Helper()
	require.Len(s.t, r.Findings, n,
		"expected %d divergent keys, got %d", n, len(r.Findings))
}

// RequireCategory asserts how many findings carry a category.
func (s *Session) RequireCategory(r *differ.Report, cat differ.Category, n int) {
	s.t.Helper()
	assert.Equal(s.t, n, r.ByCategory[cat],
		"expected %d findings of category %s, got %d", n, cat, r.ByCategory[cat])
}

// RequireNoConfirmedDrift asserts driftwatch confirmed nothing.
//
// This is the assertion for a scenario whose faults were on driftwatch's own
// stream. Confirming there would mean claiming the target is wrong on the
// strength of events driftwatch knows it did not receive.
func (s *Session) RequireNoConfirmedDrift() {
	s.t.Helper()
	assert.Empty(s.t, s.swp.Confirmed(),
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
func (s *Session) RequireTrust(key string, want oracle.TrustState) {
	s.t.Helper()

	entry, ok := s.orc.Get(key)
	require.True(s.t, ok, "the oracle has no entry for %q", key)
	assert.Equal(s.t, want, entry.Trust, "trust state of %q", key)
}
