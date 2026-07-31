package explain_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/explain"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/seqtrack"
	"github.com/nabrahma/driftwatch/pkg/target"
)

func epoch() time.Time { return time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC) }

// fixture drives the real pipeline so that each rule test constructs the state
// that triggers it, rather than hand-building an Explanation.
//
// Hand-building would make every test pass by construction: the point of a rule
// test is that a particular sequence of events, folded by the real projection
// into the real oracle, is what makes the rule fire.
type fixture struct {
	t   *testing.T
	clk clock.FakeClock

	orc     *oracle.Oracle
	tracker *seqtrack.Tracker
	mem     *target.MemoryTarget
	proj    projection.Projection

	window    time.Duration
	ringSize  int
	evictions uint64
}

type fixtureOption func(*fixtureConfig)

type fixtureConfig struct {
	projection string
	window     time.Duration
	ringSize   int
}

func withProjection(name string) fixtureOption {
	return func(c *fixtureConfig) { c.projection = name }
}

func withWindow(d time.Duration) fixtureOption {
	return func(c *fixtureConfig) { c.window = d }
}

func withRingSize(n int) fixtureOption {
	return func(c *fixtureConfig) { c.ringSize = n }
}

func newFixture(t *testing.T, opts ...fixtureOption) *fixture {
	t.Helper()

	cfg := fixtureConfig{projection: "keysetOwnership", window: 5 * time.Second, ringSize: 16}
	for _, opt := range opts {
		opt(&cfg)
	}

	clk := clock.Fake(epoch())
	proj, err := projection.New(cfg.projection, nil)
	require.NoError(t, err)

	return &fixture{
		t:   t,
		clk: clk,
		orc: oracle.New(oracle.Config{
			Clock:            clk,
			SettlementWindow: cfg.window,
			RingSize:         cfg.ringSize,
		}),
		tracker:  seqtrack.New(seqtrack.Config{Clock: clk}),
		mem:      target.NewMemory(target.WithClock(clk)),
		proj:     proj,
		window:   cfg.window,
		ringSize: cfg.ringSize,
	}
}

// apply runs one event through sequence tracking, the projection and the
// oracle, exactly as pkg/check does.
// and this is a test fixture.
//
//nolint:gocritic // hugeParam: an Event by value keeps the call sites readable,
func (f *fixture) apply(e event.Event) {
	f.t.Helper()

	now := f.clk.Now()
	if e.ObservedAt.IsZero() {
		e.ObservedAt = now
	}
	if e.PublishedAt.IsZero() {
		e.PublishedAt = e.ObservedAt
	}
	if e.Publisher == "" {
		e.Publisher = "replica-0"
	}

	verdict, _ := f.tracker.Observe(&e)
	if verdict == seqtrack.DropDuplicate || verdict == seqtrack.DropStaleEpoch {
		return
	}

	// The pipeline widens suspicion after a gap, because driftwatch cannot know
	// which keys the missing events touched. Mirroring it here is what makes
	// the suspect-key rule tests exercise the real path.
	if verdict == seqtrack.AcceptWithGap || verdict == seqtrack.AcceptAfterRestart {
		f.orc.MarkSuspect("", "sequence gap on "+e.Publisher)
	}

	key, err := f.proj.TargetKey(&e)
	require.NoError(f.t, err)

	prev, _ := f.orc.Get(key)
	mutation, err := f.proj.Apply(prev.Value, &e)
	require.NoError(f.t, err)

	f.orc.Apply(mutation, &e, verdict, f.tracker.Trust(e.Publisher))
}

// add emits an `add` event for the set projection.
func (f *fixture) add(key, member string, seq uint64) {
	f.t.Helper()
	f.apply(event.Event{Op: event.OpAdd, Key: key, Member: member, Seq: seq, Epoch: 1})
}

// set emits a `set` event for the scalar projection.
func (f *fixture) set(key, value string, seq uint64) {
	f.t.Helper()
	f.apply(event.Event{Op: event.OpSet, Key: key, Value: []byte(value), Seq: seq, Epoch: 1})
}

// fill emits heartbeats for a range of sequence numbers.
//
// A publisher's sequence is global across its whole keyspace, so the events
// touching one key are almost never consecutive. Without this, a fixture that
// emits seq 8841 then 8847 for the same key manufactures a gap that a real
// publisher would not have left — and the gap rule then fires over the top of
// whatever the test was actually about.
func (f *fixture) fill(from, to uint64) {
	f.t.Helper()

	for seq := from; seq <= to; seq++ {
		f.apply(event.Event{Op: event.OpHeartbeat, Seq: seq, Epoch: 1})
	}
}

// materialize writes what the oracle expects into the target, which is what a
// working materializer would have done.
func (f *fixture) materialize(keys ...string) {
	f.t.Helper()

	for _, key := range keys {
		entry, ok := f.orc.Get(key)
		if !ok {
			continue
		}
		switch entry.Value.Kind {
		case event.ValueScalar:
			f.mem.Seed(map[string][]byte{key: entry.Value.Scalar})
		case event.ValueSet:
			members := make([]string, 0, len(entry.Value.Members))
			for m := range entry.Value.Members {
				members = append(members, m)
			}
			f.mem.SeedSets(map[string][]string{key: members})
		case event.ValueAbsent, event.ValueCounter:
		}
	}
}

// advance moves the fake clock, which is how a fixture waits out the window.
func (f *fixture) advance(d time.Duration) { f.clk.Advance(d) }

// explain runs the engine over the fixture's state.
func (f *fixture) explain(key string) *explain.Explanation {
	f.t.Helper()

	e, err := explain.Explain(context.Background(), explain.Input{
		Key:                     key,
		Oracle:                  f.orc,
		Target:                  f.mem,
		SeqTrack:                f.tracker,
		Shape:                   f.proj.TargetShape(),
		Window:                  f.window,
		Clock:                   f.clk,
		RingSize:                f.ringSize,
		EvictionsSinceLastSweep: f.evictions,
	})
	require.NoError(f.t, err)
	return e
}

// explainWithoutTarget runs the engine with no store to read, which is the
// unreachable case.
func (f *fixture) explainWithoutTarget(key string) *explain.Explanation {
	f.t.Helper()

	e, err := explain.Explain(context.Background(), explain.Input{
		Key:      key,
		Oracle:   f.orc,
		SeqTrack: f.tracker,
		Shape:    f.proj.TargetShape(),
		Window:   f.window,
		Clock:    f.clk,
		RingSize: f.ringSize,
	})
	require.NoError(f.t, err)
	return e
}
