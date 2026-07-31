package sweeper_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/seqtrack"
	"github.com/nabrahma/driftwatch/pkg/sweeper"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// A small key universe so the same keys are revisited and the interesting
// interleavings — an event landing mid-confirmation, a repair arriving between
// two sweeps — actually occur instead of every step touching a fresh key.
var propKeys = []string{"a", "b", "c", "d", "e"}

// propHarness is the property tests' own rig.
//
// It is separate from the table tests' harness because these need to drive the
// sweeper through long randomized interleavings and record what happened at
// each step, which the simpler harness has no reason to carry.
type propHarness struct {
	clk clock.FakeClock
	orc *oracle.Oracle
	mem *target.MemoryTarget
	rec *target.RecordingTarget
	swp *sweeper.Sweeper

	window time.Duration
	seq    uint64
}

func newPropHarness(t *rapid.T, w time.Duration) *propHarness {
	clk := clock.Fake(epoch())
	mem := target.NewMemory(target.WithClock(clk))
	rec := target.Recording(t, mem)
	orc := oracle.New(oracle.Config{Clock: clk, SettlementWindow: w})

	h := &propHarness{
		clk: clk, orc: orc, mem: mem, rec: rec, window: w,
		swp: sweeper.New(sweeper.Config{
			Oracle:           orc,
			Target:           rec,
			Shape:            projection.ShapeScalar,
			Clock:            clk,
			SettlementWindow: func() time.Duration { return w },
		}),
	}
	t.Cleanup(func() { require.NoError(t, h.swp.Close()) })
	return h
}

func (h *propHarness) apply(key, value string) {
	h.seq++
	e := &event.Event{
		Publisher: "p", Epoch: 1, Seq: h.seq, Op: event.OpSet,
		Key: key, Value: []byte(value), ObservedAt: h.clk.Now(),
	}
	h.orc.Apply(projection.Mutation{
		Key:    key,
		Action: projection.ActionUpsert,
		Value:  event.Value{Kind: event.ValueScalar, Scalar: []byte(value)},
	}, e, seqtrack.Accept, oracle.TrustComplete)
}

func (h *propHarness) materialize(key, value string) {
	h.rec.Fixture(func() { h.mem.Seed(map[string][]byte{key: []byte(value)}) })
}

func (h *propHarness) remove(key string) {
	h.rec.Fixture(func() { h.mem.Remove(key) })
}

// TestProp_ConfirmedDivergenceImpliesTwoDisagreementsAWindowApart is invariant
// I7.
//
// It is the invariant that justifies the whole two-phase design, and the one a
// plausible-looking refactor is most likely to break: any shortcut that
// confirms a candidate early — reusing the sweep's read, treating a re-queued
// candidate as already waited, letting a shrinking W pull a deadline backwards
// — leaves every other test passing and makes driftwatch report transient lag
// as drift again.
func TestProp_ConfirmedDivergenceImpliesTwoDisagreementsAWindowApart(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		w := time.Duration(rapid.IntRange(1, 10).Draw(t, "windowSeconds")) * time.Second
		h := newPropHarness(t, w)

		ctx := context.Background()
		steps := rapid.IntRange(1, 60).Draw(t, "steps")

		for i := 0; i < steps; i++ {
			switch rapid.IntRange(0, 4).Draw(t, "action") {
			case 0: // an event arrives
				key := rapid.SampledFrom(propKeys).Draw(t, "eventKey")
				h.apply(key, rapid.SampledFrom([]string{"x", "y", "z"}).Draw(t, "eventValue"))

			case 1: // the materializer writes something, right or wrong
				key := rapid.SampledFrom(propKeys).Draw(t, "writeKey")
				h.materialize(key, rapid.SampledFrom([]string{"x", "y", "z"}).Draw(t, "writeValue"))

			case 2: // the materializer drops a key
				h.remove(rapid.SampledFrom(propKeys).Draw(t, "removeKey"))

			case 3: // time passes
				h.clk.Advance(time.Duration(rapid.IntRange(1, 20).Draw(t, "advanceSeconds")) * time.Second)

			case 4: // driftwatch does its work
				_, err := h.swp.SweepOnce(ctx)
				require.NoError(t, err)
				h.swp.ConfirmDue(ctx, h.clk.Now())
			}

			// The property, checked after every single step rather than at the
			// end, so a shrunk counterexample points at the step that broke it.
			for key, episode := range h.swp.Episodes() {
				require.False(t, episode.FirstSeenAt.IsZero(),
					"key %q was confirmed without a first disagreeing read", key)
				require.False(t, episode.ConfirmedAt.IsZero(),
					"key %q was confirmed without a confirming read", key)

				gap := episode.ConfirmedAt.Sub(episode.FirstSeenAt)
				require.GreaterOrEqual(t, gap, episode.Window,
					"key %q was confirmed %s after it was first seen, which is less "+
						"than the %s window it was judged against",
					key, gap, episode.Window)
			}
		}
	})
}

// TestProp_NoKeyIsEverBothInFlightAndReported is invariant I11.
//
// A key inside its settlement window has not been given the grace period the
// window exists to grant, so reporting it is by definition a false positive.
// The two states are meant to be mutually exclusive, and the property says so
// directly rather than trusting that every code path that reports a finding
// remembered to check.
func TestProp_NoKeyIsEverBothInFlightAndReported(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		w := time.Duration(rapid.IntRange(1, 10).Draw(t, "windowSeconds")) * time.Second
		h := newPropHarness(t, w)

		ctx := context.Background()
		steps := rapid.IntRange(1, 60).Draw(t, "steps")

		for i := 0; i < steps; i++ {
			switch rapid.IntRange(0, 3).Draw(t, "action") {
			case 0:
				key := rapid.SampledFrom(propKeys).Draw(t, "eventKey")
				h.apply(key, rapid.SampledFrom([]string{"x", "y"}).Draw(t, "eventValue"))
			case 1:
				key := rapid.SampledFrom(propKeys).Draw(t, "writeKey")
				h.materialize(key, rapid.SampledFrom([]string{"x", "y"}).Draw(t, "writeValue"))
			case 2:
				h.clk.Advance(time.Duration(rapid.IntRange(1, 20).Draw(t, "advanceSeconds")) * time.Second)
			case 3:
				rep, err := h.swp.SweepOnce(ctx)
				require.NoError(t, err)
				h.swp.ConfirmDue(ctx, h.clk.Now())

				// A finding this sweep produced must not be for a key the same
				// sweep counted as in flight. The report's own timestamp is the
				// one to judge against: the clock may have moved since.
				for _, f := range rep.Findings {
					entry, ok := h.orc.Get(f.Key)
					require.True(t, ok)
					require.False(t, inFlight(&entry, rep.StartedAt, rep.SettlementWindow),
						"key %q was reported by a sweep that also counted it in flight "+
							"(last event %s, sweep at %s, window %s)",
						f.Key, entry.LastEventAt, rep.StartedAt, rep.SettlementWindow)
				}
			}

			// And a confirmed key must not be in flight at any moment, not just
			// at the moment it was reported.
			for key := range h.swp.Confirmed() {
				entry, ok := h.orc.Get(key)
				if !ok {
					continue
				}
				require.False(t, inFlight(&entry, h.clk.Now(), w),
					"key %q is confirmed divergent while inside its settlement window", key)
			}
		}
	})
}

// inFlight reports whether a key's most recent event is still inside W.
//
// It deliberately re-implements the rule from §5.3 rather than calling into the
// oracle: a property test that asks the implementation whether it agrees with
// itself proves nothing.
func inFlight(e *oracle.Entry, now time.Time, w time.Duration) bool {
	if e.LastEventAt.IsZero() {
		return false
	}
	if !e.LastEventAt.After(now.Add(-w)) {
		return false
	}
	// The stability-window rescue: a key whose value has not moved for the
	// never-settled threshold is comparable however recently an event arrived,
	// because idempotent repeats give the materializer no new work (§5.3).
	if !e.LastValueChangeAt.IsZero() && !e.LastValueChangeAt.After(now.Add(-10*w)) {
		return false
	}
	return true
}

// TestProp_SweepIsReadOnlyUnderAnyInterleaving is invariant I13 as a property.
//
// The RecordingTarget already fails any test in which driftwatch issues a
// write, so every test in this package asserts it. This one asserts it against
// inputs nobody chose.
func TestProp_SweepIsReadOnlyUnderAnyInterleaving(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		h := newPropHarness(t, 5*time.Second)
		ctx := context.Background()

		steps := rapid.IntRange(1, 40).Draw(t, "steps")
		for i := 0; i < steps; i++ {
			key := rapid.SampledFrom(propKeys).Draw(t, "key")
			switch rapid.IntRange(0, 3).Draw(t, "action") {
			case 0:
				h.apply(key, "v"+strconv.Itoa(i))
			case 1:
				h.materialize(key, "v"+strconv.Itoa(i))
			case 2:
				h.clk.Advance(time.Duration(rapid.IntRange(1, 30).Draw(t, "seconds")) * time.Second)
			case 3:
				_, err := h.swp.SweepOnce(ctx)
				require.NoError(t, err)
				h.swp.ConfirmDue(ctx, h.clk.Now())
				_, err = h.swp.ScanExtrasOnce(ctx)
				require.NoError(t, err)
			}
		}

		require.Empty(t, h.rec.Violations(),
			"driftwatch issued a mutating command against the store it audits")
	})
}
