package sweeper_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// The target→oracle half of §5.5, which is two passes rather than one.
//
// A key in the store that no event explains is not automatically wrong. It may
// be a key the materializer wrote a moment ago whose event driftwatch has not
// folded yet, and reporting that as an extra is the same false positive the
// settlement window exists to prevent — just approached from the other side.
//
// So the first pass collects candidates and parks them; the second, a window
// later, reports only the ones still unexplained.

func TestExtras_AKeyExplainedBetweenThePassesIsNotReported(t *testing.T) {
	// The case the second pass exists for. The store holds a key the oracle has
	// not heard of when the scan runs; by the time the second pass looks, the
	// event has arrived and the oracle knows about it. Reporting it would be
	// reporting driftwatch's own lag as the store's fault.
	h := newHarness(t)

	h.materialize("explained-later", "v1")
	h.settle()

	ctx := context.Background()

	rep, err := h.swp.ScanExtrasOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, differ.PassTargetToOracle, rep.Pass)
	assert.Zero(t, rep.Total(),
		"the first pass parks candidates; reporting on one look is the whole "+
			"mistake this pass structure prevents: %s", rep.Summary())
	require.Equal(t, 1, h.swp.PendingExtras())

	// The event turns up before the second pass.
	h.apply("explained-later", "v1")
	h.advance(window + time.Second)

	rep, err = h.swp.ScanExtrasOnce(ctx)
	require.NoError(t, err)
	assert.Zero(t, rep.Total(),
		"the oracle now has an expectation for this key, so it is not an "+
			"extra: %s", rep.Summary())
	assert.Positive(t, h.swp.Stats().ExtrasSelfResolved,
		"a candidate that explained itself between the passes should be "+
			"counted as such, not silently forgotten")
}

func TestExtras_AKeyStillUnexplainedOnTheSecondPassIsReported(t *testing.T) {
	// The genuine finding: something wrote a key that no event in the stream
	// justifies. A materializer applying an event twice under two different
	// keys, a stale writer nobody remembered was still running, a manual
	// redis-cli that was never meant to reach production.
	h := newHarness(t)

	h.materialize("never-explained", "v1")
	h.settle()

	ctx := context.Background()

	_, err := h.swp.ScanExtrasOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, h.swp.PendingExtras())

	h.advance(window + time.Second)

	rep, err := h.swp.ScanExtrasOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, rep.Total(),
		"a key still unexplained a window later is a finding")
	assert.Equal(t, differ.CatExtraInTarget, rep.Findings[0].Category)
	assert.Equal(t, "never-explained", rep.Findings[0].Key)

	// The key is still an extra, so the same scan's first pass parks it again
	// for the next round. That is the steady state for a genuine extra: it is
	// reported every cycle until something explains it or removes it. What must
	// not happen is the parked set *growing* — one key parked, not two.
	assert.Equal(t, 1, h.swp.PendingExtras(),
		"a still-unexplained key is re-parked, not accumulated")
}

func TestExtras_AKeyDeletedBetweenThePassesIsNotReported(t *testing.T) {
	// The other way a candidate resolves itself: it stops existing. A key that
	// is gone by the second pass is not an extra, and reporting one would send
	// an operator looking for something that is not there.
	h := newHarness(t)

	h.materialize("deleted-later", "v1")
	h.settle()

	ctx := context.Background()
	_, err := h.swp.ScanExtrasOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, h.swp.PendingExtras())

	h.unmaterialize("deleted-later")
	h.advance(window + time.Second)

	rep, err := h.swp.ScanExtrasOnce(ctx)
	require.NoError(t, err)
	assert.Zero(t, rep.Total(),
		"a key that no longer exists cannot be an extra: %s", rep.Summary())
}

func TestExtras_AKeyTheOracleHasATombstoneForIsNotAnExtra(t *testing.T) {
	// A tombstone is an expectation, not an absence of one: the oracle is
	// saying "this key should not exist". The oracle→target sweep already
	// compares that and reports the disagreement, so reporting it here as well
	// would count one divergence twice under two different categories — and an
	// operator reconciling the two numbers would find they do not add up.
	h := newHarness(t)

	h.apply("tombstoned", "v1")
	h.applyDelete("tombstoned")
	h.materialize("tombstoned", "v1") // the store still holds it
	h.settle()

	ctx := context.Background()
	_, err := h.swp.ScanExtrasOnce(ctx)
	require.NoError(t, err)

	assert.Zero(t, h.swp.PendingExtras(),
		"a tombstoned key has an expectation, so the extras pass leaves it to "+
			"the oracle→target sweep")
}

func TestExtras_AnUnreadableKeyIsLeftToTheSweepRatherThanMiscategorised(t *testing.T) {
	// A key holding a shape this projection cannot read is present, so it
	// survives the presence half of the test — but calling it an extra puts the
	// wrong category on it. Type mismatch belongs to the oracle→target sweep,
	// where there is an expectation to mismatch against. Here there is none, so
	// the honest move is to leave it alone rather than to invent a verdict.
	h := newHarness(t, func(hc *harnessConfig) {
		hc.wrap = func(inner target.Target) target.Target {
			return &wrongTypeTarget{Target: inner, key: "wrong-shape"}
		}
	})

	h.materialize("wrong-shape", "v1")
	h.settle()

	ctx := context.Background()

	// The first pass only consults the oracle, so an unreadable key is parked
	// like any other candidate. The read — and the decision — happen on the
	// second pass.
	_, err := h.swp.ScanExtrasOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, h.swp.PendingExtras())

	h.advance(window + time.Second)

	rep, err := h.swp.ScanExtrasOnce(ctx)
	require.NoError(t, err)
	assert.Zero(t, rep.Total(),
		"a key whose shape this projection cannot read is present but has no "+
			"expectation to mismatch against, so the extras pass leaves it "+
			"alone rather than inventing a verdict: %s", rep.Summary())
}

func TestExtras_TheFirstPassStopsAtItsCap(t *testing.T) {
	// §19.2. The extras pass walks the whole keyspace, which is the one place
	// in the sweeper where the work is bounded by the store's size rather than
	// by the oracle's. The cap is what stops a store far larger than the oracle
	// from turning a scan into an allocation.
	const capacity = 10

	h := newHarness(t, func(hc *harnessConfig) {
		hc.sweeper.MaxExtrasTracked = capacity
	})

	for i := 0; i < capacity*5; i++ {
		h.materialize(keyN(i), "v1")
	}
	h.settle()

	_, err := h.swp.ScanExtrasOnce(context.Background())
	require.NoError(t, err)

	assert.LessOrEqual(t, h.swp.PendingExtras(), capacity,
		"the first pass must stop at its cap")
	assert.Positive(t, h.swp.Stats().ExtrasTruncated,
		"truncation has to be counted, or a scan that saw a fraction of the "+
			"keyspace looks the same as one that saw all of it")
}
