package sweeper_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An episode is a span, not an instant.
//
// A key that disagrees for forty minutes is one incident. Re-confirming it
// every sweep and restarting its clock each time turns that into eighty
// two-second incidents, and driftwatch's answer to "how long was this wrong
// for" becomes the sweep interval — a number about driftwatch's own schedule
// rather than about the store.

func TestEpisode_ReconfirmationKeepsTheOriginalStart(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.apply("stubborn", "v1")
	h.settle()

	// First confirmation.
	require.Equal(t, 1, h.sweep().Total())
	h.advance(window + time.Second)
	require.Positive(t, h.swp.ConfirmDue(ctx, h.clk.Now()))

	first := h.swp.Episodes()["stubborn"]
	require.False(t, first.FirstSeenAt.IsZero(), "the episode should be open")

	// Several more sweeps, each finding the same key still wrong.
	for i := 0; i < 3; i++ {
		h.advance(window + time.Second)
		require.Equal(t, 1, h.sweep().Total())
		h.advance(window + time.Second)
		h.swp.ConfirmDue(ctx, h.clk.Now())
	}

	later := h.swp.Episodes()["stubborn"]
	assert.Equal(t, first.FirstSeenAt, later.FirstSeenAt,
		"the episode's start must survive re-confirmation; restarting it each "+
			"sweep makes the reported drift duration the sweep interval rather "+
			"than the outage")
	assert.Equal(t, first.FirstSeenAt, later.Finding.FirstSeenAt,
		"the finding carries the same start as the episode it belongs to")
	assert.Len(t, h.swp.Confirmed(), 1,
		"one key disagreeing repeatedly is one confirmed finding, not four")
}

func TestEpisode_RepairClosesTheEpisodeAndRecordsItsDuration(t *testing.T) {
	// The other end of the span. Repairing the key must close the episode,
	// count a resolution, and record how long it lasted — the number that
	// answers "was this a blip or an hour" without anyone reading a log.
	h := newHarness(t)
	ctx := context.Background()

	h.apply("repaired", "v1")
	h.settle()

	require.Equal(t, 1, h.sweep().Total())
	h.advance(window + time.Second)
	require.Positive(t, h.swp.ConfirmDue(ctx, h.clk.Now()))
	require.Len(t, h.swp.Confirmed(), 1)

	// Time passes with the key still wrong, so the duration is not trivially
	// zero and a bug that reported the wrong end of the span would show.
	h.advance(2 * time.Minute)

	// The materializer catches up.
	h.materialize("repaired", "v1")
	h.advance(window + time.Second)

	rep := h.sweep()
	assert.Zero(t, rep.Total(), "the key agrees now: %s", rep.Summary())
	assert.Empty(t, h.swp.Confirmed(),
		"a repaired key must stop being reported; a finding that outlives its "+
			"cause is how an operator learns to ignore the gauge")

	stats := h.swp.Stats()
	assert.Positive(t, stats.DriftResolved,
		"the repair should be counted, not merely reflected in a gauge going down")
	assert.GreaterOrEqual(t, stats.LastDriftDuration, 2*time.Minute,
		"the episode lasted at least two minutes and was reported as %s",
		stats.LastDriftDuration)
}

func TestEpisode_AConfirmedFindingIsWithdrawnWhenTheOracleMovesUnderIt(t *testing.T) {
	// D-009. A confirmed finding is a claim about one oracle version. If a new
	// event for that key arrives, the version the finding was raised against no
	// longer exists — the question is simply open again, and continuing to
	// report the old answer is reporting something that was never re-checked.
	//
	// Not a repair: the store may still be wrong. Counted separately for
	// exactly that reason.
	h := newHarness(t)
	ctx := context.Background()

	h.apply("superseded", "v1")
	h.settle()

	require.Equal(t, 1, h.sweep().Total())
	h.advance(window + time.Second)
	require.Positive(t, h.swp.ConfirmDue(ctx, h.clk.Now()))
	require.Len(t, h.swp.Confirmed(), 1)

	// A new event replaces the expectation the finding was raised against.
	h.apply("superseded", "v2")

	// Reading is what withdraws it, not the next sweep — and that is the
	// point. The invariant is that a confirmed finding always names a live
	// oracle version, and it has to hold at every instant rather than only in
	// the moment after a sweep. Anything asking "what is currently wrong"
	// between sweeps would otherwise get an answer about a version that no
	// longer exists.
	assert.Empty(t, h.swp.Confirmed(),
		"the finding was raised against version 1 and the oracle is now at "+
			"version 2, so there is nothing left to stand behind")

	assert.Positive(t, h.swp.Stats().ConfirmedSuperseded,
		"a finding whose oracle version moved must be withdrawn and counted "+
			"as superseded rather than as resolved — nothing was repaired")
}
