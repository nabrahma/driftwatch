package sweeper_test

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/sweeper"
	"github.com/nabrahma/driftwatch/pkg/target"
)

func TestSweeper_ConfirmationIsSuspendedWhileTheTargetIsUnreachable(t *testing.T) {
	// The easiest place to forget §23 A5. The sweep checks health up front, so
	// the check is obvious there; the confirm loop is a second, separate read
	// path, and a failed read here would confirm every waiting candidate at
	// once — turning an outage into a wall of drift reports.
	h := newHarness(t)

	h.materialize("k", "wrong")
	h.apply("k", "right")
	h.settle()
	h.sweep()

	h.setHealth(func(hl *target.Health) { hl.Reachable = false })
	h.advance(window)

	assert.Zero(t, h.confirmDue(), "nothing is decided while the store is silent")
	assert.Empty(t, h.swp.Confirmed())
	assert.Equal(t, 1, h.swp.PendingConfirmations(),
		"the candidate is kept, not dropped: its wait has already been served")

	// The store comes back. The candidate is decided on the next cycle without
	// having to wait another window.
	h.setHealth(func(hl *target.Health) { hl.Reachable = true })

	require.Equal(t, 1, h.confirmDue())
	assert.Len(t, h.swp.Confirmed(), 1)
}

// failingGetTarget answers health normally and fails single-key reads on
// demand. A store that is up but cannot serve one read is the case a health
// check alone does not catch.
type failingGetTarget struct {
	target.Target
	fail atomic.Bool
}

func (f *failingGetTarget) Get(
	ctx context.Context,
	key string,
	shape projection.Shape,
) (event.Value, error) {
	if f.fail.Load() {
		return event.Value{}, errors.New("connection reset by peer")
	}
	return f.Target.Get(ctx, key, shape)
}

func TestSweeper_AFailedConfirmingReadLeavesTheCandidateOpen(t *testing.T) {
	// A store that answers health but fails the read is the nastier version of
	// the same problem: neither confirming nor resolving is correct, because
	// driftwatch did not find out.
	var flaky failingGetTarget
	h := newHarness(t, func(c *harnessConfig) {
		c.wrap = func(inner target.Target) target.Target {
			flaky.Target = inner
			return &flaky
		}
	})

	h.materialize("k", "wrong")
	h.apply("k", "right")
	h.settle()
	h.sweep()
	h.advance(window)

	flaky.fail.Store(true)

	require.Equal(t, 1, h.confirmDue())

	assert.Empty(t, h.swp.Confirmed(), "a read that failed is not a confirmation")
	assert.Zero(t, h.swp.Stats().TransientResolved, "and it is not a resolution either")
	assert.Equal(t, int64(1), h.swp.Stats().ConfirmReadFailed)
	assert.Contains(t, h.swp.Requeued(), "k")
}

func TestSweeper_ASecondSweepDoesNotRestartAWaitingCandidatesClock(t *testing.T) {
	// A key swept more often than W would never be confirmed at all if each
	// sweep replaced the pending candidate, because its wait would restart
	// before it ever elapsed. Drift on a frequently-swept key would be
	// invisible, which is the opposite of the intended behavior.
	h := newHarness(t)

	h.materialize("k", "wrong")
	h.apply("k", "right")
	h.settle()

	h.sweep()
	require.Equal(t, int64(1), h.swp.Stats().CandidatesEnqueued)

	// Two more sweeps inside the window, each of which sees the disagreement.
	h.advance(window / 3)
	h.sweep()
	h.advance(window / 3)
	h.sweep()

	assert.Equal(t, int64(1), h.swp.Stats().CandidatesEnqueued,
		"the candidate is not re-raised while one is already pending")
	assert.Equal(t, 1, h.swp.PendingConfirmations())

	h.advance(window/3 + time.Second)
	require.Equal(t, 1, h.confirmDue())
	assert.Len(t, h.swp.Confirmed(), 1)
}

func TestSweeper_ConfirmedDriftThatPersistsKeepsItsOriginalStartTime(t *testing.T) {
	// driftwatch_drift_duration_seconds has to measure the episode. If every
	// sweep reset the start, a drift that lasted a week would report as having
	// lasted one sweep interval, and the number would be worse than useless
	// because it would look plausible.
	h := newHarness(t)

	h.materialize("k", "wrong")
	h.apply("k", "right")
	h.settle()

	h.sweep()
	h.advance(window)
	require.Equal(t, 1, h.confirmDue())

	first := h.swp.Episodes()["k"].FirstSeenAt

	for i := 0; i < 3; i++ {
		h.advance(time.Hour)
		rep := h.sweep()
		require.Len(t, rep.Findings, 1)
		assert.True(t, rep.Findings[0].Confirmed)
	}

	assert.Equal(t, first, h.swp.Episodes()["k"].FirstSeenAt)
	assert.Equal(t, first, h.swp.Confirmed()["k"].FirstSeenAt)

	// And the duration is measured from there when it finally clears.
	h.materialize("k", "right")
	h.advance(time.Minute)
	h.sweep()

	assert.Equal(t, h.clk.Now().Sub(first), h.swp.Stats().LastDriftDuration)
}

func TestSweeper_ANewEventWithdrawsAConfirmedFinding(t *testing.T) {
	// Found by TestProp_NoKeyIsEverBothInFlightAndReported, which is why the
	// property test exists.
	//
	// A finding is a statement about one specific expectation: at version 7 the
	// target should have held x and held y. A new event replaces that
	// expectation, and whether the target is wrong about the new value is a
	// question driftwatch has not asked yet. Keeping the old answer would leave
	// the key simultaneously in flight and reported as divergent, which is the
	// exact false positive the settlement window exists to prevent.
	h := newHarness(t)

	h.materialize("k", "wrong")
	h.apply("k", "right")
	h.settle()
	h.sweep()
	h.advance(window)
	require.Equal(t, 1, h.confirmDue())
	require.Len(t, h.swp.Confirmed(), 1)

	// One event, and the claim is withdrawn immediately — not at the next
	// sweep, because nothing tells the sweeper an event arrived.
	h.apply("k", "newer")

	assert.Empty(t, h.swp.Confirmed(),
		"the finding is about a version the oracle has moved past")
	assert.Equal(t, int64(1), h.swp.Stats().ConfirmedSuperseded)
	assert.Zero(t, h.swp.Stats().DriftResolved,
		"withdrawing a claim is not the same as the target being repaired")

	// And the key is re-established on its own terms once it settles again.
	h.settle()
	h.sweep()
	h.advance(window)
	require.Equal(t, 1, h.confirmDue())

	confirmed := h.swp.Confirmed()
	require.Len(t, confirmed, 1)
	assert.Equal(t, []byte("newer"), confirmed["k"].OracleValue.Scalar)
}

func TestSweeper_ADroppedCandidateIsNotSilentlyForgotten(t *testing.T) {
	// Row 49 of the fault matrix in miniature: the queue overflows, but the
	// findings already confirmed are retained and the drop is counted.
	h := newHarness(t, func(c *harnessConfig) { c.sweeper.MaxConfirmQueue = 2 })

	for i := 0; i < 6; i++ {
		key := "k" + strconv.Itoa(i)
		h.materialize(key, "wrong")
		h.apply(key, "right")
	}
	h.settle()
	h.sweep()

	require.Equal(t, int64(2), h.swp.Stats().CandidatesEnqueued)
	require.Equal(t, int64(4), h.swp.Stats().ConfirmQueueDropped)

	h.advance(window)
	require.Equal(t, 2, h.confirmDue())
	require.Len(t, h.swp.Confirmed(), 2)

	// The next sweep re-raises the keys that were dropped, so overflow delays
	// confirmation rather than losing it.
	h.advance(time.Minute)
	h.sweep()
	assert.Equal(t, int64(4), h.swp.Stats().CandidatesEnqueued)
	assert.Len(t, h.swp.Confirmed(), 2, "the already-confirmed findings are retained")
}

func TestSweeper_ACancelledConfirmCycleLosesNothing(t *testing.T) {
	h := newHarness(t)

	for i := 0; i < 5; i++ {
		key := "k" + strconv.Itoa(i)
		h.materialize(key, "wrong")
		h.apply(key, "right")
	}
	h.settle()
	h.sweep()
	h.advance(window)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.Zero(t, h.swp.ConfirmDue(ctx, h.clk.Now()))
	assert.Equal(t, 5, h.swp.PendingConfirmations(),
		"a canceled cycle puts its candidates back with their timing intact")

	require.Equal(t, 5, h.confirmDue())
	assert.Len(t, h.swp.Confirmed(), 5)
}

func TestSweeper_ClosedSweeperStopsWorkingRatherThanPanicking(t *testing.T) {
	h := newHarness(t)
	findings := h.swp.Subscribe()

	require.NoError(t, h.swp.Close())
	require.NoError(t, h.swp.Close(), "Close is idempotent (fault matrix row 57)")

	_, err := h.swp.SweepOnce(context.Background())
	require.ErrorIs(t, err, sweeper.ErrClosed)
	assert.Zero(t, h.swp.ConfirmDue(context.Background(), h.clk.Now()))

	_, open := <-findings
	assert.False(t, open, "subscribers are closed, not left hanging")
}
