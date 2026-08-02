package check_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// What the check reports about itself, and what it must not report.
//
// The status object and the metrics are the whole product from an operator's
// side: a correct oracle whose status says `coverageRatio: 0` is
// indistinguishable from a broken one.

func TestReporting_TheExtrasScanDoesNotClobberTheSweepsStatus(t *testing.T) {
	// D-020, both halves.
	//
	// §5.5's two passes come through the same OnReport callback, and they are
	// not interchangeable. The oracle→target sweep compares oracle keys and
	// knows what fraction of the expectation it verified. The target→oracle
	// scan walks the *store*, so its KeysCompared counts store keys visited —
	// a number that means something completely different.
	//
	// Letting the extras report land in lastReport made `kubectl get
	// driftcheck` show coverageRatio: 0 and targetKeyspaceSize: 0 every
	// extraScanInterval, on a check that was tracking its keyspace perfectly
	// well. The metrics half was found first, on the demo dashboard, where the
	// coverage gauge dropped to zero and came back. The status half survived
	// that fix and was found later by e2e E1, which asserts coverage above 0.90
	// and got exactly zero.
	//
	// The two are worth one test because fixing either alone is worse than
	// fixing neither: the dashboard and the CRD would disagree, and the CRD is
	// what an operator reads first.
	clk := clock.Fake(epoch())
	c := newCheckWith(t, inProcessSpec, clk)
	stop := running(t, c)
	defer stop()

	store, ok := c.Target().(*target.MemoryTarget)
	require.True(t, ok)

	const keys = 6
	for i := 0; i < keys; i++ {
		publish(t, c, addEventJSON("replica-0", uint64(i+1), keyIdx(i), "replica-0"))
		store.SeedSets(map[string][]string{"block:" + keyIdx(i): {"replica-0"}})
	}

	clk.Advance(5 * time.Second)

	ctx := context.Background()
	rep, err := c.SweepNow(ctx)
	require.NoError(t, err)
	require.Equal(t, keys, rep.KeysCompared)

	before := c.Status()
	require.InDelta(t, 1.0, before.CoverageRatio, 0.001,
		"the sweep compared every tracked key")

	// Now the other pass. It walks the store, finds nothing unexplained, and
	// must leave every gauge the sweep owns exactly where it was.
	_, err = c.ScanExtras(ctx)
	require.NoError(t, err)

	after := c.Status()
	assert.InDelta(t, before.CoverageRatio, after.CoverageRatio, 0.001,
		"the extras scan has nothing to say about coverage and must not "+
			"recompute it; it walked the store, not the oracle (D-020)")
	assert.Equal(t, before.LastSweepKeysCompared, after.LastSweepKeysCompared,
		"lastSweepKeysCompared is the sweep's number, not the scan's")
	assert.Equal(t, before.LastSweepTime, after.LastSweepTime,
		"an extras scan is not a sweep and must not advance lastSweepTime — "+
			"an operator reading it would conclude the sweep is healthy when "+
			"it may have stopped entirely")
}

func TestReporting_AnUnreachableTargetDegradesRatherThanReportingDrift(t *testing.T) {
	// §23 A5, in the status. Absence of data is not evidence of divergence, and
	// the phase has to say which of the two happened. A check that went to
	// Failed would page someone about driftwatch; a check that stayed Watching
	// would claim a clean bill of health it has no basis for.
	clk := clock.Fake(epoch())
	c := newCheckWith(t, inProcessSpec, clk)
	stop := running(t, c)
	defer stop()

	publish(t, c, addEventJSON("replica-0", 1, "0", "replica-0"))
	clk.Advance(5 * time.Second)

	ctx := context.Background()
	_, err := c.SweepNow(ctx)
	require.NoError(t, err)
	require.Equal(t, check.PhaseWatching, c.Status().Phase)

	// The store goes away.
	store, ok := c.Target().(*target.MemoryTarget)
	require.True(t, ok)
	health, err := store.Health(ctx)
	require.NoError(t, err)
	health.Reachable = false
	store.SetHealth(health)

	_, err = c.SweepNow(ctx)
	require.Error(t, err, "a sweep against an unreachable store must fail")

	status := c.Status()
	assert.Equal(t, check.PhaseDegraded, status.Phase,
		"an unreachable target is a degraded check, not a failed one and not "+
			"a healthy one")
	assert.False(t, status.TargetReachable)
	assert.Zero(t, status.DivergentKeys,
		"nothing was compared, so nothing may be reported as divergent")

	// And it recovers on its own when the store comes back, without needing a
	// restart. A degraded phase that is sticky is a phase an operator learns to
	// ignore.
	health.Reachable = true
	store.SetHealth(health)

	_, err = c.SweepNow(ctx)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return c.Status().Phase == check.PhaseWatching },
		eventuallyFor, eventuallyPoll,
		"the check should return to Watching once the store answers again; "+
			"phase is %s", c.Status().Phase)
}

func TestReporting_CoverageIsOneWhenThereIsNothingToCover(t *testing.T) {
	// The degenerate case, and it has to be 1 rather than 0. A check whose
	// oracle is empty has verified everything it claims to know about —
	// vacuously, but correctly — and reporting 0 would fire the low-coverage
	// alert on every check that has not seen its first event yet.
	clk := clock.Fake(epoch())
	c := newCheckWith(t, inProcessSpec, clk)
	stop := running(t, c)
	defer stop()

	clk.Advance(5 * time.Second)
	rep, err := c.SweepNow(context.Background())
	require.NoError(t, err)
	require.Zero(t, rep.KeysCompared)

	assert.InDelta(t, 1.0, c.Status().CoverageRatio, 0.001,
		"an empty oracle has covered everything it knows about; reporting 0 "+
			"would alert on every check that has not seen an event yet")
}

// keyIdx names block i.
func keyIdx(i int) string { return string(rune('a' + i)) }
