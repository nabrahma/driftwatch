package check_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/metrics"
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

func TestReporting_AFailedSweepDoesNotErasePreviousCoverage(t *testing.T) {
	// D-020's third instance, and the one that survived the first two fixes.
	//
	// A sweep that fails part-way still produces a report, and that report has
	// compared however many keys it reached before stopping — usually none.
	// Letting it land in lastReport replaces a real measurement with a
	// worthless one, so coverageRatio collapses to zero every time the store
	// blinks, on a check that verified its whole keyspace a second earlier.
	//
	// Zero does not mean "could not check just now". It means "verified
	// nothing", and it is the number the low-coverage alert fires on.
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
	_, err := c.SweepNow(ctx)
	require.NoError(t, err)

	before := c.Status()
	require.InDelta(t, 1.0, before.CoverageRatio, 0.001)

	// The store blinks.
	health, err := store.Health(ctx)
	require.NoError(t, err)
	health.Reachable = false
	store.SetHealth(health)

	_, err = c.SweepNow(ctx)
	require.Error(t, err)

	after := c.Status()
	assert.InDelta(t, before.CoverageRatio, after.CoverageRatio, 0.001,
		"a sweep that could not read the store must not overwrite what the "+
			"last successful one measured; §6.4 says the reported counts stay "+
			"the last ones driftwatch actually knew")
	assert.Equal(t, before.LastSweepTime, after.LastSweepTime,
		"and lastSweepTime must not advance, or the stale coverage would look "+
			"fresh — that pairing is what makes keeping it honest")
	assert.False(t, after.TargetReachable,
		"the status has to say why the numbers are stale")
}

// extrasCadenceSpec puts the extras scan on an interval that is not a multiple
// of the sweep interval, so the clock can be advanced to land an extras report
// with no sweep behind it. On a shared cadence the sweep that followed would
// re-measure the target and clear the state under test, and the assertion below
// would pass whether or not the defect was there.
const extrasCadenceSpec = `
name: kvcache-index
namespace: inference
source:
  type: memory
codec:
  type: json
projection:
  type: keysetOwnership
  keyTemplate: "block:{{.Key}}"
target:
  type: memory
policy:
  settlementWindow: {mode: static, static: 2s}
  sweepInterval: 10s
  extraScanInterval: 25s
  bootstrap: Wait
`

func TestReporting_AnExtrasScanDoesNotClaimTheTargetIsUnreachable(t *testing.T) {
	// D-020's fourth instance, in the one place the previous three fixes left.
	//
	// Both passes come through the same callback, and only the oracle→target
	// one measures target health. TargetHealth is a struct and the extras scan
	// never fills it in, so what reached the status and the gauge was the zero
	// value: Reachable false, from a pass that had not looked.
	//
	// The effect is a check that reports TargetAvailable=False and goes
	// Degraded every extraScanInterval and recovers on the next sweep, forever,
	// against a store that never stopped answering. Every twenty seconds under
	// the e2e policy; every five minutes under the shipped default. It reads
	// exactly like a flapping Redis, which is where the debugging time goes —
	// e2e E1 spent a run on it before the periodicity gave it away.
	//
	// The status half and the metric half are asserted together because fixing
	// one alone leaves the CRD and the dashboard disagreeing, which is worse
	// than either being wrong on its own.
	clk := clock.Fake(epoch())
	reg := prometheus.NewRegistry()

	parsed, err := check.Load(strings.NewReader(extrasCadenceSpec))
	require.NoError(t, err)

	c, err := check.New(parsed, check.Deps{
		Clock:   clk,
		Metrics: metrics.New(metrics.Options{Registry: reg}),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })

	stop := running(t, c)
	defer stop()

	store, ok := c.Target().(*target.MemoryTarget)
	require.True(t, ok)

	const keys = 4
	for i := 0; i < keys; i++ {
		publish(t, c, addEventJSON("replica-0", uint64(i+1), keyIdx(i), "replica-0"))
		store.SeedSets(map[string][]string{"block:" + keyIdx(i): {"replica-0"}})
	}
	clk.Advance(5 * time.Second)

	_, err = c.SweepNow(context.Background())
	require.NoError(t, err)
	require.True(t, c.Status().TargetReachable,
		"the store is up, so the sweep should have said so")
	require.InDelta(t, 1, gaugeValue(t, reg, "driftwatch_target_reachable"), 0.001)

	// One tick per Advance, each one drained before the next is allowed to
	// fire, and the arithmetic is written out because getting it wrong is
	// silent.
	//
	// Run's tickers are at t=10,20,30... for sweeps and t=25,50... for extras.
	// The clock is at t=5. Crossing two boundaries in a single Advance drops
	// one of the ticks — a fake ticker models time.Ticker, whose channel holds
	// one tick and discards the rest rather than queueing them — so a test that
	// jumped straight to t=20 saw one sweep or two depending on how quickly the
	// Run goroutine got back to its select. That is a flake, and it is the same
	// mistake this whole file is about: reading a number without checking which
	// pass produced it.
	//
	// It also matters that the extras tick lands last and alone. A sweep
	// arriving after it would re-measure the target and clear the state under
	// test, and the assertion would pass whether or not the defect was there.
	advanceOneTick := func(by time.Duration, metric, kind string, want int) {
		t.Helper()
		clk.Advance(by)
		require.Eventually(t, func() bool {
			return labelledValue(t, reg, metric, "kind", kind) >= want
		}, eventuallyFor, eventuallyPoll,
			"the %s tick never ran; %s reached %d, wanted %d",
			kind, metric, labelledValue(t, reg, metric, "kind", kind), want)
	}

	// t=5 -> t=10, the first sweep tick. Two sweeps now: this and SweepNow.
	advanceOneTick(5*time.Second, "driftwatch_sweeps_total", "oracle_to_target", 2)
	// t=10 -> t=20, the second.
	advanceOneTick(10*time.Second, "driftwatch_sweeps_total", "oracle_to_target", 3)

	// t=20 -> t=25, the extras tick, alone.
	advanceOneTick(5*time.Second, "driftwatch_sweeps_total", "target_to_oracle", 1)

	assert.True(t, c.Status().TargetReachable,
		"the extras scan does not measure reachability, so it must not report "+
			"on it; the store answered every request throughout and the only "+
			"thing that changed was which pass reported last")
	assert.InDelta(t, 1, gaugeValue(t, reg, "driftwatch_target_reachable"), 0.001,
		"and the gauge an alert fires on must not flap on the extras interval")
	assert.NotEqual(t, check.PhaseDegraded, c.Status().Phase,
		"a check whose store is up must not go Degraded on a timer")
}
