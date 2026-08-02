package sweeper_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/differ"
)

// Run is the loop that drives the sweeper in production, and everything else in
// this package tests the operations it calls rather than the loop that calls
// them. That distinction matters more than it sounds: each of the three tickers
// runs at a different interval on purpose, and wiring any of them to the wrong
// operation produces a sweeper that looks healthy — sweeps happen, findings
// appear — while quietly never doing one of its three jobs.
//
// A fake clock makes the loop deterministic: advancing time fires a specific
// ticker, so each test asserts that one interval drives one operation.

// runningSweeper starts Run and returns a stop function that asserts the loop
// exited for the reason it was asked to.
func runningSweeper(t *testing.T, h *harness) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.swp.Run(ctx) }()

	// Run creates three tickers. Advancing before they are registered fires
	// nothing, which the clock's own documentation warns about — and which
	// presents as all three ticker tests timing out at once rather than as a
	// race, because there is nothing intermittent about losing every tick.
	h.clk.BlockUntil(3)

	return func() {
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled,
				"Run should return the context's error and nothing else")
		case <-time.After(30 * time.Second):
			t.Fatal("Run did not return after its context was canceled")
		}
	}
}

func TestRun_TheSweepTickerDrivesSweepsAndReportsEachOne(t *testing.T) {
	var mu sync.Mutex
	var reports []*differ.Report

	h := newHarness(t, func(hc *harnessConfig) {
		hc.sweeper.SweepInterval = 10 * time.Second
		hc.sweeper.OnReport = func(rep *differ.Report, err error) {
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				reports = append(reports, rep)
			}
		}
	})

	h.apply("k1", "v1")
	h.materialize("k1", "v1")
	h.settle()

	stop := runningSweeper(t, h)
	defer stop()

	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(reports)
	}

	for want := 1; want <= 3; want++ {
		h.advance(10 * time.Second)
		require.Eventually(t, func() bool { return count() >= want },
			30*time.Second, 10*time.Millisecond,
			"the sweep ticker fired %d times and produced %d reports", want, count())
	}

	mu.Lock()
	defer mu.Unlock()
	for i, rep := range reports {
		assert.Equal(t, differ.PassOracleToTarget, rep.Pass,
			"report %d came from the sweep ticker, so it is the oracle→target "+
				"pass; a target→oracle report here means the extras ticker is "+
				"wired to the wrong operation", i)
	}
}

func TestRun_TheExtrasTickerDrivesTheOtherHalfOfTheComparison(t *testing.T) {
	// §5.5 has two passes and they are not symmetric. oracle→target finds keys
	// the materializer failed to write; target→oracle finds keys it wrote that
	// no event ever justified. The second runs on its own, much slower ticker
	// because scanning the keyspace is the expensive half.
	//
	// Wiring both tickers to the same pass is a mistake that hides completely:
	// sweeps keep happening, findings keep appearing, and extras are simply
	// never looked for.
	var mu sync.Mutex
	passes := map[differ.Pass]int{}

	h := newHarness(t, func(hc *harnessConfig) {
		hc.sweeper.SweepInterval = 1 * time.Hour // long enough not to interfere
		hc.sweeper.ExtraScanInterval = 10 * time.Second
		hc.sweeper.OnReport = func(rep *differ.Report, err error) {
			if err != nil || rep == nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			passes[rep.Pass]++
		}
	})

	// A key in the store that the oracle has never heard of: exactly what the
	// extras pass exists to find.
	h.materialize("orphan", "written-by-nobody")
	h.settle()

	stop := runningSweeper(t, h)
	defer stop()

	h.advance(10 * time.Second)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return passes[differ.PassTargetToOracle] > 0
	}, 30*time.Second, 10*time.Millisecond,
		"the extras ticker fired and no target→oracle pass ran")

	mu.Lock()
	defer mu.Unlock()
	assert.Zero(t, passes[differ.PassOracleToTarget],
		"the sweep interval is an hour, so no oracle→target pass should have "+
			"run; one here means the extras ticker is driving the wrong pass")
}

func TestRun_TheConfirmTickerPromotesCandidatesWithoutASweep(t *testing.T) {
	// The confirmation cycle is what turns a candidate into a finding, and it
	// runs on its own ticker precisely so that it does not have to wait for the
	// next sweep. If it were folded into the sweep, the time to confirm would
	// be a settlement window *plus* up to a sweep interval — which on the
	// default 30s interval more than doubles it, and would make the convergence
	// histogram measure the sweeper's schedule rather than the store's latency.
	h := newHarness(t, func(hc *harnessConfig) {
		hc.sweeper.SweepInterval = 1 * time.Hour
		// Longer than W on purpose, so that a single Advance produces exactly
		// one tick and that tick is already past the candidate's deadline.
		//
		// A 1s interval does not work here, and the reason is worth stating: a
		// fake tick carries its own scheduled time, and an undrained tick is
		// dropped rather than queued — the same behavior as time.Ticker. One
		// Advance of W+2s therefore fires the 1s ticker seven times, delivers
		// whichever few the Run goroutine happened to be ready for, and drops
		// the rest. The candidate stays pending behind a tick that never
		// arrives, and the test fails for a reason that has nothing to do with
		// the confirmation logic it is meant to be checking.
		hc.sweeper.ConfirmInterval = window + 2*time.Second
	})

	h.apply("missing", "v1")
	h.settle()

	// One sweep by hand raises the candidate. Nothing after this point calls a
	// sweep again, so anything that happens is the confirm ticker's doing.
	rep := h.sweep()
	require.Equal(t, 1, rep.Total(), "the missing key should have been noticed")
	require.Empty(t, h.swp.Confirmed(),
		"one disagreeing read is a candidate, not a finding")
	require.Equal(t, 1, h.swp.PendingConfirmations())

	stop := runningSweeper(t, h)
	defer stop()

	h.advance(window + 2*time.Second)

	require.Eventually(t, func() bool { return len(h.swp.Confirmed()) == 1 },
		30*time.Second, 10*time.Millisecond,
		"the confirm ticker never promoted the candidate; pending=%d",
		h.swp.PendingConfirmations())

	finding := h.swp.Confirmed()["missing"]
	assert.Equal(t, differ.CatMissingInTarget, finding.Category)
	assert.True(t, finding.Confirmed)
}

func TestRun_ASweepThatOverrunsItsIntervalIsSkippedRatherThanQueued(t *testing.T) {
	// §9 M10. A sweeper that cannot keep up must not stack sweeps behind each
	// other: that turns a slow sweeper into an unbounded queue, and the symptom
	// — memory growth in the process auditing someone else's memory growth — is
	// about the worst one to be handed.
	//
	// Skipping is only defensible because it is counted. An operator watching
	// sweeps_skipped climb knows the interval is too short for the keyspace;
	// without the counter the sweeper would quietly run less often than it was
	// configured to and nothing would say so.
	h := newHarness(t)

	h.apply("k1", "v1")
	h.materialize("k1", "v1")
	h.settle()

	// The first sweep parks inside its first read until the test releases it,
	// which is what "a sweep in flight" means here. ObserveCommands runs before
	// the operation, so blocking in it holds the sweep open.
	release := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	h.mem.ObserveCommands(func(string) {
		once.Do(func() {
			close(started)
			<-release
		})
	})

	first := make(chan bool, 1)
	go func() { first <- h.swp.TrySweepOnce(context.Background()) }()

	select {
	case <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("the first sweep never reached the target")
	}

	assert.False(t, h.swp.TrySweepOnce(context.Background()),
		"a sweep should be skipped while another is in flight, not queued")
	assert.Equal(t, int64(1), h.swp.Stats().SweepsSkipped,
		"the skip has to be counted, or the sweeper silently runs less often "+
			"than it was configured to")

	close(release)
	assert.True(t, <-first, "the first sweep should have completed")
}

func TestRun_ReturnsTheContextErrorAndStopsItsTickers(t *testing.T) {
	// goleak in TestMain is what actually proves the tickers stopped: a Run
	// that returned while leaving three tickers behind fails the package, not
	// this test. What this asserts is the contract callers depend on — Run
	// returns context.Canceled rather than nil, so a caller that treats a nil
	// return as "the sweeper finished its work" cannot be written by accident.
	h := newHarness(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.swp.Run(ctx) }()

	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled),
			"Run returned %v, want context.Canceled", err)
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}
