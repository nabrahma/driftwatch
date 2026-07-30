package clock_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/nabrahma/driftwatch/pkg/clock"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// recv returns the value pending on ch, or ok=false if none is pending.
// It never blocks, so no test needs a timeout or a sleep.
func recv(ch <-chan time.Time) (time.Time, bool) {
	select {
	case v := <-ch:
		return v, true
	default:
		return time.Time{}, false
	}
}

func TestReal_NowAndSinceAdvanceMonotonically(t *testing.T) {
	c := clock.Real()

	start := c.Now()
	require.False(t, start.IsZero(), "Now must return a real time")

	// Since is measured against a monotonic source, so it is never negative.
	assert.GreaterOrEqual(t, c.Since(start), time.Duration(0))
}

func TestReal_SleepReturnsWhenTheDurationElapses(t *testing.T) {
	c := clock.Real()

	// A microsecond is short enough not to slow the suite and long enough to
	// prove the code path. This is the one place a real duration is legitimate:
	// the subject under test is the real clock itself.
	require.NoError(t, c.Sleep(context.Background(), time.Microsecond))
}

func TestReal_SleepReturnsImmediatelyForNonPositiveDurations(t *testing.T) {
	c := clock.Real()

	require.NoError(t, c.Sleep(context.Background(), 0))
	require.NoError(t, c.Sleep(context.Background(), -time.Second))
}

func TestReal_SleepReturnsTheContextErrorWhenCancelled(t *testing.T) {
	c := clock.Real()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.ErrorIs(t, c.Sleep(ctx, time.Hour), context.Canceled)
}

func TestReal_SleepUnblocksWhenTheContextIsCancelledWhileWaiting(t *testing.T) {
	c := clock.Real()

	ctx, cancel := context.WithCancel(context.Background())

	errc := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		errc <- c.Sleep(ctx, time.Hour)
	}()

	<-started
	cancel()

	assert.ErrorIs(t, <-errc, context.Canceled)
}

func TestReal_TimerAndTickerAreDrivenByRealTime(t *testing.T) {
	c := clock.Real()

	timer := c.NewTimer(time.Microsecond)
	<-timer.C()
	assert.False(t, timer.Stop(), "Stop reports false for an already-fired timer")
	assert.False(t, timer.Reset(time.Hour), "Reset reports false for an inactive timer")
	assert.True(t, timer.Stop(), "Stop reports true after Reset rearmed the timer")

	ticker := c.NewTicker(time.Microsecond)
	defer ticker.Stop()
	<-ticker.C()
	ticker.Reset(time.Hour)
}

func TestFake_NowReportsTheStartTimeUntilAdvanced(t *testing.T) {
	c := clock.Fake(epoch)

	assert.Equal(t, epoch, c.Now())
	assert.Equal(t, time.Duration(0), c.Since(epoch))

	c.Advance(90 * time.Second)

	assert.Equal(t, epoch.Add(90*time.Second), c.Now())
	assert.Equal(t, 90*time.Second, c.Since(epoch))
}

func TestFake_AdvanceFiresTimersThatAreDue(t *testing.T) {
	tests := []struct {
		name      string
		timerFor  time.Duration
		advanceBy time.Duration
		wantFired bool
		wantAt    time.Duration // offset from epoch of the fired value
	}{
		{
			name:      "a timer does not fire before its deadline",
			timerFor:  10 * time.Second,
			advanceBy: 9 * time.Second,
			wantFired: false,
		},
		{
			name:      "a timer fires exactly at its deadline",
			timerFor:  10 * time.Second,
			advanceBy: 10 * time.Second,
			wantFired: true,
			wantAt:    10 * time.Second,
		},
		{
			name:      "a timer fires with its own deadline, not the advance target",
			timerFor:  10 * time.Second,
			advanceBy: time.Hour,
			wantFired: true,
			wantAt:    10 * time.Second,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := clock.Fake(epoch)
			timer := c.NewTimer(tc.timerFor)
			defer timer.Stop()

			c.Advance(tc.advanceBy)

			got, ok := recv(timer.C())
			require.Equal(t, tc.wantFired, ok)
			if tc.wantFired {
				assert.Equal(t, epoch.Add(tc.wantAt), got)
			}
		})
	}
}

func TestFake_AdvanceZeroFiresTimersAlreadyDue(t *testing.T) {
	c := clock.Fake(epoch)

	// A zero-duration timer is due the moment it is created; nothing has fired
	// it yet because the fake clock only fires inside Advance.
	timer := c.NewTimer(0)
	defer timer.Stop()

	_, ok := recv(timer.C())
	require.False(t, ok, "creating a timer must not fire it")

	c.Advance(0)

	got, ok := recv(timer.C())
	require.True(t, ok, "Advance(0) must flush timers that are already due")
	assert.Equal(t, epoch, got)
}

func TestFake_AdvanceZeroDoesNotFireAPendingTimer(t *testing.T) {
	c := clock.Fake(epoch)
	timer := c.NewTimer(time.Second)
	defer timer.Stop()

	c.Advance(0)

	_, ok := recv(timer.C())
	assert.False(t, ok)
}

func TestFake_AdvanceRejectsGoingBackwards(t *testing.T) {
	c := clock.Fake(epoch)

	assert.Panics(t, func() { c.Advance(-time.Second) })
}

func TestFake_TickerFiresRepeatedlyOnItsPeriod(t *testing.T) {
	c := clock.Fake(epoch)
	tk := c.NewTicker(10 * time.Second)
	defer tk.Stop()

	for i := 1; i <= 3; i++ {
		c.Advance(10 * time.Second)

		got, ok := recv(tk.C())
		require.True(t, ok, "tick %d must fire", i)
		assert.Equal(t, epoch.Add(time.Duration(i)*10*time.Second), got)
	}
}

func TestFake_TickerCoalescesTicksMissedWhileTheChannelWasFull(t *testing.T) {
	c := clock.Fake(epoch)
	tk := c.NewTicker(10 * time.Second)
	defer tk.Stop()

	// Five periods pass without the channel being drained. time.Ticker drops
	// sends into a full channel, so exactly one tick is pending afterwards and
	// it carries the time of the first tick, which is the one that landed.
	c.Advance(50 * time.Second)

	got, ok := recv(tk.C())
	require.True(t, ok)
	assert.Equal(t, epoch.Add(10*time.Second), got)

	_, ok = recv(tk.C())
	assert.False(t, ok, "missed ticks must coalesce to a single pending tick")
}

func TestFake_TickerDropsTicksRatherThanBlockingAnUndrainedChannel(t *testing.T) {
	c := clock.Fake(epoch)
	tk := c.NewTicker(time.Second)
	defer tk.Stop()

	// If Advance blocked on an undrained ticker channel this would hang rather
	// than fail, which is itself the assertion.
	for i := 0; i < 100; i++ {
		c.Advance(time.Second)
	}

	_, ok := recv(tk.C())
	require.True(t, ok)
	_, ok = recv(tk.C())
	assert.False(t, ok)
}

func TestFake_TickerStopPreventsFurtherTicks(t *testing.T) {
	c := clock.Fake(epoch)
	tk := c.NewTicker(10 * time.Second)

	tk.Stop()
	c.Advance(time.Hour)

	_, ok := recv(tk.C())
	assert.False(t, ok)

	tk.Stop() // idempotent
}

func TestFake_TickerResetToAShorterIntervalTakesEffectImmediately(t *testing.T) {
	c := clock.Fake(epoch)
	tk := c.NewTicker(time.Hour)
	defer tk.Stop()

	c.Advance(30 * time.Minute)
	_, ok := recv(tk.C())
	require.False(t, ok, "the original hourly deadline has not been reached")

	// Reset re-bases the deadline on the current fake time, matching
	// time.Ticker.Reset.
	tk.Reset(time.Second)

	c.Advance(time.Second)
	got, ok := recv(tk.C())
	require.True(t, ok)
	assert.Equal(t, epoch.Add(30*time.Minute+time.Second), got)
}

func TestFake_TickerResetRestartsAStoppedTicker(t *testing.T) {
	c := clock.Fake(epoch)
	tk := c.NewTicker(time.Hour)
	defer tk.Stop()

	tk.Stop()
	tk.Reset(time.Second)

	c.Advance(time.Second)
	_, ok := recv(tk.C())
	assert.True(t, ok)
}

func TestFake_NewTickerRejectsNonPositivePeriods(t *testing.T) {
	c := clock.Fake(epoch)

	assert.Panics(t, func() { c.NewTicker(0) })
	assert.Panics(t, func() { c.NewTicker(-time.Second) })

	tk := c.NewTicker(time.Second)
	defer tk.Stop()
	assert.Panics(t, func() { tk.Reset(0) })
}

func TestFake_TimerStopReportsWhetherItWasActive(t *testing.T) {
	c := clock.Fake(epoch)
	timer := c.NewTimer(time.Second)

	assert.True(t, timer.Stop(), "an unfired timer is active")
	assert.False(t, timer.Stop(), "a stopped timer is no longer active")

	timer2 := c.NewTimer(time.Second)
	defer timer2.Stop()
	c.Advance(time.Second)
	assert.False(t, timer2.Stop(), "a fired timer is no longer active")
}

func TestFake_TimerResetReportsWhetherItWasActiveAndRearms(t *testing.T) {
	c := clock.Fake(epoch)
	timer := c.NewTimer(time.Hour)
	defer timer.Stop()

	assert.True(t, timer.Reset(time.Second), "resetting an armed timer reports true")

	c.Advance(time.Second)
	_, ok := recv(timer.C())
	require.True(t, ok)

	assert.False(t, timer.Reset(time.Second), "resetting a fired timer reports false")
	c.Advance(time.Second)
	_, ok = recv(timer.C())
	assert.True(t, ok, "Reset rearms a fired timer")
}

func TestFake_StopDuringAdvanceIsSerializedAndSafe(t *testing.T) {
	c := clock.Fake(epoch)

	tickers := make([]clock.Ticker, 50)
	for i := range tickers {
		tickers[i] = c.NewTicker(time.Second)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			c.Advance(time.Second)
		}
	}()
	go func() {
		defer wg.Done()
		for _, tk := range tickers {
			tk.Stop()
		}
	}()
	wg.Wait()

	// Reaching this line at all is most of the assertion: a lock ordering
	// mistake would deadlock, and a missing lock would trip -race. The clock
	// must also have applied every advance exactly once.
	c.Advance(time.Second)
	assert.Equal(t, epoch.Add(101*time.Second), c.Now())
}

func TestFake_ConcurrentAdvanceIsSerialized(t *testing.T) {
	c := clock.Fake(epoch)

	const goroutines, steps = 8, 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < steps; j++ {
				c.Advance(time.Millisecond)
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, epoch.Add(goroutines*steps*time.Millisecond), c.Now(),
		"every advance must be applied exactly once")
}

func TestFake_BlockUntilReturnsImmediatelyWhenEnoughWaitersExist(t *testing.T) {
	c := clock.Fake(epoch)
	tk := c.NewTicker(time.Second)
	defer tk.Stop()

	c.BlockUntil(1) // already satisfied by the ticker
	c.BlockUntil(0) // trivially satisfied

	// Returning at all is the assertion; time must not have moved.
	assert.Equal(t, epoch, c.Now())
}

func TestFake_BlockUntilWaitsForASleeperToRegister(t *testing.T) {
	c := clock.Fake(epoch)

	slept := make(chan error, 1)
	go func() {
		slept <- c.Sleep(context.Background(), time.Minute)
	}()

	// Without BlockUntil this Advance could run before Sleep registers its
	// timer, and the sleeper would hang forever. That is the flake this method
	// exists to prevent.
	c.BlockUntil(1)
	c.Advance(time.Minute)

	require.NoError(t, <-slept)
}

func TestFake_BlockUntilWaitsForEveryWaiter(t *testing.T) {
	c := clock.Fake(epoch)

	const sleepers = 16
	done := make(chan error, sleepers)
	for i := 0; i < sleepers; i++ {
		go func() { done <- c.Sleep(context.Background(), time.Minute) }()
	}

	c.BlockUntil(sleepers)
	c.Advance(time.Minute)

	for i := 0; i < sleepers; i++ {
		require.NoError(t, <-done)
	}
}

func TestFake_BlockUntilOnlyReleasesBlockersThatAreSatisfied(t *testing.T) {
	c := clock.Fake(epoch)

	// One blocker wants two waiters, another wants three. Registering exactly
	// two must release the first and leave the second parked.
	released := make(chan int, 2)
	go func() { c.BlockUntil(2); released <- 2 }()
	go func() { c.BlockUntil(3); released <- 3 }()

	// Registering a third waiter would satisfy both, so register two and then
	// prove only the first blocker was released by satisfying the second
	// separately.
	t1 := c.NewTimer(time.Hour)
	defer t1.Stop()
	t2 := c.NewTimer(time.Hour)
	defer t2.Stop()

	assert.Equal(t, 2, <-released)

	t3 := c.NewTimer(time.Hour)
	defer t3.Stop()

	assert.Equal(t, 3, <-released)
}

func TestFake_NonPositiveTimerDurationsAreDueImmediately(t *testing.T) {
	c := clock.Fake(epoch)

	timer := c.NewTimer(-time.Hour)
	defer timer.Stop()

	c.Advance(0)
	got, ok := recv(timer.C())
	require.True(t, ok, "a negative duration must not schedule the timer in the past")
	assert.Equal(t, epoch, got)

	timer.Reset(-time.Minute)
	c.Advance(0)
	got, ok = recv(timer.C())
	require.True(t, ok)
	assert.Equal(t, epoch, got)
}

func TestFake_SleepReturnsImmediatelyForNonPositiveDurations(t *testing.T) {
	c := clock.Fake(epoch)

	require.NoError(t, c.Sleep(context.Background(), 0))
	require.NoError(t, c.Sleep(context.Background(), -time.Second))
}

func TestFake_SleepReturnsTheContextErrorWhenCancelled(t *testing.T) {
	c := clock.Fake(epoch)

	ctx, cancel := context.WithCancel(context.Background())

	errc := make(chan error, 1)
	go func() { errc <- c.Sleep(ctx, time.Hour) }()

	c.BlockUntil(1)
	cancel()

	assert.True(t, errors.Is(<-errc, context.Canceled))
}

func TestFake_SleepReturnsTheContextErrorWhenAlreadyCancelled(t *testing.T) {
	c := clock.Fake(epoch)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.ErrorIs(t, c.Sleep(ctx, time.Hour), context.Canceled)
}

func TestFake_CancelledSleepDeregistersItsWaiter(t *testing.T) {
	c := clock.Fake(epoch)

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- c.Sleep(ctx, time.Hour) }()

	c.BlockUntil(1)
	cancel()
	require.Error(t, <-errc)

	// A second sleeper must be able to satisfy BlockUntil(1) on its own, which
	// it can only do if the canceled waiter was removed.
	done := make(chan error, 1)
	go func() { done <- c.Sleep(context.Background(), time.Minute) }()
	c.BlockUntil(1)
	c.Advance(time.Minute)
	require.NoError(t, <-done)
}

// TestFake_AdvanceIsDeterministicAcrossRuns is the guarantee every later
// phase's tests are built on: the same schedule of advances against the same
// set of tickers must produce identical firing behavior every time.
func TestFake_AdvanceIsDeterministicAcrossRuns(t *testing.T) {
	const runs = 1000

	// run executes a multi-ticker schedule and returns the observed firings as
	// a comparable transcript.
	run := func() []string {
		c := clock.Fake(epoch)
		periods := []time.Duration{3 * time.Second, 5 * time.Second, 7 * time.Second}
		tickers := make([]clock.Ticker, len(periods))
		for i, p := range periods {
			tickers[i] = c.NewTicker(p)
		}
		defer func() {
			for _, tk := range tickers {
				tk.Stop()
			}
		}()

		timer := c.NewTimer(11 * time.Second)
		defer timer.Stop()

		transcript := make([]string, 0, 32)
		for step := 0; step < 20; step++ {
			c.Advance(time.Second)
			for i, tk := range tickers {
				if got, ok := recv(tk.C()); ok {
					transcript = append(transcript,
						"ticker"+strconv.Itoa(i)+"@"+got.Format(time.RFC3339))
				}
			}
			if got, ok := recv(timer.C()); ok {
				transcript = append(transcript, "timer@"+got.Format(time.RFC3339))
			}
		}
		return transcript
	}

	want := run()
	require.NotEmpty(t, want)

	for i := 1; i < runs; i++ {
		if !assert.Equal(t, want, run(), "run %d diverged", i) {
			t.Fatalf("Advance is not deterministic")
		}
	}
}
