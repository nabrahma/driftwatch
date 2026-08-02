// Package clock provides the injectable Clock abstraction used everywhere (M1).
//
// Nothing else in driftwatch calls time.Now, time.NewTimer or time.NewTicker
// directly. Every elapsed-time decision goes through a Clock, so tests can
// control time exactly rather than sleeping and hoping (PRD §1.1.4, §16.4).
//
// Fake is the reason this package exists. Its Advance fires due timers and
// tickers synchronously before returning, and BlockUntil lets a test wait until
// the code under test has actually registered its waiter — without which
// Advance can fire into the void and produce an intermittent failure.
package clock

import (
	"context"
	"sync"
	"time"
)

// Clock abstracts time for testability.
type Clock interface {
	// Now returns the current wall-clock time. Use only for display and for
	// timestamps written to output, never for elapsed-time decisions.
	Now() time.Time

	// Since returns elapsed time using a monotonic source.
	Since(t time.Time) time.Duration

	// NewTicker returns a ticker driven by this clock. It panics if d is not
	// positive, matching time.NewTicker.
	NewTicker(d time.Duration) Ticker

	// NewTimer returns a timer driven by this clock.
	NewTimer(d time.Duration) Timer

	// Sleep blocks for d, respecting ctx cancellation. It returns ctx.Err() if
	// the context is done first, and nil if d elapsed or was non-positive.
	Sleep(ctx context.Context, d time.Duration) error
}

// Ticker delivers ticks on a channel at a fixed interval.
//
// Like time.Ticker, a tick is dropped rather than queued when the channel
// already holds an undelivered one, so a slow consumer can never block the
// clock or accumulate an unbounded backlog.
type Ticker interface {
	// C returns the channel ticks are delivered on.
	C() <-chan time.Time
	// Stop halts the ticker. It is idempotent and does not close the channel.
	Stop()
	// Reset changes the interval and re-bases the next tick on the current
	// time. It panics if d is not positive.
	Reset(d time.Duration)
}

// Timer delivers a single value on a channel after a duration.
type Timer interface {
	// C returns the channel the value is delivered on.
	C() <-chan time.Time
	// Stop prevents the timer from firing and reports whether it was still
	// armed at the time of the call.
	Stop() bool
	// Reset rearms the timer for d and reports whether it was still armed at
	// the time of the call.
	Reset(d time.Duration) bool
}

// Real returns a Clock backed by the time package.
func Real() Clock { return realClock{} }

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) Since(t time.Time) time.Duration { return time.Since(t) }

func (realClock) NewTicker(d time.Duration) Ticker { return &realTicker{t: time.NewTicker(d)} }

func (realClock) NewTimer(d time.Duration) Timer { return &realTimer{t: time.NewTimer(d)} }

func (realClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type realTicker struct{ t *time.Ticker }

func (r *realTicker) C() <-chan time.Time   { return r.t.C }
func (r *realTicker) Stop()                 { r.t.Stop() }
func (r *realTicker) Reset(d time.Duration) { r.t.Reset(d) }

type realTimer struct{ t *time.Timer }

func (r *realTimer) C() <-chan time.Time        { return r.t.C }
func (r *realTimer) Stop() bool                 { return r.t.Stop() }
func (r *realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

// FakeClock is a manually-advanced Clock for tests.
type FakeClock interface {
	Clock

	// Advance moves time forward and fires any due tickers/timers
	// synchronously before returning. It panics if d is negative: a fake clock
	// that can go backwards produces tests that pass for the wrong reason.
	//
	// "Synchronously" means the value is in the waiter's channel before Advance
	// returns. It does not mean a goroutine woken by that value has already
	// run — assert on a channel the woken goroutine writes to, never on a
	// variable it sets.
	Advance(d time.Duration)

	// BlockUntil waits until at least n timers, tickers or Sleep calls are
	// registered and pending on this clock. Prevents flaky tests that advance
	// before a waiter has registered.
	BlockUntil(n int)
}

// Fake returns a manually-advanced Clock for tests, starting at start.
func Fake(start time.Time) FakeClock {
	return &fakeClock{now: start, waiters: make(map[uint64]*fakeWaiter)}
}

type fakeClock struct {
	mu       sync.Mutex
	now      time.Time
	nextID   uint64
	waiters  map[uint64]*fakeWaiter
	blockers []*blocker
}

// blocker is one goroutine parked inside BlockUntil.
type blocker struct {
	n  int
	ch chan struct{}
}

// fakeWaiter is a registered timer or ticker. period is zero for timers.
type fakeWaiter struct {
	clk      *fakeClock
	id       uint64
	deadline time.Time
	period   time.Duration
	ch       chan time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Since(t time.Time) time.Duration {
	return c.Now().Sub(t)
}

// NewTicker returns a ticker driven by this clock.
//
// It panics on a non-positive interval, matching time.NewTicker. That is a
// programmer error rather than a runtime condition: a ticker that fires every
// zero seconds has no meaningful behavior, and returning a broken one would
// surface as a test that hangs rather than as the mistake it is.
func (c *fakeClock) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("clock: NewTicker requires a positive interval")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return &fakeTicker{w: c.registerLocked(d, d)}
}

func (c *fakeClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	return &fakeTimer{w: c.registerLocked(d, 0)}
}

func (c *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d <= 0 {
		return nil
	}
	t := c.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *fakeClock) Advance(d time.Duration) {
	// A programmer error. A fake clock that can go backwards produces tests
	// that pass for the wrong reason — a settlement window that "elapsed"
	// because time reversed under it — and those are far more expensive to
	// find later than a panic here.
	if d < 0 {
		panic("clock: Fake.Advance requires a non-negative duration")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	target := c.now.Add(d)
	for {
		w := c.earliestDueLocked(target)
		if w == nil {
			break
		}
		// Move the clock to the waiter's own deadline before firing, so a tick
		// carries its scheduled time rather than the advance target. This is
		// what makes one large advance produce the same transcript as the
		// equivalent sequence of small ones.
		c.now = w.deadline

		select {
		case w.ch <- c.now:
		default:
			// A tick is already pending and undelivered: drop this one,
			// matching time.Ticker. Never block — a fake clock that blocks on
			// an undrained channel deadlocks the test instead of failing it.
		}

		if w.period > 0 {
			w.deadline = w.deadline.Add(w.period)
		} else {
			delete(c.waiters, w.id)
		}
	}
	c.now = target

	c.checkBlockersLocked()
}

func (c *fakeClock) BlockUntil(n int) {
	c.mu.Lock()
	if len(c.waiters) >= n {
		c.mu.Unlock()
		return
	}
	b := &blocker{n: n, ch: make(chan struct{})}
	c.blockers = append(c.blockers, b)
	c.mu.Unlock()

	<-b.ch
}

// registerLocked adds a waiter due after d. A non-positive d is due
// immediately, meaning the next Advance — including Advance(0) — fires it.
func (c *fakeClock) registerLocked(d, period time.Duration) *fakeWaiter {
	if d < 0 {
		d = 0
	}
	c.nextID++
	w := &fakeWaiter{
		clk:      c,
		id:       c.nextID,
		deadline: c.now.Add(d),
		period:   period,
		ch:       make(chan time.Time, 1),
	}
	c.waiters[w.id] = w
	c.checkBlockersLocked()
	return w
}

// earliestDueLocked returns the pending waiter with the smallest deadline at or
// before target, breaking ties by registration order. Map iteration order is
// randomized, so selecting the minimum explicitly is what makes Advance
// deterministic.
func (c *fakeClock) earliestDueLocked(target time.Time) *fakeWaiter {
	var best *fakeWaiter
	for _, w := range c.waiters {
		if w.deadline.After(target) {
			continue
		}
		if best == nil || w.deadline.Before(best.deadline) ||
			(w.deadline.Equal(best.deadline) && w.id < best.id) {
			best = w
		}
	}
	return best
}

func (c *fakeClock) checkBlockersLocked() {
	pending := len(c.waiters)
	kept := c.blockers[:0]
	for _, b := range c.blockers {
		if pending >= b.n {
			close(b.ch)
			continue
		}
		kept = append(kept, b)
	}
	c.blockers = kept
}

// stop removes the waiter and reports whether it was still registered.
func (w *fakeWaiter) stop() bool {
	w.clk.mu.Lock()
	defer w.clk.mu.Unlock()
	_, active := w.clk.waiters[w.id]
	if active {
		delete(w.clk.waiters, w.id)
		w.clk.checkBlockersLocked()
	}
	return active
}

// reset rearms the waiter for d from the current fake time and reports whether
// it was still registered beforehand.
func (w *fakeWaiter) reset(d, period time.Duration) bool {
	w.clk.mu.Lock()
	defer w.clk.mu.Unlock()
	_, active := w.clk.waiters[w.id]
	if d < 0 {
		d = 0
	}
	w.deadline = w.clk.now.Add(d)
	w.period = period
	w.clk.waiters[w.id] = w
	w.clk.checkBlockersLocked()
	return active
}

type fakeTicker struct{ w *fakeWaiter }

func (f *fakeTicker) C() <-chan time.Time { return f.w.ch }

func (f *fakeTicker) Stop() { f.w.stop() }

func (f *fakeTicker) Reset(d time.Duration) {
	// A programmer error, as in NewTicker above and for the same reason.
	if d <= 0 {
		panic("clock: Ticker.Reset requires a positive interval")
	}
	f.w.reset(d, d)
}

type fakeTimer struct{ w *fakeWaiter }

func (f *fakeTimer) C() <-chan time.Time { return f.w.ch }

func (f *fakeTimer) Stop() bool { return f.w.stop() }

func (f *fakeTimer) Reset(d time.Duration) bool { return f.w.reset(d, 0) }
