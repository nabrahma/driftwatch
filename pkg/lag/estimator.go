package lag

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// Config configures an Estimator. The zero value of every duration field takes
// the documented default.
type Config struct {
	Oracle *oracle.Oracle
	Target target.Target
	Shape  projection.Shape
	Clock  clock.Clock

	// ProbeCount is how many keys are watched at once. Default 200.
	ProbeCount int
	// ProbeRotation is how often the sample is replaced. Default 1m.
	ProbeRotation time.Duration
	// MaxPollDelay is when a probe is given up on. Default 60s. A probe that
	// reaches it is recorded at this value rather than discarded.
	MaxPollDelay time.Duration
	// WindowSize is how many observations are retained. Default 10,000.
	WindowSize int

	// MinWindow floors W. Default 1s.
	MinWindow time.Duration
	// MaxWindow ceilings W. Default 120s.
	MaxWindow time.Duration
	// SafetyFactor multiplies the measured p99. Default 3.0.
	SafetyFactor float64
	// Static disables adaptation when set. The distribution is still measured,
	// because knowing how far W is from the truth is useful even when W is not
	// being driven by it.
	Static *time.Duration

	// Seed makes probe sampling reproducible. Tests set it; production leaves
	// it zero, which seeds from the clock.
	Seed int64

	// OnWindowChange is called whenever W moves. §9 M11 requires every change
	// to be visible; the logger lands in Phase 5, so this is the seam.
	OnWindowChange func(old, next time.Duration, s Stats)
}

func (c *Config) applyDefaults() {
	if c.ProbeCount <= 0 {
		c.ProbeCount = defaultProbeCount
	}
	if c.ProbeRotation <= 0 {
		c.ProbeRotation = defaultProbeRotation
	}
	if c.MaxPollDelay <= 0 {
		c.MaxPollDelay = defaultMaxPollDelay
	}
	if c.WindowSize <= 0 {
		c.WindowSize = defaultWindowSize
	}
	if c.MinWindow <= 0 {
		c.MinWindow = defaultMinWindow
	}
	if c.MaxWindow <= 0 {
		c.MaxWindow = defaultMaxWindow
	}
	if c.MaxWindow < c.MinWindow {
		c.MaxWindow = c.MinWindow
	}
	if c.SafetyFactor <= 0 {
		c.SafetyFactor = defaultSafetyFactor
	}
	if c.Clock == nil {
		c.Clock = clock.Real()
	}
}

// Estimator measures event-to-target convergence and derives W from it.
type Estimator struct {
	cfg    Config
	probes *probeSet
	win    *window
	ctrl   *controller

	// window is read on the sweeper's hot path — once per key per sweep — so
	// it is an atomic rather than a mutex. §9 M11 requires the read path to be
	// lock-free.
	window atomic.Int64

	mu      sync.Mutex
	pending map[string]*pending

	abandoned atomic.Int64
	polls     atomic.Int64
}

// New returns an Estimator configured by cfg.
//
// Config goes by value to match oracle.New and seqtrack.New, and because
// applyDefaults mutates the copy rather than the caller's struct. New is called
// once at startup, so the copy costs nothing.
//
//nolint:gocritic // hugeParam: deliberate, see above.
func New(cfg Config) *Estimator {
	cfg.applyDefaults()

	seed := cfg.Seed
	if seed == 0 {
		seed = cfg.Clock.Now().UnixNano()
	}

	e := &Estimator{
		cfg:     cfg,
		probes:  newProbeSet(cfg.ProbeCount, seed),
		win:     newWindow(cfg.WindowSize),
		pending: map[string]*pending{},
		ctrl: &controller{
			minWindow:    cfg.MinWindow,
			maxWindow:    cfg.MaxWindow,
			safetyFactor: cfg.SafetyFactor,
		},
	}

	if cfg.Static != nil {
		e.window.Store(int64(*cfg.Static))
	} else {
		e.window.Store(int64(cfg.MinWindow))
	}
	return e
}

// SettlementWindow returns the current W. Safe for concurrent use and lock-free.
func (e *Estimator) SettlementWindow() time.Duration {
	return time.Duration(e.window.Load())
}

// OfferKey presents a key as a probe candidate.
//
// The sweeper calls this for keys it is already visiting, which is how
// selection stays cheap: no separate scan, just a reservoir sample over traffic
// that was happening anyway.
func (e *Estimator) OfferKey(key string) { e.probes.offer(key) }

// IsProbe reports whether a key is currently being measured, so the applier
// knows whether calling Observe is worthwhile.
func (e *Estimator) IsProbe(key string) bool { return e.probes.contains(key) }

// Observe is called by the applier when a probe key changes.
//
// It starts a measurement: from now until the target reflects this version, or
// until the deadline. A second Observe for the same key replaces the first,
// because the older measurement is now waiting for a value the target should no
// longer converge to.
func (e *Estimator) Observe(key string, version uint64, at time.Time) {
	if !e.probes.contains(key) {
		return
	}

	entry, ok := e.cfg.Oracle.Get(key)
	if !ok || entry.Version != version {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.pending[key] = &pending{
		key:        key,
		version:    version,
		expected:   entry.Value,
		startedAt:  at,
		nextPollAt: at.Add(basePollInterval),
		interval:   basePollInterval,
	}
}

// PendingCount returns how many measurements are in flight.
func (e *Estimator) PendingCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.pending)
}

// Run drives probing until ctx is done.
//
// It ticks rather than sleeping per probe, so a hundred concurrent
// measurements cost one timer. With a fake clock that also makes the whole
// thing deterministic: advance the clock, and exactly the probes that were due
// are polled.
func (e *Estimator) Run(ctx context.Context) error {
	ticker := e.cfg.Clock.NewTicker(basePollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-ticker.C():
			e.Tick(ctx, now)
		}
	}
}

// Tick performs one poll round. Exposed so tests can drive the estimator
// synchronously against a fake clock instead of racing a goroutine.
func (e *Estimator) Tick(ctx context.Context, now time.Time) {
	e.pollDue(ctx, now)

	if e.probes.rotateIfDue(now, e.cfg.ProbeRotation) {
		// The sample changed, so measurements for keys that are no longer
		// probes are no longer wanted.
		e.dropNonProbes()
	}
	e.Recompute(now)
}

// pollDue polls every pending measurement whose next poll time has arrived.
func (e *Estimator) pollDue(ctx context.Context, now time.Time) {
	e.mu.Lock()
	due := make([]*pending, 0, len(e.pending))
	for _, p := range e.pending {
		if !p.nextPollAt.After(now) {
			due = append(due, p)
		}
	}
	e.mu.Unlock()

	for _, p := range due {
		if ctx.Err() != nil {
			return
		}
		e.polls.Add(1)

		switch e.poll(ctx, p, now) {
		case pollConverged:
			e.win.record(now.Sub(p.startedAt))
			e.remove(p.key)
		case pollTimedOut:
			e.win.recordTimeout(e.cfg.MaxPollDelay)
			e.remove(p.key)
		case pollAbandoned:
			e.abandoned.Add(1)
			e.remove(p.key)
		case pollPending:
			// poll updated nextPollAt in place; the probe stays.
		}
	}
}

func (e *Estimator) remove(key string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.pending, key)
}

func (e *Estimator) dropNonProbes() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for key := range e.pending {
		if !e.probes.contains(key) {
			delete(e.pending, key)
		}
	}
}

// Recompute derives W from the distribution and applies it if the hysteresis
// rules allow.
//
// A static window short-circuits the control path but not the measurement:
// knowing how far a hand-picked W is from the measured truth is exactly the
// information that would justify changing it.
func (e *Estimator) Recompute(now time.Time) time.Duration {
	sorted, _, _ := e.win.snapshot()

	if e.cfg.Static != nil {
		return time.Duration(e.window.Load())
	}

	old := time.Duration(e.window.Load())
	next, changed := e.ctrl.decide(sorted, len(sorted), now)
	if !changed || next == old {
		return old
	}

	e.window.Store(int64(next))
	if e.cfg.OnWindowChange != nil {
		e.cfg.OnWindowChange(old, next, e.Stats())
	}
	return next
}

// Stats exposes the distribution for status reporting.
func (e *Estimator) Stats() Stats {
	sorted, timedOut, _ := e.win.snapshot()
	_, adaptive, clamped, changes := e.ctrl.state()

	s := Stats{
		Observations:  len(sorted),
		TimedOut:      timedOut,
		CurrentWindow: time.Duration(e.window.Load()),
		Adaptive:      adaptive && e.cfg.Static == nil,
		Clamped:       clamped,
		Changes:       changes,
	}
	if len(sorted) > 0 {
		s.P50 = percentile(sorted, 0.50)
		s.P90 = percentile(sorted, 0.90)
		s.P99 = percentile(sorted, 0.99)
		s.Max = sorted[len(sorted)-1]
	}
	return s
}

// Abandoned returns how many measurements were dropped because the oracle moved
// on or the target held the wrong type. They are not observations in either
// direction.
func (e *Estimator) Abandoned() int64 { return e.abandoned.Load() }

// Polls returns how many target reads the estimator has made, so a test can
// assert the probing is bounded rather than a busy loop.
func (e *Estimator) Polls() int64 { return e.polls.Load() }

// ProbeCount returns the number of keys currently sampled.
func (e *Estimator) ProbeCount() int { return e.probes.size() }
