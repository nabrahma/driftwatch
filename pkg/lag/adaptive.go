// Package lag measures target convergence time and derives the settlement
// window (M11).
//
// The settlement window W is the grace period after a key's last event during
// which driftwatch refuses to call a disagreement drift, because the real
// materializer may simply not have applied the event yet (§5.3). Picking W by
// hand is guesswork: too small and every busy key is a false positive, too
// large and real drift sits undetected for minutes.
//
// So it is measured. A small rotating sample of keys is watched from the moment
// an event is applied until the target reflects it, and W is derived from the
// p99 of that distribution with a safety factor.
//
// The subtlety that makes or breaks this is what happens to a probe that never
// converges. See the note on recordTimeout.
package lag

import (
	"sort"
	"sync"
	"time"
)

// Defaults for Config.
const (
	defaultProbeCount    = 200
	defaultProbeRotation = time.Minute
	defaultMaxPollDelay  = 60 * time.Second
	defaultWindowSize    = 10_000

	defaultMinWindow    = time.Second
	defaultMaxWindow    = 120 * time.Second
	defaultSafetyFactor = 3.0

	// minObservations is how much data is required before W is allowed to move
	// off its floor. A p99 computed from a handful of samples is noise, and
	// acting on noise is how W ends up oscillating.
	minObservations = 100

	// hysteresisRatio is how far the computed window must be from the current
	// one before W actually moves.
	hysteresisRatio = 0.20

	// minChangeInterval rate-limits changes to W. Together with the ratio this
	// is what stops W chasing every fluctuation in the materializer's latency.
	minChangeInterval = time.Minute
)

// Stats exposes the distribution for status reporting.
type Stats struct {
	P50, P90, P99, Max time.Duration
	Observations       int
	// TimedOut counts probes that never converged. They are included in the
	// distribution, not excluded from it; see recordTimeout.
	TimedOut      int
	CurrentWindow time.Duration
	// Adaptive reports whether W is being driven by measurement yet. It is
	// false until minObservations have accumulated, and false permanently when
	// a static window is configured.
	Adaptive bool
	// Clamped reports that the measured p99 wants a larger window than
	// MaxWindow allows, which means the materializer is slower than driftwatch
	// can meaningfully audit.
	Clamped bool
	// Changes counts how many times W has moved.
	Changes int
}

// window is the sliding distribution of convergence observations.
//
// It is a ring rather than a growing slice because it runs for the life of the
// process: at 200 probes rotating every minute, an unbounded history would be
// an unbounded collection, which §19.2 forbids.
type window struct {
	mu       sync.Mutex
	samples  []time.Duration
	next     int
	filled   bool
	timedOut int
	total    int
}

func newWindow(size int) *window {
	if size <= 0 {
		size = defaultWindowSize
	}
	return &window{samples: make([]time.Duration, size)}
}

// record adds a converged observation.
func (w *window) record(d time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.push(d)
}

// recordTimeout adds a probe that never converged, at the full poll deadline.
//
// This is the single most important line in the package, and it is the
// counter-intuitive one: a probe that timed out is recorded as an observation
// of MaxPollDelay rather than thrown away.
//
// Discarding it feels right — it is not a real measurement, the key may have
// been deleted, the read may have been unlucky. But the probes that time out
// are exactly the slowest ones, so discarding them removes the entire right
// tail of the distribution. The p99 of what is left is not the p99 of reality;
// it is the p99 of the subset that was fast enough to be measured. W is then
// computed from that shrunken figure, which makes it too small, which makes
// driftwatch start reporting keys as divergent that were only slow.
//
// The failure is self-reinforcing and it points the wrong way: the worse the
// materializer gets, the more probes time out, the more the tail is truncated,
// the smaller W becomes, and the more false positives are produced — precisely
// when the operator most needs to trust the output. See docs/DISCOVERIES.md
// D-008 for the measured effect.
func (w *window) recordTimeout(maxPollDelay time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.timedOut++
	w.push(maxPollDelay)
}

func (w *window) push(d time.Duration) {
	w.samples[w.next] = d
	w.next = (w.next + 1) % len(w.samples)
	if w.next == 0 {
		w.filled = true
	}
	w.total++
}

// snapshot returns the retained observations, sorted ascending.
func (w *window) snapshot() (sorted []time.Duration, timedOut, total int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	n := w.next
	if w.filled {
		n = len(w.samples)
	}
	out := make([]time.Duration, n)
	copy(out, w.samples[:n])
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })

	return out, w.timedOut, w.total
}

// percentile returns the p-th percentile of a sorted slice using nearest-rank,
// which is the definition that never interpolates a value that was not
// observed.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(p*float64(len(sorted))+0.999999) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

// controller turns the distribution into a settlement window, applying the
// floor, the ceiling, and the hysteresis that stops W oscillating.
type controller struct {
	minWindow    time.Duration
	maxWindow    time.Duration
	safetyFactor float64

	mu           sync.Mutex
	current      time.Duration
	lastChangeAt time.Time
	changes      int
	clamped      bool
	adaptive     bool
}

// decide computes the window implied by the distribution and reports whether it
// should replace the current one.
//
// Two guards stand between a computed value and an actual change, and they
// solve different problems. The ratio stops W twitching at every small shift in
// the materializer's latency. The rate limit stops a sustained drift in latency
// from walking W upward in a dozen small steps inside a minute, each of which
// would individually pass the ratio test.
func (c *controller) decide(sorted []time.Duration, observations int, now time.Time) (time.Duration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if observations < minObservations {
		// Not enough data to say anything. Sit on the floor and report that W
		// is not being driven by measurement, rather than adapting to noise.
		c.adaptive = false
		if c.current == 0 {
			c.current = c.minWindow
			return c.current, true
		}
		return c.current, false
	}
	c.adaptive = true

	p99 := percentile(sorted, 0.99)
	want := time.Duration(float64(p99) * c.safetyFactor)

	c.clamped = false
	switch {
	case want < c.minWindow:
		want = c.minWindow
	case want > c.maxWindow:
		// The materializer is slower than driftwatch can meaningfully audit.
		// Clamping is right — growing without bound would mean never asserting
		// anything — but it has to be visible, because past this point
		// driftwatch is knowingly using a window it has measured to be too
		// small.
		want = c.maxWindow
		c.clamped = true
	}

	if c.current == 0 {
		c.current = want
		c.lastChangeAt = now
		c.changes++
		return c.current, true
	}
	if want == c.current {
		return c.current, false
	}

	delta := float64(want-c.current) / float64(c.current)
	if delta < 0 {
		delta = -delta
	}
	if delta <= hysteresisRatio {
		return c.current, false
	}
	if !c.lastChangeAt.IsZero() && now.Sub(c.lastChangeAt) < minChangeInterval {
		return c.current, false
	}

	c.current = want
	c.lastChangeAt = now
	c.changes++
	return c.current, true
}

// state returns the controller's reportable state.
func (c *controller) state() (current time.Duration, adaptive, clamped bool, changes int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current, c.adaptive, c.clamped, c.changes
}
