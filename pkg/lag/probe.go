package lag

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// Poll backoff. §5.3 specifies 10ms, 20ms, 40ms and so on, capped, until the
// deadline.
const (
	basePollInterval = 10 * time.Millisecond
	maxPollInterval  = time.Second
)

// probeSet holds the rotating sample of keys whose convergence is measured.
//
// Selection is reservoir sampling over the keys the sweeper already visits,
// rather than a separate scan: §9 M11 requires it to be cheap, and a keyspace
// walk purely to pick probes would cost more than the measurement is worth.
// Rotation matters because a fixed sample drifts toward whichever keys happen
// to be cold, and cold keys converge instantly — they would measure nothing.
type probeSet struct {
	mu       sync.Mutex
	rnd      *rand.Rand
	capacity int

	keys  []string
	index map[string]struct{}
	// seen counts offers since the last rotation, which is the denominator
	// reservoir sampling needs to stay uniform.
	seen      int
	rotatedAt time.Time
	rotations int
}

func newProbeSet(capacity int, seed int64) *probeSet {
	if capacity <= 0 {
		capacity = defaultProbeCount
	}
	return &probeSet{
		rnd:      rand.New(rand.NewSource(seed)), //nolint:gosec // sampling, not security
		capacity: capacity,
		index:    map[string]struct{}{},
	}
}

// offer presents a key as a probe candidate.
func (p *probeSet) offer(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, already := p.index[key]; already {
		return
	}
	p.seen++

	if len(p.keys) < p.capacity {
		p.keys = append(p.keys, key)
		p.index[key] = struct{}{}
		return
	}

	// Classic reservoir: replace a random slot with probability capacity/seen,
	// which keeps every offered key equally likely to be held.
	if j := p.rnd.Intn(p.seen); j < p.capacity {
		delete(p.index, p.keys[j])
		p.keys[j] = key
		p.index[key] = struct{}{}
	}
}

// contains reports whether a key is currently a probe.
func (p *probeSet) contains(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.index[key]
	return ok
}

// rotateIfDue empties the sample so a fresh one accumulates, and reports
// whether it did.
func (p *probeSet) rotateIfDue(now time.Time, every time.Duration) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.rotatedAt.IsZero() {
		p.rotatedAt = now
		return false
	}
	if now.Sub(p.rotatedAt) < every {
		return false
	}

	p.keys = p.keys[:0]
	p.index = map[string]struct{}{}
	p.seen = 0
	p.rotatedAt = now
	p.rotations++
	return true
}

func (p *probeSet) size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.keys)
}

// pending is one convergence measurement in flight: an event has been applied
// to the oracle and the target has not caught up yet.
type pending struct {
	key      string
	version  uint64
	expected event.Value

	startedAt  time.Time
	nextPollAt time.Time
	interval   time.Duration
}

// pollOutcome is what one poll of a pending probe concluded.
type pollOutcome uint8

const (
	// pollPending means the target has not caught up and the probe stays.
	pollPending pollOutcome = iota
	// pollConverged means the target now matches; the elapsed time is an
	// observation.
	pollConverged
	// pollTimedOut means the deadline passed; recorded at MaxPollDelay.
	pollTimedOut
	// pollAbandoned means the measurement is no longer meaningful — the oracle
	// moved on, so what we are waiting for is not what the target should now
	// hold. Not an observation either way.
	pollAbandoned
)

// poll reads the target once and decides what became of the probe.
func (e *Estimator) poll(ctx context.Context, p *pending, now time.Time) pollOutcome {
	// If the oracle has advanced past the version being measured, the target is
	// no longer converging toward the value we recorded. Timing it out would
	// invent a slow observation and converging it would invent a fast one, so
	// the honest thing is to drop the measurement entirely.
	current, ok := e.cfg.Oracle.Version(p.key)
	if !ok || current != p.version {
		return pollAbandoned
	}

	tv, err := e.cfg.Target.Get(ctx, p.key, e.cfg.Shape)
	switch {
	case err == nil:
		if p.expected.Equal(tv) {
			return pollConverged
		}
	case errors.Is(err, target.ErrWrongType):
		// The target holds something else entirely. That is drift for the
		// sweeper to report, not a latency measurement.
		return pollAbandoned
	default:
		// A read failure says nothing about convergence. Leave the probe
		// pending; the deadline will end it if the store stays broken.
	}

	if now.Sub(p.startedAt) >= e.cfg.MaxPollDelay {
		return pollTimedOut
	}

	p.interval *= 2
	if p.interval > maxPollInterval {
		p.interval = maxPollInterval
	}
	p.nextPollAt = now.Add(p.interval)
	return pollPending
}
