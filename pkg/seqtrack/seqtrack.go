// Package seqtrack tracks per-publisher sequence numbers, gaps and epochs (§5.2, M5).
//
// This is where driftwatch decides whether it can be trusted. Every other part
// of the tool compares an expectation against reality; this part answers the
// prior question of whether the expectation was built from a complete view.
//
// Without sequence numbers, "the oracle and the target disagree" has two
// explanations — the target is wrong, or driftwatch missed an event — and no
// way to tell them apart. With them, a gap is direct evidence that driftwatch
// itself is the unreliable party, and the affected keys are reported separately
// and never alerted on. That single distinction is what makes the tool
// something an operator will act on rather than silence.
package seqtrack

import (
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/event"
)

// Defaults for Config. Each bounds something a misbehaving publisher could
// otherwise make unbounded.
const (
	defaultMaxPublishers          = 1024
	defaultImplicitRestartDelta   = 1000
	defaultImplicitRestartCeiling = 100
)

// maxSeq is the top of the sequence space, where successor arithmetic wraps.
const maxSeq = ^uint64(0)

// Verdict is the classification Observe assigns to an event.
type Verdict uint8

// The verdicts, in the order the §5.2 algorithm produces them.
const (
	// Accept means the event is the immediate successor of the last one seen.
	Accept Verdict = iota
	// AcceptWithGap means the event skipped ahead, revealing lost events.
	AcceptWithGap
	// AcceptLateFill means the event filled a hole recorded earlier.
	AcceptLateFill
	// AcceptAfterRestart means the publisher began a new incarnation.
	AcceptAfterRestart
	// AcceptFirstSeen means this is the first event from this publisher, whose
	// sequence is adopted as a baseline rather than treated as a gap.
	AcceptFirstSeen
	// DropDuplicate means the event was already seen.
	DropDuplicate
	// DropStaleEpoch means the event belongs to a superseded incarnation.
	DropStaleEpoch
)

var verdictNames = [...]string{
	Accept:             "accept",
	AcceptWithGap:      "accept_with_gap",
	AcceptLateFill:     "accept_late_fill",
	AcceptAfterRestart: "accept_after_restart",
	AcceptFirstSeen:    "accept_first_seen",
	DropDuplicate:      "drop_duplicate",
	DropStaleEpoch:     "drop_stale_epoch",
}

// String returns the metric-friendly name of the verdict.
func (v Verdict) String() string {
	if int(v) >= len(verdictNames) {
		return "Verdict(" + strconv.Itoa(int(v)) + ")"
	}
	return verdictNames[v]
}

// Accepted reports whether the event should be applied to the oracle.
func (v Verdict) Accepted() bool {
	switch v {
	case Accept, AcceptWithGap, AcceptLateFill, AcceptAfterRestart, AcceptFirstSeen:
		return true
	case DropDuplicate, DropStaleEpoch:
		return false
	default:
		return false
	}
}

// Gap is a range of sequence numbers that were never observed. It is direct
// evidence of event loss.
type Gap struct {
	Publisher  string
	Epoch      uint64
	From, To   uint64 // inclusive
	DetectedAt time.Time
}

// String renders the gap for logs.
func (g Gap) String() string {
	return g.Publisher + "/" + strconv.FormatUint(g.Epoch, 10) +
		" missing " + Interval{From: g.From, To: g.To}.String() +
		" (" + strconv.FormatUint(g.To-g.From+1, 10) + ")"
}

// PublisherState is a snapshot of what the tracker knows about one publisher.
type PublisherState struct {
	ID string

	// Epoch is the incarnation the publisher declared, as sent on the wire.
	Epoch uint64
	// Incarnation counts restarts driftwatch inferred without the publisher
	// declaring one. It is kept separate from Epoch because comparing wire
	// epochs is how stale events are rejected, and raising the tracked wire
	// epoch on an inferred restart would reject every later event.
	Incarnation uint64

	HWM  uint64
	Gaps *GapSet

	FirstSeen time.Time
	LastSeen  time.Time

	EventCount   uint64
	RestartCount uint64

	// Bootstrap records that the publisher's first observed sequence was
	// adopted as a baseline rather than seen from the start of its stream, so
	// anything emitted before driftwatch attached is unaccounted for.
	Bootstrap bool
}

// Config configures a Tracker. The zero value is usable; every field has a
// documented default.
type Config struct {
	// MaxPublishers bounds the publisher map. Default 1024.
	MaxPublishers int
	// MaxGapIntervals bounds each publisher's interval list. Default 1024.
	MaxGapIntervals int
	// ImplicitRestartDelta is how far a sequence must fall below the high-water
	// mark before a restart is inferred. Default 1000.
	ImplicitRestartDelta uint64
	// ImplicitRestartCeiling is how small a sequence must be for a restart to
	// be inferred. Default 100.
	ImplicitRestartCeiling uint64
	// Clock is the injected clock. Defaults to the real one.
	Clock clock.Clock
}

func (c *Config) applyDefaults() {
	if c.MaxPublishers <= 0 {
		c.MaxPublishers = defaultMaxPublishers
	}
	if c.MaxGapIntervals <= 0 {
		c.MaxGapIntervals = defaultMaxIntervals
	}
	if c.ImplicitRestartDelta == 0 {
		c.ImplicitRestartDelta = defaultImplicitRestartDelta
	}
	if c.ImplicitRestartCeiling == 0 {
		c.ImplicitRestartCeiling = defaultImplicitRestartCeiling
	}
	if c.Clock == nil {
		c.Clock = clock.Real()
	}
}

// New returns a Tracker configured by cfg.
func New(cfg Config) *Tracker {
	cfg.applyDefaults()
	return &Tracker{cfg: cfg, pubs: make(map[string]*PublisherState)}
}

// Tracker classifies events against per-publisher sequence history.
//
// Observe is called only from the single applier goroutine (§6.3), but
// Publishers and Trust are read concurrently by the sweeper and the explain
// engine, so the state is behind a mutex and readers get copies.
type Tracker struct {
	mu   sync.RWMutex
	cfg  Config
	pubs map[string]*PublisherState

	evictions uint64
}

// Observe classifies an event and updates publisher state. It returns the
// verdict plus any gap that was newly detected.
//
// This implements the algorithm in PRD §5.2 exactly.
func (t *Tracker) Observe(e *event.Event) (Verdict, *Gap) {
	now := t.cfg.Clock.Now()

	t.mu.Lock()
	defer t.mu.Unlock()

	st, known := t.pubs[e.Publisher]
	if !known {
		t.admitLocked(e, now)
		return AcceptFirstSeen, nil
	}

	st.LastSeen = now

	switch {
	case e.Epoch > st.Epoch:
		// The publisher declared a restart. Its previous sequence space no
		// longer exists, so carrying its gaps forward would keep the publisher
		// permanently suspect for events that can never arrive.
		st.Epoch = e.Epoch
		st.HWM = e.Seq
		st.Gaps.Clear()
		st.RestartCount++
		st.EventCount++
		return AcceptAfterRestart, nil

	case e.Epoch < st.Epoch:
		// A straggler from a previous incarnation. Applying it would resurrect
		// state the restart already invalidated.
		return DropStaleEpoch, nil
	}

	// Same epoch.
	switch {
	// The HWM guard is not redundant. At MaxUint64 the successor arithmetic
	// wraps to zero, so a publisher that reset to seq 0 would be classified as
	// an ordinary in-order event and its restart would go unrecorded. Excluding
	// the top of the space sends that case to the implicit-restart heuristic
	// below, which is where a wrap and a restart — genuinely indistinguishable
	// from the outside — are handled together.
	case st.HWM != maxSeq && e.Seq == st.HWM+1:
		st.HWM = e.Seq
		st.EventCount++
		return Accept, nil

	case e.Seq > st.HWM+1:
		gap := &Gap{
			Publisher:  e.Publisher,
			Epoch:      st.Epoch,
			From:       st.HWM + 1,
			To:         e.Seq - 1,
			DetectedAt: now,
		}
		st.Gaps.Add(gap.From, gap.To)
		st.HWM = e.Seq
		st.EventCount++
		return AcceptWithGap, gap

	default: // e.Seq <= st.HWM
		if t.isImplicitRestart(st, e.Seq) {
			st.HWM = e.Seq
			st.Gaps.Clear()
			st.Incarnation++
			st.RestartCount++
			st.EventCount++
			return AcceptAfterRestart, nil
		}
		if st.Gaps.Contains(e.Seq) {
			st.Gaps.Fill(e.Seq)
			st.EventCount++
			return AcceptLateFill, nil
		}
		return DropDuplicate, nil
	}
}

// admitLocked records a publisher seen for the first time, adopting its current
// sequence as the baseline.
//
// The alternative — treating the first sequence as evidence that everything
// below it was lost — would mark a publisher that has been running for a week
// as missing millions of events, which is true but useless. Bootstrap records
// that the baseline was adopted rather than observed.
func (t *Tracker) admitLocked(e *event.Event, now time.Time) {
	if len(t.pubs) >= t.cfg.MaxPublishers {
		t.evictOldestLocked()
	}
	t.pubs[e.Publisher] = &PublisherState{
		ID:         e.Publisher,
		Epoch:      e.Epoch,
		HWM:        e.Seq,
		Gaps:       NewGapSet(t.cfg.MaxGapIntervals),
		FirstSeen:  now,
		LastSeen:   now,
		EventCount: 1,
		Bootstrap:  true,
	}
}

// isImplicitRestart reports whether a backwards sequence step is better
// explained by a publisher that restarted without saying so than by a delayed
// duplicate.
//
// The heuristic is deliberately narrow: the sequence must be both far below the
// high-water mark and close to zero. A publisher that restarts and renumbers
// from 1 while still reporting the same epoch would otherwise have every event
// it ever sends again rejected as a duplicate — a silent, permanent blindness.
//
// The same test catches wraparound at MaxUint64, which is indistinguishable
// from a restart and is treated as one.
func (t *Tracker) isImplicitRestart(st *PublisherState, seq uint64) bool {
	return st.HWM > seq &&
		st.HWM-seq > t.cfg.ImplicitRestartDelta &&
		seq < t.cfg.ImplicitRestartCeiling
}

// evictOldestLocked drops the least recently seen publisher.
func (t *Tracker) evictOldestLocked() {
	var oldestID string
	var oldestAt time.Time
	for id, st := range t.pubs {
		if oldestID == "" || st.LastSeen.Before(oldestAt) ||
			(st.LastSeen.Equal(oldestAt) && id < oldestID) {
			oldestID, oldestAt = id, st.LastSeen
		}
	}
	if oldestID != "" {
		delete(t.pubs, oldestID)
		t.evictions++
	}
}

// Publishers returns a snapshot of all publisher states, sorted by ID.
//
// The GapSet in each snapshot is a deep copy. Handing out the live pointer
// would let a reader mutate tracker state from another goroutine.
func (t *Tracker) Publishers() []PublisherState {
	t.mu.RLock()
	defer t.mu.RUnlock()

	out := make([]PublisherState, 0, len(t.pubs))
	for _, st := range t.pubs {
		snapshot := *st
		snapshot.Gaps = st.Gaps.clone()
		out = append(out, snapshot)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Trust reports whether the tracker's view of a publisher is complete.
//
// An unknown publisher is Complete: driftwatch has no evidence of loss, and
// inventing suspicion would suppress findings for no reason. A publisher with
// any outstanding gap is Suspect, and so is one whose gap set was truncated,
// because a truncated set no longer knows exactly what it is missing.
func (t *Tracker) Trust(publisher string) event.TrustState {
	t.mu.RLock()
	defer t.mu.RUnlock()

	st, known := t.pubs[publisher]
	if !known {
		return event.TrustComplete
	}
	if st.Gaps.Count() > 0 || st.Gaps.Truncated() {
		return event.TrustSuspect
	}
	return event.TrustComplete
}

// ClearGaps resets gap state for a publisher, called on snapshot completion.
// A completed snapshot means the publisher retransmitted its whole state, so
// what was missed before it no longer affects the oracle.
func (t *Tracker) ClearGaps(publisher string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if st, known := t.pubs[publisher]; known {
		st.Gaps.Clear()
	}
}

// Reset drops all state. Used on source reconnect when replay is unavailable,
// where every publisher's continuity is broken and pretending otherwise would
// manufacture gaps that never happened.
func (t *Tracker) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.pubs = make(map[string]*PublisherState)
}

// Evictions returns how many publishers were dropped to stay within
// MaxPublishers. A non-zero value means coverage is incomplete.
func (t *Tracker) Evictions() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.evictions
}
