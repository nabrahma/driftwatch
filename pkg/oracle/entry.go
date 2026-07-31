// Package oracle holds driftwatch's independently computed expectation of
// target state (M7).
//
// The oracle is the "notebook" from PRD §0.2: what the target store should
// contain, computed by folding the event stream rather than by reading the
// store. Everything driftwatch reports is a comparison against this.
//
// Four properties make it usable in production rather than only in a test:
//
//   - Sharded, so the single applier goroutine and the concurrent readers do
//     not contend on one lock.
//   - Versioned per key, so a reader can tell whether the value it compared
//     against was superseded while it was reading the target (§5.5).
//   - Bounded, so a keyspace larger than expected degrades coverage instead of
//     killing the process.
//   - Indexed by settlement time, so finding the keys eligible for comparison
//     does not mean scanning a million entries every sweep.
package oracle

import (
	"time"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/seqtrack"
)

// TrustState is re-exported from pkg/event so that callers of the oracle do not
// need to import two packages to name the same concept.
type TrustState = event.TrustState

// The trust states a key can be in.
const (
	// TrustComplete means no known gap affects this key.
	TrustComplete = event.TrustComplete
	// TrustSuspect means a gap was observed that may have affected it.
	TrustSuspect = event.TrustSuspect
	// TrustAdopted means the key was loaded from the target at bootstrap and
	// has never been confirmed by an event.
	TrustAdopted = event.TrustAdopted
)

// Entry is a snapshot of one key's expected state.
//
// Every Entry handed out by the oracle is a deep copy. Returning a reference
// into a shard would be a data race waiting to happen, since the applier
// mutates entries while the sweeper reads them.
type Entry struct {
	Key   string
	Value event.Value

	// Version increments on every applied mutation and is never reused, not
	// even across a delete and a recreate. It is the fence a sweeper uses to
	// detect that the oracle moved while it was reading the target.
	Version uint64

	Trust       TrustState
	LastEventAt time.Time // monotonic local receive time, never a publisher clock
	// LastValueChangeAt is when Value last actually changed, which is not the
	// same as when an event last arrived. A key fed idempotent repeats has a
	// moving LastEventAt and a still LastValueChangeAt, and the difference is
	// what lets a permanently-hot key ever be compared (§5.3).
	LastValueChangeAt time.Time

	LastSeq       uint64
	LastEpoch     uint64
	LastPublisher string

	TTL *time.Duration

	// Truncated records that a bound was hit while computing this value, so the
	// expectation is approximate. A divergence on a truncated key says more
	// about driftwatch's limits than about the target.
	Truncated bool

	CreatedAt time.Time
}

// IsAbsent reports whether the oracle expects this key to be missing from the
// target.
//
// A deleted key stays in the oracle as a tombstone rather than being removed.
// Two things depend on that: the version must survive a delete or fencing
// breaks across it, and the differ needs to distinguish "the target should not
// have this key" from "driftwatch has never heard of this key" — the first is
// an extra_in_target finding and the second is not a finding at all.
func (e *Entry) IsAbsent() bool { return e.Value.IsAbsent() }

// HistoryEntry is one step in a key's observed history, kept for `explain`.
type HistoryEntry struct {
	Event       event.Event
	Verdict     seqtrack.Verdict
	ResultValue event.Value
	Version     uint64
	AppliedAt   time.Time
}

// ApplyResult reports what one Apply did.
type ApplyResult struct {
	Key     string
	Version uint64
	Created bool
	Deleted bool
	// Evicted names a key dropped to make room for this one, and DidEvict
	// reports whether one was. Both are needed because the empty string is a
	// legal Redis key, so Evicted alone cannot distinguish "evicted the empty
	// key" from "evicted nothing". An eviction means coverage just shrank.
	Evicted  string
	DidEvict bool
	// Applied reports whether the mutation changed anything. A no-op mutation
	// neither bumps the version nor restarts the settlement window.
	Applied bool
}

// Counts reports cardinalities for metrics.
type Counts struct {
	Total    int
	Settled  int
	InFlight int
	// NeverSettled counts keys that have been in flight longer than the
	// never-settled threshold and that the stability-window check could not
	// rescue either, because their value keeps changing. These are the keys
	// driftwatch has never managed to compare — §5.3's documented blind spot,
	// counted rather than left to be discovered.
	NeverSettled int
	ByTrust      map[TrustState]int
	Truncated    int
}

// entry is the internal, mutable form. Only the applier goroutine writes it,
// and only while holding its shard's write lock.
type entry struct {
	key   string
	value event.Value

	version           uint64
	trust             TrustState
	lastEventAt       time.Time
	lastValueChangeAt time.Time

	lastSeq       uint64
	lastEpoch     uint64
	lastPublisher string

	ttl       *time.Duration
	truncated bool
	createdAt time.Time

	// gen is the value of the oracle's suspect generation when this entry was
	// last written. An entry older than the current suspect floor is suspect
	// without anything having touched it, which is what makes marking a million
	// keys an O(1) operation instead of a million writes.
	gen uint64

	// bucket is the settlement bucket this entry currently sits in, so a
	// re-apply can move it without searching.
	bucket int64

	ring *ring
}

// snapshot returns the deep copy handed out to callers.
func (e *entry) snapshot(effectiveTrust TrustState) Entry {
	return Entry{
		Key:         e.key,
		Value:       e.value.Clone(),
		Version:     e.version,
		Trust:       effectiveTrust,
		LastEventAt: e.lastEventAt,

		LastValueChangeAt: e.lastValueChangeAt,
		LastSeq:           e.lastSeq,
		LastEpoch:         e.lastEpoch,
		LastPublisher:     e.lastPublisher,
		TTL:               e.ttl,
		Truncated:         e.truncated,
		CreatedAt:         e.createdAt,
	}
}
