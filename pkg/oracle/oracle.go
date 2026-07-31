package oracle

import (
	"path"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/seqtrack"
)

// Defaults for Config.
const (
	defaultShards             = 64
	defaultNeverSettledFactor = 10
	defaultMaxTrackedKeys     = 1_000_000
	defaultRingSize           = 16
	defaultBucketWidth        = time.Second
	defaultMaxSettlementWin   = 120 * time.Second
)

// Config configures an Oracle. The zero value is usable.
type Config struct {
	// Shards is the number of independent lock domains. Default 64.
	Shards int
	// MaxTrackedKeys bounds total keys. Default 1,000,000. On reaching it the
	// oracle evicts rather than growing: a keyspace larger than expected must
	// degrade coverage, never kill the process.
	//
	// It is an upper bound, not a capacity to size exactly against. The budget
	// is enforced per shard so that eviction never needs two locks, and hashing
	// does not divide keys evenly: at 64 shards and a million keys the busiest
	// shard runs about 1.6% above the fair share, so roughly 0.3% of keys are
	// evicted while the oracle is globally not full. Allow a few percent of
	// headroom. See docs/DISCOVERIES.md D-003.
	MaxTrackedKeys int
	// RingSize is the per-key event history depth. Default 16.
	RingSize int
	// RetainRaw keeps Event.Raw in the history ring. Default false, and it
	// matters: raw payloads are roughly 2 KB against 200 bytes without, which
	// at sixteen entries across a million keys is 3 GB against 300 MB.
	RetainRaw bool
	// SettlementWindow is W. May be updated at runtime.
	SettlementWindow time.Duration
	// NeverSettledFactor multiplies W to give the threshold past which a
	// permanently in-flight key is rescued by the stability-window check.
	// Default 10, from §5.3.
	NeverSettledFactor int
	// MaxSettlementWindow is the largest W that can ever be configured. It sets
	// the horizon past which a key is settled regardless of W. Default 120s,
	// matching the hard maxW in §5.3.
	MaxSettlementWindow time.Duration
	// BucketWidth is the granularity of the settlement index. Default 1s.
	BucketWidth time.Duration
	// Clock is the injected clock. Defaults to the real one.
	Clock clock.Clock
}

func (c *Config) applyDefaults() {
	if c.Shards <= 0 {
		c.Shards = defaultShards
	}
	if c.NeverSettledFactor <= 0 {
		c.NeverSettledFactor = defaultNeverSettledFactor
	}
	if c.MaxTrackedKeys <= 0 {
		c.MaxTrackedKeys = defaultMaxTrackedKeys
	}
	if c.Shards > c.MaxTrackedKeys {
		// More shards than keys would leave some shards with a budget of zero,
		// which either drops every key that hashes there or lets each shard
		// keep one anyway and blows past the global bound. Neither is
		// acceptable, and a keyspace smaller than the shard count does not need
		// the concurrency in the first place.
		c.Shards = c.MaxTrackedKeys
	}
	if c.RingSize < 0 {
		c.RingSize = defaultRingSize
	}
	if c.RingSize == 0 {
		c.RingSize = defaultRingSize
	}
	if c.MaxSettlementWindow <= 0 {
		c.MaxSettlementWindow = defaultMaxSettlementWin
	}
	if c.BucketWidth <= 0 {
		c.BucketWidth = defaultBucketWidth
	}
	if c.SettlementWindow < 0 {
		c.SettlementWindow = 0
	}
	if c.SettlementWindow > c.MaxSettlementWindow {
		c.SettlementWindow = c.MaxSettlementWindow
	}
	if c.Clock == nil {
		c.Clock = clock.Real()
	}
}

// Oracle holds the expected state of the target store.
type Oracle struct {
	cfg    Config
	shards []*shard

	// settlementWindow is read on every sweep and written by the adaptive
	// estimator, so it is atomic rather than lock-protected.
	settlementWindow atomic.Int64

	// suspectGen and suspectFloor implement O(1) mass suspicion. Marking every
	// key suspect after a sequence gap must not mean writing a million entries:
	// at that cost it would take seconds under a global lock, during exactly
	// the incident where driftwatch needs to stay responsive. Instead each
	// entry records the generation it was written in, and any entry older than
	// the floor is suspect by comparison. See MarkSuspect.
	suspectGen   atomic.Uint64
	suspectFloor atomic.Uint64
}

// New returns an Oracle configured by cfg.
// Config is by value so applyDefaults mutates the copy rather than the
// caller's struct. New is called once per check at startup.
//
//nolint:gocritic // hugeParam: deliberate, see above.
func New(cfg Config) *Oracle {
	cfg.applyDefaults()

	o := &Oracle{cfg: cfg, shards: make([]*shard, cfg.Shards)}
	o.settlementWindow.Store(int64(cfg.SettlementWindow))

	// Distribute the key budget so the per-shard shares sum to exactly
	// MaxTrackedKeys. Rounding up per shard would let the total exceed the
	// configured bound, which invariant I8 forbids.
	base := cfg.MaxTrackedKeys / cfg.Shards
	remainder := cfg.MaxTrackedKeys % cfg.Shards
	for i := range o.shards {
		share := base
		if i < remainder {
			share++
		}
		o.shards[i] = newShard(cfg.BucketWidth, share)
	}
	return o
}

func (o *Oracle) shardFor(key string) *shard {
	return o.shards[xxhash.Sum64String(key)%uint64(len(o.shards))] //nolint:gosec // len(shards) is positive
}

// window returns the current settlement window.
func (o *Oracle) window() time.Duration {
	return time.Duration(o.settlementWindow.Load())
}

// Apply mutates the oracle. Called ONLY from the single applier goroutine.
//
// The single-writer discipline is what makes version bumping and the settlement
// index correct without a lock on the hot path beyond the shard's own. Do not
// call this concurrently.
//
//nolint:gocritic // hugeParam: Mutation is passed by value to match the M7 interface
func (o *Oracle) Apply(m projection.Mutation, e *event.Event, verdict seqtrack.Verdict, trust TrustState) ApplyResult {
	if m.Action == projection.ActionNone {
		return ApplyResult{Key: m.Key}
	}

	now := o.observedAt(e)
	sh := o.shardFor(m.Key)
	floor := o.suspectFloor.Load()
	gen := o.suspectGen.Load()

	sh.mu.Lock()
	defer sh.mu.Unlock()

	res := ApplyResult{Key: m.Key, Applied: true}

	ent, exists := sh.entries[m.Key]
	if !exists {
		if len(sh.entries) >= sh.maxKeys {
			res.Evicted, res.DidEvict = sh.evictOneLocked(floor)
		}
		ent = &entry{key: m.Key, createdAt: now, ring: newRing(o.cfg.RingSize)}
		sh.entries[m.Key] = ent
		sh.remember(ent, floor)
		res.Created = true
	}

	sh.forget(ent, floor)

	// The version is monotonic per key and never reused, including across a
	// delete: a recreated key continues from where it left off, or a sweeper
	// holding a pre-delete version would read a post-recreate value and believe
	// nothing had changed.
	ent.version++
	ent.gen = gen
	ent.trust = trust
	ent.lastEventAt = now
	ent.lastSeq = e.Seq
	ent.lastEpoch = e.Epoch
	ent.lastPublisher = e.Publisher
	ent.truncated = m.Truncated

	// Whether the value actually moved decides whether the materializer has
	// any new work to do, which is what the settlement window is really
	// measuring. A key fed idempotent repeats keeps its old change time and so
	// becomes comparable despite never going quiet (§5.3).
	previous := ent.value

	switch m.Action {
	case projection.ActionUpsert:
		ent.value = m.Value
		ent.ttl = m.TTL
	case projection.ActionDelete:
		// A delete leaves a tombstone rather than removing the entry. The
		// version has to survive for fencing, and the differ needs to tell
		// "the target should not have this key" apart from "driftwatch has
		// never heard of this key".
		ent.value = event.Value{}
		ent.ttl = nil
		res.Deleted = true
	case projection.ActionNone:
	}

	if res.Created || !previous.Equal(ent.value) {
		ent.lastValueChangeAt = now
	}

	sh.remember(ent, floor)
	sh.place(ent, now)

	// The resulting value is stored by reference rather than cloned. Projections
	// return freshly built values and never mutate one they have handed back,
	// and the oracle replaces ent.value rather than editing it in place, so
	// nothing can change underneath the ring. ring.snapshot clones on the way
	// out, which is where a caller could otherwise reach into live state.
	//
	// Cloning here instead would mean a map allocation per event per key —
	// a million of them at a million keys, for a copy nobody ever reads.
	ent.ring.push(&HistoryEntry{
		Event:       o.historyEvent(e),
		Verdict:     verdict,
		ResultValue: ent.value,
		Version:     ent.version,
		AppliedAt:   now,
	})

	res.Version = ent.version
	return res
}

// observedAt returns the local receive time for an event, falling back to the
// clock when the source did not stamp one.
//
// It is deliberately never the publisher's clock. A producer whose clock is
// skewed by more than the settlement window would otherwise make settlement
// decisions unsound in a way that is invisible from the outside (§5.3, F5).
func (o *Oracle) observedAt(e *event.Event) time.Time {
	if !e.ObservedAt.IsZero() {
		return e.ObservedAt
	}
	return o.cfg.Clock.Now()
}

// historyEvent copies the event for the ring, dropping the raw payload unless
// it was asked for.
func (o *Oracle) historyEvent(e *event.Event) event.Event {
	stored := *e
	if !o.cfg.RetainRaw {
		stored.Raw = nil
	}
	return stored
}

// Get returns a snapshot copy of the entry. Safe for concurrent use.
func (o *Oracle) Get(key string) (Entry, bool) {
	sh := o.shardFor(key)
	floor := o.suspectFloor.Load()

	sh.mu.RLock()
	defer sh.mu.RUnlock()

	ent, ok := sh.entries[key]
	if !ok {
		return Entry{}, false
	}
	return ent.snapshot(effectiveTrust(ent, floor)), true
}

// Version returns just the version, cheaply. Used for fencing (§5.5): read it
// before the target, read it again after, and discard the comparison if it
// moved.
func (o *Oracle) Version(key string) (uint64, bool) {
	sh := o.shardFor(key)

	sh.mu.RLock()
	defer sh.mu.RUnlock()

	ent, ok := sh.entries[key]
	if !ok {
		return 0, false
	}
	return ent.version, true
}

// effectiveTrust combines an entry's own trust with the generation floor.
func effectiveTrust(e *entry, floor uint64) TrustState {
	if e.trust == TrustComplete && e.gen < floor {
		return TrustSuspect
	}
	return e.trust
}

// neverSettledThreshold is how long a key may stay in flight before the
// stability-window check tries to rescue it (§5.3, default 10x W).
func (o *Oracle) neverSettledThreshold(w time.Duration) time.Duration {
	d := time.Duration(o.cfg.NeverSettledFactor) * w
	if d > o.cfg.MaxSettlementWindow {
		// The horizon past which promote() has already coalesced everything.
		// A threshold beyond it could never fire, so the check would be
		// decorative rather than merely slow.
		d = o.cfg.MaxSettlementWindow
	}
	return d
}

// SettledKeys returns an iterator over keys settled as of now.
//
// The iterator holds no lock between yields, so callers must use version
// fencing when comparing (§5.5). It snapshots one shard at a time rather than
// the whole oracle, which bounds the transient memory to a shard's worth of
// keys instead of the entire keyspace.
func (o *Oracle) SettledKeys(now time.Time) func(yield func(string) bool) {
	return func(yield func(string) bool) {
		w := o.window()
		neverSettled := o.neverSettledThreshold(w)

		// One buffer, reused across shards and grown to the largest shard seen.
		// Letting append grow it from a small start costs a reallocation per
		// doubling per shard, which at a million keys across 64 shards is
		// several hundred allocations of steadily increasing size — the
		// dominant cost of an otherwise pointer-copying loop.
		buf := make([]string, 0, 1024)

		for _, sh := range o.shards {
			sh.mu.Lock()
			sh.promote(now, o.cfg.MaxSettlementWindow)
			if n := len(sh.entries); cap(buf) < n {
				buf = make([]string, 0, n)
			}
			buf = sh.settledKeys(buf[:0], now, w, neverSettled)
			sh.mu.Unlock()

			for _, k := range buf {
				if !yield(k) {
					return
				}
			}
		}
	}
}

// Counts returns cardinalities for metrics.
func (o *Oracle) Counts(now time.Time) Counts {
	w := o.window()
	neverSettled := o.neverSettledThreshold(w)
	out := Counts{ByTrust: map[TrustState]int{}}

	for _, sh := range o.shards {
		sh.mu.Lock()
		sh.promote(now, o.cfg.MaxSettlementWindow)

		settled, never := sh.countSettled(now, w, neverSettled)
		out.Total += len(sh.entries)
		out.Settled += settled
		out.NeverSettled += never
		out.Truncated += sh.truncated
		out.ByTrust[TrustComplete] += sh.fresh
		out.ByTrust[TrustAdopted] += sh.adopted
		out.ByTrust[TrustSuspect] += sh.suspectCount()
		sh.mu.Unlock()
	}

	out.InFlight = out.Total - out.Settled
	return out
}

// History returns the per-key event ring, oldest first. An unknown or evicted
// key returns nil rather than an error: history is diagnostic, and its absence
// is not a failure.
func (o *Oracle) History(key string) []HistoryEntry {
	sh := o.shardFor(key)

	sh.mu.RLock()
	defer sh.mu.RUnlock()

	ent, ok := sh.entries[key]
	if !ok {
		return nil
	}
	return ent.ring.snapshot()
}

// AdoptSnapshot loads baseline state from the target (bootstrap Adopt mode,
// §5.6).
//
// Adopted keys are marked TrustAdopted: they were read from the target rather
// than derived from events, so they cannot be used to assert the target is
// wrong. They exist so that pre-existing keys are not all reported as
// extra_in_target, and Adopt mode's guarantee is only ever "no new drift since
// I started".
func (o *Oracle) AdoptSnapshot(entries map[string]event.Value, at time.Time) {
	floor := o.suspectFloor.Load()
	gen := o.suspectGen.Load()

	for key, value := range entries {
		sh := o.shardFor(key)

		sh.mu.Lock()
		ent, exists := sh.entries[key]
		if !exists {
			if len(sh.entries) >= sh.maxKeys {
				sh.evictOneLocked(floor)
			}
			ent = &entry{key: key, createdAt: at, ring: newRing(o.cfg.RingSize)}
			sh.entries[key] = ent
			sh.remember(ent, floor)
		}

		sh.forget(ent, floor)
		ent.version++
		ent.gen = gen
		ent.trust = TrustAdopted
		ent.value = value.Clone()
		ent.lastEventAt = at
		sh.remember(ent, floor)
		sh.place(ent, at)
		sh.mu.Unlock()
	}
}

// SetSettlementWindow updates W at runtime (adaptive mode). Values above
// MaxSettlementWindow are clamped, because the settlement index only guarantees
// correctness up to that horizon.
func (o *Oracle) SetSettlementWindow(d time.Duration) {
	if d < 0 {
		d = 0
	}
	if d > o.cfg.MaxSettlementWindow {
		d = o.cfg.MaxSettlementWindow
	}
	o.settlementWindow.Store(int64(d))
}

// SettlementWindow returns the current W.
func (o *Oracle) SettlementWindow() time.Duration { return o.window() }

// MarkSuspect flags keys as untrustworthy after a gap. pattern == "" means all
// keys.
//
// The empty-pattern case is O(1) regardless of how many keys are tracked. It
// advances a generation counter and a floor; every entry written before the
// floor is suspect by comparison, without being touched. The naive
// implementation — walking a million entries under each shard's write lock —
// takes seconds, and it would take them at the exact moment a publisher is
// flapping and the applier most needs to keep up.
//
// A non-empty pattern is a glob matched against every key, so it is O(n). That
// is the price of scoping suspicion to one publisher's partition, and it is
// worth paying: the alternative is marking the whole keyspace suspect and
// suppressing every finding until a snapshot arrives.
func (o *Oracle) MarkSuspect(pattern, reason string) {
	_ = reason // recorded by the caller's metrics and logs, not by the oracle

	if pattern == "" {
		o.suspectFloor.Store(o.suspectGen.Add(1))
		for _, sh := range o.shards {
			sh.mu.Lock()
			// Nothing is at or above the new floor any more.
			sh.fresh = 0
			sh.mu.Unlock()
		}
		return
	}

	floor := o.suspectFloor.Load()
	for _, sh := range o.shards {
		sh.mu.Lock()
		for key, ent := range sh.entries {
			if ent.trust != TrustComplete || !matchGlob(pattern, key) {
				continue
			}
			sh.forget(ent, floor)
			ent.trust = TrustSuspect
			sh.remember(ent, floor)
		}
		sh.mu.Unlock()
	}
}

// ClearSuspect returns keys to Complete after a snapshot cycle. pattern == ""
// means all keys.
//
// Unlike MarkSuspect this walks every entry even for the empty pattern, because
// the per-entry flags set by a pattern mark have to be cleared individually.
// That asymmetry is deliberate: marking happens on every gap and must be free,
// while clearing happens once per snapshot cycle and can afford a pass.
func (o *Oracle) ClearSuspect(pattern string) {
	if pattern == "" {
		o.suspectFloor.Store(0)
	}
	gen := o.suspectGen.Load()
	floor := o.suspectFloor.Load()

	for _, sh := range o.shards {
		sh.mu.Lock()
		for key, ent := range sh.entries {
			if pattern != "" && !matchGlob(pattern, key) {
				continue
			}
			if ent.trust == TrustSuspect {
				ent.trust = TrustComplete
			}
			if ent.trust == TrustComplete {
				// Lift the entry to the current generation so the floor
				// comparison stops reporting it as suspect too. Clearing one
				// mechanism and not the other would restore trust for some keys
				// and silently leave others suspect forever.
				ent.gen = gen
			}
		}
		// Both mechanisms just moved for an arbitrary subset of entries, so the
		// cached cardinalities are rebuilt rather than patched. This path runs
		// once per snapshot cycle and is already O(n); the extra pass removes a
		// whole class of incremental-counter bugs.
		sh.recount(floor)
		sh.mu.Unlock()
	}
}

// Evictions returns how many keys were dropped to stay within MaxTrackedKeys.
// A non-zero value means coverage is incomplete and findings are partial.
func (o *Oracle) Evictions() uint64 {
	var total uint64
	for _, sh := range o.shards {
		sh.mu.RLock()
		total += sh.evictions
		sh.mu.RUnlock()
	}
	return total
}

// Len returns the number of tracked keys, including tombstones.
func (o *Oracle) Len() int {
	n := 0
	for _, sh := range o.shards {
		sh.mu.RLock()
		n += len(sh.entries)
		sh.mu.RUnlock()
	}
	return n
}

// matchGlob reports whether key matches a shell-style pattern. A malformed
// pattern matches nothing rather than panicking; the alternative is a bad
// DriftCheck taking down a check at the first gap.
func matchGlob(pattern, key string) bool {
	ok, err := path.Match(pattern, key)
	return err == nil && ok
}
