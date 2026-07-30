package oracle

import (
	"sync"
	"time"
)

// shard is one lock domain of the oracle.
//
// Sharding by key hash is what lets the single applier goroutine write while
// the sweeper, the extras scanner, the confirmer and the explain engine read.
// No operation ever needs two keys at once, so no code path takes two shard
// locks — which is the whole reason there is no lock ordering to get wrong.
type shard struct {
	mu sync.RWMutex

	entries map[string]*entry

	// buckets indexes keys by coarse settlement time; settled holds keys old
	// enough to be settled under any legal window. See settle.go.
	buckets map[int64]map[string]struct{}
	settled map[string]struct{}

	bucketWidth time.Duration

	// maxKeys is this shard's share of the global key budget. Enforcing the
	// budget per shard keeps eviction shard-local; the shares are distributed
	// so they sum to exactly the global limit rather than rounding up per
	// shard, which would let the total exceed it.
	maxKeys int

	// fresh counts entries whose trust is Complete and whose generation is at
	// or above the current suspect floor. Maintained incrementally so that
	// reporting trust cardinalities does not mean walking every entry.
	fresh     int
	adopted   int
	suspect   int
	truncated int

	evictions uint64
}

func newShard(bucketWidth time.Duration, maxKeys int) *shard {
	return &shard{
		entries:     make(map[string]*entry),
		buckets:     make(map[int64]map[string]struct{}),
		settled:     make(map[string]struct{}),
		bucketWidth: bucketWidth,
		maxKeys:     maxKeys,
	}
}

// countsFor adjusts the cached trust cardinalities when an entry changes.
func (s *shard) forget(e *entry, floor uint64) {
	switch {
	case e.trust == TrustAdopted:
		s.adopted--
	case e.trust == TrustSuspect:
		s.suspect--
	case e.gen >= floor:
		s.fresh--
	}
	if e.truncated {
		s.truncated--
	}
}

func (s *shard) remember(e *entry, floor uint64) {
	switch {
	case e.trust == TrustAdopted:
		s.adopted++
	case e.trust == TrustSuspect:
		s.suspect++
	case e.gen >= floor:
		s.fresh++
	}
	if e.truncated {
		s.truncated++
	}
}

// suspectCount returns how many entries are suspect, counting both the ones
// explicitly marked and the ones the generation floor swept up.
func (s *shard) suspectCount() int {
	return s.suspect + (len(s.entries) - s.adopted - s.suspect - s.fresh)
}

// recount rebuilds the cached trust cardinalities from the entries themselves.
// Used on paths that move many entries at once, where patching the counters
// incrementally is more error-prone than recomputing them.
func (s *shard) recount(floor uint64) {
	s.fresh, s.adopted, s.suspect, s.truncated = 0, 0, 0, 0
	for _, e := range s.entries {
		s.remember(e, floor)
	}
}

// drop removes an entry entirely, including from the settlement index.
func (s *shard) drop(e *entry, floor uint64) {
	s.forget(e, floor)
	s.removeFromIndex(e)
	delete(s.entries, e.key)
}
