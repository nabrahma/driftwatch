package oracle

// Eviction.
//
// The oracle is bounded because the alternative is an out-of-memory kill, and a
// monitoring tool that dies when the system it watches grows is worse than no
// monitoring tool. On reaching the key budget the oracle drops the
// least-recently-touched key it can find rather than refusing the new one:
// recent keys are the ones a divergence is most likely to concern.
//
// The choice is approximate by design. Exact global LRU would need an ordered
// structure updated on every event and cross-shard coordination to pick a
// global minimum — real cost paid continuously to improve a decision that only
// happens under saturation. The settlement index already groups keys by coarse
// age, so a victim is drawn from the oldest group available.
//
// Every eviction is counted. A non-zero eviction count means coverage_ratio has
// fallen and findings are partial, which the operator has to be told rather
// than left to infer from a quiet dashboard.

// evictOneLocked drops one key to make room and reports which. The shard's
// write lock must be held.
//
// Every "did we find one" answer travels in a bool rather than in an empty
// string, because the empty string is a legal Redis key. Using "" as a sentinel
// here made a shard holding only the empty key decline to evict and exceed the
// key budget — caught by TestProp_MemoryBounded, and pinned by
// TestOracle_TheEmptyKeyIsEvictableLikeAnyOther.
func (s *shard) evictOneLocked(floor uint64) (key string, evicted bool) {
	victim, found := s.oldestIndexed()
	if !found {
		// Defensive. The settlement index is written on every apply, so it
		// should always cover the entry map; falling back to an arbitrary entry
		// keeps the bound enforced rather than letting the shard grow past it
		// on a bookkeeping slip.
		for k := range s.entries {
			victim, found = k, true
			break
		}
	}

	ent, ok := s.entries[victim]
	if !found || !ok {
		return "", false
	}

	s.drop(ent, floor)
	s.evictions++
	return victim, true
}
