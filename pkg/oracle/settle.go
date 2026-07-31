package oracle

import "time"

// The settled/in-flight index.
//
// A key is settled once its most recent event is older than the settlement
// window W, and only settled keys are eligible for divergence assertion (§5.3).
// Finding them must not mean scanning every entry: at a million keys and a
// thirty-second sweep interval, a full scan is a performance bug the benchmark
// will catch.
//
// The index is a coarse ring of time buckets per shard, plus one coalesced set
// for everything old enough to be settled under any legal W. Bucketing keeps
// the per-event cost to a map insert and delete, and the coalesced set keeps
// the number of live buckets bounded no matter how long the process runs.
//
// The coalescing threshold is MaxSettlementWindow rather than the current W on
// purpose. W can be raised at runtime by the adaptive estimator (§5.3), which
// would move keys back into the in-flight state. Anything older than the
// largest W that can ever be configured is settled under every W, so promoting
// it is safe permanently; promoting on the current W would not be.

// bucketOf returns the bucket index a timestamp falls in.
func (s *shard) bucketOf(at time.Time) int64 {
	return at.UnixNano() / int64(s.bucketWidth)
}

// place puts a key into the bucket for at, removing it from its previous one.
func (s *shard) place(e *entry, at time.Time) {
	b := s.bucketOf(at)
	if e.bucket == b && s.inBuckets(e) {
		return
	}
	s.removeFromIndex(e)
	e.bucket = b

	keys, ok := s.buckets[b]
	if !ok {
		keys = map[string]struct{}{}
		s.buckets[b] = keys
	}
	keys[e.key] = struct{}{}
}

// inBuckets reports whether the entry currently sits in a live bucket rather
// than the coalesced settled set.
func (s *shard) inBuckets(e *entry) bool {
	keys, ok := s.buckets[e.bucket]
	if !ok {
		return false
	}
	_, present := keys[e.key]
	return present
}

// removeFromIndex takes a key out of whichever structure holds it.
func (s *shard) removeFromIndex(e *entry) {
	if keys, ok := s.buckets[e.bucket]; ok {
		delete(keys, e.key)
		if len(keys) == 0 {
			delete(s.buckets, e.bucket)
		}
	}
	delete(s.settled, e.key)
}

// promote folds every bucket older than the coalescing horizon into the settled
// set. Amortized over the keys it moves, and it is what keeps the number of
// live buckets proportional to MaxSettlementWindow rather than to uptime.
func (s *shard) promote(now time.Time, maxWindow time.Duration) {
	horizon := s.bucketOf(now.Add(-maxWindow))
	for b, keys := range s.buckets {
		// A bucket is only fully past the horizon once its whole width is,
		// hence the strict inequality on the bucket below the horizon.
		if b >= horizon {
			continue
		}
		for k := range keys {
			s.settled[k] = struct{}{}
		}
		delete(s.buckets, b)
	}
}

// isSettled reports whether an entry may be compared against the target.
//
// The first clause is the settlement window itself: the key has been quiet for
// longer than W, so the materializer has had its grace period (§5.3).
//
// The second is the stability-window check, and it exists because of a blind
// spot the first clause has on its own. A key updated every 100ms with W at 5s
// is permanently in flight and would never be compared at all — a hot key is
// exactly the key most worth auditing, and it would be the one key driftwatch
// silently never looked at.
//
// §5.3 states the rescue as two conditions: the key has been in flight longer
// than the never-settled threshold, and its value has been unchanged for W
// despite events arriving. Both collapse into one comparison. If the value has
// not changed for the whole threshold while events keep arriving, the key has
// necessarily been in flight for that long too, so a single timestamp decides
// it. The collapsed form is the stricter of the two — it waits the full
// threshold for stability where §5.3 asks only for W — which is the safe
// direction: it can delay a comparison, never rush one.
//
// Comparing such a key is sound because W measures the time the materializer
// needs after the value it must write last changed. Idempotent repeats give it
// no new work, so the wait has already been served.
func isSettled(e *entry, now time.Time, w, neverSettled time.Duration) bool {
	if e.lastEventAt.Before(now.Add(-w)) {
		return true
	}
	return !e.lastValueChangeAt.IsZero() && e.lastValueChangeAt.Before(now.Add(-neverSettled))
}

// settledKeys appends the keys settled as of now under window w.
func (s *shard) settledKeys(dst []string, now time.Time, w, neverSettled time.Duration) []string {
	for k := range s.settled {
		dst = append(dst, k)
	}

	for _, keys := range s.buckets {
		for k := range keys {
			e, ok := s.entries[k]
			if !ok {
				continue
			}
			if isSettled(e, now, w, neverSettled) {
				dst = append(dst, k)
			}
		}
	}
	return dst
}

// countSettled returns how many keys are settled as of now, and how many are so
// persistently in flight that the stability check could not rescue them either.
//
// The second number is §5.3's honest blind spot: keys driftwatch has never been
// able to compare. An undocumented blind spot is a lie by omission, so it is
// counted and exported rather than left to be discovered.
func (s *shard) countSettled(now time.Time, w, neverSettled time.Duration) (settled, neverSettledKeys int) {
	settled = len(s.settled)

	horizon := now.Add(-neverSettled)
	for _, keys := range s.buckets {
		for k := range keys {
			e, ok := s.entries[k]
			if !ok {
				continue
			}
			if isSettled(e, now, w, neverSettled) {
				settled++
				continue
			}
			if e.createdAt.Before(horizon) {
				neverSettledKeys++
			}
		}
	}
	return settled, neverSettledKeys
}

// oldestIndexed returns a key from the least recently touched part of the
// index, used to choose an eviction victim.
//
// It is approximate: any key from the coalesced settled set is older than
// MaxSettlementWindow, and within a bucket the choice is arbitrary. Exact LRU
// would need an ordered structure updated on every event, which is real cost
// paid continuously to improve a decision made only under saturation.
//
// The ok return is not decoration. The empty string is a legal Redis key, so
// "" cannot double as "nothing found" — a shard holding only the empty key
// would silently decline to evict and let the bound be exceeded.
func (s *shard) oldestIndexed() (key string, ok bool) {
	for k := range s.settled {
		return k, true
	}

	oldest := int64(1<<63 - 1)
	for b, keys := range s.buckets {
		if b > oldest {
			continue
		}
		for k := range keys {
			oldest, key, ok = b, k, true
			break
		}
	}
	return key, ok
}
