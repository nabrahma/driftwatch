package oracle

// ring is a bounded circular buffer of history entries for one key.
//
// It exists so `driftwatch explain <key>` can replay what was observed, and it
// is bounded because a per-key list of every event ever seen is an
// out-of-memory kill in a system that emits millions of events per key per day.
//
// It grows lazily rather than allocating its full capacity up front, which
// matters more than it looks. A history entry is roughly 300 bytes, so
// preallocating sixteen of them costs about 5 KB for every key — including the
// overwhelming majority that have only ever seen one or two events. At a
// million keys that difference is gigabytes of mostly-zero memory, and it is
// what took BenchmarkOracleMemory1M from 5.2 GiB to inside its budget.
type ring struct {
	entries []HistoryEntry
	// capacity is the maximum number of entries retained. Zero disables
	// history entirely.
	capacity int
	// next is the write position, and once the buffer is full it is also the
	// index of the oldest entry.
	next int
}

func newRing(capacity int) *ring {
	return &ring{capacity: capacity}
}

// push records one applied event, overwriting the oldest once full.
//
// It takes a pointer because a HistoryEntry is 288 bytes and this runs once per
// event on the applier's hot path; the value has to be copied into the slice
// either way, and passing it by value copies it twice.
func (r *ring) push(h *HistoryEntry) {
	if r.capacity == 0 {
		return
	}
	if len(r.entries) < r.capacity {
		r.entries = append(r.entries, *h)
		r.next = len(r.entries) % r.capacity
		return
	}
	r.entries[r.next] = *h
	r.next = (r.next + 1) % r.capacity
}

// snapshot returns the history oldest first, deep copying the values so a
// caller cannot reach back into live state.
func (r *ring) snapshot() []HistoryEntry {
	n := len(r.entries)
	if n == 0 {
		return nil
	}

	// While the buffer is still filling, entries sit in order from index zero.
	// Once it is full, next is the oldest.
	start := 0
	if n == r.capacity {
		start = r.next
	}

	out := make([]HistoryEntry, 0, n)
	for i := 0; i < n; i++ {
		h := r.entries[(start+i)%n]
		h.ResultValue = h.ResultValue.Clone()
		out = append(out, h)
	}
	return out
}
