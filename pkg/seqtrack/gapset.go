package seqtrack

import (
	"sort"
	"strconv"
)

// defaultMaxIntervals bounds the interval list when the caller does not.
const defaultMaxIntervals = 1024

// Interval is an inclusive range of missing sequence numbers.
type Interval struct {
	From, To uint64
}

// Count returns how many sequence numbers the interval covers.
func (i Interval) Count() uint64 { return i.To - i.From + 1 }

// String renders the interval as [from,to].
func (i Interval) String() string {
	return "[" + strconv.FormatUint(i.From, 10) + "," + strconv.FormatUint(i.To, 10) + "]"
}

// GapSet is an interval set over uint64 sequence numbers.
//
// Intervals rather than a per-sequence bitmap, because a publisher that
// restarts after emitting ten million events leaves a ten-million-wide hole,
// and that must cost one entry rather than ten million bits.
//
// The interval count is capped. Unbounded gap tracking is a memory-exhaustion
// vector under a flapping publisher, which is exactly the condition under which
// driftwatch most needs to stay up. When the cap is reached the two closest
// intervals are merged, which over-approximates coverage — Contains may report
// a sequence as missing that was never missing — but never loses one. Losing a
// gap would let driftwatch claim a complete view it does not have, and that is
// the one error this package exists to prevent.
type GapSet struct {
	// intervals is sorted by From, non-overlapping and non-adjacent.
	intervals    []Interval
	maxIntervals int
	truncated    bool
}

// NewGapSet returns an empty set holding at most maxIntervals intervals. A
// non-positive maxIntervals uses the default.
func NewGapSet(maxIntervals int) *GapSet {
	if maxIntervals <= 0 {
		maxIntervals = defaultMaxIntervals
	}
	return &GapSet{maxIntervals: maxIntervals}
}

// Add records [from,to] as missing, coalescing with any overlapping or adjacent
// interval. It panics if from > to.
func (g *GapSet) Add(from, to uint64) {
	if from > to {
		panic("seqtrack: GapSet.Add called with from > to")
	}

	// Find every interval that touches or abuts [from,to] and absorb it. Two
	// intervals are adjacent when one ends immediately before the other begins,
	// and adjacent intervals must merge because no sequence sits between them.
	lo := sort.Search(len(g.intervals), func(i int) bool {
		// Guard the subtraction: from == 0 has nothing below it to abut.
		if from == 0 {
			return g.intervals[i].To >= from
		}
		return g.intervals[i].To >= from-1
	})
	hi := lo
	for hi < len(g.intervals) {
		start := g.intervals[hi].From
		if to != ^uint64(0) && start > to+1 {
			break
		}
		if to == ^uint64(0) {
			// Everything at or above from is absorbed.
			hi = len(g.intervals)
			break
		}
		hi++
	}

	merged := Interval{From: from, To: to}
	if lo < hi {
		if g.intervals[lo].From < merged.From {
			merged.From = g.intervals[lo].From
		}
		if g.intervals[hi-1].To > merged.To {
			merged.To = g.intervals[hi-1].To
		}
	}

	// Splice merged in place of intervals[lo:hi] without a temporary slice.
	// Add runs on the ingest path during exactly the condition driftwatch
	// exists to detect, so it must not allocate per event.
	switch {
	case hi > lo:
		g.intervals[lo] = merged
		g.intervals = append(g.intervals[:lo+1], g.intervals[hi:]...)
	default:
		g.intervals = append(g.intervals, Interval{})
		copy(g.intervals[lo+1:], g.intervals[lo:])
		g.intervals[lo] = merged
	}
	g.enforceCap()
}

// Fill removes seq from the set, splitting an interval when seq is interior. A
// seq that is not recorded as missing is a no-op.
func (g *GapSet) Fill(seq uint64) {
	i := g.find(seq)
	if i < 0 {
		return
	}

	in := g.intervals[i]
	switch {
	case in.From == in.To:
		g.intervals = append(g.intervals[:i], g.intervals[i+1:]...)
	case seq == in.From:
		g.intervals[i].From = seq + 1
	case seq == in.To:
		g.intervals[i].To = seq - 1
	default:
		// Splitting grows the list, which can trip the cap even though nothing
		// was added. This is the non-obvious case the cap has to survive.
		g.intervals = append(g.intervals, Interval{})
		copy(g.intervals[i+1:], g.intervals[i:])
		g.intervals[i] = Interval{From: in.From, To: seq - 1}
		g.intervals[i+1] = Interval{From: seq + 1, To: in.To}
		g.enforceCap()
	}
}

// Contains reports whether seq is recorded as missing.
//
// After truncation this over-approximates: a sequence inside a merged span
// reports as missing even if it was never missing. Truncated reports whether
// that has happened.
func (g *GapSet) Contains(seq uint64) bool { return g.find(seq) >= 0 }

// Count returns the total number of sequence numbers covered by the set.
//
// After truncation this is an upper bound rather than an exact figure, for the
// same reason as Contains. Over-reporting lost events is the safe direction:
// it costs precision, while under-reporting would cost correctness.
func (g *GapSet) Count() uint64 {
	var total uint64
	for _, in := range g.intervals {
		total += in.Count()
	}
	return total
}

// Intervals returns a copy of the intervals, sorted ascending.
func (g *GapSet) Intervals() []Interval {
	if len(g.intervals) == 0 {
		return nil
	}
	out := make([]Interval, len(g.intervals))
	copy(out, g.intervals)
	return out
}

// Truncated reports whether the interval cap was ever exceeded, meaning
// Contains and Count now over-approximate.
func (g *GapSet) Truncated() bool { return g.truncated }

// Clear empties the set and resets the truncation flag. Called when a publisher
// completes a snapshot cycle and its history stops mattering.
func (g *GapSet) Clear() {
	g.intervals = g.intervals[:0]
	g.truncated = false
}

// find returns the index of the interval containing seq, or -1.
func (g *GapSet) find(seq uint64) int {
	i := sort.Search(len(g.intervals), func(i int) bool {
		return g.intervals[i].To >= seq
	})
	if i < len(g.intervals) && g.intervals[i].From <= seq {
		return i
	}
	return -1
}

// enforceCap merges intervals until the list fits, always choosing the pair
// separated by the smallest hole so the least amount of precision is lost.
func (g *GapSet) enforceCap() {
	for len(g.intervals) > g.maxIntervals {
		best, bestHole := 0, ^uint64(0)
		for i := 0; i+1 < len(g.intervals); i++ {
			hole := g.intervals[i+1].From - g.intervals[i].To
			if hole < bestHole {
				best, bestHole = i, hole
			}
		}

		g.intervals[best].To = g.intervals[best+1].To
		g.intervals = append(g.intervals[:best+1], g.intervals[best+2:]...)
		g.truncated = true
	}
}

// clone returns a deep copy, so a snapshot handed to a caller cannot be mutated
// through the original.
func (g *GapSet) clone() *GapSet {
	out := &GapSet{
		maxIntervals: g.maxIntervals,
		truncated:    g.truncated,
	}
	if len(g.intervals) > 0 {
		out.intervals = make([]Interval, len(g.intervals))
		copy(out.intervals, g.intervals)
	}
	return out
}
