package seqtrack_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/seqtrack"
)

func iv(from, to uint64) seqtrack.Interval { return seqtrack.Interval{From: from, To: to} }

func TestGapSet_AddCoalescesOverlappingAndAdjacentIntervals(t *testing.T) {
	tests := []struct {
		name string
		adds []seqtrack.Interval
		want []seqtrack.Interval
	}{
		{
			name: "a single add is stored verbatim",
			adds: []seqtrack.Interval{iv(5, 9)},
			want: []seqtrack.Interval{iv(5, 9)},
		},
		{
			name: "a single missing sequence is a one-wide interval",
			adds: []seqtrack.Interval{iv(7, 7)},
			want: []seqtrack.Interval{iv(7, 7)},
		},
		{
			name: "disjoint intervals stay separate and sorted",
			adds: []seqtrack.Interval{iv(20, 25), iv(5, 9)},
			want: []seqtrack.Interval{iv(5, 9), iv(20, 25)},
		},
		{
			name: "adjacent intervals coalesce, since there is no sequence between them",
			adds: []seqtrack.Interval{iv(5, 9), iv(10, 14)},
			want: []seqtrack.Interval{iv(5, 14)},
		},
		{
			name: "adjacent intervals coalesce in either insertion order",
			adds: []seqtrack.Interval{iv(10, 14), iv(5, 9)},
			want: []seqtrack.Interval{iv(5, 14)},
		},
		{
			name: "overlapping intervals coalesce",
			adds: []seqtrack.Interval{iv(5, 12), iv(9, 20)},
			want: []seqtrack.Interval{iv(5, 20)},
		},
		{
			name: "an interval fully inside an existing one changes nothing",
			adds: []seqtrack.Interval{iv(5, 20), iv(9, 12)},
			want: []seqtrack.Interval{iv(5, 20)},
		},
		{
			name: "an interval spanning several existing ones merges them all",
			adds: []seqtrack.Interval{iv(5, 6), iv(10, 11), iv(20, 21), iv(1, 30)},
			want: []seqtrack.Interval{iv(1, 30)},
		},
		{
			name: "adding the same interval twice is idempotent",
			adds: []seqtrack.Interval{iv(5, 9), iv(5, 9)},
			want: []seqtrack.Interval{iv(5, 9)},
		},
		{
			name: "an interval at zero is legal",
			adds: []seqtrack.Interval{iv(0, 0)},
			want: []seqtrack.Interval{iv(0, 0)},
		},
		{
			name: "an interval at the top of the sequence space is legal",
			adds: []seqtrack.Interval{iv(1<<64-3, 1<<64-1)},
			want: []seqtrack.Interval{iv(1<<64-3, 1<<64-1)},
		},
		{
			// The absorb loop compares against to+1, which overflows to zero at
			// the top of the space and would otherwise stop absorbing at the
			// first interval instead of the last.
			name: "an interval reaching the top of the space absorbs everything above its start",
			adds: []seqtrack.Interval{iv(10, 10), iv(20, 20), iv(5, 1<<64-1)},
			want: []seqtrack.Interval{iv(5, 1<<64-1)},
		},
		{
			name: "an interval spanning the entire space absorbs everything",
			adds: []seqtrack.Interval{iv(10, 10), iv(0, 1<<64-1)},
			want: []seqtrack.Interval{iv(0, 1<<64-1)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := seqtrack.NewGapSet(1024)
			for _, in := range tc.adds {
				g.Add(in.From, in.To)
			}

			assert.Equal(t, tc.want, g.Intervals())
			assert.False(t, g.Truncated())
		})
	}
}

func TestGapSet_AddPanicsOnAnInvertedInterval(t *testing.T) {
	g := seqtrack.NewGapSet(1024)

	// Reachable only from a programming error: Observe derives from and to from
	// the same comparison that guarantees from <= to. Malformed wire input is
	// rejected at the codec boundary, so an inversion here means the caller is
	// broken and should fail loudly rather than corrupt the gap set.
	assert.Panics(t, func() { g.Add(9, 5) })
}

func TestGapSet_CountsEveryMissingSequence(t *testing.T) {
	tests := []struct {
		name string
		adds []seqtrack.Interval
		want uint64
	}{
		{name: "an empty set counts nothing", want: 0},
		{name: "a one-wide interval counts one", adds: []seqtrack.Interval{iv(7, 7)}, want: 1},
		{name: "a five-wide interval counts five", adds: []seqtrack.Interval{iv(5, 9)}, want: 5},
		{
			name: "disjoint intervals sum",
			adds: []seqtrack.Interval{iv(5, 9), iv(20, 24)},
			want: 10,
		},
		{
			name: "coalesced intervals are not double counted",
			adds: []seqtrack.Interval{iv(5, 9), iv(7, 12)},
			want: 8,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := seqtrack.NewGapSet(1024)
			for _, in := range tc.adds {
				g.Add(in.From, in.To)
			}

			assert.Equal(t, tc.want, g.Count())
		})
	}
}

func TestGapSet_Contains(t *testing.T) {
	g := seqtrack.NewGapSet(1024)
	g.Add(5, 9)
	g.Add(20, 20)

	tests := []struct {
		name string
		seq  uint64
		want bool
	}{
		{name: "below every interval", seq: 4, want: false},
		{name: "the first sequence of an interval", seq: 5, want: true},
		{name: "inside an interval", seq: 7, want: true},
		{name: "the last sequence of an interval", seq: 9, want: true},
		{name: "between two intervals", seq: 12, want: false},
		{name: "a one-wide interval", seq: 20, want: true},
		{name: "above every interval", seq: 99, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, g.Contains(tc.seq))
		})
	}
}

func TestGapSet_FillRemovesASequenceSplittingTheIntervalWhenItIsInterior(t *testing.T) {
	tests := []struct {
		name      string
		add       seqtrack.Interval
		fill      uint64
		want      []seqtrack.Interval
		wantCount uint64
	}{
		{
			name:      "filling the first sequence shrinks the interval from the left",
			add:       iv(5, 9),
			fill:      5,
			want:      []seqtrack.Interval{iv(6, 9)},
			wantCount: 4,
		},
		{
			name:      "filling the last sequence shrinks the interval from the right",
			add:       iv(5, 9),
			fill:      9,
			want:      []seqtrack.Interval{iv(5, 8)},
			wantCount: 4,
		},
		{
			name:      "filling an interior sequence splits one interval into two",
			add:       iv(5, 9),
			fill:      7,
			want:      []seqtrack.Interval{iv(5, 6), iv(8, 9)},
			wantCount: 4,
		},
		{
			name:      "filling a one-wide interval removes it entirely",
			add:       iv(7, 7),
			fill:      7,
			want:      nil,
			wantCount: 0,
		},
		{
			name:      "filling a sequence that is not missing is a no-op",
			add:       iv(5, 9),
			fill:      42,
			want:      []seqtrack.Interval{iv(5, 9)},
			wantCount: 5,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := seqtrack.NewGapSet(1024)
			g.Add(tc.add.From, tc.add.To)

			g.Fill(tc.fill)

			assert.Equal(t, tc.want, g.Intervals())
			assert.Equal(t, tc.wantCount, g.Count())
			assert.False(t, g.Contains(tc.fill))
		})
	}
}

func TestGapSet_FillOnAnEmptySetIsANoOp(t *testing.T) {
	g := seqtrack.NewGapSet(1024)

	g.Fill(7)

	assert.Empty(t, g.Intervals())
	assert.Equal(t, uint64(0), g.Count())
}

func TestGapSet_TruncationMergesTheClosestIntervalsAndSaysSo(t *testing.T) {
	// Unbounded gap tracking is a memory-exhaustion vector under a flapping
	// publisher, so the interval list is capped. Detail is lost by merging
	// across the smallest hole, never by dropping an interval, so coverage is
	// always over-approximated and a real gap can never disappear.
	g := seqtrack.NewGapSet(3)

	g.Add(0, 0)
	g.Add(10, 10)
	g.Add(20, 20)
	require.Len(t, g.Intervals(), 3)
	require.False(t, g.Truncated())

	// 21 sits one away from 20, the smallest hole in the set, so those two
	// merge rather than any of the wider-spaced pairs.
	g.Add(22, 22)

	assert.True(t, g.Truncated())
	assert.Equal(t, []seqtrack.Interval{iv(0, 0), iv(10, 10), iv(20, 22)}, g.Intervals())
	assert.LessOrEqual(t, len(g.Intervals()), 3)
}

func TestGapSet_TruncationNeverLosesASequenceItWasTrackingAsMissing(t *testing.T) {
	g := seqtrack.NewGapSet(4)

	missing := []uint64{1, 100, 1000, 10000, 100000, 1000000, 10000000}
	for _, seq := range missing {
		g.Add(seq, seq)
	}

	require.True(t, g.Truncated())
	require.LessOrEqual(t, len(g.Intervals()), 4)

	// Every sequence that was ever recorded as missing must still report as
	// missing. Over-reporting after truncation is acceptable; under-reporting
	// would mean driftwatch claims a complete view it does not have.
	for _, seq := range missing {
		assert.True(t, g.Contains(seq), "seq %d was lost by truncation", seq)
	}
	assert.GreaterOrEqual(t, g.Count(), uint64(len(missing)))
}

func TestGapSet_FillCanPushTheIntervalCountBackOverTheCap(t *testing.T) {
	// The non-obvious case: splitting an interval adds one to the count, so a
	// fill can trip the cap even though nothing was added.
	g := seqtrack.NewGapSet(2)
	g.Add(0, 10)
	g.Add(20, 30)
	require.Len(t, g.Intervals(), 2)
	require.False(t, g.Truncated())

	g.Fill(5)

	assert.LessOrEqual(t, len(g.Intervals()), 2)
	assert.True(t, g.Truncated())
}

func TestGapSet_ClearResetsEverythingIncludingTruncation(t *testing.T) {
	g := seqtrack.NewGapSet(2)
	g.Add(0, 0)
	g.Add(10, 10)
	g.Add(20, 20)
	require.True(t, g.Truncated())

	g.Clear()

	assert.Empty(t, g.Intervals())
	assert.Equal(t, uint64(0), g.Count())
	assert.False(t, g.Truncated())
	assert.False(t, g.Contains(10))
}

func TestGapSet_IntervalsReturnsACopyTheCallerCannotUseToCorruptTheSet(t *testing.T) {
	g := seqtrack.NewGapSet(1024)
	g.Add(5, 9)

	got := g.Intervals()
	got[0] = iv(999, 1000)

	assert.Equal(t, []seqtrack.Interval{iv(5, 9)}, g.Intervals())
}

func TestGapSet_ANonPositiveCapFallsBackToTheDefault(t *testing.T) {
	for _, cap := range []int{0, -1} {
		g := seqtrack.NewGapSet(cap)
		for i := uint64(0); i < 100; i++ {
			g.Add(i*2, i*2)
		}
		assert.False(t, g.Truncated(), "cap %d must not truncate at 100 intervals", cap)
	}
}

func TestInterval_StringRendersTheRange(t *testing.T) {
	assert.Equal(t, "[5,9]", iv(5, 9).String())
	assert.Equal(t, "[7,7]", iv(7, 7).String())
}

func TestInterval_Count(t *testing.T) {
	assert.Equal(t, uint64(1), iv(7, 7).Count())
	assert.Equal(t, uint64(5), iv(5, 9).Count())
}
