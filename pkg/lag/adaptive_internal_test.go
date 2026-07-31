package lag

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/nabrahma/driftwatch/pkg/clock"
)

var testEpoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// TestProp_SettlementWindowIsAlwaysWithinBounds is the M11 property: whatever
// the observation stream, W stays inside its configured range.
//
// It matters because everything downstream trusts W without checking it. A
// window of zero would make every key eligible the instant its event landed,
// turning the whole settlement mechanism off; an unbounded one would mean
// driftwatch never asserts anything at all. Both failures are silent.
//
// This is an in-package test so it can feed the distribution directly. Driving
// it through a real target would test the probe loop instead, which is what the
// other tests are for, and would make the generated observation stream
// impossible to control.
func TestProp_SettlementWindowIsAlwaysWithinBounds(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		minWindow := time.Duration(rapid.IntRange(1, 2000).Draw(t, "minMs")) * time.Millisecond
		maxWindow := minWindow +
			time.Duration(rapid.IntRange(0, 60000).Draw(t, "spanMs"))*time.Millisecond
		safety := float64(rapid.IntRange(1, 10).Draw(t, "safetyFactor"))

		e := New(Config{
			Clock:        clock.Fake(testEpoch),
			MinWindow:    minWindow,
			MaxWindow:    maxWindow,
			SafetyFactor: safety,
			WindowSize:   rapid.IntRange(1, 500).Draw(t, "windowSize"),
			MaxPollDelay: 60 * time.Second,
			Seed:         1,
		})

		require.GreaterOrEqual(t, e.SettlementWindow(), minWindow,
			"the initial window is already out of bounds")
		require.LessOrEqual(t, e.SettlementWindow(), maxWindow)

		now := testEpoch
		rounds := rapid.IntRange(0, 15).Draw(t, "rounds")
		for i := 0; i < rounds; i++ {
			batch := rapid.IntRange(0, 200).Draw(t, "batch")
			for j := 0; j < batch; j++ {
				ms := rapid.IntRange(0, 300_000).Draw(t, "observationMs")
				e.win.record(time.Duration(ms) * time.Millisecond)
			}
			for j := 0; j < rapid.IntRange(0, 20).Draw(t, "timeouts"); j++ {
				e.win.recordTimeout(e.cfg.MaxPollDelay)
			}

			now = now.Add(time.Duration(rapid.IntRange(0, 300).Draw(t, "gapSeconds")) * time.Second)
			got := e.Recompute(now)

			require.GreaterOrEqual(t, got, minWindow, "W fell below the floor in round %d", i)
			require.LessOrEqual(t, got, maxWindow, "W rose above the ceiling in round %d", i)
			require.Equal(t, got, e.SettlementWindow(),
				"Recompute and SettlementWindow disagree in round %d", i)
		}
	})
}

// TestProp_TimedOutObservationsNeverLowerThePercentile is the statistical heart
// of D-008 stated as a property: adding a timeout can only move the p99 up.
//
// If it could move it down, the estimate would get more optimistic as the
// materializer got worse, which is the exact failure the discarding bug
// produces.
func TestProp_TimedOutObservationsNeverLowerThePercentile(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		w := newWindow(rapid.IntRange(10, 500).Draw(t, "windowSize"))

		n := rapid.IntRange(1, 300).Draw(t, "observations")
		for i := 0; i < n; i++ {
			ms := rapid.IntRange(0, 5000).Draw(t, "observationMs")
			w.record(time.Duration(ms) * time.Millisecond)
		}

		sorted, _, _ := w.snapshot()
		before := percentile(sorted, 0.99)

		const deadline = 60 * time.Second
		w.recordTimeout(deadline)

		sorted, timedOut, _ := w.snapshot()
		after := percentile(sorted, 0.99)

		require.Equal(t, 1, timedOut)
		require.GreaterOrEqual(t, after, before,
			"recording a timeout lowered p99 from %s to %s", before, after)
	})
}

func TestPercentile(t *testing.T) {
	tests := []struct {
		name   string
		sorted []time.Duration
		p      float64
		want   time.Duration
	}{
		{name: "an empty distribution has no percentile", p: 0.99, want: 0},
		{
			name:   "a single observation is every percentile",
			sorted: []time.Duration{5 * time.Second},
			p:      0.99,
			want:   5 * time.Second,
		},
		{
			name: "nearest-rank never interpolates a value that was not observed",
			sorted: []time.Duration{
				1 * time.Second, 2 * time.Second, 3 * time.Second,
				4 * time.Second, 5 * time.Second,
			},
			p:    0.50,
			want: 3 * time.Second,
		},
		{
			name: "p99 of a hundred observations is the largest",
			sorted: func() []time.Duration {
				out := make([]time.Duration, 100)
				for i := range out {
					out[i] = time.Duration(i+1) * time.Millisecond
				}
				return out
			}(),
			p:    0.99,
			want: 99 * time.Millisecond,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, percentile(tc.sorted, tc.p))
		})
	}
}

func TestWindow_IsARingRatherThanAnUnboundedHistory(t *testing.T) {
	// It runs for the life of the process, so an unbounded history would be the
	// unbounded collection §19.2 forbids.
	w := newWindow(10)

	for i := 0; i < 1000; i++ {
		w.record(time.Duration(i) * time.Millisecond)
	}

	sorted, _, total := w.snapshot()
	require.Len(t, sorted, 10, "only the last ten observations are retained")
	require.Equal(t, 1000, total, "but the count of everything seen survives")
	require.Equal(t, 990*time.Millisecond, sorted[0], "the retained ones are the most recent")
}
