package lag_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/lag"
)

// feed drives the estimator to a steady state where every probe converges after
// converging, then returns the resulting W.
//
// It is a helper rather than inline because the hysteresis tests need to do it
// repeatedly with different latencies and compare what W did.
func (h *harness) feed(keys int, converging time.Duration, round int) time.Duration {
	h.t.Helper()

	for i := 0; i < keys; i++ {
		key := "r" + strconv.Itoa(round) + "-k" + strconv.Itoa(i)
		h.est.OfferKey(key)
		h.applyEvent(key, "v")
	}
	h.advance(converging)
	for i := 0; i < keys; i++ {
		h.materialize("r"+strconv.Itoa(round)+"-k"+strconv.Itoa(i), "v")
	}
	h.advance(2 * converging)

	return h.est.SettlementWindow()
}

func TestAdaptive_HysteresisIgnoresASmallShiftAndActsOnALargeOne(t *testing.T) {
	// W must not oscillate. The materializer's latency wobbles constantly, and
	// a window that chased every wobble would change the meaning of every sweep
	// while it was happening.
	tests := []struct {
		name       string
		first      time.Duration
		second     time.Duration
		wantChange bool
	}{
		{
			name:       "a ten percent shift is absorbed",
			first:      200 * time.Millisecond,
			second:     220 * time.Millisecond,
			wantChange: false,
		},
		{
			name:       "a thirty percent shift moves the window",
			first:      200 * time.Millisecond,
			second:     600 * time.Millisecond,
			wantChange: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, lag.Config{
				ProbeCount: 1000, MinWindow: 50 * time.Millisecond,
				MaxWindow: 120 * time.Second, SafetyFactor: 3,
				MaxPollDelay: 30 * time.Second, WindowSize: 200,
			})

			before := h.feed(150, tc.first, 1)
			require.Positive(t, before)

			// Past the rate limit, so only the ratio is under test.
			h.advance(2 * time.Minute)

			after := h.feed(150, tc.second, 2)

			if tc.wantChange {
				assert.NotEqual(t, before, after,
					"a %s shift must move W (was %s)", tc.name, before)
				return
			}
			assert.Equal(t, before, after,
				"a small shift must leave W alone (was %s, now %s)", before, after)
		})
	}
}

func TestAdaptive_ChangesAreRateLimited(t *testing.T) {
	// The ratio alone is not enough: a sustained drift in latency would walk W
	// upward in a series of small steps that each individually clear the ratio.
	h := newHarness(t, lag.Config{
		ProbeCount: 1000, MinWindow: 50 * time.Millisecond,
		MaxWindow: 120 * time.Second, SafetyFactor: 3,
		MaxPollDelay: 30 * time.Second, WindowSize: 400,
	})

	first := h.feed(150, 100*time.Millisecond, 1)
	require.Positive(t, first)
	changesAfterFirst := h.est.Stats().Changes

	// A large shift, but immediately after the previous change.
	second := h.feed(150, 900*time.Millisecond, 2)

	assert.Equal(t, first, second,
		"a change inside the rate-limit interval must be deferred, not applied")
	assert.Equal(t, changesAfterFirst, h.est.Stats().Changes)

	// Once the interval has passed the same measurement is allowed through.
	h.advance(2 * time.Minute)
	third := h.feed(150, 900*time.Millisecond, 3)

	assert.NotEqual(t, first, third, "after the interval the change lands")
	assert.Greater(t, h.est.Stats().Changes, changesAfterFirst)
}

func TestAdaptive_EveryChangeIsReported(t *testing.T) {
	// §9 M11 requires every change to be visible. The logger arrives in Phase
	// 5; this is the seam it will hang from, and it has to actually fire.
	type change struct{ old, next time.Duration }
	var changes []change

	h := newHarness(t, lag.Config{
		ProbeCount: 1000, MinWindow: 50 * time.Millisecond,
		MaxWindow: 120 * time.Second, SafetyFactor: 3, MaxPollDelay: 30 * time.Second,
		OnWindowChange: func(old, next time.Duration, _ lag.Stats) {
			changes = append(changes, change{old, next})
		},
	})

	h.feed(150, 200*time.Millisecond, 1)

	require.NotEmpty(t, changes, "the first adaptation must be reported")
	assert.NotEqual(t, changes[0].old, changes[0].next)
}
