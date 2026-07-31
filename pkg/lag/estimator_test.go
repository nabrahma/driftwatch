package lag_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/lag"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/seqtrack"
	"github.com/nabrahma/driftwatch/pkg/target"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// Third-party, started at package init and never stopped. §16.5 permits
		// an ignore with a reason; none of driftwatch's own are ignored here.
		goleak.IgnoreAnyFunction("github.com/redis/go-redis/v9/internal/pool.startGlobalTimeCache.func1"),
		goleak.IgnoreAnyFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).reaper"),
	)
}

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// harness wires an oracle, a memory target and an estimator over one fake
// clock. Every test in this package drives time by hand; nothing sleeps.
type harness struct {
	t   *testing.T
	clk clock.FakeClock
	orc *oracle.Oracle
	tgt *target.MemoryTarget
	est *lag.Estimator
	now time.Time
}

// newHarness wires an oracle, a memory target and an estimator over a single
// fake clock. Config is by value so each test's copy is independent.
//
//nolint:gocritic // hugeParam: deliberate, see lag.New.
func newHarness(t *testing.T, cfg lag.Config) *harness {
	t.Helper()

	clk := clock.Fake(epoch)
	orc := oracle.New(oracle.Config{Clock: clk, SettlementWindow: 0})
	tgt := target.NewMemory()

	cfg.Oracle = orc
	cfg.Target = tgt
	cfg.Clock = clk
	if cfg.Shape == 0 {
		cfg.Shape = projection.ShapeScalar
	}
	if cfg.Seed == 0 {
		cfg.Seed = 1
	}

	return &harness{t: t, clk: clk, orc: orc, tgt: tgt, est: lag.New(cfg), now: epoch}
}

// applyEvent writes a key into the oracle and tells the estimator about it,
// which is what the applier does for a probe key.
func (h *harness) applyEvent(key, value string) uint64 {
	h.t.Helper()

	e := &event.Event{
		Publisher: "p", Epoch: 1, Seq: 1, Op: event.OpSet,
		Key: key, Value: []byte(value), ObservedAt: h.now,
	}
	res := h.orc.Apply(projection.Mutation{
		Key:    key,
		Action: projection.ActionUpsert,
		Value:  event.Value{Kind: event.ValueScalar, Scalar: []byte(value)},
	}, e, seqtrack.Accept, oracle.TrustComplete)

	h.est.Observe(key, res.Version, h.now)
	return res.Version
}

// materialize applies the value to the target, standing in for the real
// materializer catching up.
func (h *harness) materialize(key, value string) {
	h.t.Helper()
	h.tgt.Seed(map[string][]byte{key: []byte(value)})
}

// advance moves the clock and runs the estimator's poll rounds, exactly as the
// Run loop would, with no wall-clock time passing.
func (h *harness) advance(d time.Duration) {
	h.t.Helper()

	const step = 10 * time.Millisecond
	for elapsed := time.Duration(0); elapsed < d; elapsed += step {
		h.now = h.now.Add(step)
		h.clk.Advance(step)
		h.est.Tick(context.Background(), h.now)
	}
}

func TestEstimator_MeasuresConvergenceTime(t *testing.T) {
	h := newHarness(t, lag.Config{ProbeCount: 10, MaxPollDelay: 5 * time.Second})
	h.est.OfferKey("k")

	h.applyEvent("k", "v1")
	require.Equal(t, 1, h.est.PendingCount())

	// The materializer takes 150ms to catch up.
	h.advance(150 * time.Millisecond)
	h.materialize("k", "v1")
	h.advance(200 * time.Millisecond)

	stats := h.est.Stats()
	require.Equal(t, 1, stats.Observations)
	assert.Zero(t, stats.TimedOut)

	// Polling backs off 10, 20, 40, 80, 160..., so the observation lands on the
	// first poll at or after convergence rather than exactly on it.
	assert.GreaterOrEqual(t, stats.P50, 150*time.Millisecond)
	assert.LessOrEqual(t, stats.P50, 400*time.Millisecond)
	assert.Zero(t, h.est.PendingCount())
}

func TestEstimator_PollingBacksOffRatherThanSpinning(t *testing.T) {
	// §5.3 specifies 10ms, 20ms, 40ms and so on. Without the backoff a probe
	// that takes a minute to converge would cost six thousand reads.
	h := newHarness(t, lag.Config{ProbeCount: 10, MaxPollDelay: 10 * time.Second})
	h.est.OfferKey("k")
	h.applyEvent("k", "v1")

	h.advance(5 * time.Second)

	// 10+20+40+...+1000 capped: about a dozen doublings then one per second.
	assert.Less(t, h.est.Polls(), int64(20),
		"a five-second wait must not cost hundreds of reads")
	assert.Positive(t, h.est.Polls())
}

func TestEstimator_TimedOutProbesAreRecordedNotDiscarded(t *testing.T) {
	// The statistics trap. A probe that never converges is the slowest kind
	// there is, so dropping it removes the right tail of the distribution and
	// makes W too small — which produces false positives exactly when the
	// materializer is struggling. See docs/DISCOVERIES.md D-008.
	h := newHarness(t, lag.Config{ProbeCount: 10, MaxPollDelay: time.Second})
	h.est.OfferKey("k")

	h.applyEvent("k", "never-arrives")
	h.advance(2 * time.Second)

	stats := h.est.Stats()
	assert.Equal(t, 1, stats.Observations,
		"the timed-out probe must appear in the distribution")
	assert.Equal(t, 1, stats.TimedOut)
	assert.Equal(t, time.Second, stats.P99,
		"it must be recorded at the full poll deadline, not at zero and not omitted")
	assert.Zero(t, h.est.PendingCount())
}

func TestEstimator_AbandonsAMeasurementTheOracleMovedPast(t *testing.T) {
	// Neither a fast observation nor a slow one: the target is no longer
	// converging toward the value being measured, so any number recorded here
	// would be fiction.
	h := newHarness(t, lag.Config{ProbeCount: 10, MaxPollDelay: 10 * time.Second})
	h.est.OfferKey("k")

	h.applyEvent("k", "v1")
	h.advance(50 * time.Millisecond)
	h.applyEvent("k", "v2") // the oracle advances

	// Observe replaced the pending measurement, so the first one is gone
	// without being recorded either way.
	h.advance(100 * time.Millisecond)

	stats := h.est.Stats()
	assert.Zero(t, stats.TimedOut)
	assert.LessOrEqual(t, stats.Observations, 1)
}

func TestEstimator_ATargetHoldingTheWrongTypeIsNotALatencyMeasurement(t *testing.T) {
	h := newHarness(t, lag.Config{
		ProbeCount: 10, MaxPollDelay: time.Second, Shape: projection.ShapeSet,
	})
	h.est.OfferKey("k")

	// The oracle expects a set; the target holds a string.
	e := &event.Event{Publisher: "p", Epoch: 1, Seq: 1, Op: event.OpAdd, Key: "k", Member: "m", ObservedAt: h.now}
	res := h.orc.Apply(projection.Mutation{
		Key: "k", Action: projection.ActionUpsert,
		Value: event.Value{Kind: event.ValueSet, Members: map[string]struct{}{"m": {}}},
	}, e, seqtrack.Accept, oracle.TrustComplete)
	h.est.Observe("k", res.Version, h.now)
	h.tgt.Seed(map[string][]byte{"k": []byte("a-string")})

	h.advance(100 * time.Millisecond)

	assert.Zero(t, h.est.Stats().Observations, "a type mismatch is drift, not latency")
	assert.Positive(t, h.est.Abandoned())
}

func TestEstimator_DoesNotAdaptUntilItHasEnoughData(t *testing.T) {
	// A p99 from a handful of samples is noise, and acting on noise is how W
	// starts oscillating.
	h := newHarness(t, lag.Config{
		ProbeCount: 500, MinWindow: time.Second, MaxWindow: 120 * time.Second,
		MaxPollDelay: time.Second,
	})

	for i := 0; i < 50; i++ {
		key := "k" + strconv.Itoa(i)
		h.est.OfferKey(key)
		h.applyEvent(key, "v")
		h.materialize(key, "v")
	}
	h.advance(100 * time.Millisecond)

	stats := h.est.Stats()
	require.Less(t, stats.Observations, 100)
	assert.False(t, stats.Adaptive, "W must not be driven by fewer than 100 observations")
	assert.Equal(t, time.Second, h.est.SettlementWindow(), "it sits on the floor instead")
}

func TestEstimator_AdaptsOnceItHasEnoughData(t *testing.T) {
	h := newHarness(t, lag.Config{
		ProbeCount: 500, MinWindow: 100 * time.Millisecond, MaxWindow: 120 * time.Second,
		SafetyFactor: 3, MaxPollDelay: 5 * time.Second,
	})

	// 150 keys that each take about 100ms to converge.
	for i := 0; i < 150; i++ {
		key := "k" + strconv.Itoa(i)
		h.est.OfferKey(key)
		h.applyEvent(key, "v")
	}
	h.advance(100 * time.Millisecond)
	for i := 0; i < 150; i++ {
		h.materialize("k"+strconv.Itoa(i), "v")
	}
	h.advance(500 * time.Millisecond)

	stats := h.est.Stats()
	require.GreaterOrEqual(t, stats.Observations, 100)
	assert.True(t, stats.Adaptive)

	// W tracks p99 times the safety factor, inside the configured bounds.
	assert.Equal(t, stats.CurrentWindow, h.est.SettlementWindow())
	assert.GreaterOrEqual(t, h.est.SettlementWindow(), 100*time.Millisecond)
	assert.LessOrEqual(t, h.est.SettlementWindow(), 120*time.Second)
}

func TestEstimator_StaticWindowDisablesControlButNotMeasurement(t *testing.T) {
	// Knowing how far a hand-picked W is from the measured truth is exactly the
	// information that would justify changing it.
	static := 7 * time.Second
	h := newHarness(t, lag.Config{
		ProbeCount: 10, Static: &static, MaxPollDelay: time.Second,
	})
	h.est.OfferKey("k")

	h.applyEvent("k", "v")
	h.materialize("k", "v")
	h.advance(200 * time.Millisecond)

	assert.Equal(t, static, h.est.SettlementWindow(), "a static window never moves")
	assert.False(t, h.est.Stats().Adaptive)
	assert.Positive(t, h.est.Stats().Observations, "but the distribution is still measured")
}

func TestEstimator_SettlementWindowIsAlwaysWithinItsBounds(t *testing.T) {
	tests := []struct {
		name       string
		converging time.Duration
		minWindow  time.Duration
		maxWindow  time.Duration
	}{
		{
			name:       "a very fast materializer is floored at the minimum",
			converging: 10 * time.Millisecond,
			minWindow:  2 * time.Second,
			maxWindow:  120 * time.Second,
		},
		{
			name:       "a very slow materializer is clamped at the maximum",
			converging: 3 * time.Second,
			minWindow:  time.Second,
			maxWindow:  2 * time.Second,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, lag.Config{
				ProbeCount: 500, MinWindow: tc.minWindow, MaxWindow: tc.maxWindow,
				SafetyFactor: 3, MaxPollDelay: 10 * time.Second,
			})

			for i := 0; i < 150; i++ {
				key := "k" + strconv.Itoa(i)
				h.est.OfferKey(key)
				h.applyEvent(key, "v")
			}
			h.advance(tc.converging)
			for i := 0; i < 150; i++ {
				h.materialize("k"+strconv.Itoa(i), "v")
			}
			h.advance(2 * tc.converging)

			got := h.est.SettlementWindow()
			assert.GreaterOrEqual(t, got, tc.minWindow)
			assert.LessOrEqual(t, got, tc.maxWindow)
		})
	}
}

func TestEstimator_ClampingAtTheMaximumIsVisible(t *testing.T) {
	// Past the ceiling driftwatch is knowingly using a window it has measured
	// to be too small, so it has to say so rather than quietly carry on.
	h := newHarness(t, lag.Config{
		ProbeCount: 500, MinWindow: time.Second, MaxWindow: 2 * time.Second,
		SafetyFactor: 3, MaxPollDelay: 10 * time.Second,
	})

	for i := 0; i < 150; i++ {
		key := "k" + strconv.Itoa(i)
		h.est.OfferKey(key)
		h.applyEvent(key, "v")
	}
	h.advance(3 * time.Second)
	for i := 0; i < 150; i++ {
		h.materialize("k"+strconv.Itoa(i), "v")
	}
	h.advance(4 * time.Second)

	stats := h.est.Stats()
	assert.True(t, stats.Clamped, "the operator must be told the ceiling was hit")
	assert.Equal(t, 2*time.Second, h.est.SettlementWindow())
}

func TestEstimator_NoProbesDoesNotBusyLoop(t *testing.T) {
	// Fault matrix row 53: an empty oracle must not spin.
	h := newHarness(t, lag.Config{ProbeCount: 200, MinWindow: time.Second})

	h.advance(5 * time.Second)

	assert.Zero(t, h.est.Polls(), "with nothing to measure there is nothing to read")
	assert.Zero(t, h.est.PendingCount())
	assert.Equal(t, time.Second, h.est.SettlementWindow())
	assert.False(t, h.est.Stats().Adaptive)
}

func TestEstimator_ObserveIgnoresKeysThatAreNotProbes(t *testing.T) {
	h := newHarness(t, lag.Config{ProbeCount: 1})
	h.est.OfferKey("watched")

	h.applyEvent("watched", "v")
	h.applyEvent("unwatched", "v")

	assert.Equal(t, 1, h.est.PendingCount())
	assert.True(t, h.est.IsProbe("watched"))
	assert.False(t, h.est.IsProbe("unwatched"))
}

func TestEstimator_ProbeSampleIsBoundedAndRotates(t *testing.T) {
	// A fixed sample drifts toward whichever keys are cold, and cold keys
	// converge instantly — they would measure nothing.
	h := newHarness(t, lag.Config{ProbeCount: 5, ProbeRotation: time.Minute})

	for i := 0; i < 1000; i++ {
		h.est.OfferKey("k" + strconv.Itoa(i))
	}
	assert.Equal(t, 5, h.est.ProbeCount(), "the sample is bounded regardless of offers")

	h.advance(2 * time.Minute)
	assert.LessOrEqual(t, h.est.ProbeCount(), 5)
}

func TestEstimator_RunExitsOnContextCancellation(t *testing.T) {
	h := newHarness(t, lag.Config{ProbeCount: 10})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.est.Run(ctx) }()

	// Wait for Run to register its ticker before canceling, so the test is not
	// racing the goroutine's startup.
	h.clk.BlockUntil(1)
	cancel()

	assert.ErrorIs(t, <-done, context.Canceled)
}

func TestEstimator_ConfigDefaultsProduceAUsableEstimator(t *testing.T) {
	est := lag.New(lag.Config{
		Oracle: oracle.New(oracle.Config{}),
		Target: target.NewMemory(),
		Clock:  clock.Fake(epoch),
	})

	assert.Equal(t, time.Second, est.SettlementWindow(), "the default floor")
	stats := est.Stats()
	assert.Zero(t, stats.Observations)
	assert.False(t, stats.Adaptive)
}
