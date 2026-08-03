package check_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/source"
	"github.com/nabrahma/driftwatch/pkg/target"
)

func TestCheck_PausedIngestsWithoutSweeping(t *testing.T) {
	// §10.1's `paused`, which exists for the moment an operator needs the
	// alerting to stop without losing the oracle they have built up. Stopping
	// ingestion instead would mean restarting from nothing afterwards, and
	// every key would be suspect for as long as it took to refill.
	clk := clock.Fake(epoch())
	c := newCheckWith(t, strings.Replace(scalarSpec,
		"  bootstrap: Wait", "  bootstrap: Wait\n  paused: true", 1), clk)

	stop := running(t, c)
	defer stop()

	publish(t, c, setEvent("replica-0", 1, "a", "v1"))
	clk.Advance(30 * time.Second) // several sweep intervals

	status := c.Status()
	assert.Equal(t, check.PhasePaused, status.Phase)
	assert.Contains(t, status.Message, "not sweeping")
	assert.Equal(t, 1, status.TrackedKeys, "the oracle keeps filling while paused")
	assert.True(t, status.LastSweepTime.IsZero(), "no sweep ran")
}

func TestCheck_ABootstrapScanThatFailsRetriesRatherThanGivingUp(t *testing.T) {
	// §9 M14's edge case. A store that is down at startup is common and
	// temporary; a check that gave up would need a human to restart it, which
	// is the opposite of what an auditor should need.
	clk := clock.Fake(epoch())
	failures := &atomic.Int64{}
	failures.Store(2)
	flakyScans.Store(failures)

	c := newCheckWith(t, `
source: {type: memory}
projection: {type: scalar}
target: {type: `+flakyTargetName+`}
policy:
  settlementWindow: {mode: static, static: 1s, min: 1s, max: 60s}
  sweepInterval: 10s
  bootstrap: Adopt
`, clk)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	require.Eventually(t, func() bool {
		return strings.Contains(c.Status().Message, "adopt scan failed")
	}, eventuallyFor, eventuallyPoll, "the first failure should be reported, not swallowed")

	assert.Equal(t, check.PhaseBootstrapping, c.Status().Phase,
		"a store that cannot be read yet is not a check that has failed")

	// Each retry waits on the injected clock, so the test drives the backoff
	// rather than waiting it out. Advancing in a loop rather than counting
	// BlockUntil waiters, because the lag estimator and the metrics refresh
	// have tickers registered too: waiting for "a" waiter says nothing about
	// which one.
	require.Eventually(t, func() bool {
		select {
		case <-c.Bootstrapped():
			return true
		default:
			clk.Advance(10 * time.Second)
			return false
		}
	}, 10*time.Second, 5*time.Millisecond,
		"bootstrap never completed after the store recovered")

	assert.Equal(t, int64(0), failures.Load(), "every injected failure was consumed")

	cancel()
	require.NoError(t, <-done)
}

func TestCheck_APanicInOneComponentIsRecoveredAndCounted(t *testing.T) {
	// §19.2 and §12's panics_total. A panic in one goroutine takes the whole
	// process down, and in a multi-check operator that stops every other check
	// too. Recovering, counting and failing this one check is the containment.
	c := newCheck(t, `
source: {type: memory}
projection: {type: `+panicProjectionName+`}
target: {type: memory}
policy:
  settlementWindow: {mode: static, static: 1s, min: 1s, max: 60s}
  sweepInterval: 10s
  bootstrap: Wait
`)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	<-c.Bootstrapped()

	src, ok := c.Source().(*source.MemorySource)
	require.True(t, ok)
	require.True(t, src.PublishPayload([]byte(setEvent("replica-0", 1, "a", "v1"))))

	select {
	case err := <-done:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "panic in ingest")
		assert.Contains(t, err.Error(), "this projection always panics")
	case <-time.After(5 * time.Second):
		t.Fatal("the panic was neither recovered nor propagated")
	}

	assert.Equal(t, check.PhaseFailed, c.Status().Phase,
		"a check whose applier died must not keep reporting clean sweeps")
}

func TestSpec_ExpiryPolicyReachesTheDiffer(t *testing.T) {
	// The policy decides whether a missing key is drift at all (§5.7), so a
	// mapping that silently fell back to Strict would turn every expired key
	// into a finding.
	tests := []struct {
		policy string
		extra  string
		want   differ.ExpiryPolicy
	}{
		{"Strict", "", differ.ExpiryStrict},
		{"Model", "", differ.ExpiryModel},
		{"Ignore", "\n  assumedTTL: 5m", differ.ExpiryIgnore},
	}

	for _, tc := range tests {
		t.Run(tc.policy, func(t *testing.T) {
			spec, err := check.Load(strings.NewReader(
				"source: {type: memory}\npolicy:\n  expiryPolicy: " + tc.policy + tc.extra))
			require.NoError(t, err)
			require.NoError(t, spec.Validate())

			assert.Equal(t, tc.want, spec.DifferOptions().ExpiryPolicy)
		})
	}
}

func TestSpec_FileSpeedIsValidatedAndTranslated(t *testing.T) {
	// §10.1 spells the fastest replay `fast` and pkg/source spells it
	// `asFastAsPossible`. Two vocabularies for one concept is a trap, so the
	// translation is tested rather than assumed.
	tests := []struct {
		speed   string
		want    string
		wantErr string
	}{
		{speed: "", want: "asFastAsPossible"},
		{speed: "fast", want: "asFastAsPossible"},
		{speed: "realtime", want: "realtime"},
		{speed: "2.0", want: "2.0"},
		{speed: "brisk", wantErr: "source.file.speed"},
		{speed: "-1", wantErr: "source.file.speed"},
	}

	for _, tc := range tests {
		name := tc.speed
		if name == "" {
			name = "unset"
		}

		t.Run(name, func(t *testing.T) {
			spec, err := check.Load(strings.NewReader(
				"source:\n  type: file\n  file: {path: events.jsonl, speed: \"" + tc.speed + "\"}"))
			require.NoError(t, err)

			if tc.wantErr != "" {
				require.Error(t, spec.Validate())
				assert.Contains(t, spec.Validate().Error(), tc.wantErr)
				return
			}

			require.NoError(t, spec.Validate())
			assert.Equal(t, tc.want, spec.SourceSettings()["speed"])
		})
	}
}

func TestSpec_LoopingStdinIsRejected(t *testing.T) {
	// stdin cannot be rewound, so a looping replay from it reads the capture
	// once and then spins forever producing nothing.
	spec, err := check.Load(strings.NewReader(
		"source:\n  type: file\n  file: {path: \"-\", loop: true}"))
	require.NoError(t, err)

	require.Error(t, spec.Validate())
	assert.Contains(t, spec.Validate().Error(), "cannot be rewound")
}

// ---------------------------------------------------------------------------
// Test doubles, registered so the real construction path builds them.
// ---------------------------------------------------------------------------

const (
	flakyTargetName     = "test_flaky"
	panicProjectionName = "test_panic"
)

// flakyScans carries the remaining injected scan failures into the registered
// constructor, which the registry gives no other way to configure.
var flakyScans atomic.Pointer[atomic.Int64]

func init() {
	target.Register(flakyTargetName, newFlakyTarget)
	projection.Register(panicProjectionName, newPanicProjection)
}

// flakyTarget is a memory target whose Scan fails a configured number of times,
// standing in for a store that is not up yet when the check starts.
type flakyTarget struct {
	*target.MemoryTarget
	remaining *atomic.Int64
}

func newFlakyTarget(cfg target.Config) (target.Target, error) {
	remaining := flakyScans.Load()
	if remaining == nil {
		remaining = &atomic.Int64{}
	}

	opts := []target.MemoryOption{}
	if cfg.Clock != nil {
		opts = append(opts, target.WithClock(cfg.Clock))
	}
	return &flakyTarget{MemoryTarget: target.NewMemory(opts...), remaining: remaining}, nil
}

func (f *flakyTarget) Scan(ctx context.Context, pattern string, batch int) target.Iterator {
	if f.remaining.Add(-1) >= 0 {
		return failedIterator{}
	}
	f.remaining.Store(0)
	return f.MemoryTarget.Scan(ctx, pattern, batch)
}

type failedIterator struct{}

func (failedIterator) Next(context.Context) bool { return false }
func (failedIterator) Keys() []string            { return nil }
func (failedIterator) Err() error                { return errors.New("the store is not up yet") }
func (failedIterator) Close() error              { return nil }

// panicProjection panics on the first event, so the recovery in Check.guard is
// exercised through the path a real panic would take.
type panicProjection struct{}

func newPanicProjection(map[string]string) (projection.Projection, error) {
	return panicProjection{}, nil
}

func (panicProjection) Name() string      { return panicProjectionName }
func (panicProjection) Commutative() bool { return true }
func (panicProjection) TargetShape() projection.Shape {
	return projection.ShapeScalar
}

func (panicProjection) KeyOwnership() projection.OwnershipModel {
	return projection.OwnershipModel{}
}

func (panicProjection) TargetKey(e *event.Event) (string, error) { return e.Key, nil }

func (panicProjection) Apply(event.Value, *event.Event) (projection.Mutation, error) {
	panic("this projection always panics")
}

func TestCheck_PausingARunningCheckKeepsTheOracle(t *testing.T) {
	// The claim §10.1's pause rests on, and the one that was not true.
	//
	// Pause exists so an operator can silence a check during a deploy without
	// paying to relearn the keyspace afterwards. If it cost the oracle it would
	// be delete with extra steps: everything would be suspect until the stream
	// refreshed it, and §5.2 would be right to say so, because driftwatch
	// really would have missed those events.
	//
	// It did cost the oracle. Paused was read once when the check was built, so
	// the only way to apply it was to build a new check — and the controller
	// restarts a runner on any spec change, which pausing is. The e2e scenario
	// asserting this passed anyway, because Adopt bootstrap re-read the store
	// and trackedKeys came straight back. The keyspace had been discarded and
	// re-adopted from the very store the check is meant to be auditing, and
	// adopted keys are skipped by the sweep until an event refreshes them.
	//
	// So the number the scenario checked recovered while the property it stood
	// for did not. This asserts the property: the same oracle, across the
	// pause, without a restart.
	clk := clock.Fake(epoch())
	c := newCheckWith(t, scalarSpec, clk)

	stop := running(t, c)
	defer stop()

	store, ok := c.Target().(*target.MemoryTarget)
	require.True(t, ok)

	const keys = 5
	for i := 0; i < keys; i++ {
		publish(t, c, setEvent("replica-0", uint64(i+1), keyIdx(i), "v1"))
		store.Seed(map[string][]byte{keyIdx(i): []byte("v1")})
	}
	clk.Advance(2 * time.Second)

	_, err := c.SweepNow(context.Background())
	require.NoError(t, err)

	before := c.Status()
	require.Equal(t, keys, before.TrackedKeys)
	require.Equal(t, check.PhaseWatching, before.Phase)
	require.False(t, before.LastSweepTime.IsZero())

	// Pause. No restart, no new Check, no second bootstrap.
	c.SetPaused(true)

	paused := c.Status()
	assert.Equal(t, check.PhasePaused, paused.Phase)
	assert.Equal(t, keys, paused.TrackedKeys,
		"pausing must not cost the oracle; that is the entire difference "+
			"between pausing a check and deleting one")

	// Ingestion continues, which is the other half. An unpaused check has to
	// resume asserting on a keyspace that is current rather than one it has to
	// relearn, and that only works if events kept folding in throughout.
	publish(t, c, setEvent("replica-0", keys+1, keyIdx(keys), "v1"))
	assert.Equal(t, keys+1, c.Status().TrackedKeys,
		"a paused check keeps ingesting; it stops asserting, not listening")

	// And nothing is swept while paused. The store is changed behind
	// driftwatch's back first, so a sweep that did run would have something to
	// report and the silence means something.
	store.Seed(map[string][]byte{keyIdx(0): []byte("wrong")})

	// One sweep interval, then real time for the Run goroutine to act on it.
	//
	// The waiting is the part that makes this an assertion. Advancing a fake
	// clock in a loop and reading the status straight afterwards proves
	// nothing: the ticks go to a goroutine that may not have been scheduled
	// yet, and the test passes whether or not pause does anything. It did —
	// neutralizing the pause gate entirely left this test green, which is worse
	// than not having written it.
	clk.Advance(10 * time.Second)
	require.Never(t, func() bool {
		return !c.Status().LastSweepTime.Equal(before.LastSweepTime)
	}, time.Second, 10*time.Millisecond,
		"a paused check ran a sweep: lastSweepTime moved")

	during := c.Status()
	assert.Zero(t, during.DivergentKeys,
		"a paused check reported drift, which is what pausing exists to stop")
	assert.Equal(t, check.PhasePaused, during.Phase)

	// Resuming finds the divergence that was there all along, on the oracle it
	// never lost. If pause had discarded it, this key would have been adopted
	// from the store at its wrong value and would agree with it forever.
	c.SetPaused(false)
	assert.Equal(t, check.PhaseWatching, c.Status().Phase)

	// The control arm, and the reason the silence above is evidence. The same
	// clock advance against the same running loop now has to produce a sweep —
	// if it does not, the tick never reached the sweeper and the paused check's
	// stillness was the harness rather than the feature.
	clk.Advance(10 * time.Second)
	require.Eventually(t, func() bool {
		return !c.Status().LastSweepTime.Equal(before.LastSweepTime)
	}, eventuallyFor, eventuallyPoll,
		"an unpaused check did not sweep on its tick either, so the silence "+
			"while paused was never evidence of anything")

	ctx := context.Background()
	rep, err := c.SweepNow(ctx)
	require.NoError(t, err)
	require.Positive(t, rep.Total(),
		"the resumed sweep compared %d keys and found nothing, against a store "+
			"that was changed behind it", rep.KeysCompared)

	// Raised, then confirmed. A finding is a claim about a disagreement that
	// survived a re-read a settlement window later, so one sweep only ever
	// produces a candidate.
	//
	// Asserted on the outcome rather than by draining the queue here, because
	// the resumed Run loop has a confirm ticker of its own and advancing the
	// clock hands it the same candidates. Whichever gets there first is not the
	// property under test.
	clk.Advance(2 * time.Second)
	require.Eventually(t, func() bool {
		c.ConfirmDue(ctx)
		return c.Status().DivergentKeys > 0
	}, eventuallyFor, eventuallyPoll,
		"the store was changed during the pause and the oracle still knows "+
			"what it should hold; resuming has to find it")
}
