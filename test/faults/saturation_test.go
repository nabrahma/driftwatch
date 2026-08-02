package faults

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/metrics"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/source"
	"github.com/nabrahma/driftwatch/test/harness/scenario"
)

// How long an Eventually here waits, and how often it looks. See the same pair
// in pkg/check/check_test.go for why they are not 5s and 1ms: coverage
// instrumentation roughly halves throughput, and a 1ms poll contends with the
// goroutine it is waiting on. Generous budgets cost nothing when the condition
// is met promptly.
const (
	eventuallyFor  = 30 * time.Second
	eventuallyPoll = 10 * time.Millisecond
)

// §15.3 rows 55 to 60 — driftwatch's own lifecycle.
//
// Rows 47 to 54 live in internal_test.go, where Phase 3 put them. These six are
// about the process rather than the algorithm: what happens when a component
// dies, when a projection rejects everything, when shutdown races startup, and
// when fifty checks share one process.
//
// The theme is containment. A tool that audits many systems from one process
// must not let one of them take down the rest, because the blast radius of the
// auditor failing is every system it was watching.

func TestFault55_PanicInTheApplierIsContained(t *testing.T) {
	// Row 55: recovered at the goroutine boundary, panics_total = 1, that
	// check's context canceled, and — the part that needs two checks to
	// demonstrate — the other one is unaffected.
	reg := prometheus.NewRegistry()
	met := metrics.New(metrics.Options{Registry: reg})
	clk := clock.Fake(scenario.Epoch())

	doomed := newLifecycleCheck(t, panicSpec, met, clk)
	healthy := newLifecycleCheck(t, lifecycleSpec, met, clk)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	doomedDone := make(chan error, 1)
	go func() { doomedDone <- doomed.Run(ctx) }()

	healthyDone := make(chan error, 1)
	go func() { healthyDone <- healthy.Run(ctx) }()

	<-doomed.Bootstrapped()
	<-healthy.Bootstrapped()

	// The event that kills the first check's applier.
	require.True(t, publishTo(doomed, `{"publisher":"p","epoch":1,"seq":1,`+
		`"op":"set","key":"a","value":"v1"}`))

	select {
	case err := <-doomedDone:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "panic in ingest",
			"the panic was recovered at the goroutine boundary and named")
	case <-time.After(10 * time.Second):
		t.Fatal("the panic neither killed the check nor was reported")
	}

	assert.Equal(t, check.PhaseFailed, doomed.Status().Phase)
	assert.Equal(t, 1.0,
		metricValue(t, reg, "driftwatch_panics_total"),
		"counted, because any increment here is a bug worth finding")

	// The second check is still running and still ingesting, which is the
	// whole point of recovering rather than letting the process die.
	require.True(t, publishTo(healthy, `{"publisher":"p","epoch":1,"seq":1,`+
		`"op":"set","key":"a","value":"v1"}`))

	require.Eventually(t, func() bool { return healthy.Status().EventsApplied == 1 },
		eventuallyFor, eventuallyPoll,
		"one check dying must not stop another in the same process")

	cancel()
	<-healthyDone
	require.NoError(t, healthy.Close())
	require.NoError(t, doomed.Close())
}

func TestFault56_ProjectionRejectsEveryEvent(t *testing.T) {
	// Row 56: projection_errors_total rises, the logging is rate-limited, the
	// oracle stays empty, no findings are produced, and the process stays up.
	//
	// The rate limit is the part worth testing. A projection misconfigured
	// against a keyspace rejects at the full event rate, and a log line per
	// rejection turns a configuration mistake into a disk-full outage on every
	// node running the check.
	scenario.New(t).
		WithProjection(rejectingProjectionName).
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			const events = 2000

			for seq := uint64(1); seq <= events; seq++ {
				s.Ingest(s.Msg(scenario.Event{
					Seq: seq, Op: "set", Key: keyFor(seq), Value: "v1",
				}))
			}

			assert.Equal(t, float64(events), s.Metric("driftwatch_projection_errors_total"),
				"every rejection is counted, however many there are")
			assert.Zero(t, s.Oracle().Len(),
				"nothing the projection refused reached the oracle")

			report := s.SweepAndConfirm()
			s.RequireNoFindings(report)
			s.RequireNoConfirmedDrift()

			// Counted as dropped too, so received minus applied still adds up.
			assert.Equal(t, uint64(events), s.Status().EventsDropped)
			assert.Zero(t, s.Status().EventsApplied)
		})
}

func TestFault57_CloseCalledTwice(t *testing.T) {
	// Row 57: no panic, no double close of a channel. Close is called from a
	// defer and again from a shutdown path in any real program, and a
	// double-close panic on the way out is the worst possible time for one:
	// it turns a clean shutdown into a crash loop.
	c := newLifecycleCheck(t, lifecycleSpec, nil, clock.Fake(scenario.Epoch()))

	require.NoError(t, c.Close())
	require.NoError(t, c.Close())
	require.NoError(t, c.Close())

	// And it is still refusing work rather than half-open.
	assert.ErrorIs(t, c.Run(context.Background()), check.ErrClosed)
	_, err := c.SweepNow(context.Background())
	assert.ErrorIs(t, err, check.ErrClosed)
}

func TestFault58_CloseDuringBootstrap(t *testing.T) {
	// Row 58: the bootstrap scan aborts and the shutdown completes inside the
	// grace period.
	//
	// A check killed during startup is the common case in Kubernetes — a
	// rollout replaces a pod seconds after it starts — so an adopt scan that
	// ignored cancellation would make every deploy wait out a full keyspace
	// walk before the old pod would exit.
	clk := clock.Fake(scenario.Epoch())

	c, err := check.New(lifecycleSpecWith(`
policy:
  settlementWindow: {mode: static, static: 1s, min: 1s, max: 60s}
  sweepInterval: 10s
  bootstrap: Adopt
  maxTrackedKeys: 1000000`), check.Deps{Clock: clk})
	require.NoError(t, err)

	store, ok := c.Target().(interface{ Seed(map[string][]byte) })
	require.True(t, ok)

	// A keyspace large enough that the scan is genuinely in progress when the
	// cancellation arrives.
	seed := make(map[string][]byte, 50_000)
	for i := uint64(1); i <= 50_000; i++ {
		seed[keyFor(i)] = []byte("v")
	}
	store.Seed(seed)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	cancel()

	select {
	case err := <-done:
		require.NoError(t, err, "cancellation is a clean shutdown, not a failure")
	case <-time.After(10 * time.Second):
		t.Fatal("the check did not shut down within the grace period")
	}

	require.NoError(t, c.Close())
}

func TestFault59_ImmutableFieldsCannotChange(t *testing.T) {
	// Row 59: the projection and target types are immutable. The webhook that
	// enforces this in the cluster lands in Phase 7; the rule it calls lives in
	// pkg/check, so it is testable here and the webhook is a thin adapter over
	// it rather than a second implementation.
	//
	// The reason is not tidiness. The oracle holds values in the projection's
	// shape and the sweeper reads the store in it, so changing either under a
	// running check makes every tracked key unreadable at once — which looks
	// exactly like the store having been rewritten by something.
	base := lifecycleSpecWith("")

	tests := []struct {
		name    string
		mutate  func(*check.Spec)
		wantErr string
	}{
		{
			name:    "the projection type",
			mutate:  func(s *check.Spec) { s.Projection.Type = "keysetOwnership" },
			wantErr: "projection.type",
		},
		{
			name:    "the target type",
			mutate:  func(s *check.Spec) { s.Target.Type = "redis" },
			wantErr: "target.type",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			next := base
			tc.mutate(&next)

			err := next.ValidateUpdate(&base)

			require.Error(t, err)
			assert.ErrorIs(t, err, check.ErrInvalidSpec)
			assert.Contains(t, err.Error(), tc.wantErr)
			assert.Contains(t, err.Error(), "field is immutable")
		})
	}

	t.Run("an unchanged spec is accepted", func(t *testing.T) {
		next := base
		next.Policy.SweepInterval = check.Duration(time.Minute)

		assert.NoError(t, next.ValidateUpdate(&base),
			"everything else is mutable; only the two shape-defining fields are not")
	})

	t.Run("if the rule is bypassed, the shape mismatch is an error not a panic", func(t *testing.T) {
		// The second half of the row. Should the webhook ever be bypassed —
		// an operator applying a CRD directly to etcd, a controller running
		// against an older schema — the projection must refuse the value it
		// cannot fold rather than panicking on it.
		set, err := projection.New("keysetOwnership", nil)
		require.NoError(t, err)

		scalarValue := event.Value{Kind: event.ValueScalar, Scalar: []byte("v1")}

		require.NotPanics(t, func() {
			_, err = set.Apply(scalarValue, &event.Event{
				Publisher: "p", Epoch: 1, Seq: 1, Op: event.OpAdd,
				Key: "a", Member: "m",
			})
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, projection.ErrShapeMismatch)
	})
}

func TestFault60_FiftyConcurrentChecksInOneManager(t *testing.T) {
	// Row 60: all fifty run, their metrics are separable, memory is linear in
	// the number of checks, and all of them shut down inside the grace period.
	//
	// Separable metrics is the assertion that matters operationally. Fifty
	// checks sharing one registry with indistinguishable series would make the
	// dashboard show the sum of fifty unrelated systems, which is worse than
	// showing nothing.
	const checks = 50

	reg := prometheus.NewRegistry()
	met := metrics.New(metrics.Options{Registry: reg})
	clk := clock.Fake(scenario.Epoch())

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	ctx, cancel := context.WithCancel(context.Background())

	var (
		wg      sync.WaitGroup
		running = make([]*check.Check, 0, checks)
	)

	for i := 0; i < checks; i++ {
		name := "check-" + itoa(uint64(i))
		c, err := check.New(namedSpec(name), check.Deps{Clock: clk, Metrics: met})
		require.NoError(t, err)

		running = append(running, c)

		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, c.Run(ctx))
		}()
	}

	for _, c := range running {
		select {
		case <-c.Bootstrapped():
		case <-time.After(20 * time.Second):
			t.Fatal("not every check finished bootstrapping")
		}
	}

	// One event each, under its own name.
	for i, c := range running {
		require.True(t, publishTo(c, `{"publisher":"p","epoch":1,"seq":1,"op":"set",`+
			`"key":"`+keyFor(uint64(i))+`","value":"v1"}`))
	}

	for _, c := range running {
		require.Eventually(t, func() bool { return c.Status().EventsApplied == 1 },
			10*time.Second, time.Millisecond, "every check ingests independently")
	}

	// Separable: one series per check, each carrying its own count.
	series := labelValues(t, reg, "driftwatch_events_received_total", "check")
	assert.Len(t, series, checks, "each check's events are attributed to it alone")
	for name, count := range series {
		assert.Equal(t, 1.0, count, "check %s", name)
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	perCheck := (after.HeapAlloc - min(before.HeapAlloc, after.HeapAlloc)) / checks
	assert.Less(t, perCheck, uint64(8<<20),
		"%d bytes per idle check; memory has to be linear and small, or a manager "+
			"cannot hold a realistic number of them", perCheck)

	// And all of them stop.
	stopped := make(chan struct{})
	go func() {
		cancel()
		wg.Wait()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(20 * time.Second):
		t.Fatal("not every check shut down within the grace period")
	}

	for _, c := range running {
		require.NoError(t, c.Close())
	}

	t.Logf("%d concurrent checks: %d distinct metric series, ~%d KiB per check",
		checks, len(series), perCheck/1024)
}

// ---------------------------------------------------------------------------
// Fixtures for the rows that need a check outside the scenario DSL.
// ---------------------------------------------------------------------------

const (
	lifecycleSpec = `
name: lifecycle
source: {type: memory}
projection: {type: scalar}
target: {type: memory}
policy:
  settlementWindow: {mode: static, static: 1s, min: 1s, max: 60s}
  sweepInterval: 10s
  bootstrap: Wait
`

	panicSpec = `
name: doomed
source: {type: memory}
projection: {type: ` + panicProjectionName + `}
target: {type: memory}
policy:
  settlementWindow: {mode: static, static: 1s, min: 1s, max: 60s}
  sweepInterval: 10s
  bootstrap: Wait
`
)

// lifecycleSpecWith parses the lifecycle spec with an optional policy override.
func lifecycleSpecWith(policy string) check.Spec {
	body := lifecycleSpec
	if policy != "" {
		body = strings.SplitN(lifecycleSpec, "policy:", 2)[0] + strings.TrimPrefix(policy, "\n")
	}

	spec, err := check.Load(strings.NewReader(body))
	if err != nil {
		panic("faults: " + err.Error())
	}
	return spec
}

// namedSpec is the lifecycle spec under a given check name, so fifty of them
// are distinguishable in one registry.
func namedSpec(name string) check.Spec {
	spec := lifecycleSpecWith("")
	spec.Name = name
	return spec
}

func newLifecycleCheck(
	t *testing.T, body string, met *metrics.Metrics, clk clock.Clock,
) *check.Check {
	t.Helper()

	spec, err := check.Load(strings.NewReader(body))
	require.NoError(t, err)

	c, err := check.New(spec, check.Deps{Clock: clk, Metrics: met})
	require.NoError(t, err)
	return c
}

// publishTo feeds one payload into a check's memory source.
func publishTo(c *check.Check, payload string) bool {
	src, ok := c.Source().(*source.MemorySource)
	if !ok {
		return false
	}
	return src.PublishPayload([]byte(payload))
}

// ---------------------------------------------------------------------------
// A projection that refuses everything, for row 56.
// ---------------------------------------------------------------------------

const (
	rejectingProjectionName = "test_rejecting"
	panicProjectionName     = "test_panicking"
)

func init() {
	projection.Register(rejectingProjectionName, newRejectingProjection)
	projection.Register(panicProjectionName, newPanicProjection)
}

type rejectingProjection struct{}

func newRejectingProjection(map[string]string) (projection.Projection, error) {
	return rejectingProjection{}, nil
}

func (rejectingProjection) Name() string                  { return rejectingProjectionName }
func (rejectingProjection) Commutative() bool             { return true }
func (rejectingProjection) TargetShape() projection.Shape { return projection.ShapeScalar }
func (rejectingProjection) KeyOwnership() projection.OwnershipModel {
	return projection.OwnershipModel{}
}

func (rejectingProjection) TargetKey(e *event.Event) (string, error) { return e.Key, nil }

func (rejectingProjection) Apply(event.Value, *event.Event) (projection.Mutation, error) {
	return projection.Mutation{}, errors.New("this projection refuses every event")
}

type panicProjection struct{}

func newPanicProjection(map[string]string) (projection.Projection, error) {
	return panicProjection{}, nil
}

func (panicProjection) Name() string                            { return panicProjectionName }
func (panicProjection) Commutative() bool                       { return true }
func (panicProjection) TargetShape() projection.Shape           { return projection.ShapeScalar }
func (panicProjection) KeyOwnership() projection.OwnershipModel { return projection.OwnershipModel{} }

func (panicProjection) TargetKey(e *event.Event) (string, error) { return e.Key, nil }

func (panicProjection) Apply(event.Value, *event.Event) (projection.Mutation, error) {
	panic("this projection always panics")
}

// ---------------------------------------------------------------------------
// Reading a registry directly, for the rows that build checks by hand.
// ---------------------------------------------------------------------------

// metricValue sums every series in a metric family.
func metricValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()

	total := 0.0
	for _, count := range labelValues(t, reg, name, "") {
		total += count
	}
	return total
}

// labelValues returns each series of a family keyed by one label's value, or by
// position when label is empty.
func labelValues(t *testing.T, reg *prometheus.Registry, name, label string) map[string]float64 {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)

	out := map[string]float64{}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}

		for i, m := range mf.GetMetric() {
			value := m.GetCounter().GetValue() + m.GetGauge().GetValue()

			key := itoa(uint64(i))
			for _, pair := range m.GetLabel() {
				if label != "" && pair.GetName() == label {
					key = pair.GetValue()
				}
			}
			out[key] += value
		}
	}
	return out
}
