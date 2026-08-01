package controller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	"github.com/nabrahma/driftwatch/api/v1alpha1"
	"github.com/nabrahma/driftwatch/pkg/check"
)

// The registry's own tests run against a stub runnable rather than a real
// check, because what is under test here is the supervision — restart, panic
// isolation, shutdown deadlines — and a real check would only make it harder to
// arrange the state each of those needs. The reconciler tests in
// driftcheck_controller_test.go drive real checks end to end.

// stubRunnable is a check that does exactly what a test needs.
type stubRunnable struct {
	// block, if set, is what Run waits on as well as the context. It is how the
	// mid-bootstrap deletion test makes a runner that is genuinely busy.
	block chan struct{}
	// ignoreCancel makes Run wait on block alone, modeling a check that does
	// not honor cancellation. Only the shutdown-grace test wants it.
	ignoreCancel bool
	// panicOnRun makes Run panic, for the isolation test.
	panicOnRun bool
	// runErr is returned from Run once the context is done.
	runErr error

	// statusMu guards status. The reconciler reads it while a test is writing
	// to it, which is the same concurrency a real check has and the same
	// concurrency -race would otherwise flag here.
	statusMu sync.Mutex
	status   check.Status

	started  atomic.Int64
	finished atomic.Int64
	closed   atomic.Int64
	ctxDone  atomic.Bool
}

func (s *stubRunnable) Run(ctx context.Context) error {
	s.started.Add(1)
	defer s.finished.Add(1)

	if s.panicOnRun {
		panic("projection folded a nil map")
	}

	if s.ignoreCancel {
		<-s.block
		return s.runErr
	}

	if s.block != nil {
		// A runner that is busy doing something long, plus the context — so the
		// test can prove cancellation is what ends it.
		select {
		case <-s.block:
		case <-ctx.Done():
			s.ctxDone.Store(true)
		}
		return s.runErr
	}

	<-ctx.Done()
	s.ctxDone.Store(true)
	return s.runErr
}

func (s *stubRunnable) Status() check.Status {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()
	return s.status
}

// setStatus mutates the reported status under the same lock Status reads it.
func (s *stubRunnable) setStatus(mutate func(*check.Status)) {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()
	mutate(&s.status)
}

func (s *stubRunnable) Close() error {
	s.closed.Add(1)
	return nil
}

// testRegistry builds a registry whose builder hands out stubs.
//
// The builder records every stub it made, so a test can assert on how many were
// ever constructed — which is how "a spec change never leaves two runners" is
// checked from the other side: not just that the map holds one, but that the
// ones it does not hold were stopped and closed.
type testRegistry struct {
	*RunnerRegistry

	mu    sync.Mutex
	built []*stubRunnable
	// configure customizes each stub as it is built.
	configure func(spec check.Spec, s *stubRunnable)
}

func newTestRegistry(t *testing.T, opts RegistryOptions) *testRegistry {
	t.Helper()

	tr := &testRegistry{}

	// A caller that supplied its own builder wants the real thing — the
	// invalid-spec test needs check.New's validation, which no stub can fake.
	if opts.Build == nil {
		opts.Build = func(spec check.Spec, _ check.Deps) (Runnable, error) {
			// Pending, because that is what check.New leaves a freshly built
			// check in. A stub reporting an empty phase would let the
			// reconciler write an empty phase into the CRD and nothing here
			// would notice.
			stub := &stubRunnable{status: check.Status{Phase: check.PhasePending}}

			tr.mu.Lock()
			if tr.configure != nil {
				tr.configure(spec, stub)
			}
			tr.built = append(tr.built, stub)
			tr.mu.Unlock()

			return stub, nil
		}
	}
	tr.RunnerRegistry = NewRunnerRegistry(opts)

	t.Cleanup(func() {
		require.NoError(t, tr.StopAll(context.Background()))
	})
	return tr
}

func (tr *testRegistry) stubs() []*stubRunnable {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]*stubRunnable(nil), tr.built...)
}

func key(name string) types.NamespacedName {
	return types.NamespacedName{Namespace: "inference", Name: name}
}

func specFor(window time.Duration) check.Spec {
	spec := check.Spec{Name: "kvcache-index", Namespace: "inference"}
	spec.Policy.SettlementWindow.Static = check.Duration(window)
	spec.ApplyDefaults()
	return spec
}

// --- The rapid update storm -------------------------------------------------

func TestRegistry_RapidUpdateStormLeavesExactlyOneRunner(t *testing.T) {
	// The test §10.3 asks for by name: 20 sequential updates, registry size 1,
	// and exactly the final hash.
	//
	// The failure it exists to catch is silent. A registry that started the new
	// runner before stopping the old one would leave nineteen orphans, each
	// holding an oracle and a connection pool, each writing metrics under the
	// same check label — and no reconcile would ever find them again to stop
	// them. It looks like a memory leak and a drift count that alternates
	// between two values, and neither symptom points at the registry.
	//
	// So the assertions are on both sides: one runner in the map, and every
	// other runner ever built confirmed stopped and closed.
	ctx := context.Background()
	reg := newTestRegistry(t, RegistryOptions{
		Logger: testLogger(t),
		// Throttling off, so every one of the twenty updates really does
		// restart. With the default 5s debounce this test would pass by
		// coalescing nineteen of them, which proves nothing about the mutex.
		RestartInterval: -1,
	})

	k := key("kvcache-index")
	var finalHash string

	for i := 1; i <= 20; i++ {
		spec := &v1alpha1.DriftCheckSpec{}
		spec.Default()
		spec.Policy.MaxTrackedKeys = 10_000 + i

		hash, err := SpecHash(spec, nil)
		require.NoError(t, err)
		finalHash = hash

		outcome, err := reg.Ensure(ctx, k, hash, specFor(time.Duration(i)*time.Second))
		require.NoError(t, err)

		if i == 1 {
			require.Equal(t, ActionStarted, outcome.Action)
		} else {
			require.Equal(t, ActionRestarted, outcome.Action,
				"update %d should have replaced the previous runner", i)
		}
	}

	require.Equal(t, 1, reg.Len(), "exactly one runner survives the storm")

	runner := reg.Get(k)
	require.NotNil(t, runner)
	assert.Equal(t, finalHash, runner.Hash, "and it is the one from the final spec")

	// The other side of the same guarantee: twenty were built, nineteen were
	// stopped, and none of them is still running.
	stubs := reg.stubs()
	require.Len(t, stubs, 20)

	for i, stub := range stubs[:19] {
		assert.Equal(t, int64(1), stub.finished.Load(),
			"runner %d was left running after being replaced", i+1)
		assert.Equal(t, int64(1), stub.closed.Load(),
			"runner %d was never closed, so its connections leaked", i+1)
	}
	assert.Zero(t, stubs[19].finished.Load(), "the final runner is still running")
}

func TestRegistry_ConcurrentEnsuresNeverLeaveTwoRunners(t *testing.T) {
	// The storm above is sequential, which is what §10.3 asks for — but the
	// reconciler runs with MaxConcurrentReconciles above one, so two reconciles
	// of the same key really can overlap. Under -race this is the test that
	// exercises the per-key mutex rather than merely relying on it.
	ctx := context.Background()
	reg := newTestRegistry(t, RegistryOptions{
		Logger:          testLogger(t),
		RestartInterval: -1,
	})

	k := key("kvcache-index")

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := reg.Ensure(ctx, k, fmt.Sprintf("hash-%d", i%4),
				specFor(time.Duration(i+1)*time.Second))
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()

	assert.Equal(t, 1, reg.Len())

	running := 0
	for _, stub := range reg.stubs() {
		if stub.started.Load() > 0 && stub.finished.Load() == 0 {
			running++
		}
	}
	assert.Equal(t, 1, running, "exactly one runner is still executing")
}

// --- Deleting a check mid-bootstrap -----------------------------------------

func TestRegistry_DeleteMidBootstrapTerminatesWithinTwoSeconds(t *testing.T) {
	// §10.3's second named test. A bootstrap scan of a large keyspace runs for
	// minutes, and a delete that waited for it to finish would leave `kubectl
	// delete driftcheck` hanging — so the operator would reach for
	// --force --grace-period=0, which strips the finalizer and leaves the
	// runner alive with nothing left to stop it.
	//
	// The 2s budget is wall-clock and measured with the real clock on purpose:
	// the property under test is that cancellation is what ends the runner, and
	// a fake clock would let a broken implementation pass by never actually
	// waiting for anything.
	ctx := context.Background()
	reg := newTestRegistry(t, RegistryOptions{Logger: testLogger(t)})

	// A runner that is busy in a scan and will not finish on its own.
	blocked := make(chan struct{})
	t.Cleanup(func() { close(blocked) })

	reg.configure = func(_ check.Spec, s *stubRunnable) { s.block = blocked }

	k := key("kvcache-index")
	_, err := reg.Ensure(ctx, k, "hash-1", specFor(time.Second))
	require.NoError(t, err)
	require.Equal(t, 1, reg.Len())

	started := time.Now()
	require.NoError(t, reg.Stop(ctx, k))
	elapsed := time.Since(started)

	assert.Less(t, elapsed, 2*time.Second,
		"deleting a check mid-bootstrap must terminate within 2s, took %s", elapsed)
	assert.Zero(t, reg.Len())

	stub := reg.stubs()[0]
	assert.True(t, stub.ctxDone.Load(),
		"the runner ended because its context was canceled, not because the "+
			"scan happened to finish")
	assert.Equal(t, int64(1), stub.closed.Load())

	t.Logf("mid-bootstrap deletion completed in %s", elapsed)
}

// --- Panic isolation ---------------------------------------------------------

func TestRegistry_PanicInOneCheckDoesNotAffectAnother(t *testing.T) {
	// §10.3: a panic or fatal error in one check must not affect others. The
	// test is two checks and an injected panic, because the interesting claim
	// is not that the process survives — it is that the second check is
	// untouched and still supervised.
	ctx := context.Background()
	reg := newTestRegistry(t, RegistryOptions{Logger: testLogger(t)})

	reg.configure = func(spec check.Spec, s *stubRunnable) {
		s.panicOnRun = spec.Name == "poisoned"
	}

	poisoned := types.NamespacedName{Namespace: "inference", Name: "poisoned"}
	healthy := types.NamespacedName{Namespace: "inference", Name: "healthy"}

	poisonedSpec := specFor(time.Second)
	poisonedSpec.Name = "poisoned"
	healthySpec := specFor(time.Second)
	healthySpec.Name = "healthy"

	_, err := reg.Ensure(ctx, poisoned, "hash-p", poisonedSpec)
	require.NoError(t, err)
	_, err = reg.Ensure(ctx, healthy, "hash-h", healthySpec)
	require.NoError(t, err)

	// The poisoned runner's goroutine has to have unwound before its error is
	// observable, and that is the only thing worth waiting for.
	poisonedRunner := reg.Get(poisoned)
	require.NotNil(t, poisonedRunner)
	select {
	case <-poisonedRunner.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("the panicking runner never finished")
	}

	require.ErrorContains(t, poisonedRunner.Err(), "check panicked")
	require.ErrorContains(t, poisonedRunner.Err(), "projection folded a nil map")

	assert.Equal(t, check.PhaseFailed, poisonedRunner.Status().Phase,
		"the panic is reported through status, so it reaches the CRD rather than "+
			"only the manager's log")

	healthyRunner := reg.Get(healthy)
	require.NotNil(t, healthyRunner)
	assert.NoError(t, healthyRunner.Err(), "the second check is untouched")

	select {
	case <-healthyRunner.Done():
		t.Fatal("the healthy runner stopped when the other one panicked")
	default:
	}

	assert.Equal(t, 2, reg.Len(),
		"the failed runner stays registered: the reconciler has to be able to "+
			"find it to write the failure into the object's status")
}

func TestRegistry_AFailedRunnerIsRestartedOnTheNextReconcile(t *testing.T) {
	// A runner that died must not stay dead until somebody edits the spec. The
	// hash is unchanged, so the naive "same hash, do nothing" comparison would
	// leave the check permanently stopped while its status still said Failed.
	ctx := context.Background()
	reg := newTestRegistry(t, RegistryOptions{
		Logger:          testLogger(t),
		RestartInterval: -1,
	})

	failing := true
	reg.configure = func(_ check.Spec, s *stubRunnable) {
		if failing {
			s.panicOnRun = true
		}
	}

	k := key("kvcache-index")
	_, err := reg.Ensure(ctx, k, "hash-1", specFor(time.Second))
	require.NoError(t, err)

	runner := reg.Get(k)
	require.NotNil(t, runner)
	<-runner.Done()
	require.Error(t, runner.Err())

	failing = false
	outcome, err := reg.Ensure(ctx, k, "hash-1", specFor(time.Second))
	require.NoError(t, err)

	assert.Equal(t, ActionRestarted, outcome.Action,
		"the same hash still restarts when the runner behind it is dead")
	assert.NoError(t, reg.Get(k).Err())
}

// --- Spec-hash comparison ----------------------------------------------------

func TestRegistry_UnchangedSpecDoesNotRestart(t *testing.T) {
	// The point of hashing at all. Status is refreshed every 15s, so without
	// this the oracle would be rebuilt four times a minute and the check would
	// never leave Bootstrapping.
	ctx := context.Background()
	reg := newTestRegistry(t, RegistryOptions{Logger: testLogger(t)})

	k := key("kvcache-index")
	spec := specFor(time.Second)

	first, err := reg.Ensure(ctx, k, "hash-1", spec)
	require.NoError(t, err)
	require.Equal(t, ActionStarted, first.Action)

	started := reg.Get(k).StartedAt

	for i := 0; i < 5; i++ {
		outcome, err := reg.Ensure(ctx, k, "hash-1", spec)
		require.NoError(t, err)
		assert.Equal(t, ActionUnchanged, outcome.Action)
	}

	assert.Equal(t, started, reg.Get(k).StartedAt, "the runner was never replaced")
	assert.Len(t, reg.stubs(), 1, "and only one was ever built")
}

func TestRegistry_RestartsAreRateLimited(t *testing.T) {
	// §10.3's debounce: a spec updated ten times in ten seconds must not
	// rebuild the oracle ten times. The old runner keeps serving the old spec
	// meanwhile, which is better than a coverage gap while a new one
	// bootstraps.
	ctx := context.Background()
	clk := newFakeClock()
	reg := newTestRegistry(t, RegistryOptions{
		Logger:          testLogger(t),
		Clock:           clk,
		RestartInterval: 5 * time.Second,
	})

	k := key("kvcache-index")

	_, err := reg.Ensure(ctx, k, "hash-1", specFor(time.Second))
	require.NoError(t, err)

	clk.Advance(time.Second)

	outcome, err := reg.Ensure(ctx, k, "hash-2", specFor(2*time.Second))
	require.NoError(t, err)

	assert.Equal(t, ActionThrottled, outcome.Action)
	assert.Equal(t, 4*time.Second, outcome.RetryAfter,
		"the reconciler is told exactly when to come back")
	assert.Equal(t, "hash-1", reg.Get(k).Hash,
		"the old runner keeps auditing while the restart is held off")

	clk.Advance(5 * time.Second)

	outcome, err = reg.Ensure(ctx, k, "hash-2", specFor(2*time.Second))
	require.NoError(t, err)
	assert.Equal(t, ActionRestarted, outcome.Action)
	assert.Equal(t, "hash-2", reg.Get(k).Hash)
}

func TestRegistry_ForgetClearsTheRateLimit(t *testing.T) {
	// Recreating a deleted check must not be throttled on the strength of a
	// runner that no longer exists — which is what happens when someone deletes
	// a DriftCheck and immediately reapplies a corrected one.
	ctx := context.Background()
	clk := newFakeClock()
	reg := newTestRegistry(t, RegistryOptions{
		Logger:          testLogger(t),
		Clock:           clk,
		RestartInterval: 5 * time.Second,
	})

	k := key("kvcache-index")

	_, err := reg.Ensure(ctx, k, "hash-1", specFor(time.Second))
	require.NoError(t, err)
	require.NoError(t, reg.Stop(ctx, k))
	reg.Forget(k)

	outcome, err := reg.Ensure(ctx, k, "hash-2", specFor(2*time.Second))
	require.NoError(t, err)
	assert.Equal(t, ActionStarted, outcome.Action)
}

// --- Shutdown ----------------------------------------------------------------

func TestRegistry_StopAllStopsEveryRunnerInParallel(t *testing.T) {
	// §10.3: fifty checks must all stop within the grace period. Serial
	// shutdown would make that fifty grace periods in the worst case, and a pod
	// that overruns its termination grace is SIGKILLed — which skips every
	// Close in the process and leaves fifty connection pools to time out
	// server-side.
	ctx := context.Background()
	reg := newTestRegistry(t, RegistryOptions{Logger: testLogger(t)})

	for i := 0; i < 50; i++ {
		k := types.NamespacedName{Namespace: "inference", Name: fmt.Sprintf("check-%02d", i)}
		_, err := reg.Ensure(ctx, k, "hash", specFor(time.Second))
		require.NoError(t, err)
	}
	require.Equal(t, 50, reg.Len())

	started := time.Now()
	require.NoError(t, reg.StopAll(ctx))
	elapsed := time.Since(started)

	assert.Zero(t, reg.Len())
	for i, stub := range reg.stubs() {
		assert.Equal(t, int64(1), stub.closed.Load(), "check %d was not closed", i)
	}
	t.Logf("50 checks stopped in %s", elapsed)
}

func TestRegistry_ShutdownRefusesLateStarts(t *testing.T) {
	// The bug this was written for is a real one, found by running the manager
	// test under -race: controller-runtime cancels its leader-elected runnables
	// together, so the stopper can finish while a reconcile is still in flight.
	// That reconcile then starts a runner nothing will ever stop — the manager
	// has gone, so no further reconcile arrives — and it goes on sweeping a
	// store for a lease this process no longer holds.
	//
	// StopAll alone cannot prevent it. Only latching the registry closed can.
	ctx := context.Background()
	reg := newTestRegistry(t, RegistryOptions{Logger: testLogger(t)})

	_, err := reg.Ensure(ctx, key("early"), "hash-1", specFor(time.Second))
	require.NoError(t, err)

	require.NoError(t, reg.Shutdown(ctx))
	assert.Zero(t, reg.Len())

	_, err = reg.Ensure(ctx, key("late"), "hash-2", specFor(time.Second))

	require.ErrorIs(t, err, ErrRegistryClosed)
	assert.Zero(t, reg.Len(), "a check started after shutdown would never be stopped")

	for i, stub := range reg.stubs() {
		assert.Equal(t, int64(1), stub.closed.Load(),
			"runnable %d survived shutdown", i)
	}
}

func TestRegistry_ShutdownRacingWithEnsureLeavesNothingRunning(t *testing.T) {
	// The same guarantee under the ordering that actually produced the bug:
	// Ensure and Shutdown overlapping. Whichever wins, nothing may be left
	// running afterwards.
	//
	// Worth running under -race, where the interleaving varies between runs.
	ctx := context.Background()
	reg := newTestRegistry(t, RegistryOptions{Logger: testLogger(t)})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			k := types.NamespacedName{
				Namespace: "inference", Name: fmt.Sprintf("check-%d", i),
			}
			_, err := reg.Ensure(ctx, k, "hash", specFor(time.Second))
			if err != nil {
				assert.ErrorIs(t, err, ErrRegistryClosed)
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, reg.Shutdown(ctx))
	}()

	wg.Wait()

	assert.Zero(t, reg.Len())
	for i, stub := range reg.stubs() {
		assert.Equal(t, int64(1), stub.closed.Load(),
			"runnable %d was left running after shutdown", i)
	}
}

func TestRegistry_StopIsIdempotent(t *testing.T) {
	ctx := context.Background()
	reg := newTestRegistry(t, RegistryOptions{Logger: testLogger(t)})

	k := key("kvcache-index")
	_, err := reg.Ensure(ctx, k, "hash-1", specFor(time.Second))
	require.NoError(t, err)

	require.NoError(t, reg.Stop(ctx, k))
	require.NoError(t, reg.Stop(ctx, k))
	require.NoError(t, reg.Stop(ctx, key("never-existed")))
}

func TestRegistry_ShutdownGraceIsEnforced(t *testing.T) {
	// A runner that ignores cancellation must not hang the manager forever. The
	// registry gives up, says which check it was, and lets the process exit —
	// because a manager that will not shut down is one Kubernetes eventually
	// kills without running any Close at all.
	clk := newFakeClock()
	reg := newTestRegistry(t, RegistryOptions{
		Logger:        testLogger(t),
		Clock:         clk,
		ShutdownGrace: 30 * time.Second,
	})

	stuck := make(chan struct{})
	t.Cleanup(func() { close(stuck) })

	reg.configure = func(_ check.Spec, s *stubRunnable) {
		s.block, s.ignoreCancel = stuck, true
	}

	ctx := context.Background()
	k := key("stuck")

	_, err := reg.Ensure(ctx, k, "hash-1", specFor(time.Second))
	require.NoError(t, err)

	// Run the stop on its own goroutine so the fake clock can be advanced past
	// the grace period while the wait is in progress.
	errCh := make(chan error, 1)
	go func() { errCh <- reg.Stop(ctx, k) }()

	require.Eventually(t, func() bool {
		clk.Advance(10 * time.Second)
		select {
		case err := <-errCh:
			require.ErrorContains(t, err, "did not stop within 30s")
			require.ErrorContains(t, err, "inference/stuck")
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond)

	assert.Zero(t, reg.Len(),
		"a runner that would not stop is still removed, or the next reconcile "+
			"would find it and treat it as live")
}

func TestRegistry_BuildFailureIsReported(t *testing.T) {
	reg := NewRunnerRegistry(RegistryOptions{
		Logger: testLogger(t),
		Build: func(check.Spec, check.Deps) (Runnable, error) {
			return nil, errors.New("codec: unknown type")
		},
	})

	_, err := reg.Ensure(context.Background(), key("bad"), "hash-1", specFor(time.Second))

	require.ErrorContains(t, err, "building check inference/bad")
	require.ErrorContains(t, err, "codec: unknown type")
	assert.Zero(t, reg.Len(), "a check that could not be built leaves nothing behind")
}

func TestRegistry_TheRealBuilderIsCheckNew(t *testing.T) {
	// The stub above makes every other test in this file independent of
	// pkg/check, which is the point — but it also means nothing here would
	// notice if the production builder stopped working. This is that check.
	reg := NewRunnerRegistry(RegistryOptions{Logger: testLogger(t)})
	t.Cleanup(func() { require.NoError(t, reg.StopAll(context.Background())) })

	spec := specFor(time.Second)
	spec.Source.Type = "memory"
	spec.Target.Type = "memory"
	spec.Projection.Type = "scalar"
	spec.ApplyDefaults()

	ctx := context.Background()
	outcome, err := reg.Ensure(ctx, key("real"), "hash-1", spec)
	require.NoError(t, err)
	assert.Equal(t, ActionStarted, outcome.Action)

	runner := reg.Get(key("real"))
	require.NotNil(t, runner)
	assert.NotEmpty(t, runner.Status().Phase)
}

// --- Spec hashing ------------------------------------------------------------

func TestSpecHash_IsStableAndSensitive(t *testing.T) {
	spec := &v1alpha1.DriftCheckSpec{}
	spec.Default()

	first, err := SpecHash(spec, nil)
	require.NoError(t, err)

	second, err := SpecHash(spec, nil)
	require.NoError(t, err)
	assert.Equal(t, first, second, "the same spec hashes the same way twice")

	changed := spec.DeepCopy()
	changed.Policy.SweepInterval.Duration = 90 * time.Second

	third, err := SpecHash(changed, nil)
	require.NoError(t, err)
	assert.NotEqual(t, first, third, "a changed field changes the hash")
}

func TestSpecHash_CoversTheResolvedSecrets(t *testing.T) {
	// The half that is easy to leave out. A rotated Redis password produces a
	// byte-identical spec: without the secret in the hash the check would keep
	// running against a pool holding the old credentials and report the store
	// as unreachable, until somebody happened to edit the DriftCheck.
	spec := &v1alpha1.DriftCheckSpec{}
	spec.Default()

	before, err := SpecHash(spec, map[string]string{"target.redis.passwordSecretRef": "hunter2"})
	require.NoError(t, err)

	after, err := SpecHash(spec, map[string]string{"target.redis.passwordSecretRef": "hunter3"})
	require.NoError(t, err)

	assert.NotEqual(t, before, after, "rotating the password restarts the check")
}

func TestSpecHash_DoesNotDependOnMapIterationOrder(t *testing.T) {
	// A hash that depended on iteration order would differ between two
	// reconciles of an unchanged object, which is a restart loop that only
	// appears once a check has more than one secret — so it would ship.
	spec := &v1alpha1.DriftCheckSpec{}
	spec.Default()

	secrets := map[string]string{
		"target.redis.passwordSecretRef":   "a",
		"target.redis.tls.caSecretRef":     "b",
		"source.nats.credentialsSecretRef": "c",
	}

	first, err := SpecHash(spec, secrets)
	require.NoError(t, err)

	for i := 0; i < 50; i++ {
		again, err := SpecHash(spec, secrets)
		require.NoError(t, err)
		require.Equal(t, first, again)
	}
}

func TestSpecHash_LeaksNothing(t *testing.T) {
	// The hash goes into log lines. Only a digest of each secret goes in, so a
	// short or guessable password cannot be read back out of one.
	spec := &v1alpha1.DriftCheckSpec{}
	spec.Default()

	hash, err := SpecHash(spec, map[string]string{"target.redis.passwordSecretRef": "hunter2"})
	require.NoError(t, err)

	assert.NotContains(t, hash, "hunter2")
	assert.Len(t, hash, 32)
}

func TestAction_String(t *testing.T) {
	assert.Equal(t, "started", ActionStarted.String())
	assert.Equal(t, "restarted", ActionRestarted.String())
	assert.Equal(t, "throttled", ActionThrottled.String())
	assert.Equal(t, "unchanged", ActionUnchanged.String())
	assert.Equal(t, "unknown", Action(99).String())
}
