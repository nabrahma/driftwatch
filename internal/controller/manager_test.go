package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/nabrahma/driftwatch/api/v1alpha1"
	"github.com/nabrahma/driftwatch/pkg/clock"
)

// The manager tests are the only ones here that run a real controller-runtime
// manager rather than calling Reconcile directly.
//
// They exist for two claims that a direct call cannot make: that the wiring in
// SetupWithManager actually delivers events, and that a manager which has run
// checks and then stopped leaves nothing behind. The second is §20 Phase 7's
// goleak criterion, and it matters more than a leak check usually does: every
// runner owns a goroutine, a target connection pool and a source subscription,
// so a manager that leaked one per check would hold sockets open against a
// store it is no longer auditing.

// managerLeakOptions lists the goroutines that legitimately outlive a manager.
//
// Every entry is a package-level singleton started on first use and never
// stopped by design, so ignoring them is honest rather than convenient. None of
// them belongs to driftwatch: the point of this test is that no *runner*
// survives, and a runner's goroutine sits under supervise, which is not on this
// list.
func managerLeakOptions() []goleak.Option {
	return []goleak.Option{
		// klog's flush daemon: started by the first log line in the process,
		// never stopped.
		goleak.IgnoreTopFunction("k8s.io/klog/v2.(*loggingT).flushDaemon"),
		goleak.IgnoreAnyFunction("k8s.io/klog/v2.(*loggingT).flushDaemon"),
		// The shared HTTP transport's connection pool, kept warm for the next
		// client. envtest's API server connection lives here.
		goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"),
		goleak.IgnoreAnyFunction("internal/poll.runtime_pollWait"),
		// client-go's reflector backoff and its transport cache janitor.
		goleak.IgnoreAnyFunction("k8s.io/apimachinery/pkg/util/wait.BackoffUntil"),
		goleak.IgnoreAnyFunction("k8s.io/apimachinery/pkg/util/wait.JitterUntilWithContext"),
		goleak.IgnoreAnyFunction("k8s.io/apimachinery/pkg/util/wait.loopConditionUntilContext"),
		// The test binary's own machinery.
		goleak.IgnoreTopFunction("testing.(*T).Parallel"),
		goleak.IgnoreCurrent(),
	}
}

// requireNoLeaks fails if anything is still running after the budget.
//
// goleak's own retry window is short, and one goroutine here — client-go's
// workqueue metrics loop — exits a moment after the queue is shut down rather
// than synchronously with it. Ignoring that function outright would blind this
// test to a real leak inside it, so it waits instead.
//
// require.Eventually rather than a sleep loop: §16.4 forbids time.Sleep in
// tests, and this is a genuine wait on another goroutine's scheduling rather
// than an assumption about how long something takes.
func requireNoLeaks(t *testing.T, opts ...goleak.Option) {
	t.Helper()

	var last error
	ok := assert.Eventually(t, func() bool {
		last = goleak.Find(opts...)
		return last == nil
	}, 15*time.Second, 25*time.Millisecond)

	if !ok {
		t.Fatalf("goroutines still running 15s after the manager stopped:\n%v", last)
	}
}

// namespacedCache restricts a manager's cache to one namespace.
func namespacedCache(namespace string) ctrlcache.Options {
	return ctrlcache.Options{
		DefaultNamespaces: map[string]ctrlcache.Config{namespace: {}},
	}
}

func TestManager_RunsChecksAndLeavesNothingBehindAfterTeardown(t *testing.T) {
	// Snapshot first: IgnoreCurrent inside managerLeakOptions captures whatever
	// the shared envtest server and previous tests already had running, so the
	// only goroutines this can fail on are ones this test started.
	ignore := managerLeakOptions()

	ctx, cancel := context.WithCancel(context.Background())

	cl, err := client.New(testCfg, client.Options{Scheme: testScheme})
	require.NoError(t, err)

	namespace := uniqueNamespace(t)
	require.NoError(t, cl.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	}))

	mgr, err := ctrl.NewManager(testCfg, ctrl.Options{
		Scheme: testScheme,
		// No metrics server and no probes: two more listening sockets would add
		// nothing to what this test is proving and would make the leak check
		// depend on how quickly the OS releases a port.
		Metrics:        metricsserver.Options{BindAddress: "0"},
		LeaderElection: false,
		// Scoped to this test's namespace. A cluster-wide cache would pick up
		// every DriftCheck the rest of the suite left behind and start a runner
		// for each, which has nothing to do with what is being tested here —
		// and it exercises the namespace restriction §18 prefers for a real
		// deployment.
		Cache: namespacedCache(namespace),
	})
	require.NoError(t, err)

	registry := newTestRegistry(t, RegistryOptions{
		Logger:        testLogger(t),
		Clock:         clock.Fake(testEpoch),
		ShutdownGrace: 5 * time.Second,
	})

	reconciler := &DriftCheckReconciler{
		Client:                mgr.GetClient(),
		Scheme:                testScheme,
		Clock:                 clock.Fake(testEpoch),
		Runners:               registry.RunnerRegistry,
		Recorder:              mgr.GetEventRecorderFor("driftwatch"),
		Log:                   testLogger(t),
		StatusRefreshInterval: time.Second,
	}
	require.NoError(t, reconciler.SetupWithManager(mgr))
	require.NoError(t, mgr.Add(NewRunnerStopper(registry.RunnerRegistry, testLogger(t))))

	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()

	require.True(t, mgr.GetCache().WaitForCacheSync(ctx), "the cache never synced")

	// Two checks, so teardown has more than one runner to unwind.
	for _, name := range []string{"by-key", "by-count"} {
		dc := &v1alpha1.DriftCheck{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: v1alpha1.DriftCheckSpec{
				Source:     v1alpha1.SourceSpec{Type: "memory"},
				Projection: v1alpha1.ProjectionSpec{Type: "scalar"},
				Target:     v1alpha1.TargetSpec{Type: "memory"},
			},
		}
		dc.Spec.Default()
		require.NoError(t, cl.Create(ctx, dc))
	}

	// The manager delivered the create events and the reconciler acted on them,
	// which is the wiring no direct Reconcile call can prove.
	require.Eventually(t, func() bool { return registry.Len() == 2 },
		30*time.Second, 20*time.Millisecond, "the manager never started both checks")

	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("the manager did not stop")
	}

	assert.Zero(t, registry.Len(),
		"the stopper ran: every runner is gone, not merely unreferenced")
	for i, stub := range registry.stubs() {
		assert.Equal(t, int64(1), stub.closed.Load(), "check %d was not closed", i)
		assert.Equal(t, int64(1), stub.finished.Load(), "check %d is still running", i)
	}

	// goleak last: a manager that leaked a runner per check would hold a target
	// connection pool and a source subscription open against a store it is no
	// longer auditing, and nothing else in this suite would notice.
	requireNoLeaks(t, ignore...)
}

func TestManager_StopperStopsEveryRunnerOnLeadershipLoss(t *testing.T) {
	// §10.3 requires all runners to stop when this manager stops leading.
	// controller-runtime stops delivering reconciles when the lease goes, but a
	// runner is a goroutine this process started: nothing cancels it just
	// because no more reconciles arrive. Two managers both convinced they lead
	// would sweep the same store and patch the same status from two different
	// oracles, and the symptom — a divergent-key count alternating between two
	// values — reads like flapping drift rather than like a split brain.
	ctx, cancel := context.WithCancel(context.Background())

	registry := newTestRegistry(t, RegistryOptions{Logger: testLogger(t)})

	for i := 0; i < 3; i++ {
		_, err := registry.Ensure(context.Background(),
			key("check-"+string(rune('a'+i))), "hash", specFor(time.Second))
		require.NoError(t, err)
	}
	require.Equal(t, 3, registry.Len())

	stopper := NewRunnerStopper(registry.RunnerRegistry, testLogger(t))

	done := make(chan error, 1)
	go func() { done <- stopper.Start(ctx) }()

	// Still running while the lease is held. No wait is needed to assert this
	// and none would make it stronger: Start blocks on ctx.Done, which has not
	// fired, so nothing has been asked to stop yet.
	require.Equal(t, 3, registry.Len())

	cancel() // the lease is lost

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the stopper never returned")
	}

	assert.Zero(t, registry.Len())
	for i, stub := range registry.stubs() {
		assert.Equal(t, int64(1), stub.closed.Load(), "check %d was not closed", i)
	}
}

func TestManager_StopperIsLeaderElected(t *testing.T) {
	// If this ever returned false, controller-runtime would start the stopper
	// immediately and cancel it on process shutdown rather than on lease loss —
	// which is the same code path in the happy case and completely wrong in the
	// one it exists for.
	stopper := NewRunnerStopper(NewRunnerRegistry(RegistryOptions{}), testLogger(t))
	assert.True(t, stopper.NeedLeaderElection())
}

func TestManager_ReconcilerFillsItsOwnDependencies(t *testing.T) {
	// A reconciler constructed with only a client must still work: the manager
	// entrypoint sets everything, but a test or a future caller may not, and
	// the failure mode of a nil recorder or a nil registry is a panic inside a
	// reconcile rather than an error at setup.
	//
	// This drives applyDefaults rather than SetupWithManager because
	// controller-runtime refuses two controllers with the same name in one
	// process, and the name is what keeps their metrics apart. SetupWithManager
	// itself is covered by the manager test above.
	mgr, err := ctrl.NewManager(testCfg, ctrl.Options{
		Scheme:         testScheme,
		Metrics:        metricsserver.Options{BindAddress: "0"},
		LeaderElection: false,
	})
	require.NoError(t, err)

	reconciler := &DriftCheckReconciler{Client: mgr.GetClient(), Log: testLogger(t)}
	reconciler.applyDefaults(mgr)

	assert.NotNil(t, reconciler.Runners)
	assert.NotNil(t, reconciler.Recorder)
	assert.NotNil(t, reconciler.Scheme)
	assert.NotNil(t, reconciler.Clock)
	assert.NotNil(t, reconciler.events)
	assert.Equal(t, DefaultStatusRefreshInterval, reconciler.StatusRefreshInterval)
	assert.Equal(t, DefaultSecretRetryInterval, reconciler.SecretRetryInterval)
	assert.Zero(t, reconciler.Runners.Len())
}
