package controller

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/nabrahma/driftwatch/api/v1alpha1"
	"github.com/nabrahma/driftwatch/pkg/check"
)

// --- Create -----------------------------------------------------------------

func TestReconcile_CreateStartsARunnerAndAddsTheFinalizer(t *testing.T) {
	f := newFixture(t, fixtureOptions{})

	f.create("kvcache-index")
	result := f.reconcile("kvcache-index")

	assert.Equal(t, f.reconciler.StatusRefreshInterval, result.RequeueAfter,
		"status is refreshed on an interval rather than by watching anything")

	require.Equal(t, 1, f.reconciler.Runners.Len())

	dc := f.get("kvcache-index")
	assert.Contains(t, dc.Finalizers, v1alpha1.Finalizer,
		"without the finalizer a delete would remove the object before the "+
			"runner had stopped, and nothing would ever come back to stop it")
	assert.Equal(t, dc.Generation, dc.Status.ObservedGeneration)
	assert.NotEmpty(t, dc.Status.Phase)
	assert.NotEmpty(t, dc.Status.Conditions, "conditions are set on the first pass")
}

func TestReconcile_MissingObjectStopsItsRunner(t *testing.T) {
	// A DriftCheck removed while the manager was down comes back as NotFound on
	// the next reconcile, and the runner it left behind has to go with it.
	f := newFixture(t, fixtureOptions{})

	f.create("kvcache-index")
	f.reconcile("kvcache-index")
	require.Equal(t, 1, f.reconciler.Runners.Len())

	// Bypass the finalizer entirely, which is what a --force delete does.
	dc := f.get("kvcache-index")
	dc.Finalizers = nil
	require.NoError(t, f.client.Update(f.ctx, dc))
	require.NoError(t, f.client.Delete(f.ctx, dc))

	f.reconcile("kvcache-index")

	assert.Zero(t, f.reconciler.Runners.Len(),
		"a check the API server has forgotten must not keep sweeping")
}

// --- Update -----------------------------------------------------------------

func TestReconcile_UnchangedSpecDoesNotRestartTheRunner(t *testing.T) {
	// Status is rewritten every 15s. Without the hash comparison the oracle
	// would be rebuilt four times a minute and the check would never leave
	// Bootstrapping — a bug whose only symptom is a phase that never advances.
	f := newFixture(t, fixtureOptions{})

	f.create("kvcache-index")
	f.reconcile("kvcache-index")

	runner := f.reconciler.Runners.Get(f.key("kvcache-index"))
	require.NotNil(t, runner)
	hash, startedAt := runner.Hash, runner.StartedAt

	for i := 0; i < 5; i++ {
		f.reconcile("kvcache-index")
	}

	after := f.reconciler.Runners.Get(f.key("kvcache-index"))
	require.NotNil(t, after)
	assert.Equal(t, hash, after.Hash)
	assert.Equal(t, startedAt, after.StartedAt, "the same runner throughout")
	assert.Len(t, f.registry.stubs(), 1, "only one check was ever built")
}

func TestReconcile_SpecChangeRestartsTheRunner(t *testing.T) {
	f := newFixture(t, fixtureOptions{restartInterval: -1})

	f.create("kvcache-index")
	f.reconcile("kvcache-index")

	before := f.reconciler.Runners.Get(f.key("kvcache-index")).Hash

	f.update("kvcache-index", func(dc *v1alpha1.DriftCheck) {
		dc.Spec.Policy.SweepInterval = metav1.Duration{Duration: 90 * time.Second}
	})
	f.reconcile("kvcache-index")

	after := f.reconciler.Runners.Get(f.key("kvcache-index"))
	require.NotNil(t, after)
	assert.NotEqual(t, before, after.Hash, "the new spec produced a new hash")
	assert.Equal(t, 1, f.reconciler.Runners.Len())

	stubs := f.registry.stubs()
	require.Len(t, stubs, 2)
	assert.Equal(t, int64(1), stubs[0].closed.Load(),
		"the replaced runner was closed rather than orphaned")
}

func TestReconcile_RapidUpdateStormThroughTheAPIServer(t *testing.T) {
	// The registry's own version of this test drives Ensure directly. This one
	// goes through the API server, so it also covers the part that is easy to
	// get wrong at the reconciler level: recomputing the hash from an object
	// that has been through a round trip, where field ordering, defaulting and
	// zero-value omission can all change the bytes being hashed.
	f := newFixture(t, fixtureOptions{restartInterval: -1})

	f.create("kvcache-index")
	f.reconcile("kvcache-index")

	var finalHash string

	for i := 1; i <= 20; i++ {
		f.update("kvcache-index", func(dc *v1alpha1.DriftCheck) {
			dc.Spec.Policy.MaxTrackedKeys = 10_000 + i
		})
		f.reconcile("kvcache-index")

		runner := f.reconciler.Runners.Get(f.key("kvcache-index"))
		require.NotNil(t, runner, "update %d left no runner at all", i)
		require.Equal(t, 1, f.reconciler.Runners.Len(),
			"update %d left %d runners", i, f.reconciler.Runners.Len())
		finalHash = runner.Hash
	}

	// The hash the final spec produces, computed independently.
	dc := f.get("kvcache-index")
	expected, err := SpecHash(&dc.Spec, nil)
	require.NoError(t, err)

	assert.Equal(t, expected, finalHash,
		"the surviving runner is the one from the final spec")
	assert.Equal(t, 10_020, dc.Spec.Policy.MaxTrackedKeys)

	running := 0
	for _, stub := range f.registry.stubs() {
		if stub.started.Load() > 0 && stub.finished.Load() == 0 {
			running++
		}
	}
	assert.Equal(t, 1, running, "exactly one check is still executing")
}

func TestReconcile_RestartsAreDebounced(t *testing.T) {
	// §10.3's "a spec updated 10 times in 10 seconds". With the default 5s
	// debounce the runner is replaced at most twice, and the reconciler is told
	// when to come back for the rest.
	f := newFixture(t, fixtureOptions{restartInterval: 5 * time.Second})

	f.create("kvcache-index")
	f.reconcile("kvcache-index")

	for i := 1; i <= 10; i++ {
		f.update("kvcache-index", func(dc *v1alpha1.DriftCheck) {
			dc.Spec.Policy.MaxTrackedKeys = 10_000 + i
		})
		result := f.reconcile("kvcache-index")
		require.Positive(t, result.RequeueAfter)
		f.clock.Advance(time.Second)
	}

	assert.Equal(t, 1, f.reconciler.Runners.Len())
	assert.LessOrEqual(t, len(f.registry.stubs()), 3,
		"ten edits in ten seconds must not rebuild the oracle ten times")

	// And the final spec does eventually get picked up.
	f.clock.Advance(10 * time.Second)
	f.reconcile("kvcache-index")

	dc := f.get("kvcache-index")
	expected, err := SpecHash(&dc.Spec, nil)
	require.NoError(t, err)
	assert.Equal(t, expected, f.reconciler.Runners.Get(f.key("kvcache-index")).Hash)
}

// --- Pause and resume --------------------------------------------------------

func TestReconcile_PauseAndResume(t *testing.T) {
	// Pausing stops sweeping while ingestion continues, which is the whole
	// point: stopping ingestion instead would make every key suspect for as
	// long as it took to refill the oracle, so silencing a check for a deploy
	// would cost an hour of coverage afterwards.
	f := newFixture(t, fixtureOptions{restartInterval: -1})

	f.create("kvcache-index")
	f.reconcile("kvcache-index")
	f.setStatus("kvcache-index", func(st *check.Status) { st.Phase = check.PhaseWatching })
	f.reconcile("kvcache-index")

	require.Equal(t, string(check.PhaseWatching), f.get("kvcache-index").Status.Phase)

	f.update("kvcache-index", func(dc *v1alpha1.DriftCheck) { dc.Spec.Policy.Paused = true })
	f.reconcile("kvcache-index")
	f.setStatus("kvcache-index", func(st *check.Status) { st.Phase = check.PhasePaused })
	f.reconcile("kvcache-index")

	dc := f.get("kvcache-index")
	assert.Equal(t, string(check.PhasePaused), dc.Status.Phase)
	assert.Equal(t, 1, f.reconciler.Runners.Len(),
		"a paused check keeps its runner: the oracle is still being fed")

	ready := meta.FindStatusCondition(dc.Status.Conditions, v1alpha1.ConditionReady)
	require.NotNil(t, ready)
	assert.Equal(t, metav1.ConditionTrue, ready.Status,
		"paused is ready: reporting otherwise would light up every dashboard "+
			"that gates on readiness the moment somebody silenced one check")
	assert.Equal(t, ReasonPaused, ready.Reason)

	f.update("kvcache-index", func(dc *v1alpha1.DriftCheck) { dc.Spec.Policy.Paused = false })
	f.reconcile("kvcache-index")
	f.setStatus("kvcache-index", func(st *check.Status) { st.Phase = check.PhaseWatching })
	f.reconcile("kvcache-index")

	dc = f.get("kvcache-index")
	assert.Equal(t, string(check.PhaseWatching), dc.Status.Phase)
	ready = meta.FindStatusCondition(dc.Status.Conditions, v1alpha1.ConditionReady)
	assert.Equal(t, ReasonWatching, ready.Reason)
}

// --- Delete ------------------------------------------------------------------

func TestReconcile_DeleteStopsTheRunnerBeforeReleasingTheObject(t *testing.T) {
	f := newFixture(t, fixtureOptions{})

	f.create("kvcache-index")
	f.reconcile("kvcache-index")
	require.Equal(t, 1, f.reconciler.Runners.Len())

	stub := f.registry.stubs()[0]

	require.NoError(t, f.client.Delete(f.ctx, f.get("kvcache-index")))

	// The object is still there, held by the finalizer.
	dc := f.get("kvcache-index")
	require.False(t, dc.DeletionTimestamp.IsZero())
	require.Contains(t, dc.Finalizers, v1alpha1.Finalizer)

	f.reconcile("kvcache-index")

	assert.Zero(t, f.reconciler.Runners.Len())
	assert.Equal(t, int64(1), stub.closed.Load(),
		"the check was closed, not merely dropped")

	var gone v1alpha1.DriftCheck
	err := f.client.Get(f.ctx, f.key("kvcache-index"), &gone)
	assert.True(t, apierrors.IsNotFound(err),
		"with the finalizer gone the API server completed the delete")
}

func TestReconcile_DeleteMidBootstrapTerminatesWithinTwoSeconds(t *testing.T) {
	// The reconciler's side of §10.3's named test. The registry's version
	// proves cancellation reaches the runner; this one proves the whole delete
	// path — finalizer, stop, finalizer removal, object gone — completes inside
	// the budget while a bootstrap scan is in progress.
	//
	// It matters because an operator whose `kubectl delete driftcheck` hangs
	// will reach for --force --grace-period=0, which strips the finalizer and
	// leaves the runner alive with nothing left that would ever stop it.
	blocked := make(chan struct{})
	t.Cleanup(func() { close(blocked) })

	f := newFixture(t, fixtureOptions{
		configure: func(_ check.Spec, s *stubRunnable) { s.block = blocked },
	})

	f.create("kvcache-index")
	f.reconcile("kvcache-index")
	require.Equal(t, 1, f.reconciler.Runners.Len())

	stub := f.registry.stubs()[0]
	require.Eventually(t, func() bool { return stub.started.Load() == 1 },
		2*time.Second, time.Millisecond, "the bootstrap never started")

	require.NoError(t, f.client.Delete(f.ctx, f.get("kvcache-index")))

	started := time.Now()
	f.reconcile("kvcache-index")
	elapsed := time.Since(started)

	assert.Less(t, elapsed, 2*time.Second,
		"deleting a check mid-bootstrap must terminate within 2s, took %s", elapsed)

	assert.True(t, stub.ctxDone.Load(),
		"the scan was aborted by cancellation rather than left to finish")
	assert.Zero(t, f.reconciler.Runners.Len())

	var gone v1alpha1.DriftCheck
	err := f.client.Get(f.ctx, f.key("kvcache-index"), &gone)
	assert.True(t, apierrors.IsNotFound(err))

	t.Logf("delete mid-bootstrap completed in %s", elapsed)
}

func TestReconcile_DeleteIsIdempotent(t *testing.T) {
	// A reconcile can be delivered twice for the same deletion, and the second
	// one must not error on an object that has already gone.
	f := newFixture(t, fixtureOptions{})

	f.create("kvcache-index")
	f.reconcile("kvcache-index")

	require.NoError(t, f.client.Delete(f.ctx, f.get("kvcache-index")))
	f.reconcile("kvcache-index")
	require.NoError(t, f.reconcileErr("kvcache-index"))
}

// --- Secrets -----------------------------------------------------------------

func TestReconcile_SecretResolutionFailureAndRecovery(t *testing.T) {
	// The controller's half of §10.2's secret rule. The webhook admits a
	// reference to a secret that does not exist — it must, or a manifest
	// creating both together would be rejected on ordering alone — so the
	// controller is what reports it, and it has to recover on its own when the
	// secret appears.
	f := newFixture(t, fixtureOptions{})

	f.create("kvcache-index", func(dc *v1alpha1.DriftCheck) {
		dc.Spec.Target = v1alpha1.TargetSpec{
			Type: "redis",
			Redis: &v1alpha1.RedisSpec{
				Addr:              "redis.inference.svc:6379",
				PasswordSecretRef: &v1alpha1.SecretKeyRef{Name: "redis-creds", Key: "password"},
			},
		}
	})

	result := f.reconcile("kvcache-index")

	assert.Equal(t, f.reconciler.SecretRetryInterval, result.RequeueAfter,
		"a missing secret is retried rather than given up on")
	assert.Zero(t, f.reconciler.Runners.Len(), "nothing was started")

	dc := f.get("kvcache-index")
	ready := meta.FindStatusCondition(dc.Status.Conditions, v1alpha1.ConditionReady)
	require.NotNil(t, ready)
	assert.Equal(t, metav1.ConditionFalse, ready.Status)
	assert.Equal(t, ReasonSecretMissing, ready.Reason)
	assert.Contains(t, ready.Message,
		`target.redis.passwordSecretRef: secret "redis-creds" not found`,
		"the message names the field, not just the secret: an operator told "+
			"only the secret name still has to work out which of three "+
			"references meant it")

	requireEvent(t, f.events(), "Warning", ReasonSecretMissing)

	// The secret appears, and the next reconcile recovers without intervention.
	f.secret("redis-creds", "password", "hunter2")
	f.reconcile("kvcache-index")

	assert.Equal(t, 1, f.reconciler.Runners.Len())

	dc = f.get("kvcache-index")
	ready = meta.FindStatusCondition(dc.Status.Conditions, v1alpha1.ConditionReady)
	require.NotNil(t, ready)
	assert.NotEqual(t, ReasonSecretMissing, ready.Reason)
}

func TestReconcile_SecretIsSubstitutedIntoTheRunningSpec(t *testing.T) {
	// Resolving the secret is only half the job. A controller that read it and
	// then handed the runner a spec with an empty password would connect
	// unauthenticated and report the store as unreachable, with a status that
	// said the secret resolved fine.
	var got string
	var mu sync.Mutex

	f := newFixture(t, fixtureOptions{
		configure: func(spec check.Spec, _ *stubRunnable) {
			mu.Lock()
			defer mu.Unlock()
			if spec.Target.Redis != nil {
				got = spec.Target.Redis.Password
			}
		},
	})

	f.secret("redis-creds", "password", "hunter2")
	f.create("kvcache-index", func(dc *v1alpha1.DriftCheck) {
		dc.Spec.Target = v1alpha1.TargetSpec{
			Type: "redis",
			Redis: &v1alpha1.RedisSpec{
				Addr:              "redis.inference.svc:6379",
				PasswordSecretRef: &v1alpha1.SecretKeyRef{Name: "redis-creds", Key: "password"},
			},
		}
	})

	f.reconcile("kvcache-index")

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, "hunter2", got)
}

func TestReconcile_RotatedSecretRestartsTheCheck(t *testing.T) {
	// The spec is byte-identical across a rotation, so only the secret in the
	// hash can trigger the restart. Without it the check would go on using a
	// pool holding the old credentials and report the store as unreachable
	// until somebody happened to edit the DriftCheck.
	f := newFixture(t, fixtureOptions{restartInterval: -1})

	f.secret("redis-creds", "password", "hunter2")
	f.create("kvcache-index", func(dc *v1alpha1.DriftCheck) {
		dc.Spec.Target = v1alpha1.TargetSpec{
			Type: "redis",
			Redis: &v1alpha1.RedisSpec{
				Addr:              "redis.inference.svc:6379",
				PasswordSecretRef: &v1alpha1.SecretKeyRef{Name: "redis-creds", Key: "password"},
			},
		}
	})

	f.reconcile("kvcache-index")
	before := f.reconciler.Runners.Get(f.key("kvcache-index")).Hash

	var secret corev1.Secret
	require.NoError(t, f.client.Get(f.ctx, f.key("redis-creds"), &secret))
	secret.Data["password"] = []byte("hunter3")
	require.NoError(t, f.client.Update(f.ctx, &secret))

	f.reconcile("kvcache-index")

	after := f.reconciler.Runners.Get(f.key("kvcache-index"))
	require.NotNil(t, after)
	assert.NotEqual(t, before, after.Hash, "the rotation restarted the check")
	assert.Equal(t, 1, f.reconciler.Runners.Len())
}

func TestReconcile_SecretMissingTheNamedKey(t *testing.T) {
	// A secret that exists with the wrong key inside it is a different mistake
	// from a secret that does not exist, and reporting them the same way sends
	// the operator to check the wrong thing.
	f := newFixture(t, fixtureOptions{})

	f.secret("redis-creds", "passwd", "hunter2")
	f.create("kvcache-index", func(dc *v1alpha1.DriftCheck) {
		dc.Spec.Target = v1alpha1.TargetSpec{
			Type: "redis",
			Redis: &v1alpha1.RedisSpec{
				Addr:              "redis.inference.svc:6379",
				PasswordSecretRef: &v1alpha1.SecretKeyRef{Name: "redis-creds", Key: "password"},
			},
		}
	})

	f.reconcile("kvcache-index")

	dc := f.get("kvcache-index")
	ready := meta.FindStatusCondition(dc.Status.Conditions, v1alpha1.ConditionReady)
	require.NotNil(t, ready)
	assert.Contains(t, ready.Message,
		`target.redis.passwordSecretRef: secret "redis-creds" has no key "password"`)
}

// --- Status patching ---------------------------------------------------------

func TestReconcile_StatusPatchSurvivesAConcurrentSpecEdit(t *testing.T) {
	// §10.3 requires Status().Patch with conflict retry and forbids Update from
	// a stale copy. This is why: the reconciler holds an object read at the
	// start of the pass, and the operator may edit the spec during it. An
	// Update would send the whole object back and silently revert that edit.
	f := newFixture(t, fixtureOptions{})

	f.create("kvcache-index")
	f.reconcile("kvcache-index")

	// A stale copy, exactly as a slow reconcile would hold.
	stale := f.get("kvcache-index")

	// Meanwhile the operator raises a bound.
	f.update("kvcache-index", func(dc *v1alpha1.DriftCheck) {
		dc.Spec.Policy.MaxTrackedKeys = 2_000_000
	})

	f.setStatus("kvcache-index", func(st *check.Status) {
		st.Phase = check.PhaseWatching
		st.TrackedKeys = 4242
	})
	require.NoError(t, f.reconciler.updateStatus(f.ctx, stale))

	dc := f.get("kvcache-index")
	assert.Equal(t, 4242, dc.Status.TrackedKeys, "the status was written")
	assert.Equal(t, 2_000_000, dc.Spec.Policy.MaxTrackedKeys,
		"and the operator's edit survived: this is the assertion that fails "+
			"the moment somebody replaces the patch with an Update")
}

func TestReconcile_StatusPatchRetriesOnConflict(t *testing.T) {
	// A conflict is manufactured by editing the object between the reconciler's
	// read and its write. RetryOnConflict re-reads and reapplies, so the patch
	// lands rather than the reconcile failing and requeueing with backoff.
	f := newFixture(t, fixtureOptions{})

	f.create("kvcache-index")
	f.reconcile("kvcache-index")

	var attempts int
	err := f.reconciler.patchStatus(f.ctx, f.get("kvcache-index"),
		func(status *v1alpha1.DriftCheckStatus) {
			attempts++
			if attempts == 1 {
				// Bump the resourceVersion out from under this attempt.
				f.update("kvcache-index", func(dc *v1alpha1.DriftCheck) {
					dc.Spec.Policy.RingSize = 32
				})
			}
			status.TrackedKeys = 7
		})

	require.NoError(t, err)
	assert.Equal(t, 7, f.get("kvcache-index").Status.TrackedKeys)
}

func TestReconcile_StatusCarriesEveryFieldTheCheckReports(t *testing.T) {
	// The status block is the only thing most operators ever read about a
	// check, so a field the reconciler forgets to copy is a number nobody ever
	// sees. Asserting on the values rather than on "no error" is what catches
	// that.
	f := newFixture(t, fixtureOptions{})

	f.create("kvcache-index")
	f.reconcile("kvcache-index")

	sweptAt := testEpoch.Add(-30 * time.Second)

	f.setStatus("kvcache-index", func(st *check.Status) {
		st.Phase = check.PhaseWatching
		st.Message = "steady state"
		st.TrackedKeys = 984_312
		st.SettledKeys = 981_002
		st.InFlightKeys = 3_310
		st.CoverageRatio = 0.9966
		st.DivergentKeys = 12
		st.SuspectDivergentKeys = 3
		st.DivergenceByCategory = map[string]int{"member_subset": 9, "missing": 3}
		st.DriftDurationSeconds = 245.5
		st.SettlementWindowSeconds = 6.2
		st.ConvergenceP99Seconds = 2.1
		st.EventsApplied = 41_882_133
		st.EventsDropped = 4
		st.SweepsSkipped = 2
		st.LastSweepTime = sweptAt
		st.LastSweepDurationSeconds = 1.84
		st.LastSweepKeysCompared = 981_002
		st.TargetReachable = true
		st.TargetRole = "master"
		st.TargetKeyspaceSize = 984_300
		st.SourceConnected = true
		st.Publishers = []check.PublisherStatus{
			{ID: "replica-1", Epoch: 3, HighWaterMark: 8_812_004, LastSeenSeconds: 0.2},
			{ID: "replica-0", Epoch: 7, HighWaterMark: 9_112_004, MissingEvents: 14},
		}
	})
	f.reconcile("kvcache-index")

	got := f.get("kvcache-index").Status

	assert.Equal(t, "Watching", got.Phase)
	assert.Equal(t, "steady state", got.Message)
	assert.Equal(t, 984_312, got.TrackedKeys)
	assert.Equal(t, 981_002, got.SettledKeys)
	assert.Equal(t, 3_310, got.InFlightKeys)
	assert.Equal(t, "0.9966", got.CoverageRatio)
	assert.Equal(t, 12, got.DivergentKeys)
	assert.Equal(t, 3, got.SuspectDivergentKeys)
	assert.Equal(t, map[string]int{"member_subset": 9, "missing": 3}, got.DivergenceByCategory)
	assert.Equal(t, "245.5", got.DriftDurationSeconds)
	assert.Equal(t, "6.2", got.SettlementWindowSeconds)
	assert.Equal(t, "2.1", got.ConvergenceP99Seconds)
	assert.Equal(t, int64(41_882_133), got.EventsApplied)
	assert.Equal(t, int64(4), got.EventsDropped)
	assert.Equal(t, int64(2), got.SweepsSkipped)
	assert.Equal(t, "1.84", got.LastSweepDurationSeconds)
	assert.Equal(t, 981_002, got.LastSweepKeysCompared)
	require.NotNil(t, got.LastSweepTime)
	assert.Equal(t, sweptAt.Unix(), got.LastSweepTime.Unix())
	assert.True(t, got.TargetReachable)
	assert.Equal(t, "master", got.TargetRole)
	assert.Equal(t, int64(984_300), got.TargetKeyspaceSize)

	require.Len(t, got.Publishers, 2)
	assert.Equal(t, "replica-0", got.Publishers[0].ID,
		"sorted, so an unsorted map does not make every status patch a change")
	assert.Equal(t, int64(14), got.Publishers[0].MissingEvents)
	assert.Equal(t, int64(9_112_004), got.Publishers[0].HighWaterMark)
}

func TestReconcile_SuspectAndConfirmedNeverMerge(t *testing.T) {
	// §23 A7. suspectDivergentKeys measures driftwatch's own event loss rather
	// than the store's correctness, so a status that added the two together
	// would page somebody about driftwatch. The DriftDetected condition follows
	// the confirmed count alone.
	f := newFixture(t, fixtureOptions{})

	f.create("kvcache-index")
	f.reconcile("kvcache-index")

	f.setStatus("kvcache-index", func(st *check.Status) {
		st.Phase = check.PhaseDegraded
		st.DivergentKeys = 0
		st.SuspectDivergentKeys = 4_000
	})
	f.reconcile("kvcache-index")

	dc := f.get("kvcache-index")
	assert.Zero(t, dc.Status.DivergentKeys)
	assert.Equal(t, 4_000, dc.Status.SuspectDivergentKeys)

	drift := meta.FindStatusCondition(dc.Status.Conditions, v1alpha1.ConditionDriftDetected)
	require.NotNil(t, drift)
	assert.Equal(t, metav1.ConditionFalse, drift.Status,
		"four thousand suspect keys are not drift: they are driftwatch saying "+
			"its own view is incomplete")
	assert.Contains(t, drift.Message, "do not alert on these")

	assert.Empty(t, f.eventsMatching(ReasonDriftDetected),
		"and no event fires either")
}

// --- Conditions ---------------------------------------------------------------

func TestReconcile_EveryConditionIsSet(t *testing.T) {
	// All of them, every time, including the ones that are true. A condition
	// that only appears when it is false leaves an operator unable to tell
	// "this is fine" from "this has not been evaluated".
	f := newFixture(t, fixtureOptions{})

	f.create("kvcache-index")
	f.reconcile("kvcache-index")

	dc := f.get("kvcache-index")

	for _, kind := range []string{
		v1alpha1.ConditionReady,
		v1alpha1.ConditionSourceConnected,
		v1alpha1.ConditionTargetAvailable,
		v1alpha1.ConditionDriftDetected,
		v1alpha1.ConditionOracleSaturated,
		v1alpha1.ConditionSequenceIntegrity,
		v1alpha1.ConditionAwaitingSnapshot,
		v1alpha1.ConditionMultiWriterUnsafe,
		v1alpha1.ConditionProjectionNotCommutative,
		v1alpha1.ConditionSweepIntervalTight,
	} {
		cond := meta.FindStatusCondition(dc.Status.Conditions, kind)
		require.NotNil(t, cond, "condition %s was never set", kind)
		assert.NotEmpty(t, cond.Reason, "condition %s has no reason", kind)
		assert.NotEmpty(t, cond.Message, "condition %s has no message", kind)
		assert.Equal(t, dc.Generation, cond.ObservedGeneration)
	}
}

func TestReconcile_SaturationConditionExplainsWhatItChanges(t *testing.T) {
	// Saturation changes how every other number on the object should be read,
	// so the condition says so rather than merely being true.
	f := newFixture(t, fixtureOptions{})

	f.create("kvcache-index")
	f.reconcile("kvcache-index")

	f.setStatus("kvcache-index", func(st *check.Status) {
		st.Phase = check.PhaseWatching
		st.OracleSaturated = true
		st.OracleEvictions = 44_218
		st.DivergentKeys = 3
	})
	f.reconcile("kvcache-index")

	dc := f.get("kvcache-index")
	cond := meta.FindStatusCondition(dc.Status.Conditions, v1alpha1.ConditionOracleSaturated)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Contains(t, cond.Message, "44218 evictions")
	assert.Contains(t, cond.Message, "only part of the store")
}

func TestReconcile_SequenceIntegrityNamesTheWorstPublisher(t *testing.T) {
	f := newFixture(t, fixtureOptions{})

	f.create("kvcache-index")
	f.reconcile("kvcache-index")

	f.setStatus("kvcache-index", func(st *check.Status) {
		st.Phase = check.PhaseDegraded
		st.Publishers = []check.PublisherStatus{
			{ID: "replica-0", MissingEvents: 2},
			{ID: "replica-1", MissingEvents: 14},
		}
	})
	f.reconcile("kvcache-index")

	dc := f.get("kvcache-index")
	cond := meta.FindStatusCondition(dc.Status.Conditions, v1alpha1.ConditionSequenceIntegrity)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, ReasonGapsObserved, cond.Reason)
	assert.Equal(t, "publisher replica-1: 14 missing events", cond.Message,
		"the exact message §10.1's example shows")
}

func TestReconcile_NonCommutativeProjectionIsReportedAsACondition(t *testing.T) {
	// §10.2 makes this a warning rather than an error, so the condition is the
	// only durable record of it: an operator reading a surprising counter total
	// needs to find the reason on the object rather than in an admission
	// warning printed once, weeks ago.
	f := newFixture(t, fixtureOptions{})

	f.create("kvcache-index", func(dc *v1alpha1.DriftCheck) {
		dc.Spec.Projection = v1alpha1.ProjectionSpec{Type: "counter", IncrOnly: false}
	})
	f.reconcile("kvcache-index")

	dc := f.get("kvcache-index")
	cond := meta.FindStatusCondition(dc.Status.Conditions,
		v1alpha1.ConditionProjectionNotCommutative)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Contains(t, cond.Message, "reordered stream converges to a different total")
}

// --- Events -------------------------------------------------------------------

func TestReconcile_EmitsTheEightEventsFromTheSpec(t *testing.T) {
	// §10.3 lists eight cases. They are what an operator sees first in
	// `kubectl describe`, and getting them wrong in the other direction is just
	// as bad: a controller that emitted its state on every pass would produce
	// four identical "drift detected" events a minute for one unchanging
	// finding, and the describe output would be a wall of noise.
	f := newFixture(t, fixtureOptions{})

	f.create("kvcache-index")
	f.reconcile("kvcache-index")
	f.events() // discard whatever the first pass produced

	// Establish a healthy baseline, so the transitions below are transitions.
	f.setStatus("kvcache-index", func(st *check.Status) {
		st.Phase = check.PhaseBootstrapping
		st.SourceConnected = true
		st.TargetReachable = true
	})
	f.reconcile("kvcache-index")
	f.events()

	// 1. Bootstrap complete.
	f.setStatus("kvcache-index", func(st *check.Status) {
		st.Phase = check.PhaseWatching
		st.Bootstrapped = true
		st.TrackedKeys = 984_312
	})
	f.reconcile("kvcache-index")
	requireEvent(t, f.events(), "Normal", ReasonBootstrapComplete, "984312 keys tracked")

	// 2. Drift detected, with the count.
	f.setStatus("kvcache-index", func(st *check.Status) { st.DivergentKeys = 12 })
	f.reconcile("kvcache-index")
	requireEvent(t, f.events(), "Warning", ReasonDriftDetected, "12 confirmed divergent keys")

	// 3. Drift resolved.
	f.setStatus("kvcache-index", func(st *check.Status) { st.DivergentKeys = 0 })
	f.reconcile("kvcache-index")
	requireEvent(t, f.events(), "Normal", ReasonDriftResolved, "12 keys agree again")

	// 4. Source disconnected.
	f.setStatus("kvcache-index", func(st *check.Status) {
		st.SourceConnected = false
		st.SourceLastError = "dial tcp 10.0.0.4:5557: connect: connection refused"
	})
	f.reconcile("kvcache-index")
	requireEvent(t, f.events(), "Warning", ReasonSourceDisconnected, "connection refused")

	// 5. Source reconnected.
	f.setStatus("kvcache-index", func(st *check.Status) {
		st.SourceConnected = true
		st.SourceReconnects = 3
	})
	f.reconcile("kvcache-index")
	requireEvent(t, f.events(), "Normal", ReasonSourceReconnected, "3 reconnects")

	// 6. Target unavailable.
	f.setStatus("kvcache-index", func(st *check.Status) { st.TargetReachable = false })
	f.reconcile("kvcache-index")
	requireEvent(t, f.events(), "Warning", ReasonTargetUnavailable,
		"the last ones driftwatch actually knew")

	f.setStatus("kvcache-index", func(st *check.Status) { st.TargetReachable = true })
	f.reconcile("kvcache-index")
	requireEvent(t, f.events(), "Normal", ReasonTargetAvailable)

	// 7. Oracle saturated.
	f.setStatus("kvcache-index", func(st *check.Status) {
		st.OracleSaturated = true
		st.OracleEvictions = 44_218
	})
	f.reconcile("kvcache-index")
	requireEvent(t, f.events(), "Warning", ReasonOracleSaturated, "44218 evictions")

	// 8. Publisher restart.
	f.setStatus("kvcache-index", func(st *check.Status) {
		st.Publishers = []check.PublisherStatus{{ID: "replica-0", Epoch: 7, Restarts: 1}}
	})
	f.reconcile("kvcache-index")
	requireEvent(t, f.events(), "Warning", ReasonPublisherRestart, "replica-0 restarted")
}

func TestReconcile_UnchangedStateEmitsNoEvents(t *testing.T) {
	// The other half of getting events right. Four reconciles a minute against
	// an unchanging finding must produce nothing, or `kubectl describe` becomes
	// useless exactly when it is most needed.
	f := newFixture(t, fixtureOptions{})

	f.create("kvcache-index")
	f.reconcile("kvcache-index")

	f.setStatus("kvcache-index", func(st *check.Status) {
		st.Phase = check.PhaseWatching
		st.Bootstrapped = true
		st.SourceConnected = true
		st.TargetReachable = true
		st.DivergentKeys = 12
	})
	f.reconcile("kvcache-index")
	f.events()

	for i := 0; i < 10; i++ {
		f.reconcile("kvcache-index")
	}

	assert.Empty(t, f.events(), "ten reconciles over unchanged state emitted events")
}

func TestReconcile_AnAdoptedCheckDoesNotReplayItsHistory(t *testing.T) {
	// A manager that has just taken the lease sees every check for the first
	// time, in whatever state it is already in. Emitting an event for each of
	// those would fill the event stream on every failover with a history of
	// things that did not just happen.
	f := newFixture(t, fixtureOptions{})

	f.create("kvcache-index")
	f.reconcile("kvcache-index")
	f.events()

	// The very first observation is of a check that is already unhappy.
	f.reconciler.events = newTransitionTracker()
	f.setStatus("kvcache-index", func(st *check.Status) {
		st.Phase = check.PhaseDegraded
		st.SourceConnected = false
		st.TargetReachable = false
		st.DivergentKeys = 12
		st.Publishers = []check.PublisherStatus{{ID: "replica-0", Restarts: 4}}
	})
	f.reconcile("kvcache-index")

	events := f.events()
	assert.Empty(t, events,
		"adopting a check reports its state through conditions, not by replaying "+
			"events for things that happened before this manager existed: %v", events)

	// But the conditions do say what is wrong.
	dc := f.get("kvcache-index")
	target := meta.FindStatusCondition(dc.Status.Conditions, v1alpha1.ConditionTargetAvailable)
	require.NotNil(t, target)
	assert.Equal(t, metav1.ConditionFalse, target.Status)
}

func TestReconcile_DriftGrowthIsItsOwnEvent(t *testing.T) {
	// Twelve keys becoming four thousand is a different incident from the one
	// already open, and an operator watching the event stream should see it.
	f := newFixture(t, fixtureOptions{})

	f.create("kvcache-index")
	f.reconcile("kvcache-index")
	f.setStatus("kvcache-index", func(st *check.Status) { st.DivergentKeys = 12 })
	f.reconcile("kvcache-index")
	f.events()

	f.setStatus("kvcache-index", func(st *check.Status) { st.DivergentKeys = 4_000 })
	f.reconcile("kvcache-index")

	requireEvent(t, f.events(), "Warning", ReasonDriftDetected,
		"4000 confirmed divergent keys (was 12)")
}

// --- Isolation ----------------------------------------------------------------

func TestReconcile_TwoChecksOverTheSameSourceAndTargetDoNotInterfere(t *testing.T) {
	// §10.3's edge case, and it is legal: two projections auditing the same
	// data is a reasonable thing to want. What must not happen is one check's
	// runner or status leaking into the other's.
	f := newFixture(t, fixtureOptions{})

	f.create("by-key", func(dc *v1alpha1.DriftCheck) {
		dc.Spec.Projection = v1alpha1.ProjectionSpec{Type: "scalar"}
	})
	f.create("by-count", func(dc *v1alpha1.DriftCheck) {
		dc.Spec.Projection = v1alpha1.ProjectionSpec{Type: "counter", IncrOnly: true}
	})

	f.reconcile("by-key")
	f.reconcile("by-count")

	require.Equal(t, 2, f.reconciler.Runners.Len())

	f.setStatus("by-key", func(st *check.Status) {
		st.Phase = check.PhaseWatching
		st.DivergentKeys = 12
	})
	f.setStatus("by-count", func(st *check.Status) {
		st.Phase = check.PhaseWatching
		st.DivergentKeys = 0
	})
	f.reconcile("by-key")
	f.reconcile("by-count")

	assert.Equal(t, 12, f.get("by-key").Status.DivergentKeys)
	assert.Zero(t, f.get("by-count").Status.DivergentKeys)

	// Deleting one leaves the other running.
	require.NoError(t, f.client.Delete(f.ctx, f.get("by-key")))
	f.reconcile("by-key")

	assert.Equal(t, 1, f.reconciler.Runners.Len())
	assert.NotNil(t, f.reconciler.Runners.Get(f.key("by-count")))
}

func TestReconcile_APanickingCheckIsReportedNotHidden(t *testing.T) {
	// The panic isolation from the registry, seen from the object: the failure
	// has to reach the CRD's status, or an operator's only clue is a log line
	// in a manager they may not be watching.
	f := newFixture(t, fixtureOptions{
		configure: func(spec check.Spec, s *stubRunnable) {
			s.panicOnRun = spec.Name == "poisoned"
		},
	})

	f.create("poisoned")
	f.create("healthy")

	f.reconcile("poisoned")
	f.reconcile("healthy")

	runner := f.reconciler.Runners.Get(f.key("poisoned"))
	require.NotNil(t, runner)
	<-runner.Done()

	// The next reconcile is inside the restart debounce, so the dead runner is
	// still the registered one and its failure is what gets written. That is
	// the behavior worth having: a check that panics during bootstrap should
	// settle on Failed rather than alternate between Failed and Bootstrapping
	// on every status refresh.
	f.reconcile("poisoned")

	dc := f.get("poisoned")
	assert.Equal(t, string(check.PhaseFailed), dc.Status.Phase)
	assert.Contains(t, dc.Status.Message, "check panicked")

	ready := meta.FindStatusCondition(dc.Status.Conditions, v1alpha1.ConditionReady)
	require.NotNil(t, ready)
	assert.Equal(t, metav1.ConditionFalse, ready.Status)

	// And the other check is untouched.
	f.reconcile("healthy")
	assert.NotEqual(t, string(check.PhaseFailed), f.get("healthy").Status.Phase)
}

func TestReconcile_AnInvalidSpecIsReportedRatherThanCrashing(t *testing.T) {
	// The webhook should have caught this, but the webhook can be disabled, and
	// a controller that crash-looped on a spec it could not build would take
	// every other check down with it.
	//
	// Real checks here, not stubs: the behavior under test is check.New
	// rejecting a spec, which no stub can reproduce.
	f := newFixture(t, fixtureOptions{realChecks: true})

	f.create("kvcache-index", func(dc *v1alpha1.DriftCheck) {
		// A registered source with no configuration behind it: valid against
		// the CRD schema, impossible to construct.
		dc.Spec.Source = v1alpha1.SourceSpec{Type: "nats"}
	})

	result, err := f.reconciler.Reconcile(f.ctx,
		ctrl.Request{NamespacedName: f.key("kvcache-index")})

	require.NoError(t, err, "a rejected spec is reported, not returned as an error")
	assert.Positive(t, result.RequeueAfter)
	assert.Zero(t, f.reconciler.Runners.Len())

	dc := f.get("kvcache-index")
	assert.Equal(t, string(check.PhaseFailed), dc.Status.Phase)

	ready := meta.FindStatusCondition(dc.Status.Conditions, v1alpha1.ConditionReady)
	require.NotNil(t, ready)
	assert.Equal(t, ReasonStartFailed, ready.Reason)
	assert.Contains(t, ready.Message, "source.nats.url")

	requireEvent(t, f.events(), "Warning", ReasonStartFailed)
}

// --- Helpers ------------------------------------------------------------------

// requireEvent asserts that one of the recorded events matches.
func requireEvent(t *testing.T, events []string, eventType, reason string, contains ...string) {
	t.Helper()

	prefix := eventType + " " + reason
	for _, e := range events {
		if !strings.HasPrefix(e, prefix) {
			continue
		}
		for _, want := range contains {
			assert.Contains(t, e, want)
		}
		return
	}
	t.Fatalf("no %s event; recorded:\n  %s", prefix, strings.Join(events, "\n  "))
}

// eventsMatching returns the recorded events with a given reason.
func (f *fixture) eventsMatching(reason string) []string {
	f.t.Helper()

	var out []string
	for _, e := range f.events() {
		if strings.Contains(e, " "+reason+" ") {
			out = append(out, e)
		}
	}
	return out
}

func TestFixture_NamespaceNamesAreLegal(t *testing.T) {
	// The fixture derives a namespace from the test's own name, and an illegal
	// one fails with a message about DNS labels rather than about the
	// controller — which is a genuinely confusing half-hour if it happens
	// intermittently. It did: truncating a long name from the left occasionally
	// left a leading hyphen, so one run in four failed in a different test.
	//
	// The subtests are the long and awkward names that produced it, because
	// this test's own name is short enough never to exercise the truncation.
	for _, name := range []string{
		"short",
		"TestManager_RunsChecksAndLeavesNothingBehindAfterTeardown",
		"TestReconcile_SomethingWithAVeryLongName/and_a_subtest_name_that_" +
			"pushes_the_whole_thing_well_past_sixty_three_characters",
		"Test____",
	} {
		t.Run(name, func(t *testing.T) {
			got := uniqueNamespace(t)

			assert.LessOrEqual(t, len(got), 63, "%q is longer than a DNS label", got)
			assert.Regexp(t, `^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`, got)
		})
	}
}
