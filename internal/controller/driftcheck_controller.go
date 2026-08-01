// Package controller reconciles DriftCheck resources and owns Check lifecycles (§10.3).
package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/nabrahma/driftwatch/api/v1alpha1"
	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/metrics"
)

// RBAC. §10.3 is explicit that this is least privilege and that the generated
// role is committed, so a reviewer can see the whole of what the manager can do
// without running controller-gen.
//
// There is deliberately no access to pods, nodes, deployments or any other
// workload resource. driftwatch reads an event stream and a datastore; it has
// no reason to enumerate what is running in the cluster, and a controller that
// could would be a far more interesting thing to compromise than one that
// cannot. Secrets are get-only, and only so that a password can be read out of
// one.
//
// Leader-election leases are deliberately not here. They live in a namespaced
// Role (config/rbac/leader_election_role.yaml) because the manager needs
// exactly one lease in its own namespace, and granting cluster-wide lease
// write would let a compromised manager evict the leader of every other
// operator in the cluster. Same reasoning as §18's preference for a Role per
// namespace on secrets.
//
// +kubebuilder:rbac:groups=driftwatch.io,resources=driftchecks,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=driftwatch.io,resources=driftchecks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=driftwatch.io,resources=driftchecks/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// DriftCheckReconciler turns DriftCheck objects into running checks.
type DriftCheckReconciler struct {
	client.Client

	// Scheme is the manager's scheme.
	Scheme *runtime.Scheme
	// Clock is the injected clock.
	Clock clock.Clock
	// Metrics is the process-wide metric set.
	Metrics *metrics.Metrics
	// Runners owns every running check.
	Runners *RunnerRegistry
	// Recorder emits the Kubernetes Events §10.3 lists.
	Recorder record.EventRecorder
	// Log receives the reconcile lines.
	Log logr.Logger

	// StatusRefreshInterval is how often a healthy check's status is rewritten.
	// §10.3 defaults it to 15s: often enough that `kubectl get driftcheck` is
	// worth reading, seldom enough not to be watch churn.
	StatusRefreshInterval time.Duration
	// SecretRetryInterval is how long to wait after a secret could not be
	// resolved. §10.3 says 30s.
	SecretRetryInterval time.Duration

	// events remembers the state each check was last seen in, so a transition
	// can be told from a steady state. Without it every reconcile would emit
	// "drift detected" for the same unchanged finding, four times a minute,
	// until `kubectl describe` was useless.
	events *transitionTracker
}

// Defaults for the reconciler's intervals.
const (
	DefaultStatusRefreshInterval = 15 * time.Second
	DefaultSecretRetryInterval   = 30 * time.Second
)

// SetupWithManager registers the reconciler.
func (r *DriftCheckReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.applyDefaults(mgr)

	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.DriftCheck{}).
		// Owns nothing: a DriftCheck creates no Kubernetes objects. It is a
		// declaration of an audit, and the audit runs inside this process.
		WithOptions(controller.Options{MaxConcurrentReconciles: 4}).
		Named("driftcheck").
		Complete(r)
}

func (r *DriftCheckReconciler) applyDefaults(mgr ctrl.Manager) {
	if r.Clock == nil {
		r.Clock = clock.Real()
	}
	if r.StatusRefreshInterval <= 0 {
		r.StatusRefreshInterval = DefaultStatusRefreshInterval
	}
	if r.SecretRetryInterval <= 0 {
		r.SecretRetryInterval = DefaultSecretRetryInterval
	}
	if r.events == nil {
		r.events = newTransitionTracker()
	}
	if mgr == nil {
		return
	}
	if r.Scheme == nil {
		r.Scheme = mgr.GetScheme()
	}
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor("driftwatch")
	}
	if r.Runners == nil {
		r.Runners = NewRunnerRegistry(RegistryOptions{
			Logger:  r.Log,
			Clock:   r.Clock,
			Metrics: r.Metrics,
		})
	}
}

// Reconcile implements §10.3's nine steps.
func (r *DriftCheckReconciler) Reconcile(
	ctx context.Context, req ctrl.Request,
) (ctrl.Result, error) {
	r.applyDefaults(nil)

	log := logf.FromContext(ctx).WithValues("check", req.String())

	// 1. Fetch. A DriftCheck that no longer exists takes its runner with it.
	var dc v1alpha1.DriftCheck
	if err := r.Get(ctx, req.NamespacedName, &dc); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, r.forget(ctx, req.NamespacedName)
		}
		return ctrl.Result{}, err
	}

	// 2. Deletion. The runner stops before the finalizer comes off, so the
	// object never disappears while a check still holds its connections.
	if !dc.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, r.finalize(ctx, &dc)
	}

	// 3. The finalizer, which is what makes step 2 reachable at all.
	if !controllerutil.ContainsFinalizer(&dc, v1alpha1.Finalizer) {
		patch := client.MergeFrom(dc.DeepCopy())
		controllerutil.AddFinalizer(&dc, v1alpha1.Finalizer)

		if err := r.Patch(ctx, &dc, patch); err != nil {
			return ctrl.Result{}, fmt.Errorf("adding the finalizer: %w", err)
		}
	}

	// 4. Secrets. Resolved by the controller rather than the webhook, because a
	// webhook that depended on other objects existing would reject a manifest
	// that creates the Secret and the DriftCheck together.
	secrets, err := r.resolveSecrets(ctx, &dc)
	if err != nil {
		return r.secretFailure(ctx, &dc, err)
	}

	// 5 and 6. Hash, then make the registry agree with it.
	hash, err := SpecHash(&dc.Spec, secrets)
	if err != nil {
		return ctrl.Result{}, err
	}

	spec := dc.Spec.ToCheckSpec(dc.Name, dc.Namespace)
	applySecrets(&spec, secrets)

	// 7. Start, restart, or leave alone.
	outcome, err := r.Runners.Ensure(ctx, req.NamespacedName, hash, spec)
	switch {
	case errors.Is(err, ErrRegistryClosed):
		// This manager is shutting down or has lost the lease. Not a failure of
		// the check, and writing Failed onto the object here would leave a
		// misleading status behind for the replica that takes over.
		log.V(1).Info("registry is closed; leaving the check to the next leader")
		return ctrl.Result{}, nil
	case err != nil:
		return r.startFailure(ctx, &dc, err)
	}

	switch outcome.Action {
	case ActionStarted, ActionRestarted:
		log.Info("runner "+outcome.Action.String(), "hash", hash)
	case ActionThrottled:
		// The old runner is still serving the old spec, which is better than a
		// gap in coverage while a new one bootstraps.
		log.V(1).Info("restart throttled", "retryAfter", outcome.RetryAfter)
	case ActionUnchanged:
	}

	// 8. Status, and the events that come out of comparing it to last time.
	if err := r.updateStatus(ctx, &dc); err != nil {
		return ctrl.Result{}, err
	}

	// 9. Come back, so status stays fresh without watching anything.
	requeue := r.StatusRefreshInterval
	if outcome.RetryAfter > 0 && outcome.RetryAfter < requeue {
		requeue = outcome.RetryAfter
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// forget stops the runner for a DriftCheck that is going away.
func (r *DriftCheckReconciler) forget(ctx context.Context, key types.NamespacedName) error {
	if err := r.Runners.Stop(ctx, key); err != nil {
		return err
	}
	r.Runners.Forget(key)
	r.events.forget(key)
	return nil
}

// finalize stops the runner and then releases the object.
//
// The order is the whole point of having a finalizer. Removing it first would
// let the API server delete the object while the check still held a Redis
// connection pool and a ZMQ subscription, and nothing would ever come back to
// reconcile it — the runner would go on sweeping a store on behalf of a
// DriftCheck that no longer exists.
func (r *DriftCheckReconciler) finalize(ctx context.Context, dc *v1alpha1.DriftCheck) error {
	key := client.ObjectKeyFromObject(dc)

	if err := r.forget(ctx, key); err != nil {
		// Leave the finalizer on. A check that could not be stopped cleanly is
		// exactly the one that must not be forgotten about.
		return fmt.Errorf("stopping the runner for %s: %w", key, err)
	}

	if !controllerutil.ContainsFinalizer(dc, v1alpha1.Finalizer) {
		return nil
	}

	patch := client.MergeFrom(dc.DeepCopy())
	controllerutil.RemoveFinalizer(dc, v1alpha1.Finalizer)

	if err := r.Patch(ctx, dc, patch); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("removing the finalizer: %w", err)
	}

	logf.FromContext(ctx).Info("check stopped and released", "check", key.String())
	return nil
}

// resolveSecrets reads every secret the spec refers to.
//
// The values come back keyed by spec field path rather than by secret name, so
// the error can say which field is wrong. An operator told "secret redis-creds
// not found" still has to work out which of three references meant it.
func (r *DriftCheckReconciler) resolveSecrets(
	ctx context.Context, dc *v1alpha1.DriftCheck,
) (map[string]string, error) {
	refs := dc.Spec.SecretRefs()
	if len(refs) == 0 {
		return nil, nil
	}

	out := make(map[string]string, len(refs))

	for path, ref := range refs {
		var secret corev1.Secret
		name := types.NamespacedName{Namespace: dc.Namespace, Name: ref.Name}

		if err := r.Get(ctx, name, &secret); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("%s: secret %q not found", path, ref.Name)
			}
			// A permissions failure reads very differently from a missing
			// secret, and §10.3 asks for a clear condition rather than a
			// crash-loop when the manager cannot read a namespace's secrets.
			return nil, fmt.Errorf("%s: reading secret %q: %w", path, ref.Name, err)
		}

		value, ok := secret.Data[ref.Key]
		if !ok {
			return nil, fmt.Errorf("%s: secret %q has no key %q", path, ref.Name, ref.Key)
		}
		out[path] = string(value)
	}
	return out, nil
}

// applySecrets substitutes resolved values into the runtime spec.
func applySecrets(spec *check.Spec, secrets map[string]string) {
	if password, ok := secrets["target.redis.passwordSecretRef"]; ok && spec.Target.Redis != nil {
		spec.Target.Redis.Password = password
	}
}

// secretFailure reports an unresolvable secret and comes back later.
func (r *DriftCheckReconciler) secretFailure(
	ctx context.Context, dc *v1alpha1.DriftCheck, cause error,
) (ctrl.Result, error) {
	logf.FromContext(ctx).Info("secret could not be resolved", "error", cause.Error())

	// The runner keeps running if there is one. A secret that briefly fails to
	// resolve mid-rotation is not a reason to stop auditing with credentials
	// that still work.
	r.record(dc, ReasonSecretMissing, corev1.EventTypeWarning, "%s", cause.Error())

	err := r.patchStatus(ctx, dc, func(status *v1alpha1.DriftCheckStatus) {
		status.Phase = string(check.PhaseFailed)
		status.Message = cause.Error()
		status.ObservedGeneration = dc.Generation

		setCondition(status, metav1.Condition{
			Type:    v1alpha1.ConditionReady,
			Status:  metav1.ConditionFalse,
			Reason:  ReasonSecretMissing,
			Message: cause.Error(),
		}, dc.Generation, r.Clock.Now())
	})
	if err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: r.SecretRetryInterval}, nil
}

// startFailure reports a check that could not be built.
//
// This is a rejected spec rather than a transient error — the webhook should
// have caught it — so it does not requeue quickly. Retrying an invalid
// configuration every second would fill the event stream with one message.
func (r *DriftCheckReconciler) startFailure(
	ctx context.Context, dc *v1alpha1.DriftCheck, cause error,
) (ctrl.Result, error) {
	logf.FromContext(ctx).Error(cause, "check could not be started")

	r.record(dc, ReasonStartFailed, corev1.EventTypeWarning, "%s", cause.Error())

	err := r.patchStatus(ctx, dc, func(status *v1alpha1.DriftCheckStatus) {
		status.Phase = string(check.PhaseFailed)
		status.Message = cause.Error()
		status.ObservedGeneration = dc.Generation

		setCondition(status, metav1.Condition{
			Type:    v1alpha1.ConditionReady,
			Status:  metav1.ConditionFalse,
			Reason:  ReasonStartFailed,
			Message: cause.Error(),
		}, dc.Generation, r.Clock.Now())
	})
	if err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: r.SecretRetryInterval}, nil
}

// updateStatus writes the running check's state onto the object.
func (r *DriftCheckReconciler) updateStatus(
	ctx context.Context, dc *v1alpha1.DriftCheck,
) error {
	key := client.ObjectKeyFromObject(dc)

	runner := r.Runners.Get(key)
	if runner == nil {
		return nil
	}

	st := runner.Status()
	r.emitTransitions(dc, &st)

	generation := dc.Generation
	now := r.Clock.Now()

	return r.patchStatus(ctx, dc, func(status *v1alpha1.DriftCheckStatus) {
		renderStatus(status, &st, &dc.Spec, generation, now)
	})
}

// patchStatus applies a status change with optimistic concurrency.
//
// §10.3 requires Status().Patch with conflict retry, never Update from a stale
// copy, and the difference matters more than it sounds. The reconciler holds an
// object read at the start of the reconcile; the runner has been writing to its
// own state throughout, and the operator may have edited the spec in between.
// An Update would send the whole object back and silently revert that edit to
// whatever this reconcile happened to read.
func (r *DriftCheckReconciler) patchStatus(
	ctx context.Context, dc *v1alpha1.DriftCheck, mutate func(*v1alpha1.DriftCheckStatus),
) error {
	key := client.ObjectKeyFromObject(dc)

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// Re-read on every attempt, so a conflict is resolved against what is
		// actually stored rather than against the copy that just lost.
		var latest v1alpha1.DriftCheck
		if err := r.Get(ctx, key, &latest); err != nil {
			return err
		}

		patch := client.MergeFrom(latest.DeepCopy())
		mutate(&latest.Status)

		return r.Status().Patch(ctx, &latest, patch)
	})

	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("patching status of %s: %w", key, err)
	}
	return nil
}
