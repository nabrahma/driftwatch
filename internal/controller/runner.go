package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/types"

	"github.com/nabrahma/driftwatch/api/v1alpha1"
	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/metrics"
)

// The registry is the part of the controller most able to do quiet damage.
//
// A reconciler that starts a runner without stopping the old one leaves two
// checks folding the same stream into two oracles and sweeping the same store.
// Nothing crashes. Both emit metrics under the same `check` label, so the
// divergent-key gauge flips between two values depending on which wrote last,
// and the orphaned runner holds a full oracle — up to the whole memory budget —
// that no reconcile will ever find again to stop. It presents as flapping drift
// and a slow leak, and neither symptom points here.
//
// Two things prevent it. Every mutation of a key's runner happens under that
// key's own mutex, so a stop and the start that replaces it cannot interleave
// with another reconcile of the same key. And the map is only ever written by
// code holding that lock, so a runner that exists is either in the map or on
// its way in, never orphaned.
//
// The lock is per key rather than global on purpose: §10.3 requires many checks
// in one manager, and a global lock would let one slow shutdown stall every
// other check's reconcile.

// Runnable is what the registry supervises.
//
// *check.Check satisfies it. The interface exists so the registry's own
// behavior — restart, panic isolation, shutdown deadlines — is testable
// against a runnable that does exactly what a test needs, without contorting a
// real check into bootstrapping slowly or panicking on cue.
type Runnable interface {
	// Run blocks until the context is canceled or the check fails.
	Run(ctx context.Context) error
	// Status reports what the check currently knows.
	Status() check.Status
	// SetPaused suspends or resumes sweeping without restarting anything.
	//
	// Part of the interface rather than an optional one a type assertion looks
	// for, because a Runnable that quietly did not implement it would leave
	// pausing silently doing nothing — and the way that presents is a check
	// that goes on alerting through the deploy it was silenced for.
	SetPaused(paused bool)
	// Close releases everything the check holds.
	Close() error
}

// BuildFunc constructs a runnable from a spec.
//
// The spec travels by value the whole way down, matching check.New. It is 472
// bytes and a pointer would be cheaper, but New mutates its copy — it applies
// defaults before validating — so handing it a pointer would rewrite the
// caller's spec as a side effect of construction. Once per check start is not a
// cost worth that.
//
//nolint:gocritic // hugeParam: by value deliberately; see above
type BuildFunc func(spec check.Spec, deps check.Deps) (Runnable, error)

// buildCheck is the production builder.
//
//nolint:gocritic // hugeParam: matches BuildFunc
func buildCheck(spec check.Spec, deps check.Deps) (Runnable, error) {
	return check.New(spec, deps)
}

// RegistryOptions configures a RunnerRegistry.
type RegistryOptions struct {
	// Logger receives the lifecycle lines. The zero value is a working no-op.
	Logger logr.Logger
	// Clock is the injected clock, used for the restart rate limit and by the
	// checks themselves. Defaults to the real one.
	Clock clock.Clock
	// Metrics is the process-wide metric set, passed to every check.
	Metrics *metrics.Metrics
	// Build constructs a runnable. Defaults to check.New.
	Build BuildFunc
	// RestartInterval is the minimum time between restarts of one check. §10.3
	// asks for once per 5s, so a spec edited ten times in ten seconds does not
	// rebuild ten oracles. Zero uses that default; a negative value disables
	// the throttle, which is what a test that wants to observe every restart
	// needs and what an operator debugging a specific check may want.
	RestartInterval time.Duration
	// ShutdownGrace bounds how long a stop waits for a runner before giving up.
	// §10.3 requires a check deleted mid-bootstrap to terminate promptly, and a
	// manager holding fifty checks to stop within the grace period.
	ShutdownGrace time.Duration
}

// Defaults for RegistryOptions.
const (
	DefaultRestartInterval = 5 * time.Second
	DefaultShutdownGrace   = 30 * time.Second
)

// Runner is one supervised check.
type Runner struct {
	// Key identifies the DriftCheck this runner serves.
	Key types.NamespacedName
	// Hash is the spec hash the runner was started from. A reconcile that
	// computes the same hash leaves the runner alone.
	Hash string
	// StartedAt is when the runner was created.
	StartedAt time.Time

	runnable Runnable
	cancel   context.CancelFunc
	done     chan struct{}

	mu      sync.Mutex
	runErr  error
	stopped bool
}

// Status reports the check's state, or a Failed phase if the runner died.
//
// A runner whose goroutine has exited still answers, because the reconciler has
// to write the reason into the CRD's status. A registry that simply forgot a
// dead runner would leave the object saying Bootstrapping forever.
func (r *Runner) Status() check.Status {
	st := r.runnable.Status()

	if err := r.Err(); err != nil {
		st.Phase = check.PhaseFailed
		st.Message = err.Error()
	}
	return st
}

// Err returns the error the runner exited with, or nil while it is running.
func (r *Runner) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.runErr
}

// Done closes when the runner's goroutine has exited.
func (r *Runner) Done() <-chan struct{} { return r.done }

// Check returns the supervised runnable.
func (r *Runner) Check() Runnable { return r.runnable }

// RunnerRegistry owns every running check in the process.
type RunnerRegistry struct {
	opts RegistryOptions
	log  logr.Logger
	clk  clock.Clock

	// mu guards the maps below. It is held only for map operations, never
	// across a start or a stop — those hold the per-key lock instead, so one
	// slow shutdown cannot stall an unrelated check.
	mu        sync.Mutex
	closed    bool
	runners   map[types.NamespacedName]*Runner
	locks     map[types.NamespacedName]*sync.Mutex
	waiters   map[types.NamespacedName]int
	lastStart map[types.NamespacedName]time.Time
}

// ErrRegistryClosed reports an attempt to start a check after shutdown.
//
// It is not a failure to report on the object: it means the manager is going
// away, and the check will be started by whichever replica takes the lease.
var ErrRegistryClosed = errors.New("the runner registry has been shut down")

// NewRunnerRegistry builds an empty registry.
func NewRunnerRegistry(opts RegistryOptions) *RunnerRegistry {
	if opts.Clock == nil {
		opts.Clock = clock.Real()
	}
	if opts.Build == nil {
		opts.Build = buildCheck
	}
	if opts.RestartInterval == 0 {
		opts.RestartInterval = DefaultRestartInterval
	}
	if opts.ShutdownGrace <= 0 {
		opts.ShutdownGrace = DefaultShutdownGrace
	}

	return &RunnerRegistry{
		opts:      opts,
		log:       opts.Logger,
		clk:       opts.Clock,
		runners:   map[types.NamespacedName]*Runner{},
		locks:     map[types.NamespacedName]*sync.Mutex{},
		waiters:   map[types.NamespacedName]int{},
		lastStart: map[types.NamespacedName]time.Time{},
	}
}

// Action reports what Ensure did, so the reconciler can emit the right event
// and set the right phase without inspecting the registry again.
type Action int

// The outcomes of Ensure.
const (
	// ActionUnchanged means a runner with the same hash was already running.
	ActionUnchanged Action = iota
	// ActionStarted means no runner existed and one was started.
	ActionStarted
	// ActionRestarted means the spec changed, so the old runner was stopped and
	// a new one started.
	ActionRestarted
	// ActionThrottled means the spec changed but the previous start was too
	// recent. The old runner is still running and the reconcile should be
	// retried after RetryAfter.
	ActionThrottled
)

// String renders the action for a log line.
func (a Action) String() string {
	switch a {
	case ActionStarted:
		return "started"
	case ActionRestarted:
		return "restarted"
	case ActionThrottled:
		return "throttled"
	case ActionUnchanged:
		return "unchanged"
	default:
		return "unknown"
	}
}

// Outcome is what Ensure did and when to come back.
type Outcome struct {
	// Action is what happened.
	Action Action
	// RetryAfter is how long to wait before reconciling again. Non-zero only
	// when the restart was throttled.
	RetryAfter time.Duration
}

// Ensure makes the registry hold exactly one runner for key, at hash.
//
// Everything it does to that key happens under the key's own mutex, which is
// what makes "never two runners for one check" a property of the code rather
// than a hope about reconcile timing.
//
//nolint:gocritic // hugeParam: the spec goes to check.New by value; see BuildFunc
func (r *RunnerRegistry) Ensure(
	ctx context.Context, key types.NamespacedName, hash string, spec check.Spec,
) (Outcome, error) {
	unlock := r.lock(key)
	defer unlock()

	if r.isClosed() {
		return Outcome{}, ErrRegistryClosed
	}

	existing := r.get(key)

	switch {
	case existing == nil:
		// Nothing running. Fall through to the start below.

	case existing.Hash == hash && existing.Err() == nil:
		// The one field applied to a live runner rather than by replacing it.
		// SpecHash leaves policy.paused out, so a pause arrives here as an
		// unchanged spec — which is the point: the oracle, the sequence tracker
		// and every key's trust state carry through untouched.
		//
		// Idempotent, and it has to be: this runs on every reconcile of an
		// unchanged object, not only when the operator edits one.
		existing.Check().SetPaused(spec.Policy.Paused)
		return Outcome{Action: ActionUnchanged}, nil

	case existing.Hash == hash:
		// Same spec, but the runner died. Restarting is right — the alternative
		// is a check that stays dead until someone happens to edit its spec —
		// but it goes through the same debounce as a spec change, which turns
		// it into a crash-loop backoff. Without that, a check that panics
		// during bootstrap would be rebuilt on every status refresh, and the
		// object's phase would alternate between Bootstrapping and Failed
		// rather than settling on the failure an operator has to see to act.
		if wait := r.throttle(key); wait > 0 {
			return Outcome{Action: ActionThrottled, RetryAfter: wait}, nil
		}
		r.log.Info("restarting a failed runner",
			"check", key.String(), "error", existing.Err())

	default:
		// §10.3's debounce. A spec edited repeatedly must not rebuild the
		// oracle on every edit: bootstrap is the expensive part, and the
		// intermediate specs are ones nobody wanted running anyway.
		if wait := r.throttle(key); wait > 0 {
			return Outcome{Action: ActionThrottled, RetryAfter: wait}, nil
		}
		r.log.Info("spec changed, replacing the runner",
			"check", key.String(), "from", existing.Hash, "to", hash)
	}

	action := ActionStarted
	if existing != nil {
		if err := r.stopLocked(ctx, key, existing); err != nil {
			return Outcome{}, err
		}
		action = ActionRestarted
	}

	if err := r.startLocked(ctx, key, hash, spec); err != nil {
		return Outcome{}, err
	}

	// Re-check under the same per-key lock the start happened under, and this
	// is not belt and braces — it closes a window that a real shutdown hits.
	//
	// controller-runtime cancels its leader-elected runnables together, so the
	// stopper can finish while a reconcile is still in flight. That reconcile
	// would then start a runner nothing would ever stop: the manager has gone,
	// so no further reconcile will arrive, and the goroutine would go on
	// sweeping a store for a lease this process no longer holds.
	//
	// Shutdown latches `closed` before it enumerates. So either this re-check
	// sees the latch and undoes its own start, or the latch came afterwards —
	// in which case the entry was already in the map when Shutdown enumerated,
	// and it will be stopped there.
	if r.isClosed() {
		if runner := r.get(key); runner != nil {
			if err := r.stopLocked(ctx, key, runner); err != nil {
				return Outcome{}, err
			}
		}
		return Outcome{}, ErrRegistryClosed
	}

	return Outcome{Action: action}, nil
}

// isClosed reports whether Shutdown has been called.
func (r *RunnerRegistry) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

// Shutdown stops every runner and refuses to start any more.
//
// The latch is what makes it final. StopAll alone would leave the registry
// willing to accept a runner from a reconcile that was already in flight, and
// that runner would outlive the manager that started it.
func (r *RunnerRegistry) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()

	return r.StopAll(ctx)
}

// startLocked builds and starts a runner. The caller holds key's mutex.
//
//nolint:gocritic // hugeParam: the spec goes to check.New by value; see BuildFunc
func (r *RunnerRegistry) startLocked(
	ctx context.Context, key types.NamespacedName, hash string, spec check.Spec,
) error {
	runnable, err := r.opts.Build(spec, check.Deps{
		Clock:   r.clk,
		Logger:  r.log.WithValues("check", key.String()),
		Metrics: r.opts.Metrics,
	})
	if err != nil {
		return fmt.Errorf("building check %s: %w", key, err)
	}

	// The runner's context descends from the manager's, so leadership loss or
	// process shutdown cancels every check without the registry having to
	// enumerate them.
	runCtx, cancel := context.WithCancel(ctx)

	runner := &Runner{
		Key:       key,
		Hash:      hash,
		StartedAt: r.clk.Now(),
		runnable:  runnable,
		cancel:    cancel,
		done:      make(chan struct{}),
	}

	r.mu.Lock()
	r.runners[key] = runner
	r.lastStart[key] = runner.StartedAt
	r.mu.Unlock()

	go r.supervise(runCtx, runner)

	r.log.Info("started runner", "check", key.String(), "hash", hash)
	return nil
}

// supervise runs one check and survives its failure.
//
// §10.3 requires that a panic or fatal error in one check cannot affect
// another. Without the recover here, a nil map write in one projection would
// take down the manager and every other check with it — and because the manager
// holds the lease, the standby would take the lease duration to notice.
func (r *RunnerRegistry) supervise(ctx context.Context, runner *Runner) {
	defer close(runner.done)

	defer func() {
		if v := recover(); v != nil {
			err := fmt.Errorf("check panicked: %v", v)

			r.log.Error(err, "runner panicked; the other checks are unaffected",
				"check", runner.Key.String(), "stack", string(debug.Stack()))

			runner.mu.Lock()
			runner.runErr = err
			runner.mu.Unlock()
		}
	}()

	err := runner.runnable.Run(ctx)

	// A canceled context is how every clean stop ends, so it is not a failure.
	// Recording it as one would put "context canceled" in the status of every
	// check that was ever paused, restarted or deleted.
	if err != nil && !errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded) {
		r.log.Error(err, "runner exited", "check", runner.Key.String())

		runner.mu.Lock()
		runner.runErr = err
		runner.mu.Unlock()
	}
}

// Stop stops and forgets the runner for key. It is a no-op if there is none.
func (r *RunnerRegistry) Stop(ctx context.Context, key types.NamespacedName) error {
	unlock := r.lock(key)
	defer unlock()

	runner := r.get(key)
	if runner == nil {
		return nil
	}
	return r.stopLocked(ctx, key, runner)
}

// stopLocked cancels a runner, waits for it, and removes it from the map. The
// caller holds key's mutex.
func (r *RunnerRegistry) stopLocked(
	ctx context.Context, key types.NamespacedName, runner *Runner,
) error {
	runner.mu.Lock()
	already := runner.stopped
	runner.stopped = true
	runner.mu.Unlock()

	if !already {
		runner.cancel()
	}

	// The map entry goes before the wait rather than after. A caller that gives
	// up on a slow shutdown must not leave behind a runner the next reconcile
	// would find and treat as live, and Len must never count a runner that has
	// already been told to stop.
	r.mu.Lock()
	if r.runners[key] == runner {
		delete(r.runners, key)
	}
	r.mu.Unlock()

	err := r.wait(ctx, runner)

	// Close after the goroutine has gone, or Close would release a target the
	// run loop is still reading through.
	if closeErr := runner.runnable.Close(); closeErr != nil && err == nil {
		err = fmt.Errorf("closing check %s: %w", key, closeErr)
	}

	// The check is gone, so its series must go with it.
	//
	// Left behind, every gauge freezes at whatever it last held. A check deleted
	// while it had drift keeps exporting that number forever, so the §12.2
	// alert on it keeps firing about an object that no longer exists, and the
	// only way to clear it is to restart the manager — which discards every
	// other check's history at the same time.
	//
	// After the wait rather than before: a runner still draining could write one
	// more sample, and deleting first would leave exactly the series this is
	// meant to remove.
	if r.opts.Metrics != nil {
		r.opts.Metrics.ForgetCheck(key.String())
	}

	r.log.Info("stopped runner", "check", key.String(), "hash", runner.Hash)
	return err
}

// wait blocks until the runner's goroutine exits or the grace period expires.
//
// It deliberately does not abandon the wait when the caller's context is
// canceled. That context is canceled the moment the manager shuts down, which
// is exactly when a half-stopped runner does the most damage: returning early
// would call Close on a target the run loop is still reading through.
func (r *RunnerRegistry) wait(ctx context.Context, runner *Runner) error {
	timer := r.clk.NewTimer(r.opts.ShutdownGrace)
	defer timer.Stop()

	select {
	case <-runner.done:
		return nil
	case <-timer.C():
		return fmt.Errorf("check %s did not stop within %s",
			runner.Key, r.opts.ShutdownGrace)
	case <-ctx.Done():
		select {
		case <-runner.done:
			return nil
		case <-timer.C():
			return fmt.Errorf("check %s did not stop within %s",
				runner.Key, r.opts.ShutdownGrace)
		}
	}
}

// StopAll stops every runner, in parallel.
//
// §10.3 requires a manager holding fifty checks to shut down within the grace
// period. Stopping them one at a time would make that fifty grace periods in
// the worst case, and a pod that misses its termination grace is SIGKILLed —
// which skips every Close in the process.
func (r *RunnerRegistry) StopAll(ctx context.Context) error {
	keys := r.Keys()

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)

	for _, key := range keys {
		wg.Add(1)
		go func(key types.NamespacedName) {
			defer wg.Done()
			if err := r.Stop(ctx, key); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}(key)
	}
	wg.Wait()

	return errors.Join(errs...)
}

// Get returns the runner for key, or nil.
func (r *RunnerRegistry) Get(key types.NamespacedName) *Runner {
	return r.get(key)
}

// Len returns how many runners are registered.
func (r *RunnerRegistry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.runners)
}

// Keys returns every registered key.
func (r *RunnerRegistry) Keys() []types.NamespacedName {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]types.NamespacedName, 0, len(r.runners))
	for key := range r.runners {
		out = append(out, key)
	}
	return out
}

// get reads the map without taking the per-key lock.
func (r *RunnerRegistry) get(key types.NamespacedName) *Runner {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.runners[key]
}

// throttle returns how long to wait before this key may restart again.
func (r *RunnerRegistry) throttle(key types.NamespacedName) time.Duration {
	if r.opts.RestartInterval < 0 {
		return 0
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	last, ok := r.lastStart[key]
	if !ok {
		return 0
	}

	elapsed := r.clk.Now().Sub(last)
	if elapsed >= r.opts.RestartInterval {
		return 0
	}
	return r.opts.RestartInterval - elapsed
}

// lock takes key's mutex and returns the release.
//
// The mutexes are created on demand and dropped when nobody holds or wants
// them, so a controller that has reconciled ten thousand deleted DriftChecks
// does not keep ten thousand mutexes.
func (r *RunnerRegistry) lock(key types.NamespacedName) func() {
	r.mu.Lock()
	mu, ok := r.locks[key]
	if !ok {
		mu = &sync.Mutex{}
		r.locks[key] = mu
	}
	r.waiters[key]++
	r.mu.Unlock()

	mu.Lock()

	return func() {
		mu.Unlock()

		r.mu.Lock()
		defer r.mu.Unlock()

		r.waiters[key]--
		if r.waiters[key] <= 0 {
			delete(r.locks, key)
			delete(r.waiters, key)
		}
	}
}

// Forget drops the restart rate-limit record for a key.
//
// Called when a DriftCheck is deleted. Without it, recreating a check with the
// same name inside the restart interval would be throttled on the strength of a
// runner that no longer exists.
func (r *RunnerRegistry) Forget(key types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.lastStart, key)
}

// ---------------------------------------------------------------------------
// Spec hashing.
// ---------------------------------------------------------------------------

// SpecHash is the fingerprint a reconcile compares against a running runner.
//
// It covers the spec and the resolved secret material, and the second half is
// the one that is easy to leave out. A rotated Redis password produces an
// identical spec: without the secret in the hash, the check would keep running
// against a pool holding the old credentials and report the store as
// unreachable until somebody happened to edit the DriftCheck.
//
// Only a digest of each secret goes in, never the value, so the hash is safe to
// put in a log line and in the object's status.
func SpecHash(spec *v1alpha1.DriftCheckSpec, secrets map[string]string) (string, error) {
	// policy.paused is deliberately not part of the hash.
	//
	// The hash decides whether the runner is replaced, and replacing it throws
	// away the oracle. Pause exists precisely so an operator can silence a
	// check during a deploy and have it resume with the keyspace it already
	// knows — so a pause that restarted the runner would destroy the thing it
	// exists to preserve, and then the check would come back adopting whatever
	// the store happened to hold at that moment. Adopting from the store it is
	// meant to be auditing is the one starting position that cannot detect
	// anything wrong with it.
	//
	// Ensure applies this field to the running check instead, which is why
	// leaving it out here does not lose it.
	hashed := *spec
	hashed.Policy.Paused = false

	encoded, err := json.Marshal(&hashed)
	if err != nil {
		return "", fmt.Errorf("hashing spec: %w", err)
	}

	sum := sha256.New()
	sum.Write(encoded)

	// Sorted, because a hash that depended on map iteration order would differ
	// between two reconciles of an unchanged object — which is a restart loop
	// that only appears once a check has more than one secret.
	fields := make([]string, 0, len(secrets))
	for field := range secrets {
		fields = append(fields, field)
	}
	sort.Strings(fields)

	for _, field := range fields {
		sum.Write([]byte(field))
		digest := sha256.Sum256([]byte(secrets[field]))
		sum.Write(digest[:])
	}

	return hex.EncodeToString(sum.Sum(nil))[:hashLength], nil
}

// hashLength is how much of the digest is kept.
//
// 128 bits, which is far past the point where an accidental collision between
// two specs is worth thinking about, and short enough to read in a log line and
// in `kubectl get driftcheck -o yaml`.
const hashLength = 32
