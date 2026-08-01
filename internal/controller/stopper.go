package controller

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// RunnerStopper stops every check when this manager stops leading.
//
// §10.3 requires leader election and requires all runners to stop on leadership
// loss, and the second half is the one that needs code. controller-runtime
// stops calling Reconcile when the lease goes, but a runner is a goroutine this
// process started: nothing cancels it just because no more reconciles arrive.
// Two managers each convinced they hold the lease would sweep the same store,
// write metrics under the same check label, and patch the same status from two
// different oracles — and the symptom would be a divergent-key count that
// alternates between two values, which reads like flapping drift rather than
// like a split brain.
//
// Registering it as a leader-elected runnable makes the guarantee structural:
// controller-runtime starts it only after the lease is acquired and cancels its
// context the moment the lease is lost, so the stop is on the same path as
// every other leader-elected shutdown rather than in a defer nobody reaches
// when the process is killed.
type RunnerStopper struct {
	runners *RunnerRegistry
	log     logr.Logger
}

// stopTimeout is the outer bound on stopping everything.
//
// The registry's own ShutdownGrace bounds each runner, and the stops run in
// parallel, so this only fires if something below is not honoring its own
// deadline. Generous enough never to truncate a legitimate shutdown, finite so
// that a manager cannot hang forever on the way out — a pod that overruns its
// termination grace is SIGKILLed, which skips every Close in the process.
const stopTimeout = 2 * time.Minute

// NewRunnerStopper builds the runnable.
func NewRunnerStopper(runners *RunnerRegistry, log logr.Logger) *RunnerStopper {
	return &RunnerStopper{runners: runners, log: log.WithName("stopper")}
}

// Start blocks until the manager stops leading, then stops every runner.
func (s *RunnerStopper) Start(ctx context.Context) error {
	<-ctx.Done()

	s.log.Info("leadership released; stopping every check", "runners", s.runners.Len())

	// The parent's values without its cancellation. ctx is already done — that
	// is what woke this up — and passing it on would work, because the wait in
	// the registry deliberately survives a canceled context. Relying on that
	// here would make this depend on a subtlety two files away, so the
	// detachment is explicit instead. WithoutCancel rather than Background so
	// anything carried on the context, such as a logger, comes with it.
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stopTimeout)
	defer cancel()

	// Shutdown rather than StopAll: it latches the registry closed first, so a
	// reconcile still in flight cannot start a runner behind this one's back.
	// controller-runtime cancels its leader-elected runnables together, so that
	// really does happen.
	if err := s.runners.Shutdown(stopCtx); err != nil {
		s.log.Error(err, "some checks did not stop cleanly")
		return err
	}

	s.log.Info("every check stopped")
	return nil
}

// NeedLeaderElection makes this a leader-elected runnable, which is what ties
// its context to the lease.
func (s *RunnerStopper) NeedLeaderElection() bool { return true }

var (
	_ manager.Runnable               = (*RunnerStopper)(nil)
	_ manager.LeaderElectionRunnable = (*RunnerStopper)(nil)
)
