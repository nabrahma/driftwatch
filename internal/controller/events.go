package controller

import (
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/nabrahma/driftwatch/api/v1alpha1"
	"github.com/nabrahma/driftwatch/pkg/check"
)

// Events are what an operator sees first in `kubectl describe`, which makes
// them the highest-leverage output the controller has — and the easiest to
// ruin. A controller that emitted its state on every reconcile would produce
// four identical "drift detected" events a minute for one unchanging finding,
// and `kubectl describe` would be a wall of noise with the one interesting
// line somewhere in it.
//
// So this file emits transitions only. The tracker remembers what each check
// looked like last time and an event is written when the answer changes,
// which means the event list reads as a history of what happened rather than a
// sample of what is.

// The event reasons. They double as condition reasons, so an operator who has
// read one has read the other.
const (
	// ReasonBootstrapComplete reports the oracle becoming ready to sweep.
	ReasonBootstrapComplete = "BootstrapComplete"
	// ReasonDriftDetected reports confirmed divergence appearing or growing.
	ReasonDriftDetected = "DriftDetected"
	// ReasonDriftResolved reports the last confirmed divergence clearing.
	ReasonDriftResolved = "DriftResolved"
	// ReasonSourceDisconnected reports losing the event subscription.
	ReasonSourceDisconnected = "SourceDisconnected"
	// ReasonSourceReconnected reports getting it back.
	ReasonSourceReconnected = "SourceReconnected"
	// ReasonTargetUnavailable reports the store becoming unreadable.
	ReasonTargetUnavailable = "TargetUnavailable"
	// ReasonTargetAvailable reports it coming back.
	ReasonTargetAvailable = "TargetAvailable"
	// ReasonOracleSaturated reports the keyspace not fitting in the budget.
	ReasonOracleSaturated = "OracleSaturated"
	// ReasonPublisherRestart reports a publisher restarting.
	ReasonPublisherRestart = "PublisherRestart"

	// ReasonSecretMissing reports a secret reference that would not resolve.
	ReasonSecretMissing = "SecretMissing"
	// ReasonStartFailed reports a check that could not be built.
	ReasonStartFailed = "StartFailed"
)

// The condition reasons that are not also event reasons.
const (
	ReasonWatching         = "Watching"
	ReasonPaused           = "Paused"
	ReasonDegraded         = "Degraded"
	ReasonBootstrapping    = "Bootstrapping"
	ReasonAwaitingSnapshot = "AwaitingSnapshot"
	ReasonSnapshotSeen     = "SnapshotSeen"
	ReasonFailed           = "Failed"
	ReasonConnected        = "Connected"
	ReasonDisconnected     = "Disconnected"
	ReasonReachable        = "Reachable"
	ReasonUnreachable      = "Unreachable"
	ReasonDriftConfirmed   = "DriftConfirmed"
	ReasonNoDrift          = "NoDrift"
	ReasonSaturated        = "Saturated"
	ReasonWithinBudget     = "WithinBudget"
	ReasonComplete         = "Complete"
	ReasonGapsObserved     = "GapsObserved"
	ReasonMultiWriter      = "MultiWriterDetected"
	ReasonSingleWriter     = "SingleWriterPerKey"
	ReasonCommutative      = "Commutative"
	ReasonOrderDependent   = "OrderDependent"
	ReasonIntervalTight    = "SweepIntervalTight"
	ReasonIntervalAdequate = "SweepIntervalAdequate"
)

// observed is what a check looked like at the end of the last reconcile.
type observed struct {
	seen            bool
	bootstrapped    bool
	divergentKeys   int
	sourceConnected bool
	targetReachable bool
	oracleSaturated bool
	restarts        map[string]int64
}

// transitionTracker remembers the last observation of every check.
type transitionTracker struct {
	mu    sync.Mutex
	state map[types.NamespacedName]*observed
}

func newTransitionTracker() *transitionTracker {
	return &transitionTracker{state: map[types.NamespacedName]*observed{}}
}

func (t *transitionTracker) forget(key types.NamespacedName) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.state, key)
}

// record swaps in a new observation and returns the previous one.
func (t *transitionTracker) record(key types.NamespacedName, next *observed) observed {
	t.mu.Lock()
	defer t.mu.Unlock()

	previous, ok := t.state[key]
	t.state[key] = next

	if !ok {
		// First sight of this check. Everything reads as "already in this
		// state" so that a manager restart does not replay the whole history
		// of every check it adopts.
		return observed{
			seen:            false,
			bootstrapped:    next.bootstrapped,
			divergentKeys:   next.divergentKeys,
			sourceConnected: next.sourceConnected,
			targetReachable: next.targetReachable,
			oracleSaturated: next.oracleSaturated,
			restarts:        next.restarts,
		}
	}
	return *previous
}

// emitTransitions writes an event for each of §10.3's eight cases that changed.
func (r *DriftCheckReconciler) emitTransitions(dc *v1alpha1.DriftCheck, st *check.Status) {
	key := client.ObjectKeyFromObject(dc)

	next := &observed{
		seen:            true,
		bootstrapped:    st.Bootstrapped,
		divergentKeys:   st.DivergentKeys,
		sourceConnected: st.SourceConnected,
		targetReachable: st.TargetReachable,
		oracleSaturated: st.OracleSaturated,
		restarts:        publisherRestarts(st),
	}
	previous := r.events.record(key, next)

	// 1. Bootstrap complete. The moment the check starts asserting anything.
	if st.Bootstrapped && !previous.bootstrapped {
		r.record(dc, ReasonBootstrapComplete, corev1.EventTypeNormal,
			"oracle ready: %d keys tracked, comparing against the target",
			st.TrackedKeys)
	}

	// 2 and 3. Drift appearing, growing, and clearing. Growth is worth an event
	// of its own: "12 keys" becoming "4,000 keys" is a different incident from
	// the one that was already open.
	switch {
	case st.DivergentKeys > previous.divergentKeys:
		r.record(dc, ReasonDriftDetected, corev1.EventTypeWarning,
			"%d confirmed divergent keys (was %d)",
			st.DivergentKeys, previous.divergentKeys)

	case st.DivergentKeys == 0 && previous.divergentKeys > 0:
		r.record(dc, ReasonDriftResolved, corev1.EventTypeNormal,
			"divergence resolved: %d keys agree again", previous.divergentKeys)
	}

	// 4 and 5. The subscription. Suppressed until the check has been observed
	// once, or every newly adopted check would report a disconnection it never
	// had.
	if previous.seen && st.SourceConnected != previous.sourceConnected {
		if st.SourceConnected {
			r.record(dc, ReasonSourceReconnected, corev1.EventTypeNormal,
				"event subscription re-established after %d reconnects",
				st.SourceReconnects)
		} else {
			r.record(dc, ReasonSourceDisconnected, corev1.EventTypeWarning,
				"event subscription lost: %s; keys will go stale while it is down",
				orUnknown(st.SourceLastError))
		}
	}

	// 6. The store. §23 A5 is in the message on purpose: the operator reading
	// this needs to know that the counts they can see are frozen rather than
	// current.
	if previous.seen && st.TargetReachable != previous.targetReachable {
		if st.TargetReachable {
			r.record(dc, ReasonTargetAvailable, corev1.EventTypeNormal,
				"target reachable again; comparison resumed")
		} else {
			r.record(dc, ReasonTargetUnavailable, corev1.EventTypeWarning,
				"target unreachable: nothing is being compared, and the reported "+
					"counts are the last ones driftwatch actually knew")
		}
	}

	// 7. Saturation. The one that changes how every other number should be read.
	if st.OracleSaturated && !previous.oracleSaturated {
		r.record(dc, ReasonOracleSaturated, corev1.EventTypeWarning,
			"the keyspace did not fit in policy.maxTrackedKeys: %d evictions, "+
				"so every finding now covers only part of the store",
			st.OracleEvictions)
	}

	// 8. Publisher restarts.
	r.emitRestarts(dc, st, &previous)
}

// emitRestarts reports each publisher whose restart count moved.
func (r *DriftCheckReconciler) emitRestarts(
	dc *v1alpha1.DriftCheck, st *check.Status, previous *observed,
) {
	if !previous.seen {
		return
	}

	for i := range st.Publishers {
		p := &st.Publishers[i]
		count := int64(p.Restarts) //nolint:gosec // a counter

		if count <= previous.restarts[p.ID] {
			continue
		}
		r.record(dc, ReasonPublisherRestart, corev1.EventTypeWarning,
			"publisher %s restarted (epoch %d, restart %d); its keys are suspect "+
				"until it retransmits", p.ID, p.Epoch, count)
	}
}

func publisherRestarts(st *check.Status) map[string]int64 {
	if len(st.Publishers) == 0 {
		return nil
	}

	out := make(map[string]int64, len(st.Publishers))
	for i := range st.Publishers {
		p := &st.Publishers[i]
		out[p.ID] = int64(p.Restarts) //nolint:gosec // a counter
	}
	return out
}

// record emits one event, if a recorder is configured.
func (r *DriftCheckReconciler) record(
	dc *v1alpha1.DriftCheck, reason, eventType, format string, args ...any,
) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(dc, eventType, reason, format, args...)
}

func orUnknown(s string) string {
	if s == "" {
		return "no error reported"
	}
	return s
}
