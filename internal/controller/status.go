package controller

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/nabrahma/driftwatch/api/v1alpha1"
	"github.com/nabrahma/driftwatch/pkg/check"
)

// The status block is the only thing most operators will ever read about a
// check, so what it says has to survive being read quickly and out of context.
//
// The rule this file follows throughout is §23 A7: divergentKeys is what
// driftwatch will stand behind, suspectDivergentKeys is what it suspects while
// knowing its own view is incomplete, and the two never merge. A status that
// added them together would be a status that pages someone about driftwatch's
// event loss, and an alert an operator learns to ignore is worse than no alert.

// renderStatus writes a check's state onto the CRD status block.
func renderStatus(
	status *v1alpha1.DriftCheckStatus,
	st *check.Status,
	spec *v1alpha1.DriftCheckSpec,
	generation int64,
	now time.Time,
) {
	status.Phase = string(st.Phase)
	status.Message = st.Message
	status.ObservedGeneration = generation

	status.DivergentKeys = st.DivergentKeys
	status.SuspectDivergentKeys = st.SuspectDivergentKeys
	status.DivergenceByCategory = nonEmpty(st.DivergenceByCategory)
	status.DriftDurationSeconds = seconds(st.DriftDurationSeconds)

	status.TrackedKeys = st.TrackedKeys
	status.SettledKeys = st.SettledKeys
	status.InFlightKeys = st.InFlightKeys
	status.CoverageRatio = ratio(st.CoverageRatio)

	status.SettlementWindowSeconds = seconds(st.SettlementWindowSeconds)
	status.ConvergenceP99Seconds = seconds(st.ConvergenceP99Seconds)

	status.LastSweepDurationSeconds = seconds(st.LastSweepDurationSeconds)
	status.LastSweepKeysCompared = st.LastSweepKeysCompared
	status.SweepsSkipped = st.SweepsSkipped
	if !st.LastSweepTime.IsZero() {
		t := metav1.NewTime(st.LastSweepTime)
		status.LastSweepTime = &t
	}

	status.TargetReachable = st.TargetReachable
	status.TargetRole = st.TargetRole
	status.TargetKeyspaceSize = st.TargetKeyspaceSize

	status.EventsApplied = int64(st.EventsApplied) //nolint:gosec // a count, not a size
	status.EventsDropped = int64(st.EventsDropped) //nolint:gosec // a count, not a size
	status.Publishers = renderPublishers(st)

	renderConditions(status, st, spec, generation, now)
}

// renderPublishers copies the per-publisher sequence state.
//
// Sorted by id, because an unsorted list would reorder itself between
// reconciles and make every status patch a change — which is watch churn, and
// makes `kubectl diff` useless for seeing what actually moved.
func renderPublishers(st *check.Status) []v1alpha1.PublisherStatus {
	if len(st.Publishers) == 0 {
		return nil
	}

	out := make([]v1alpha1.PublisherStatus, 0, len(st.Publishers))
	for i := range st.Publishers {
		p := &st.Publishers[i]
		out = append(out, v1alpha1.PublisherStatus{
			ID:               p.ID,
			Epoch:            int64(p.Epoch),         //nolint:gosec // a counter
			HighWaterMark:    int64(p.HighWaterMark), //nolint:gosec // a counter
			MissingEvents:    int64(p.MissingEvents), //nolint:gosec // a counter
			Restarts:         int64(p.Restarts),      //nolint:gosec // a counter
			LastSeenSeconds:  seconds(p.LastSeenSeconds),
			ClockSkewSeconds: seconds(p.ClockSkewSeconds),
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// renderConditions sets every condition §10.1 lists.
//
// All of them, every time, including the ones that are true. A condition that
// only appears when it is false leaves an operator unable to tell "this is
// fine" from "this has not been evaluated", and those need different responses.
//
//nolint:gocyclo // one condition per branch; splitting it would only move the list
func renderConditions(
	status *v1alpha1.DriftCheckStatus,
	st *check.Status,
	spec *v1alpha1.DriftCheckSpec,
	generation int64,
	now time.Time,
) {
	set := func(c metav1.Condition) { setCondition(status, c, generation, now) }

	// Ready. Paused counts as ready: the check is doing what it was told to do,
	// and reporting it as not ready would light up every dashboard that gates
	// on readiness the moment somebody silenced one check for a deploy.
	switch st.Phase {
	case check.PhaseWatching:
		set(condition(v1alpha1.ConditionReady, true, ReasonWatching,
			"comparing the oracle against the target"))
	case check.PhasePaused:
		set(condition(v1alpha1.ConditionReady, true, ReasonPaused,
			"policy.paused is set: ingesting, not sweeping"))
	case check.PhaseDegraded:
		set(condition(v1alpha1.ConditionReady, true, ReasonDegraded, degradedMessage(st)))
	case check.PhaseBootstrapping, check.PhasePending:
		set(condition(v1alpha1.ConditionReady, false, ReasonBootstrapping,
			"building the oracle from the event stream"))
	case check.PhaseAwaitingSnapshot:
		set(condition(v1alpha1.ConditionReady, false, ReasonAwaitingSnapshot,
			"bootstrap=Strict: nothing is asserted until a publisher retransmits"))
	case check.PhaseFailed:
		set(condition(v1alpha1.ConditionReady, false, ReasonFailed, st.Message))
	default:
		set(condition(v1alpha1.ConditionReady, false, ReasonFailed, st.Message))
	}

	set(condition(v1alpha1.ConditionSourceConnected, st.SourceConnected,
		reasonFor(st.SourceConnected, ReasonConnected, ReasonDisconnected),
		sourceMessage(st)))

	set(condition(v1alpha1.ConditionTargetAvailable, st.TargetReachable,
		reasonFor(st.TargetReachable, ReasonReachable, ReasonUnreachable),
		targetMessage(st)))

	drift := st.DivergentKeys > 0
	set(condition(v1alpha1.ConditionDriftDetected, drift,
		reasonFor(drift, ReasonDriftConfirmed, ReasonNoDrift), driftMessage(st)))

	set(condition(v1alpha1.ConditionOracleSaturated, st.OracleSaturated,
		reasonFor(st.OracleSaturated, ReasonSaturated, ReasonWithinBudget),
		saturationMessage(st, spec)))

	missing := missingEvents(st)
	set(condition(v1alpha1.ConditionSequenceIntegrity, missing == 0,
		reasonFor(missing == 0, ReasonComplete, ReasonGapsObserved),
		sequenceMessage(st, missing)))

	set(condition(v1alpha1.ConditionAwaitingSnapshot, st.AwaitingSnapshot,
		reasonFor(st.AwaitingSnapshot, ReasonAwaitingSnapshot, ReasonSnapshotSeen),
		snapshotMessage(st)))

	// The two advisory conditions. Neither blocks anything; both exist so that
	// a finding an operator is about to distrust comes with the reason it might
	// be wrong, attached to the object rather than buried in a log line.
	set(condition(v1alpha1.ConditionMultiWriterUnsafe, st.MultiWriterUnsafe,
		reasonFor(st.MultiWriterUnsafe, ReasonMultiWriter, ReasonSingleWriter),
		multiWriterMessage(st)))

	if spec != nil {
		commutative := spec.Projection.Type != "counter" || spec.Projection.IncrOnly
		set(condition(v1alpha1.ConditionProjectionNotCommutative, !commutative,
			reasonFor(commutative, ReasonCommutative, ReasonOrderDependent),
			commutativeMessage(commutative)))

		tight := sweepIntervalTight(spec, st)
		set(condition(v1alpha1.ConditionSweepIntervalTight, tight,
			reasonFor(tight, ReasonIntervalTight, ReasonIntervalAdequate),
			sweepIntervalMessage(spec, st, tight)))
	}
}

// sweepIntervalTight reports whether sweeps are scheduled closer together than
// twice the settlement window currently in force.
//
// It is evaluated against the running window rather than the configured one,
// because in adaptive mode the window moves: a configuration that was
// comfortable at a 1s window is not at 40s, and the operator has no other way
// to find that out.
func sweepIntervalTight(spec *v1alpha1.DriftCheckSpec, st *check.Status) bool {
	window := time.Duration(st.SettlementWindowSeconds * float64(time.Second))
	return window > 0 && spec.Policy.SweepInterval.Duration < 2*window
}

func degradedMessage(st *check.Status) string {
	if st.Message != "" {
		return st.Message
	}
	return "running with an incomplete view; findings are reported as suspect"
}

func sourceMessage(st *check.Status) string {
	if st.SourceConnected {
		return fmt.Sprintf("subscribed; %d events applied", st.EventsApplied)
	}
	if st.SourceLastError != "" {
		return "not subscribed: " + st.SourceLastError
	}
	return "not subscribed"
}

func targetMessage(st *check.Status) string {
	if !st.TargetReachable {
		// §23 A5. The distinction is the point: a store that cannot be read has
		// told driftwatch nothing about whether it has drifted, so the last
		// known counts stand rather than being zeroed or inflated.
		return "the target could not be reached; no new findings are being reported"
	}
	if st.TargetRole != "" {
		return fmt.Sprintf("reachable, role %s, %d keys", st.TargetRole, st.TargetKeyspaceSize)
	}
	return "reachable"
}

func driftMessage(st *check.Status) string {
	if st.DivergentKeys == 0 && st.SuspectDivergentKeys == 0 {
		return "no divergence"
	}

	msg := fmt.Sprintf("%d confirmed divergent keys", st.DivergentKeys)
	if st.SuspectDivergentKeys > 0 {
		msg += fmt.Sprintf(", %d suspect (driftwatch's own view is incomplete; "+
			"do not alert on these)", st.SuspectDivergentKeys)
	}
	return msg
}

func saturationMessage(st *check.Status, spec *v1alpha1.DriftCheckSpec) string {
	if !st.OracleSaturated {
		return fmt.Sprintf("%d keys tracked", st.TrackedKeys)
	}

	limit := 0
	if spec != nil {
		limit = spec.Policy.MaxTrackedKeys
	}
	return fmt.Sprintf(
		"the keyspace did not fit in policy.maxTrackedKeys (%d): %d evictions, "+
			"so every finding covers only part of the store",
		limit, st.OracleEvictions)
}

func sequenceMessage(st *check.Status, missing uint64) string {
	if missing == 0 {
		return fmt.Sprintf("no gaps across %d publishers", len(st.Publishers))
	}

	worst, count := "", uint64(0)
	for i := range st.Publishers {
		if p := &st.Publishers[i]; p.MissingEvents > count {
			worst, count = p.ID, p.MissingEvents
		}
	}
	return fmt.Sprintf("publisher %s: %d missing events", worst, count)
}

func snapshotMessage(st *check.Status) string {
	if st.AwaitingSnapshot {
		return "waiting for a publisher to retransmit its state"
	}
	return fmt.Sprintf("%d snapshot cycles seen", st.SnapshotsSeen)
}

func multiWriterMessage(st *check.Status) string {
	if !st.MultiWriterUnsafe {
		return "each key is written by one publisher"
	}
	return fmt.Sprintf(
		"more than one publisher writes %q under an order-dependent projection, "+
			"so findings on that keyspace reflect one arbitrary interleaving",
		st.MultiWriterKey)
}

func commutativeMessage(commutative bool) string {
	if commutative {
		return "the projection's fold is order-independent"
	}
	return "a counter that accepts absolute sets is not commutative, so a " +
		"reordered stream converges to a different total"
}

func sweepIntervalMessage(spec *v1alpha1.DriftCheckSpec, st *check.Status, tight bool) string {
	if !tight {
		return fmt.Sprintf("sweeping every %s against a %.1fs window",
			spec.Policy.SweepInterval.Duration, st.SettlementWindowSeconds)
	}
	return fmt.Sprintf(
		"sweepInterval (%s) is less than twice the settlement window (%.1fs): "+
			"candidates will still be awaiting confirmation when the next sweep "+
			"raises them again",
		spec.Policy.SweepInterval.Duration, st.SettlementWindowSeconds)
}

func missingEvents(st *check.Status) uint64 {
	var total uint64
	for i := range st.Publishers {
		total += st.Publishers[i].MissingEvents
	}
	return total
}

// setCondition applies a condition, preserving its transition time when nothing
// material changed.
//
// meta.SetStatusCondition already does that for status; the wrapper adds the
// observed generation and the injected clock. The clock matters for the tests:
// a condition stamped with time.Now cannot be asserted on without either
// tolerance or sleeping.
//
// reach the caller's condition, which is built inline at every call site
//
//nolint:gocritic // hugeParam: by value so the local mutations below cannot
func setCondition(
	status *v1alpha1.DriftCheckStatus, c metav1.Condition, generation int64, now time.Time,
) {
	c.ObservedGeneration = generation
	if c.LastTransitionTime.IsZero() {
		c.LastTransitionTime = metav1.NewTime(now)
	}
	if c.Message == "" {
		c.Message = c.Reason
	}
	meta.SetStatusCondition(&status.Conditions, c)
}

func condition(kind string, ok bool, reason, message string) metav1.Condition {
	statusValue := metav1.ConditionFalse
	if ok {
		statusValue = metav1.ConditionTrue
	}
	return metav1.Condition{
		Type:    kind,
		Status:  statusValue,
		Reason:  reason,
		Message: message,
	}
}

func reasonFor(ok bool, whenTrue, whenFalse string) string {
	if ok {
		return whenTrue
	}
	return whenFalse
}

// seconds renders a duration the way the CRD carries it.
//
// A string rather than a float, following Kubernetes API convention, and
// trimmed to milliseconds: an adaptive window that jittered in the seventh
// decimal place would rewrite the status on every reconcile and make every
// `kubectl diff` show a change nobody made.
func seconds(v float64) string {
	if v == 0 {
		return "0"
	}
	return strconv.FormatFloat(round(v, 3), 'f', -1, 64)
}

// ratio renders a fraction to four places, which is one more than the default
// divergence threshold of 0.001 needs to be readable.
func ratio(v float64) string {
	if v == 0 {
		return "0"
	}
	return strconv.FormatFloat(round(v, 4), 'f', -1, 64)
}

func round(v float64, places int) float64 {
	scale := 1.0
	for i := 0; i < places; i++ {
		scale *= 10
	}
	rounded := float64(int64(v*scale + copysign(0.5, v)))
	return rounded / scale
}

func copysign(magnitude, sign float64) float64 {
	if sign < 0 {
		return -magnitude
	}
	return magnitude
}

func nonEmpty(m map[string]int) map[string]int {
	if len(m) == 0 {
		return nil
	}
	return m
}
