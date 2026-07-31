package explain

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/seqtrack"
)

// rule is one named diagnosis from the §9 M13 table.
//
// Each returns a Diagnosis or nil, and none of them may look at another's
// output. Rules that fire together are all reported, ordered by confidence,
// because the useful case is usually several at once: a key missing from the
// target, on a publisher with a gap, during a window in which the store was
// evicting. Any one of those alone points somewhere different.
type rule struct {
	code string
	fire func(e *Explanation, in *Input, now time.Time) *Diagnosis
}

// rules is the §9 M13 table, in the order it is written there.
var rules = []rule{
	{CodeAgree, ruleAgree},
	{CodeInFlight, ruleInFlight},
	{CodeSeqGapAffectingPublisher, ruleSeqGapAffectingPublisher},
	{CodeMissingInTargetNoGaps, ruleMissingInTargetNoGaps},
	{CodeExtraInTarget, ruleExtraInTarget},
	{CodePublisherRestarted, rulePublisherRestarted},
	{CodeTargetEvictionLikely, ruleTargetEvictionLikely},
	{CodeTTLExpired, ruleTTLExpired},
	{CodeTypeMismatch, ruleTypeMismatch},
	{CodeClockSkewHigh, ruleClockSkewHigh},
	{CodeMemberSubset, ruleMemberSubset},
	{CodeMemberSuperset, ruleMemberSuperset},
	{CodeHistoryTruncated, ruleHistoryTruncated},
	{CodeNoHistory, ruleNoHistory},
}

// Rules returns every diagnosis code, in table order. Used by the tests that
// assert each one is individually covered.
func Rules() []string {
	out := make([]string, 0, len(rules))
	for _, r := range rules {
		out = append(out, r.code)
	}
	return out
}

// diagnose runs every rule and orders the results.
func (e *Explanation) diagnose(in *Input, now time.Time) {
	for _, r := range rules {
		if d := r.fire(e, in, now); d != nil {
			d.Code = r.code
			e.Diagnosis = append(e.Diagnosis, *d)
		}
	}

	// Confidence descending, then code, so the output is deterministic and the
	// line an operator should read first is the line at the top.
	sort.SliceStable(e.Diagnosis, func(i, j int) bool {
		if e.Diagnosis[i].Confidence != e.Diagnosis[j].Confidence {
			return e.Diagnosis[i].Confidence > e.Diagnosis[j].Confidence
		}
		return e.Diagnosis[i].Code < e.Diagnosis[j].Code
	})
}

// Has reports whether a diagnosis with this code fired.
func (e *Explanation) Has(code string) bool {
	_, ok := e.Find(code)
	return ok
}

// Find returns the diagnosis with this code.
func (e *Explanation) Find(code string) (Diagnosis, bool) {
	for _, d := range e.Diagnosis {
		if d.Code == code {
			return d, true
		}
	}
	return Diagnosis{}, false
}

// ---------------------------------------------------------------------------
// The rules.
// ---------------------------------------------------------------------------

// ruleAgree fires when the oracle and the target hold the same value.
func ruleAgree(e *Explanation, _ *Input, _ time.Time) *Diagnosis {
	if e.Verdict != VerdictAgree {
		return nil
	}

	return &Diagnosis{
		Confidence: ConfidenceHigh,
		Statement: fmt.Sprintf("Oracle and target agree at version %d.",
			e.OracleVersion),
		Evidence: []string{
			"both hold " + e.OracleValue.String(),
			fmt.Sprintf("last event %s ago, settlement window %s",
				compact(e.Age()), compact(e.SettlementWindow)),
		},
	}
}

// ruleInFlight fires when the key changed inside the settlement window.
//
// This is the rule that stops the most false alarms. A materializer that is
// merely behind produces exactly the same observation as one that has lost the
// event, and the only thing separating them is how long ago the event arrived.
func ruleInFlight(e *Explanation, _ *Input, _ time.Time) *Diagnosis {
	if e.Settled || e.LastEventAt.IsZero() {
		return nil
	}

	return &Diagnosis{
		Confidence: ConfidenceHigh,
		Statement: fmt.Sprintf(
			"Last event was %s ago, inside the %s settlement window; disagreement here is expected.",
			compact(e.Age()), compact(e.SettlementWindow)),
		Evidence: []string{
			fmt.Sprintf("the key settles in %s",
				compact(e.SettlementWindow-e.Age())),
			"the materializer has not been given time to apply it yet",
		},
	}
}

// ruleSeqGapAffectingPublisher fires when the key's last publisher has gaps.
//
// It is the most important rule in the table, because it is the one that turns
// driftwatch's answer from an accusation into an admission. When the publisher
// that owns this key has missing sequence numbers, driftwatch's own view is
// incomplete, and a disagreement is at least as likely to be its fault as the
// store's.
func ruleSeqGapAffectingPublisher(e *Explanation, _ *Input, _ time.Time) *Diagnosis {
	publisher := e.LastPublisher()
	if publisher == "" {
		return nil
	}

	ps, ok := e.publisherState(publisher)
	if !ok || ps.Gaps == nil || ps.Gaps.Count() == 0 {
		return nil
	}

	return &Diagnosis{
		Confidence: ConfidenceHigh,
		Statement: fmt.Sprintf(
			"Publisher %s has %d missing sequence numbers (%s); driftwatch's own view "+
				"may be incomplete, so this disagreement may be driftwatch's fault, "+
				"not the target's.",
			publisher, ps.Gaps.Count(), renderIntervals(ps.Gaps)),
		Evidence: []string{
			fmt.Sprintf("publisher %s: high-water mark %d, %d events observed",
				publisher, ps.HWM, ps.EventCount),
			gapTruncationNote(ps),
			"keys touched by the missing events are marked suspect and never alerted on",
		},
	}
}

// ruleMissingInTargetNoGaps fires when the oracle has a value, the target does
// not, and driftwatch saw the publisher's whole sequence.
//
// This is the finding worth waking someone for, and the reason the sequence
// tracking exists: with no gaps, driftwatch can say the event stream it folded
// was complete, which makes the target the party that lost something.
func ruleMissingInTargetNoGaps(e *Explanation, _ *Input, _ time.Time) *Diagnosis {
	if !e.KnownToOracle || e.OracleValue.IsAbsent() || !e.TargetValue.IsAbsent() {
		return nil
	}
	if !e.TargetReachable || !e.Settled || e.OracleTrust != oracle.TrustComplete {
		return nil
	}

	publisher := e.LastPublisher()
	first, last, count := e.seqRange(publisher)

	d := &Diagnosis{
		Confidence: ConfidenceHigh,
		Statement: fmt.Sprintf(
			"Target is missing this key. driftwatch observed a complete event sequence "+
				"for publisher %s (seq %d..%d, no gaps), so the materializer most likely "+
				"dropped or failed to apply seq %d.",
			publisher, first, last, last),
		Evidence: []string{
			fmt.Sprintf("oracle expects %s at version %d", e.OracleValue, e.OracleVersion),
			"target holds nothing, " + readNote(e.readAge()),
			fmt.Sprintf("%d events for this key in the retained history", count),
		},
	}

	// Ruling eviction out is worth a line of its own. It is the first thing an
	// operator will suspect, and answering it before it is asked saves a round
	// trip through the Redis dashboard.
	if e.TargetHealth.Reachable {
		d.Evidence = append(d.Evidence, fmt.Sprintf(
			"the store reports %d evictions in total, so eviction is unlikely",
			e.TargetHealth.EvictedKeys))
	}
	return d
}

// ruleExtraInTarget fires when the target holds a key no event created.
//
// Conservative by design (§5.5). An extra key is the finding most likely to be
// driftwatch's own fault: it sees them constantly at startup, when the store
// legitimately predates the subscription.
func ruleExtraInTarget(e *Explanation, _ *Input, _ time.Time) *Diagnosis {
	if !e.TargetReachable || e.TargetValue.IsAbsent() {
		return nil
	}
	if e.KnownToOracle && !e.OracleValue.IsAbsent() {
		return nil
	}

	mode := "Wait"
	if e.OracleTrust == oracle.TrustAdopted {
		mode = "Adopt"
	}

	return &Diagnosis{
		Confidence: ConfidenceMedium,
		Statement: fmt.Sprintf(
			"Target holds this key but no event ever created it. Either it predates "+
				"driftwatch (bootstrap mode %s) or it was written out-of-band.", mode),
		Evidence: []string{
			"target holds " + e.TargetValue.String(),
			e.oracleSideOfExtra(),
		},
	}
}

// oracleSideOfExtra distinguishes a key the oracle deleted from one it has
// never heard of. The two look identical in the target and mean opposite
// things: the first is the materializer failing to apply a delete, the second
// is a key from before driftwatch attached.
func (e *Explanation) oracleSideOfExtra() string {
	if e.KnownToOracle {
		return fmt.Sprintf(
			"the oracle holds a tombstone at version %d: an event deleted this key "+
				"and the target still has it", e.OracleVersion)
	}
	return "the oracle has no record of this key at all, not even a deletion"
}

// rulePublisherRestarted fires when a restart appears in the key's history.
func rulePublisherRestarted(e *Explanation, _ *Input, _ time.Time) *Diagnosis {
	for i := range e.History {
		step := &e.History[i]
		if step.Verdict != seqtrack.AcceptAfterRestart {
			continue
		}

		kind := "implicit"
		if ps, ok := e.publisherState(step.Event.Publisher); ok && ps.Epoch > 0 {
			kind = "explicit"
		}

		return &Diagnosis{
			Confidence: ConfidenceMedium,
			Statement: fmt.Sprintf(
				"Publisher %s restarted at %s (%s); events around that boundary may "+
					"have been lost.",
				step.Event.Publisher, step.AppliedAt.UTC().Format(time.RFC3339), kind),
			Evidence: []string{
				fmt.Sprintf("history step #%d resumed at seq %d", i, step.Event.Seq),
				"a restart resets the sequence space, so anything in flight at the " +
					"moment of the restart is unaccounted for",
			},
		}
	}
	return nil
}

// ruleTargetEvictionLikely fires when a key is missing while the store is
// evicting.
//
// §5.7's point: a sweep that finds mass absence at the same moment the eviction
// counter jumped has an obvious explanation, and saying so saves the operator
// an hour of looking in the wrong place.
func ruleTargetEvictionLikely(e *Explanation, in *Input, _ time.Time) *Diagnosis {
	if !e.TargetReachable || !e.TargetValue.IsAbsent() || !e.KnownToOracle {
		return nil
	}
	if e.OracleValue.IsAbsent() || in.EvictionsSinceLastSweep == 0 {
		return nil
	}

	pct := memoryPressure(e.TargetHealth.UsedMemoryBytes, e.TargetHealth.MaxMemoryBytes)

	confidence := ConfidenceMedium
	if pct >= 90 {
		confidence = ConfidenceHigh
	}

	return &Diagnosis{
		Confidence: confidence,
		Statement: fmt.Sprintf(
			"Redis reported %d evictions since the last sweep and is at %d%% of "+
				"maxmemory; this key was probably evicted.",
			in.EvictionsSinceLastSweep, pct),
		Evidence: []string{
			fmt.Sprintf("used %s of %s",
				bytesHuman(e.TargetHealth.UsedMemoryBytes),
				bytesHuman(e.TargetHealth.MaxMemoryBytes)),
			"an evicted key is the store working as configured, not drift",
		},
	}
}

// ruleTTLExpired fires when the oracle's own TTL for the key has elapsed.
func ruleTTLExpired(e *Explanation, _ *Input, now time.Time) *Diagnosis {
	if e.OracleTTL == nil || !e.TargetValue.IsAbsent() || e.LastEventAt.IsZero() {
		return nil
	}

	expiresAt := e.LastEventAt.Add(*e.OracleTTL)
	if now.Before(expiresAt) {
		return nil
	}

	return &Diagnosis{
		Confidence: ConfidenceHigh,
		Statement: fmt.Sprintf(
			"The oracle's TTL for this key expired %s ago; absence in the target is expected.",
			compact(now.Sub(expiresAt))),
		Evidence: []string{
			fmt.Sprintf("the last event set a TTL of %s at %s",
				compact(*e.OracleTTL), e.LastEventAt.UTC().Format(time.RFC3339)),
			"this is not drift under any expiry policy",
		},
	}
}

// ruleTypeMismatch fires when the store holds a shape the projection cannot
// read.
func ruleTypeMismatch(e *Explanation, in *Input, _ time.Time) *Diagnosis {
	if e.TargetType == "" {
		return nil
	}

	return &Diagnosis{
		Confidence: ConfidenceHigh,
		Statement: fmt.Sprintf(
			"Target holds type %s but the %s projection expects %s; the projection "+
				"may be misconfigured for this keyspace.",
			e.TargetType, in.Shape, expectedRedisType(in.Shape)),
		Evidence: []string{
			"the store answered, so this is what it holds rather than a failed read",
			"a whole keyspace of these usually means the wrong keyTemplate",
		},
	}
}

// ruleClockSkewHigh fires when a publisher's clock is further out than W.
//
// Worth stating plainly that it does not affect correctness. Settlement runs on
// driftwatch's local receive time precisely so that a skewed publisher cannot
// make the decision unsound (F5) — but every timestamp in this output comes
// from the publisher, so the reader needs to know they cannot be trusted.
func ruleClockSkewHigh(e *Explanation, _ *Input, now time.Time) *Diagnosis {
	publisher := e.LastPublisher()
	if publisher == "" || e.SettlementWindow == 0 {
		return nil
	}

	skew, ok := e.observedSkew(publisher)
	if !ok || abs(skew) <= e.SettlementWindow {
		return nil
	}

	return &Diagnosis{
		Confidence: ConfidenceMedium,
		Statement: fmt.Sprintf(
			"Publisher %s's clock differs from driftwatch's by %s, which exceeds the "+
				"settlement window; publisher timestamps in this output are unreliable "+
				"(settlement itself is unaffected — it uses local receive time).",
			publisher, compact(skew)),
		Evidence: []string{
			fmt.Sprintf("measured against driftwatch's clock at %s",
				now.UTC().Format(time.RFC3339)),
			"gaps, ordering and settlement all use local receive time, so this is a " +
				"display problem rather than a correctness one",
		},
	}
}

// observedSkew returns the publisher clock offset seen in the key's history.
func (e *Explanation) observedSkew(publisher string) (time.Duration, bool) {
	for i := len(e.History) - 1; i >= 0; i-- {
		ev := &e.History[i].Event
		if ev.Publisher != publisher || ev.PublishedAt.IsZero() {
			continue
		}
		return ev.PublishedAt.Sub(ev.ObservedAt), true
	}
	return 0, false
}

// ruleMemberSubset fires when the target's members are a strict subset of the
// oracle's.
func ruleMemberSubset(e *Explanation, _ *Input, _ time.Time) *Diagnosis {
	if !e.isSetComparison() || e.MissingMemberCount == 0 || e.ExtraMemberCount != 0 {
		return nil
	}

	return &Diagnosis{
		Confidence: ConfidenceHigh,
		Statement: fmt.Sprintf(
			"Target is missing %d of %d members (%s); consistent with dropped add "+
				"events rather than a wholesale failure.",
			e.MissingMemberCount, len(e.OracleValue.Members), sample(e.MissingMembers)),
		Evidence: []string{
			fmt.Sprintf("target holds %s, oracle expects %d",
				plural(len(e.TargetValue.Members), "member"), len(e.OracleValue.Members)),
			"every member the target does hold is one the oracle expects, so the " +
				"materializer is applying events rather than failing entirely",
		},
	}
}

// ruleMemberSuperset fires when the target holds members the oracle does not.
func ruleMemberSuperset(e *Explanation, _ *Input, _ time.Time) *Diagnosis {
	if !e.isSetComparison() || e.ExtraMemberCount == 0 || e.MissingMemberCount != 0 {
		return nil
	}

	return &Diagnosis{
		Confidence: ConfidenceHigh,
		Statement: fmt.Sprintf(
			"Target holds %s (%s); consistent with dropped remove events.",
			plural(e.ExtraMemberCount, "extra member"), sample(e.ExtraMembers)),
		Evidence: []string{
			fmt.Sprintf("target holds %s, oracle expects %d",
				plural(len(e.TargetValue.Members), "member"), len(e.OracleValue.Members)),
			"a removal that never reached the store leaves exactly this shape",
		},
	}
}

// isSetComparison reports whether both sides are sets, which is what makes a
// member difference meaningful.
func (e *Explanation) isSetComparison() bool {
	if !e.TargetReachable {
		return false
	}
	oracleSet := e.OracleValue.Kind == event.ValueSet
	targetSet := e.TargetValue.Kind == event.ValueSet
	return oracleSet && targetSet
}

// ruleHistoryTruncated fires when the per-key ring is full.
func ruleHistoryTruncated(e *Explanation, in *Input, _ time.Time) *Diagnosis {
	if !e.HistoryTruncated || len(e.History) == 0 {
		return nil
	}

	return &Diagnosis{
		Confidence: ConfidenceHigh,
		Statement: fmt.Sprintf(
			"Only the last %d events are retained; earlier history is unavailable.",
			len(e.History)),
		Evidence: []string{
			fmt.Sprintf("the per-key ring holds %d entries (policy.ringSize)", in.RingSize),
			fmt.Sprintf("the oracle is at version %d, so at least %d events are not shown",
				e.OracleVersion, maxInt(0, int(e.OracleVersion)-len(e.History))),
		},
	}
}

// ruleNoHistory fires when driftwatch has never seen an event for the key.
func ruleNoHistory(e *Explanation, _ *Input, _ time.Time) *Diagnosis {
	if e.KnownToOracle || len(e.History) > 0 {
		return nil
	}

	d := &Diagnosis{
		Confidence: ConfidenceHigh,
		Statement:  "driftwatch has never observed an event for this key.",
		Evidence: []string{
			"the oracle has no entry, not even a tombstone from a deletion",
		},
	}

	switch {
	case !e.TargetReachable:
		d.Evidence = append(d.Evidence,
			"the store could not be read, so it is unknown whether the key exists there")
	case e.TargetValue.IsAbsent():
		d.Evidence = append(d.Evidence,
			"the store does not hold it either: most likely the key name is wrong, "+
				"or the keyTemplate does not produce this shape")
	default:
		d.Evidence = append(d.Evidence,
			"the store does hold it, so it predates driftwatch's subscription or was "+
				"written out-of-band")
	}
	return d
}

// ---------------------------------------------------------------------------
// Small helpers shared by the rules.
// ---------------------------------------------------------------------------

// readAge returns how long ago the target was read.
func (e *Explanation) readAge() time.Duration {
	if e.TargetReadAt.IsZero() {
		return 0
	}
	return e.GeneratedAt.Sub(e.TargetReadAt)
}

func renderIntervals(gaps *seqtrack.GapSet) string {
	intervals := gaps.Intervals()
	if len(intervals) == 0 {
		return "none"
	}

	const shown = 3
	parts := make([]string, 0, shown+1)
	for i, iv := range intervals {
		if i == shown {
			parts = append(parts, fmt.Sprintf("and %d more", len(intervals)-shown))
			break
		}
		parts = append(parts, iv.String())
	}
	return strings.Join(parts, ", ")
}

// snapshot and this runs once per explanation.
//
//nolint:gocritic // hugeParam: PublisherState comes straight from the tracker's
func gapTruncationNote(ps seqtrack.PublisherState) string {
	if ps.Gaps != nil && ps.Gaps.Truncated() {
		return "the gap list hit its bound, so the missing count is a floor rather than a total"
	}
	return "the gap list is complete, so the missing count is exact"
}

// memoryPressure returns used memory as a percentage of the configured maximum.
func memoryPressure(used, limit uint64) int {
	if limit == 0 {
		return 0
	}
	return int(used * 100 / limit)
}

func bytesHuman(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	if n == 0 {
		return "unset"
	}

	div, exp := uint64(unit), 0
	for v := n / unit; v >= unit && exp < 4; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGTP"[exp])
}

// expectedRedisType names the store type a projection shape reads.
func expectedRedisType(shape interface{ String() string }) string {
	switch shape.String() {
	case "set":
		return "set"
	case "counter":
		return "string holding an integer"
	default:
		return "string"
	}
}

// compact renders a duration the way a person would say it.
//
// time.Duration's own String is exact and unreadable at these scales:
// "1m47.283910284s" carries nine digits nobody reads, in the one place where
// the output is meant to be scanned rather than parsed.
func compact(d time.Duration) string {
	switch {
	case d < 0:
		return "-" + compact(-d)
	case d < time.Millisecond:
		return fmt.Sprintf("%dµs", d.Microseconds())
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	case d < time.Minute:
		return strings.TrimSuffix(fmt.Sprintf("%.1f", d.Seconds()), ".0") + "s"
	case d < time.Hour:
		return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
	default:
		return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
	}
}

// plural renders a count with its noun, so the output never says "1 members".
func plural(n int, noun string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, noun)
	}
	return fmt.Sprintf("%d %ss", n, noun)
}

func abs(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
