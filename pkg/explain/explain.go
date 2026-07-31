// Package explain answers "what happened to this key?" (§0.2, M13).
//
// This is the module that makes driftwatch a debugging tool rather than an
// alarm. A gauge that says twelve keys diverged tells an operator that
// something is wrong; it does not tell them what, and at three in the morning
// the difference between those two is the difference between a fix and an
// escalation.
//
// The design rule is that every claim carries its evidence. A diagnosis is a
// hypothesis with a confidence and the observations behind it, never a verdict
// on its own, because driftwatch is frequently in a position to say what most
// likely happened and almost never in a position to be certain. Saying
// "probably the materializer dropped seq 8842, here is the sequence range I
// observed" is useful and honest. Saying "the materializer dropped seq 8842" is
// useful and sometimes wrong, which is worse.
//
// The second rule is that a partial answer beats an error. If the store is
// unreachable, this still returns the oracle's state, the key's history and the
// publishers' sequence positions — everything driftwatch knows without the
// store — because that is usually enough to work out what is happening, and an
// error message is never enough.
package explain

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/seqtrack"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// Verdict is the one-word answer.
type Verdict uint8

// The verdicts, in the order §9 M13 lists them.
const (
	// VerdictAgree means the oracle and the target hold the same value.
	VerdictAgree Verdict = iota
	// VerdictDiverged means they disagree and driftwatch stands behind that.
	VerdictDiverged
	// VerdictInFlight means they disagree inside the settlement window, where
	// disagreement is expected rather than meaningful.
	VerdictInFlight
	// VerdictSuspect means they disagree on a key whose event stream driftwatch
	// knows it partly missed, so the disagreement may be driftwatch's own fault.
	VerdictSuspect
	// VerdictUnknownKey means neither side has ever heard of the key.
	VerdictUnknownKey
	// VerdictTargetUnavailable means the store could not be read. Nothing is
	// claimed about the key; §23 A5 is explicit that absence of data is not
	// evidence of divergence.
	VerdictTargetUnavailable
)

var verdictNames = [...]string{
	VerdictAgree:             "AGREE",
	VerdictDiverged:          "DIVERGED",
	VerdictInFlight:          "IN FLIGHT",
	VerdictSuspect:           "SUSPECT",
	VerdictUnknownKey:        "UNKNOWN KEY",
	VerdictTargetUnavailable: "TARGET UNAVAILABLE",
}

// String returns the display name of the verdict.
func (v Verdict) String() string {
	if int(v) >= len(verdictNames) {
		return fmt.Sprintf("Verdict(%d)", uint8(v))
	}
	return verdictNames[v]
}

// Confidence grades how far a diagnosis can be trusted.
type Confidence uint8

// The confidence levels, ordered so that sorting descending puts the most
// load-bearing hypothesis first.
const (
	ConfidenceLow Confidence = iota
	ConfidenceMedium
	ConfidenceHigh
)

var confidenceNames = [...]string{
	ConfidenceLow:    "low",
	ConfidenceMedium: "medium",
	ConfidenceHigh:   "high",
}

// String returns the display name of the confidence level.
func (c Confidence) String() string {
	if int(c) >= len(confidenceNames) {
		return fmt.Sprintf("Confidence(%d)", uint8(c))
	}
	return confidenceNames[c]
}

// The diagnosis codes from §9 M13. They are stable identifiers: an operator may
// grep for one, a runbook may cite one, and an alert annotation may carry one,
// so renaming any of them is a breaking change.
const (
	CodeAgree                    = "AGREE"
	CodeInFlight                 = "IN_FLIGHT"
	CodeSeqGapAffectingPublisher = "SEQ_GAP_AFFECTING_PUBLISHER"
	CodeMissingInTargetNoGaps    = "MISSING_IN_TARGET_NO_GAPS"
	CodeExtraInTarget            = "EXTRA_IN_TARGET"
	CodePublisherRestarted       = "PUBLISHER_RESTARTED"
	CodeTargetEvictionLikely     = "TARGET_EVICTION_LIKELY"
	CodeTTLExpired               = "TTL_EXPIRED"
	CodeTypeMismatch             = "TYPE_MISMATCH"
	CodeClockSkewHigh            = "CLOCK_SKEW_HIGH"
	CodeMemberSubset             = "MEMBER_SUBSET"
	CodeMemberSuperset           = "MEMBER_SUPERSET"
	CodeHistoryTruncated         = "HISTORY_TRUNCATED"
	CodeNoHistory                = "NO_HISTORY"
)

// Diagnosis is a plain-language hypothesis with the observations behind it.
type Diagnosis struct {
	// Code is a stable identifier, safe to grep for and to cite in a runbook.
	Code string
	// Confidence grades the hypothesis. Low is not filler: ruling something out
	// is often the most useful line in the output.
	Confidence Confidence
	// Statement is one human sentence.
	Statement string
	// Evidence is the concrete observations backing the statement.
	Evidence []string
}

// Step is one event in a key's observed history.
type Step struct {
	Index         int
	Event         event.Event
	Verdict       seqtrack.Verdict
	ValueAfter    event.Value
	Version       uint64
	AppliedAt     time.Time
	DeltaFromPrev time.Duration
	// Note carries anything worth flagging about this step, such as the gap
	// that preceded it.
	Note string
}

// Explanation is everything driftwatch knows about one key.
type Explanation struct {
	Key         string
	GeneratedAt time.Time

	OracleValue   event.Value
	OracleVersion uint64
	OracleTrust   oracle.TrustState
	OracleTTL     *time.Duration
	LastEventAt   time.Time
	// KnownToOracle distinguishes a key no event has ever touched from one that
	// was deleted. The oracle keeps a tombstone for the second, and the two
	// have completely different explanations.
	KnownToOracle bool

	TargetValue     event.Value
	TargetTTL       *time.Duration
	TargetReadAt    time.Time
	TargetReachable bool
	// TargetType is set when the store holds a type the projection cannot read.
	TargetType   string
	TargetHealth target.Health

	Verdict   Verdict
	Diagnosis []Diagnosis

	History []Step
	// HistoryTruncated reports that the per-key ring is full, so the steps
	// above are the most recent ones rather than all of them.
	HistoryTruncated bool
	PublisherStates  []seqtrack.PublisherState

	SettlementWindow time.Duration
	Settled          bool
	Shape            projection.Shape

	// MissingMembers and ExtraMembers are the set difference, for the set
	// shape. Both are truncated to MaxMembersShown.
	MissingMembers     []string
	ExtraMembers       []string
	MissingMemberCount int
	ExtraMemberCount   int
}

// Input is what Explain needs to answer the question.
type Input struct {
	Key      string
	Oracle   *oracle.Oracle
	Target   target.Target
	SeqTrack *seqtrack.Tracker
	Shape    projection.Shape
	Window   time.Duration
	Clock    clock.Clock

	// RingSize is the oracle's per-key history depth, needed to tell a key with
	// exactly that many events from one whose earlier history was discarded.
	// Zero means the ring size is unknown and HISTORY_TRUNCATED cannot fire.
	RingSize int
	// EvictionsSinceLastSweep is the store's eviction counter delta. It is
	// supplied rather than read because the delta belongs to whoever is running
	// the sweeps; without it TARGET_EVICTION_LIKELY has nothing to fire on.
	EvictionsSinceLastSweep uint64
	// MaxMembersShown truncates member lists. Default 8.
	MaxMembersShown int
	// MaxHistoryShown truncates the rendered history. Default 8.
	MaxHistoryShown int
}

// defaultMembersShown and defaultHistoryShown keep the text output on a
// terminal without scrolling. A key with a hundred thousand members is a real
// case (§9 M13's edge cases) and printing it is never the right answer.
const (
	defaultMembersShown = 8
	defaultHistoryShown = 8
)

func (in *Input) applyDefaults() {
	if in.Clock == nil {
		in.Clock = clock.Real()
	}
	if in.MaxMembersShown <= 0 {
		in.MaxMembersShown = defaultMembersShown
	}
	if in.MaxHistoryShown <= 0 {
		in.MaxHistoryShown = defaultHistoryShown
	}
}

// ErrNoOracle reports an Input with nothing to explain from.
var ErrNoOracle = errors.New("explain: an oracle is required")

// Explain gathers everything known about one key and diagnoses it.
//
// It never returns an error for a store it could not read. That case produces
// VerdictTargetUnavailable and the oracle-side half of the answer, because a
// partial answer is far more useful than an error — and because refusing to
// answer when the store is down is exactly the behavior §23 A5 forbids.
// the copy, matching the §9 M13 signature.
//
//nolint:gocritic // hugeParam: Input is passed by value so applyDefaults mutates
func Explain(ctx context.Context, in Input) (*Explanation, error) {
	in.applyDefaults()

	if in.Oracle == nil {
		return nil, ErrNoOracle
	}

	now := in.Clock.Now()
	e := &Explanation{
		Key:              in.Key,
		GeneratedAt:      now,
		SettlementWindow: in.Window,
		Shape:            in.Shape,
		TargetReachable:  true,
	}

	e.readOracle(&in)
	e.readTarget(ctx, &in)
	e.readPublishers(&in)
	e.diff(&in)
	e.decideVerdict(now)
	e.diagnose(&in, now)

	return e, nil
}

func (e *Explanation) readOracle(in *Input) {
	entry, ok := in.Oracle.Get(in.Key)
	e.KnownToOracle = ok
	if ok {
		e.OracleValue = entry.Value
		e.OracleVersion = entry.Version
		e.OracleTrust = entry.Trust
		e.OracleTTL = entry.TTL
		e.LastEventAt = entry.LastEventAt
	}

	history := in.Oracle.History(in.Key)
	e.History = make([]Step, 0, len(history))

	for i := range history {
		h := &history[i]
		step := Step{
			Index:      i,
			Event:      h.Event,
			Verdict:    h.Verdict,
			ValueAfter: h.ResultValue,
			Version:    h.Version,
			AppliedAt:  h.AppliedAt,
		}
		if i > 0 {
			step.DeltaFromPrev = h.AppliedAt.Sub(history[i-1].AppliedAt)
		}
		if h.Verdict == seqtrack.AcceptWithGap {
			step.Note = "a gap precedes this event: earlier sequence numbers were never seen"
		}
		if h.Verdict == seqtrack.AcceptAfterRestart {
			step.Note = "the publisher restarted before this event"
		}
		e.History = append(e.History, step)
	}

	// A ring that is exactly full is the signal that earlier history was
	// discarded. It can be wrong in one direction — a key with precisely
	// ringSize events looks truncated — and that is the safe direction: saying
	// "earlier history may be unavailable" when it happens to be complete costs
	// nothing, while the reverse invites conclusions drawn from a partial view.
	e.HistoryTruncated = in.RingSize > 0 && len(history) >= in.RingSize
}

func (e *Explanation) readTarget(ctx context.Context, in *Input) {
	if in.Target == nil {
		e.TargetReachable = false
		return
	}

	e.TargetReadAt = in.Clock.Now()

	value, err := in.Target.Get(ctx, in.Key, in.Shape)
	switch {
	case err == nil:
		e.TargetValue = value

	case errors.Is(err, target.ErrWrongType):
		// Not a failure to read: the store answered, and the answer is that it
		// holds something the projection cannot interpret. That is drift.
		var wt *target.WrongTypeError
		if errors.As(err, &wt) {
			e.TargetType = wt.Got
		}

	default:
		e.TargetReachable = false
		return
	}

	if ttl, err := in.Target.TTL(ctx, in.Key); err == nil {
		e.TargetTTL = ttl
	}
	if health, err := in.Target.Health(ctx); err == nil {
		e.TargetHealth = health
	}
}

func (e *Explanation) readPublishers(in *Input) {
	if in.SeqTrack == nil {
		return
	}

	e.PublisherStates = in.SeqTrack.Publishers()
	sort.Slice(e.PublisherStates, func(i, j int) bool {
		return e.PublisherStates[i].ID < e.PublisherStates[j].ID
	})
}

// diff computes the member difference for the set shape.
func (e *Explanation) diff(in *Input) {
	if e.OracleValue.Kind != event.ValueSet && e.TargetValue.Kind != event.ValueSet {
		return
	}

	missing := difference(e.OracleValue.Members, e.TargetValue.Members)
	extra := difference(e.TargetValue.Members, e.OracleValue.Members)

	e.MissingMemberCount, e.ExtraMemberCount = len(missing), len(extra)
	e.MissingMembers = truncate(missing, in.MaxMembersShown)
	e.ExtraMembers = truncate(extra, in.MaxMembersShown)
}

func (e *Explanation) decideVerdict(now time.Time) {
	e.Settled = e.settledAt(now)

	switch {
	case !e.KnownToOracle && e.TargetValue.IsAbsent() && e.TargetType == "":
		// Neither side has heard of it. Reporting divergence here would mean
		// inventing a key nobody asked about.
		e.Verdict = VerdictUnknownKey
	case !e.TargetReachable:
		e.Verdict = VerdictTargetUnavailable
	case e.TargetType != "":
		e.Verdict = VerdictDiverged
	case e.OracleValue.Equal(e.TargetValue):
		e.Verdict = VerdictAgree
	case !e.Settled:
		e.Verdict = VerdictInFlight
	case e.OracleTrust == oracle.TrustSuspect:
		e.Verdict = VerdictSuspect
	default:
		e.Verdict = VerdictDiverged
	}
}

// settledAt reports whether the key's last event is older than the window.
//
// A key nobody has ever sent an event for is settled by default: there is
// nothing in flight, so there is nothing to wait for.
func (e *Explanation) settledAt(now time.Time) bool {
	if e.LastEventAt.IsZero() {
		return true
	}
	return now.Sub(e.LastEventAt) > e.SettlementWindow
}

// Age returns how long ago the key's last event arrived.
func (e *Explanation) Age() time.Duration {
	if e.LastEventAt.IsZero() {
		return 0
	}
	return e.GeneratedAt.Sub(e.LastEventAt)
}

// LastPublisher returns the publisher of the key's most recent event.
func (e *Explanation) LastPublisher() string {
	if len(e.History) == 0 {
		return ""
	}
	return e.History[len(e.History)-1].Event.Publisher
}

// publisherState returns the tracked state of a publisher by name.
func (e *Explanation) publisherState(id string) (seqtrack.PublisherState, bool) {
	for _, ps := range e.PublisherStates {
		if ps.ID == id {
			return ps, true
		}
	}
	return seqtrack.PublisherState{}, false
}

// seqRange returns the first and last sequence numbers in the key's history for
// one publisher.
func (e *Explanation) seqRange(publisher string) (first, last uint64, count int) {
	for i := range e.History {
		ev := &e.History[i].Event
		if ev.Publisher != publisher {
			continue
		}
		if count == 0 {
			first = ev.Seq
		}
		last = ev.Seq
		count++
	}
	return first, last, count
}

func difference(a, b map[string]struct{}) []string {
	out := make([]string, 0, len(a))
	for m := range a {
		if _, ok := b[m]; !ok {
			out = append(out, m)
		}
	}
	sort.Strings(out)
	return out
}

func truncate(members []string, limit int) []string {
	if len(members) <= limit {
		return members
	}
	return members[:limit]
}

// sample renders up to three members for a statement.
func sample(members []string) string {
	const shown = 3
	if len(members) > shown {
		return strings.Join(members[:shown], ", ") + ", ..."
	}
	return strings.Join(members, ", ")
}
