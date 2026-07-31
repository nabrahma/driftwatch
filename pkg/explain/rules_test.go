package explain_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/explain"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// requireDiagnosis asserts a code fired with a given confidence, and returns it.
func requireDiagnosis(t *testing.T, e *explain.Explanation, code string, want explain.Confidence) explain.Diagnosis {
	t.Helper()

	d, ok := e.Find(code)
	require.True(t, ok, "%s did not fire; got %v", code, codesOf(e))
	assert.Equal(t, want, d.Confidence, "%s fired at the wrong confidence", code)
	assert.NotEmpty(t, d.Statement, "%s fired with no statement", code)
	assert.NotEmpty(t, d.Evidence, "%s fired with no supporting evidence", code)
	return d
}

func codesOf(e *explain.Explanation) []string {
	out := make([]string, 0, len(e.Diagnosis))
	for _, d := range e.Diagnosis {
		out = append(out, d.Code)
	}
	return out
}

// ---------------------------------------------------------------------------
// One test per rule in the §9 M13 table.
// ---------------------------------------------------------------------------

func TestRule_AGREE(t *testing.T) {
	f := newFixture(t)
	f.add("block:9f3a", "replica-0", 1)
	f.materialize("block:9f3a")
	f.advance(time.Minute)

	e := f.explain("block:9f3a")

	assert.Equal(t, explain.VerdictAgree, e.Verdict)
	d := requireDiagnosis(t, e, explain.CodeAgree, explain.ConfidenceHigh)
	assert.Contains(t, d.Statement, "version 1")
}

func TestRule_IN_FLIGHT(t *testing.T) {
	// The rule that stops the most false alarms: a materializer that is merely
	// behind looks exactly like one that lost the event, and the only thing
	// separating them is elapsed time.
	f := newFixture(t, withWindow(5*time.Second))
	f.add("block:9f3a", "replica-0", 1)
	f.advance(2 * time.Second) // inside the window; the target has not caught up

	e := f.explain("block:9f3a")

	assert.Equal(t, explain.VerdictInFlight, e.Verdict)
	assert.False(t, e.Settled)
	d := requireDiagnosis(t, e, explain.CodeInFlight, explain.ConfidenceHigh)
	assert.Contains(t, d.Statement, "settlement window")
	assert.Contains(t, d.Statement, "expected")
}

func TestRule_SEQ_GAP_AFFECTING_PUBLISHER(t *testing.T) {
	// The most important rule in the table: it is the one that turns
	// driftwatch's answer from an accusation into an admission.
	f := newFixture(t)
	f.add("block:9f3a", "replica-0", 1)
	f.advance(time.Second)
	f.add("block:9f3a", "replica-1", 9) // seq 2..8 never arrived
	f.advance(time.Minute)

	e := f.explain("block:9f3a")

	d := requireDiagnosis(t, e, explain.CodeSeqGapAffectingPublisher, explain.ConfidenceHigh)
	assert.Contains(t, d.Statement, "7 missing sequence numbers")
	assert.Contains(t, d.Statement, "[2,8]")
	assert.Contains(t, d.Statement, "may be driftwatch's fault, not the target's")
	assert.Equal(t, oracle.TrustSuspect, e.OracleTrust)
}

func TestRule_MISSING_IN_TARGET_NO_GAPS(t *testing.T) {
	// The finding worth waking someone for, and the whole reason the sequence
	// tracking exists: with no gaps, driftwatch can say the stream it folded
	// was complete, which makes the target the party that lost something.
	f := newFixture(t)
	f.add("block:9f3a", "replica-0", 1)
	f.advance(time.Second)
	f.add("block:9f3a", "replica-0", 2)
	f.advance(time.Minute) // the materializer never wrote it

	e := f.explain("block:9f3a")

	assert.Equal(t, explain.VerdictDiverged, e.Verdict)
	d := requireDiagnosis(t, e, explain.CodeMissingInTargetNoGaps, explain.ConfidenceHigh)
	assert.Contains(t, d.Statement, "seq 1..2, no gaps")
	assert.Contains(t, d.Statement, "failed to apply seq 2")
	assert.Contains(t, d.Evidence[len(d.Evidence)-1], "eviction is unlikely",
		"the first thing an operator suspects is answered before it is asked")
}

func TestRule_MISSING_IN_TARGET_NO_GAPS_StaysQuietWhenThereAreGaps(t *testing.T) {
	// The rule claims a complete sequence. With a gap the claim is false, and
	// making it anyway is exactly the dishonesty §5.2 exists to prevent.
	f := newFixture(t)
	f.add("block:9f3a", "replica-0", 1)
	f.advance(time.Second)
	f.add("block:9f3a", "replica-0", 50)
	f.advance(time.Minute)

	e := f.explain("block:9f3a")

	assert.False(t, e.Has(explain.CodeMissingInTargetNoGaps))
	assert.True(t, e.Has(explain.CodeSeqGapAffectingPublisher))
	assert.Equal(t, explain.VerdictSuspect, e.Verdict)
}

func TestRule_EXTRA_IN_TARGET(t *testing.T) {
	f := newFixture(t)
	f.mem.SeedSets(map[string][]string{"block:orphan": {"replica-3"}})
	f.advance(time.Minute)

	e := f.explain("block:orphan")

	d := requireDiagnosis(t, e, explain.CodeExtraInTarget, explain.ConfidenceMedium)
	assert.Contains(t, d.Statement, "no event ever created it")
	assert.Contains(t, d.Evidence[1], "no record of this key at all")
}

func TestRule_EXTRA_IN_TARGET_DistinguishesATombstoneFromAnUnknownKey(t *testing.T) {
	// The two look identical in the target and mean opposite things: a
	// tombstone is the materializer failing to apply a delete, an unknown key
	// predates the subscription.
	f := newFixture(t, withProjection("scalar"))
	f.set("block:9f3a", "v1", 1)
	f.materialize("block:9f3a")
	f.advance(time.Second)
	f.apply(event.Event{Op: event.OpDelete, Key: "block:9f3a", Seq: 2, Epoch: 1})
	f.advance(time.Minute)

	e := f.explain("block:9f3a")

	d := requireDiagnosis(t, e, explain.CodeExtraInTarget, explain.ConfidenceMedium)
	assert.Contains(t, d.Evidence[1], "tombstone")
	assert.Contains(t, d.Evidence[1], "the target still has it")
}

func TestRule_PUBLISHER_RESTARTED(t *testing.T) {
	f := newFixture(t)
	f.add("block:9f3a", "replica-0", 1)
	f.advance(time.Second)
	f.apply(event.Event{
		Op: event.OpAdd, Key: "block:9f3a", Member: "replica-0",
		Publisher: "replica-0", Seq: 1, Epoch: 2, // a declared new incarnation
	})
	f.advance(time.Minute)

	e := f.explain("block:9f3a")

	d := requireDiagnosis(t, e, explain.CodePublisherRestarted, explain.ConfidenceMedium)
	assert.Contains(t, d.Statement, "restarted at")
	assert.Contains(t, d.Statement, "may have been lost")
}

func TestRule_TARGET_EVICTION_LIKELY(t *testing.T) {
	// §5.7: a sweep that finds absence at the moment the eviction counter
	// jumped has an obvious explanation, and saying so saves an hour of looking
	// in the wrong place.
	f := newFixture(t)
	f.add("block:9f3a", "replica-0", 1)
	f.advance(time.Minute)

	f.evictions = 4218
	f.mem.SetHealth(target.Health{
		Reachable:       true,
		EvictedKeys:     4218,
		UsedMemoryBytes: 3_900_000_000,
		MaxMemoryBytes:  4_000_000_000,
	})

	e := f.explain("block:9f3a")

	d := requireDiagnosis(t, e, explain.CodeTargetEvictionLikely, explain.ConfidenceHigh)
	assert.Contains(t, d.Statement, "4218 evictions")
	assert.Contains(t, d.Statement, "97% of maxmemory")
	assert.Contains(t, d.Evidence[1], "not drift")
}

func TestRule_TARGET_EVICTION_LIKELY_IsOnlyMediumWithoutMemoryPressure(t *testing.T) {
	// Evictions without memory pressure are a weaker explanation: something
	// evicted, but not obviously this key.
	f := newFixture(t)
	f.add("block:9f3a", "replica-0", 1)
	f.advance(time.Minute)

	f.evictions = 3
	f.mem.SetHealth(target.Health{Reachable: true, EvictedKeys: 3})

	e := f.explain("block:9f3a")

	requireDiagnosis(t, e, explain.CodeTargetEvictionLikely, explain.ConfidenceMedium)
}

func TestRule_TTL_EXPIRED(t *testing.T) {
	ttl := 30 * time.Second

	f := newFixture(t, withProjection("scalar"))
	f.apply(event.Event{Op: event.OpSet, Key: "session:9f3a", Value: []byte("v1"), Seq: 1, Epoch: 1, TTL: &ttl})
	f.advance(2 * time.Minute) // well past the lifetime

	e := f.explain("session:9f3a")

	d := requireDiagnosis(t, e, explain.CodeTTLExpired, explain.ConfidenceHigh)
	assert.Contains(t, d.Statement, "expired 1m30s ago")
	assert.Contains(t, d.Statement, "absence in the target is expected")
}

func TestRule_TTL_EXPIRED_StaysQuietBeforeTheLifetimeElapses(t *testing.T) {
	ttl := 30 * time.Second

	f := newFixture(t, withProjection("scalar"))
	f.apply(event.Event{Op: event.OpSet, Key: "session:9f3a", Value: []byte("v1"), Seq: 1, Epoch: 1, TTL: &ttl})
	f.advance(10 * time.Second)

	assert.False(t, f.explain("session:9f3a").Has(explain.CodeTTLExpired))
}

func TestRule_TYPE_MISMATCH(t *testing.T) {
	f := newFixture(t) // the set projection
	f.add("block:9f3a", "replica-0", 1)
	f.mem.Seed(map[string][]byte{"block:9f3a": []byte("a string, not a set")})
	f.advance(time.Minute)

	e := f.explain("block:9f3a")

	assert.Equal(t, "string", e.TargetType)
	assert.Equal(t, explain.VerdictDiverged, e.Verdict)
	d := requireDiagnosis(t, e, explain.CodeTypeMismatch, explain.ConfidenceHigh)
	assert.Contains(t, d.Statement, "Target holds type string")
	assert.Contains(t, d.Statement, "may be misconfigured")
}

func TestRule_CLOCK_SKEW_HIGH(t *testing.T) {
	// Settlement runs on local receive time precisely so a skewed publisher
	// cannot make the decision unsound (F5), but every timestamp in this output
	// comes from the publisher, so the reader has to be told.
	f := newFixture(t, withWindow(5*time.Second))
	f.apply(event.Event{
		Op: event.OpAdd, Key: "block:9f3a", Member: "replica-0", Seq: 1, Epoch: 1,
		ObservedAt:  epoch(),
		PublishedAt: epoch().Add(4 * time.Minute), // the publisher's clock is ahead
	})
	f.advance(time.Minute)

	e := f.explain("block:9f3a")

	d := requireDiagnosis(t, e, explain.CodeClockSkewHigh, explain.ConfidenceMedium)
	assert.Contains(t, d.Statement, "differs from driftwatch's by 4m00s")
	assert.Contains(t, d.Statement, "settlement itself is unaffected")
}

func TestRule_CLOCK_SKEW_HIGH_StaysQuietInsideTheWindow(t *testing.T) {
	f := newFixture(t, withWindow(5*time.Second))
	f.apply(event.Event{
		Op: event.OpAdd, Key: "block:9f3a", Member: "replica-0", Seq: 1, Epoch: 1,
		ObservedAt:  epoch(),
		PublishedAt: epoch().Add(time.Second),
	})
	f.advance(time.Minute)

	assert.False(t, f.explain("block:9f3a").Has(explain.CodeClockSkewHigh))
}

func TestRule_MEMBER_SUBSET(t *testing.T) {
	f := newFixture(t)
	f.add("block:9f3a", "replica-0", 1)
	f.materialize("block:9f3a") // the target keeps up with replica-0
	f.advance(time.Second)
	f.add("block:9f3a", "replica-2", 2) // and never learns about replica-2
	f.advance(time.Minute)

	e := f.explain("block:9f3a")

	d := requireDiagnosis(t, e, explain.CodeMemberSubset, explain.ConfidenceHigh)
	assert.Contains(t, d.Statement, "missing 1 of 2 members (replica-2)")
	assert.Contains(t, d.Statement, "dropped add events")
	assert.False(t, e.Has(explain.CodeMemberSuperset))
}

func TestRule_MEMBER_SUPERSET(t *testing.T) {
	f := newFixture(t)
	f.add("block:9f3a", "replica-0", 1)
	f.advance(time.Second)
	f.add("block:9f3a", "replica-2", 2)
	f.materialize("block:9f3a")
	f.advance(time.Second)
	f.apply(event.Event{Op: event.OpRemove, Key: "block:9f3a", Member: "replica-2", Seq: 3, Epoch: 1})
	f.advance(time.Minute) // the removal never reached the store

	e := f.explain("block:9f3a")

	d := requireDiagnosis(t, e, explain.CodeMemberSuperset, explain.ConfidenceHigh)
	assert.Contains(t, d.Statement, "1 extra member (replica-2)")
	assert.Contains(t, d.Statement, "dropped remove events")
	assert.False(t, e.Has(explain.CodeMemberSubset))
}

func TestRule_HISTORY_TRUNCATED(t *testing.T) {
	f := newFixture(t, withRingSize(4))
	for seq := uint64(1); seq <= 10; seq++ {
		f.add("block:9f3a", "replica-0", seq)
		f.advance(time.Second)
	}
	f.materialize("block:9f3a")
	f.advance(time.Minute)

	e := f.explain("block:9f3a")

	assert.True(t, e.HistoryTruncated)
	assert.Len(t, e.History, 4)
	d := requireDiagnosis(t, e, explain.CodeHistoryTruncated, explain.ConfidenceHigh)
	assert.Contains(t, d.Statement, "Only the last 4 events are retained")
	assert.Contains(t, d.Evidence[1], "6 events are not shown")
}

func TestRule_NO_HISTORY(t *testing.T) {
	f := newFixture(t)
	f.advance(time.Minute)

	e := f.explain("block:never-seen")

	assert.Equal(t, explain.VerdictUnknownKey, e.Verdict)
	d := requireDiagnosis(t, e, explain.CodeNoHistory, explain.ConfidenceHigh)
	assert.Contains(t, d.Statement, "never observed an event for this key")
	assert.Contains(t, d.Evidence[1], "the keyTemplate does not produce this shape")
}

// TestRules_EveryCodeHasATest is what keeps the table above honest.
//
// A rule added to §9 M13's table and implemented but never tested is worse than
// one that was not implemented: it looks covered. This asserts every code the
// package declares is named by a test function in this file.
func TestRules_EveryCodeHasATest(t *testing.T) {
	tested := map[string]bool{
		explain.CodeAgree:                    true,
		explain.CodeInFlight:                 true,
		explain.CodeSeqGapAffectingPublisher: true,
		explain.CodeMissingInTargetNoGaps:    true,
		explain.CodeExtraInTarget:            true,
		explain.CodePublisherRestarted:       true,
		explain.CodeTargetEvictionLikely:     true,
		explain.CodeTTLExpired:               true,
		explain.CodeTypeMismatch:             true,
		explain.CodeClockSkewHigh:            true,
		explain.CodeMemberSubset:             true,
		explain.CodeMemberSuperset:           true,
		explain.CodeHistoryTruncated:         true,
		explain.CodeNoHistory:                true,
	}

	for _, code := range explain.Rules() {
		assert.True(t, tested[code],
			"rule %s has no TestRule_%s; add one that constructs the state it fires on",
			code, code)
	}
	assert.Len(t, explain.Rules(), 14, "§9 M13 specifies fourteen diagnosis rules")
}

// ---------------------------------------------------------------------------
// Ordering, verdicts and the edge cases from §9 M13.
// ---------------------------------------------------------------------------

func TestExplain_OrdersDiagnosesByConfidenceThenCode(t *testing.T) {
	// Several rules firing at once is the useful case, not the exceptional one:
	// a key missing from the target, on a publisher with a gap, while the store
	// is evicting. Which one an operator reads first should not be an accident.
	f := newFixture(t)
	f.add("block:9f3a", "replica-0", 1)
	f.advance(time.Second)
	f.add("block:9f3a", "replica-0", 40)
	f.advance(time.Minute)

	f.evictions = 12
	f.mem.SetHealth(target.Health{Reachable: true, EvictedKeys: 12})

	e := f.explain("block:9f3a")

	require.Greater(t, len(e.Diagnosis), 1, "this state should fire several rules")
	for i := 1; i < len(e.Diagnosis); i++ {
		prev, cur := e.Diagnosis[i-1], e.Diagnosis[i]
		if prev.Confidence == cur.Confidence {
			assert.Less(t, prev.Code, cur.Code, "same confidence must order by code")
			continue
		}
		assert.Greater(t, prev.Confidence, cur.Confidence)
	}
}

func TestExplain_AnUnreachableTargetStillAnswersFromTheOracle(t *testing.T) {
	// §23 A5 and §9 M13's edge case: a partial answer is far better than an
	// error, because the oracle half is usually enough to work out what is
	// happening.
	f := newFixture(t)
	f.add("block:9f3a", "replica-0", 1)
	f.advance(time.Minute)

	e := f.explainWithoutTarget("block:9f3a")

	assert.Equal(t, explain.VerdictTargetUnavailable, e.Verdict)
	assert.False(t, e.TargetReachable)
	assert.Len(t, e.History, 1, "the history survives an unreadable store")
	assert.NotEmpty(t, e.PublisherStates)
	assert.False(t, e.Has(explain.CodeMissingInTargetNoGaps),
		"driftwatch must not claim the target is missing a key it could not read")
}

func TestExplain_RequiresAnOracle(t *testing.T) {
	_, err := explain.Explain(context.Background(), explain.Input{Key: "block:9f3a"})

	require.ErrorIs(t, err, explain.ErrNoOracle)
}

func TestExplain_HandlesAKeyWithAHundredThousandMembers(t *testing.T) {
	// §9 M13's edge case. Rendering the whole set is never the right answer,
	// and neither is dropping the count.
	f := newFixture(t)

	members := make([]string, 0, 100_000)
	for i := 0; i < 100_000; i++ {
		members = append(members, "replica-"+itoa(i))
	}
	f.add("block:huge", "replica-0", 1)
	f.mem.SeedSets(map[string][]string{"block:huge": members})
	f.advance(time.Minute)

	e := f.explain("block:huge")

	assert.Equal(t, 99_999, e.ExtraMemberCount, "the count is exact")
	assert.LessOrEqual(t, len(e.ExtraMembers), 8, "the list is truncated")
	assert.Contains(t, e.Text(), "and 99991 more",
		"the magnitude survives even though the list does not")
}

func TestExplain_RendersABinaryKeyAsHex(t *testing.T) {
	// A Redis key is arbitrary bytes, and printing one raw resets a terminal's
	// encoding or paints control characters into the output.
	binary := "block:\x00\xff\x01"

	f := newFixture(t, withProjection("scalar"))
	f.set(binary, "v1", 1)
	f.advance(time.Minute)

	e := f.explain(binary)

	assert.Equal(t, "hex:626c6f636b3a00ff01", explain.DisplayKey(binary))
	assert.Contains(t, e.Text(), "hex:626c6f636b3a00ff01")
	assert.NotContains(t, e.Text(), "\x00")
}

func TestExplain_AnEmptyKeyIsLabelledRatherThanRenderedBlank(t *testing.T) {
	// Redis allows the empty string as a key, so "KEY  " with nothing after it
	// is a legitimate output that reads like a bug.
	assert.Contains(t, explain.DisplayKey(""), "the empty key")
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [8]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
