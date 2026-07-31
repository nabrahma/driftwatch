package faults

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/explain"
	"github.com/nabrahma/driftwatch/test/harness/scenario"
)

// §15.1 rows 23 and 24 — publisher clock skew.
//
// The assertion that matters in both is the same, and it is a negative one:
// settlement behaves identically to the no-skew case. Publisher clocks are
// wrong all the time, and a detector that let a producer's clock decide when a
// key was eligible for comparison could be silenced by a single NTP failure —
// or made to report constantly by one five minutes fast. Settlement runs on
// driftwatch's own receive time, and these rows are what prove it rather than
// assert it in a comment.

func TestFault23_PublisherClockFiveMinutesAhead(t *testing.T) {
	runSkewCase(t, 5*time.Minute)
}

func TestFault24_PublisherClockFiveMinutesBehind(t *testing.T) {
	runSkewCase(t, -5*time.Minute)
}

// runSkewCase drives one skew direction and compares it against an identical
// run with no skew at all.
//
// The comparison is what makes the row meaningful. Asserting that settlement
// "works" under skew proves nothing unless it produces the same answer as the
// unskewed control — the two runs differ in exactly one variable.
func runSkewCase(t *testing.T, skew time.Duration) {
	t.Helper()

	var control, skewed int

	scenario.New(t).
		WithSettlementWindow(2 * time.Second).
		Run(func(s *scenario.Session) {
			control = skewRun(s, 0)
		})

	scenario.New(t).
		WithSettlementWindow(2 * time.Second).
		Run(func(s *scenario.Session) {
			skewed = skewRun(s, skew)

			publishers := s.Status().Publishers
			require.Len(t, publishers, 1)
			assert.InDelta(t, skew.Seconds(), publishers[0].ClockSkewSeconds, 1.0,
				"the skew is measured and reported, because an operator reading "+
					"publisher timestamps in the output needs to know they are wrong")

			s.Explain("block:late").RequireDiagnosis(explain.CodeClockSkewHigh).
				RequireText("uses local receive time")
		})

	assert.Equal(t, control, skewed,
		"a publisher clock %s produced a different number of findings than one "+
			"that was correct; settlement must depend on driftwatch's receive time "+
			"and nothing else", skew)
	t.Logf("skew %s: %d findings, identical to the unskewed control", skew, skewed)
}

// skewRun publishes one settled key and one key still in flight, with every
// publisher timestamp offset by skew, and returns how many findings a sweep
// produced.
func skewRun(s *scenario.Session, skew time.Duration) int {
	stamp := func() string {
		return s.Now().Add(skew).Format(time.RFC3339Nano)
	}

	// A key written and materialized, then left alone long enough to settle.
	settled := s.Msg(scenario.Event{
		Seq: 1, Op: "set", Key: "block:settled", Value: "v1", TS: stamp(),
	})
	s.Ingest(settled)
	s.Materialize(settled)

	s.AdvanceClock(10 * time.Second)

	// A key written just now, which the store has not caught up with. It is
	// inside the window whatever the publisher's clock says.
	late := s.Msg(scenario.Event{
		Seq: 2, Op: "set", Key: "block:late", Value: "v1", TS: stamp(),
	})
	s.Ingest(late)

	s.AdvanceClock(500 * time.Millisecond)

	report := s.Sweep()
	return report.Total()
}
