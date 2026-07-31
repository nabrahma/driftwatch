package faults

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/test/harness/scenario"
)

// §15.1 rows 9 and 10 — duplicate delivery.
//
// At-least-once transports duplicate constantly, so this has to be free of
// consequence. Row 10 is the interesting one: a duplicate that arrives after
// the key has settled must not look like fresh activity, or the key drops back
// into flight and the sweep that would have compared it skips it instead. A
// duplicate every few seconds on a hot key would then mean that key is never
// compared at all — silence that looks exactly like health.

func TestFault09_ImmediateDuplicateIsAbsorbed(t *testing.T) {
	// Row 9: events_dropped_total{reason="duplicate"} increments, the oracle is
	// unchanged, and there are no findings.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			first := s.Msg(scenario.Event{Seq: 1, Op: "set", Key: "block:a", Value: "v1"})
			second := s.Msg(scenario.Event{Seq: 2, Op: "set", Key: "block:b", Value: "v1"})

			s.Ingest(first)
			s.Ingest(first) // the duplicate, immediately behind the original
			s.Ingest(second)

			s.Materialize(first)
			s.Materialize(second)

			report := s.SweepAndConfirm()

			assert.Equal(t, 1.0,
				s.MetricWith("driftwatch_events_dropped_total",
					map[string]string{"reason": "duplicate"}),
				"the duplicate was recognized rather than applied")

			s.RequireNoFindings(report)
			s.RequireNoConfirmedDrift()

			entry, ok := s.Oracle().Get("block:a")
			require.True(t, ok)
			assert.Equal(t, uint64(1), entry.Version,
				"an idempotent redelivery must not bump the version")
		})
}

func TestFault10_DelayedDuplicateDoesNotResetSettlement(t *testing.T) {
	// Row 10: as row 9, plus the assertion the row calls out — a duplicate
	// arriving after settlement must not move LastEventAt, or the key falsely
	// returns to being in flight.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			original := s.Msg(scenario.Event{Seq: 1, Op: "set", Key: "block:a", Value: "v1"})
			s.Ingest(original)
			s.Materialize(original)

			s.Settle()
			before, ok := s.Oracle().Get("block:a")
			require.True(t, ok)
			require.NotZero(t, before.LastEventAt)

			// Thirty seconds later the transport redelivers it.
			s.AdvanceClock(30 * time.Second)
			s.Ingest(s.Msg(scenario.Event{Seq: 1, Op: "set", Key: "block:a", Value: "v1"}))

			after, ok := s.Oracle().Get("block:a")
			require.True(t, ok)

			assert.Equal(t, before.LastEventAt, after.LastEventAt,
				"a duplicate is not new activity; moving LastEventAt would put a "+
					"settled key back in flight and stop it being compared")
			assert.Equal(t, before.Version, after.Version,
				"and the version must not move either, or a sweeper holding the "+
					"old one would believe the oracle had changed under it")

			assert.Equal(t, 1.0,
				s.MetricWith("driftwatch_events_dropped_total",
					map[string]string{"reason": "duplicate"}))

			// The key is still settled, so a sweep still compares it.
			report := s.Sweep()
			assert.Equal(t, 1, report.KeysCompared,
				"the key stayed eligible for comparison throughout")
			s.RequireNoFindings(report)
		})
}
