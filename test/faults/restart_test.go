package faults

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/test/harness/scenario"
)

// §15.1 rows 20 to 22 — publisher restarts.
//
// A restart resets a sequence space, and a sequence space is the only thing
// driftwatch has for deciding whether it saw everything. Handle it naively and
// the tool reports either a catastrophe that did not happen or nothing at all.

func TestFault20_ExplicitRestartWithAnEpochBump(t *testing.T) {
	// Row 20: publisher_restarts_total{kind="explicit"} = 1, no gap recorded,
	// the high-water mark resets, and the keys are Suspect — because a restart
	// without a snapshot means events in flight at the moment of the restart
	// are unaccounted for. The row asks for both halves: declared-and-clean
	// only holds once a snapshot has followed.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			for seq := uint64(1); seq <= 40; seq++ {
				msg := s.Msg(scenario.Event{Seq: seq, Op: "set", Key: keyFor(seq), Value: "v1"})
				s.Ingest(msg)
				s.Materialize(msg)
			}
			s.RequireTrust(keyFor(1), oracle.TrustComplete)

			// The publisher restarts and says so: a new epoch, sequence back to
			// the start.
			restart := s.Msg(scenario.Event{
				Epoch: 2, Seq: 0, Op: "set", Key: keyFor(100), Value: "v1",
			})
			s.Ingest(restart)
			s.Materialize(restart)

			assert.Zero(t, s.Metric("driftwatch_seq_gaps_total"),
				"a declared restart is not forty thousand missing events; the epoch "+
					"says the sequence space is new")
			assert.Zero(t, s.Metric("driftwatch_seq_missing_events"))

			status := s.Status()
			require.Len(t, status.Publishers, 1)
			assert.Equal(t, uint64(2), status.Publishers[0].Epoch)
			assert.Equal(t, uint64(0), status.Publishers[0].HighWaterMark,
				"the high-water mark follows the new epoch rather than the old one")

			// Without a snapshot, the keys are suspect: whatever the publisher
			// had in flight when it died is unaccounted for.
			s.RequireTrust(keyFor(1), oracle.TrustSuspect,
				"a restart with no snapshot leaves a hole nobody can size")

			report := s.SweepAndConfirm()
			s.RequireNoConfirmedDrift()
			s.RequireAllFindingsSuspect(report)

			// With one, it is clean again.
			s.Ingest(s.Msg(scenario.Event{Epoch: 2, Seq: 1, Op: "snapshotBegin"}))
			s.Ingest(s.Msg(scenario.Event{Epoch: 2, Seq: 2, Op: "snapshotEnd"}))

			s.RequireTrust(keyFor(1), oracle.TrustComplete,
				"a declared restart followed by a snapshot is a clean restart")
		})
}

func TestFault21_ImplicitRestart(t *testing.T) {
	// Row 21: the sequence drops to 1 with no epoch change, and this is the
	// test that catches the naive implementation.
	//
	// The naive reading of "seq 1 arrived and I was at 900,000" is that
	// 899,999 events went missing. driftwatch would mark every key suspect,
	// report nothing for as long as the operator kept it running, and the
	// metric would show a number so large it would be assumed to be a bug —
	// which it would be. The heuristic exists so a publisher that came back
	// without saying so is recognized as a restart rather than an apocalypse.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			const before = 900_000

			// A publisher a long way into its sequence space. The events in
			// between are never sent — driftwatch adopts the first sequence it
			// sees as a baseline rather than assuming it missed everything
			// before it.
			s.Ingest(s.Msg(scenario.Event{Seq: before, Op: "set", Key: "block:a", Value: "v1"}))
			s.Ingest(s.Msg(scenario.Event{Seq: before + 1, Op: "set", Key: "block:b", Value: "v1"}))

			require.Zero(t, s.Metric("driftwatch_seq_gaps_total"),
				"the first sequence number seen is a baseline, not a gap")

			// The process restarts and forgets to bump its epoch, which is what
			// a publisher that keeps its epoch in memory does.
			s.Ingest(s.Msg(scenario.Event{Seq: 1, Op: "set", Key: "block:c", Value: "v1"}))
			s.Ingest(s.Msg(scenario.Event{Seq: 2, Op: "set", Key: "block:d", Value: "v1"}))

			assert.Equal(t, 1.0,
				s.MetricWith("driftwatch_publisher_restarts_total",
					map[string]string{"kind": "implicit"}),
				"the drop was recognized as a restart, not as loss")

			missing := s.Metric("driftwatch_seq_missing_events")
			assert.Less(t, missing, 1000.0,
				"a restart recorded %.0f missing events; the naive implementation "+
					"records ~900,000 here, and this row exists to catch exactly that",
				missing)
			assert.Less(t, s.Metric("driftwatch_seq_gaps_total"), 2.0,
				"one restart is at most one gap, whatever the sequence numbers say")

			status := s.Status()
			require.Len(t, status.Publishers, 1)
			assert.Equal(t, uint64(1), status.Publishers[0].Restarts)
			assert.Equal(t, uint64(2), status.Publishers[0].HighWaterMark,
				"the high-water mark follows the publisher's new incarnation")

			t.Logf("sequence dropped from %d to 1: %.0f missing events recorded, "+
				"%.0f restarts", before+1, missing, 1.0)
		})
}

func TestFault22_StaleEventFromAPreviousEpoch(t *testing.T) {
	// Row 22: events_dropped_total{reason="stale_epoch"} = 1 and the oracle is
	// unchanged. A message that was in flight across a restart carries the old
	// epoch, and applying it would resurrect state the new incarnation has
	// already moved past.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			s.Ingest(s.Msg(scenario.Event{Epoch: 1, Seq: 1, Op: "set", Key: "block:a", Value: "v1"}))
			s.Ingest(s.Msg(scenario.Event{Epoch: 2, Seq: 1, Op: "set", Key: "block:a", Value: "v2"}))

			before, ok := s.Oracle().Get("block:a")
			require.True(t, ok)
			require.Equal(t, `scalar("v2")`, before.Value.String())

			// The straggler from epoch 1, arriving after the new incarnation
			// has already written.
			s.Ingest(s.Msg(scenario.Event{Epoch: 1, Seq: 2, Op: "set", Key: "block:a", Value: "stale"}))

			assert.Equal(t, 1.0,
				s.MetricWith("driftwatch_events_dropped_total",
					map[string]string{"reason": "stale_epoch"}),
				"the event was refused for its epoch, not for its content")

			after, ok := s.Oracle().Get("block:a")
			require.True(t, ok)
			assert.Equal(t, before.Value.String(), after.Value.String(),
				"a superseded incarnation cannot overwrite the current one")
			assert.Equal(t, before.Version, after.Version,
				"and it must not bump the version either")
		})
}

var _ = time.Second
