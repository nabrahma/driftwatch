package faults

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/source"
	"github.com/nabrahma/driftwatch/test/harness/scenario"
)

// §15.1 rows 13 and 14 — partitions.
//
// The pair follows the same logic as rows 1 and 2, one level up: a partition of
// driftwatch's source is driftwatch going blind, and a partition of the
// materializer's is the store falling behind. The first must never produce a
// finding, however far the two views diverge while it lasts.

func TestFault13_PartitionOfDriftwatchsSource(t *testing.T) {
	// Row 13: on reconnect every key is marked Suspect, because PUB/SUB offers
	// no replay and the subscriber cannot find out what it missed. The
	// confirmed count stays 0 throughout.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			for seq := uint64(1); seq <= 20; seq++ {
				msg := s.Msg(scenario.Event{Seq: seq, Op: "set", Key: keyFor(seq), Value: "v1"})
				s.Ingest(msg)
				s.Materialize(msg)
			}

			s.Settle()
			s.RequireNoFindings(s.Sweep())
			s.RequireTrust(keyFor(1), oracle.TrustComplete)

			// Thirty seconds of silence on driftwatch's side. The publisher
			// keeps publishing and the store keeps up.
			for seq := uint64(21); seq <= 60; seq++ {
				s.Materialize(s.Msg(scenario.Event{
					Seq: seq, Op: "set", Key: keyFor(seq), Value: "v1",
				}))
			}
			s.AdvanceClock(30 * time.Second)

			before := s.Gaps()
			s.SignalSourceGap(source.GapReconnect, "the socket was down for 30s")

			assert.Greater(t, s.Gaps(), before, "the possible loss was recorded")
			s.RequireTrust(keyFor(1), oracle.TrustSuspect,
				"a key driftwatch can no longer vouch for")

			report := s.SweepAndConfirm()

			s.RequireNoConfirmedDrift()
			s.RequireAllFindingsSuspect(report)
			assert.Zero(t, s.Status().DivergentKeys,
				"driftwatch was blind, so it has nothing to accuse the store of")

			// Once a publisher retransmits, trust is restored and driftwatch
			// starts asserting again — otherwise a single reconnect would
			// silence the tool permanently.
			s.Ingest(s.Msg(scenario.Event{Seq: 61, Op: "snapshotBegin"}))
			for seq := uint64(62); seq <= 101; seq++ {
				msg := s.Msg(scenario.Event{
					Seq: seq, Op: "set", Key: keyFor(seq - 41), Value: "v1",
				})
				s.Ingest(msg)
				s.Materialize(msg)
			}
			s.Ingest(s.Msg(scenario.Event{Seq: 102, Op: "snapshotEnd"}))

			// The stream resumes at seq 61, so every one of these waits for the
			// predecessors the partition swallowed. They are applied once that
			// wait runs out, which is the reorder buffer refusing to call a hole
			// a gap until it is sure nothing will fill it.
			s.Settle()
			s.Sweep()

			assert.Equal(t, uint64(1), s.Status().SnapshotsSeen)
			s.RequireTrust(keyFor(1), oracle.TrustComplete,
				"a completed snapshot makes what was missed irrelevant")
		})
}

func TestFault14_PartitionOfTheMaterializersSource(t *testing.T) {
	// Row 14: confirmed findings for every event in the window, which resolve
	// if the materializer re-syncs and persist if it does not.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			missed := make([]source.RawMessage, 0, 20)

			for seq := uint64(1); seq <= 40; seq++ {
				msg := s.Msg(scenario.Event{Seq: seq, Op: "set", Key: keyFor(seq), Value: "v1"})
				s.Ingest(msg)

				// The materializer is partitioned for the middle twenty.
				if seq > 10 && seq <= 30 {
					missed = append(missed, msg)
					continue
				}
				s.Materialize(msg)
			}

			s.SweepAndConfirm()

			require.Len(t, s.Confirmed(), 20,
				"every write the store missed is a key that is genuinely wrong")
			assert.Equal(t, 20, s.Status().DivergentKeys)
			assert.Zero(t, s.Status().SuspectDivergentKeys,
				"driftwatch saw the whole stream, so it stands behind all of it")

			// The materializer re-syncs and every finding is withdrawn.
			for _, msg := range missed {
				s.Materialize(msg)
			}
			s.Settle()
			s.Confirm()

			s.RequireNoFindings(s.Sweep())
			s.RequireNoConfirmedDrift()
			assert.Equal(t, 20.0, s.Metric("driftwatch_drift_resolved_total"),
				"a repaired episode is withdrawn rather than left on the dashboard")
		})
}
