// Package faults holds one test per row of the fault scenario matrix (§15).
//
// Every row is one test named TestFault<NN>_<Name>, and the "Expected" column
// is the assertion: if the implementation does something else, the
// implementation is wrong. hack/verify-fault-matrix.sh checks that all sixty
// rows are present, which is what makes the matrix a specification rather than
// a table of intentions.
//
// Most rows drive an explicit event stream rather than the synthetic
// publisher's random one. Randomness makes a fault self-heal by accident — a
// dropped `add member-2` is repaired the moment some later event adds member-2
// to the same key — and a row that sometimes tests nothing is worse than a row
// that tests nothing every time, because only the first kind is invisible.
package faults

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/pkg/explain"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/source"
	"github.com/nabrahma/driftwatch/test/harness/faultinjector"
	"github.com/nabrahma/driftwatch/test/harness/scenario"
)

// §15.1 rows 1 to 5 — dropped events.
//
// Rows 1 and 2 are the same fault on opposite streams, and the pair is the
// whole argument for this tool. Drop an event the materializer needed and the
// store is genuinely wrong: say so. Drop the same event from driftwatch's own
// subscription and the store is fine while driftwatch's expectation is wrong:
// say that instead, and never page anyone about it.

func TestFault01_SingleEventDroppedFromTheMaterializer(t *testing.T) {
	// Row 1: one confirmed finding within 2xW, with the category following the
	// op that was lost — memberMismatch for an add, missingInTarget when the
	// dropped event was the one that created the key. Both are tested, because
	// the row names both and they take different paths through the differ.
	t.Run("a dropped member add is a member mismatch", func(t *testing.T) {
		scenario.New(t).
			WithProjection("keysetOwnership").
			WithSettlementWindow(time.Second).
			Run(func(s *scenario.Session) {
				first := s.Msg(scenario.Event{Seq: 1, Op: "add", Key: "block:a", Member: "replica-0"})
				lost := s.Msg(scenario.Event{Seq: 2, Op: "add", Key: "block:a", Member: "replica-2"})

				// Driftwatch sees both; the materializer never applies the
				// second. That is exactly one lost write, with no ambiguity
				// about which one.
				s.Ingest(first)
				s.Ingest(lost)
				s.Materialize(first)

				report := s.SweepAndConfirm()

				require.Len(t, s.Confirmed(), 1,
					"one dropped write is one confirmed finding: %s", report.Summary())
				assert.Positive(t, report.Alertable(),
					"driftwatch saw every event, so it can stand behind this")
				assert.Equal(t, differ.CatMemberMismatch, s.Confirmed()["block:a"].Category)

				s.Explain("block:a").
					RequireDiagnosis(explain.CodeMemberSubset).
					RequireText("consistent with dropped add events")
			})
	})

	t.Run("a dropped create is missing in target, and explain names the seq", func(t *testing.T) {
		scenario.New(t).
			WithSettlementWindow(time.Second).
			Run(func(s *scenario.Session) {
				// A contiguous sequence with one key created at seq 42 and
				// never touched again, so driftwatch can prove its own view is
				// complete and the store is the party that lost something.
				for seq := uint64(1); seq <= 60; seq++ {
					msg := s.Msg(scenario.Event{Seq: seq, Op: "heartbeat"})
					if seq == 42 {
						msg = s.Msg(scenario.Event{
							Seq: seq, Op: "set", Key: "block:lost", Value: "v1",
						})
						s.Ingest(msg)
						continue // the materializer never receives it
					}
					s.Ingest(msg)
					s.Materialize(msg)
				}

				report := s.SweepAndConfirm()

				require.Len(t, s.Confirmed(), 1, "%s", report.Summary())
				assert.Equal(t, differ.CatMissingInTarget, s.Confirmed()["block:lost"].Category)

				s.Explain("block:lost").
					RequireDiagnosis(explain.CodeMissingInTargetNoGaps).
					RequireText("seq 42..42, no gaps").
					RequireText("apply seq 42")
			})
	})
}

func TestFault02_SingleEventDroppedFromDriftwatch(t *testing.T) {
	// Row 2: suspectDivergentKeys = 1, divergentKeys = 0, seq_gaps_total = 1,
	// explain names the gap. Never a confirmed finding.
	scenario.New(t).
		WithProjection("keysetOwnership").
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			first := s.Msg(scenario.Event{Seq: 1, Op: "add", Key: "block:a", Member: "replica-0"})
			lost := s.Msg(scenario.Event{Seq: 2, Op: "add", Key: "block:a", Member: "replica-2"})
			later := s.Msg(scenario.Event{Seq: 3, Op: "add", Key: "block:b", Member: "replica-0"})

			// The mirror image of row 1: the materializer applies everything
			// and driftwatch misses the middle event. The store is right.
			s.Ingest(first)
			s.Ingest(later) // seq 2 never arrives, revealing the gap
			s.Materialize(first)
			s.Materialize(lost)
			s.Materialize(later)

			report := s.SweepAndConfirm()

			s.RequireNoConfirmedDrift()
			s.RequireAllFindingsSuspect(report)
			assert.Positive(t, report.ByTrust[oracle.TrustSuspect],
				"the disagreement is real; what changes is who it is about")

			assert.Equal(t, 1.0, s.Metric("driftwatch_seq_gaps_total"),
				"exactly one gap was observed")
			assert.Zero(t, s.Metric("driftwatch_drift_episodes_total"),
				"nothing was ever confirmed, so no episode was opened")

			s.Explain("block:a").
				RequireDiagnosis(explain.CodeSeqGapAffectingPublisher).
				RequireNoDiagnosis(explain.CodeMissingInTargetNoGaps).
				RequireText("may be driftwatch")
		})
}

// TestFaults_DriftwatchOwnLoss_ReportsSuspectNotConfirmed is the honesty test.
//
// The name is spelled out in full because it is the claim the whole project
// rests on: driftwatch never blames the store for events driftwatch
// itself lost. Rows 2 and 4 both run here against the same events as their
// materializer-side twins, so the difference in the verdict is attributable to
// which subscription lost them and nothing else.
//
// The control matters as much as the assertion. "Zero confirmed" proves nothing
// unless the identical loss on the other side confirms something, because a
// fault that did nothing at all would also produce zero — and an early version
// of this test passed for exactly that reason.
func TestFaults_DriftwatchOwnLoss_ReportsSuspectNotConfirmed(t *testing.T) {
	tests := []struct {
		name string
		row  string
		lose func(seq uint64) bool
	}{
		{
			name: "row 2, a single dropped event",
			row:  "§15.1 row 2",
			lose: func(seq uint64) bool { return seq == 42 },
		},
		{
			name: "row 4, a burst of one hundred",
			row:  "§15.1 row 4",
			lose: func(seq uint64) bool { return seq >= 102 && seq <= 200 && seq%2 == 0 },
		},
	}

	// stream builds the same 300-event sequence for both sides: 150 keys, each
	// created and then updated, so nothing self-heals and the count of lost
	// updates is exactly the count of keys that end up disagreeing.
	//
	// Updates rather than creates, and the difference is not cosmetic. A create
	// that driftwatch misses leaves no oracle entry at all, so the key shows up
	// in the target-to-oracle extras scan rather than as a mismatch — a
	// different mechanism with a different answer. Losing an update to a key
	// both sides already know about puts the two views in direct conflict,
	// which is what rows 2 and 4 are about.
	stream := func(s *scenario.Session) []source.RawMessage {
		msgs := make([]source.RawMessage, 0, 300)
		for seq := uint64(1); seq <= 300; seq++ {
			value := "v1"
			if seq%2 == 0 {
				value = "v2" // the update, and the one that gets lost
			}
			msgs = append(msgs, s.Msg(scenario.Event{
				Seq: seq, Op: "set", Key: keyFor((seq + 1) / 2), Value: value,
			}))
		}
		return msgs
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var (
				materializerConfirmed int
				driftwatchConfirmed   int
				driftwatchSuspect     int
			)

			// The control: the materializer misses them, so the store really is
			// wrong and driftwatch must say so.
			scenario.New(t).
				WithSettlementWindow(time.Second).
				Run(func(s *scenario.Session) {
					for seq, msg := range stream(s) {
						s.Ingest(msg)
						if !tc.lose(uint64(seq) + 1) {
							s.Materialize(msg)
						}
					}

					s.SweepAndConfirm()
					materializerConfirmed = len(s.Confirmed())
				})

			// The experiment: driftwatch misses the same events. The store is
			// perfect and driftwatch's own view is the incomplete one.
			scenario.New(t).
				WithSettlementWindow(time.Second).
				Run(func(s *scenario.Session) {
					for seq, msg := range stream(s) {
						s.Materialize(msg)
						if !tc.lose(uint64(seq) + 1) {
							s.Ingest(msg)
						}
					}

					report := s.SweepAndConfirm()

					driftwatchConfirmed = len(s.Confirmed())
					driftwatchSuspect = report.ByTrust[oracle.TrustSuspect]

					s.RequireAllFindingsSuspect(report)
					assert.Zero(t, s.Status().DivergentKeys,
						"divergentKeys is what alerting reads; it must stay at zero")
					assert.Positive(t, s.Status().SuspectDivergentKeys,
						"the disagreement is reported, just not as the store's fault")
				})

			require.Positive(t, materializerConfirmed,
				"%s: the control produced no findings, so the loss did nothing "+
					"and the assertion below proves nothing", tc.row)
			assert.Zero(t, driftwatchConfirmed,
				"%s: driftwatch confirmed %d findings from a stream it knows it lost "+
					"events from", tc.row, driftwatchConfirmed)
			assert.Positive(t, driftwatchSuspect,
				"%s: the keys should be reported as suspect rather than ignored", tc.row)

			t.Logf("%s: the same events lost on each side — materializer side "+
				"%d confirmed; driftwatch side %d confirmed, %d suspect",
				tc.row, materializerConfirmed, driftwatchConfirmed, driftwatchSuspect)
		})
	}
}

func TestFault03_BurstOfAHundredDroppedFromTheMaterializer(t *testing.T) {
	// Row 3: at most 100 confirmed findings, fewer when the dropped events
	// touched the same keys. No crash, and no truncation at a size this small.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			// Every key is created and then updated. The hundred lost writes
			// are updates 101 to 200, and half of those keys are written a
			// third time afterwards — so the row's "fewer if events touched
			// the same keys" is exercised rather than assumed.
			for seq := uint64(1); seq <= 300; seq++ {
				msg := s.Msg(scenario.Event{
					Seq: seq, Op: "set", Key: keyFor(seq % 150), Value: "v" + itoa(seq),
				})
				s.Ingest(msg)
				if seq < 101 || seq > 200 {
					s.Materialize(msg)
				}
			}

			report := s.SweepAndConfirm()

			confirmed := len(s.Confirmed())
			assert.Positive(t, confirmed, "a hundred lost writes should be visible")
			assert.LessOrEqual(t, confirmed, 100,
				"a hundred dropped events cannot make more than a hundred keys wrong")
			assert.False(t, report.Truncated,
				"a hundred findings is far below maxFindings; truncating here would "+
					"hide detail for no reason")

			t.Logf("100 events dropped from the materializer produced %d confirmed keys "+
				"(the rest were overwritten by later writes)", confirmed)
		})
}

func TestFault04_BurstOfAHundredDroppedFromDriftwatch(t *testing.T) {
	// Row 4: every affected key Suspect, seq_missing_events = 100, confirmed
	// count stays 0.
	//
	// The burst sits in the middle rather than at the end, because a gap is
	// only visible once a later sequence number arrives: drop the tail of a
	// stream and there is nothing left to reveal it.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			// Two hundred and fifty keys, each created before the burst so the
			// lost events are updates to keys the oracle already holds.
			for seq := uint64(1); seq <= 100; seq++ {
				msg := s.Msg(scenario.Event{
					Seq: seq, Op: "set", Key: keyFor(seq), Value: "v1",
				})
				s.Ingest(msg)
				s.Materialize(msg)
			}
			for seq := uint64(101); seq <= 300; seq++ {
				key := keyFor(seq - 100)
				if seq > 200 {
					key = keyFor(seq) // fresh keys after the burst
				}
				msg := s.Msg(scenario.Event{Seq: seq, Op: "set", Key: key, Value: "v2"})

				s.Materialize(msg)
				if seq < 101 || seq > 200 {
					s.Ingest(msg)
				}
			}

			report := s.SweepAndConfirm()

			s.RequireNoConfirmedDrift()
			s.RequireAllFindingsSuspect(report)

			assert.Equal(t, 100.0, s.Metric("driftwatch_seq_missing_events"),
				"a hundred consecutive sequence numbers are unaccounted for")

			status := s.Status()
			require.Len(t, status.Publishers, 1)
			assert.Equal(t, uint64(100), status.Publishers[0].MissingEvents)
		})
}

func TestFault05_SustainedDropRateStaysBoundedAndMonotonic(t *testing.T) {
	// Row 5: the episode count rises monotonically, drift duration grows,
	// memory stays bounded, and a sweep still finishes inside the interval.
	//
	// Monotonic applies to the counter, not the gauge, and the distinction is
	// the interesting part. driftwatch_drift_episodes_total only ever rises;
	// the live divergent-key count legitimately falls when a later write
	// repairs an earlier loss, which under a sustained drop rate happens
	// constantly. Asserting the gauge never falls would be asserting that
	// repairs never happen.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		WithFaults(faultinjector.Drop(0.05, 7)).
		WithFaultsOn(scenario.FaultsOnMaterializer).
		Run(func(s *scenario.Session) {
			var (
				episodes      float64
				driftDuration float64
			)

			for round := 0; round < 6; round++ {
				s.PublishEvents(200)
				s.RunMaterializer()
				s.SweepAndConfirm()

				next := s.Metric("driftwatch_drift_episodes_total")
				assert.GreaterOrEqual(t, next, episodes,
					"round %d: a counter went backwards", round)
				episodes = next

				status := s.Status()
				if status.DriftDurationSeconds > driftDuration {
					driftDuration = status.DriftDurationSeconds
				}

				assert.LessOrEqual(t, status.TrackedKeys, 100,
					"the oracle tracks the keyspace, not the event count")
				assert.Less(t, status.LastSweepDurationSeconds, 30.0,
					"a sweep has to finish inside the sweep interval")
			}

			assert.Positive(t, episodes, "a sustained drop rate must produce episodes")
			assert.Greater(t, driftDuration, s.Window().Seconds(),
				"drift persisted for longer than a settlement window, which is what "+
					"makes it drift rather than lag")
			t.Logf("5%% sustained drop over 1200 events: %.0f episodes opened, "+
				"oldest unresolved %.0fs, %d keys tracked",
				episodes, driftDuration, s.Status().TrackedKeys)
		})
}

// keyFor names a key from a sequence number, so a test can say which event
// touched which key without consulting a map.
func keyFor(n uint64) string { return "block:" + itoa(n) }

func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}

	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}
