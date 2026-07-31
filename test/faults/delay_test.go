package faults

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/test/harness/scenario"
)

// §15.1 rows 11 and 12 — delay.
//
// Delay is the fault the settlement window exists for, and row 11 is where the
// static and adaptive modes visibly part company. A uniform 2s delay under a
// static 1s window produces a steady stream of findings that are all wrong; the
// same delay under an adaptive window widens W past the measured lag and the
// findings stop. Both halves are asserted, because "adaptive is better" is only
// worth claiming if the static case demonstrably suffers.

func TestFault11_UniformDelayBeyondTheStaticWindow(t *testing.T) {
	// Row 11: static mode keeps reporting; adaptive mode recovers.
	t.Run("a static window narrower than the delay keeps finding drift", func(t *testing.T) {
		scenario.New(t).
			WithSettlementWindow(time.Second).
			Run(func(s *scenario.Session) {
				// driftwatch learns the value now; the store learns it two
				// seconds later, which is outside the one-second window.
				msg := s.Msg(scenario.Event{Seq: 1, Op: "set", Key: "block:a", Value: "v1"})
				s.Ingest(msg)

				s.AdvanceClock(1500 * time.Millisecond)
				report := s.Sweep()

				require.Equal(t, 1, report.Total(),
					"the key settled while the store was still behind, which under a "+
						"window this narrow is indistinguishable from drift")
				assert.Equal(t, 1, report.Alertable())

				// The write lands late, and the finding withdraws itself.
				s.AdvanceClock(500 * time.Millisecond)
				s.Materialize(msg)

				s.Settle()
				s.Confirm()
				s.RequireNoFindings(s.Sweep())
				assert.Positive(t, s.Sweeper().Stats().TransientResolved,
					"a candidate that resolved before confirmation is the mechanism "+
						"working, and it is counted rather than silently discarded")
			})
	})

	t.Run("an adaptive window grows past the measured lag", func(t *testing.T) {
		scenario.New(t).
			WithAdaptiveWindow(time.Second, 120*time.Second, 3.0).
			Run(func(s *scenario.Session) {
				before := s.Status().SettlementWindowSeconds
				require.InDelta(t, 1.0, before, 0.001,
					"an adaptive window starts at its floor, because there are no "+
						"measurements yet to widen it")

				// A run of keys whose writes land two seconds late, which is
				// what the estimator measures. Enough of them to pass the
				// hundred-observation floor: below that the estimator reports
				// Adaptive false and leaves W alone, because a window derived
				// from a handful of samples would swing on noise.
				for seq := uint64(1); seq <= 140; seq++ {
					msg := s.Msg(scenario.Event{
						Seq: seq, Op: "set", Key: keyFor(seq), Value: "v1",
					})
					s.Ingest(msg)
					s.AdvanceClock(2 * time.Second)
					s.Materialize(msg)
					s.Observe()
				}

				s.Settle()
				s.Sweep()

				after := s.Status().SettlementWindowSeconds
				assert.Greater(t, after, before,
					"the window widened once the lag had been measured")
				assert.LessOrEqual(t, after, 120.0, "and stayed inside its ceiling")

				t.Logf("adaptive window moved from %.1fs to %.1fs under a 2s delay",
					before, after)
			})
	})
}

func TestFault12_OnePublisherDelayedAffectsOnlyItsOwnKeys(t *testing.T) {
	// Row 12: only the delayed publisher's keys are affected, and its clock
	// skew stays at zero — this is receive delay, not a publisher whose clock
	// is wrong, and conflating the two would send an operator to the wrong
	// system entirely.
	scenario.New(t).
		WithProjection("scalar", map[string]string{
			"keyTemplate": "{{.Publisher}}:{{.Key}}",
		}).
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			prompt := s.Msg(scenario.Event{
				Publisher: "replica-0", Seq: 1, Op: "set", Key: "a", Value: "v1",
			})
			delayed := s.Msg(scenario.Event{
				Publisher: "replica-1", Seq: 1, Op: "set", Key: "a", Value: "v1",
			})

			s.Ingest(prompt)
			s.Ingest(delayed)
			s.Materialize(prompt) // replica-0's write lands immediately

			s.AdvanceClock(10 * time.Second)
			report := s.Sweep()

			require.Equal(t, 1, report.Total(),
				"only the delayed publisher's key should be reported: %s", report.Summary())
			require.Len(t, report.Findings, 1)
			assert.Equal(t, "replica-1:a", report.Findings[0].Key)

			// The skew gauge is about publisher clocks, and neither publisher's
			// clock is wrong.
			for _, p := range s.Status().Publishers {
				assert.InDelta(t, 0.0, p.ClockSkewSeconds, 0.001,
					"publisher %s: a slow network is not a skewed clock", p.ID)
			}

			// The write lands eventually and the finding withdraws itself.
			s.Materialize(delayed)
			s.Settle()
			s.Confirm()
			s.RequireNoFindings(s.Sweep())
		})
}
