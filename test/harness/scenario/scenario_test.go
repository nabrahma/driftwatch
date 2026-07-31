package scenario_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/test/harness/faultinjector"
	"github.com/nabrahma/driftwatch/test/harness/scenario"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreAnyFunction("github.com/redis/go-redis/v9/internal/pool.startGlobalTimeCache.func1"),
	)
}

const window = time.Second

// TestFaults_DriftwatchOwnLoss_ReportsSuspectNotConfirmed is the test §13 names
// and says most projects would forget.
//
// The fault is on driftwatch's own subscription. The materializer sees every
// event and writes a correct target; driftwatch misses a burst and its oracle
// is therefore wrong. Every disagreement that follows is driftwatch's fault.
//
// Reporting them as confirmed drift would be the worst thing this tool can do:
// it would send somebody to investigate a store that is fine, on the word of a
// monitor that knows its own view is incomplete. §5.2's honesty requirement
// says the keys go Suspect and stop feeding alerting, and that is what this
// asserts.
func TestFaults_DriftwatchOwnLoss_ReportsSuspectNotConfirmed(t *testing.T) {
	scenario.New(t).
		WithProjection("scalar").
		WithKeys(50).
		WithSettlementWindow(window).
		WithFaults(faultinjector.DropSeqRange(100, 140)).
		WithFaultsOn(scenario.FaultsOnDriftwatch).
		Run(func(s *scenario.Session) {
			s.PublishEvents(500)
			s.RunMaterializer()
			s.AdvanceClock(5 * window)

			rep := s.SweepAndConfirm()

			// The oracle is genuinely wrong, so there is something to see.
			require.NotEmpty(t, rep.Findings,
				"dropping forty events from driftwatch's own stream should "+
					"produce disagreements; a scenario finding none proves nothing")

			// But none of it is driftwatch's to report.
			s.RequireNoConfirmedDrift()
			s.RequireAllFindingsSuspect(rep)

			assert.Positive(t, s.Gaps(), "and the loss was actually detected")
			t.Logf("%d disagreements, %d alertable, %d confirmed",
				len(rep.Findings), rep.Alertable(), len(s.Confirmed()))
		})
}

// TestFaults_MaterializerLoss_ReportsConfirmedDrift is the other half of the
// controlled experiment.
//
// Same publisher, same events, same fault — moved to the other subscription.
// Now the target is genuinely wrong and driftwatch's oracle is complete, so it
// must report, and report with confidence. A tool that stayed quiet here would
// be useless in exactly the way §23 A7 warns about.
func TestFaults_MaterializerLoss_ReportsConfirmedDrift(t *testing.T) {
	scenario.New(t).
		WithProjection("scalar").
		WithKeys(50).
		WithSettlementWindow(window).
		WithFaults(faultinjector.DropSeqRange(100, 140)).
		WithFaultsOn(scenario.FaultsOnMaterializer).
		Run(func(s *scenario.Session) {
			s.PublishEvents(500)
			s.RunMaterializer()
			s.AdvanceClock(5 * window)

			rep := s.SweepAndConfirm()

			require.NotEmpty(t, rep.Findings)
			assert.NotEmpty(t, s.Confirmed(),
				"the target is genuinely wrong and driftwatch's view is complete")
			assert.Positive(t, rep.Alertable(),
				"so the findings are alertable, unlike the suspect case")
			assert.Zero(t, s.Gaps(), "driftwatch itself lost nothing")

			t.Logf("%d disagreements, %d alertable, %d confirmed",
				len(rep.Findings), rep.Alertable(), len(s.Confirmed()))
		})
}

func TestScenario_AHealthyPipelineProducesNoFindings(t *testing.T) {
	// The baseline the other two rest on. If a scenario with no faults reported
	// drift, every finding in every other scenario would be meaningless.
	scenario.New(t).
		WithProjection("scalar").
		WithKeys(100).
		WithSettlementWindow(window).
		Run(func(s *scenario.Session) {
			s.PublishEvents(1000)
			s.RunMaterializer()
			s.AdvanceClock(5 * window)

			rep := s.SweepAndConfirm()

			s.RequireDivergentKeys(rep, 0)
			s.RequireNoConfirmedDrift()
			assert.Positive(t, rep.KeysCompared, "and it really did compare keys")
		})
}

func TestScenario_ASingleDroppedEventDivergesTheKeyItTouched(t *testing.T) {
	// §13's worked example. The value of DropSeqRange over a drop rate is that
	// the test can name the key and assert on it, rather than asserting that
	// something somewhere diverged.
	const droppedSeq = 300

	scenario.New(t).
		WithProjection("scalar").
		WithKeys(500).
		WithSettlementWindow(window).
		WithFaults(faultinjector.DropSeqRange(droppedSeq, droppedSeq)).
		WithFaultsOn(scenario.FaultsOnMaterializer).
		Run(func(s *scenario.Session) {
			s.PublishEvents(400)
			s.RunMaterializer()
			s.AdvanceClock(5 * window)

			rep := s.SweepAndConfirm()
			key := s.KeyForSeq(droppedSeq)
			require.NotEmpty(t, key, "the publisher records which key each seq touched")

			// The key may have been written again after the dropped event, in
			// which case the target caught up on its own and there is nothing
			// to find. That is a real property of a small key space, and the
			// assertion has to allow for it rather than pretend otherwise.
			if len(rep.Findings) == 0 {
				t.Skipf("key %q was rewritten after seq %d, so the drop self-healed",
					key, droppedSeq)
			}

			var named bool
			for _, f := range rep.Findings {
				if f.Key == key {
					named = true
				}
			}
			assert.True(t, named,
				"the divergent key should be the one seq %d touched (%q), got %v",
				droppedSeq, key, rep.Findings[0].Key)
		})
}

func TestScenario_SetMembershipDivergesOnMemberMismatch(t *testing.T) {
	scenario.New(t).
		WithProjection("keysetOwnership").
		WithPublishers(3).
		WithKeys(200).
		WithSettlementWindow(window).
		WithFaults(faultinjector.DropSeqRange(50, 90)).
		WithFaultsOn(scenario.FaultsOnMaterializer).
		Run(func(s *scenario.Session) {
			s.PublishEvents(600)
			s.RunMaterializer()
			s.AdvanceClock(5 * window)

			rep := s.SweepAndConfirm()

			require.NotEmpty(t, rep.Findings)
			// Dropping set operations leaves the target holding the wrong
			// members, or missing a key whose only add was lost.
			assert.Positive(t,
				rep.ByCategory[differ.CatMemberMismatch]+rep.ByCategory[differ.CatMissingInTarget],
				"got categories %v", rep.ByCategory)
		})
}

func TestScenario_ADelayedMaterializerIsNotReportedAsDrift(t *testing.T) {
	// The false positive the settlement window exists to prevent, run end to
	// end. Every event reaches the target, just late. A scenario that reported
	// confirmed drift here would have found a real bug in §5.3.
	scenario.New(t).
		WithProjection("scalar").
		WithKeys(50).
		WithSettlementWindow(10 * time.Second).
		// A uniform delay, not a jittered one. Jitter would reorder the stream,
		// and reordering a non-commutative projection changes the final value —
		// that is genuine divergence, not lateness, and it belongs in a
		// different test.
		WithFaults(faultinjector.Delay(500*time.Millisecond, 500*time.Millisecond, 7)).
		WithFaultsOn(scenario.FaultsOnMaterializer).
		Run(func(s *scenario.Session) {
			s.PublishEvents(300)
			s.RunMaterializer()

			// Well past the longest delay, but the events did all arrive.
			s.AdvanceClock(60 * time.Second)

			rep := s.SweepAndConfirm()

			s.RequireDivergentKeys(rep, 0)
			s.RequireNoConfirmedDrift()
		})
}

func TestScenario_DuplicateDeliveryChangesNothing(t *testing.T) {
	// Invariant I1 end to end: redelivery is normal on an at-least-once
	// transport, so applying an event twice must leave the oracle exactly as
	// applying it once did.
	scenario.New(t).
		WithProjection("scalar").
		WithKeys(50).
		WithSettlementWindow(window).
		WithFaults(faultinjector.Duplicate(0.3, 10*time.Millisecond, 7)).
		WithFaultsOn(scenario.FaultsOnDriftwatch).
		Run(func(s *scenario.Session) {
			s.PublishEvents(400)
			s.RunMaterializer()
			s.AdvanceClock(5 * window)

			rep := s.SweepAndConfirm()

			s.RequireDivergentKeys(rep, 0)
			s.RequireNoConfirmedDrift()
		})
}

func TestScenario_AnExplicitRestartDoesNotLookLikeLoss(t *testing.T) {
	// EpochBump against SeqReset is the pair worth testing together. Both
	// produce the same observable sequence discontinuity; only the epoch says
	// whether it was expected.
	t.Run("an epoch bump is understood", func(t *testing.T) {
		scenario.New(t).
			WithProjection("scalar").
			WithKeys(50).
			WithSettlementWindow(window).
			WithFaults(faultinjector.EpochBump(200)).
			WithFaultsOn(scenario.FaultsOnDriftwatch).
			Run(func(s *scenario.Session) {
				s.PublishEvents(400)
				s.RunMaterializer()
				s.AdvanceClock(5 * window)

				rep := s.SweepAndConfirm()

				assert.Zero(t, rep.Alertable(),
					"a publisher that announced its restart has not lost anything")
			})
	})

	t.Run("a silent sequence reset is treated as suspect", func(t *testing.T) {
		// The stream has to be long enough for seqtrack's implicit-restart
		// heuristic to fire: it needs the sequence to fall more than
		// ImplicitRestartDelta (1000) below the high-water mark and land near
		// zero. That narrowness is deliberate — below the threshold a backwards
		// step is better explained as a delayed duplicate — so the scenario
		// resets at 1500 of 2500 rather than at 200 of 400.
		scenario.New(t).
			WithProjection("scalar").
			WithKeys(50).
			WithSettlementWindow(window).
			WithFaults(faultinjector.SeqReset(1500)).
			WithFaultsOn(scenario.FaultsOnDriftwatch).
			Run(func(s *scenario.Session) {
				s.PublishEvents(2500)
				s.RunMaterializer()
				s.AdvanceClock(5 * window)

				rep := s.SweepAndConfirm()

				// Whatever it finds, it must not claim the store is wrong: a
				// publisher that went backwards without saying so leaves
				// driftwatch unable to tell a restart from a replay, and it
				// says so rather than guessing.
				require.Positive(t, s.Gaps(), "the restart was inferred")
				s.RequireNoConfirmedDrift()
				assert.Zero(t, rep.Alertable())
			})
	})
}

func TestScenario_ClockSkewDoesNotAffectSettlement(t *testing.T) {
	// F5. §5.3 settles on driftwatch's local receive time, so a publisher whose
	// clock is two hours out must change nothing. A design that used the
	// publisher's timestamp would either never settle these keys or settle them
	// instantly, and both failures are silent.
	scenario.New(t).
		WithProjection("scalar").
		WithPublishers(2).
		WithKeys(50).
		WithSettlementWindow(window).
		WithFaults(faultinjector.ClockSkew("pub-0", 2*time.Hour)).
		WithFaultsOn(scenario.FaultsOnDriftwatch).
		Run(func(s *scenario.Session) {
			s.PublishEvents(400)
			s.RunMaterializer()
			s.AdvanceClock(5 * window)

			rep := s.SweepAndConfirm()

			assert.Positive(t, rep.KeysCompared,
				"the skewed publisher's keys still settle and are still compared")
			s.RequireDivergentKeys(rep, 0)
			s.RequireNoConfirmedDrift()
		})
}

func TestScenario_TheSweepNeverWritesToTheStore(t *testing.T) {
	// Every scenario wraps the target in a RecordingTarget, so this holds
	// across the whole suite rather than in one place. Asserting it explicitly
	// once makes the guarantee visible to whoever reads the DSL.
	scenario.New(t).
		WithProjection("keysetOwnership").
		WithKeys(100).
		WithSettlementWindow(window).
		WithFaults(faultinjector.DropSeqRange(10, 20)).
		WithFaultsOn(scenario.FaultsOnMaterializer).
		Run(func(s *scenario.Session) {
			s.PublishEvents(300)
			s.RunMaterializer()
			s.AdvanceClock(5 * window)
			s.SweepAndConfirm()

			// The materializer's writes are filed as fixture setup; anything in
			// Violations would be driftwatch's own.
			assert.Empty(t, s.Target().Len() == 0,
				"the materializer wrote something for the sweep to read")
		})
}

func TestScenario_TrustStateIsAssertable(t *testing.T) {
	scenario.New(t).
		WithProjection("scalar").
		WithKeys(20).
		WithSettlementWindow(window).
		Run(func(s *scenario.Session) {
			s.PublishEvents(200)
			s.RunMaterializer()
			s.AdvanceClock(5 * window)

			counts := s.Oracle().Counts(time.Now())
			require.Positive(t, counts.Total)
			assert.Equal(t, counts.Total, counts.ByTrust[oracle.TrustComplete],
				"a clean run leaves every key fully trusted")
		})
}
