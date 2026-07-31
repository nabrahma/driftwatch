package faults

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/sweeper"
	"github.com/nabrahma/driftwatch/test/harness/scenario"
)

// §15.2 rows 36, 37 and 39 — the store being unavailable, slow, or the wrong
// one.
//
// Every row here is §23 A5: absence of data is not evidence of divergence. A
// store that cannot be read tells driftwatch nothing about whether it has
// drifted, and the only honest response is to report nothing new and say why.
// The failure mode this forbids is the tempting one — a detector that treats an
// unreachable store as a store full of missing keys reports a catastrophe every
// time the network hiccups, and is switched off within a week.

func TestFault36_TargetUnreachable(t *testing.T) {
	// Row 36: target_reachable goes to 0, the sweep is counted as
	// target_unavailable, and — the assertion the row emphasizes — the
	// divergent-key count is neither zeroed nor inflated. It is simply the last
	// thing driftwatch actually knew.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			// Establish a real, confirmed finding first, so there is something
			// that could be wrongly zeroed.
			for seq := uint64(1); seq <= 10; seq++ {
				msg := s.Msg(scenario.Event{Seq: seq, Op: "set", Key: keyFor(seq), Value: "v1"})
				s.Ingest(msg)
				if seq != 5 {
					s.Materialize(msg)
				}
			}

			s.SweepAndConfirm()
			require.Len(t, s.Confirmed(), 1)
			before := s.Status().DivergentKeys
			require.Equal(t, 1, before)

			// The store goes away.
			s.SetTargetHealth(unreachable())

			_, err := s.TrySweep()
			require.ErrorIs(t, err, sweeper.ErrTargetUnavailable,
				"a sweep against an unreachable store compares nothing and says so")

			assert.Equal(t, before, s.Status().DivergentKeys,
				"the count is what driftwatch last knew; zeroing it would clear an "+
					"alert that nothing has been fixed, and inflating it would page "+
					"someone about a network problem")
			assert.Equal(t, 1.0,
				s.MetricWith("driftwatch_sweeps_total",
					map[string]string{"result": "target_unavailable"}),
				"counted apart from an error, because it is not driftwatch failing")
			assert.Zero(t, s.Metric("driftwatch_target_reachable"))

			// On recovery, normal sweeps resume and the finding is still there
			// to be resolved or re-confirmed.
			s.SetTargetHealth(targetHealth(0, 10))

			report, err := s.TrySweep()
			require.NoError(t, err)
			assert.Equal(t, 1, report.Total(), "the sweep resumed where it left off")
			assert.Equal(t, 1.0, s.Metric("driftwatch_target_reachable"))
		})
}

func TestFault37_TargetHighLatency(t *testing.T) {
	// Row 37: sweep duration rises, no false findings appear, and the context
	// deadline is respected.
	//
	// A slow store is where a detector is most tempted to cut corners: give up
	// on a read and call the key missing, and every latency spike becomes a
	// drift report.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			fillBothSides(s, 40)

			// Latency is injected by moving the clock from the store's own
			// command callback rather than by sleeping in the target. A sleep
			// would be on the same injected clock the test advances, from the
			// same goroutine, which is a deadlock rather than a slow store.
			s.Target().ObserveCommands(func(name string) {
				if name != "INFO" && name != "DBSIZE" {
					s.AdvanceClock(50 * time.Millisecond)
				}
			})

			before := s.Now()
			report := s.Sweep()

			s.RequireNoFindings(report)
			assert.Positive(t, s.Now().Sub(before),
				"the latency shows up on the clock the sweep is measured by")
			assert.Positive(t, report.Duration(),
				"and in the duration the report carries, which is what "+
					"sweep_duration_seconds is built from")
			assert.Positive(t, s.Metric("driftwatch_sweep_duration_seconds"),
				"a slow sweep is observable rather than merely slow")

			t.Logf("a sweep of %d keys at 50ms per read took %s",
				report.KeysCompared, report.Duration())
		})
}

func TestFault39_FailoverToALaggingReplica(t *testing.T) {
	// Row 39: with requirePrimary the sweep refuses outright; without it, the
	// transient findings a lagging replica produces must resolve rather than be
	// confirmed, because two-phase confirmation rides out a lag shorter than W.
	t.Run("requirePrimary refuses to compare against a replica", func(t *testing.T) {
		scenario.New(t).
			WithSettlementWindow(time.Second).
			WithPolicy(func(p *check.PolicySpec) { p.RequirePrimary = true }).
			Run(func(s *scenario.Session) {
				fillBothSides(s, 20)
				s.RequireNoFindings(s.Sweep())

				// Sentinel promotes a replica and driftwatch is now pointed at
				// one. A replica is legitimately behind, so comparing against it
				// manufactures drift that does not exist.
				s.SetTargetHealth(replicaHealth())

				_, err := s.TrySweep()
				require.ErrorIs(t, err, sweeper.ErrNotPrimary)
				s.RequireNoConfirmedDrift()

				assert.Positive(t,
					s.MetricWith("driftwatch_sweeps_total",
						map[string]string{"result": "error"}),
					"refusing to compare is counted, so an operator can see the "+
						"check has stopped asserting rather than gone quiet")
			})
	})

	t.Run("without it, replica lag resolves rather than confirming", func(t *testing.T) {
		scenario.New(t).
			WithSettlementWindow(10 * time.Second).
			Run(func(s *scenario.Session) {
				s.SetTargetHealth(replicaHealth())

				// A write driftwatch has seen and the replica has not caught up
				// with yet. The lag is shorter than the settlement window.
				msg := s.Msg(scenario.Event{Seq: 1, Op: "set", Key: "block:a", Value: "v1"})
				s.Ingest(msg)

				s.AdvanceClock(11 * time.Second)
				first := s.Sweep()
				require.Equal(t, 1, first.Total(),
					"the replica really is behind, and the first read sees it")
				require.Equal(t, 1, s.Sweeper().PendingConfirmations())

				// Replication catches up before the confirmation is due, which
				// is what "lag < W" means.
				s.Materialize(msg)

				s.AdvanceClock(11 * time.Second)
				s.Confirm()

				s.RequireNoConfirmedDrift()
				assert.Positive(t, s.Sweeper().Stats().TransientResolved,
					"classified as transient rather than confirmed, which is "+
						"two-phase confirmation doing exactly its job")
				s.RequireNoFindings(s.Sweep())
			})
	})
}
