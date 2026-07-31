package faults

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/pkg/explain"
	"github.com/nabrahma/driftwatch/pkg/target"
	"github.com/nabrahma/driftwatch/test/harness/scenario"
)

// §15.2 rows 29 to 32 — keys the store removed on purpose.
//
// Eviction and expiry produce exactly the observation drift does: the oracle
// expects a key and the store does not have it. Everything here is about
// telling the three apart, because the operator's next move is completely
// different in each case — fix the materializer, raise maxmemory, or change a
// setting that was always going to do this.

// targetHealth builds a store health report with an eviction count and a
// memory pressure percentage.
func targetHealth(evicted uint64, pressurePercent int) target.Health {
	const limit = 4 << 30

	return target.Health{
		Reachable:       true,
		Role:            "master",
		Version:         "memory",
		EvictedKeys:     evicted,
		MaxMemoryBytes:  limit,
		UsedMemoryBytes: uint64(limit / 100 * pressurePercent),
	}
}

// unreachable is a store that cannot be read at all.
func unreachable() target.Health {
	return target.Health{Reachable: false}
}

// replicaHealth is a store that answers, correctly, that it is not the primary.
func replicaHealth() target.Health {
	h := targetHealth(0, 10)
	h.Role = "replica"
	h.ReplicationLagMs = 250
	return h
}

func TestFault29_EvictionUnderMaxmemory(t *testing.T) {
	// Row 29: confirmed missingInTarget, plus a rising eviction counter, plus
	// explain saying eviction is the likely cause. All three together, because
	// the finding alone is indistinguishable from a materializer that lost the
	// write.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			fillBothSides(s, 200)
			s.RequireNoFindings(s.Sweep())

			// The store is nearly full but has not evicted yet, so the sweep
			// that establishes the baseline sees a counter of zero.
			s.SetTargetHealth(targetHealth(0, 97))
			s.Sweep()

			evicted := s.EvictFromTarget(20)
			require.Len(t, evicted, 20)
			s.SetTargetHealth(targetHealth(20, 97))

			// explain reads the delta against the last sweep, so it is asked
			// before the next sweep moves that baseline.
			s.Explain(evicted[0]).
				RequireDiagnosis(explain.CodeTargetEvictionLikely).
				RequireText("probably evicted").
				RequireText("of maxmemory")

			// The counter has to move *during* a sweep for EvictionSuspected:
			// a sweep that finds mass absence while the store is actively
			// evicting has an explanation, and one that finds it afterwards
			// does not. Bumping it from the store's own command callback is how
			// a real eviction would interleave.
			// The health probe bookending the sweep is what the comparison is
			// between, so the bump has to land on a data command in the middle
			// of it rather than on the probes themselves.
			// The health probes bookend a sweep, so the counter has to move on a
			// data command between them rather than on the probes themselves.
			// It keeps moving, because a store under memory pressure does not
			// evict once and stop.
			evictions := uint64(20)
			s.Target().ObserveCommands(func(name string) {
				if name == "INFO" || name == "DBSIZE" {
					return
				}
				evictions++
				s.SetTargetHealth(targetHealth(evictions, 98))
			})

			report := s.SweepAndConfirm()

			assert.Equal(t, 20, report.Total())
			assert.Equal(t, 20, report.ByCategory[differ.CatMissingInTarget])
			assert.True(t, report.EvictionSuspected,
				"the eviction counter moved during the sweep, which is the whole "+
					"explanation and has to reach the report")
			assert.Positive(t, s.Metric("driftwatch_target_evictions_observed_total"))
		})
}

func TestFault30_TTLExpiryUnderStrictPolicy(t *testing.T) {
	// Row 30: confirmed missingInTarget. Strict is the default because an index
	// usually has no TTLs at all, so a key that vanished is drift until the
	// operator says otherwise.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		WithPolicy(func(p *check.PolicySpec) { p.ExpiryPolicy = check.ExpiryStrict }).
		Run(func(s *scenario.Session) {
			msg := s.Msg(scenario.Event{Seq: 1, Op: "set", Key: "session:a", Value: "v1"})
			s.Ingest(msg)
			s.Materialize(msg)

			s.ExpireInTarget("session:a", s.Now().Add(5*time.Second))
			s.AdvanceClock(10 * time.Second)

			report := s.SweepAndConfirm()

			require.Equal(t, 1, report.Total(),
				"under Strict, a key the store no longer has is drift: %s", report.Summary())
			assert.Equal(t, 1, report.ByCategory[differ.CatMissingInTarget])
			assert.Len(t, s.Confirmed(), 1)
		})
}

func TestFault31_TTLExpiryUnderIgnorePolicy(t *testing.T) {
	// Row 31: no finding. Ignore is the blunt instrument for a keyspace whose
	// TTLs driftwatch cannot see: past assumedTTL, a missing key stops being
	// reported at all.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		WithPolicy(func(p *check.PolicySpec) {
			p.ExpiryPolicy = check.ExpiryIgnore
			p.AssumedTTL = check.Duration(5 * time.Second)
		}).
		Run(func(s *scenario.Session) {
			msg := s.Msg(scenario.Event{Seq: 1, Op: "set", Key: "session:a", Value: "v1"})
			s.Ingest(msg)
			s.Materialize(msg)

			s.ExpireInTarget("session:a", s.Now().Add(2*time.Second))
			s.AdvanceClock(30 * time.Second)

			report := s.SweepAndConfirm()

			s.RequireNoFindings(report)
			s.RequireNoConfirmedDrift()
		})
}

func TestFault32_TTLExpiryUnderModelPolicy(t *testing.T) {
	// Row 32: no finding, and the oracle expired the key itself.
	//
	// Model is the honest one: the events carry the TTL, so the oracle knows
	// when a key should go and the comparison stays meaningful in both
	// directions — a key still present after its TTL is as much a finding as
	// one missing before it.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		WithPolicy(func(p *check.PolicySpec) {
			p.ExpiryPolicy = check.ExpiryModel
			p.TTLTolerance = check.Duration(2 * time.Second)
		}).
		Run(func(s *scenario.Session) {
			// The event carries the lifetime, which is what Model requires: the
			// oracle can only expire a key itself if the stream told it when to.
			msg := s.Msg(scenario.Event{
				Seq: 1, Op: "set", Key: "session:a", Value: "v1", TTL: "5s",
			})
			s.Ingest(msg)
			s.Materialize(msg)

			held, ok := s.Oracle().Get("session:a")
			require.True(t, ok)
			require.False(t, held.Value.IsAbsent(), "the oracle holds it while it lives")

			s.ExpireInTarget("session:a", s.Now().Add(5*time.Second))
			s.AdvanceClock(30 * time.Second)

			report := s.SweepAndConfirm()

			s.RequireNoFindings(report)
			assert.Equal(t, 1, report.KeysCompared,
				"the key is still compared — the oracle expects nothing and the "+
					"store holds nothing, which is agreement rather than silence")

			gone, ok := s.Oracle().Get("session:a")
			require.True(t, ok, "the entry survives as a record of what was there")
			assert.True(t, gone.Expired, "and the oracle expired it too")
			assert.True(t, gone.Value.IsAbsent(),
				"so it no longer expects the store to hold anything")
		})
}
