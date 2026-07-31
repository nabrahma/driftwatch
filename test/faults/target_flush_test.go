package faults

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/test/harness/scenario"
)

// §15.2 rows 28, 38, 40 and 43 — the store losing everything at once.
//
// Mass absence is the case where a detector either proves its worth or becomes
// the outage. Thousands of findings arriving at once must be bounded in memory,
// truthful about their magnitude even when the detail is truncated, and honest
// about the cause: a flush is not an eviction, and saying so is the difference
// between an operator looking at their memory limits and looking at whoever ran
// the command.

// fillBothSides writes n keys through driftwatch and the store, and settles.
func fillBothSides(s *scenario.Session, n uint64) {
	for seq := uint64(1); seq <= n; seq++ {
		msg := s.Msg(scenario.Event{Seq: seq, Op: "set", Key: keyFor(seq), Value: "v1"})
		s.Ingest(msg)
		s.Materialize(msg)
	}
	s.Settle()
}

func TestFault28_FlushdbMidRun(t *testing.T) {
	// Row 28: mass confirmed missingInTarget, EvictionSuspected false because a
	// flush is not an eviction, the keyspace size visibly drops, the finding
	// list is capped with Truncated set, and no out-of-memory.
	const (
		keys        = 5000
		maxFindings = 500
	)

	scenario.New(t).
		WithSettlementWindow(time.Second).
		WithPolicy(func(p *check.PolicySpec) { p.MaxFindings = maxFindings }).
		Run(func(s *scenario.Session) {
			fillBothSides(s, keys)
			s.RequireNoFindings(s.Sweep())

			before := s.Status().TargetKeyspaceSize
			require.Equal(t, int64(keys), before)

			s.FlushTarget()

			report := s.SweepAndConfirm()

			assert.Equal(t, keys, report.Total(),
				"the count is complete even though the list is not: an operator "+
					"seeing 500 findings has to know whether the real number is 500 "+
					"or five thousand")
			assert.Len(t, report.Findings, maxFindings,
				"the list is capped so that mass divergence cannot exhaust memory")
			assert.True(t, report.Truncated, "and it says so rather than looking complete")
			assert.Equal(t, keys, report.ByCategory[differ.CatMissingInTarget])

			assert.False(t, report.EvictionSuspected,
				"a flush is not an eviction; blaming memory pressure would send the "+
					"operator to the wrong place entirely")
			assert.Zero(t, s.Status().TargetKeyspaceSize,
				"the keyspace collapsing is visible on its own metric")
			assert.Less(t, s.Status().TargetKeyspaceSize, before)
		})
}

func TestFault38_TargetRestartWithoutPersistence(t *testing.T) {
	// Row 38: a restart with no persistence is a flush plus a reconnect. The
	// findings must be the same, and the client must come back on its own —
	// a detector that needs restarting after its dependency restarts is a
	// detector that is down whenever it matters.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			fillBothSides(s, 200)
			s.RequireNoFindings(s.Sweep())

			// The store goes away and comes back empty.
			s.FlushTarget()
			s.SetTargetHealth(targetHealth(0, 0))

			report := s.SweepAndConfirm()

			assert.Equal(t, 200, report.Total(), "everything the store forgot is reported")
			assert.True(t, report.TargetHealth.Reachable,
				"the client reconnected without being told to")

			// And when the materializer refills it, every finding is withdrawn.
			for seq := uint64(1); seq <= 200; seq++ {
				s.Materialize(s.Msg(scenario.Event{
					Seq: seq, Op: "set", Key: keyFor(seq), Value: "v1",
				}))
			}
			s.Settle()
			s.Confirm()

			s.RequireNoFindings(s.Sweep())
			s.RequireNoConfirmedDrift()
		})
}

func TestFault40_ScanCursorResetByAConcurrentFlush(t *testing.T) {
	// Row 40: the scan must terminate rather than loop forever, and the run
	// time is bounded by a hard test timeout rather than by hope.
	//
	// The infinite loop this guards against is real: a SCAN cursor that goes
	// backwards after a FLUSHDB can walk the keyspace endlessly, and an
	// auditor that hangs is worse than one that reports nothing, because
	// nothing about it looks wrong from the outside.
	done := make(chan struct{})

	go func() {
		defer close(done)

		scenario.New(t).
			WithSettlementWindow(time.Second).
			Run(func(s *scenario.Session) {
				fillBothSides(s, 2000)

				// Flush from underneath the scan, on the callback the store
				// invokes for every command it issues.
				flushed := false
				s.Target().ObserveCommands(func(name string) {
					if name == "SCAN" && !flushed {
						flushed = true
						s.FlushTarget()
					}
				})

				report := s.ScanExtras()
				assert.NotNil(t, report, "the scan returned rather than looping")

				// Whatever it saw, it stopped. The second pass over an empty
				// keyspace then reports nothing, which is correct: every key it
				// might have called an extra is gone.
				s.Settle()
				s.RequireNoFindings(s.ScanExtras())
			})
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the extras scan did not terminate after the keyspace was flushed")
	}
}

func TestFault43_EmptyTargetAndAFullOracle(t *testing.T) {
	// Row 43: mass confirmed missingInTarget. The simplest possible statement
	// of what this tool is for — driftwatch knows what should be there, the
	// store has none of it, and nothing else in the system has noticed.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			for seq := uint64(1); seq <= 300; seq++ {
				s.Ingest(s.Msg(scenario.Event{
					Seq: seq, Op: "set", Key: keyFor(seq), Value: "v1",
				}))
			}

			report := s.SweepAndConfirm()

			assert.Equal(t, 300, report.Total())
			assert.Equal(t, 300, report.ByCategory[differ.CatMissingInTarget])
			assert.Len(t, s.Confirmed(), 300, "every one survived confirmation")
			assert.Equal(t, 300, s.Status().DivergentKeys)
			assert.Zero(t, s.Status().SuspectDivergentKeys,
				"driftwatch saw a complete stream, so it stands behind all of it")
		})
}
