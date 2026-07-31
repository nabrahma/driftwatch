package faults

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/target"
	"github.com/nabrahma/driftwatch/test/harness/scenario"
)

// §15.2 rows 44 to 46 — a store that already had contents before driftwatch
// attached.
//
// This is the first thing that happens to every deployment, and the three modes
// are three answers to one question: what does driftwatch believe about keys it
// never saw an event for? Getting it wrong is not subtle. A tool that reports
// the entire pre-existing keyspace as drift on startup is a tool nobody gets
// past the first five minutes with.

// seedExisting fills the store with keys that predate the subscription.
func seedExisting(n uint64) func(*target.MemoryTarget) {
	return func(m *target.MemoryTarget) {
		values := make(map[string][]byte, n)
		for i := uint64(1); i <= n; i++ {
			values[keyFor(i)] = []byte("pre-existing")
		}
		m.Seed(values)
	}
}

func TestFault44_FullTargetEmptyOracleUnderAdopt(t *testing.T) {
	// Row 44: zero findings, everything adopted, and the coverage ratio
	// reflects that adopted keys are not asserted on.
	//
	// Adopt is the pragmatic mode: read the store once, believe it, and start
	// watching from there. What it must not do is then claim to have verified
	// those keys — comparing an adopted key against the store proves only that
	// the store agrees with itself.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		WithPolicy(func(p *check.PolicySpec) { p.Bootstrap = check.BootstrapAdopt }).
		WithSeededTarget(seedExisting(200)).
		Run(func(s *scenario.Session) {
			require.Equal(t, 200, s.Oracle().Len(),
				"the whole keyspace was read in as a baseline")

			report := s.SweepAndConfirm()

			s.RequireNoFindings(report)
			s.RequireNoConfirmedDrift()

			s.RequireTrust(keyFor(1), oracle.TrustAdopted,
				"adopted, not verified: it came from the store rather than from an event")
			assert.Equal(t, 200.0,
				s.MetricWith("driftwatch_oracle_keys", map[string]string{"trust": "adopted"}))
			assert.Positive(t, report.KeysSkippedAdopted,
				"the sweep skipped them rather than comparing the store against itself")

			// An event arrives for one of them, and that key becomes something
			// driftwatch can genuinely vouch for.
			msg := s.Msg(scenario.Event{Seq: 1, Op: "set", Key: keyFor(1), Value: "v1"})
			s.Ingest(msg)
			s.Materialize(msg)

			s.RequireTrust(keyFor(1), oracle.TrustComplete,
				"an observed event replaces a borrowed belief with a derived one")
		})
}

func TestFault45_FullTargetEmptyOracleUnderWait(t *testing.T) {
	// Row 45: zero findings, coverage near zero, rising as events arrive.
	//
	// Wait is the conservative mode: believe nothing about what was there
	// before. The cost is that coverage starts at nothing and the tool is
	// useless until the stream has touched enough of the keyspace, which is a
	// trade worth making explicit rather than discovering.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		WithPolicy(func(p *check.PolicySpec) { p.Bootstrap = check.BootstrapWait }).
		WithSeededTarget(seedExisting(200)).
		Run(func(s *scenario.Session) {
			require.Zero(t, s.Oracle().Len(),
				"Wait starts from nothing: the store's contents are not evidence")

			s.Settle()
			report := s.Sweep()

			s.RequireNoFindings(report)
			assert.Zero(t, report.KeysCompared,
				"there is nothing to compare yet, which is not the same as agreement")

			// Coverage rises as events arrive for keys the store already had.
			for seq := uint64(1); seq <= 50; seq++ {
				msg := s.Msg(scenario.Event{
					Seq: seq, Op: "set", Key: keyFor(seq), Value: "pre-existing",
				})
				s.Ingest(msg)
			}

			s.Settle()
			after := s.Sweep()

			assert.Equal(t, 50, after.KeysCompared, "coverage grew with the stream")
			s.RequireNoFindings(after)
			assert.Positive(t, s.Metric("driftwatch_coverage_ratio"))
		})
}

func TestFault46_FullTargetEmptyOracleUnderStrict(t *testing.T) {
	// Row 46: zero findings until a snapshot cycle completes, and the phase
	// says so rather than the check looking healthy while asserting nothing.
	//
	// Strict is the mode for a system where being wrong is expensive: assert
	// nothing at all until a publisher has retransmitted its whole state, so
	// the first finding driftwatch ever makes is backed by a complete view.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		WithPolicy(func(p *check.PolicySpec) { p.Bootstrap = check.BootstrapStrict }).
		WithSeededTarget(seedExisting(50)).
		Run(func(s *scenario.Session) {
			require.True(t, s.Status().AwaitingSnapshot,
				"the status has to say the check is not asserting yet; zero findings "+
					"without that would read as a clean bill of health")

			// Events arrive and disagree with the store, and none of it is
			// reported, because driftwatch has not been told it has a complete
			// view.
			for seq := uint64(1); seq <= 20; seq++ {
				s.Ingest(s.Msg(scenario.Event{
					Seq: seq, Op: "set", Key: keyFor(seq), Value: "different",
				}))
			}

			report := s.SweepAndConfirm()

			assert.Zero(t, report.Alertable(),
				"nothing is asserted before a snapshot: %s", report.Summary())
			s.RequireNoConfirmedDrift()
			assert.Zero(t, s.Status().DivergentKeys)
			assert.True(t, s.Status().AwaitingSnapshot)

			// The publisher retransmits, and from that point driftwatch is
			// prepared to stand behind what it says.
			s.Ingest(s.Msg(scenario.Event{Seq: 21, Op: "snapshotBegin"}))
			for seq := uint64(22); seq <= 41; seq++ {
				s.Ingest(s.Msg(scenario.Event{
					Seq: seq, Op: "set", Key: keyFor(seq - 21), Value: "different",
				}))
			}
			s.Ingest(s.Msg(scenario.Event{Seq: 42, Op: "snapshotEnd"}))

			assert.Equal(t, uint64(1), s.Status().SnapshotsSeen)
			assert.False(t, s.Status().AwaitingSnapshot,
				"the check leaves the waiting state once it has a complete view")

			confirmed := s.SweepAndConfirm()
			assert.Positive(t, confirmed.Alertable(),
				"and now it reports: the store holds values no event produced")
		})
}

var _ = time.Second
