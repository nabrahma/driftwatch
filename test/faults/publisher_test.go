package faults

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/metrics"
	"github.com/nabrahma/driftwatch/test/harness/scenario"
)

// §15.1 rows 25 to 27 — the publisher population itself.
//
// This file is not in the §7 tree, which lists fourteen files for test/faults
// and predates the matrix having sixty rows. These three are about publishers
// rather than about a fault applied to a stream, and filing them under
// restart_test.go or clockskew_test.go would put them somewhere nobody would
// look for them. Recorded in docs/DECISIONS.md ADR-0010.

func TestFault25_TwoPublishersWritingTheSameKey(t *testing.T) {
	// Row 25: driftwatch must not report drift when the target reflects either
	// valid interleaving, and must declare MultiWriterUnsafe when a scalar
	// projection sees the same key from two publishers.
	//
	// The row is really about the limits of what sequence numbers can tell you.
	// They order events within one publisher's stream and say nothing at all
	// across streams, so with two writers on one key there is no fact about
	// which write was second — only two answers that are equally defensible.
	t.Run("a set projection is well defined and stays quiet", func(t *testing.T) {
		// Adds and removes of distinct members commute, so every interleaving
		// reaches the same set and there is nothing ambiguous to report.
		scenario.New(t).
			WithProjection("keysetOwnership").
			WithSettlementWindow(time.Second).
			Run(func(s *scenario.Session) {
				fromA := s.Msg(scenario.Event{
					Publisher: "replica-0", Seq: 1, Op: "add",
					Key: "block:shared", Member: "replica-0",
				})
				fromB := s.Msg(scenario.Event{
					Publisher: "replica-1", Seq: 1, Op: "add",
					Key: "block:shared", Member: "replica-1",
				})

				// driftwatch sees one order and the store applies the other.
				s.Ingest(fromA)
				s.Ingest(fromB)
				s.Materialize(fromB)
				s.Materialize(fromA)

				report := s.SweepAndConfirm()

				s.RequireNoFindings(report)
				s.RequireNoConfirmedDrift()
				assert.False(t, s.Status().MultiWriterUnsafe,
					"a commutative fold has no ambiguity to declare")
			})
	})

	t.Run("a scalar projection declares itself unsafe rather than guessing", func(t *testing.T) {
		scenario.New(t).
			WithSettlementWindow(time.Second).
			Run(func(s *scenario.Session) {
				s.Ingest(s.Msg(scenario.Event{
					Publisher: "replica-0", Seq: 1, Op: "set", Key: "block:shared", Value: "a",
				}))
				s.Ingest(s.Msg(scenario.Event{
					Publisher: "replica-1", Seq: 1, Op: "set", Key: "block:shared", Value: "b",
				}))

				status := s.Status()
				assert.True(t, status.MultiWriterUnsafe,
					"two publishers, one key, an order-dependent fold: driftwatch's "+
						"expectation is one arbitrary choice among several")
				assert.Equal(t, "block:shared", status.MultiWriterKey,
					"and it says which key showed it, so an operator can go and look")

				// The store settles on the other writer's value. driftwatch
				// holds a different one and would happily call that drift,
				// which is why the condition exists: the finding is real but
				// the conclusion an operator would draw from it is not.
				s.WriteOutOfBand(map[string][]byte{"block:shared": []byte("a")})
				s.SweepAndConfirm()

				assert.True(t, s.Status().MultiWriterUnsafe,
					"the declaration is sticky; a keyspace does not stop being "+
						"ambiguous because one sweep happened to agree")
			})
	})
}

func TestFault26_HeartbeatOnlyStream(t *testing.T) {
	// Row 26: sequence advances, no keys are created, no findings, oracle_keys
	// is 0, and coverage_ratio does not divide by zero.
	//
	// A heartbeat-only stream is what an idle system looks like, and dividing
	// by zero on the quietest possible input would be a poor way to find out
	// the tool cannot handle it.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			for seq := uint64(1); seq <= 200; seq++ {
				s.Ingest(s.Msg(scenario.Event{Seq: seq, Op: "heartbeat"}))
			}

			report := s.SweepAndConfirm()

			s.RequireNoFindings(report)
			assert.Zero(t, s.Oracle().Len(), "a heartbeat touches no key")
			assert.Zero(t, s.Metric("driftwatch_oracle_keys"))
			assert.Zero(t, s.Metric("driftwatch_events_dropped_total"),
				"heartbeats are accepted, not dropped: they advance the sequence")

			status := s.Status()
			require.Len(t, status.Publishers, 1)
			assert.Equal(t, uint64(200), status.Publishers[0].HighWaterMark,
				"the sequence advanced, which is what a heartbeat is for")

			coverage := s.Metric("driftwatch_coverage_ratio")
			assert.False(t, isNaNOrInf(coverage),
				"coverage_ratio was %v with an empty oracle", coverage)
			assert.Equal(t, 1.0, coverage,
				"nothing to compare is complete coverage, not zero coverage")
		})
}

func TestFault27_FifteenHundredPublishers(t *testing.T) {
	// Row 27: the oldest publishers are evicted, memory stays bounded, and the
	// metric labels collapse into __other__ past the configured limit.
	//
	// Two independent bounds, and both matter. seqtrack's bound stops one
	// misconfigured fleet from growing driftwatch's memory without limit; the
	// metric bound stops the same fleet from doing it to Prometheus, which is
	// the failure that takes other people's alerting down with it.
	const (
		publishers    = 1500
		maxPublishers = 512
		maxLabels     = 20
	)

	scenario.New(t).
		WithSettlementWindow(time.Second).
		WithMaxPublisherLabels(maxLabels).
		WithPolicy(func(p *check.PolicySpec) { p.MaxPublishers = maxPublishers }).
		Run(func(s *scenario.Session) {
			for i := 0; i < publishers; i++ {
				s.Ingest(s.Msg(scenario.Event{
					Publisher: "replica-" + itoa(uint64(i)),
					Seq:       1,
					Op:        "set",
					Key:       keyFor(uint64(i)),
					Value:     "v1",
				}))
			}

			s.Settle()
			s.Sweep()

			tracked := s.Status().Publishers
			assert.LessOrEqual(t, len(tracked), maxPublishers,
				"seqtrack evicts rather than growing: %d publishers tracked", len(tracked))
			assert.Equal(t, float64(len(tracked)), s.Metric("driftwatch_publishers_tracked"),
				"and reports the true count, which is what an operator needs when "+
					"the labels have collapsed")

			// The label bound is separate and tighter.
			labels := s.MetricLabelValues("driftwatch_events_received_total", "publisher")
			assert.LessOrEqual(t, len(labels), maxLabels+1,
				"%d distinct publisher label values were exported", len(labels))
			assert.Contains(t, labels, metrics.OtherPublisher,
				"everything past the limit is aggregated rather than dropped")
			assert.Positive(t, labels[metrics.OtherPublisher],
				"and the aggregate really carries the events, so no count is lost")

			// Every event was still applied: bounding the bookkeeping must not
			// bound the audit.
			assert.Equal(t, uint64(publishers), s.Status().EventsApplied)
			assert.Equal(t, publishers, s.Oracle().Len())

			t.Logf("%d publishers: %d tracked by seqtrack, %d metric label values, "+
				"%d keys in the oracle",
				publishers, len(tracked), len(labels), s.Oracle().Len())
		})
}

// isNaNOrInf reports a float that would render as NaN or +Inf in an exposition,
// which is what a division by zero looks like on a dashboard.
func isNaNOrInf(f float64) bool { return f != f || f > 1e308 || f < -1e308 }
