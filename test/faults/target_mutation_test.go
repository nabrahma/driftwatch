package faults

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/pkg/explain"
	"github.com/nabrahma/driftwatch/test/harness/scenario"
)

// §15.2 rows 33 to 35 and 41 to 42 — something else touched the store.
//
// These are the findings driftwatch exists to produce. Nothing in the event
// stream explains them, no error was returned to anybody, and no other monitor
// has any way to notice: the store simply holds something the events never
// asked for, or has stopped holding something they did.
//
// Rows 41 and 42 are the counterweight. A live keyspace changes underneath a
// non-atomic scan constantly, and reporting those changes as drift would bury
// the real findings above in noise.

func TestFault33_OutOfBandWrite(t *testing.T) {
	// Row 33: confirmed valueMismatch or extraInTarget, and explain says the
	// key was written out of band rather than blaming the materializer.
	t.Run("a value overwritten behind driftwatch", func(t *testing.T) {
		scenario.New(t).
			WithSettlementWindow(time.Second).
			Run(func(s *scenario.Session) {
				msg := s.Msg(scenario.Event{Seq: 1, Op: "set", Key: "block:a", Value: "v1"})
				s.Ingest(msg)
				s.Materialize(msg)

				s.Settle()
				s.RequireNoFindings(s.Sweep())

				// A third party writes a value no event ever produced.
				s.WriteOutOfBand(map[string][]byte{"block:a": []byte("something-else")})

				report := s.SweepAndConfirm()

				require.Equal(t, 1, report.Total(), "%s", report.Summary())
				assert.Equal(t, 1, report.ByCategory[differ.CatValueMismatch])
				assert.Len(t, s.Confirmed(), 1)
			})
	})

	t.Run("a key created behind driftwatch", func(t *testing.T) {
		scenario.New(t).
			WithSettlementWindow(time.Second).
			Run(func(s *scenario.Session) {
				s.WriteOutOfBand(map[string][]byte{"block:orphan": []byte("nobody wrote me")})

				// Extras need both passes: one is a race with an event still in
				// flight, two a window apart is a key nothing is coming for.
				report := s.ScanExtrasTwice()

				require.Equal(t, 1, report.Total(), "%s", report.Summary())
				assert.Equal(t, 1, report.ByCategory[differ.CatExtraInTarget])

				s.Explain("block:orphan").
					RequireDiagnosis(explain.CodeExtraInTarget).
					RequireText("no event ever created it")
			})
	})
}

func TestFault34_OutOfBandDelete(t *testing.T) {
	// Row 34: confirmed missingInTarget with no sequence gap, so explain can
	// name the materializer rather than hedging — which is the difference
	// between a finding an operator acts on and one they investigate.
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

			s.DeleteOutOfBand(keyFor(7))

			report := s.SweepAndConfirm()

			require.Equal(t, 1, report.Total(), "%s", report.Summary())
			assert.Equal(t, 1, report.ByCategory[differ.CatMissingInTarget])
			assert.Zero(t, s.Metric("driftwatch_seq_gaps_total"),
				"driftwatch missed nothing, which is what lets it be specific")

			s.Explain(keyFor(7)).
				RequireDiagnosis(explain.CodeMissingInTargetNoGaps).
				RequireNoDiagnosis(explain.CodeSeqGapAffectingPublisher)
		})
}

func TestFault35_WrongTypeIsDriftRatherThanAnError(t *testing.T) {
	// Row 35: CatTypeMismatch and a TYPE_MISMATCH diagnosis, with no error
	// surfaced to the operator. A key holding a string where a set belongs is
	// something that happened to the data, not a bug in the reader, and
	// treating it as a failed read would hide it.
	scenario.New(t).
		WithProjection("keysetOwnership").
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			msg := s.Msg(scenario.Event{
				Seq: 1, Op: "add", Key: "block:a", Member: "replica-0",
			})
			s.Ingest(msg)
			s.Materialize(msg)

			s.Settle()
			s.RequireNoFindings(s.Sweep())

			// Something replaces the set with a plain string.
			s.DeleteOutOfBand("block:a")
			s.WriteOutOfBand(map[string][]byte{"block:a": []byte("replica-0")})

			// The sweep returns a report rather than an error: that is the row.
			report, err := s.TrySweep()
			require.NoError(t, err,
				"a wrong-typed key is drift; failing the sweep would suppress "+
					"every other finding in it")
			require.Equal(t, 1, report.Total())
			assert.Equal(t, 1, report.ByCategory[differ.CatTypeMismatch])

			s.Explain("block:a").
				RequireDiagnosis(explain.CodeTypeMismatch).
				RequireText("Target holds type string")
		})
}

func TestFault41_KeyAddedMidScanIsNotAnExtra(t *testing.T) {
	// Row 41: a key that appears while the scan is running must not be
	// reported. The two-pass rule is what makes this hold, and the row asks for
	// it to be asserted explicitly because a single-pass scan over a live
	// keyspace reports every in-flight write as drift.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			fillBothSides(s, 50)

			// The first pass collects candidates. A key appearing during it, or
			// between the passes, is exactly the race the second pass exists to
			// filter out.
			added := false
			s.Target().ObserveCommands(func(name string) {
				if name == "SCAN" && !added {
					added = true
					s.WriteOutOfBand(map[string][]byte{"block:appeared": []byte("mid-scan")})
				}
			})

			first := s.ScanExtras()
			s.RequireNoFindings(first)

			// And the event that explains it arrives before the second pass, so
			// by the time driftwatch looks again the key is accounted for.
			s.Ingest(s.Msg(scenario.Event{
				Seq: 51, Op: "set", Key: "block:appeared", Value: "mid-scan",
			}))

			s.Settle()
			second := s.ScanExtras()

			s.RequireNoFindings(second)
			s.RequireNoConfirmedDrift()
		})
}

func TestFault42_KeyRemovedMidScanIsNotAnExtra(t *testing.T) {
	// Row 42: the mirror image. A key present in the first pass and gone by the
	// second was never an extra either — it was a key on its way out, and
	// reporting it would be reporting a race with the store's own lifecycle.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			s.WriteOutOfBand(map[string][]byte{
				"block:transient": []byte("here for now"),
				"block:staying":   []byte("here to stay"),
			})

			first := s.ScanExtras()
			s.RequireNoFindings(first)

			// One of the two disappears before the second pass.
			s.DeleteOutOfBand("block:transient")
			s.Settle()

			second := s.ScanExtras()

			require.Equal(t, 1, second.Total(),
				"only the key that survived both passes is an extra: %s", second.Summary())
			require.Len(t, second.Findings, 1)
			assert.Equal(t, "block:staying", second.Findings[0].Key,
				"the key that vanished between the passes was never reported")
		})
}
