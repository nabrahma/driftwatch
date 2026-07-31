package faults

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/source"
	"github.com/nabrahma/driftwatch/test/harness/scenario"
)

// §15.1 rows 6 to 8 — reordering.
//
// The pair of rows 6 and 7 is the one most likely to be got wrong, and getting
// it wrong in either direction is bad. Reordering the materializer's stream
// changes what the store ends up holding, so it is real drift. Reordering
// driftwatch's stream changes nothing about the final state, because the fold
// arrives at the same place — so reporting drift there would be a false
// positive manufactured entirely by driftwatch.

func TestFault06_AdjacentPairReorderedOnTheMaterializer(t *testing.T) {
	// Row 6: add then remove, applied as remove then add, leaves the member
	// present when it should be absent. This is the case that proves ordering
	// matters — for the store, which applies events one at a time.
	scenario.New(t).
		WithProjection("keysetOwnership").
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			add := s.Msg(scenario.Event{Seq: 1, Op: "add", Key: "block:a", Member: "replica-0"})
			remove := s.Msg(scenario.Event{Seq: 2, Op: "remove", Key: "block:a", Member: "replica-0"})

			// driftwatch sees add then remove and expects the key to be gone.
			s.Ingest(add)
			s.Ingest(remove)

			// The materializer applies them the other way round, so the member
			// it removed is put back by the add that arrived late.
			s.Materialize(remove)
			s.Materialize(add)

			report := s.SweepAndConfirm()

			require.Len(t, s.Confirmed(), 1,
				"a member present when it should be absent is real drift: %s",
				report.Summary())
			assert.Positive(t, report.Alertable())

			// The oracle removed the last member, so it expects no key at all
			// while the store still holds one. Either categorization describes
			// the same observation, and both are drift.
			assert.Contains(t,
				[]differ.Category{differ.CatMemberMismatch, differ.CatExtraInTarget},
				s.Confirmed()["block:a"].Category)
		})
}

func TestFault07_AdjacentPairReorderedOnDriftwatch(t *testing.T) {
	// Row 7: zero findings. Reordering loses ordering, not information.
	//
	// The row asks for this to be asserted explicitly because it is a classic
	// false-positive source: the oracle really is transiently wrong between the
	// two events, and a detector that compared at that instant would report
	// drift that does not exist. The settlement window is what makes the
	// transient invisible, and this is the test that proves it.
	scenario.New(t).
		WithProjection("keysetOwnership").
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			// A stream that is already flowing before the pair is reordered.
			// The first event a publisher sends establishes the baseline, so a
			// pair swapped at the very start of a stream is indistinguishable
			// from a stream that simply began at seq 2 — driftwatch has nothing
			// to detect the swap against, and cannot.
			seed := s.Msg(scenario.Event{Seq: 1, Op: "add", Key: "block:seed", Member: "replica-0"})
			add := s.Msg(scenario.Event{Seq: 2, Op: "add", Key: "block:a", Member: "replica-0"})
			remove := s.Msg(scenario.Event{Seq: 3, Op: "remove", Key: "block:a", Member: "replica-0"})

			// The materializer applies them in order and ends up correct.
			s.Materialize(seed)
			s.Materialize(add)
			s.Materialize(remove)

			// driftwatch receives the pair swapped. Applied in arrival order
			// the fold would end up holding a member the store does not have,
			// which is the false positive this row exists to forbid.
			s.Ingest(seed)
			s.Ingest(remove)
			s.Ingest(add)

			report := s.SweepAndConfirm()

			s.RequireNoFindings(report)
			s.RequireNoConfirmedDrift()
			assert.Zero(t, s.Status().SuspectDivergentKeys,
				"nothing was lost, so nothing should even be suspect")
			assert.Zero(t, s.Metric("driftwatch_seq_gaps_total"),
				"both events arrived; only their order changed")

			// And the state really did converge, rather than the sweep having
			// compared nothing at all.
			entry, ok := s.Oracle().Get("block:a")
			require.True(t, ok)
			assert.True(t, entry.Value.IsAbsent(),
				"the fold reaches the same place whichever order it saw")
		})
}

func TestFault08_WindowShuffleOverTenThousandEvents(t *testing.T) {
	// Row 8: zero confirmed findings after settlement over a window-8 shuffle
	// of ten thousand events, and the oracle's final state equals the reference
	// implementation's.
	//
	// The reference comparison is what makes this more than "no findings". A
	// pipeline that dropped every shuffled event would also report nothing;
	// folding to the same state as an independent naive implementation is what
	// says the events were applied rather than discarded.
	scenario.New(t).
		WithProjection("keysetOwnership").
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			const (
				events = 10_000
				keys   = 400
				window = 8
			)

			ordered := make([]event.Event, 0, events)
			msgs := make([]source.RawMessage, 0, events)

			for seq := uint64(1); seq <= events; seq++ {
				op, member := "add", "replica-"+itoa(seq%4)
				if seq%5 == 0 {
					op = "remove"
				}
				key := keyFor(seq % keys)

				ordered = append(ordered, event.Event{
					Publisher: "replica-0", Epoch: 1, Seq: seq,
					Op: opFor(op), Key: key, Member: member,
				})
				msgs = append(msgs, s.Msg(scenario.Event{
					Seq: seq, Op: op, Key: key, Member: member,
				}))
			}

			// Shuffled within a sliding window of eight, deterministically: a
			// fixed rotation rather than a random permutation, so a failure is
			// reproducible from the test alone.
			shuffled := make([]source.RawMessage, len(msgs))
			copy(shuffled, msgs)
			for i := 0; i+window <= len(shuffled); i += window {
				block := shuffled[i : i+window]
				block[0], block[window-1] = block[window-1], block[0]
				block[1], block[window-2] = block[window-2], block[1]
			}

			for _, msg := range msgs {
				s.Materialize(msg)
			}
			for _, msg := range shuffled {
				s.Ingest(msg)
			}

			report := s.SweepAndConfirm()

			s.RequireNoFindings(report)
			s.RequireNoConfirmedDrift()

			// The independent fold, from pkg/projection's reference
			// implementation, over the events in their true order.
			want := projection.NewReference(projection.ShapeSet).Fold(ordered)

			for key, value := range want {
				entry, ok := s.Oracle().Get(key)
				require.True(t, ok, "the oracle lost key %q entirely", key)
				assert.True(t, entry.Value.Equal(value),
					"key %q: oracle has %s, the reference fold says %s",
					key, entry.Value, value)
			}

			t.Logf("%d events shuffled within a window of %d: %d keys compared, "+
				"%d findings, oracle matches the reference fold on all %d keys",
				events, window, report.KeysCompared, report.Total(), len(want))
		})
}

// opFor maps a wire op name onto the event constant, for the rows that build
// both a raw message and a reference event from one description.
func opFor(name string) event.Op {
	op, err := event.ParseOp(name)
	if err != nil {
		panic("faults: unknown op " + name)
	}
	return op
}
