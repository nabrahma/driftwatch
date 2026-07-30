package seqtrack_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/seqtrack"
	"github.com/nabrahma/driftwatch/pkg/testgen"
)

// TestProp_WithheldSeqsAlwaysDetected is invariant I4: if any event is
// withheld, the GapSet contains its sequence number.
//
// This is the property the whole trust model rests on. If a lost event can slip
// past undetected, driftwatch will confidently report that a target is wrong
// when the truth is that driftwatch never saw the event that would have made
// them agree.
func TestProp_WithheldSeqsAlwaysDetected(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		publishers := rapid.IntRange(1, 3).Draw(t, "publishers")
		count := rapid.IntRange(1, 60).Draw(t, "count")

		all := testgen.EventStream(t, publishers, count)
		kept, withheld := testgen.WithdrawSubset(t, all)

		// Delivery order is arbitrary: a lossy broadcast reorders as well as
		// drops, and gap detection must survive both at once.
		delivered := testgen.Permutation(t, kept)

		tr := seqtrack.New(seqtrack.Config{
			Clock: clock.Fake(epoch),
			// Large enough that truncation cannot blur the answer; the bound
			// itself is I9's subject, tested separately.
			MaxGapIntervals: 4096,
		})
		for i := range delivered {
			tr.Observe(&delivered[i])
		}

		// A withheld event is only detectable once something after it arrives.
		// The tracker adopts each publisher's first observed sequence as its
		// baseline, so anything withheld at or below that baseline was never
		// observable and must not be claimed as a gap.
		detectable := detectableWithheld(delivered, withheld)

		gaps := gapsByPublisher(tr)
		for _, e := range detectable {
			g, ok := gaps[e.Publisher]
			require.True(t, ok, "publisher %s has no gap set", e.Publisher)
			assert.True(t, g.Contains(e.Seq),
				"seq %d from %s was withheld but not detected as missing", e.Seq, e.Publisher)
		}

		// The count is exact, not merely a superset: over-reporting would mark
		// keys suspect for events that were never lost and suppress real
		// findings.
		var totalDetectable int
		perPublisher := map[string]uint64{}
		for _, e := range detectable {
			perPublisher[e.Publisher]++
			totalDetectable++
		}
		for pub, want := range perPublisher {
			assert.Equal(t, want, gaps[pub].Count(),
				"publisher %s: gap count disagrees with the number of withheld events", pub)
		}
	})
}

// detectableWithheld returns the withheld events the tracker could possibly
// have noticed.
//
// A loss is only observable from the inside when something on both sides of it
// arrived. Two categories are therefore invisible by construction, and claiming
// them as gaps would be driftwatch inventing evidence:
//
//   - below the baseline: the first sequence actually delivered is adopted as
//     the starting point, so nothing beneath it was ever expected;
//   - above the high-water mark: a publisher that goes silent after a drop is
//     indistinguishable from one that had nothing more to say.
func detectableWithheld(delivered, withheld []event.Event) []event.Event {
	baseline := map[string]uint64{}
	highest := map[string]uint64{}
	for i := range delivered {
		e := &delivered[i]
		if _, seen := baseline[e.Publisher]; !seen {
			baseline[e.Publisher] = e.Seq
		}
		if e.Seq > highest[e.Publisher] {
			highest[e.Publisher] = e.Seq
		}
	}

	out := make([]event.Event, 0, len(withheld))
	for i := range withheld {
		e := withheld[i]
		base, seen := baseline[e.Publisher]
		if !seen {
			// Every event from this publisher was withheld, so the tracker
			// never heard of it at all.
			continue
		}
		if e.Seq > base && e.Seq < highest[e.Publisher] {
			out = append(out, e)
		}
	}
	return out
}

func gapsByPublisher(tr *seqtrack.Tracker) map[string]*seqtrack.GapSet {
	out := map[string]*seqtrack.GapSet{}
	for _, st := range tr.Publishers() {
		out[st.ID] = st.Gaps
	}
	return out
}

// TestProp_DuplicatesNeverAcceptedTwice is invariants I1 and I5: applying an
// event twice has the same effect as applying it once, and no event is ever
// double counted.
func TestProp_DuplicatesNeverAcceptedTwice(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		evs := testgen.EventStream(t, rapid.IntRange(1, 3).Draw(t, "publishers"),
			rapid.IntRange(1, 40).Draw(t, "count"))

		tr := seqtrack.New(seqtrack.Config{Clock: clock.Fake(epoch), MaxGapIntervals: 4096})

		accepted := map[event.Fingerprint]int{}
		// Deliver the stream, then deliver a permutation of the whole thing
		// again. A lossy broadcast redelivers as readily as it drops.
		for _, pass := range [][]event.Event{evs, testgen.Permutation(t, evs)} {
			for i := range pass {
				e := &pass[i]
				if verdict, _ := tr.Observe(e); verdict.Accepted() {
					accepted[e.Fingerprint()]++
				}
			}
		}

		for fp, times := range accepted {
			assert.Equal(t, 1, times, "%s was accepted %d times", fp, times)
		}

		// Every event was delivered at least once, so the tracker's own count
		// must match the number of distinct events rather than the number of
		// deliveries.
		distinct := map[event.Fingerprint]struct{}{}
		for i := range evs {
			distinct[evs[i].Fingerprint()] = struct{}{}
		}
		var counted uint64
		for _, st := range tr.Publishers() {
			counted += st.EventCount
		}
		assert.Equal(t, uint64(len(distinct)), counted)
	})
}

// TestProp_GapSetIntervalsBounded is invariant I9: the interval count never
// exceeds the cap, whatever the input.
//
// Unbounded gap tracking is a memory-exhaustion vector under a flapping
// publisher, which is exactly when driftwatch most needs to stay running.
func TestProp_GapSetIntervalsBounded(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		maxIntervals := rapid.IntRange(1, 8).Draw(t, "maxIntervals")
		g := seqtrack.NewGapSet(maxIntervals)

		ops := rapid.IntRange(0, 60).Draw(t, "ops")
		for i := 0; i < ops; i++ {
			if rapid.Bool().Draw(t, "isAdd") {
				from := rapid.Uint64Range(0, 1000).Draw(t, "from")
				width := rapid.Uint64Range(0, 20).Draw(t, "width")
				g.Add(from, from+width)
			} else {
				g.Fill(rapid.Uint64Range(0, 1000).Draw(t, "fill"))
			}

			require.LessOrEqual(t, len(g.Intervals()), maxIntervals,
				"interval count exceeded the cap after operation %d", i)
		}
	})
}

// TestProp_GapSetIntervalsStayNormalized asserts the representation invariant:
// intervals are sorted, non-empty, non-overlapping and non-adjacent.
//
// Every other GapSet property depends on this holding, and a violation would
// make Contains and Count quietly wrong rather than loudly broken.
func TestProp_GapSetIntervalsStayNormalized(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		g := seqtrack.NewGapSet(4096)

		ops := rapid.IntRange(0, 80).Draw(t, "ops")
		for i := 0; i < ops; i++ {
			if rapid.Bool().Draw(t, "isAdd") {
				from := rapid.Uint64Range(0, 500).Draw(t, "from")
				width := rapid.Uint64Range(0, 10).Draw(t, "width")
				g.Add(from, from+width)
			} else {
				g.Fill(rapid.Uint64Range(0, 500).Draw(t, "fill"))
			}

			intervals := g.Intervals()
			for j, in := range intervals {
				require.LessOrEqual(t, in.From, in.To,
					"interval %d is inverted after operation %d", j, i)
				if j > 0 {
					prev := intervals[j-1]
					require.Greater(t, in.From, prev.To,
						"intervals %d and %d overlap after operation %d", j-1, j, i)
					require.Greater(t, in.From-prev.To, uint64(1),
						"intervals %d and %d are adjacent and should have coalesced", j-1, j)
				}
			}
		}
	})
}

// TestProp_GapSetContainsAgreesWithABruteForceSet checks the optimized interval
// set against the obviously-correct implementation over the same operations.
func TestProp_GapSetContainsAgreesWithABruteForceSet(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		const universe = 200

		g := seqtrack.NewGapSet(4096)
		want := map[uint64]bool{}

		ops := rapid.IntRange(0, 60).Draw(t, "ops")
		for i := 0; i < ops; i++ {
			if rapid.Bool().Draw(t, "isAdd") {
				from := rapid.Uint64Range(0, universe).Draw(t, "from")
				to := from + rapid.Uint64Range(0, 15).Draw(t, "width")
				g.Add(from, to)
				for s := from; s <= to; s++ {
					want[s] = true
				}
			} else {
				seq := rapid.Uint64Range(0, universe).Draw(t, "fill")
				g.Fill(seq)
				delete(want, seq)
			}
		}

		var wantCount uint64
		for s := uint64(0); s <= universe+20; s++ {
			assert.Equal(t, want[s], g.Contains(s), "disagreement at seq %d", s)
			if want[s] {
				wantCount++
			}
		}
		assert.Equal(t, wantCount, g.Count())
	})
}

// TestProp_ObserveNeverPanics fuzzes the classifier with arbitrary identity
// fields. Observe runs on the ingest hot path against publisher-controlled
// input, and a panic there takes down the auditor at the moment the system it
// is auditing is misbehaving.
func TestProp_ObserveNeverPanics(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		tr := seqtrack.New(seqtrack.Config{
			Clock:                  clock.Fake(epoch),
			MaxPublishers:          rapid.IntRange(1, 4).Draw(t, "maxPublishers"),
			MaxGapIntervals:        rapid.IntRange(1, 8).Draw(t, "maxGapIntervals"),
			ImplicitRestartDelta:   rapid.Uint64Range(1, 100).Draw(t, "delta"),
			ImplicitRestartCeiling: rapid.Uint64Range(1, 100).Draw(t, "ceiling"),
		})

		events := rapid.IntRange(0, 50).Draw(t, "events")
		for i := 0; i < events; i++ {
			e := event.Event{
				Publisher: rapid.SampledFrom([]string{"a", "b", "c", "d", "e", ""}).Draw(t, "pub"),
				Epoch:     rapid.SampledFrom([]uint64{0, 1, 2, maxUint64}).Draw(t, "epoch"),
				Seq: rapid.SampledFrom([]uint64{
					0, 1, 2, 99, 100, 1000, 1 << 62, maxUint64 - 1, maxUint64,
				}).Draw(t, "seq"),
				Op: testgen.Op(t),
			}

			verdict, gap := tr.Observe(&e)
			if gap != nil {
				require.LessOrEqual(t, gap.From, gap.To, "Observe produced an inverted gap")
				require.True(t, verdict.Accepted(), "a gap was reported for a rejected event")
			}
			tr.Trust(e.Publisher)
		}
		tr.Publishers()
	})
}

const maxUint64 = ^uint64(0)
