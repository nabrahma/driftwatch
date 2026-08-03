package seqtrack_test

import (
	"testing"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/seqtrack"
)

// BenchmarkSeqTrackObserve measures the steady state: in-order events from a
// small set of publishers, which is what a healthy system produces almost all
// of the time.
//
// §16.8 targets more than 5M ops/sec/core with zero allocations. Zero
// allocations matters more than the throughput number: Observe runs once per
// event on the single applier goroutine, so any allocation here becomes garbage
// collector pressure proportional to the entire event rate.
func BenchmarkSeqTrackObserve(b *testing.B) {
	tr := seqtrack.New(seqtrack.Config{Clock: clock.Fake(epoch)})
	e := event.Event{Publisher: "replica-2", Epoch: 1, Op: event.OpHeartbeat}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		e.Seq = uint64(i) //nolint:gosec // loop counter, never negative
		tr.Observe(&e)
	}
}

// BenchmarkSeqTrackObserveManyPublishers adds the map lookup cost of a realistic
// fleet, which is the difference between a microbenchmark and the real thing.
func BenchmarkSeqTrackObserveManyPublishers(b *testing.B) {
	const publishers = 64

	tr := seqtrack.New(seqtrack.Config{Clock: clock.Fake(epoch)})
	ids := make([]string, publishers)
	seqs := make([]uint64, publishers)
	for i := range ids {
		ids[i] = "replica-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
	}

	e := event.Event{Epoch: 1, Op: event.OpHeartbeat}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p := i % publishers
		seqs[p]++
		e.Publisher = ids[p]
		e.Seq = seqs[p]
		tr.Observe(&e)
	}
}

// BenchmarkSeqTrackObserveWithGaps measures the unhealthy path, where every
// event reveals a hole. A detector whose cost explodes under exactly the
// condition it exists to detect is not much use.
func BenchmarkSeqTrackObserveWithGaps(b *testing.B) {
	tr := seqtrack.New(seqtrack.Config{Clock: clock.Fake(epoch), MaxGapIntervals: 1024})
	e := event.Event{Publisher: "replica-2", Epoch: 1, Op: event.OpHeartbeat}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		e.Seq = uint64(i) * 2 //nolint:gosec // loop counter, never negative
		tr.Observe(&e)
	}
}

func BenchmarkGapSetAdd(b *testing.B) {
	g := seqtrack.NewGapSet(1024)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		seq := uint64(i) * 2 //nolint:gosec // loop counter, never negative
		g.Add(seq, seq)
	}
}

func BenchmarkGapSetContains(b *testing.B) {
	g := seqtrack.NewGapSet(1024)
	for i := uint64(0); i < 1024; i++ {
		g.Add(i*4, i*4+1)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		g.Contains(uint64(i) % 4096) //nolint:gosec // loop counter, never negative
	}
}
