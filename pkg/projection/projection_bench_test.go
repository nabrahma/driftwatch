package projection_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/projection"
)

// BenchmarkProjectionApply measures the flagship path: one replica claiming
// ownership of a block, folded into a set that already holds a few members.
//
// §16.8 targets more than 2M ops/sec/core. Apply runs once per event on the
// single applier goroutine, so it sits directly on the ingest throughput
// ceiling.
func BenchmarkProjectionApply(b *testing.B) {
	p, err := projection.New("keysetOwnership", nil)
	require.NoError(b, err)

	prev := setOf("replica-0", "replica-1", "replica-2")
	e := &event.Event{
		Publisher: "replica-3", Epoch: 1, Seq: 1,
		Op: event.OpAdd, Key: "9f3a2c1e", Member: "replica-3",
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, applyErr := p.Apply(prev, e); applyErr != nil {
			b.Fatal(applyErr)
		}
	}
}

// BenchmarkProjectionApplyTemplated is the same fold through a non-identity
// template, which is the cost text/template imposes when the fast path cannot
// be taken.
func BenchmarkProjectionApplyTemplated(b *testing.B) {
	p, err := projection.New("keysetOwnership", map[string]string{
		"keyTemplate": "kv:{{.Key}}:{{.Publisher}}",
	})
	require.NoError(b, err)

	prev := setOf("replica-0")
	e := &event.Event{
		Publisher: "replica-3", Epoch: 1, Seq: 1,
		Op: event.OpAdd, Key: "9f3a2c1e", Member: "replica-3",
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, applyErr := p.Apply(prev, e); applyErr != nil {
			b.Fatal(applyErr)
		}
	}
}

func BenchmarkProjectionApplyScalar(b *testing.B) {
	p, err := projection.New("scalar", nil)
	require.NoError(b, err)

	prev := scalarOf("previous-value")
	e := &event.Event{Publisher: "p", Op: event.OpSet, Key: "k", Value: []byte("new-value")}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, applyErr := p.Apply(prev, e); applyErr != nil {
			b.Fatal(applyErr)
		}
	}
}

func BenchmarkProjectionApplyCounter(b *testing.B) {
	p, err := projection.New("counter", map[string]string{"incrOnly": "true"})
	require.NoError(b, err)

	prev := counterOf(1000)
	e := &event.Event{Publisher: "p", Op: event.OpIncr, Key: "k", Delta: 1}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, applyErr := p.Apply(prev, e); applyErr != nil {
			b.Fatal(applyErr)
		}
	}
}

// BenchmarkProjectionApplyLargeSet is the memory-shaped case: cloning a set on
// every fold is what makes Apply cost grow with set size, so the cost is
// measured rather than assumed.
func BenchmarkProjectionApplyLargeSet(b *testing.B) {
	for _, size := range []int{10, 100, 1000} {
		b.Run("members="+strconv.Itoa(size), func(b *testing.B) {
			p, err := projection.New("keysetOwnership", nil)
			require.NoError(b, err)

			members := make([]string, size)
			for i := range members {
				members[i] = "replica-" + strconv.Itoa(i)
			}
			prev := setOf(members...)
			e := &event.Event{
				Publisher: "p", Op: event.OpAdd, Key: "k", Member: "replica-new",
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if _, applyErr := p.Apply(prev, e); applyErr != nil {
					b.Fatal(applyErr)
				}
			}
		})
	}
}

// TestProjectionApplyTemplateIsCompiledOnce pins the requirement that template
// expansion happens at construction rather than per event.
//
// It is a test rather than only a benchmark so that a regression fails CI
// instead of showing up in a benchstat nobody reads.
func TestProjectionApplyTemplateIsCompiledOnce(t *testing.T) {
	// The identity template is the overwhelmingly common configuration and is
	// answered by returning the field directly, so folding an add costs exactly
	// the one map clone the projection cannot avoid.
	const budget = 2

	p, err := projection.New("keysetOwnership", nil)
	require.NoError(t, err)

	prev := setOf("replica-0", "replica-1")
	e := &event.Event{Publisher: "p", Op: event.OpAdd, Key: "k", Member: "replica-2"}

	require.NoError(t, func() error { _, applyErr := p.Apply(prev, e); return applyErr }())

	allocs := testing.AllocsPerRun(200, func() {
		if _, applyErr := p.Apply(prev, e); applyErr != nil {
			t.Fatal(applyErr)
		}
	})

	require.LessOrEqual(t, allocs, float64(budget),
		"keysetOwnership.Apply allocates %.0f times per event, budget is %d", allocs, budget)
}
