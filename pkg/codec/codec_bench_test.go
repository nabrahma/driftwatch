package codec_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/codec"
	"github.com/nabrahma/driftwatch/pkg/event"
)

// canonicalAdd is the shape of a real KV-cache ownership event: the case that
// runs millions of times a day.
var canonicalAdd = []byte(
	`{"publisher":"replica-2","epoch":1,"seq":8847,"ts":"2026-07-30T11:02:31.412Z",` +
		`"op":"add","key":"9f3a2c1e","member":"replica-2"}`)

func BenchmarkCodecJSONDecode(b *testing.B) {
	c, err := codec.New("json", nil)
	require.NoError(b, err)

	var dst event.Event
	b.ReportAllocs()
	b.SetBytes(int64(len(canonicalAdd)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := c.Decode(canonicalAdd, "kv-events", &dst); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCodecJSONDecodeParallel(b *testing.B) {
	c, err := codec.New("json", nil)
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var dst event.Event
		for pb.Next() {
			if err := c.Decode(canonicalAdd, "kv-events", &dst); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// TestJSONDecodeAllocationBudget pins the allocation count on the hot path.
//
// Three allocations are structural and cannot be removed without changing the
// Event type: the key string, the member string, and the timestamp parse. The
// publisher and topic are interned, because a fixed set of publishers repeats
// on every event and allocating a fresh string for each one is pure waste.
//
// This is a test rather than only a benchmark so that a regression fails CI
// instead of quietly showing up in a benchstat nobody reads.
func TestJSONDecodeAllocationBudget(t *testing.T) {
	const budget = 3

	c, err := codec.New("json", nil)
	require.NoError(t, err)

	var dst event.Event
	// Warm the interner and the scratch pool, so the measurement reflects the
	// steady state rather than first-call setup.
	require.NoError(t, c.Decode(canonicalAdd, "kv-events", &dst))

	allocs := testing.AllocsPerRun(200, func() {
		if decodeErr := c.Decode(canonicalAdd, "kv-events", &dst); decodeErr != nil {
			t.Fatal(decodeErr)
		}
	})

	require.LessOrEqual(t, allocs, float64(budget),
		"json decode allocates %.0f times per event, budget is %d", allocs, budget)
}
