package target_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// benchTarget builds a redis target over an in-process server.
//
// miniredis rather than a container: these benchmarks measure driftwatch's
// pipelining and iteration overhead, and a real network would bury that under
// round-trip latency. BenchmarkFullSweep1M in Phase 3 is the one that belongs
// against a real server, because there the network is the subject.
func benchTarget(b *testing.B) (*target.RedisTarget, *miniredis.Miniredis) {
	b.Helper()

	server := miniredis.RunT(b)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	tgt := target.NewRedisFromClient(client, 0, 0)
	b.Cleanup(func() { require.NoError(b, tgt.Close()) })

	return tgt, server
}

// BenchmarkGetMany500 measures one pipelined batch, which is the unit the
// sweeper reads in.
//
// PRD §16.8 expects this to be dominated by the network and asks for fewer than
// five allocations per key. Against an in-process server the network is gone,
// so what is left is the pipeline construction and reply decoding — which is
// exactly the part driftwatch controls.
func BenchmarkGetMany500(b *testing.B) {
	tgt, server := benchTarget(b)
	ctx := context.Background()

	const batch = 500
	keys := make([]string, batch)
	for i := range keys {
		keys[i] = "block-" + strconv.Itoa(i)
		require.NoError(b, server.Set(keys[i], "replica-0"))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		values, err := tgt.GetMany(ctx, keys, projection.ShapeScalar)
		if err != nil {
			b.Fatal(err)
		}
		if len(values) != batch {
			b.Fatalf("expected %d values, got %d", batch, len(values))
		}
	}

	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batch), "ns/key")
}

// BenchmarkGetMany500Sets is the same batch for the flagship shape, where each
// reply is a member list rather than a single string.
func BenchmarkGetMany500Sets(b *testing.B) {
	tgt, server := benchTarget(b)
	ctx := context.Background()

	const batch = 500
	keys := make([]string, batch)
	for i := range keys {
		keys[i] = "block-" + strconv.Itoa(i)
		_, err := server.SAdd(keys[i], "replica-0", "replica-1")
		require.NoError(b, err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := tgt.GetMany(ctx, keys, projection.ShapeSet); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batch), "ns/key")
}

// BenchmarkReadMany500 measures the per-key-outcome path the sweeper actually
// uses, so the cost of preserving wrong-type information is visible rather than
// assumed.
func BenchmarkReadMany500(b *testing.B) {
	tgt, server := benchTarget(b)
	ctx := context.Background()

	const batch = 500
	keys := make([]string, batch)
	for i := range keys {
		keys[i] = "block-" + strconv.Itoa(i)
		require.NoError(b, server.Set(keys[i], "replica-0"))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := tgt.ReadMany(ctx, keys, projection.ShapeScalar); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkScan1M walks a million keys.
//
// The thing being measured is that the scan streams: it holds one batch plus a
// dedup set, never the whole keyspace. A scan that accumulated every key would
// look fine here on time and be an out-of-memory kill at ten million.
func BenchmarkScan1M(b *testing.B) {
	tgt, server := benchTarget(b)
	ctx := context.Background()

	const keyCount = 1_000_000
	for i := 0; i < keyCount; i++ {
		require.NoError(b, server.Set("block-"+strconv.Itoa(i), "v"))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		seen := 0
		it := tgt.Scan(ctx, "*", 1000)
		for it.Next(ctx) {
			seen += len(it.Keys())
		}
		if err := it.Err(); err != nil {
			b.Fatal(err)
		}
		if err := it.Close(); err != nil {
			b.Fatal(err)
		}
		if seen != keyCount {
			b.Fatalf("expected %d keys, scanned %d", keyCount, seen)
		}
	}

	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*keyCount), "ns/key")
}

// BenchmarkMemoryGet is the fake's read path, which every fault test and most
// sweeper tests run through, so its cost sets the floor for those suites.
func BenchmarkMemoryGet(b *testing.B) {
	mem := target.NewMemory()
	mem.SeedSets(map[string][]string{"block-1": {"replica-0", "replica-1"}})
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := mem.Get(ctx, "block-1", projection.ShapeSet); err != nil {
			b.Fatal(err)
		}
	}
}

// TestGetMany500AllocationBudget pins driftwatch's own allocation cost per key.
//
// PRD §16.8 asks for fewer than five allocations per key. That number is not
// reachable and never was: a bare go-redis pipeline of 500 GETs, with no
// driftwatch code involved at all, allocates about 16 times per key. The budget
// is below the floor the mandated client imposes (docs/DISCOVERIES.md D-007).
//
// So this measures the thing driftwatch actually controls: the overhead above
// that floor. Both figures are measured in the same run, which also makes the
// assertion immune to a go-redis upgrade moving the floor in either direction.
func TestGetMany500AllocationBudget(t *testing.T) {
	const (
		batch = 500
		// Overhead budget per key, above the bare client. driftwatch's share is
		// the reply conversion into event.Value plus the per-key Read slot.
		overheadBudget = 4.0
	)

	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	tgt := target.NewRedisFromClient(client, 0, 0)
	t.Cleanup(func() { require.NoError(t, tgt.Close()) })

	ctx := context.Background()
	keys := make([]string, batch)
	for i := range keys {
		keys[i] = "block-" + strconv.Itoa(i)
		require.NoError(t, server.Set(keys[i], "replica-0"))
	}

	// Warm the connection pool and the pipeline buffers so the measurement is
	// of the steady state rather than of first-call setup.
	_, err := tgt.GetMany(ctx, keys, projection.ShapeScalar)
	require.NoError(t, err)

	floor := testing.AllocsPerRun(20, func() {
		pipe := client.Pipeline()
		cmds := make([]*redis.StringCmd, 0, batch)
		for _, key := range keys {
			cmds = append(cmds, pipe.Get(ctx, key))
		}
		if _, execErr := pipe.Exec(ctx); execErr != nil {
			t.Fatal(execErr)
		}
		for _, cmd := range cmds {
			if _, resErr := cmd.Result(); resErr != nil {
				t.Fatal(resErr)
			}
		}
	})

	actual := testing.AllocsPerRun(20, func() {
		if _, batchErr := tgt.GetMany(ctx, keys, projection.ShapeScalar); batchErr != nil {
			t.Fatal(batchErr)
		}
	})

	floorPerKey := floor / batch
	actualPerKey := actual / batch
	overhead := actualPerKey - floorPerKey

	t.Logf("bare go-redis pipeline: %.2f allocs/key; driftwatch GetMany: %.2f allocs/key; "+
		"driftwatch overhead: %.2f allocs/key", floorPerKey, actualPerKey, overhead)

	require.Less(t, overhead, overheadBudget,
		"driftwatch adds %.2f allocations per key above the client's %.2f, budget is %.0f",
		overhead, floorPerKey, overheadBudget)
}
