//go:build integration

// The one benchmark in §16.8 that has to run against a real server.
//
// Every other benchmark in this repository measures driftwatch's own overhead
// and runs against miniredis or an in-memory target, because a network round
// trip would bury the thing being measured. Here the round trip *is* the thing
// being measured: S6 claims a full sweep of 1,000,000 keys completes in under
// ten seconds, and that claim is about pipelining against a real server under a
// real protocol, not about how fast a map lookup is.
//
// Running it: `make bench-sweep` (Docker required). It is excluded from the
// default `make bench` because it seeds a million keys, which takes longer than
// the whole rest of the benchmark suite.
package sweeper_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcwait "github.com/testcontainers/testcontainers-go/wait"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/seqtrack"
	"github.com/nabrahma/driftwatch/pkg/sweeper"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// sweepKeys is the S6 figure. It is a constant rather than a parameter because
// the success criterion names this number specifically; a benchmark that swept
// 100,000 keys and extrapolated would not be evidence for it.
const sweepKeys = 1_000_000

// BenchmarkFullSweep1M sweeps a million real keys out of a real Redis.
//
// §16.8 target: under 10 seconds (S6).
//
// What is deliberately included: the settled-key iteration, the batched
// pipelined reads, the per-key comparison, and the categorisation of every
// disagreement. What is deliberately excluded: seeding, which happens before
// the timer starts, and confirmation, which by construction happens a
// settlement window later and is not part of a sweep's latency.
//
// The oracle and the store are seeded to *agree*. A sweep that found a million
// divergences would spend its time building findings, and would be measuring
// the reporting path rather than the read path. §5.5's first pass over a
// healthy keyspace is the operation S6 is about.
func BenchmarkFullSweep1M(b *testing.B) {
	ctx := context.Background()

	addr := startRedisForBench(ctx, b)
	seedRedis(ctx, b, addr, sweepKeys)

	// A real clock would make settlement depend on how long seeding took.
	clk := clock.Fake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	// 10% headroom, not sweepKeys+1. D-003: the key budget is enforced per
	// shard, so hash imbalance evicts ~0.3% of the keyspace while the oracle is
	// globally not full. Sizing this to the exact key count loses ~3,300 keys
	// and reads as an eviction bug — which is the wrong turn D-003 exists to
	// stop someone taking twice.
	orc := oracle.New(oracle.Config{
		Clock:            clk,
		SettlementWindow: window,
		MaxTrackedKeys:   sweepKeys + sweepKeys/10,
	})
	seedOracle(b, orc, clk, sweepKeys)

	// Past the window, so every key is eligible. Without this the sweep reads
	// nothing and returns in microseconds — which has happened, and looks like
	// a spectacular result rather than a broken benchmark.
	clk.Advance(window + time.Second)

	tgt, err := target.NewRedis(target.RedisOptions{
		Addrs:     []string{addr},
		BatchSize: 500,
		ScanCount: 1000,
	})
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, tgt.Close()) })

	swp := sweeper.New(sweeper.Config{
		Oracle:           orc,
		Target:           tgt,
		Shape:            projection.ShapeScalar,
		Clock:            clk,
		ReadBatchSize:    500,
		SettlementWindow: func() time.Duration { return window },
	})
	b.Cleanup(func() { require.NoError(b, swp.Close()) })

	// One sweep outside the timer. The first one pays for connection setup and
	// for the pool growing to its steady size, neither of which recurs.
	warm, err := swp.SweepOnce(ctx)
	require.NoError(b, err)
	require.Equal(b, sweepKeys, warm.KeysCompared,
		"the warm-up sweep did not compare the whole keyspace, so the "+
			"measured runs would be timing a smaller sweep than S6 claims")
	require.Empty(b, warm.Findings,
		"oracle and store were seeded to agree but %d keys diverged, so this "+
			"would be measuring the reporting path", len(warm.Findings))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rep, err := swp.SweepOnce(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if rep.KeysCompared != sweepKeys {
			b.Fatalf("compared %d keys, want %d", rep.KeysCompared, sweepKeys)
		}
	}

	b.StopTimer()
	// Per-key cost, which is the figure that survives a change to sweepKeys.
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/sweepKeys, "ns/key")
}

// startRedisForBench brings up a Redis container for the duration of the
// benchmark.
//
// It does not reuse startRedis from redis_integration_test.go because that
// file is in package target_test. The duplication is small and the alternative
// — exporting a container helper from non-test code — would put testcontainers
// in the shipped dependency graph.
func startRedisForBench(ctx context.Context, b *testing.B) string {
	b.Helper()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7.2-alpine",
			ExposedPorts: []string{"6379/tcp"},
			// A million keys at the default maxmemory-policy would start
			// evicting, and an eviction mid-sweep is a different measurement.
			Cmd:        []string{"redis-server", "--save", "", "--maxmemory-policy", "noeviction"},
			WaitingFor: tcwait.ForListeningPort("6379/tcp").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	require.NoError(b, err)
	b.Cleanup(func() {
		if termErr := testcontainers.TerminateContainer(container); termErr != nil {
			b.Logf("terminating redis: %v", termErr)
		}
	})

	host, err := container.Host(ctx)
	require.NoError(b, err)
	port, err := container.MappedPort(ctx, "6379/tcp")
	require.NoError(b, err)

	return fmt.Sprintf("%s:%s", host, port.Port())
}

// seedRedis writes n keys via pipelined batches.
//
// A million individual SETs over a mapped container port takes minutes; in
// batches of 10,000 it takes seconds. This is fixture cost, not measured cost,
// but a benchmark nobody waits for is a benchmark nobody runs.
func seedRedis(ctx context.Context, b *testing.B, addr string, n int) {
	b.Helper()

	client := redis.NewClient(&redis.Options{Addr: addr})
	b.Cleanup(func() {
		if closeErr := client.Close(); closeErr != nil {
			b.Logf("closing the seeding client: %v", closeErr)
		}
	})
	require.NoError(b, client.Ping(ctx).Err())

	const batch = 10_000
	pipe := client.Pipeline()
	for i := 0; i < n; i++ {
		pipe.Set(ctx, benchKey(i), benchValue(i), 0)
		if (i+1)%batch == 0 {
			_, err := pipe.Exec(ctx)
			require.NoError(b, err)
		}
	}
	_, err := pipe.Exec(ctx)
	require.NoError(b, err)

	size, err := client.DBSize(ctx).Result()
	require.NoError(b, err)
	require.EqualValues(b, n, size, "seeded keyspace is the wrong size")
}

// seedOracle folds n events in, so the oracle holds the same n keys with the
// same values the store holds.
func seedOracle(b *testing.B, orc *oracle.Oracle, clk clock.FakeClock, n int) {
	b.Helper()

	for i := 0; i < n; i++ {
		key, value := benchKey(i), benchValue(i)
		orc.Apply(projection.Mutation{
			Key:    key,
			Action: projection.ActionUpsert,
			Value:  event.Value{Kind: event.ValueScalar, Scalar: []byte(value)},
		}, &event.Event{
			Publisher:  "bench",
			Epoch:      1,
			Seq:        uint64(i) + 1,
			Op:         event.OpSet,
			Key:        key,
			Value:      []byte(value),
			ObservedAt: clk.Now(),
		}, seqtrack.Accept, oracle.TrustComplete)
	}
	require.Equal(b, n, orc.Len(), "oracle did not retain the whole keyspace")
}

func benchKey(i int) string   { return "block:" + strconv.Itoa(i) }
func benchValue(i int) string { return "replica-" + strconv.Itoa(i%8) }
