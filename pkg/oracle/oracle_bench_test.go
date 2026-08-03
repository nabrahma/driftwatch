package oracle_test

import (
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/seqtrack"
)

// BenchmarkOracleApply measures the single applier's hot path.
// §16.8 targets more than 500k ops/sec/core.
func BenchmarkOracleApply(b *testing.B) {
	o := oracle.New(oracle.Config{Clock: clock.Fake(epoch)})

	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = "block-" + strconv.Itoa(i)
	}
	value := setOf("replica-0", "replica-1")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		o.Apply(
			projection.Mutation{Key: key, Action: projection.ActionUpsert, Value: value},
			&event.Event{
				Publisher: "replica-0", Epoch: 1, Seq: uint64(i + 1), //nolint:gosec // loop counter
				Op: event.OpAdd, Key: key, ObservedAt: epoch,
			},
			seqtrack.Accept, oracle.TrustComplete,
		)
	}
}

// BenchmarkOracleGet measures the concurrent read path.
// §16.8 targets more than 2M ops/sec/core.
func BenchmarkOracleGet(b *testing.B) {
	o := oracle.New(oracle.Config{Clock: clock.Fake(epoch)})

	const keyCount = 1024
	keys := make([]string, keyCount)
	for i := range keys {
		keys[i] = "block-" + strconv.Itoa(i)
		m, e := upsert(keys[i], epoch, "replica-0", "replica-1")
		o.Apply(m, e, seqtrack.Accept, oracle.TrustComplete)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, ok := o.Get(keys[i%keyCount]); !ok {
			b.Fatal("missing key")
		}
	}
}

// BenchmarkOracleVersion measures the fence read, which the sweeper does twice
// per key per sweep and which therefore has to be cheaper than Get.
func BenchmarkOracleVersion(b *testing.B) {
	o := oracle.New(oracle.Config{Clock: clock.Fake(epoch)})

	const keyCount = 1024
	keys := make([]string, keyCount)
	for i := range keys {
		keys[i] = "block-" + strconv.Itoa(i)
		m, e := upsert(keys[i], epoch, "replica-0")
		o.Apply(m, e, seqtrack.Accept, oracle.TrustComplete)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, ok := o.Version(keys[i%keyCount]); !ok {
			b.Fatal("missing key")
		}
	}
}

// seedMillion fills an oracle with a million keys, which is the scale every
// §19.1 target is stated at.
//
//nolint:gocritic // hugeParam: mirrors oracle.New's by-value Config.
func seedMillion(b *testing.B, cfg oracle.Config) *oracle.Oracle {
	b.Helper()

	cfg.Clock = clock.Fake(epoch)
	if cfg.MaxTrackedKeys == 0 {
		// Headroom, so the measurement is of a million resident keys rather
		// than of eviction churn. Sizing the budget to exactly a million loses
		// about 0.3% of them to shard imbalance (docs/DISCOVERIES.md D-003).
		cfg.MaxTrackedKeys = 2_000_000
	}
	o := oracle.New(cfg)

	value := setOf("replica-0")
	e := &event.Event{Publisher: "replica-0", Epoch: 1, Op: event.OpAdd, ObservedAt: epoch}
	for i := 0; i < 1_000_000; i++ {
		key := "block-" + strconv.Itoa(i)
		e.Key = key
		e.Seq = uint64(i + 1) //nolint:gosec // loop counter
		o.Apply(
			projection.Mutation{Key: key, Action: projection.ActionUpsert, Value: value},
			e, seqtrack.Accept, oracle.TrustComplete,
		)
	}
	require.Equal(b, 1_000_000, o.Len())
	return o
}

// BenchmarkSettledKeys1M measures iterating a million settled keys.
// §16.8 targets under 50ms. A naive full scan of every entry every sweep is
// the performance bug this benchmark exists to catch.
func BenchmarkSettledKeys1M(b *testing.B) {
	o := seedMillion(b, oracle.Config{SettlementWindow: time.Second})
	now := epoch.Add(time.Hour)

	// Collect before timing. Seeding a million keys leaves a large amount of
	// garbage, and letting the collector work it off inside the measurement
	// makes the first iterations two to three times slower than the steady
	// state — which is not what this benchmark is measuring.
	runtime.GC()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		n := 0
		for range o.SettledKeys(now) {
			n++
		}
		if n != 1_000_000 {
			b.Fatalf("expected 1,000,000 settled keys, iterated %d", n)
		}
	}
}

// BenchmarkMarkSuspectAll1M is the benchmark the generation-counter design
// exists for. §16.8 targets under 1ms at a million keys.
//
// The naive implementation writes every entry under each shard's write lock,
// which at a million keys takes seconds — and it would take them at exactly the
// moment a publisher is flapping and the applier most needs to keep up.
func BenchmarkMarkSuspectAll1M(b *testing.B) {
	o := seedMillion(b, oracle.Config{})
	runtime.GC()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		o.MarkSuspect("", "sequence gap")
	}
}

// BenchmarkOracleMemory1M reports the resident cost of tracking a million keys.
// §16.8 and success criterion S5 budget 512 MiB.
func BenchmarkOracleMemory1M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		o := seedMillion(b, oracle.Config{})

		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)

		used := after.HeapAlloc - before.HeapAlloc
		b.ReportMetric(float64(used)/(1<<20), "MiB/1Mkeys")

		// Keep the oracle alive across the measurement so the collector cannot
		// reclaim what is being measured.
		runtime.KeepAlive(o)
	}
}

// BenchmarkOracleApplyLargeMemberSets measures the cost the oracle inherits
// from holding whole member sets, which is what dominates memory at a million
// keys with wide sets.
func BenchmarkOracleApplyLargeMemberSets(b *testing.B) {
	for _, size := range []int{1, 10, 100} {
		b.Run("members="+strconv.Itoa(size), func(b *testing.B) {
			o := oracle.New(oracle.Config{Clock: clock.Fake(epoch)})

			members := make([]string, size)
			for i := range members {
				members[i] = "replica-" + strconv.Itoa(i)
			}
			value := setOf(members...)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				key := "block-" + strconv.Itoa(i%1024)
				o.Apply(
					projection.Mutation{Key: key, Action: projection.ActionUpsert, Value: value},
					&event.Event{
						Publisher: "p", Epoch: 1, Seq: uint64(i + 1), //nolint:gosec // loop counter
						Op: event.OpAdd, Key: key, ObservedAt: epoch,
					},
					seqtrack.Accept, oracle.TrustComplete,
				)
			}
		})
	}
}
