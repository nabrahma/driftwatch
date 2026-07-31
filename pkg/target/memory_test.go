package target_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/target"
)

func TestMemory_GetReadsEachShape(t *testing.T) {
	mem := target.NewMemory()
	mem.Seed(map[string][]byte{"scalar": []byte("hello"), "counter": []byte("42")})
	mem.SeedSets(map[string][]string{"set": {"replica-0", "replica-1"}})
	ctx := context.Background()

	tests := []struct {
		name  string
		key   string
		shape projection.Shape
		want  event.Value
	}{
		{
			name:  "a scalar reads back",
			key:   "scalar",
			shape: projection.ShapeScalar,
			want:  event.Value{Kind: event.ValueScalar, Scalar: []byte("hello")},
		},
		{
			name:  "a set reads back its members",
			key:   "set",
			shape: projection.ShapeSet,
			want:  setOf("replica-0", "replica-1"),
		},
		{
			name:  "an integer scalar reads as a counter",
			key:   "counter",
			shape: projection.ShapeCounter,
			want:  event.Value{Kind: event.ValueCounter, Counter: 42},
		},
		{name: "a missing scalar is absent", key: "gone", shape: projection.ShapeScalar},
		{name: "a missing set is absent", key: "gone", shape: projection.ShapeSet},
		{name: "a missing counter is absent", key: "gone", shape: projection.ShapeCounter},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := mem.Get(ctx, tc.key, tc.shape)

			require.NoError(t, err)
			assert.True(t, tc.want.Equal(got), "want %s, got %s", tc.want, got)
		})
	}
}

func TestMemory_MatchesTheRedisEmptySetDecision(t *testing.T) {
	// Seeding a key with no members must leave no key at all, the same way
	// Redis removes a set when its last member goes. A fake that kept an empty
	// set would let a bug through that the real store would catch.
	mem := target.NewMemory()
	mem.SeedSets(map[string][]string{"empty": {}})

	got, err := mem.Get(context.Background(), "empty", projection.ShapeSet)

	require.NoError(t, err)
	assert.True(t, got.IsAbsent())
	assert.Equal(t, 0, mem.Len())
}

func TestMemory_WrongType(t *testing.T) {
	mem := target.NewMemory()
	mem.Seed(map[string][]byte{"a-string": []byte("v"), "not-a-number": []byte("abc")})
	mem.SeedSets(map[string][]string{"a-set": {"m"}})
	ctx := context.Background()

	tests := []struct {
		name  string
		key   string
		shape projection.Shape
		got   string
	}{
		{name: "a set read as a scalar", key: "a-set", shape: projection.ShapeScalar, got: "set"},
		{name: "a set read as a counter", key: "a-set", shape: projection.ShapeCounter, got: "set"},
		{name: "a string read as a set", key: "a-string", shape: projection.ShapeSet, got: "string"},
		{
			name:  "a non-integer string read as a counter",
			key:   "not-a-number",
			shape: projection.ShapeCounter,
			got:   "non-integer string",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := mem.Get(ctx, tc.key, tc.shape)

			require.ErrorIs(t, err, target.ErrWrongType)
			var wrong *target.WrongTypeError
			require.True(t, errors.As(err, &wrong))
			assert.Equal(t, tc.got, wrong.Got)
		})
	}
}

func TestMemory_AnUnknownShapeIsReportedRatherThanGuessed(t *testing.T) {
	mem := target.NewMemory()
	mem.Seed(map[string][]byte{"k": []byte("v")})

	_, err := mem.Get(context.Background(), "k", projection.Shape(9))

	assert.ErrorIs(t, err, target.ErrWrongType)
}

func TestMemory_ReadManyPreservesPerKeyOutcomes(t *testing.T) {
	mem := target.NewMemory()
	mem.Seed(map[string][]byte{"present": []byte("v")})
	mem.SeedSets(map[string][]string{"a-set": {"m"}})

	reads, err := mem.ReadMany(context.Background(),
		[]string{"present", "missing", "a-set"}, projection.ShapeScalar)

	require.NoError(t, err)
	require.Len(t, reads, 3)
	assert.NoError(t, reads[0].Err)
	assert.NoError(t, reads[1].Err)
	assert.True(t, reads[1].Value.IsAbsent())
	assert.ErrorIs(t, reads[2].Err, target.ErrWrongType)
}

func TestMemory_GetManyFlattensUnreadableKeysToAbsent(t *testing.T) {
	mem := target.NewMemory()
	mem.SeedSets(map[string][]string{"a-set": {"m"}})

	values, err := mem.GetMany(context.Background(), []string{"a-set"}, projection.ShapeScalar)

	require.NoError(t, err)
	require.Len(t, values, 1)
	assert.True(t, values[0].IsAbsent())
}

func TestMemory_GetReturnsCopiesTheCallerCannotWriteThrough(t *testing.T) {
	mem := target.NewMemory()
	mem.Seed(map[string][]byte{"scalar": []byte("original")})
	mem.SeedSets(map[string][]string{"set": {"a"}})
	ctx := context.Background()

	scalar, err := mem.Get(ctx, "scalar", projection.ShapeScalar)
	require.NoError(t, err)
	scalar.Scalar[0] = 'X'

	set, err := mem.Get(ctx, "set", projection.ShapeSet)
	require.NoError(t, err)
	set.Members["injected"] = struct{}{}

	again, err := mem.Get(ctx, "scalar", projection.ShapeScalar)
	require.NoError(t, err)
	assert.Equal(t, []byte("original"), again.Scalar)

	againSet, err := mem.Get(ctx, "set", projection.ShapeSet)
	require.NoError(t, err)
	assert.True(t, againSet.Equal(setOf("a")))
}

func TestMemory_SeedCopiesTheBytesItIsGiven(t *testing.T) {
	mem := target.NewMemory()
	value := []byte("original")
	mem.Seed(map[string][]byte{"k": value})

	value[0] = 'X'

	got, err := mem.Get(context.Background(), "k", projection.ShapeScalar)
	require.NoError(t, err)
	assert.Equal(t, []byte("original"), got.Scalar)
}

func TestMemory_Scan(t *testing.T) {
	mem := target.NewMemory()
	mem.Seed(map[string][]byte{"kv:a": []byte("1"), "kv:b": []byte("2"), "other:c": []byte("3")})
	ctx := context.Background()

	t.Run("a match-all pattern yields every key", func(t *testing.T) {
		assert.ElementsMatch(t, []string{"kv:a", "kv:b", "other:c"}, drainMem(t, mem, "*", 10))
	})

	t.Run("an empty pattern also yields every key", func(t *testing.T) {
		assert.Len(t, drainMem(t, mem, "", 10), 3)
	})

	t.Run("a glob pattern filters", func(t *testing.T) {
		assert.ElementsMatch(t, []string{"kv:a", "kv:b"}, drainMem(t, mem, "kv:*", 10))
	})

	t.Run("a pattern matching nothing yields nothing", func(t *testing.T) {
		assert.Empty(t, drainMem(t, mem, "nope:*", 10))
	})

	t.Run("batching splits the results without losing any", func(t *testing.T) {
		it := mem.Scan(ctx, "*", 2)
		batches, total := 0, 0
		for it.Next(ctx) {
			batches++
			total += len(it.Keys())
			assert.LessOrEqual(t, len(it.Keys()), 2)
		}
		require.NoError(t, it.Err())
		require.NoError(t, it.Close())
		assert.Equal(t, 2, batches)
		assert.Equal(t, 3, total)
	})

	t.Run("a non-positive batch falls back to the default", func(t *testing.T) {
		assert.Len(t, drainMem(t, mem, "*", 0), 3)
	})
}

func TestMemory_ScanIsInterruptibleByContext(t *testing.T) {
	mem := target.NewMemory()
	for i := 0; i < 50; i++ {
		mem.Seed(map[string][]byte{"k" + strconv.Itoa(i): []byte("v")})
	}

	ctx, cancel := context.WithCancel(context.Background())
	it := mem.Scan(ctx, "*", 5)
	require.True(t, it.Next(ctx))
	cancel()

	assert.False(t, it.Next(ctx))
	assert.ErrorIs(t, it.Err(), context.Canceled)
}

func TestMemory_ExpiryHidesAKeyOnceTheClockPassesIt(t *testing.T) {
	clk := clock.Fake(epoch)
	mem, err := target.New("memory", target.Config{Clock: clk})
	require.NoError(t, err)

	inner, ok := mem.(*target.MemoryTarget)
	require.True(t, ok)
	inner.Seed(map[string][]byte{"doomed": []byte("v"), "eternal": []byte("v")})
	inner.SetExpiry("doomed", epoch.Add(time.Minute))
	ctx := context.Background()

	t.Run("before the deadline the key is present with a remaining lifetime", func(t *testing.T) {
		got, getErr := mem.Get(ctx, "doomed", projection.ShapeScalar)
		require.NoError(t, getErr)
		assert.False(t, got.IsAbsent())

		ttl, ttlErr := mem.TTL(ctx, "doomed")
		require.NoError(t, ttlErr)
		require.NotNil(t, ttl)
		assert.Equal(t, time.Minute, *ttl)
	})

	t.Run("a key with no expiry reports nil rather than an error", func(t *testing.T) {
		ttl, ttlErr := mem.TTL(ctx, "eternal")
		require.NoError(t, ttlErr)
		assert.Nil(t, ttl)
	})

	t.Run("a missing key is reported as missing", func(t *testing.T) {
		_, ttlErr := mem.TTL(ctx, "never-existed")
		assert.ErrorIs(t, ttlErr, target.ErrNotFound)
	})

	clk.Advance(2 * time.Minute)

	t.Run("after the deadline the key reads as absent", func(t *testing.T) {
		got, getErr := mem.Get(ctx, "doomed", projection.ShapeScalar)
		require.NoError(t, getErr)
		assert.True(t, got.IsAbsent())
	})

	t.Run("an expired key is gone as far as TTL is concerned", func(t *testing.T) {
		_, ttlErr := mem.TTL(ctx, "doomed")
		assert.ErrorIs(t, ttlErr, target.ErrNotFound)
	})

	t.Run("an expired key does not appear in a scan", func(t *testing.T) {
		it := mem.Scan(ctx, "*", 10)
		var keys []string
		for it.Next(ctx) {
			keys = append(keys, it.Keys()...)
		}
		require.NoError(t, it.Err())
		assert.Equal(t, []string{"eternal"}, keys)
	})
}

func TestMemory_HealthReportsTheLiveKeyspaceSize(t *testing.T) {
	mem := target.NewMemory()
	mem.Seed(map[string][]byte{"a": []byte("1"), "b": []byte("2")})
	mem.SeedSets(map[string][]string{"c": {"m"}})

	got, err := mem.Health(context.Background())

	require.NoError(t, err)
	assert.True(t, got.Reachable)
	assert.Equal(t, int64(3), got.KeyspaceSize)
	assert.Equal(t, "master", got.Role)
}

func TestMemory_SetHealthReplacesTheReportedDiagnostics(t *testing.T) {
	mem := target.NewMemory()
	mem.SetHealth(target.Health{
		Reachable: true, Role: "replica", EvictedKeys: 7, MaxMemoryBytes: 1 << 20,
	})

	got, err := mem.Health(context.Background())

	require.NoError(t, err)
	assert.Equal(t, "replica", got.Role)
	assert.Equal(t, uint64(7), got.EvictedKeys)
	assert.Equal(t, uint64(1<<20), got.MaxMemoryBytes)
}

func TestMemory_SimulateEvictDropsKeysAndCountsThem(t *testing.T) {
	// The fault matrix needs a store-side eviction, and no read-only interface
	// can express one.
	mem := target.NewMemory()
	for i := 0; i < 10; i++ {
		mem.Seed(map[string][]byte{"k" + strconv.Itoa(i): []byte("v")})
	}

	victims := mem.SimulateEvict(3)

	assert.Len(t, victims, 3)
	assert.Equal(t, 7, mem.Len())

	got, err := mem.Health(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(3), got.EvictedKeys,
		"a sweep finding mass absence needs the eviction counter to explain it")

	for _, victim := range victims {
		value, getErr := mem.Get(context.Background(), victim, projection.ShapeScalar)
		require.NoError(t, getErr)
		assert.True(t, value.IsAbsent())
	}
}

func TestMemory_SimulateEvictAsksForMoreThanExists(t *testing.T) {
	mem := target.NewMemory()
	mem.Seed(map[string][]byte{"only": []byte("v")})

	victims := mem.SimulateEvict(10)

	assert.Len(t, victims, 1)
	assert.Equal(t, 0, mem.Len())
}

func TestMemory_SimulateFlushEmptiesEverything(t *testing.T) {
	mem := target.NewMemory()
	mem.Seed(map[string][]byte{"a": []byte("1")})
	mem.SeedSets(map[string][]string{"b": {"m"}})
	mem.SetExpiry("a", epoch)
	require.Equal(t, 2, mem.Len())

	mem.SimulateFlush()

	assert.Equal(t, 0, mem.Len())
}

func TestMemory_InjectedFailuresAreDeterministic(t *testing.T) {
	// A rate rather than a probability: a flaky fault injector produces flaky
	// tests, and a flaky test of an error path is worse than no test at all.
	mem := target.NewMemory(target.WithFailureRate(0.25))
	ctx := context.Background()

	var outcomes []bool
	for i := 0; i < 8; i++ {
		_, err := mem.Get(ctx, "k", projection.ShapeScalar)
		outcomes = append(outcomes, err != nil)
	}

	assert.Equal(t,
		[]bool{false, false, false, true, false, false, false, true},
		outcomes,
		"a rate of 0.25 must fail exactly every fourth call, not roughly one in four")

	_, err := mem.Get(ctx, "k", projection.ShapeScalar)
	_ = err
	_, err = mem.Get(ctx, "k", projection.ShapeScalar)
	_ = err
	_, err = mem.Get(ctx, "k", projection.ShapeScalar)
	_ = err
	_, err = mem.Get(ctx, "k", projection.ShapeScalar)
	assert.ErrorIs(t, err, target.ErrInjected)
}

func TestMemory_FailureRateBounds(t *testing.T) {
	ctx := context.Background()

	t.Run("a rate of zero never fails", func(t *testing.T) {
		mem := target.NewMemory(target.WithFailureRate(0))
		for i := 0; i < 10; i++ {
			_, err := mem.Get(ctx, "k", projection.ShapeScalar)
			require.NoError(t, err)
		}
	})

	t.Run("a rate of one fails every call", func(t *testing.T) {
		mem := target.NewMemory(target.WithFailureRate(1))
		for i := 0; i < 5; i++ {
			_, err := mem.Get(ctx, "k", projection.ShapeScalar)
			require.ErrorIs(t, err, target.ErrInjected)
		}
	})

	t.Run("a negative rate is treated as zero", func(t *testing.T) {
		mem := target.NewMemory(target.WithFailureRate(-1))
		_, err := mem.Get(ctx, "k", projection.ShapeScalar)
		require.NoError(t, err)
	})
}

func TestMemory_InjectedLatencyUsesTheClockRatherThanSleeping(t *testing.T) {
	// The delay has to be visible in the test rather than hidden in a sleep,
	// which is the whole reason the clock is injected (§16.4).
	clk := clock.Fake(epoch)
	mem, err := target.New("memory", target.Config{
		Settings: map[string]string{"latency": "50ms"},
		Clock:    clk,
	})
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		_, getErr := mem.Get(context.Background(), "k", projection.ShapeScalar)
		done <- getErr
	}()

	clk.BlockUntil(1)
	select {
	case <-done:
		t.Fatal("the read completed without the clock advancing")
	default:
	}

	clk.Advance(50 * time.Millisecond)
	require.NoError(t, <-done)
	assert.Equal(t, epoch.Add(50*time.Millisecond), clk.Now())
}

func TestMemory_LatencyRespectsContextCancellation(t *testing.T) {
	clk := clock.Fake(epoch)
	mem, err := target.New("memory", target.Config{
		Settings: map[string]string{"latency": "1h"},
		Clock:    clk,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, getErr := mem.Get(ctx, "k", projection.ShapeScalar)
		done <- getErr
	}()

	clk.BlockUntil(1)
	cancel()

	assert.ErrorIs(t, <-done, context.Canceled)
}

func TestMemory_ClosedTargetRefusesEveryOperation(t *testing.T) {
	mem := target.NewMemory()
	ctx := context.Background()
	require.NoError(t, mem.Close())

	_, err := mem.Get(ctx, "k", projection.ShapeScalar)
	assert.ErrorIs(t, err, target.ErrClosed)

	_, err = mem.ReadMany(ctx, []string{"k"}, projection.ShapeScalar)
	assert.ErrorIs(t, err, target.ErrClosed)

	_, err = mem.TTL(ctx, "k")
	assert.ErrorIs(t, err, target.ErrClosed)

	_, err = mem.Health(ctx)
	assert.ErrorIs(t, err, target.ErrClosed)

	it := mem.Scan(ctx, "*", 10)
	assert.False(t, it.Next(ctx))
	assert.ErrorIs(t, it.Err(), target.ErrClosed)

	assert.NoError(t, mem.Close(), "Close is idempotent")
}

func TestMemory_ConstructorValidatesItsConfiguration(t *testing.T) {
	tests := []struct {
		name     string
		settings map[string]string
	}{
		{name: "a non-numeric failure rate is rejected", settings: map[string]string{"failureRate": "often"}},
		{name: "a failure rate above one is rejected", settings: map[string]string{"failureRate": "2"}},
		{name: "a negative failure rate is rejected", settings: map[string]string{"failureRate": "-0.5"}},
		{name: "an unparseable latency is rejected", settings: map[string]string{"latency": "soon"}},
		{name: "a negative latency is rejected", settings: map[string]string{"latency": "-1s"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := target.New("memory", target.Config{Settings: tc.settings})

			require.Error(t, err)
			assert.Contains(t, err.Error(), "invalid target configuration")
		})
	}
}

func TestMemory_NameIsTheRegistryName(t *testing.T) {
	assert.Equal(t, "memory", target.NewMemory().Name())
}

func TestRegistry_TargetNames(t *testing.T) {
	names := target.Names()

	assert.Contains(t, names, "memory")
	assert.Contains(t, names, "redis")
	for i := 1; i < len(names); i++ {
		assert.LessOrEqual(t, names[i-1], names[i], "Names must be sorted")
	}
}

func TestRegistry_NewReportsAnUnknownTarget(t *testing.T) {
	_, err := target.New("cassandra", target.Config{})

	require.ErrorIs(t, err, target.ErrUnknownTarget)
	assert.Contains(t, err.Error(), "redis", "the error must say what is available")
}

func TestRegistry_RegisterRejectsDuplicateAndEmptyNames(t *testing.T) {
	ctor := func(target.Config) (target.Target, error) { return target.NewMemory(), nil }

	target.Register("target-test-stub", ctor)
	assert.Contains(t, target.Names(), "target-test-stub")

	assert.Panics(t, func() { target.Register("target-test-stub", ctor) })
	assert.Panics(t, func() { target.Register("", ctor) })
	assert.Panics(t, func() { target.Register("target-test-nil", nil) })
}

func TestConfig_SettingFallsBackToTheDefault(t *testing.T) {
	cfg := target.Config{Settings: map[string]string{"present": "value"}}

	assert.Equal(t, "value", cfg.Setting("present", "fallback"))
	assert.Equal(t, "fallback", cfg.Setting("absent", "fallback"))
}

func TestWrongTypeError_MessageNamesTheKeyAndBothTypes(t *testing.T) {
	err := &target.WrongTypeError{Key: "block-1", Want: projection.ShapeSet, Got: "string"}

	assert.Contains(t, err.Error(), "block-1")
	assert.Contains(t, err.Error(), "string")
	assert.Contains(t, err.Error(), "set")
	assert.ErrorIs(t, err, target.ErrWrongType)
}

func drainMem(t *testing.T, mem target.Target, pattern string, batch int) []string {
	t.Helper()

	ctx := context.Background()
	it := mem.Scan(ctx, pattern, batch)
	var got []string
	for it.Next(ctx) {
		got = append(got, it.Keys()...)
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	return got
}
