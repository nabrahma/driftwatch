package target_test

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// newMiniredis starts an in-process Redis and returns a target pointed at it,
// plus the server so a test can seed it directly.
//
// Seeding goes through the miniredis handle rather than the target, because the
// target refuses to write and that refusal is the feature.
func newMiniredis(t *testing.T) (*target.RedisTarget, *miniredis.Miniredis) {
	t.Helper()

	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	tgt := target.NewRedisFromClient(client, 0, 0)
	t.Cleanup(func() { require.NoError(t, tgt.Close()) })

	return tgt, server
}

func TestRedis_GetReadsEachShape(t *testing.T) {
	tgt, server := newMiniredis(t)
	ctx := context.Background()

	require.NoError(t, server.Set("scalar", "hello"))
	sadd(t, server, "set", "replica-0", "replica-1")
	require.NoError(t, server.Set("counter", "42"))

	tests := []struct {
		name  string
		key   string
		shape projection.Shape
		want  event.Value
	}{
		{
			name:  "a string reads as a scalar",
			key:   "scalar",
			shape: projection.ShapeScalar,
			want:  event.Value{Kind: event.ValueScalar, Scalar: []byte("hello")},
		},
		{
			name:  "a set reads as its members",
			key:   "set",
			shape: projection.ShapeSet,
			want:  setOf("replica-0", "replica-1"),
		},
		{
			name:  "an integer string reads as a counter",
			key:   "counter",
			shape: projection.ShapeCounter,
			want:  event.Value{Kind: event.ValueCounter, Counter: 42},
		},
		{
			name:  "a missing scalar is absent, not an error",
			key:   "nope",
			shape: projection.ShapeScalar,
			want:  event.Value{},
		},
		{
			name:  "a missing set is absent",
			key:   "nope",
			shape: projection.ShapeSet,
			want:  event.Value{},
		},
		{
			name:  "a missing counter is absent",
			key:   "nope",
			shape: projection.ShapeCounter,
			want:  event.Value{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tgt.Get(ctx, tc.key, tc.shape)

			require.NoError(t, err, "a missing key is an absent value, never an error")
			assert.True(t, tc.want.Equal(got), "want %s, got %s", tc.want, got)
		})
	}
}

func TestRedis_AnEmptySetReadsAsAbsent(t *testing.T) {
	// Redis deletes a set key when its last member is removed, so there is no
	// such thing as an empty set to read back. The target has to make the same
	// call event.Value.Equal does (M2), or the differ compares an absent oracle
	// value against a present-but-empty target value and reports drift on every
	// key that ever emptied.
	tgt, server := newMiniredis(t)
	ctx := context.Background()

	sadd(t, server, "emptied", "only-member")
	srem(t, server, "emptied", "only-member")

	got, err := tgt.Get(ctx, "emptied", projection.ShapeSet)

	require.NoError(t, err)
	assert.True(t, got.IsAbsent())
	assert.True(t, got.Equal(event.Value{}))
}

func TestRedis_WrongTypeIsSurfacedAsDriftRatherThanSwallowed(t *testing.T) {
	// Something wrote a different shape into the index. That is a finding, and
	// the most informative one a sweep can produce — swallowing it would leave
	// the key looking merely absent.
	tgt, server := newMiniredis(t)
	ctx := context.Background()

	sadd(t, server, "a-set", "member")
	require.NoError(t, server.Set("a-string", "value"))

	tests := []struct {
		name  string
		key   string
		shape projection.Shape
	}{
		{name: "reading a set as a scalar", key: "a-set", shape: projection.ShapeScalar},
		{name: "reading a set as a counter", key: "a-set", shape: projection.ShapeCounter},
		{name: "reading a string as a set", key: "a-string", shape: projection.ShapeSet},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tgt.Get(ctx, tc.key, tc.shape)

			require.ErrorIs(t, err, target.ErrWrongType)

			var wrong *target.WrongTypeError
			require.True(t, errors.As(err, &wrong))
			assert.Equal(t, tc.key, wrong.Key)
			assert.Equal(t, tc.shape, wrong.Want)
		})
	}
}

func TestRedis_ANonIntegerStringIsAWrongTypeForACounter(t *testing.T) {
	tgt, server := newMiniredis(t)
	ctx := context.Background()
	require.NoError(t, server.Set("k", "not-a-number"))

	_, err := tgt.Get(ctx, "k", projection.ShapeCounter)

	require.ErrorIs(t, err, target.ErrWrongType)
	assert.Contains(t, err.Error(), "non-integer")
}

func TestRedis_ReadManyPreservesPerKeyOutcomes(t *testing.T) {
	// One wrong-typed key must not sink a batch of 500. That key is a finding
	// and the other 499 still need comparing.
	tgt, server := newMiniredis(t)
	ctx := context.Background()

	require.NoError(t, server.Set("present", "v"))
	sadd(t, server, "a-set", "m")

	reads, err := tgt.ReadMany(ctx,
		[]string{"present", "missing", "a-set"}, projection.ShapeScalar)

	require.NoError(t, err)
	require.Len(t, reads, 3)

	assert.NoError(t, reads[0].Err)
	assert.True(t, reads[0].Value.Equal(event.Value{Kind: event.ValueScalar, Scalar: []byte("v")}))

	assert.NoError(t, reads[1].Err, "a missing key is absent, not an error")
	assert.True(t, reads[1].Value.IsAbsent())

	assert.ErrorIs(t, reads[2].Err, target.ErrWrongType)
}

func TestRedis_GetManyMapsAnUnreadableKeyToAbsent(t *testing.T) {
	tgt, server := newMiniredis(t)
	ctx := context.Background()
	sadd(t, server, "a-set", "m")

	values, err := tgt.GetMany(ctx, []string{"a-set"}, projection.ShapeScalar)

	require.NoError(t, err)
	require.Len(t, values, 1)
	assert.True(t, values[0].IsAbsent(),
		"GetMany has nowhere to report the type, which is why ReadMany exists")
}

func TestRedis_GetManyPreservesOrderAcrossPipelineBatches(t *testing.T) {
	tgt, server := newMiniredis(t)
	ctx := context.Background()

	const keyCount = 1500 // more than the default batch of 500
	keys := make([]string, keyCount)
	for i := range keys {
		keys[i] = "k" + strconv.Itoa(i)
		require.NoError(t, server.Set(keys[i], strconv.Itoa(i)))
	}

	values, err := tgt.GetMany(ctx, keys, projection.ShapeCounter)

	require.NoError(t, err)
	require.Len(t, values, keyCount)
	for i, v := range values {
		assert.Equal(t, int64(i), v.Counter, "result %d is out of order", i)
	}
}

func TestRedis_GetManyOnNoKeysDoesNothing(t *testing.T) {
	tgt, _ := newMiniredis(t)

	values, err := tgt.GetMany(context.Background(), nil, projection.ShapeScalar)

	require.NoError(t, err)
	assert.Empty(t, values)
}

func TestRedis_ReadManyReadsSets(t *testing.T) {
	tgt, server := newMiniredis(t)
	ctx := context.Background()

	sadd(t, server, "a", "1", "2")
	sadd(t, server, "b", "3")

	reads, err := tgt.ReadMany(ctx, []string{"a", "b", "gone"}, projection.ShapeSet)

	require.NoError(t, err)
	require.Len(t, reads, 3)
	assert.True(t, reads[0].Value.Equal(setOf("1", "2")))
	assert.True(t, reads[1].Value.Equal(setOf("3")))
	assert.True(t, reads[2].Value.IsAbsent())
}

func TestRedis_TTL(t *testing.T) {
	tgt, server := newMiniredis(t)
	ctx := context.Background()

	require.NoError(t, server.Set("with-ttl", "v"))
	server.SetTTL("with-ttl", 90*time.Second)
	require.NoError(t, server.Set("no-ttl", "v"))

	t.Run("a key with an expiry reports its remaining lifetime", func(t *testing.T) {
		got, err := tgt.TTL(ctx, "with-ttl")

		require.NoError(t, err)
		require.NotNil(t, got)
		assert.InDelta(t, float64(90*time.Second), float64(*got), float64(time.Second))
	})

	t.Run("a key with no expiry reports nil, which is not the same as missing", func(t *testing.T) {
		got, err := tgt.TTL(ctx, "no-ttl")

		require.NoError(t, err)
		assert.Nil(t, got, "nil means the key will never expire")
	})

	t.Run("a missing key is reported as such", func(t *testing.T) {
		_, err := tgt.TTL(ctx, "gone")

		assert.ErrorIs(t, err, target.ErrNotFound,
			"a key that is gone and a key that never expires are different facts")
	})
}

func TestRedis_ScanIteratesTheKeyspace(t *testing.T) {
	tgt, server := newMiniredis(t)

	const keyCount = 2500
	for i := 0; i < keyCount; i++ {
		require.NoError(t, server.Set("block-"+strconv.Itoa(i), "v"))
	}

	got := collectScan(t, tgt, "*", 100)

	assert.Len(t, got, keyCount, "every key present for the whole scan must appear")
	assert.Len(t, unique(got), keyCount, "the scan must deduplicate within itself")
}

func TestRedis_ScanHonorsAPattern(t *testing.T) {
	tgt, server := newMiniredis(t)
	for _, key := range []string{"kv:a", "kv:b", "other:c"} {
		require.NoError(t, server.Set(key, "v"))
	}

	got := collectScan(t, tgt, "kv:*", 10)

	sort.Strings(got)
	assert.Equal(t, []string{"kv:a", "kv:b"}, got)
}

func TestRedis_ScanMatchesKeysContainingPatternMetacharacters(t *testing.T) {
	// A key is bytes, and Redis lets it contain the very characters MATCH
	// treats as syntax. Escaping is the caller's problem, and a scan that
	// silently returned nothing would look like an empty keyspace.
	tgt, server := newMiniredis(t)

	for _, key := range []string{"weird*key", "weird?key", "weird[key", "normal"} {
		require.NoError(t, server.Set(key, "v"))
	}

	all := collectScan(t, tgt, "*", 10)
	assert.Len(t, all, 4, "every key is reachable with a match-all pattern")

	escaped := collectScan(t, tgt, `weird\*key`, 10)
	assert.Equal(t, []string{"weird*key"}, escaped,
		"an escaped metacharacter matches the literal key")
}

func TestRedis_ScanIsInterruptibleByContext(t *testing.T) {
	// A keyspace of ten million keys must not be a ten-million-key commitment.
	tgt, server := newMiniredis(t)

	for i := 0; i < 500; i++ {
		require.NoError(t, server.Set("k"+strconv.Itoa(i), "v"))
	}

	ctx, cancel := context.WithCancel(context.Background())
	it := tgt.Scan(ctx, "*", 10)
	require.True(t, it.Next(ctx))
	cancel()

	assert.False(t, it.Next(ctx))
	assert.ErrorIs(t, it.Err(), context.Canceled)
	require.NoError(t, it.Close())
}

func TestRedis_ScanOnAnEmptyKeyspaceYieldsNothing(t *testing.T) {
	tgt, _ := newMiniredis(t)

	got := collectScan(t, tgt, "*", 10)

	assert.Empty(t, got)
}

func TestRedis_HealthParsesInfoAndDBSize(t *testing.T) {
	tgt, server := newMiniredis(t)
	ctx := context.Background()

	for i := 0; i < 7; i++ {
		require.NoError(t, server.Set("k"+strconv.Itoa(i), "v"))
	}

	got, err := tgt.Health(ctx)

	require.NoError(t, err)
	assert.True(t, got.Reachable)
	assert.Equal(t, int64(7), got.KeyspaceSize)

	// Version and the eviction counters come from INFO fields miniredis does
	// not emit, so they are asserted against real Redis 6 and 7 in
	// redis_integration_test.go rather than against the fake here.
}

func TestRedis_HealthReportsUnreachableRatherThanGuessing(t *testing.T) {
	// §23 A5: absence of data is not evidence of divergence. A store that
	// cannot be reached must report exactly that, so the sweeper suppresses
	// findings instead of reporting an empty keyspace as mass drift.
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{
		Addr: server.Addr(),
		// No retries and a short timeout: the point is to observe the failure,
		// not to spend five dial attempts logging about it.
		MaxRetries:  -1,
		DialTimeout: 100 * time.Millisecond,
	})
	tgt := target.NewRedisFromClient(client, 0, 0)
	server.Close()

	got, err := tgt.Health(context.Background())

	require.Error(t, err)
	assert.False(t, got.Reachable)
	require.NoError(t, tgt.Close())
}

func TestRedis_TheHookRejectsAWriteEvenWhenCalledDirectly(t *testing.T) {
	// The hook is the production half of the read-only guarantee: there is no
	// test to fail against a live store, so the only useful outcome is that the
	// command does not reach the socket.
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	tgt := target.NewRedisFromClient(client, 0, 0)
	t.Cleanup(func() { require.NoError(t, tgt.Close()) })

	ctx := context.Background()
	require.NoError(t, server.Set("k", "original"))

	writes := []struct {
		name string
		run  func() error
	}{
		{name: "SET", run: func() error { return client.Set(ctx, "k", "hacked", 0).Err() }},
		{name: "DEL", run: func() error { return client.Del(ctx, "k").Err() }},
		{name: "FLUSHDB", run: func() error { return client.FlushDB(ctx).Err() }},
		{name: "SADD", run: func() error { return client.SAdd(ctx, "s", "m").Err() }},
		{name: "EXPIRE", run: func() error { return client.Expire(ctx, "k", time.Hour).Err() }},
		{name: "INCR", run: func() error { return client.Incr(ctx, "n").Err() }},
		{name: "KEYS", run: func() error { return client.Keys(ctx, "*").Err() }},
	}

	for _, w := range writes {
		t.Run(w.name, func(t *testing.T) {
			err := w.run()

			require.ErrorIs(t, err, target.ErrReadOnlyViolation)
			assert.Contains(t, err.Error(), w.name)
		})
	}

	got, err := server.Get("k")
	require.NoError(t, err)
	assert.Equal(t, "original", got, "the store was modified despite the hook")
	assert.False(t, server.Exists("s"))
}

func TestRedis_TheHookRejectsAWriteInsideAPipeline(t *testing.T) {
	// A pipeline sends many commands in one round trip. Checking only the first
	// would let a write ride along behind a read.
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	tgt := target.NewRedisFromClient(client, 0, 0)
	t.Cleanup(func() { require.NoError(t, tgt.Close()) })

	ctx := context.Background()
	require.NoError(t, server.Set("k", "original"))

	pipe := client.Pipeline()
	pipe.Get(ctx, "k")
	pipe.Set(ctx, "k", "hacked", 0)
	_, err := pipe.Exec(ctx)

	require.ErrorIs(t, err, target.ErrReadOnlyViolation)
	got, getErr := server.Get("k")
	require.NoError(t, getErr)
	assert.Equal(t, "original", got)
}

func TestRedis_RecordingTargetSeesTheCommandsTheClientSends(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	inner := target.NewRedisFromClient(client, 0, 0)

	tb := &fakeTB{}
	rec := target.Recording(tb, inner)
	t.Cleanup(func() { require.NoError(t, rec.Close()) })

	ctx := context.Background()
	_, err := rec.Get(ctx, "k", projection.ShapeScalar)
	require.NoError(t, err)
	_, err = rec.Get(ctx, "s", projection.ShapeSet)
	require.NoError(t, err)

	assert.False(t, tb.failed, tb.output())
	assert.Contains(t, rec.Commands(), "GET")
	assert.Contains(t, rec.Commands(), "SMEMBERS")
}

func TestRedis_ClosedTargetRefusesEveryOperation(t *testing.T) {
	tgt, _ := newMiniredis(t)
	ctx := context.Background()
	require.NoError(t, tgt.Close())

	_, err := tgt.Get(ctx, "k", projection.ShapeScalar)
	assert.ErrorIs(t, err, target.ErrClosed)

	_, err = tgt.ReadMany(ctx, []string{"k"}, projection.ShapeScalar)
	assert.ErrorIs(t, err, target.ErrClosed)

	_, err = tgt.TTL(ctx, "k")
	assert.ErrorIs(t, err, target.ErrClosed)

	_, err = tgt.Health(ctx)
	assert.ErrorIs(t, err, target.ErrClosed)

	it := tgt.Scan(ctx, "*", 10)
	assert.False(t, it.Next(ctx))
	assert.ErrorIs(t, it.Err(), target.ErrClosed)

	assert.NoError(t, tgt.Close(), "Close is idempotent")
}

func TestRedis_ConstructorValidatesItsConfiguration(t *testing.T) {
	tests := []struct {
		name     string
		settings map[string]string
	}{
		{name: "no address is rejected", settings: map[string]string{}},
		{name: "a non-numeric batch size is rejected", settings: map[string]string{"addrs": "localhost:6379", "batchSize": "many"}},
		{name: "a negative scan count is rejected", settings: map[string]string{"addrs": "localhost:6379", "scanCount": "-1"}},
		{name: "a non-numeric db is rejected", settings: map[string]string{"addrs": "localhost:6379", "db": "first"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := target.New("redis", target.Config{Settings: tc.settings})

			assert.ErrorIs(t, err, target.ErrBadConfig)
		})
	}
}

func TestRedis_NameIsTheRegistryName(t *testing.T) {
	tgt, _ := newMiniredis(t)

	assert.Equal(t, "redis", tgt.Name())
}

// collectScan drains an iterator and returns every key it yielded.
func collectScan(t *testing.T, tgt target.Target, pattern string, batch int) []string {
	t.Helper()

	ctx := context.Background()
	it := tgt.Scan(ctx, pattern, batch)
	t.Cleanup(func() { require.NoError(t, it.Close()) })

	var got []string
	for it.Next(ctx) {
		got = append(got, it.Keys()...)
	}
	require.NoError(t, it.Err())
	return got
}

func unique(keys []string) []string {
	seen := make(map[string]struct{}, len(keys))
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, k)
	}
	return out
}

// sadd seeds a set, failing the test rather than discarding miniredis's error.
func sadd(t *testing.T, server *miniredis.Miniredis, key string, members ...string) {
	t.Helper()
	_, err := server.SAdd(key, members...)
	require.NoError(t, err)
}

// srem removes set members.
func srem(t *testing.T, server *miniredis.Miniredis, key string, members ...string) {
	t.Helper()
	_, err := server.SRem(key, members...)
	require.NoError(t, err)
}

func setOf(members ...string) event.Value {
	m := make(map[string]struct{}, len(members))
	for _, s := range members {
		m[s] = struct{}{}
	}
	return event.Value{Kind: event.ValueSet, Members: m}
}
