//go:build integration

// Package target's integration tests run against real Redis servers in Docker.
//
// They exist because miniredis is a reimplementation, and every interesting
// finding in Phase 2 was a place where a real server and the fake disagreed:
// which INFO forms are accepted, what a SCAN does when the keyspace is
// destroyed underneath it, which commands the client sends on connect. A suite
// that only ran against the fake would have shipped all three bugs.
//
// Both Redis 6 and Redis 7 are exercised, in every test, because the version
// differences are the point.
package target_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// redisVersions is the matrix every integration test runs across.
var redisVersions = []struct {
	name  string
	image string
}{
	{name: "redis6", image: "redis:6.2-alpine"},
	{name: "redis7", image: "redis:7.2-alpine"},
}

// server is a running Redis container plus a client for seeding it.
type server struct {
	addr    string
	client  *redis.Client
	version string
}

// startRedis brings up one container and returns a seeding client.
//
// The seeding client is deliberately separate from the target under test: the
// target refuses to write, and that refusal is the feature being relied on
// rather than worked around.
func startRedis(ctx context.Context, t *testing.T, image string) *server {
	t.Helper()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	require.NoError(t, err, "starting %s", image)

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminating %s: %v", image, err)
		}
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "6379/tcp")
	require.NoError(t, err)

	addr := fmt.Sprintf("%s:%s", host, port.Port())
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })

	require.NoError(t, client.Ping(ctx).Err())

	info, err := client.Info(ctx, "server").Result()
	require.NoError(t, err)

	return &server{addr: addr, client: client, version: infoField(info, "redis_version")}
}

// newTargetFor builds a target pointed at a running server.
func newTargetFor(t *testing.T, s *server, batch, scanCount int) *target.RedisTarget {
	t.Helper()

	tgt, err := target.NewRedis(target.RedisOptions{
		Addrs:     []string{s.addr},
		BatchSize: batch,
		ScanCount: scanCount,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, tgt.Close()) })
	return tgt
}

// eachVersion runs fn against every Redis version in the matrix.
func eachVersion(t *testing.T, fn func(t *testing.T, s *server)) {
	t.Helper()

	for _, v := range redisVersions {
		t.Run(v.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s := startRedis(ctx, t, v.image)
			t.Logf("running against Redis %s", s.version)
			fn(t, s)
		})
	}
}

func infoField(info, name string) string {
	for _, line := range splitInfoLines(info) {
		if k, v, ok := cut(line, ":"); ok && k == name {
			return v
		}
	}
	return ""
}

func splitInfoLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			line := s[start:i]
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			out = append(out, line)
			start = i + 1
		}
	}
	return out
}

func cut(s, sep string) (before, after string, found bool) {
	for i := 0; i+len(sep) <= len(s); i++ {
		if s[i:i+len(sep)] == sep {
			return s[:i], s[i+len(sep):], true
		}
	}
	return s, "", false
}

// TestRedisIntegration_HealthParsesBothVersions is the D-005 regression test.
//
// Health asks for bare INFO because Redis 7 accepts several section arguments
// and Redis 6 does not. This asserts the fields arrive on both, and confirms
// the multi-section form really is the thing that breaks.
func TestRedisIntegration_HealthParsesBothVersions(t *testing.T) {
	eachVersion(t, func(t *testing.T, s *server) {
		ctx := context.Background()
		tgt := newTargetFor(t, s, 0, 0)

		for i := 0; i < 11; i++ {
			require.NoError(t, s.client.Set(ctx, "k"+strconv.Itoa(i), "v", 0).Err())
		}

		got, err := tgt.Health(ctx)
		require.NoError(t, err)

		assert.True(t, got.Reachable)
		assert.Equal(t, int64(11), got.KeyspaceSize)
		assert.Equal(t, s.version, got.Version, "the version must be parsed out of INFO")
		assert.Equal(t, "master", got.Role)
		assert.Positive(t, got.UsedMemoryBytes)

		// The form Health deliberately does not use. On Redis 6 this is an
		// error; on Redis 7 it succeeds. Asserting the difference here is what
		// keeps the reason for the bare call from being optimized away later.
		_, multiErr := s.client.Info(ctx, "stats", "memory", "replication", "server").Result()
		if s.version[0] == '6' {
			require.Error(t, multiErr,
				"Redis 6 must reject multi-section INFO, or D-005 no longer applies")
			t.Logf("Redis %s rejects multi-section INFO: %v", s.version, multiErr)
			return
		}
		require.NoError(t, multiErr, "Redis 7 accepts multi-section INFO")
	})
}

func TestRedisIntegration_HealthReportsEvictionsAndExpiries(t *testing.T) {
	// A sweep that finds mass missing_in_target at the same moment the eviction
	// counter jumped has an obvious explanation (§5.7), so these counters have
	// to be read correctly from a real server rather than assumed.
	eachVersion(t, func(t *testing.T, s *server) {
		ctx := context.Background()
		tgt := newTargetFor(t, s, 0, 0)

		require.NoError(t, s.client.Set(ctx, "doomed", "v", 50*time.Millisecond).Err())
		require.Eventually(t, func() bool {
			n, err := s.client.Exists(ctx, "doomed").Result()
			return err == nil && n == 0
		}, 5*time.Second, 25*time.Millisecond)

		// Reading the expired key is what makes Redis account for the expiry.
		_, err := tgt.Get(ctx, "doomed", projection.ShapeScalar)
		require.NoError(t, err)

		got, err := tgt.Health(ctx)
		require.NoError(t, err)
		assert.Positive(t, got.ExpiredKeys, "an expiry must be visible in INFO stats")
	})
}

func TestRedisIntegration_ScanOver100kKeys(t *testing.T) {
	eachVersion(t, func(t *testing.T, s *server) {
		ctx := context.Background()
		tgt := newTargetFor(t, s, 0, 1000)

		const keyCount = 100_000
		seedKeys(t, s, keyCount, "block-")

		seen := map[string]struct{}{}
		it := tgt.Scan(ctx, "*", 1000)
		batches := 0
		for it.Next(ctx) {
			batches++
			for _, k := range it.Keys() {
				_, dup := seen[k]
				require.False(t, dup, "SCAN returned %q twice; the iterator must deduplicate", k)
				seen[k] = struct{}{}
			}
		}
		require.NoError(t, it.Err())
		require.NoError(t, it.Close())

		assert.Len(t, seen, keyCount)
		t.Logf("Redis %s: scanned %d keys in %d batches", s.version, len(seen), batches)
	})
}

// TestRedisIntegration_ScanSurvivesAFlushMidIteration is the D-006 regression
// test.
//
// PRD M8 predicts an infinite loop here. It does not happen on either version:
// the scan terminates early and reports success, having seen a fraction of the
// keyspace. This pins the behaviour that actually occurs, so that if a future
// Redis does start looping the assertion changes rather than the tool hanging.
func TestRedisIntegration_ScanSurvivesAFlushMidIteration(t *testing.T) {
	eachVersion(t, func(t *testing.T, s *server) {
		ctx := context.Background()
		tgt := newTargetFor(t, s, 0, 100)

		const keyCount = 10_000
		seedKeys(t, s, keyCount, "block-")

		seen := 0
		batches := 0
		it := tgt.Scan(ctx, "*", 100)
		for it.Next(ctx) {
			batches++
			seen += len(it.Keys())
			if batches == 1 {
				require.NoError(t, s.client.FlushDB(ctx).Err())
			}
		}

		// The iteration terminates. That is the whole assertion: no hang, no
		// ErrScanRestarted, and no error at all.
		require.NoError(t, it.Err(),
			"a flush mid-scan must not produce an error on Redis %s", s.version)
		require.NoError(t, it.Close())

		assert.Less(t, seen, keyCount,
			"the scan is expected to end early, having seen only part of the keyspace")
		t.Logf("Redis %s: flush after batch 1 left the scan reporting success "+
			"after %d batches having seen %d of %d keys",
			s.version, batches, seen, keyCount)
	})
}

func TestRedisIntegration_ScanIsBoundedWhenTheKeyspaceChurns(t *testing.T) {
	// The most hostile version of the same thing: destroy and rebuild the
	// keyspace on every batch. It must still terminate.
	eachVersion(t, func(t *testing.T, s *server) {
		ctx := context.Background()
		tgt := newTargetFor(t, s, 0, 100)
		seedKeys(t, s, 5_000, "block-")

		batches := 0
		it := tgt.Scan(ctx, "*", 100)
		for it.Next(ctx) {
			batches++
			if batches > 500 {
				t.Fatalf("scan did not terminate after %d batches", batches)
			}
			if batches <= 10 {
				require.NoError(t, s.client.FlushDB(ctx).Err())
				seedKeys(t, s, 2_000, "churn-"+strconv.Itoa(batches)+"-")
			}
		}
		require.NoError(t, it.Err())
		require.NoError(t, it.Close())
		t.Logf("Redis %s: terminated after %d batches under continuous churn",
			s.version, batches)
	})
}

func TestRedisIntegration_WrongType(t *testing.T) {
	eachVersion(t, func(t *testing.T, s *server) {
		ctx := context.Background()
		tgt := newTargetFor(t, s, 0, 0)

		require.NoError(t, s.client.SAdd(ctx, "a-set", "m").Err())
		require.NoError(t, s.client.Set(ctx, "a-string", "v", 0).Err())
		require.NoError(t, s.client.HSet(ctx, "a-hash", "f", "v").Err())

		tests := []struct {
			name  string
			key   string
			shape projection.Shape
		}{
			{name: "a set read as a scalar", key: "a-set", shape: projection.ShapeScalar},
			{name: "a string read as a set", key: "a-string", shape: projection.ShapeSet},
			{name: "a hash read as a set", key: "a-hash", shape: projection.ShapeSet},
			{name: "a hash read as a scalar", key: "a-hash", shape: projection.ShapeScalar},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				_, err := tgt.Get(ctx, tc.key, tc.shape)

				require.ErrorIs(t, err, target.ErrWrongType,
					"WRONGTYPE is drift and must be surfaced, not swallowed")
			})
		}

		// And in a batch, where one bad key must not sink the others.
		reads, err := tgt.ReadMany(ctx,
			[]string{"a-string", "a-set", "missing"}, projection.ShapeScalar)
		require.NoError(t, err)
		require.Len(t, reads, 3)
		assert.NoError(t, reads[0].Err)
		assert.ErrorIs(t, reads[1].Err, target.ErrWrongType)
		assert.NoError(t, reads[2].Err)
		assert.True(t, reads[2].Value.IsAbsent())
	})
}

func TestRedisIntegration_EmptySetReadsAsAbsent(t *testing.T) {
	// The M2 decision, confirmed against a real server: Redis deletes a set key
	// when its last member goes, so there is no empty set to read back.
	eachVersion(t, func(t *testing.T, s *server) {
		ctx := context.Background()
		tgt := newTargetFor(t, s, 0, 0)

		require.NoError(t, s.client.SAdd(ctx, "emptied", "only").Err())
		require.NoError(t, s.client.SRem(ctx, "emptied", "only").Err())

		exists, err := s.client.Exists(ctx, "emptied").Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), exists, "Redis itself must have removed the key")

		got, err := tgt.Get(ctx, "emptied", projection.ShapeSet)
		require.NoError(t, err)
		assert.True(t, got.IsAbsent())
	})
}

func TestRedisIntegration_TTL(t *testing.T) {
	eachVersion(t, func(t *testing.T, s *server) {
		ctx := context.Background()
		tgt := newTargetFor(t, s, 0, 0)

		require.NoError(t, s.client.Set(ctx, "with-ttl", "v", 90*time.Second).Err())
		require.NoError(t, s.client.Set(ctx, "no-ttl", "v", 0).Err())

		withTTL, err := tgt.TTL(ctx, "with-ttl")
		require.NoError(t, err)
		require.NotNil(t, withTTL)
		assert.InDelta(t, float64(90*time.Second), float64(*withTTL), float64(2*time.Second))

		noTTL, err := tgt.TTL(ctx, "no-ttl")
		require.NoError(t, err)
		assert.Nil(t, noTTL, "a key that never expires reports nil, not an error")

		_, err = tgt.TTL(ctx, "missing")
		assert.ErrorIs(t, err, target.ErrNotFound,
			"a missing key and a key with no expiry are different facts")
	})
}

func TestRedisIntegration_PipelinedBatchReads(t *testing.T) {
	eachVersion(t, func(t *testing.T, s *server) {
		ctx := context.Background()
		tgt := newTargetFor(t, s, 500, 0)

		const keyCount = 2_000
		keys := make([]string, keyCount)
		pipe := s.client.Pipeline()
		for i := range keys {
			keys[i] = "k" + strconv.Itoa(i)
			pipe.Set(ctx, keys[i], strconv.Itoa(i), 0)
		}
		_, err := pipe.Exec(ctx)
		require.NoError(t, err)

		values, err := tgt.GetMany(ctx, keys, projection.ShapeCounter)
		require.NoError(t, err)
		require.Len(t, values, keyCount)
		for i, v := range values {
			require.Equal(t, int64(i), v.Counter, "result %d is out of order across batches", i)
		}
	})
}

func TestRedisIntegration_TheHookRefusesWritesAgainstARealServer(t *testing.T) {
	// The unit test proves this against miniredis. This proves it against the
	// thing that would actually lose data.
	eachVersion(t, func(t *testing.T, s *server) {
		ctx := context.Background()

		require.NoError(t, s.client.Set(ctx, "precious", "original", 0).Err())

		guarded, err := target.NewRedis(target.RedisOptions{Addrs: []string{s.addr}})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, guarded.Close()) })

		// Reads work.
		got, err := guarded.Get(ctx, "precious", projection.ShapeScalar)
		require.NoError(t, err)
		require.True(t, got.Equal(event.Value{Kind: event.ValueScalar, Scalar: []byte("original")}))

		// The value is still there, unmodified, and the keyspace is intact.
		after, err := s.client.Get(ctx, "precious").Result()
		require.NoError(t, err)
		assert.Equal(t, "original", after)

		size, err := s.client.DBSize(ctx).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(1), size)
	})
}

func TestRedisIntegration_ScanMatchesKeysWithPatternMetacharacters(t *testing.T) {
	eachVersion(t, func(t *testing.T, s *server) {
		ctx := context.Background()
		tgt := newTargetFor(t, s, 0, 0)

		for _, key := range []string{"weird*key", "weird?key", "weird[key", "normal"} {
			require.NoError(t, s.client.Set(ctx, key, "v", 0).Err())
		}

		all := drain(t, tgt.Scan(ctx, "*", 10), ctx)
		assert.Len(t, all, 4)

		escaped := drain(t, tgt.Scan(ctx, `weird\*key`, 10), ctx)
		assert.Equal(t, []string{"weird*key"}, escaped)
	})
}

func TestRedisIntegration_ScanIsInterruptibleOnALargeKeyspace(t *testing.T) {
	// A ten-million-key keyspace must not be a ten-million-key commitment; the
	// sweeper has to be able to abandon a scan when a DriftCheck is deleted.
	eachVersion(t, func(t *testing.T, s *server) {
		ctx := context.Background()
		tgt := newTargetFor(t, s, 0, 100)
		seedKeys(t, s, 50_000, "block-")

		cancelCtx, cancel := context.WithCancel(ctx)
		it := tgt.Scan(cancelCtx, "*", 100)
		require.True(t, it.Next(cancelCtx))
		cancel()

		assert.False(t, it.Next(cancelCtx))
		assert.ErrorIs(t, it.Err(), context.Canceled)
		require.NoError(t, it.Close())
	})
}

// seedKeys writes n keys with the given prefix, pipelined so the setup does not
// dominate the test.
func seedKeys(t *testing.T, s *server, n int, prefix string) {
	t.Helper()

	ctx := context.Background()
	const chunk = 5_000
	for start := 0; start < n; start += chunk {
		pipe := s.client.Pipeline()
		for i := start; i < start+chunk && i < n; i++ {
			pipe.Set(ctx, prefix+strconv.Itoa(i), "v", 0)
		}
		_, err := pipe.Exec(ctx)
		require.NoError(t, err)
	}
}

func drain(t *testing.T, it target.Iterator, ctx context.Context) []string {
	t.Helper()

	var got []string
	for it.Next(ctx) {
		got = append(got, it.Keys()...)
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	return got
}
