//go:build soak

// Package soak holds the long-running steady-state soak test (§16.7).
package soak

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"
	"github.com/shirou/gopsutil/v4/process"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/metrics"
	"github.com/nabrahma/driftwatch/pkg/source"
)

// The soak is the only test in the repository that runs on real time.
//
// Everything else uses the injected clock, because a suite that sleeps is slow,
// then flaky, then unrun. Here elapsed time is the subject: the question is what
// happens to memory, goroutines and correctness over an hour of continuous
// operation, and there is no way to fake an hour of allocator behavior.
//
// What it is really testing is not "does it crash". §16.7's most interesting
// assertion is the one at the halfway mark: a deliberate drop, detected and then
// resolved, thirty minutes in. A process that survives an hour and has quietly
// stopped detecting anything has failed in the way that matters most, and
// breaking something late is the only way to catch that.

// Config is read from the environment so `make soak DURATION=5m` can shorten a
// run for a smoke test without editing anything.
type Config struct {
	Duration   time.Duration
	Publishers int
	Rate       int
	Keys       int
	// SampleInterval is how often the assertions run. §16.7 says every minute.
	SampleInterval time.Duration
	// WarmupFraction is the share of the run excluded from the memory and
	// goroutine trend assertions. §16.7 allows warmup explicitly: the oracle is
	// still filling for the first several minutes, and its growth over that
	// period is the tool working rather than a leak.
	WarmupFraction float64
	ArtifactDir    string
}

func configFromEnv(t *testing.T) Config {
	t.Helper()

	cfg := Config{
		Duration:       envDuration(t, "DRIFTWATCH_SOAK_DURATION", time.Hour),
		Publishers:     envInt(t, "DRIFTWATCH_SOAK_PUBLISHERS", 3),
		Rate:           envInt(t, "DRIFTWATCH_SOAK_RATE", 5000),
		Keys:           envInt(t, "DRIFTWATCH_SOAK_KEYS", 500_000),
		SampleInterval: envDuration(t, "DRIFTWATCH_SOAK_SAMPLE", time.Minute),
		WarmupFraction: 0.25,
	}

	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	cfg.ArtifactDir = filepath.Join(root, "docs", "evidence")

	// A short run still has to sample often enough to say anything. Without
	// this, `make soak DURATION=2m` would take one sample and assert nothing.
	if cfg.Duration < 10*cfg.SampleInterval {
		cfg.SampleInterval = maxDuration(cfg.Duration/10, time.Second)
	}
	return cfg
}

// ringSize mirrors policy.ringSize in soakSpec. Named here because the warmup
// calculation depends on it and a spec edit that left this behind would make
// the memory assertion silently start measuring the wrong window.
const ringSize = 16

// ringFillTime is how long until every key's event ring holds ringSize events.
//
// Each key is touched once per keys/rate seconds, so a ring fills after
// ringSize of those. Until then the oracle's memory is still growing by design.
func (c Config) ringFillTime() time.Duration {
	if c.Rate <= 0 {
		return 0
	}
	perKey := time.Duration(float64(c.Keys) / float64(c.Rate) * float64(time.Second))
	return time.Duration(ringSize) * perKey
}

// sweepInterval and maxWindow mirror soakSpec's policy. The midpoint fault's
// observability depends on both, so they are named rather than buried.
const (
	sweepInterval = 30 * time.Second
	maxWindow     = 60 * time.Second
)

// touchInterval is how often the workload revisits a given key.
func (c Config) touchInterval() time.Duration {
	if c.Rate <= 0 {
		return 0
	}
	return time.Duration(float64(c.Keys) / float64(c.Rate) * float64(time.Second))
}

// requireFaultIsObservable fails early when the parameters make the midpoint
// drop impossible to catch.
//
// The fault removes a member from a set. The workload then puts it back the next
// time it touches that key — so if keys are revisited faster than a sweep plus a
// settlement window, the store has healed before driftwatch can confirm anything
// and the run reports "never detected" for a reason that has nothing to do with
// driftwatch.
//
// That is not hypothetical: at 20,000 keys and 5,000 events/sec each key comes
// round every 4 seconds, against a 30-second sweep. Nothing was ever going to be
// caught, and the failure looked exactly like a detection bug. §16.7's own
// parameters give 100 seconds per key, which is why they work.
func (c Config) requireFaultIsObservable(t *testing.T) {
	t.Helper()

	needed := sweepInterval + maxWindow
	touch := c.touchInterval()

	require.Greater(t, touch, needed,
		"these parameters cannot observe the midpoint fault: %d keys at %d "+
			"events/sec revisits each key every %s, but confirming a finding "+
			"takes up to a %s sweep plus a %s settlement window. The removed "+
			"member would be written back before driftwatch could confirm it "+
			"was ever gone.\n\nRaise DRIFTWATCH_SOAK_KEYS or lower "+
			"DRIFTWATCH_SOAK_RATE so that keys/rate > %s.",
		c.Keys, c.Rate, touch.Round(time.Second), sweepInterval, maxWindow,
		needed)
}

func TestSoak(t *testing.T) {
	cfg := configFromEnv(t)
	cfg.requireFaultIsObservable(t)

	t.Logf("soak: %s, %d publishers, %d events/sec, %d keys, sampling every %s",
		cfg.Duration, cfg.Publishers, cfg.Rate, cfg.Keys, cfg.SampleInterval)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration+10*time.Minute)
	defer cancel()

	rig := newRig(ctx, t, cfg)
	defer rig.close()

	rig.profile(t, "start")

	// The publisher and the materializer run for the whole duration. Both are
	// in-process: §16.7 asks for a real Redis and a real materializer, and this
	// is a real materializer — a separate goroutine that only ever learns of an
	// event by receiving it, writing to the same Redis driftwatch reads. What it
	// deliberately is not is the ZMQ transport, which the e2e suite covers and
	// which would turn an hour of soak into an hour of measuring a socket.
	runCtx, stopWorkload := context.WithCancel(ctx)
	defer stopWorkload()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); rig.runWorkload(runCtx) }()

	samples := rig.sample(runCtx, t, cfg)

	stopWorkload()
	wg.Wait()

	rig.profile(t, "end")
	rig.assertAll(t, cfg, samples)
	rig.writeReport(t, cfg, samples)
}

// ---------------------------------------------------------------------------
// The rig.
// ---------------------------------------------------------------------------

type rig struct {
	cfg      Config
	check    *check.Check
	registry *prometheus.Registry
	rdb      *redis.Client
	proc     *process.Process

	// applied is the materializer's position in the stream, so the injected
	// drop can be expressed as a range of events rather than a wall-clock
	// moment nothing else can see.
	applied atomic.Uint64
	// dropFrom and dropTo bound the deliberate drop, read on every event.
	dropFrom atomic.Uint64
	dropTo   atomic.Uint64
	// dropped records the keys the materializer skipped, so they can be
	// repaired afterwards and the resolution asserted.
	dropMu  sync.Mutex
	dropped map[string]string

	cancelRun context.CancelFunc
	runDone   chan error
}

func newRig(ctx context.Context, t *testing.T, cfg Config) *rig {
	t.Helper()

	addr, rdb := startRedis(ctx, t)

	registry := prometheus.NewRegistry()
	met := metrics.New(metrics.Options{Registry: registry})

	spec, err := check.Load(strings.NewReader(fmt.Sprintf(soakSpec, addr, cfg.Keys*2)))
	require.NoError(t, err)

	c, err := check.New(spec, check.Deps{Clock: clock.Real(), Metrics: met})
	require.NoError(t, err)

	runCtx, cancelRun := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Run(runCtx) }()

	self, err := process.NewProcess(int32(os.Getpid())) //nolint:gosec // a pid
	require.NoError(t, err)

	r := &rig{
		cfg: cfg, check: c, registry: registry, rdb: rdb, proc: self,
		dropped: map[string]string{}, cancelRun: cancelRun, runDone: done,
	}

	require.NoError(t, os.MkdirAll(cfg.ArtifactDir, 0o750))
	return r
}

func (r *rig) close() {
	r.cancelRun()
	<-r.runDone
	_ = r.check.Close() //nolint:errcheck // the run is over; a close error changes nothing
}

// soakSpec is the check under soak: a memory source fed by hand, a real Redis
// target, and the §10.1 defaults everywhere they are not the subject.
const soakSpec = `
name: soak
namespace: soak
source:
  type: memory
codec:
  type: json
projection:
  type: keysetOwnership
  keyTemplate: "{{.Key}}"
  memberTemplate: "{{.Member}}"
target:
  type: redis
  redis:
    addr: %q
    keyPattern: "block:*"
    readBatchSize: 1000
    scanCount: 2000
    poolSize: 16
policy:
  settlementWindow:
    mode: adaptive
    static: 5s
    min: 2s
    max: 60s
    safetyFactor: "3.0"
  sweepInterval: 30s
  extraScanInterval: 5m
  bootstrap: Wait
  expiryPolicy: Strict
  requirePrimary: false
  maxTrackedKeys: %d
  ringSize: 16
`

func startRedis(ctx context.Context, t *testing.T) (string, *redis.Client) {
	t.Helper()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7.4-alpine",
			ExposedPorts: []string{"6379/tcp"},
			// No persistence and no eviction. An hour of writes with an AOF
			// would measure the disk, and an eviction policy would make the
			// store legitimately lose keys — which driftwatch would correctly
			// report as drift, failing the run for something that is not a bug.
			Cmd: []string{
				"redis-server", "--save", "", "--appendonly", "no",
				"--maxmemory-policy", "noeviction",
			},
			WaitingFor: wait.ForListeningPort("6379/tcp").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	require.NoError(t, err, "starting redis")

	t.Cleanup(func() {
		if termErr := testcontainers.TerminateContainer(container); termErr != nil {
			t.Logf("terminating redis: %v", termErr)
		}
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "6379/tcp")
	require.NoError(t, err)

	addr := fmt.Sprintf("%s:%s", host, port.Port())
	rdb := redis.NewClient(&redis.Options{Addr: addr, PoolSize: 16})
	t.Cleanup(func() { _ = rdb.Close() }) //nolint:errcheck // the process is ending

	require.NoError(t, rdb.Ping(ctx).Err())
	return addr, rdb
}

// ---------------------------------------------------------------------------
// The workload.
// ---------------------------------------------------------------------------

// runWorkload emits events into driftwatch and, separately, into Redis.
//
// The two paths are independent on purpose: driftwatch's oracle is built from
// the event, Redis is written by the materializer from the same event, and
// nothing carries state between them. That independence is the entire premise
// of the tool, and a soak that fed both from one write would measure nothing.
func (r *rig) runWorkload(ctx context.Context) {
	const batchInterval = 20 * time.Millisecond

	perBatch := maxInt(1, r.cfg.Rate/int(time.Second/batchInterval))

	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()

	seq := make([]uint64, r.cfg.Publishers)
	var emitted uint64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		pipe := r.rdb.Pipeline()

		for i := 0; i < perBatch; i++ {
			which := int(emitted % uint64(r.cfg.Publishers)) //nolint:gosec // bounded
			seq[which]++
			emitted++

			publisher := "replica-" + strconv.Itoa(which)
			key := "block:" + strconv.Itoa(int(emitted%uint64(r.cfg.Keys))) //nolint:gosec // bounded

			payload := fmt.Sprintf(
				`{"publisher":%q,"epoch":1,"seq":%d,"op":"add","key":%q,"member":%q,"ts":%q}`,
				publisher, seq[which], key, publisher,
				time.Now().UTC().Format(time.RFC3339Nano))

			r.check.Ingest(source.RawMessage{
				Topic:      "kv-events",
				Payload:    []byte(payload),
				ObservedAt: time.Now(),
			})

			position := r.applied.Add(1)

			if from := r.dropFrom.Load(); from > 0 && position >= from && position <= r.dropTo.Load() {
				// The materializer misses this event, and the store ends up in
				// the state it would be in had that add never been applied.
				//
				// That is a removal rather than a skip, and the difference is
				// the whole reason this took two attempts to get right. SADD is
				// idempotent, and this workload cycles each key through the same
				// three publishers forever — so by the midpoint every member is
				// already in every set, and simply not sending one more add
				// leaves the store *correct*. driftwatch reported nothing, which
				// was the right answer to the wrong question: the fault had not
				// actually changed anything.
				//
				// Removing the member produces a genuine member_mismatch: the
				// oracle expects {replica-0, replica-1, replica-2}, the store
				// holds two of them. That is what a materializer that lost this
				// event would have left behind if it had lost every earlier one
				// for the same pair too.
				r.dropMu.Lock()
				r.dropped[key] = publisher
				r.dropMu.Unlock()

				pipe.SRem(ctx, key, publisher)
				continue
			}

			pipe.SAdd(ctx, key, publisher)
		}

		if pipe.Len() == 0 {
			continue
		}
		if _, err := pipe.Exec(ctx); err != nil && ctx.Err() == nil {
			// Not fatal, and not silent: a Redis that rejects a write makes the
			// store genuinely diverge, and the run should report that rather
			// than pretend the write happened.
			fmt.Printf("soak: materializer write failed: %v\n", err)
		}
	}
}

// injectDrop makes the materializer skip the next n events.
func (r *rig) injectDrop(n uint64) (from, to uint64) {
	from = r.applied.Load() + 1
	to = from + n - 1

	// `to` first: the workload reads `from` to decide whether to check `to` at
	// all, so setting `from` last means it can never see a range with an
	// unwritten upper bound.
	r.dropTo.Store(to)
	r.dropFrom.Store(from)
	return from, to
}

// repairDrop writes the skipped keys, as a materializer catching up would.
func (r *rig) repairDrop(ctx context.Context) int {
	r.dropFrom.Store(0)
	r.dropTo.Store(0)

	r.dropMu.Lock()
	pending := r.dropped
	r.dropped = map[string]string{}
	r.dropMu.Unlock()

	pipe := r.rdb.Pipeline()
	for key, member := range pending {
		pipe.SAdd(ctx, key, member)
	}
	if pipe.Len() > 0 {
		_, _ = pipe.Exec(ctx) //nolint:errcheck // a failed repair shows up as unresolved drift, which is the assertion
	}
	return len(pending)
}

// ---------------------------------------------------------------------------
// Sampling.
// ---------------------------------------------------------------------------

// Sample is one interval's observation.
type Sample struct {
	At            time.Duration
	DivergentKeys int
	SuspectKeys   int
	CoverageRatio float64
	TrackedKeys   int
	Goroutines    int
	RSSBytes      uint64
	HeapBytes     uint64
	Panics        int
	SweepP99      time.Duration
	EventsApplied uint64
	EventsDropped uint64
}

// sample runs for the configured duration, asserting as it goes.
//
// The per-instant assertions run inside the loop rather than only at the end,
// because a soak that fails at minute three and says so at minute sixty has
// wasted an hour. The trend assertions — memory and goroutines — need the whole
// series to mean anything and are checked afterwards.
//
//nolint:gocyclo // one branch per phase of the run; splitting it hides the order
func (r *rig) sample(ctx context.Context, t *testing.T, cfg Config) []Sample {
	t.Helper()

	start := time.Now()
	deadline := start.Add(cfg.Duration)
	midpoint := start.Add(cfg.Duration / 2)

	ticker := time.NewTicker(cfg.SampleInterval)
	defer ticker.Stop()

	var (
		samples     []Sample
		dropDone    bool
		detectedAt  time.Duration
		repairedAt  time.Duration
		resolvedAt  time.Duration
		profiledMid bool
	)

	const dropSize = 10

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return samples
		case <-ticker.C:
		}

		elapsed := time.Since(start)
		s := r.observe(elapsed)
		samples = append(samples, s)

		t.Logf("t=%-6s drift=%-4d suspect=%-4d coverage=%.4f keys=%-7d "+
			"goroutines=%-4d rss=%dMiB p99sweep=%s",
			elapsed.Round(time.Second), s.DivergentKeys, s.SuspectKeys,
			s.CoverageRatio, s.TrackedKeys, s.Goroutines, s.RSSBytes>>20,
			s.SweepP99.Round(time.Millisecond))

		// §16.7's midpoint fault. Everything before it must show zero drift;
		// everything after it must show the drop found and then cleared.
		switch {
		case !dropDone && time.Now().After(midpoint):
			from, to := r.injectDrop(dropSize)
			dropDone = true
			t.Logf("t=%s injected a %d-event drop at positions %d-%d",
				elapsed.Round(time.Second), dropSize, from, to)

		case dropDone && detectedAt == 0 && s.DivergentKeys > 0:
			detectedAt = elapsed
			t.Logf("t=%s the drop was detected: %d confirmed divergent keys",
				elapsed.Round(time.Second), s.DivergentKeys)

			repaired := r.repairDrop(ctx)
			repairedAt = elapsed
			t.Logf("t=%s repaired %d keys, as a materializer catching up would",
				elapsed.Round(time.Second), repaired)

		case repairedAt > 0 && resolvedAt == 0 && s.DivergentKeys == 0:
			resolvedAt = elapsed
			t.Logf("t=%s the drift resolved", elapsed.Round(time.Second))
		}

		if !profiledMid && time.Now().After(midpoint) {
			r.profile(t, "middle")
			profiledMid = true
		}

		require.Zero(t, s.Panics, "a panic at t=%s: §16.7 allows none", elapsed)

		if !dropDone {
			require.Zero(t, s.DivergentKeys,
				"t=%s: divergence before the deliberate drop. This is S2, the "+
					"headline claim: no false positives in steady state.", elapsed)
		}
	}

	require.True(t, dropDone, "the run was too short to reach the midpoint fault")
	require.NotZero(t, detectedAt,
		"the deliberate %d-event drop was never detected. Surviving an hour "+
			"while having silently stopped working is the failure that matters "+
			"most, and this assertion is the only thing that catches it.", dropSize)
	require.NotZero(t, resolvedAt,
		"the drop was detected at t=%s and repaired at t=%s, and the divergence "+
			"never cleared", detectedAt, repairedAt)

	t.Logf("midpoint fault: detected at t=%s, repaired at t=%s, resolved at t=%s",
		detectedAt.Round(time.Second), repairedAt.Round(time.Second),
		resolvedAt.Round(time.Second))

	return samples
}

func (r *rig) observe(at time.Duration) Sample {
	st := r.check.Status()

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	var rss uint64
	if info, err := r.proc.MemoryInfo(); err == nil {
		rss = info.RSS
	}

	return Sample{
		At:            at,
		DivergentKeys: st.DivergentKeys,
		SuspectKeys:   st.SuspectDivergentKeys,
		CoverageRatio: st.CoverageRatio,
		TrackedKeys:   st.TrackedKeys,
		Goroutines:    runtime.NumGoroutine(),
		RSSBytes:      rss,
		HeapBytes:     mem.HeapAlloc,
		Panics:        r.counter("driftwatch_panics_total"),
		SweepP99:      r.sweepP99(),
		EventsApplied: st.EventsApplied,
		EventsDropped: st.EventsDropped,
	}
}

// ---------------------------------------------------------------------------
// Assertions over the whole series.
// ---------------------------------------------------------------------------

func (r *rig) assertAll(t *testing.T, cfg Config, samples []Sample) {
	t.Helper()

	require.NotEmpty(t, samples, "no samples were taken")

	// Warmup is whichever is longer: the configured fraction, or the time it
	// takes every key's event ring to fill.
	//
	// The second one is not obvious and is what made this assertion fail on its
	// first three runs. The oracle keeps the last ringSize events per key for
	// `driftwatch explain`, so its memory does not level off when the key count
	// does — it levels off when the *rings* do, which takes
	// ringSize × (keys / rate) seconds. At §16.7's parameters that is 16 × 500k
	// / 5k = 1,600s, or 27 minutes of a 60-minute run.
	//
	// Measuring RSS growth before that point measures the rings filling, which
	// is the tool working exactly as designed, and calling it a leak.
	ringFill := cfg.ringFillTime()
	warmupFor := maxDuration(
		time.Duration(float64(cfg.Duration)*cfg.WarmupFraction), ringFill)

	warmup := 0
	for warmup < len(samples) && samples[warmup].At < warmupFor {
		warmup++
	}

	require.Less(t, warmup, len(samples),
		"the whole run was warmup: every key's event ring takes %s to fill at "+
			"%d keys and %d events/sec, and the run was %s. Either lengthen it "+
			"or lower DRIFTWATCH_SOAK_KEYS.", ringFill, cfg.Keys, cfg.Rate, cfg.Duration)

	steady := samples[warmup:]

	t.Logf("asserting over %d steady-state samples (%d discarded as warmup; "+
		"rings fill in %s)", len(steady), warmup, ringFill.Round(time.Second))

	// RSS growth over the steady window. §16.7 allows 5%.
	firstRSS, lastRSS := steady[0].RSSBytes, steady[len(steady)-1].RSSBytes
	if firstRSS > 0 {
		growth := (float64(lastRSS) - float64(firstRSS)) / float64(firstRSS)
		t.Logf("RSS: %d MiB -> %d MiB over the steady window (%+.1f%%)",
			firstRSS>>20, lastRSS>>20, growth*100)

		require.Less(t, growth, 0.05,
			"RSS grew %.1f%% over the steady-state window, which is a leak: "+
				"the oracle is bounded and every queue has a cap, so nothing "+
				"here should still be growing after warmup", growth*100)
	}

	// Goroutines stable within ±5 after warmup.
	minG, maxG := steady[0].Goroutines, steady[0].Goroutines
	for _, s := range steady {
		minG = minInt(minG, s.Goroutines)
		maxG = maxInt(maxG, s.Goroutines)
	}
	t.Logf("goroutines: %d to %d over the steady window", minG, maxG)

	require.LessOrEqual(t, maxG-minG, 5,
		"the goroutine count moved by %d over the steady window (%d to %d). "+
			"driftwatch starts a fixed set at construction and none per event, "+
			"so a moving count is a goroutine per sweep or per probe that is "+
			"not being reaped.", maxG-minG, minG, maxG)

	for _, s := range steady {
		require.Greater(t, s.CoverageRatio, 0.95,
			"t=%s: coverage fell to %.4f. Below this the divergence count stops "+
				"being a statement about the store and becomes a statement "+
				"about a sample of it.", s.At, s.CoverageRatio)

		require.Less(t, s.SweepP99, sweepInterval,
			"t=%s: sweep p99 reached %s against a %s interval. Past that, "+
				"sweeps start being skipped and detection latency quietly "+
				"becomes the sweep duration.", s.At, s.SweepP99, sweepInterval)

		require.Zero(t, s.Panics, "t=%s: a panic was recorded", s.At)
	}
}

// ---------------------------------------------------------------------------
// Metrics and profiles.
// ---------------------------------------------------------------------------

func (r *rig) counter(name string) int {
	families, err := r.registry.Gather()
	if err != nil {
		return 0
	}

	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		total := 0.0
		for _, m := range mf.GetMetric() {
			if m.GetCounter() != nil {
				total += m.GetCounter().GetValue()
			}
		}
		return int(total)
	}
	return 0
}

// sweepP99 estimates the 99th percentile from the histogram's buckets.
func (r *rig) sweepP99() time.Duration {
	families, err := r.registry.Gather()
	if err != nil {
		return 0
	}

	for _, mf := range families {
		if mf.GetName() != "driftwatch_sweep_duration_seconds" {
			continue
		}

		var merged []*dto.Bucket
		var total uint64
		for _, m := range mf.GetMetric() {
			h := m.GetHistogram()
			if h == nil {
				continue
			}
			total += h.GetSampleCount()
			merged = mergeBuckets(merged, h.GetBucket())
		}
		if total == 0 {
			return 0
		}

		want := uint64(float64(total) * 0.99)
		for _, b := range merged {
			if b.GetCumulativeCount() >= want {
				return time.Duration(b.GetUpperBound() * float64(time.Second))
			}
		}
	}
	return 0
}

func mergeBuckets(into, from []*dto.Bucket) []*dto.Bucket {
	if len(into) == 0 {
		out := make([]*dto.Bucket, len(from))
		copy(out, from)
		return out
	}
	for i := range into {
		if i < len(from) {
			c := into[i].GetCumulativeCount() + from[i].GetCumulativeCount()
			into[i].CumulativeCount = &c
		}
	}
	return into
}

// profile dumps heap and goroutine profiles. §16.7 asks for start, middle and
// end, which is what makes a leak attributable rather than merely visible.
func (r *rig) profile(t *testing.T, when string) {
	t.Helper()

	runtime.GC()

	for _, name := range []string{"heap", "goroutine"} {
		path := filepath.Join(r.cfg.ArtifactDir,
			fmt.Sprintf("S2-soak-%s-%s.pprof", name, when))

		f, err := os.Create(path) //nolint:gosec // a path this test built
		if err != nil {
			t.Logf("soak: creating %s: %v", path, err)
			continue
		}

		if err := pprof.Lookup(name).WriteTo(f, 0); err != nil {
			t.Logf("soak: writing %s: %v", path, err)
		}
		_ = f.Close() //nolint:errcheck // a profile that did not flush shows up as a short file
		t.Logf("wrote %s", filepath.Base(path))
	}
}

// writeReport records the run for docs/evidence/.
func (r *rig) writeReport(t *testing.T, cfg Config, samples []Sample) {
	t.Helper()

	var b strings.Builder

	fmt.Fprintf(&b, "# S2 — soak: %s, zero drift in steady state\n\n", cfg.Duration)
	fmt.Fprintf(&b, "%d publishers, %d events/sec, %d distinct keys, real Redis 7.4,\n",
		cfg.Publishers, cfg.Rate, cfg.Keys)
	fmt.Fprintf(&b, "real materializer. Sampled every %s.\n\n", cfg.SampleInterval)

	fmt.Fprintf(&b, "%-8s %-7s %-8s %-9s %-9s %-11s %-8s %s\n",
		"t", "drift", "suspect", "coverage", "keys", "goroutines", "rss_mib", "sweep_p99")

	for i := range samples {
		s := &samples[i]
		fmt.Fprintf(&b, "%-8s %-7d %-8d %-9.4f %-9d %-11d %-8d %s\n",
			s.At.Round(time.Second), s.DivergentKeys, s.SuspectKeys,
			s.CoverageRatio, s.TrackedKeys, s.Goroutines, s.RSSBytes>>20,
			s.SweepP99.Round(time.Millisecond))
	}

	last := samples[len(samples)-1]
	fmt.Fprintf(&b, "\nevents applied: %d\nevents dropped: %d\n",
		last.EventsApplied, last.EventsDropped)

	path := filepath.Join(cfg.ArtifactDir, "S2-soak-60min-zero-drift.txt")
	require.NoError(t, os.WriteFile(path, []byte(b.String()), 0o600))
	t.Logf("wrote %s", path)
}

// ---------------------------------------------------------------------------
// Small helpers.
// ---------------------------------------------------------------------------

func envDuration(t *testing.T, name string, fallback time.Duration) time.Duration {
	t.Helper()

	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	require.NoError(t, err, "%s=%q", name, raw)
	return d
}

func envInt(t *testing.T, name string, fallback int) int {
	t.Helper()

	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	require.NoError(t, err, "%s=%q", name, raw)
	return n
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
