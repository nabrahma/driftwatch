package target_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/target"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// go-winio starts a process-wide IO completion processor the first time
		// the Docker client opens a named pipe, and never stops it. It is
		// reached only from testcontainers, so it appears under the integration
		// build tag on Windows and nowhere else; the matcher simply finds
		// nothing in a normal unit run.
		//
		// Ignoring it is safe because it belongs to a third-party package and
		// outlives every test by design rather than by mistake. Per §16.5 no
		// ignore here is ever for driftwatch's own goroutines — one of those is
		// a bug to fix, not an entry to add.
		//
		// IgnoreAnyFunction rather than IgnoreTopFunction: the goroutine parks
		// in syscall.syscalln, so the name worth matching is several frames
		// down and the top-of-stack matcher never fires.
		goleak.IgnoreAnyFunction("github.com/Microsoft/go-winio.ioCompletionProcessor"),

		// go-redis starts a process-wide time cache and a connection-pool reaper
		// at package init and never stops them. They are reachable from any
		// package that links the client, which now includes this one.
		//
		// PRD §16.5 anticipates exactly this and permits an ignore for a
		// third-party goroutine with a reason. Neither of these is driftwatch's,
		// and one of driftwatch's own would be a bug to fix rather than an entry
		// to add here.
		goleak.IgnoreAnyFunction("github.com/redis/go-redis/v9/internal/pool.startGlobalTimeCache.func1"),
		goleak.IgnoreAnyFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).reaper"),
	)
}

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// fakeTB captures what a failing test would have reported, so the enforcement
// can be tested without failing the test that is testing it.
//
// FailNow runs runtime.Goexit in the real testing package, which is what stops
// the offending command from reaching the store. Here it records the fact and
// unwinds via panic, which the caller recovers — same control flow, observable
// from the outside.
type fakeTB struct {
	helpers  int
	messages []string
	failed   bool
}

func (f *fakeTB) Helper() { f.helpers++ }

func (f *fakeTB) Errorf(format string, args ...any) {
	f.messages = append(f.messages, fmt.Sprintf(format, args...))
}

func (f *fakeTB) FailNow() {
	f.failed = true
	panic(errFailNow)
}

func (f *fakeTB) output() string { return strings.Join(f.messages, "\n") }

type failNowSentinel struct{}

var errFailNow = failNowSentinel{}

// run executes fn, recovering the fake FailNow so the test can inspect what
// happened rather than dying with it.
func run(fn func()) (recovered bool) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(failNowSentinel); ok {
				recovered = true
				return
			}
			panic(r)
		}
	}()
	fn()
	return false
}

// TestRecordingTarget_FailsTheTestOnAnAttemptedWrite is the proof that NG1 is
// structurally enforced rather than merely intended.
//
// driftwatch is deployed beside production stores on the strength of a promise
// that it never writes. A promise kept by careful review is worth much less
// than one the test suite refuses to let you break, and this is the test that
// does the refusing.
func TestRecordingTarget_FailsTheTestOnAnAttemptedWrite(t *testing.T) {
	tests := []struct {
		name    string
		attempt func(*target.MemoryTarget)
		command string
	}{
		{
			name:    "FLUSHDB is refused",
			attempt: func(m *target.MemoryTarget) { m.SimulateFlush() },
			command: "FLUSHDB",
		},
		{
			name:    "an eviction is refused",
			attempt: func(m *target.MemoryTarget) { m.SimulateEvict(1) },
			command: "DEBUG EVICT",
		},
		{
			name:    "SET is refused",
			attempt: func(m *target.MemoryTarget) { m.Seed(map[string][]byte{"k": []byte("v")}) },
			command: "SET",
		},
		{
			name:    "SADD is refused",
			attempt: func(m *target.MemoryTarget) { m.SeedSets(map[string][]string{"k": {"a"}}) },
			command: "SADD",
		},
		{
			name:    "EXPIREAT is refused",
			attempt: func(m *target.MemoryTarget) { m.SetExpiry("k", epoch) },
			command: "EXPIREAT",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tb := &fakeTB{}
			mem := target.NewMemory()
			target.Recording(tb, mem)
			require.False(t, tb.failed, "wrapping a target must not fail on its own")

			failed := run(func() { tc.attempt(mem) })

			assert.True(t, failed, "the attempted write must stop the test, not merely be noted")
			assert.True(t, tb.failed)
			assert.Contains(t, tb.output(), tc.command,
				"the failure must name the command that was attempted")
			assert.Contains(t, tb.output(), "read-only",
				"the failure must say why it is a failure")
			assert.Contains(t, tb.output(), "I13",
				"the failure must point at the invariant it enforces")
		})
	}
}

func TestRecordingTarget_TheRefusedCommandNeverReachesTheStore(t *testing.T) {
	// Refusal is not advisory. FailNow unwinds the calling goroutine, so the
	// store never sees the command — a flush that was going to destroy the
	// keyspace does not destroy it and then report the problem.
	tb := &fakeTB{}
	mem := target.NewMemory()
	mem.Seed(map[string][]byte{"a": []byte("1"), "b": []byte("2")})
	require.Equal(t, 2, mem.Len())

	target.Recording(tb, mem)
	failed := run(func() { mem.SimulateFlush() })

	require.True(t, failed)
	assert.Equal(t, 2, mem.Len(), "the store was flushed despite the refusal")
}

func TestRecordingTarget_AllowsEveryReadOnlyCommand(t *testing.T) {
	tb := &fakeTB{}
	mem := target.NewMemory()
	mem.SeedSets(map[string][]string{"set": {"a", "b"}})
	mem.Seed(map[string][]byte{"scalar": []byte("v")})

	rec := target.Recording(tb, mem)
	ctx := context.Background()

	_, err := rec.Get(ctx, "scalar", projection.ShapeScalar)
	require.NoError(t, err)
	_, err = rec.GetMany(ctx, []string{"scalar"}, projection.ShapeScalar)
	require.NoError(t, err)
	_, err = rec.ReadMany(ctx, []string{"set"}, projection.ShapeSet)
	require.NoError(t, err)
	_, err = rec.TTL(ctx, "scalar")
	require.NoError(t, err)
	_, err = rec.Health(ctx)
	require.NoError(t, err)

	it := rec.Scan(ctx, "*", 10)
	for it.Next(ctx) {
		_ = it.Keys()
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	require.NoError(t, rec.Close())

	assert.False(t, tb.failed, "no read-only command may fail the test: %s", tb.output())
	assert.Empty(t, rec.Violations())

	// Every command the reads issued must be on the allowlist.
	for _, cmd := range rec.Commands() {
		assert.True(t, target.IsReadOnlyCommand(cmd), "command %q is not read-only", cmd)
	}

	calls := rec.Calls()
	assert.Equal(t, 1, calls["Get"])
	assert.Equal(t, 1, calls["GetMany"])
	assert.Equal(t, 1, calls["ReadMany"])
	assert.Equal(t, 1, calls["Scan"])
	assert.Equal(t, 1, calls["TTL"])
	assert.Equal(t, 1, calls["Health"])
	assert.Equal(t, 1, calls["Close"])
}

func TestRecordingTarget_RecordsTheCommandsItSaw(t *testing.T) {
	tb := &fakeTB{}
	mem := target.NewMemory()
	rec := target.Recording(tb, mem)
	ctx := context.Background()

	_, err := rec.Get(ctx, "k", projection.ShapeScalar)
	require.NoError(t, err)
	_, err = rec.Get(ctx, "k", projection.ShapeSet)
	require.NoError(t, err)

	// The command names have to be the store's, not the interface's, or the
	// allowlist would be checking something other than what is sent.
	assert.Equal(t, []string{"GET", "SMEMBERS"}, rec.Commands())
}

func TestRecordingTarget_ViolationsCanBeInspectedDeliberately(t *testing.T) {
	// AllowViolations is the only way to see a refusal without dying of it, and
	// it exists so that the enforcement itself can be tested. Anything else
	// calling it is opting out of the guarantee.
	tb := &fakeTB{}
	mem := target.NewMemory()
	rec := target.Recording(tb, mem).AllowViolations()

	mem.Seed(map[string][]byte{"k": []byte("v")})
	mem.SimulateFlush()

	assert.False(t, tb.failed)
	assert.Equal(t, []string{"SET", "FLUSHDB"}, rec.Violations())
}

func TestRecordingTarget_FixtureWritesAreSetupRatherThanViolations(t *testing.T) {
	// A sweeper test has to change the store while the test runs — a
	// materializer that never writes produces no drift to detect. Fixture is
	// the narrow, named seam for that, and this pins its two halves: the write
	// inside does not fail the test, and it is filed separately from a real
	// violation so the two are never confused.
	tb := &fakeTB{}
	mem := target.NewMemory()
	rec := target.Recording(tb, mem)
	ctx := context.Background()

	rec.Fixture(func() {
		mem.Seed(map[string][]byte{"k": []byte("v")})
		mem.Remove("k")
	})

	got, err := rec.Get(ctx, "k", projection.ShapeScalar)
	require.NoError(t, err)

	assert.False(t, tb.failed, "a write inside Fixture must not fail the test")
	assert.Equal(t, event.ValueAbsent, got.Kind, "the fixture write must have happened")
	assert.Empty(t, rec.Violations())
	assert.Equal(t, []string{"SET", "DEL"}, rec.FixtureCommands())
}

func TestRecordingTarget_CheckingResumesWhenTheFixtureEnds(t *testing.T) {
	// The dangerous failure is a suspension that outlives its scope: every
	// later write would pass unnoticed, and the test would still look like it
	// was enforcing read-only access.
	tb := &fakeTB{}
	mem := target.NewMemory()
	rec := target.Recording(tb, mem).AllowViolations()

	rec.Fixture(func() { mem.Seed(map[string][]byte{"k": []byte("v")}) })
	mem.Seed(map[string][]byte{"after": []byte("v")})

	assert.Equal(t, []string{"SET"}, rec.FixtureCommands())
	assert.Equal(t, []string{"SET"}, rec.Violations(),
		"the write after the fixture must be refused")
}

func TestRecordingTarget_RefusesToWrapATargetItCannotCheck(t *testing.T) {
	// A RecordingTarget around a target that cannot report its commands would
	// pass every test while enforcing nothing, which is worse than no wrapper
	// at all: it would look like the guarantee was being checked.
	tb := &fakeTB{}

	failed := run(func() { target.Recording(tb, uncheckableTarget{}) })

	assert.True(t, failed)
	assert.Contains(t, tb.output(), "Commander")
	assert.Contains(t, tb.output(), "decorative")
}

func TestIsReadOnlyCommand(t *testing.T) {
	tests := []struct {
		name    string
		command string
		want    bool
	}{
		{name: "GET is a read", command: "GET", want: true},
		{name: "matching ignores case", command: "get", want: true},
		{name: "SMEMBERS is a read", command: "SMEMBERS", want: true},
		{name: "SCAN is a read", command: "SCAN", want: true},
		{name: "a container command matches on its verb", command: "MEMORY USAGE", want: true},
		{name: "DBSIZE is a read, and Health needs it", command: "DBSIZE", want: true},
		{name: "CLUSTER is a read, and cluster scanning needs it", command: "CLUSTER SLOTS", want: true},
		{name: "SET is a write", command: "SET", want: false},
		{name: "DEL is a write", command: "DEL", want: false},
		{name: "FLUSHDB is a write", command: "FLUSHDB", want: false},
		{name: "FLUSHALL is a write", command: "FLUSHALL", want: false},
		{name: "SADD is a write", command: "SADD", want: false},
		{name: "SREM is a write", command: "SREM", want: false},
		{name: "EXPIRE is a write", command: "EXPIRE", want: false},
		{name: "INCR is a write", command: "INCR", want: false},
		{name: "RENAME is a write", command: "RENAME", want: false},
		{name: "EVAL is a write, because a script can do anything", command: "EVAL", want: false},
		{name: "KEYS is refused, and not only because it blocks the server", command: "KEYS", want: false},
		{name: "an empty command is not on the list", command: "", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, target.IsReadOnlyCommand(tc.command))
		})
	}
}

func TestReadOnlyCommands_IsSortedAndCoversTheInvariantList(t *testing.T) {
	got := target.ReadOnlyCommands()

	for i := 1; i < len(got); i++ {
		assert.LessOrEqual(t, got[i-1], got[i], "the allowlist must be sorted")
	}

	// Exactly the list named in PRD §5.8 I13 must be present.
	for _, want := range []string{
		"GET", "SMEMBERS", "SCAN", "TYPE", "TTL", "PTTL",
		"EXISTS", "HGETALL", "INFO", "STRLEN", "SCARD", "MEMORY",
	} {
		assert.Contains(t, got, want)
	}
}

// uncheckableTarget implements Target but not Commander.
type uncheckableTarget struct{ target.Target }

func (uncheckableTarget) Name() string { return "uncheckable" }
