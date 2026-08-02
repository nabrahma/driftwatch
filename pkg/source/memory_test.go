package source_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/source"
)

func epoch() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }

// The Eventually budget for the external test package. Declared again rather
// than shared with the internal one next door because `package source` and
// `package source_test` cannot see each other's identifiers; see
// pkg/check/check_test.go for why the numbers are what they are.
const (
	eventuallyFor  = 30 * time.Second
	eventuallyPoll = 10 * time.Millisecond
)

// runMemory starts a memory source and returns its output channel.
func runMemory(t *testing.T, src *source.MemorySource) (chan source.RawMessage, context.CancelFunc) {
	t.Helper()

	out := make(chan source.RawMessage, 1024)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- src.Run(ctx, out) }()

	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("MemorySource.Run did not return")
		}
	})
	return out, cancel
}

func TestMemory_DeliversWhatWasPublishedInOrder(t *testing.T) {
	clk := clock.Fake(epoch())
	src := source.NewMemory(clk)
	out, _ := runMemory(t, src)

	for i := 0; i < 100; i++ {
		require.True(t, src.Publish(source.RawMessage{
			Topic: "t", Payload: []byte("payload-" + strconv.Itoa(i)),
		}))
	}

	for i := 0; i < 100; i++ {
		select {
		case msg := <-out:
			assert.Equal(t, "payload-"+strconv.Itoa(i), string(msg.Payload))
			assert.Equal(t, "t", msg.Topic)
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d messages arrived", i)
		}
	}
	assert.Equal(t, uint64(100), src.Stats().FramesReceived)
}

func TestMemory_StampsObservedAtFromTheInjectedClock(t *testing.T) {
	// The source is the only honest place to stamp local receive time, and a
	// test clock is the only way to assert it exactly (§5.3).
	clk := clock.Fake(epoch())
	src := source.NewMemory(clk)
	out, _ := runMemory(t, src)

	require.True(t, src.PublishPayload([]byte("first")))
	first := <-out
	assert.Equal(t, epoch(), first.ObservedAt)

	clk.Advance(90 * time.Second)
	require.True(t, src.PublishPayload([]byte("second")))
	second := <-out
	assert.Equal(t, epoch().Add(90*time.Second), second.ObservedAt)

	t.Run("an explicit timestamp is kept", func(t *testing.T) {
		// The fault injector rewrites timestamps to simulate clock skew, so a
		// message that already has one must not be re-stamped.
		want := epoch().Add(-time.Hour)
		require.True(t, src.Publish(source.RawMessage{Payload: []byte("x"), ObservedAt: want}))
		assert.Equal(t, want, (<-out).ObservedAt)
	})
}

func TestMemory_BacklogReportsWhatHasNotBeenDelivered(t *testing.T) {
	src := source.NewMemory(clock.Fake(epoch()))

	for i := 0; i < 5; i++ {
		require.True(t, src.PublishPayload([]byte("x")))
	}
	assert.Equal(t, 5, src.Backlog(), "nothing is running, so nothing is delivered")

	out, _ := runMemory(t, src)
	for i := 0; i < 5; i++ {
		<-out
	}
	assert.Eventually(t, func() bool { return src.Backlog() == 0 },
		eventuallyFor, eventuallyPoll)
}

func TestMemory_AFullBacklogRefusesRatherThanBlocking(t *testing.T) {
	// A test that deadlocks instead of failing tells you nothing about what
	// went wrong, which is why Publish reports rather than waits.
	src := source.NewMemory(clock.Fake(epoch()), source.WithCapacity(3))

	for i := 0; i < 3; i++ {
		assert.True(t, src.Publish(source.RawMessage{Payload: []byte("x")}))
	}
	assert.False(t, src.Publish(source.RawMessage{Payload: []byte("one too many")}))
	assert.Equal(t, uint64(1), src.Stats().Dropped)
}

func TestMemory_RefusesAnOversizedPayload(t *testing.T) {
	src := source.NewMemory(clock.Fake(epoch()), source.WithMaxPayload(8))

	assert.True(t, src.PublishPayload([]byte("small")))
	assert.False(t, src.PublishPayload([]byte("far too long to be accepted")))
	assert.Equal(t, uint64(1), src.Stats().Dropped)
	assert.Contains(t, src.Stats().LastError, "exceeds the configured maximum")
}

func TestMemory_CloseDrainsTheBacklogThenEndsTheRun(t *testing.T) {
	// Losing already-published messages on Close would make every test that
	// publishes and then shuts down racy.
	src := source.NewMemory(clock.Fake(epoch()))
	for i := 0; i < 10; i++ {
		require.True(t, src.PublishPayload([]byte("x")))
	}

	out := make(chan source.RawMessage, 16)
	done := make(chan error, 1)
	go func() { done <- src.Run(context.Background(), out) }()

	require.NoError(t, src.Close())
	require.NoError(t, src.Close(), "Close is idempotent")

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Close")
	}
	assert.Len(t, out, 10, "everything published before Close was delivered")
	assert.False(t, src.Publish(source.RawMessage{Payload: []byte("after")}))
}

func TestMemory_RunReturnsOnContextCancellation(t *testing.T) {
	src := source.NewMemory(clock.Fake(epoch()))
	out := make(chan source.RawMessage, 1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- src.Run(ctx, out) }()

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return on cancellation")
	}
	require.NoError(t, src.Close())
}

func TestMemory_BuildsFromTheRegistry(t *testing.T) {
	src, err := source.New("memory", source.Config{
		Settings: map[string]string{"buffer": "2"},
	}, clock.Fake(epoch()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, src.Close()) })

	assert.Equal(t, "memory", src.Name())

	mem, ok := src.(*source.MemorySource)
	require.True(t, ok)
	assert.True(t, mem.PublishPayload([]byte("a")))
	assert.True(t, mem.PublishPayload([]byte("b")))
	assert.False(t, mem.PublishPayload([]byte("c")), "the configured buffer is honored")
}

func TestSource_RegistryRejectsUnknownNames(t *testing.T) {
	_, err := source.New("carrier-pigeon", source.Config{}, clock.Fake(epoch()))

	require.ErrorIs(t, err, source.ErrUnknownSource)
	assert.Contains(t, err.Error(), "carrier-pigeon")
	// The message lists what is available, because the next thing anyone does
	// after a typo is look for the correct spelling.
	assert.Contains(t, err.Error(), "memory")
}

func TestSource_RegisteredNamesAreTheFourFromM4(t *testing.T) {
	assert.Equal(t, []string{"file", "memory", "nats", "zmq"}, source.Registered())
}

func TestSource_RegisterPanicsOnADuplicate(t *testing.T) {
	// Two sources answering to one name silently does the wrong thing, and init
	// is the only moment it can be caught before it matters.
	assert.PanicsWithValue(t, "source: duplicate registration for memory", func() {
		source.Register("memory", nil)
	})
}
