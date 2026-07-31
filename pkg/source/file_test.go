package source_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/source"
)

// writeCapture writes a newline-delimited capture and returns its path.
func writeCapture(t *testing.T, lines ...string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "capture.ndjson")
	require.NoError(t, os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600))
	return path
}

// replay runs a file source to completion and returns everything it delivered.
func replay(t *testing.T, src *source.FileSource) []source.RawMessage {
	t.Helper()

	out := make(chan source.RawMessage, 4096)
	require.NoError(t, src.Run(context.Background(), out))
	close(out)

	got := make([]source.RawMessage, 0, len(out))
	for msg := range out {
		got = append(got, msg)
	}
	return got
}

func TestFile_ReplaysEveryLineInOrder(t *testing.T) {
	lines := make([]string, 100)
	for i := range lines {
		lines[i] = `{"seq":` + strconv.Itoa(i) + `,"topic":"orders"}`
	}
	path := writeCapture(t, lines...)

	src, err := source.NewFile(path, clock.Fake(epoch()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, src.Close()) })

	got := replay(t, src)

	require.Len(t, got, 100)
	for i, msg := range got {
		assert.Equal(t, lines[i], string(msg.Payload))
		assert.Equal(t, "orders", msg.Topic, "the topic is read out of the envelope")
	}
	assert.Equal(t, uint64(100), src.Stats().FramesReceived)
}

func TestFile_EachMessageOwnsItsPayload(t *testing.T) {
	// The scanner reuses its buffer. Handing the slice on without copying makes
	// every message downstream end up holding the last line read, which is the
	// kind of bug that only surfaces once something retains a payload.
	path := writeCapture(t,
		`{"seq":1,"v":"first"}`,
		`{"seq":2,"v":"second"}`,
		`{"seq":3,"v":"third"}`,
	)

	src, err := source.NewFile(path, clock.Fake(epoch()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, src.Close()) })

	got := replay(t, src)

	require.Len(t, got, 3)
	assert.Contains(t, string(got[0].Payload), "first")
	assert.Contains(t, string(got[1].Payload), "second")
	assert.Contains(t, string(got[2].Payload), "third")
}

func TestFile_SkipsBlankLinesRatherThanEmittingThem(t *testing.T) {
	// A capture concatenated from several files usually has them, and a blank
	// line is formatting rather than an event.
	path := writeCapture(t, `{"seq":1}`, "", "   ", `{"seq":2}`)

	src, err := source.NewFile(path, clock.Fake(epoch()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, src.Close()) })

	assert.Len(t, replay(t, src), 2)
}

func TestFile_RealtimeReplayHonoursTheOriginalSpacing(t *testing.T) {
	// Replay pacing runs on the injected clock, or `driftwatch replay` could
	// not be tested without waiting out the capture in real time.
	base := epoch()
	path := writeCapture(t,
		`{"seq":1,"ts":"`+base.Format(time.RFC3339Nano)+`"}`,
		`{"seq":2,"ts":"`+base.Add(30*time.Second).Format(time.RFC3339Nano)+`"}`,
		`{"seq":3,"ts":"`+base.Add(90*time.Second).Format(time.RFC3339Nano)+`"}`,
	)

	clk := clock.Fake(base)
	src, err := source.NewFile(path, clk, source.WithSpeed(1))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, src.Close()) })

	out := make(chan source.RawMessage, 8)
	done := make(chan error, 1)
	go func() { done <- src.Run(context.Background(), out) }()

	// The first is due immediately; the rest wait out their offsets.
	first := <-out
	assert.Contains(t, string(first.Payload), `"seq":1`)

	clk.BlockUntil(1)
	clk.Advance(30 * time.Second)
	assert.Contains(t, string((<-out).Payload), `"seq":2`)

	clk.BlockUntil(1)
	clk.Advance(60 * time.Second)
	assert.Contains(t, string((<-out).Payload), `"seq":3`)

	require.NoError(t, <-done)
	assert.Equal(t, base.Add(90*time.Second), clk.Now(),
		"the whole capture replayed with no real time passing")
}

func TestFile_SpeedMultiplierDividesTheWait(t *testing.T) {
	base := epoch()
	path := writeCapture(t,
		`{"seq":1,"ts":"`+base.Format(time.RFC3339Nano)+`"}`,
		`{"seq":2,"ts":"`+base.Add(60*time.Second).Format(time.RFC3339Nano)+`"}`,
	)

	clk := clock.Fake(base)
	src, err := source.NewFile(path, clk, source.WithSpeed(4))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, src.Close()) })

	out := make(chan source.RawMessage, 8)
	done := make(chan error, 1)
	go func() { done <- src.Run(context.Background(), out) }()

	<-out
	clk.BlockUntil(1)
	clk.Advance(15 * time.Second) // 60s of capture at 4x
	assert.Contains(t, string((<-out).Payload), `"seq":2`)
	require.NoError(t, <-done)
}

func TestFile_AsFastAsPossibleIgnoresTimestampsEntirely(t *testing.T) {
	base := epoch()
	path := writeCapture(t,
		`{"seq":1,"ts":"`+base.Format(time.RFC3339Nano)+`"}`,
		`{"seq":2,"ts":"`+base.Add(24*time.Hour).Format(time.RFC3339Nano)+`"}`,
	)

	clk := clock.Fake(base)
	src, err := source.NewFile(path, clk)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, src.Close()) })

	got := replay(t, src)

	assert.Len(t, got, 2)
	assert.Equal(t, base, clk.Now(), "a day of capture replays without waiting a day")
}

func TestFile_LoopReplaysEndlesslyUntilStopped(t *testing.T) {
	path := writeCapture(t, `{"seq":1}`, `{"seq":2}`)

	src, err := source.NewFile(path, clock.Fake(epoch()), source.WithLoop(true))
	require.NoError(t, err)

	out := make(chan source.RawMessage, 64)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- src.Run(ctx, out) }()

	// Three passes over a two-line file.
	for i := 0; i < 6; i++ {
		select {
		case <-out:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d messages arrived; the loop stopped early", i)
		}
	}

	cancel()
	<-done
	require.NoError(t, src.Close())
}

func TestFile_ReportsAMissingFileRatherThanReturningEmpty(t *testing.T) {
	// An empty replay and an unreadable file must not look alike: one says the
	// capture is clean, the other says nothing was read.
	src, err := source.NewFile(filepath.Join(t.TempDir(), "absent.ndjson"), clock.Fake(epoch()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, src.Close()) })

	err = src.Run(context.Background(), make(chan source.RawMessage, 1))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "absent.ndjson")
	assert.Contains(t, src.Stats().LastError, "absent.ndjson")
}

func TestFile_RefusesALineBeyondTheMaximum(t *testing.T) {
	path := writeCapture(t, `{"v":"`+strings.Repeat("x", 4096)+`"}`)

	src, err := source.New("file", source.Config{
		Settings:        map[string]string{"path": path},
		MaxPayloadBytes: 128,
	}, clock.Fake(epoch()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, src.Close()) })

	err = src.Run(context.Background(), make(chan source.RawMessage, 1))

	require.ErrorIs(t, err, source.ErrPayloadTooLarge)
	assert.Contains(t, err.Error(), "128 bytes")
}

func TestFile_RunReturnsOnCancellation(t *testing.T) {
	lines := make([]string, 10_000)
	for i := range lines {
		lines[i] = `{"seq":` + strconv.Itoa(i) + `}`
	}
	path := writeCapture(t, lines...)

	src, err := source.NewFile(path, clock.Fake(epoch()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, src.Close()) })

	// An unbuffered channel nobody reads: Run parks in the send and has to
	// notice the cancellation there.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- src.Run(ctx, make(chan source.RawMessage)) }()

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return on cancellation")
	}
}

func TestFile_ConfigurationIsValidated(t *testing.T) {
	tests := []struct {
		name     string
		settings map[string]string
		wantErr  string
	}{
		{
			name:     "no path",
			settings: map[string]string{},
			wantErr:  "file.path is required",
		},
		{
			name:     "an unrecognized speed",
			settings: map[string]string{"path": "x.ndjson", "speed": "brisk"},
			wantErr:  "file.speed must be",
		},
		{
			name:     "a negative speed",
			settings: map[string]string{"path": "x.ndjson", "speed": "-2"},
			wantErr:  "file.speed must be",
		},
		{
			name:     "loop with stdin, which cannot be rewound",
			settings: map[string]string{"path": "-", "loop": "true"},
			wantErr:  "cannot be used with stdin",
		},
		{
			name:     "a loop that is not a boolean",
			settings: map[string]string{"path": "x.ndjson", "loop": "sometimes"},
			wantErr:  "loop must be true or false",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := source.New("file", source.Config{Settings: tc.settings}, clock.Fake(epoch()))

			require.Error(t, err)
			assert.ErrorIs(t, err, source.ErrBadConfig)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
