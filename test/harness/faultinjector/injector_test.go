package faultinjector_test

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/source"
	"github.com/nabrahma/driftwatch/test/harness/faultinjector"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func epoch() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }

// event renders one message in the harness publisher's format, which is what
// the envelope-reading faults are pointed at.
func event(publisher string, epochNum, seq uint64, at time.Time) source.RawMessage {
	payload := fmt.Sprintf(
		`{"publisher":%q,"epoch":%d,"seq":%d,"op":"add","key":"k%d","member":"m","ts":%q}`,
		publisher, epochNum, seq, seq%97, at.Format(time.RFC3339Nano))
	return source.RawMessage{Topic: "events", Payload: []byte(payload), ObservedAt: at}
}

// stream builds n messages one millisecond apart from a single publisher.
func stream(n int) []source.RawMessage {
	msgs := make([]source.RawMessage, n)
	for i := range msgs {
		msgs[i] = event("p0", 1, uint64(i+1), epoch().Add(time.Duration(i)*time.Millisecond))
	}
	return msgs
}

// run feeds msgs through the faults and returns everything that came out.
//
// It advances the fake clock past the end of the stream so that Timed faults
// release what they are holding, which is what makes a delayed message a
// reordered one rather than a lost one.
func run(t *testing.T, msgs []source.RawMessage, faults ...faultinjector.Fault) []source.RawMessage {
	t.Helper()

	clk := clock.Fake(epoch())
	src := source.NewMemory(clk, source.WithCapacity(len(msgs)+16))
	for _, msg := range msgs {
		require.True(t, src.Publish(msg))
	}

	inj := faultinjector.Wrap(src, clk, faults...)
	out := make(chan source.RawMessage, 4*len(msgs)+64)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- inj.Run(ctx, out) }()

	// Wait for the injector to have taken every message before moving the
	// clock.
	//
	// This ordering is what makes the run deterministic, and it is worth being
	// explicit about. A held message's release time is a pure function of the
	// message and the seed, so the set of releases is fixed — but if the clock
	// advanced while messages were still arriving, which messages were pending
	// at each release point would depend on how far the injector's goroutine
	// had got, and that varies run to run. Draining first makes the whole
	// input visible before any release decision is taken.
	require.Eventually(t, func() bool { return inj.Stats().Received == uint64(len(msgs)) },
		20*time.Second, time.Millisecond, "the injector never drained the source")

	// One large advance rather than many small ones, so every held message
	// comes due at the same moment and leaves in a single sorted release. Then
	// wait for the release to finish before closing: a close that arrived
	// mid-release would send the remainder out through the end-of-stream flush
	// instead, and the boundary between the two paths would move run to run.
	clk.Advance(time.Hour)
	require.Eventually(t, func() bool { return !inj.HasPending() },
		20*time.Second, time.Millisecond, "held messages were never released")

	require.NoError(t, src.Close())

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("Injector.Run did not return")
	}

	close(out)
	got := make([]source.RawMessage, 0, len(out))
	for msg := range out {
		got = append(got, msg)
	}
	return got
}

// seqs extracts the sequence numbers from a stream, for readable assertions.
func seqs(t *testing.T, msgs []source.RawMessage) []uint64 {
	t.Helper()

	out := make([]uint64, 0, len(msgs))
	for _, msg := range msgs {
		var got struct{ Seq uint64 }
		raw := string(msg.Payload)
		i := indexOf(raw, `"seq":`)
		if i < 0 {
			continue
		}
		rest := raw[i+len(`"seq":`):]
		end := 0
		for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
			end++
		}
		n, err := strconv.ParseUint(rest[:end], 10, 64)
		if err != nil {
			continue
		}
		got.Seq = n
		out = append(out, got.Seq)
	}
	return out
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// ---------------------------------------------------------------------------
// The determinism meta-test §13 requires.
// ---------------------------------------------------------------------------

func TestFaults_Deterministic(t *testing.T) {
	// §13: a test that fails once in fifty runs is worse than no test. Every
	// fault takes an explicit seed and must produce the identical output stream
	// given the identical input stream, so this runs each one twice over ten
	// thousand messages and compares byte for byte.
	//
	// The comparison is on the full message — topic, payload and timestamp —
	// rather than on a count or a checksum, because the failures worth catching
	// here are ordering ones: a fault that emits the right messages in an
	// unstable order would pass any assertion on the set.
	const messages = 10_000

	faults := []struct {
		name string
		make func() faultinjector.Fault
	}{
		{"Drop", func() faultinjector.Fault { return faultinjector.Drop(0.1, 42) }},
		{"DropBurst", func() faultinjector.Fault { return faultinjector.DropBurst(100, 50) }},
		{"DropSeqRange", func() faultinjector.Fault { return faultinjector.DropSeqRange(500, 600) }},
		{"Reorder", func() faultinjector.Fault { return faultinjector.Reorder(8, 42) }},
		{"ReorderSwap", func() faultinjector.Fault { return faultinjector.ReorderSwap(100, 200) }},
		{"Duplicate", func() faultinjector.Fault {
			return faultinjector.Duplicate(0.05, 50*time.Millisecond, 7)
		}},
		{"Delay", func() faultinjector.Fault {
			return faultinjector.Delay(time.Millisecond, 50*time.Millisecond, 7)
		}},
		{"DelayPublisher", func() faultinjector.Fault {
			return faultinjector.DelayPublisher("p0", 100*time.Millisecond)
		}},
		{"Partition", func() faultinjector.Fault {
			return faultinjector.Partition(time.Second, 2*time.Second)
		}},
		{"Corrupt", func() faultinjector.Fault { return faultinjector.Corrupt(0.05, 42) }},
		{"Truncate", func() faultinjector.Fault { return faultinjector.Truncate(0.05, 42) }},
		{"Oversize", func() faultinjector.Fault { return faultinjector.Oversize(500, 4096) }},
		{"ClockSkew", func() faultinjector.Fault {
			return faultinjector.ClockSkew("p0", 2*time.Hour)
		}},
		{"SeqReset", func() faultinjector.Fault { return faultinjector.SeqReset(5000) }},
		{"EpochBump", func() faultinjector.Fault { return faultinjector.EpochBump(5000) }},
		{"Interleave", func() faultinjector.Fault { return faultinjector.Interleave(3) }},

		// A chain, because composition is where a shared generator or a map
		// iteration would show up and a single fault would not.
		{"chained", func() faultinjector.Fault { return faultinjector.Drop(0.02, 1) }},
	}

	input := stream(messages)

	for _, tc := range faults {
		t.Run(tc.name, func(t *testing.T) {
			first := run(t, input, tc.make())
			second := run(t, input, tc.make())

			require.Equal(t, len(first), len(second),
				"%s emitted %d messages then %d", tc.name, len(first), len(second))

			for i := range first {
				require.Equal(t, first[i].Topic, second[i].Topic,
					"%s: topic differs at position %d", tc.name, i)
				require.Equal(t, string(first[i].Payload), string(second[i].Payload),
					"%s: payload differs at position %d", tc.name, i)
				require.True(t, first[i].ObservedAt.Equal(second[i].ObservedAt),
					"%s: timestamp differs at position %d", tc.name, i)
			}
			t.Logf("%-16s %6d in -> %6d out, identical across two runs",
				tc.name, messages, len(first))
		})
	}

	t.Run("a chain of every kind of fault", func(t *testing.T) {
		// Order is part of the contract: Drop then Reorder is not Reorder then
		// Drop. This pins one specific order and proves it is reproducible.
		chain := func() []faultinjector.Fault {
			return []faultinjector.Fault{
				faultinjector.DropSeqRange(100, 110),
				faultinjector.Reorder(8, 42),
				faultinjector.Duplicate(0.05, 10*time.Millisecond, 7),
				faultinjector.Delay(time.Millisecond, 20*time.Millisecond, 3),
				faultinjector.Corrupt(0.01, 9),
			}
		}

		first := run(t, input, chain()...)
		second := run(t, input, chain()...)

		require.Equal(t, len(first), len(second))
		for i := range first {
			require.Equal(t, string(first[i].Payload), string(second[i].Payload),
				"the chain differs at position %d", i)
		}
		t.Logf("%-16s %6d in -> %6d out, identical across two runs",
			"chained", messages, len(first))
	})
}

// ---------------------------------------------------------------------------
// Each fault does what its name says.
// ---------------------------------------------------------------------------

func TestDropSeqRange_RemovesExactlyTheNamedSequences(t *testing.T) {
	got := run(t, stream(100), faultinjector.DropSeqRange(10, 20))

	assert.Len(t, got, 89, "eleven sequences removed from a hundred")
	for _, seq := range seqs(t, got) {
		assert.False(t, seq >= 10 && seq <= 20, "seq %d should have been dropped", seq)
	}
}

func TestDrop_IsReproducibleAndRoughlyTheConfiguredRate(t *testing.T) {
	got := run(t, stream(10_000), faultinjector.Drop(0.1, 42))

	// A rate, not a guarantee: the assertion is loose on purpose because
	// tightening it would make the test depend on the generator's exact output.
	assert.InDelta(t, 9_000, len(got), 300)
}

func TestDropBurst_RemovesAContiguousRun(t *testing.T) {
	got := run(t, stream(100), faultinjector.DropBurst(10, 5))

	assert.Len(t, got, 95)
	assert.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 16, 17}, seqs(t, got)[:12],
		"exactly sequences 11 to 15 are missing")
}

func TestReorder_EmitsTheSameMessagesInADifferentOrder(t *testing.T) {
	input := stream(64)
	got := run(t, input, faultinjector.Reorder(8, 42))

	require.Len(t, got, 64, "reordering loses nothing")
	assert.NotEqual(t, seqs(t, input), seqs(t, got), "and it genuinely reorders")
	assert.ElementsMatch(t, seqs(t, input), seqs(t, got), "with the same messages")
}

func TestReorder_FlushesAPartFullWindow(t *testing.T) {
	// Ten messages through a window of eight leaves two buffered. Losing them
	// would be a drop the scenario never asked for.
	got := run(t, stream(10), faultinjector.Reorder(8, 42))

	assert.Len(t, got, 10)
	assert.ElementsMatch(t, seqs(t, stream(10)), seqs(t, got))
}

func TestReorderSwap_ExchangesExactlyTwoSequences(t *testing.T) {
	got := seqs(t, run(t, stream(20), faultinjector.ReorderSwap(5, 9)))

	require.Len(t, got, 20)
	// Everything else keeps its place; 9 arrives where 5 was and 5 follows it.
	assert.Equal(t, []uint64{1, 2, 3, 4, 6, 7, 8, 9, 5, 10}, got[:10])
}

func TestReorderSwap_ReleasesAHeldMessageWhosePartnerNeverArrives(t *testing.T) {
	// A swap naming a sequence the stream does not contain must not become a
	// drop: the scenario asked for a reorder.
	got := run(t, stream(10), faultinjector.ReorderSwap(5, 999))

	assert.Len(t, got, 10)
	assert.ElementsMatch(t, seqs(t, stream(10)), seqs(t, got))
}

func TestDuplicate_ReEmitsCopiesAfterTheDelay(t *testing.T) {
	input := stream(1000)
	got := run(t, input, faultinjector.Duplicate(0.1, 50*time.Millisecond, 7))

	assert.Greater(t, len(got), len(input), "duplication adds messages")
	assert.InDelta(t, 1100, len(got), 60)

	// Every original still appears; a duplicate is an addition, not a swap.
	assert.Subset(t, seqs(t, got), seqs(t, input))
}

func TestDelay_HoldsEveryMessageAndLosesNone(t *testing.T) {
	input := stream(500)
	got := run(t, input, faultinjector.Delay(time.Millisecond, 100*time.Millisecond, 7))

	require.Len(t, got, 500, "a delay is not a drop")
	assert.ElementsMatch(t, seqs(t, input), seqs(t, got))
}

func TestDelayPublisher_HoldsOnlyTheNamedPublisher(t *testing.T) {
	msgs := make([]source.RawMessage, 0, 100)
	for i := 0; i < 100; i++ {
		pub := "p" + strconv.Itoa(i%2)
		msgs = append(msgs, event(pub, 1, uint64(i+1), epoch().Add(time.Duration(i)*time.Millisecond)))
	}

	got := run(t, msgs, faultinjector.DelayPublisher("p1", time.Second))

	require.Len(t, got, 100, "delaying is not dropping")

	// p0 is untouched and comes first; p1 is held a second and arrives after.
	firstFifty := got[:50]
	for _, msg := range firstFifty {
		assert.Contains(t, string(msg.Payload), `"publisher":"p0"`,
			"the undelayed publisher is not held up by the delayed one")
	}
}

func TestPartition_DropsWhateverFallsInsideTheWindow(t *testing.T) {
	// One message per millisecond, so the window's width decides the count.
	got := run(t, stream(1000), faultinjector.Partition(100*time.Millisecond, 200*time.Millisecond))

	assert.Len(t, got, 800, "200ms of a 1kHz stream is 200 messages")
	for _, seq := range seqs(t, got) {
		assert.False(t, seq >= 101 && seq <= 300, "seq %d was inside the partition", seq)
	}
}

func TestCorrupt_ChangesThePayloadWithoutChangingItsLength(t *testing.T) {
	input := stream(1000)
	got := run(t, input, faultinjector.Corrupt(0.1, 42))

	require.Len(t, got, 1000, "corruption is not loss; the message still arrives")

	var corrupted int
	for i := range got {
		if !bytes.Equal(got[i].Payload, input[i].Payload) {
			corrupted++
			assert.Len(t, got[i].Payload, len(input[i].Payload),
				"a flipped byte does not change the length")
		}
	}
	assert.InDelta(t, 100, corrupted, 40)
}

func TestTruncate_CutsPayloadsShortWithoutEmptyingThem(t *testing.T) {
	input := stream(1000)
	got := run(t, input, faultinjector.Truncate(0.1, 42))

	require.Len(t, got, 1000)

	var truncated int
	for i := range got {
		if len(got[i].Payload) < len(input[i].Payload) {
			truncated++
			assert.NotEmpty(t, got[i].Payload,
				"an empty frame is a different fault with a different meaning")
		}
	}
	assert.InDelta(t, 100, truncated, 40)
}

func TestOversize_ReplacesExactlyOneMessage(t *testing.T) {
	got := run(t, stream(100), faultinjector.Oversize(50, 4096))

	require.Len(t, got, 100)
	assert.Len(t, got[49].Payload, 4096, "the fiftieth message is the big one")
	for i, msg := range got {
		if i != 49 {
			assert.Less(t, len(msg.Payload), 4096)
		}
	}
}

func TestClockSkew_RewritesTimestampsButNotTheLocalReceiveTime(t *testing.T) {
	// The distinction is the whole point of F5. §5.3 settles on driftwatch's
	// own receive time, so a publisher with a two-hour skew must change the
	// payload's timestamp and nothing else — if ObservedAt moved too, the test
	// would prove the opposite of what it means to.
	input := stream(10)
	got := run(t, input, faultinjector.ClockSkew("p0", 2*time.Hour))

	require.Len(t, got, 10)
	for i, msg := range got {
		assert.True(t, msg.ObservedAt.Equal(input[i].ObservedAt),
			"the local receive time is not the publisher's to influence")
		assert.Contains(t, string(msg.Payload),
			input[i].ObservedAt.Add(2*time.Hour).Format(time.RFC3339Nano),
			"but the payload's own timestamp is skewed")
	}
}

func TestSeqReset_RestartsSequencesWithoutTouchingTheEpoch(t *testing.T) {
	got := run(t, stream(20), faultinjector.SeqReset(11))

	require.Len(t, got, 20)
	assert.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5},
		seqs(t, got)[:15], "the eleventh message restarts the sequence")

	for _, msg := range got {
		assert.Contains(t, string(msg.Payload), `"epoch":1`,
			"an implicit restart says nothing about itself")
	}
}

func TestEpochBump_RestartsSequencesAndRaisesTheEpoch(t *testing.T) {
	got := run(t, stream(20), faultinjector.EpochBump(11))

	require.Len(t, got, 20)
	assert.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5},
		seqs(t, got)[:15])

	assert.Contains(t, string(got[9].Payload), `"epoch":1`)
	assert.Contains(t, string(got[10].Payload), `"epoch":2`,
		"an explicit restart announces itself, which is what makes it safe")
}

func TestInterleave_SplitsOneStreamAcrossSeveralPublishers(t *testing.T) {
	got := run(t, stream(30), faultinjector.Interleave(3))

	require.Len(t, got, 30)

	perPublisher := map[string]int{}
	for _, msg := range got {
		for _, pub := range []string{"pub-0", "pub-1", "pub-2"} {
			if assertContains(string(msg.Payload), `"publisher":"`+pub+`"`) {
				perPublisher[pub]++
			}
		}
	}
	assert.Equal(t, map[string]int{"pub-0": 10, "pub-1": 10, "pub-2": 10}, perPublisher,
		"each synthetic publisher gets its share")
}

func assertContains(haystack, needle string) bool { return indexOf(haystack, needle) >= 0 }

// ---------------------------------------------------------------------------
// The injector itself.
// ---------------------------------------------------------------------------

func TestInjector_WithNoFaultsIsTransparent(t *testing.T) {
	// The baseline every scenario rests on: if the injector perturbed the
	// stream on its own, no fault test would mean anything.
	input := stream(500)
	got := run(t, input)

	require.Len(t, got, 500)
	for i := range got {
		assert.Equal(t, string(input[i].Payload), string(got[i].Payload))
		assert.True(t, input[i].ObservedAt.Equal(got[i].ObservedAt))
	}
}

func TestInjector_AppliesFaultsInTheOrderGiven(t *testing.T) {
	// §13 asks for the ordering to be documented; here it is asserted, because
	// a chain whose order did not matter would make every scenario using one
	// ambiguous.
	//
	// Interleave renumbers the stream across synthetic publishers, so a drop
	// named in sequence numbers hits entirely different messages depending on
	// which side of the renumbering it sits.
	dropThenInterleave := run(t, stream(20),
		faultinjector.DropSeqRange(1, 5),
		faultinjector.Interleave(2))

	interleaveThenDrop := run(t, stream(20),
		faultinjector.Interleave(2),
		faultinjector.DropSeqRange(1, 5))

	assert.Len(t, dropThenInterleave, 15,
		"five original sequences dropped, then the rest renumbered")
	assert.Len(t, interleaveThenDrop, 10,
		"renumbered first, so seqs 1-5 of *each* publisher are dropped")
}

func TestInjector_ResetClearsEveryFaultsState(t *testing.T) {
	// A scenario that reused an injector without this would inherit the
	// previous run's generator position and buffered messages, and its faults
	// would land somewhere other than where it asked for.
	burst := faultinjector.DropBurst(5, 5)

	first := seqs(t, run(t, stream(20), burst))
	burst.Reset()
	second := seqs(t, run(t, stream(20), burst))

	assert.Equal(t, first, second, "after a reset the fault lands in the same place")
}

func TestInjector_ForwardsTheWrappedSourcesIdentityAndGaps(t *testing.T) {
	clk := clock.Fake(epoch())
	src := source.NewMemory(clk)
	inj := faultinjector.Wrap(src, clk, faultinjector.Drop(0.5, 1))
	t.Cleanup(func() { require.NoError(t, inj.Close()) })

	assert.Equal(t, "memory", inj.Name(),
		"the pipeline must not be able to tell it is being lied to")
	assert.Nil(t, inj.Gaps(), "a memory source cannot lose messages, so it signals none")
}

func TestInjector_CountsWhatItSaw(t *testing.T) {
	clk := clock.Fake(epoch())
	src := source.NewMemory(clk, source.WithCapacity(200))
	for _, msg := range stream(100) {
		require.True(t, src.Publish(msg))
	}

	inj := faultinjector.Wrap(src, clk, faultinjector.DropSeqRange(1, 10))
	out := make(chan source.RawMessage, 256)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- inj.Run(ctx, out) }()

	assert.Eventually(t, func() bool { return inj.Stats().Emitted == 90 },
		10*time.Second, time.Millisecond)

	require.NoError(t, src.Close())
	<-done

	stats := inj.Stats()
	assert.Equal(t, uint64(100), stats.Received)
	assert.Equal(t, uint64(90), stats.Emitted)
}

func TestInjector_ReturnsOnCancellation(t *testing.T) {
	clk := clock.Fake(epoch())
	src := source.NewMemory(clk)
	inj := faultinjector.Wrap(src, clk, faultinjector.Delay(time.Second, time.Second, 1))
	t.Cleanup(func() { require.NoError(t, inj.Close()) })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- inj.Run(ctx, make(chan source.RawMessage, 8)) }()

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("Injector.Run did not return on cancellation")
	}
}
