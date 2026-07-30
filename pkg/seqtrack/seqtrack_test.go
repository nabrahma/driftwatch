package seqtrack_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/seqtrack"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func newTracker(t *testing.T, cfg seqtrack.Config) (*seqtrack.Tracker, clock.FakeClock) {
	t.Helper()
	clk := clock.Fake(epoch)
	cfg.Clock = clk
	return seqtrack.New(cfg), clk
}

// ev builds a heartbeat-shaped event; only the identity fields matter to
// sequence tracking.
func ev(pub string, epochNum, seq uint64) *event.Event {
	return &event.Event{Publisher: pub, Epoch: epochNum, Seq: seq, Op: event.OpHeartbeat}
}

func TestVerdict_AcceptedSplitsTheVerdictsIntoTwoGroups(t *testing.T) {
	tests := []struct {
		name    string
		verdict seqtrack.Verdict
		want    bool
	}{
		{name: "an in-order event is accepted", verdict: seqtrack.Accept, want: true},
		{name: "an event that revealed a gap is still accepted", verdict: seqtrack.AcceptWithGap, want: true},
		{name: "an event filling a known hole is accepted", verdict: seqtrack.AcceptLateFill, want: true},
		{name: "the first event after a restart is accepted", verdict: seqtrack.AcceptAfterRestart, want: true},
		{name: "the first event from a publisher is accepted", verdict: seqtrack.AcceptFirstSeen, want: true},
		{name: "a duplicate is not accepted", verdict: seqtrack.DropDuplicate, want: false},
		{name: "a stale epoch is not accepted", verdict: seqtrack.DropStaleEpoch, want: false},
		{
			// A verdict this build does not recognize must not be applied to
			// the oracle. Defaulting to "accepted" would let an unhandled case
			// silently corrupt state.
			name:    "a verdict outside the defined range is not accepted",
			verdict: seqtrack.Verdict(99),
			want:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.verdict.Accepted())
		})
	}
}

func TestVerdict_String(t *testing.T) {
	assert.Equal(t, "accept", seqtrack.Accept.String())
	assert.Equal(t, "accept_with_gap", seqtrack.AcceptWithGap.String())
	assert.Equal(t, "drop_duplicate", seqtrack.DropDuplicate.String())
	assert.Equal(t, "Verdict(99)", seqtrack.Verdict(99).String())
}

// The table below walks every state-transition pair in the PRD §5.2 algorithm.
// Each case seeds the tracker with a prior history and then observes one more
// event, which is the shape every transition actually takes in production.
func TestTracker_ObserveClassifiesEveryStateTransition(t *testing.T) {
	tests := []struct {
		name        string
		seed        []*event.Event
		observe     *event.Event
		wantVerdict seqtrack.Verdict
		wantGap     *seqtrack.Gap
		wantHWM     uint64
	}{
		{
			name:        "the first event from a publisher is adopted as the baseline, not treated as a gap",
			observe:     ev("p", 1, 500),
			wantVerdict: seqtrack.AcceptFirstSeen,
			wantHWM:     500,
		},
		{
			name:        "a first event with seq zero is legal",
			observe:     ev("p", 1, 0),
			wantVerdict: seqtrack.AcceptFirstSeen,
			wantHWM:     0,
		},
		{
			name:        "a publisher that starts high is adopted at that height",
			observe:     ev("p", 1, 1<<63),
			wantVerdict: seqtrack.AcceptFirstSeen,
			wantHWM:     1 << 63,
		},
		{
			name:        "the next sequence in order advances the high-water mark",
			seed:        []*event.Event{ev("p", 1, 10)},
			observe:     ev("p", 1, 11),
			wantVerdict: seqtrack.Accept,
			wantHWM:     11,
		},
		{
			name:        "a forward jump records the hole it skipped over",
			seed:        []*event.Event{ev("p", 1, 10)},
			observe:     ev("p", 1, 15),
			wantVerdict: seqtrack.AcceptWithGap,
			wantGap:     &seqtrack.Gap{Publisher: "p", Epoch: 1, From: 11, To: 14},
			wantHWM:     15,
		},
		{
			name:        "a forward jump of exactly two records a one-wide hole",
			seed:        []*event.Event{ev("p", 1, 10)},
			observe:     ev("p", 1, 12),
			wantVerdict: seqtrack.AcceptWithGap,
			wantGap:     &seqtrack.Gap{Publisher: "p", Epoch: 1, From: 11, To: 11},
			wantHWM:     12,
		},
		{
			name:        "a sequence equal to the high-water mark is a duplicate of the newest event",
			seed:        []*event.Event{ev("p", 1, 10)},
			observe:     ev("p", 1, 10),
			wantVerdict: seqtrack.DropDuplicate,
			wantHWM:     10,
		},
		{
			name:        "a sequence below the high-water mark that was never missing is a duplicate",
			seed:        []*event.Event{ev("p", 1, 10), ev("p", 1, 11)},
			observe:     ev("p", 1, 10),
			wantVerdict: seqtrack.DropDuplicate,
			wantHWM:     11,
		},
		{
			name:        "a sequence that fills a known hole arrives late rather than being a duplicate",
			seed:        []*event.Event{ev("p", 1, 10), ev("p", 1, 15)},
			observe:     ev("p", 1, 12),
			wantVerdict: seqtrack.AcceptLateFill,
			wantHWM:     15,
		},
		{
			name:        "a declared epoch bump is a restart, not a gap of the whole sequence space",
			seed:        []*event.Event{ev("p", 1, 1000000)},
			observe:     ev("p", 2, 1),
			wantVerdict: seqtrack.AcceptAfterRestart,
			wantHWM:     1,
		},
		{
			name:        "an epoch bump that skips epochs is still a single restart",
			seed:        []*event.Event{ev("p", 1, 10)},
			observe:     ev("p", 9, 3),
			wantVerdict: seqtrack.AcceptAfterRestart,
			wantHWM:     3,
		},
		{
			name:        "an event from a previous incarnation arriving late is dropped",
			seed:        []*event.Event{ev("p", 1, 10), ev("p", 2, 1)},
			observe:     ev("p", 1, 11),
			wantVerdict: seqtrack.DropStaleEpoch,
			wantHWM:     1,
		},
		{
			name: "a sequence reset without an epoch bump is recognized as an implicit restart",
			// The publisher restarted and began numbering from scratch without
			// saying so. Treating this as a duplicate would drop every event it
			// ever sends again.
			seed:        []*event.Event{ev("p", 1, 50000)},
			observe:     ev("p", 1, 1),
			wantVerdict: seqtrack.AcceptAfterRestart,
			wantHWM:     1,
		},
		{
			name:        "a small backwards step is a duplicate, not an implicit restart",
			seed:        []*event.Event{ev("p", 1, 50000)},
			observe:     ev("p", 1, 49999),
			wantVerdict: seqtrack.DropDuplicate,
			wantHWM:     50000,
		},
		{
			name: "a large backwards step to a sequence above the restart ceiling is a duplicate",
			// Far below the high-water mark but not near zero, so it is much
			// more likely to be a delayed event than a restart.
			seed:        []*event.Event{ev("p", 1, 50000)},
			observe:     ev("p", 1, 5000),
			wantVerdict: seqtrack.DropDuplicate,
			wantHWM:     50000,
		},
		{
			name: "sequence wraparound at the top of the space is treated as an implicit restart",
			// A publisher at MaxUint64 that wraps to zero looks exactly like one
			// that restarted, and there is no way to tell them apart.
			seed:        []*event.Event{ev("p", 1, 1<<64-1)},
			observe:     ev("p", 1, 0),
			wantVerdict: seqtrack.AcceptAfterRestart,
			wantHWM:     0,
		},
		{
			name:        "two publishers do not interfere with each other",
			seed:        []*event.Event{ev("a", 1, 10), ev("b", 1, 500)},
			observe:     ev("a", 1, 11),
			wantVerdict: seqtrack.Accept,
			wantHWM:     11,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tr, _ := newTracker(t, seqtrack.Config{})
			for _, e := range tc.seed {
				tr.Observe(e)
			}

			verdict, gap := tr.Observe(tc.observe)

			assert.Equal(t, tc.wantVerdict, verdict)
			if tc.wantGap == nil {
				assert.Nil(t, gap)
			} else {
				require.NotNil(t, gap)
				assert.Equal(t, tc.wantGap.Publisher, gap.Publisher)
				assert.Equal(t, tc.wantGap.Epoch, gap.Epoch)
				assert.Equal(t, tc.wantGap.From, gap.From)
				assert.Equal(t, tc.wantGap.To, gap.To)
				assert.Equal(t, epoch, gap.DetectedAt, "gaps are stamped with the injected clock")
			}

			state, ok := publisherState(tr, tc.observe.Publisher)
			require.True(t, ok)
			assert.Equal(t, tc.wantHWM, state.HWM)
		})
	}
}

func publisherState(tr *seqtrack.Tracker, id string) (seqtrack.PublisherState, bool) {
	for _, st := range tr.Publishers() {
		if st.ID == id {
			return st, true
		}
	}
	return seqtrack.PublisherState{}, false
}

func TestTracker_RestartClearsTheGapsFromThePreviousIncarnation(t *testing.T) {
	tr, _ := newTracker(t, seqtrack.Config{})

	tr.Observe(ev("p", 1, 10))
	tr.Observe(ev("p", 1, 20)) // leaves 11..19 missing
	require.Equal(t, event.TrustSuspect, tr.Trust("p"))

	tr.Observe(ev("p", 2, 1))

	// Those sequence numbers belong to an incarnation that no longer exists.
	// Carrying them forward would keep the publisher permanently suspect.
	assert.Equal(t, event.TrustComplete, tr.Trust("p"))

	st, ok := publisherState(tr, "p")
	require.True(t, ok)
	assert.Equal(t, uint64(1), st.RestartCount)
	assert.Equal(t, uint64(2), st.Epoch)
}

func TestTracker_AnImplicitRestartAdvancesTheInternalIncarnationWithoutRejectingLaterEvents(t *testing.T) {
	tr, _ := newTracker(t, seqtrack.Config{})

	tr.Observe(ev("p", 1, 50000))
	verdict, _ := tr.Observe(ev("p", 1, 1))
	require.Equal(t, seqtrack.AcceptAfterRestart, verdict)

	// The publisher never bumped its wire epoch, so every subsequent event still
	// carries epoch 1. If the internal restart had raised the tracked epoch,
	// all of them would be dropped as stale, which is worse than the problem it
	// was solving.
	verdict, _ = tr.Observe(ev("p", 1, 2))
	assert.Equal(t, seqtrack.Accept, verdict)

	st, ok := publisherState(tr, "p")
	require.True(t, ok)
	assert.Equal(t, uint64(1), st.Epoch, "the wire epoch is reported as the publisher sent it")
	assert.Equal(t, uint64(1), st.Incarnation, "the internal incarnation counts restarts driftwatch inferred")
	assert.Equal(t, uint64(1), st.RestartCount)
}

func TestTracker_ImplicitRestartThresholdsAreConfigurable(t *testing.T) {
	tests := []struct {
		name    string
		cfg     seqtrack.Config
		seedSeq uint64
		nextSeq uint64
		want    seqtrack.Verdict
	}{
		{
			name:    "a drop below the ceiling and beyond the delta is a restart",
			cfg:     seqtrack.Config{ImplicitRestartDelta: 100, ImplicitRestartCeiling: 10},
			seedSeq: 500,
			nextSeq: 5,
			want:    seqtrack.AcceptAfterRestart,
		},
		{
			name:    "a sequence at or above the ceiling is not a restart however far it fell",
			cfg:     seqtrack.Config{ImplicitRestartDelta: 100, ImplicitRestartCeiling: 10},
			seedSeq: 500,
			nextSeq: 10,
			want:    seqtrack.DropDuplicate,
		},
		{
			name:    "a fall smaller than the delta is not a restart however small the sequence",
			cfg:     seqtrack.Config{ImplicitRestartDelta: 1000, ImplicitRestartCeiling: 100},
			seedSeq: 500,
			nextSeq: 1,
			want:    seqtrack.DropDuplicate,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tr, _ := newTracker(t, tc.cfg)
			tr.Observe(ev("p", 1, tc.seedSeq))

			verdict, _ := tr.Observe(ev("p", 1, tc.nextSeq))

			assert.Equal(t, tc.want, verdict)
		})
	}
}

func TestTracker_TrustIsSuspectExactlyWhileGapsAreOutstanding(t *testing.T) {
	tr, _ := newTracker(t, seqtrack.Config{})

	assert.Equal(t, event.TrustComplete, tr.Trust("never-seen"),
		"an unknown publisher has no known gaps, so nothing about it is suspect yet")

	tr.Observe(ev("p", 1, 1))
	assert.Equal(t, event.TrustComplete, tr.Trust("p"))

	tr.Observe(ev("p", 1, 5)) // 2..4 missing
	assert.Equal(t, event.TrustSuspect, tr.Trust("p"))

	// Filling every hole restores trust: driftwatch has seen the whole stream
	// again, so it can go back to asserting the target is wrong.
	tr.Observe(ev("p", 1, 2))
	tr.Observe(ev("p", 1, 3))
	assert.Equal(t, event.TrustSuspect, tr.Trust("p"))
	tr.Observe(ev("p", 1, 4))
	assert.Equal(t, event.TrustComplete, tr.Trust("p"))
}

func TestTracker_TrustStaysSuspectWhileTheGapSetIsTruncated(t *testing.T) {
	tr, _ := newTracker(t, seqtrack.Config{MaxGapIntervals: 2})

	// Enough separate holes to force truncation, then fill every sequence the
	// set still claims is missing. Truncation over-approximates, so the count
	// cannot reach zero, and trust must not be restored on a partial view.
	tr.Observe(ev("p", 1, 1))
	for seq := uint64(3); seq <= 21; seq += 2 {
		tr.Observe(ev("p", 1, seq))
	}
	require.True(t, publisherGaps(t, tr, "p").Truncated())

	assert.Equal(t, event.TrustSuspect, tr.Trust("p"))
}

func publisherGaps(t *testing.T, tr *seqtrack.Tracker, id string) *seqtrack.GapSet {
	t.Helper()
	st, ok := publisherState(tr, id)
	require.True(t, ok)
	return st.Gaps
}

func TestTracker_ClearGapsRestoresTrustAfterASnapshotCycle(t *testing.T) {
	tr, _ := newTracker(t, seqtrack.Config{})

	tr.Observe(ev("p", 1, 1))
	tr.Observe(ev("p", 1, 100))
	require.Equal(t, event.TrustSuspect, tr.Trust("p"))

	tr.ClearGaps("p")

	// A completed snapshot means the publisher retransmitted its whole state,
	// so what was missed no longer matters.
	assert.Equal(t, event.TrustComplete, tr.Trust("p"))
	assert.Equal(t, uint64(0), publisherGaps(t, tr, "p").Count())

	tr.ClearGaps("unknown-publisher") // must not panic
}

func TestTracker_ResetDropsEverything(t *testing.T) {
	tr, _ := newTracker(t, seqtrack.Config{})
	tr.Observe(ev("a", 1, 1))
	tr.Observe(ev("b", 1, 1))
	require.Len(t, tr.Publishers(), 2)

	tr.Reset()

	assert.Empty(t, tr.Publishers())

	// After a reset the tracker has no history, so the next event from a known
	// publisher is a first sighting rather than a gap.
	verdict, gap := tr.Observe(ev("a", 1, 900))
	assert.Equal(t, seqtrack.AcceptFirstSeen, verdict)
	assert.Nil(t, gap)
}

func TestTracker_PublishersReportsCountsAndTimestampsFromTheInjectedClock(t *testing.T) {
	tr, clk := newTracker(t, seqtrack.Config{})

	tr.Observe(ev("p", 1, 1))
	clk.Advance(90 * time.Second)
	tr.Observe(ev("p", 1, 2))
	tr.Observe(ev("p", 1, 2)) // duplicate: seen, but not counted as an event

	st, ok := publisherState(tr, "p")
	require.True(t, ok)
	assert.Equal(t, uint64(2), st.EventCount, "duplicates are not double counted")
	assert.Equal(t, epoch, st.FirstSeen)
	assert.Equal(t, epoch.Add(90*time.Second), st.LastSeen)
	assert.True(t, st.Bootstrap, "the publisher's first sequence was adopted, not observed from its start")
}

func TestTracker_PublishersReturnsSnapshotsACallerCannotMutateBack(t *testing.T) {
	tr, _ := newTracker(t, seqtrack.Config{})
	tr.Observe(ev("p", 1, 1))
	tr.Observe(ev("p", 1, 5))

	snapshot := tr.Publishers()
	require.Len(t, snapshot, 1)
	snapshot[0].Gaps.Clear()
	snapshot[0].HWM = 999

	// The tracker is read concurrently by the sweeper and the explain engine.
	// Handing out a live pointer would be a data race waiting to happen.
	assert.Equal(t, event.TrustSuspect, tr.Trust("p"))
	st, ok := publisherState(tr, "p")
	require.True(t, ok)
	assert.Equal(t, uint64(5), st.HWM)
}

func TestTracker_PublishersAreSortedByIDForStableOutput(t *testing.T) {
	tr, _ := newTracker(t, seqtrack.Config{})
	for _, id := range []string{"c", "a", "b"} {
		tr.Observe(ev(id, 1, 1))
	}

	got := tr.Publishers()

	require.Len(t, got, 3)
	assert.Equal(t, "a", got[0].ID)
	assert.Equal(t, "b", got[1].ID)
	assert.Equal(t, "c", got[2].ID)
}

func TestTracker_EvictsTheLeastRecentlySeenPublisherAtTheCap(t *testing.T) {
	tr, clk := newTracker(t, seqtrack.Config{MaxPublishers: 2})

	tr.Observe(ev("oldest", 1, 1))
	clk.Advance(time.Second)
	tr.Observe(ev("middle", 1, 1))
	clk.Advance(time.Second)

	require.Len(t, tr.Publishers(), 2)
	require.Equal(t, uint64(0), tr.Evictions())

	tr.Observe(ev("newest", 1, 1))

	// A publisher map that grows with an attacker-controlled identifier is an
	// out-of-memory kill, so the least recently seen one goes.
	assert.Equal(t, uint64(1), tr.Evictions())
	_, stillThere := publisherState(tr, "oldest")
	assert.False(t, stillThere)
	_, kept := publisherState(tr, "middle")
	assert.True(t, kept)
	_, added := publisherState(tr, "newest")
	assert.True(t, added)
}

func TestTracker_AnEvictedPublisherIsAdoptedAfreshRatherThanReportingAHugeGap(t *testing.T) {
	tr, clk := newTracker(t, seqtrack.Config{MaxPublishers: 1})

	tr.Observe(ev("a", 1, 1000))
	clk.Advance(time.Second)
	tr.Observe(ev("b", 1, 1)) // evicts a
	clk.Advance(time.Second)

	verdict, gap := tr.Observe(ev("a", 1, 2000)) // evicts b, re-adopts a

	assert.Equal(t, seqtrack.AcceptFirstSeen, verdict)
	assert.Nil(t, gap, "an evicted publisher's history is gone; inventing a 1000-wide gap would be a lie")
}

func TestTracker_ConfigDefaultsAreApplied(t *testing.T) {
	tr, _ := newTracker(t, seqtrack.Config{})

	// A tracker built from a zero Config must be usable rather than degenerate:
	// a zero MaxPublishers would evict on every event.
	for i := 0; i < 100; i++ {
		tr.Observe(ev(string(rune('a'+i%26))+string(rune('0'+i/26)), 1, 1))
	}

	assert.Equal(t, uint64(0), tr.Evictions())
	assert.Len(t, tr.Publishers(), 100)
}

func TestTracker_ObserveWithoutAnInjectedClockStillWorks(t *testing.T) {
	// pkg/check always injects a clock, but a zero Config must not panic.
	tr := seqtrack.New(seqtrack.Config{})

	verdict, _ := tr.Observe(ev("p", 1, 1))

	assert.Equal(t, seqtrack.AcceptFirstSeen, verdict)
	st, ok := publisherState(tr, "p")
	require.True(t, ok)
	assert.False(t, st.FirstSeen.IsZero())
}

func TestTracker_ObserveIsSafeForConcurrentUseAlongsideReaders(t *testing.T) {
	tr, _ := newTracker(t, seqtrack.Config{})

	// Observe is called only from the single applier goroutine, but Publishers
	// and Trust are read by the sweeper and the explain engine while it runs.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for seq := uint64(1); seq <= 2000; seq++ {
			tr.Observe(ev("p", 1, seq))
		}
	}()

	readers := make(chan struct{}, 4)
	for i := 0; i < 4; i++ {
		go func() {
			defer func() { readers <- struct{}{} }()
			for j := 0; j < 500; j++ {
				tr.Publishers()
				tr.Trust("p")
			}
		}()
	}

	<-done
	for i := 0; i < 4; i++ {
		<-readers
	}

	st, ok := publisherState(tr, "p")
	require.True(t, ok)
	assert.Equal(t, uint64(2000), st.HWM)
}

func TestGap_StringNamesThePublisherAndTheRange(t *testing.T) {
	g := seqtrack.Gap{Publisher: "replica-2", Epoch: 1, From: 11, To: 14}

	assert.Equal(t, "replica-2/1 missing [11,14] (4)", g.String())
}
