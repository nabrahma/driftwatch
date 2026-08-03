package harness_test

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/codec"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/seqtrack"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// go-redis starts a process-wide time cache and pool reaper at package
		// init and never stops them. Third-party and permitted by §16.5; none
		// of driftwatch's own goroutines are ignored anywhere.
		goleak.IgnoreAnyFunction("github.com/redis/go-redis/v9/internal/pool.startGlobalTimeCache.func1"),
		goleak.IgnoreAnyFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).reaper"),
	)
}

// TestPipeline_100kEventsEndToEnd is the first end-to-end demonstration of the
// ingest path.
//
// It wires the four domain packages together the way the real ingest path does
// — codec, then sequence tracking, then projection, then oracle — and runs a
// synthetic KV-cache workload through them with a deliberately lossy channel.
// Nothing here touches the network or the clock; the whole thing is a fold over
// bytes with time supplied by a fake.
//
// The point is not coverage, which the unit tests already have. It is that the
// pieces compose: that a dropped event on the wire ends up as a counted gap and
// a suspect key rather than as silence.
func TestPipeline_100kEventsEndToEnd(t *testing.T) {
	const (
		totalEvents = 100_000
		publishers  = 4
		blocks      = 5_000
		// Roughly one event in dropRate is withheld before it reaches the
		// decoder, standing in for a ZMQ PUB socket discarding at its
		// high-water mark. The choice is drawn rather than periodic: a fixed
		// stride that shares a factor with the publisher count lands every drop
		// on the same publisher and quietly tests a quarter of what it looks
		// like it tests.
		dropRate = 500
	)

	start := epoch()
	clk := clock.Fake(start)

	dec, err := codec.New("json", nil)
	require.NoError(t, err)

	tracker := seqtrack.New(seqtrack.Config{Clock: clk})

	proj, err := projection.New("keysetOwnership", nil)
	require.NoError(t, err)

	orc := oracle.New(oracle.Config{
		SettlementWindow: 5 * time.Second,
		MaxTrackedKeys:   100_000,
		Clock:            clk,
	})

	// Deterministic pseudo-randomness: the demo has to print the same numbers
	// every run, or it is not evidence of anything.
	rng := rand.New(rand.NewSource(1)) //nolint:gosec // deterministic test fixture, not security

	// reference folds the same events the oracle sees, using the deliberately
	// naive implementation, so the demo checks the pipeline against something
	// other than itself.
	reference := projection.NewReference(projection.ShapeSet)
	var deliveredEvents []event.Event

	var (
		withheld   int
		decoded    int
		decodeErrs int
		verdicts   = map[seqtrack.Verdict]int{}
		gaps       []seqtrack.Gap
		applied    int
	)

	seqs := make([]uint64, publishers)
	var decodeInto event.Event

	for i := 0; i < totalEvents; i++ {
		p := i % publishers
		seqs[p]++

		payload := synthEvent(rng, p, seqs[p], blocks)

		// The lossy channel drops the frame before driftwatch ever sees it,
		// which is exactly the case sequence numbers exist to detect.
		//
		// The last event from each publisher is never dropped. A gap is only
		// observable once something after it arrives, so a publisher that goes
		// silent immediately after a loss is indistinguishable from one that
		// had nothing more to say — a real blind spot, and one that would make
		// the arithmetic below wrong for a reason unrelated to the pipeline.
		if i < totalEvents-publishers && rng.Intn(dropRate) == 0 {
			withheld++
			continue
		}

		clk.Advance(time.Millisecond)
		decodeInto = event.Event{ObservedAt: clk.Now()}
		if decodeErr := dec.Decode(payload, "kv-events", &decodeInto); decodeErr != nil {
			decodeErrs++
			continue
		}
		decoded++

		verdict, gap := tracker.Observe(&decodeInto)
		verdicts[verdict]++
		if gap != nil {
			gaps = append(gaps, *gap)
		}
		if !verdict.Accepted() {
			continue
		}

		existing, _ := orc.Get(decodeInto.Key)
		mutation, applyErr := proj.Apply(existing.Value, &decodeInto)
		require.NoError(t, applyErr)

		res := orc.Apply(mutation, &decodeInto, verdict, tracker.Trust(decodeInto.Publisher))
		if res.Applied {
			applied++
		}
		deliveredEvents = append(deliveredEvents, decodeInto)
	}

	// Let everything settle so the oracle reports a comparable keyspace.
	clk.Advance(time.Minute)
	now := clk.Now()
	counts := orc.Counts(now)

	var settled int
	for range orc.SettledKeys(now) {
		settled++
	}

	report := gapReport(tracker)

	t.Log(renderDemo(&demoResult{
		totalEvents: totalEvents,
		publishers:  publishers,
		withheld:    withheld,
		decoded:     decoded,
		decodeErrs:  decodeErrs,
		applied:     applied,
		verdicts:    verdicts,
		gaps:        gaps,
		report:      report,
		counts:      counts,
		settled:     settled,
		elapsed:     now.Sub(start),
	}))

	// Every withheld frame is one the tracker never saw, and every one of them
	// has to show up as a missing sequence number. This is the whole reason the
	// pipeline carries sequence numbers at all.
	require.Positive(t, withheld, "the generator must actually drop something")
	assert.Equal(t, totalEvents-withheld, decoded)
	assert.Zero(t, decodeErrs, "the codec must decode every frame the generator produced")

	var totalMissing uint64
	for _, st := range tracker.Publishers() {
		totalMissing += st.Gaps.Count()
		assert.False(t, st.Gaps.Truncated(), "publisher %s truncated its gap set", st.ID)
	}
	assert.Equal(t, uint64(withheld), totalMissing,
		"the gap report must account for exactly the events that were dropped")
	assert.Equal(t, withheld, len(gaps), "each dropped frame is detected exactly once")

	// Losing events costs trust, and losing trust is what stops driftwatch
	// claiming the target is wrong on the strength of an incomplete view.
	for _, st := range tracker.Publishers() {
		want := event.TrustComplete
		if st.Gaps.Count() > 0 {
			want = event.TrustSuspect
		}
		assert.Equal(t, want, tracker.Trust(st.ID),
			"publisher %s has %d missing sequences", st.ID, st.Gaps.Count())
	}
	assert.Equal(t, publishers, len(tracker.Publishers()))

	assert.Equal(t, counts.Total, settled, "everything has settled after a minute of quiet")
	assert.Positive(t, counts.Total)
	assert.LessOrEqual(t, counts.Total, blocks)

	// The oracle's state must match the naive fold of the same delivered events.
	// A disagreement here means the composition is wrong even though every
	// package passes its own tests.
	//
	// The two do not hold the same number of entries, and that is by design.
	// A key whose last member was removed is absent as far as the target is
	// concerned, so the reference drops it; the oracle keeps a tombstone,
	// because it has to remember the version across a delete and because
	// "the target should not have this key" is a finding while "never heard of
	// it" is not. So the comparison is over present keys, with every extra
	// oracle entry required to be a tombstone.
	want := reference.Fold(deliveredEvents)

	present, tombstones := 0, 0
	for key := range orc.SettledKeys(now) {
		got, ok := orc.Get(key)
		require.True(t, ok)
		if got.IsAbsent() {
			tombstones++
			_, inReference := want[key]
			assert.False(t, inReference,
				"key %q is a tombstone in the oracle but present in the reference", key)
			continue
		}
		present++
	}

	assert.Equal(t, len(want), present,
		"the oracle holds a different number of present keys than the reference fold")
	assert.Equal(t, counts.Total, present+tombstones)
	assert.Positive(t, tombstones,
		"the workload must empty some keys, or the tombstone path is untested here")

	for key, wantValue := range want {
		got, ok := orc.Get(key)
		require.True(t, ok, "key %q missing from the oracle", key)
		require.True(t, wantValue.Equal(got.Value),
			"key %q: reference has %s, oracle has %s", key, wantValue, got.Value)
	}
}

// synthEvent renders one KV-cache ownership event in the canonical wire format.
func synthEvent(rng *rand.Rand, publisher int, seq uint64, blocks int) []byte {
	op := "add"
	if rng.Intn(3) == 0 {
		op = "remove"
	}
	block := strconv.Itoa(rng.Intn(blocks))

	return []byte(`{"publisher":"replica-` + strconv.Itoa(publisher) +
		`","epoch":1,"seq":` + strconv.FormatUint(seq, 10) +
		`,"op":"` + op +
		`","key":"block-` + block +
		`","member":"replica-` + strconv.Itoa(publisher) + `"}`)
}

type demoResult struct {
	totalEvents int
	publishers  int
	withheld    int
	decoded     int
	decodeErrs  int
	applied     int
	verdicts    map[seqtrack.Verdict]int
	gaps        []seqtrack.Gap
	report      []string
	counts      oracle.Counts
	settled     int
	elapsed     time.Duration
}

func renderDemo(r *demoResult) string {
	out := "\n" +
		"driftwatch ingest pipeline: codec -> seqtrack -> projection -> oracle\n" +
		"=====================================================================\n\n"

	out += "ingest\n"
	out += fmt.Sprintf("  events generated     %d across %d publishers\n", r.totalEvents, r.publishers)
	out += fmt.Sprintf("  withheld on the wire %d\n", r.withheld)
	out += fmt.Sprintf("  decoded              %d (%d decode errors)\n", r.decoded, r.decodeErrs)
	out += fmt.Sprintf("  applied to oracle    %d\n\n", r.applied)

	out += "sequence tracking\n"
	verdicts := make([]seqtrack.Verdict, 0, len(r.verdicts))
	for v := range r.verdicts {
		verdicts = append(verdicts, v)
	}
	sort.Slice(verdicts, func(i, j int) bool { return verdicts[i] < verdicts[j] })
	for _, v := range verdicts {
		out += fmt.Sprintf("  %-22s %d\n", v, r.verdicts[v])
	}
	out += "\n"

	out += fmt.Sprintf("gap report (%d gaps detected)\n", len(r.gaps))
	for _, line := range r.report {
		out += "  " + line + "\n"
	}
	if len(r.gaps) > 0 {
		out += "  first three:\n"
		for i, g := range r.gaps {
			if i == 3 {
				break
			}
			out += "    " + g.String() + "\n"
		}
	}
	out += "\n"

	out += "oracle state\n"
	out += fmt.Sprintf("  tracked keys         %d\n", r.counts.Total)
	out += fmt.Sprintf("  settled / in flight  %d / %d\n", r.counts.Settled, r.counts.InFlight)
	out += fmt.Sprintf("  trust complete       %d\n", r.counts.ByTrust[oracle.TrustComplete])
	out += fmt.Sprintf("  trust suspect        %d\n", r.counts.ByTrust[oracle.TrustSuspect])
	out += fmt.Sprintf("  trust adopted        %d\n", r.counts.ByTrust[oracle.TrustAdopted])
	out += fmt.Sprintf("  fake-clock elapsed   %s\n", r.elapsed)

	return out
}

// gapReport renders one line per publisher, the way `driftwatch watch` will.
func gapReport(tracker *seqtrack.Tracker) []string {
	states := tracker.Publishers()
	out := make([]string, 0, len(states))

	for _, st := range states {
		intervals := st.Gaps.Intervals()
		shown := intervals
		suffix := ""
		if len(shown) > 3 {
			shown = shown[:3]
			suffix = fmt.Sprintf(" ... (%d intervals)", len(intervals))
		}

		ranges := ""
		for i, in := range shown {
			if i > 0 {
				ranges += " "
			}
			ranges += in.String()
		}

		out = append(out, fmt.Sprintf(
			"%-12s hwm=%-7d events=%-7d missing=%-4d trust=%-8s %s%s",
			st.ID, st.HWM, st.EventCount, st.Gaps.Count(), tracker.Trust(st.ID), ranges, suffix))
	}
	return out
}
