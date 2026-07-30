package oracle_test

import (
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/seqtrack"
	"github.com/nabrahma/driftwatch/pkg/testgen"
)

// TestProp_MemoryBounded is invariant I8: tracked keys never exceed
// MaxTrackedKeys and the per-key ring never exceeds RingSize, under any
// generated sequence of applies.
//
// This is the invariant that decides whether driftwatch can be left running.
// Both bounds are the difference between degrading coverage and being killed by
// the out-of-memory reaper, and the second failure mode arrives without warning
// during exactly the incident the tool was deployed to explain.
func TestProp_MemoryBounded(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		shards := rapid.IntRange(1, 8).Draw(t, "shards")
		maxKeys := rapid.IntRange(1, 40).Draw(t, "maxTrackedKeys")
		ringSize := rapid.IntRange(1, 8).Draw(t, "ringSize")

		o := oracle.New(oracle.Config{
			Shards:         shards,
			MaxTrackedKeys: maxKeys,
			RingSize:       ringSize,
			Clock:          clock.Fake(epoch),
		})

		// A small key universe so keys are revisited and the ring actually
		// wraps, rather than every event touching a fresh key.
		keys := []string{"", "a", "b", "c", "d", "e", "f", "\x00", "long-" + strconv.Itoa(ringSize)}

		applies := rapid.IntRange(0, 200).Draw(t, "applies")
		for i := 0; i < applies; i++ {
			key := rapid.SampledFrom(keys).Draw(t, "key")
			at := epoch.Add(time.Duration(i) * time.Second)

			var m projection.Mutation
			if rapid.Bool().Draw(t, "isDelete") {
				m = projection.Mutation{Key: key, Action: projection.ActionDelete}
			} else {
				m = projection.Mutation{
					Key: key, Action: projection.ActionUpsert,
					Value: testgen.Value(t, event.ValueSet),
				}
			}
			e := &event.Event{
				Publisher: "p", Epoch: 1, Seq: uint64(i + 1), //nolint:gosec // loop counter
				Op: event.OpAdd, Key: key, ObservedAt: at,
			}
			o.Apply(m, e, seqtrack.Accept, oracle.TrustComplete)

			require.LessOrEqual(t, o.Len(), maxKeys,
				"tracked keys exceeded the budget after %d applies", i+1)
			require.LessOrEqual(t, len(o.History(key)), ringSize,
				"the ring for %q grew past its size after %d applies", key, i+1)
		}

		// Every key still tracked must have a bounded ring, not just the last
		// one touched.
		for _, key := range keys {
			require.LessOrEqual(t, len(o.History(key)), ringSize)
		}
	})
}

// TestProp_CountsAlwaysAgreeWithTheEntriesTheyDescribe checks the cached
// cardinalities against reality.
//
// The counters are maintained incrementally on a hot path, which is exactly the
// kind of bookkeeping that drifts silently. A wrong count here would show up as
// a dashboard that disagrees with the tool's own findings.
func TestProp_CountsAlwaysAgreeWithTheEntriesTheyDescribe(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		o := oracle.New(oracle.Config{
			Shards:           rapid.IntRange(1, 4).Draw(t, "shards"),
			MaxTrackedKeys:   rapid.IntRange(4, 30).Draw(t, "maxTrackedKeys"),
			SettlementWindow: 5 * time.Second,
			Clock:            clock.Fake(epoch),
		})

		keys := []string{"a", "b", "c", "replica:0:x", "replica:1:y"}
		now := epoch

		ops := rapid.IntRange(0, 60).Draw(t, "ops")
		for i := 0; i < ops; i++ {
			now = now.Add(time.Second)

			switch rapid.IntRange(0, 5).Draw(t, "op") {
			case 0:
				o.MarkSuspect("", "gap")
			case 1:
				o.MarkSuspect("replica:0:*", "gap")
			case 2:
				o.ClearSuspect("")
			case 3:
				o.AdoptSnapshot(map[string]event.Value{
					rapid.SampledFrom(keys).Draw(t, "adoptKey"): setOf("a"),
				}, now)
			default:
				key := rapid.SampledFrom(keys).Draw(t, "key")
				m, e := upsert(key, now, "m")
				trust := oracle.TrustComplete
				if rapid.Bool().Draw(t, "suspect") {
					trust = oracle.TrustSuspect
				}
				o.Apply(m, e, seqtrack.Accept, trust)
			}
		}

		counts := o.Counts(now)

		require.Equal(t, o.Len(), counts.Total)
		require.Equal(t, counts.Total, counts.Settled+counts.InFlight,
			"every key is either settled or in flight, never both and never neither")

		byTrust := counts.ByTrust[oracle.TrustComplete] +
			counts.ByTrust[oracle.TrustSuspect] +
			counts.ByTrust[oracle.TrustAdopted]
		require.Equal(t, counts.Total, byTrust,
			"the trust breakdown must account for exactly the tracked keys")

		// Cross-check the cached counters against the entries themselves.
		var observed int
		for _, key := range keys {
			if _, ok := o.Get(key); ok {
				observed++
			}
		}
		require.Equal(t, observed, counts.Total)
	})
}

// TestProp_VersionIsStrictlyMonotonicPerKey covers creates, deletes and
// recreates. Fencing is only sound if a version never repeats: a sweeper that
// read version 3, watched the key be deleted and recreated back to 3, would
// conclude nothing had changed and compare a stale value against the target.
func TestProp_VersionIsStrictlyMonotonicPerKey(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		o := oracle.New(oracle.Config{Shards: 2, Clock: clock.Fake(epoch)})

		keys := []string{"a", "b", "c"}
		last := map[string]uint64{}

		ops := rapid.IntRange(0, 80).Draw(t, "ops")
		for i := 0; i < ops; i++ {
			key := rapid.SampledFrom(keys).Draw(t, "key")
			at := epoch.Add(time.Duration(i) * time.Second)

			var res oracle.ApplyResult
			if rapid.Bool().Draw(t, "isDelete") {
				m, e := del(key, at)
				res = o.Apply(m, e, seqtrack.Accept, oracle.TrustComplete)
			} else {
				m, e := upsert(key, at, "m")
				res = o.Apply(m, e, seqtrack.Accept, oracle.TrustComplete)
			}

			require.Greater(t, res.Version, last[key],
				"version went backwards or repeated for key %q", key)
			last[key] = res.Version

			got, ok := o.Version(key)
			require.True(t, ok)
			require.Equal(t, res.Version, got)
		}
	})
}

// TestProp_SnapshotClearsSuspect is invariant I10: after a full snapshot cycle,
// no key is Suspect.
func TestProp_SnapshotClearsSuspect(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		o := oracle.New(oracle.Config{
			Shards:         rapid.IntRange(1, 4).Draw(t, "shards"),
			MaxTrackedKeys: 100,
			Clock:          clock.Fake(epoch),
		})

		keys := []string{"a", "b", "replica:0:x", "replica:1:y"}
		for _, key := range keys {
			m, e := upsert(key, epoch, "m")
			trust := oracle.TrustComplete
			if rapid.Bool().Draw(t, "suspectAtApply") {
				trust = oracle.TrustSuspect
			}
			o.Apply(m, e, seqtrack.Accept, trust)
		}

		marks := rapid.IntRange(0, 5).Draw(t, "marks")
		for i := 0; i < marks; i++ {
			if rapid.Bool().Draw(t, "global") {
				o.MarkSuspect("", "gap")
				continue
			}
			o.MarkSuspect("replica:0:*", "gap")
		}

		o.ClearSuspect("")

		for _, key := range keys {
			got, ok := o.Get(key)
			require.True(t, ok, key)
			require.NotEqual(t, oracle.TrustSuspect, got.Trust,
				"key %q is still suspect after a full snapshot cycle", key)
		}
		require.Zero(t, o.Counts(epoch).ByTrust[oracle.TrustSuspect])
	})
}

// TestProp_FencedReadNeverTorn is invariant I12: a concurrent reader using
// version fencing never observes a torn or superseded value.
//
// This is the mechanism that defeats F3 — the oracle moving underneath a sweep.
// The reader here does exactly what the sweeper does: read the version, read
// the value, read the version again, and discard the comparison if it moved.
// Any surviving comparison must be internally consistent.
func TestProp_FencedReadNeverTorn(t *testing.T) {
	const (
		events  = 10_000
		readers = 50
		// Each reader takes a fixed number of fenced reads rather than spinning
		// until the writer finishes. Fifty goroutines in a tight read loop under
		// -race spend nearly a minute of CPU on instrumentation and add no
		// coverage past the first few thousand interleavings, which is a real
		// cost to every future run of the suite.
		readsPerReader = 200
	)

	o := oracle.New(oracle.Config{Shards: 8, Clock: clock.Fake(epoch)})
	keys := []string{"a", "b", "c", "d"}

	// The value at version v encodes v, so a value and a version that disagree
	// are detectable rather than merely suspicious. It is a scalar rather than
	// a set of v members because the set costs O(v) to build, which makes the
	// whole test quadratic and pushes the suite past its time budget for no
	// extra coverage.
	valueFor := func(v uint64) event.Value {
		return event.Value{Kind: event.ValueScalar, Scalar: []byte(strconv.FormatUint(v, 10))}
	}

	var (
		wg      sync.WaitGroup
		fenced  atomic.Int64
		checked atomic.Int64
	)

	wg.Add(1)
	go func() {
		defer wg.Done()

		versions := map[string]uint64{}
		for i := 0; i < events; i++ {
			key := keys[i%len(keys)]
			versions[key]++

			o.Apply(projection.Mutation{
				Key:    key,
				Action: projection.ActionUpsert,
				Value:  valueFor(versions[key]),
			}, &event.Event{
				Publisher: "p", Epoch: 1, Seq: uint64(i + 1), //nolint:gosec // loop counter
				Op: event.OpAdd, Key: key, ObservedAt: epoch,
			}, seqtrack.Accept, oracle.TrustComplete)
		}
	}()

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			key := keys[r%len(keys)]

			for i := 0; i < readsPerReader; i++ {
				runtime.Gosched()

				v1, ok := o.Version(key)
				if !ok {
					continue
				}
				entry, ok := o.Get(key)
				if !ok {
					continue
				}
				v2, ok := o.Version(key)
				if !ok {
					continue
				}

				if v1 != v2 || v1 != entry.Version {
					// The oracle moved mid-read. The sweeper requeues the key
					// rather than comparing, which is the whole point of the
					// fence.
					fenced.Add(1)
					continue
				}

				checked.Add(1)
				if string(entry.Value.Scalar) != strconv.FormatUint(entry.Version, 10) {
					t.Errorf("torn read on %q: version %d carried value %q",
						key, entry.Version, entry.Value.Scalar)
					return
				}
			}
		}(r)
	}

	wg.Wait()

	assert.Positive(t, checked.Load(), "the readers must have completed some fenced reads")
	t.Logf("fenced reads: %d discarded, %d verified", fenced.Load(), checked.Load())
}

// TestProp_HistoryNeverOutlivesItsEntry checks that eviction takes the ring
// with it. A ring that survived its entry would be a slow leak that only shows
// up under saturation, which is when the bound matters most.
func TestProp_HistoryNeverOutlivesItsEntry(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		maxKeys := rapid.IntRange(1, 10).Draw(t, "maxTrackedKeys")
		o := oracle.New(oracle.Config{
			Shards:         1,
			MaxTrackedKeys: maxKeys,
			RingSize:       4,
			Clock:          clock.Fake(epoch),
		})

		n := rapid.IntRange(1, 40).Draw(t, "keys")
		for i := 0; i < n; i++ {
			key := "k" + strconv.Itoa(i)
			m, e := upsert(key, epoch.Add(time.Duration(i)*time.Second), "a")
			o.Apply(m, e, seqtrack.Accept, oracle.TrustComplete)
		}

		for i := 0; i < n; i++ {
			key := "k" + strconv.Itoa(i)
			_, tracked := o.Get(key)
			history := o.History(key)

			if !tracked {
				require.Nil(t, history, "key %q was evicted but kept its history", key)
				continue
			}
			require.LessOrEqual(t, len(history), 4)
		}
	})
}
