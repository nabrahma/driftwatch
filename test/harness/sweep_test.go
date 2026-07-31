package harness_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/projection"
	"github.com/nabrahma/driftwatch/pkg/seqtrack"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// TestSweep_OracleAgainstRedis is the Phase 2 demo from PRD §20: seed a Redis,
// build an oracle from events, diff the two, and print the report.
//
// It is the first point in the project where the whole idea is visible. Events
// go in one side, a real store is read from the other, and what comes out is a
// list of the specific keys that disagree and why — with the keys driftwatch
// cannot vouch for kept separate from the ones it can.
//
// The materializer here is deliberately imperfect. It applies most events to
// the store and then five specific faults are introduced, one per category, so
// the report has something real to say.
func TestSweep_OracleAgainstRedis(t *testing.T) {
	const blocks = 200

	clk := clock.Fake(epoch())
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	tgt := target.NewRedisFromClient(client, 0, 0)
	t.Cleanup(func() { require.NoError(t, tgt.Close()) })

	// A RecordingTarget over the read path, so this demo is also a live
	// assertion that nothing in a sweep writes to the store.
	rec := target.Recording(t, tgt)

	proj, err := projection.New("keysetOwnership", nil)
	require.NoError(t, err)

	orc := oracle.New(oracle.Config{SettlementWindow: 5 * time.Second, Clock: clk})
	tracker := seqtrack.New(seqtrack.Config{Clock: clk})

	// Build the oracle from an event stream, and apply the same events to the
	// store the way a real materializer would.
	ctx := context.Background()
	for i := 0; i < blocks; i++ {
		key := "block-" + strconv.Itoa(i)
		member := "replica-" + strconv.Itoa(i%4)

		e := &event.Event{
			Publisher: member, Epoch: 1, Seq: uint64(i/4 + 1), //nolint:gosec // loop counter
			Op: event.OpAdd, Key: key, Member: member, ObservedAt: clk.Now(),
		}
		verdict, _ := tracker.Observe(e)

		prev, _ := orc.Get(key)
		mutation, applyErr := proj.Apply(prev.Value, e)
		require.NoError(t, applyErr)
		orc.Apply(mutation, e, verdict, tracker.Trust(e.Publisher))

		_, sErr := server.SAdd(key, member)
		require.NoError(t, sErr)
		clk.Advance(time.Millisecond)
	}

	// Now break the store in five specific ways, one per finding category.
	faults := struct{ missing, member, extra, wrongType, suspect string }{
		missing:   "block-10",
		member:    "block-20",
		extra:     "block-unexpected",
		wrongType: "block-30",
		suspect:   "block-40",
	}

	server.Del(faults.missing)                              // the materializer lost a write
	_, err = server.SAdd(faults.member, "replica-impostor") // an extra member appeared
	require.NoError(t, err)
	require.NoError(t, server.Set(faults.extra, "out-of-band")) // somebody wrote directly
	server.Del(faults.wrongType)
	require.NoError(t, server.Set(faults.wrongType, "a-string")) // wrong shape entirely

	// And mark one key suspect, standing in for a sequence gap on its
	// publisher: driftwatch cannot vouch for it, so the finding must be
	// reported separately rather than alerted on.
	orc.MarkSuspect(faults.suspect, "sequence gap on replica-0")
	server.Del(faults.suspect)

	// Let everything settle, then sweep.
	clk.Advance(time.Minute)
	now := clk.Now()

	opts := differ.Options{ExpiryPolicy: differ.ExpiryStrict, Now: now}
	report := differ.NewReport(now, opts)

	health, err := rec.Health(ctx)
	require.NoError(t, err)
	report.TargetHealth = health

	// Oracle to target: the exact direction, because the oracle is a local
	// structure that can be iterated under a lock with version fencing (§5.5).
	var settled []string
	for key := range orc.SettledKeys(now) {
		settled = append(settled, key)
	}

	for _, key := range settled {
		oe, ok := orc.Get(key)
		if !ok {
			continue
		}

		reads, readErr := rec.ReadMany(ctx, []string{key}, projection.ShapeSet)
		require.NoError(t, readErr)
		report.KeysCompared++

		if reads[0].Err != nil {
			report.Add(differ.CompareUnreadable(key, oe, "string", opts))
			continue
		}
		report.Add(differ.Compare(key, oe, reads[0].Value, opts))
	}

	// Target to oracle, to find keys the oracle never expected. §5.5 requires
	// the sweeper to treat these conservatively; here the demo simply reports
	// them, which is what the Phase 3 sweeper will wrap in a re-read.
	it := rec.Scan(ctx, "*", 100)
	for it.Next(ctx) {
		for _, key := range it.Keys() {
			if _, known := orc.Get(key); known {
				continue
			}
			report.Add(differ.Compare(key, oracle.Entry{Key: key}, mustRead(t, rec, key), opts))
			report.KeysCompared++
		}
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())

	report.FinishedAt = clk.Now()

	t.Log("\n" + report.Text())
	t.Log(report.Summary())

	// Every fault introduced must appear, in the right category.
	byKey := map[string]differ.Category{}
	for _, f := range report.Findings {
		byKey[f.Key] = f.Category
	}

	assert.Equal(t, differ.CatMissingInTarget, byKey[faults.missing])
	assert.Equal(t, differ.CatMemberMismatch, byKey[faults.member])
	assert.Equal(t, differ.CatExtraInTarget, byKey[faults.extra])
	assert.Equal(t, differ.CatTypeMismatch, byKey[faults.wrongType])
	assert.Equal(t, differ.CatMissingInTarget, byKey[faults.suspect])

	// And nothing else. The other 195 keys agree, which is the harder half:
	// a differ that reports everything is as useless as one that reports
	// nothing.
	assert.Equal(t, 5, report.Total(), "expected exactly the five introduced faults")

	// The suspect key is counted but not alertable, because driftwatch knows
	// its own view of that publisher is incomplete (§5.2, §23 A7).
	assert.Equal(t, 4, report.Alertable())
	assert.Equal(t, 1, report.ByTrust[oracle.TrustSuspect])

	// The sweep read the store and never wrote to it. RecordingTarget would
	// have failed this test the instant it tried.
	assert.Empty(t, rec.Violations())
	for _, cmd := range rec.Commands() {
		assert.True(t, target.IsReadOnlyCommand(cmd), "command %q is not read-only", cmd)
	}

	// The report has to be machine-readable too.
	raw, err := report.JSON()
	require.NoError(t, err)
	assert.Contains(t, string(raw), "missing_in_target")
}

func mustRead(t *testing.T, tgt target.Target, key string) event.Value {
	t.Helper()

	v, err := tgt.Get(context.Background(), key, projection.ShapeSet)
	if err != nil {
		// A wrong-typed extra key still counts as present; the demo only needs
		// to know the oracle did not expect it.
		return event.Value{Kind: event.ValueScalar, Scalar: []byte("<unreadable>")}
	}
	return v
}

func epoch() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }
