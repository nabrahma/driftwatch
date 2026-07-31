package sweeper_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/sweeper"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// scanExtras runs one extras pass and fails the test if it errors.
func (h *harness) scanExtras() *differ.Report {
	h.t.Helper()

	rep, err := h.swp.ScanExtrasOnce(context.Background())
	require.NoError(h.t, err)
	return rep
}

func TestSweeper_ExtrasNeedTwoPassesASettlementWindowApart(t *testing.T) {
	// §5.5. The scan is not atomic and the oracle is not instantaneous, so a
	// single pass cannot tell an extra key from a key whose event driftwatch
	// has not applied yet. One pass would report every key the system wrote
	// while the scan was running.
	h := newHarness(t)

	h.materialize("known", "v")
	h.apply("known", "v")
	h.materialize("orphan", "v")

	first := h.scanExtras()

	assert.Empty(t, first.Findings, "one pass is never enough to report an extra")
	assert.Equal(t, 1, h.swp.PendingExtras())

	h.advance(window + time.Second)
	second := h.scanExtras()

	require.Len(t, second.Findings, 1)
	assert.Equal(t, "orphan", second.Findings[0].Key)
	assert.Equal(t, differ.CatExtraInTarget, second.Findings[0].Category)
	assert.True(t, second.Findings[0].Confirmed)
	assert.Equal(t, int64(1), h.swp.Stats().ExtrasReported)
}

func TestSweeper_AKeyThatAppearsMidScanAndThenResolvesIsNeverReported(t *testing.T) {
	// The exact race the two passes exist for, in both of its forms. A key can
	// stop being an extra either because driftwatch caught up with the event
	// that explains it, or because whoever wrote it deleted it again.
	tests := []struct {
		name    string
		resolve func(h *harness)
	}{
		{
			name: "driftwatch catches up with the event",
			resolve: func(h *harness) {
				// The key was legitimate all along; the scan simply overtook
				// driftwatch's own ingest.
				h.apply("racing", "v")
			},
		},
		{
			name: "the key goes away again",
			resolve: func(h *harness) {
				h.unmaterialize("racing")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)

			h.materialize("racing", "v")
			require.Empty(t, h.scanExtras().Findings)
			require.Equal(t, 1, h.swp.PendingExtras())

			tc.resolve(h)
			h.advance(window + time.Second)

			assert.Empty(t, h.scanExtras().Findings)
			assert.Equal(t, int64(1), h.swp.Stats().ExtrasSelfResolved)
			assert.Zero(t, h.swp.Stats().ExtrasReported)
		})
	}
}

func TestSweeper_ATombstonedKeyIsNotAlsoReportedAsAnExtra(t *testing.T) {
	// The oracle expecting a key to be absent is an expectation like any other,
	// and the oracle→target sweep already compares it. Reporting it here as
	// well would report one divergence twice under two categories, and an
	// operator counting keys would get a number that is simply wrong.
	h := newHarness(t)

	h.apply("doomed", "v")
	h.applyDelete("doomed")
	h.materialize("doomed", "v")

	require.Empty(t, h.scanExtras().Findings)
	assert.Zero(t, h.swp.PendingExtras(),
		"a key the oracle has an opinion about is not an extras candidate")

	h.advance(window + time.Second)
	assert.Empty(t, h.scanExtras().Findings)
}

func TestSweeper_AdoptedKeysAreNeverExtras(t *testing.T) {
	// §5.6: Adopt mode reads the target into the oracle at startup precisely so
	// that pre-existing keys are not reported. If they came back as extras the
	// mode would be useless.
	h := newHarness(t)

	h.materialize("preexisting", "v")
	h.orc.AdoptSnapshot(map[string]event.Value{
		"preexisting": {Kind: event.ValueScalar, Scalar: []byte("v")},
	}, h.clk.Now())

	require.Empty(t, h.scanExtras().Findings)
	assert.Zero(t, h.swp.PendingExtras())

	h.advance(window + time.Second)
	assert.Empty(t, h.scanExtras().Findings)
}

func TestSweeper_TheFirstExtrasPassIsBounded(t *testing.T) {
	// Past the bound the magnitude matters and the list does not. An unbounded
	// candidate set would let a keyspace driftwatch knows nothing about consume
	// memory proportional to that keyspace, which §19.2 forbids.
	h := newHarness(t, func(c *harnessConfig) { c.sweeper.MaxExtrasTracked = 10 })

	for i := 0; i < 50; i++ {
		h.materialize("orphan"+strconv.Itoa(i), "v")
	}

	rep := h.scanExtras()

	assert.True(t, rep.Truncated)
	assert.Equal(t, 10, h.swp.PendingExtras())
	assert.Equal(t, int64(1), h.swp.Stats().ExtrasTruncated)
}

func TestSweeper_ExtrasScanReportsNothingWhileTheTargetIsUnreachable(t *testing.T) {
	h := newHarness(t)

	h.materialize("orphan", "v")
	require.Empty(t, h.scanExtras().Findings)
	h.advance(window + time.Second)

	h.setHealth(func(hl *target.Health) { hl.Reachable = false })

	rep, err := h.swp.ScanExtrasOnce(context.Background())

	require.ErrorIs(t, err, sweeper.ErrTargetUnavailable)
	assert.Nil(t, rep)
	assert.Equal(t, 1, h.swp.PendingExtras(),
		"the pending set survives, so the second pass still happens later")
}

func TestSweeper_ExtrasScanUsesScanNeverKeys(t *testing.T) {
	// NG1 and invariant I13 in the one place a careless implementation reaches
	// for KEYS, because it is so much easier to write.
	h := newHarness(t)

	h.materialize("a", "v")
	h.scanExtras()

	assert.Contains(t, h.rec.Commands(), "SCAN")
	assert.NotContains(t, h.rec.Commands(), "KEYS")
	assert.Empty(t, h.rec.Violations())
}
