package sweeper_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/oracle"
)

// The line between "the store is wrong" and "I do not know".
//
// §23 A7. Every other feature in this package is about not reporting drift that
// is really lag; this one is about not reporting drift that is really
// driftwatch's own missing events. It is the harder of the two, because the
// disagreement is genuine — the oracle and the store really do differ — and the
// only reason not to blame the store is that driftwatch cannot prove it did not
// cause the difference itself.
//
// Getting this backwards is the single most damaging thing this tool can do: it
// pages someone about a store that is fine, using evidence that is missing.

func TestTrust_ASuspectKeyIsReportedButNeverConfirmed(t *testing.T) {
	h := newHarness(t)

	h.apply("uncertain", "v1")

	// driftwatch missed events, so it can no longer vouch for this key.
	h.orc.MarkSuspect("", "a gap was detected")
	h.settle()

	rep := h.sweep()

	require.Equal(t, 1, rep.Total(),
		"the disagreement is real and an operator should see it")
	assert.Equal(t, oracle.TrustSuspect, rep.Findings[0].Trust,
		"and it must be marked as untrusted rather than presented as fact")
	assert.Zero(t, rep.Alertable(),
		"a suspect finding is not alertable: %s", rep.Summary())

	// The load-bearing assertion. Confirmed() drives divergent_keys and the
	// subscriber channel, which is the path that ends in somebody being paged.
	assert.Empty(t, h.swp.Confirmed(),
		"a key driftwatch cannot vouch for must not reach the confirmed set")
	assert.Zero(t, h.swp.PendingConfirmations(),
		"nor the confirmation queue — there is nothing a second read could "+
			"settle, because the uncertainty is about driftwatch's own view")
	assert.Positive(t, h.swp.Stats().SuspectNotConfirmed,
		"declining to blame the store has to be counted, or an operator "+
			"cannot tell a clean keyspace from one driftwatch stopped "+
			"asserting about")
}

func TestTrust_AKeyThatBecomesSuspectWhileAwaitingConfirmationIsDropped(t *testing.T) {
	// The gap arrives after the candidate was raised. At the moment of the
	// first read driftwatch's view was complete, so the candidate was
	// legitimate; by the time it comes due it no longer is, and confirming it
	// would be asserting on evidence that has since been withdrawn.
	h := newHarness(t)
	ctx := context.Background()

	h.apply("became-suspect", "v1")
	h.settle()

	require.Equal(t, 1, h.sweep().Total())
	require.Equal(t, 1, h.swp.PendingConfirmations())

	// The gap turns up during the wait.
	h.orc.MarkSuspect("", "a gap detected after the sweep")
	h.advance(window + time.Second)

	h.swp.ConfirmDue(ctx, h.clk.Now())

	assert.Empty(t, h.swp.Confirmed(),
		"the candidate was raised on a view driftwatch can no longer stand "+
			"behind, so it must not be promoted")
	assert.Positive(t, h.swp.Stats().SuspectNotConfirmed)
}

func TestTrust_AKeyWhoseOracleVersionMovedDuringTheWaitIsRequeuedNotConfirmed(t *testing.T) {
	// D-009's other half, at the candidate stage rather than the confirmed one.
	//
	// A candidate is a claim about one oracle version. If an event arrives for
	// that key while it waits, the second read would be comparing the store
	// against an expectation that did not exist when the disagreement was
	// noticed. That is not a confirmation; it is a different question, and it
	// goes back on the queue to be asked properly.
	h := newHarness(t)
	ctx := context.Background()

	h.apply("moved", "v1")
	h.settle()

	require.Equal(t, 1, h.sweep().Total())
	require.Equal(t, 1, h.swp.PendingConfirmations())

	// A new event for the same key.
	h.apply("moved", "v2")
	h.advance(window + time.Second)

	h.swp.ConfirmDue(ctx, h.clk.Now())

	assert.Empty(t, h.swp.Confirmed(),
		"the expectation moved under the candidate, so nothing was confirmed")
	assert.Positive(t, h.swp.Stats().TransientOracleAdvanced,
		"this is not a repair and not a false positive — it is the question "+
			"being reopened, and it is counted apart from both")
}

func TestTrust_TheSweepFencesEachKeyAgainstTheVersionItWasSelectedAt(t *testing.T) {
	// The same fence, one level earlier. A sweep selects settled keys, then
	// reads them from the store in batches — and the oracle keeps moving while
	// that happens. Comparing a key against an expectation that changed between
	// selection and read produces a disagreement that never existed at any
	// single instant.
	//
	// The fence is what makes a sweep a statement about a consistent moment
	// rather than a smear across the time the sweep took.
	h := newHarness(t)

	h.apply("racing", "v1")
	h.materialize("racing", "v1")
	h.settle()

	// A new event lands between the settled-key selection and the read.
	//
	// The hook has to be scoped to the *read* command. A sweep issues INFO and
	// SCAN first, and applying an event there would land before the settled-key
	// selection — which would make the key in-flight and exclude it from the
	// sweep entirely, so the fence would never be reached and the test would
	// pass for the wrong reason.
	var once bool
	h.mem.ObserveCommands(func(command string) {
		switch command {
		case "INFO", "SCAN", "TTL":
			return
		}
		if !once {
			once = true
			h.apply("racing", "v2")
		}
	})

	rep := h.sweep()

	assert.Zero(t, rep.Total(),
		"the key changed mid-sweep, so no finding may be raised for it: %s",
		rep.Summary())
	assert.Positive(t, h.swp.Stats().FenceFailures,
		"the skipped key has to be counted; a sweep that silently compared "+
			"fewer keys than it selected would overstate its own coverage")
}
