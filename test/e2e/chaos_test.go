//go:build e2e

package e2e

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// E3 — SelfLossReportsSuspect (§14.4). The honesty test.
//
// driftwatch's own subscription is severed with toxiproxy while the publisher
// keeps emitting and the materializer keeps writing. Redis therefore stays
// exactly correct, and driftwatch is the only component that missed anything.
//
// The assertion is `suspectDivergentKeys > 0` with `divergentKeys == 0`, and it
// is the most important single claim in the project.
//
// Here is why. When the subscription comes back, driftwatch's oracle is stale
// for every key that changed while it was cut. Sweep those keys and they will
// disagree with the store — and the store is *right*. The naive tool reports
// hundreds of divergent keys, an operator investigates, finds nothing wrong,
// and never trusts the tool again. The whole apparatus of sequence tracking and
// trust states exists so that driftwatch says "I lost events, these keys are
// suspect, do not page anyone" instead.
//
// E2 is the same underlying disagreement with the loss on the other side, where
// confirmed divergence is the right answer. Getting the two the wrong way round
// is the worst thing driftwatch can do.
var _ = Describe("E3 SelfLossReportsSuspect", Ordered, func() {
	var s *scenario
	var check string

	// The keyspace, and the cycle it implies. 6,000 keys at 150/sec is a
	// 40-second cycle: every key is rewritten every forty seconds, the oracle
	// is fully populated forty seconds in, and a key the partition made stale
	// stays stale for up to forty seconds afterwards.
	//
	// That last number is the one this scenario is built on. Suspicion decays
	// per key — §5.2 clears a key the moment a later event refreshes it, which
	// is correct, because the new event is evidence driftwatch did not miss
	// anything about that key. So the window in which a key is both suspect and
	// visibly wrong is bounded by the cycle, and the cycle has to be longer than
	// a settlement window plus a sweep plus the time it takes to read a status.
	const (
		keys  = 6_000
		rate  = 150
		blind = 1_500 // events published while the subscription is cut
	)

	BeforeAll(func() {
		// The only scenario that needs toxiproxy: driftwatch connects through
		// it, the materializer connects straight to the publisher. That
		// asymmetry is the entire fault.
		//
		// And the only scenario that publishes a scalar workload, which is not
		// a detail. Under the keyset workload the rest of the suite uses, every
		// event after a key's first is a no-op — the same member added to the
		// same set — so an event driftwatch misses could not have changed
		// anything, its stale oracle agrees with the store exactly, and no key
		// can ever disagree. This scenario spent three runs asserting suspect >
		// 0 against a workload that made a suspect *disagreement* impossible to
		// produce. See shapeEvent in the harness for the whole argument.
		s = newScenario("e3-self-loss", &FixtureOptions{
			Rate:       rate,
			Keys:       keys,
			Projection: "scalar",
			WithProxy:  true,
		})
		s.waitForPublisher(1000)

		var err error
		check, err = s.CreateCheck(suiteCtx, &CheckOptions{
			ViaProxy:   true,
			Projection: "scalar",
		})
		Expect(err).NotTo(HaveOccurred())
	})

	It("starts healthy, with a complete view of the stream", func() {
		// A complete view means driftwatch has *seen an event for* every key,
		// and the condition is on events applied rather than on keys tracked
		// for a reason that cost a run.
		//
		// Bootstrap is Adopt, so a check created against a store the publisher
		// has already filled reads the whole keyspace out of Redis in one go
		// and reports the full key count within seconds. `TrackedKeys >= 5,500`
		// was therefore satisfied by adoption, not by the stream — and an
		// adopted key is a copy of what the store said, which agrees with the
		// store by construction and is skipped by the sweep until an event
		// refreshes it. The partition then landed five seconds in, on an oracle
		// that had no expectation of its own to be wrong about, and every key
		// driftwatch missed was one it had never independently seen.
		//
		// That is invisible rather than wrong, and it is exactly the state this
		// spec is supposed to rule out. Whether it happens depends on how long
		// the fixture took to come up, which is why it passed alone and failed
		// in a full suite.
		//
		// The keyspace is walked in order, so `keys` consecutive events cover
		// every key exactly once whatever point in the cycle the subscription
		// joined at.
		status := s.waitForCheck(check, converge,
			"the check never saw a full cycle of the stream through the proxy",
			func(st *CheckStatus) bool {
				return st.Phase == "Watching" &&
					st.EventsApplied >= keys && st.SettledKeys > 0
			})

		Expect(status.DivergentKeys).To(BeZero(),
			"divergence before the partition: %s", status.Summary())
		Expect(status.TotalMissingEvents()).To(BeZero(),
			"gaps before the partition: %+v", status.Publishers)
	})

	It("loses events when its subscription is cut", func() {
		By("severing driftwatch's link while the store keeps being written")
		Expect(s.CutSubscription(suiteCtx)).To(Succeed())

		// The materializer is unaffected throughout, so Redis keeps being
		// updated correctly while driftwatch is blind. What follows is a wait
		// on an observable condition rather than on the clock — §14.5 forbids
		// sleeping for synchronization, and a fixed wait here would be guessing
		// how long the socket takes to notice it has been cut.
		By("waiting for driftwatch to notice it is no longer receiving")
		s.waitForCheck(check, converge,
			"the subscription was cut and driftwatch never noticed — it went on "+
				"reporting a complete view of a stream it was no longer receiving",
			func(st *CheckStatus) bool {
				return st.ConditionIs("SourceConnected", "False") ||
					st.TotalMissingEvents() > 0 ||
					st.Phase == "Degraded"
			})

		// Noticing is not the fault. The fault is the events that went past
		// while driftwatch could not see them, and how many that is decides
		// everything below: 1,500 events over a 6,000-key cycle is a quarter of
		// the keyspace made stale, which survives the settlement window and the
		// sweep that has to find it.
		//
		// Restoring as soon as driftwatch noticed — which is what this did —
		// made the partition about five seconds long, and five seconds of it is
		// a tenth of the keyspace on a fast machine and rather less on a loaded
		// one. The scenario was measuring how quickly a socket reports a closed
		// connection.
		By("letting a known quantity of the stream go past while it is blind")
		s.waitForPublisherEmitted(blind)

		By("restoring the link so the gap becomes observable")
		Expect(s.RestoreSubscription(suiteCtx)).To(Succeed())
	})

	It("reports suspect keys and never confirms drift", func() {
		// The assertion §14.4 names. Suspect above zero says driftwatch noticed
		// the keys it can no longer vouch for; confirmed at zero says it
		// refused to blame the store for its own blindness.
		status := s.waitForCheck(check, converge,
			"driftwatch's subscription was cut, the store stayed correct, and no "+
				"key was ever marked suspect — either the loss went unnoticed or "+
				"the affected keys were never re-swept",
			func(st *CheckStatus) bool { return st.SuspectDivergentKeys > 0 })

		By("never blaming the store for driftwatch's own loss")
		Expect(status.DivergentKeys).To(BeZero(),
			"driftwatch confirmed %d divergent keys after losing events itself. "+
				"The store was written correctly throughout — every one of these "+
				"is a false positive, and this is the exact failure that makes a "+
				"consistency tool worthless: %s",
			status.DivergentKeys, status.Summary())

		By("holding that distinction rather than confirming a moment later")
		// Confirmation is two-phase, so a tool that merely delayed the wrong
		// answer would pass a single-instant assertion. This is the one that
		// catches it.
		//
		// Thirty seconds is chosen against the cycle: within it, some of the
		// keys the partition made stale are rewritten, which clears their
		// suspicion and refreshes the oracle in the same event. If driftwatch
		// were carrying a stale expectation forward past the point where it
		// stops calling the key suspect, this is where it would surface as
		// confirmed drift.
		s.holdsForCheck(check, 30*time.Second,
			"confirmed divergence appeared after the partition healed — the "+
				"suspect marking delayed the false positive rather than "+
				"preventing it",
			func(st *CheckStatus) bool { return st.DivergentKeys == 0 })
	})

	It("says why, in the status an operator reads", func() {
		status, err := s.Status(suiteCtx, check)
		Expect(err).NotTo(HaveOccurred())

		Expect(status.TotalMissingEvents()).To(BeNumerically(">", 0),
			"the publisher's sequence should show the gap: %+v", status.Publishers)

		integrity := status.Condition("SequenceIntegrity")
		Expect(integrity).NotTo(BeNil())
		Expect(integrity.Status).To(Equal("False"),
			"SequenceIntegrity should report the gap: %+v", integrity)
		Expect(integrity.Message).To(ContainSubstring("missing events"),
			"the condition should name the publisher and the count: %q",
			integrity.Message)

		// Degraded, not Failed. driftwatch is running and honest about not
		// seeing everything, which is a different thing from broken and must
		// not page anyone the same way.
		Expect(status.Phase).NotTo(Equal("Failed"),
			"losing events is not a failure of driftwatch: %s", status.Summary())
	})
})
