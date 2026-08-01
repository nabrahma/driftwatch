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

	BeforeAll(func() {
		// The only scenario that needs toxiproxy: driftwatch connects through
		// it, the materializer connects straight to the publisher. That
		// asymmetry is the entire fault.
		s = newScenario("e3-self-loss", FixtureOptions{
			Rate:      600,
			Keys:      2000,
			WithProxy: true,
		})
		s.waitForPublisher(1000)

		var err error
		check, err = s.CreateCheck(suiteCtx, CheckOptions{ViaProxy: true})
		Expect(err).NotTo(HaveOccurred())
	})

	It("starts healthy, with a complete view of the stream", func() {
		status := s.waitForCheck(check, converge,
			"the check never reached a steady state through the proxy",
			func(st *CheckStatus) bool {
				return st.Phase == "Watching" && st.TrackedKeys > 300 && st.SettledKeys > 0
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
		// sleeping for synchronisation, and a fixed wait here would be guessing
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
