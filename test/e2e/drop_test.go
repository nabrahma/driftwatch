//go:build e2e

package e2e

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// E2 — DroppedEventDetected (§14.4).
//
// The materializer is told to skip a sequence range. driftwatch sees those
// events and Redis never gets them, so the store genuinely disagrees with what
// the stream says it should hold.
//
// This is the mirror image of E3. Here the loss is on the *materializer's* side,
// so driftwatch's own view is complete and it can stand behind what it reports:
// confirmed divergence, a Kubernetes Event, and a rising metric. In E3 the loss
// is on driftwatch's own subscription and the same underlying disagreement has
// to come out as suspect instead. Getting these two the wrong way round is the
// single most important thing driftwatch can get wrong, which is why they are
// adjacent scenarios rather than one.
var _ = Describe("E2 DroppedEventDetected", Ordered, func() {
	var s *scenario
	var check string

	// The range the materializer never writes. Wide enough that the keys it
	// touches are unlikely to be rewritten before confirmation — with 3,000
	// keys at 400/s each key comes round every 7.5s, so a narrow range would
	// heal before a sweep could confirm it.
	const (
		skipFrom = 500
		skipTo   = 900
	)

	BeforeAll(func() {
		s = newScenario("e2-dropped-event", &FixtureOptions{
			Rate:     400,
			Keys:     3000,
			SkipFrom: skipFrom,
			SkipTo:   skipTo,
		})
		// The check goes up first, and this ordering is the scenario.
		//
		// The materializer skips sequence numbers 500-900. If driftwatch only
		// subscribed after those had gone past, it would not have seen them
		// either — its oracle would agree with the store, correctly, and there
		// would be nothing to detect. The divergence exists only because
		// driftwatch is watching while the materializer is not writing.
		var err error
		check, err = s.CreateCheck(suiteCtx, &CheckOptions{})
		Expect(err).NotTo(HaveOccurred())

		// Now let the stream run past the skipped range.
		s.waitForPublisher(skipTo + 1000)
	})

	It("confirms divergence the materializer caused", func() {
		status := s.waitForCheck(check, detect,
			"the materializer skipped 400 events and driftwatch never confirmed "+
				"a single divergent key — this is the detection the whole tool is for",
			func(st *CheckStatus) bool { return st.DivergentKeys > 0 })

		By("reporting it as confirmed rather than suspect")
		// The distinction §23 A7 rests on. driftwatch's own subscription was
		// never interrupted here, so its view of the stream is complete and it
		// is entitled to say the store is wrong.
		Expect(status.SuspectDivergentKeys).To(BeZero(),
			"driftwatch saw every event, so nothing may be downgraded to "+
				"suspect: %s", status.Summary())
	})

	It("names the category rather than only the count", func() {
		status, err := s.Status(suiteCtx, check)
		Expect(err).NotTo(HaveOccurred())

		Expect(status.DivergenceByCategory).NotTo(BeEmpty(),
			"the count is %d and the breakdown is empty, so an operator has a "+
				"number and no way to guess the cause", status.DivergentKeys)

		// The materializer never created these keys at all, so the store is
		// missing them outright. member_mismatch would be the category if it
		// had written the key with the wrong membership.
		Expect(status.DivergenceByCategory).To(HaveKey("missing_in_target"),
			"expected missing_in_target, got %v", status.DivergenceByCategory)
	})

	It("says its own view of the stream was complete", func() {
		status, err := s.Status(suiteCtx, check)
		Expect(err).NotTo(HaveOccurred())

		Expect(status.TotalMissingEvents()).To(BeZero(),
			"driftwatch believes it missed events, but the loss was on the "+
				"materializer's side and its own subscription was never cut: %+v",
			status.Publishers)

		Expect(status.ConditionIs("SequenceIntegrity", "True")).To(BeTrue(),
			"SequenceIntegrity should hold: %+v", status.Condition("SequenceIntegrity"))
		Expect(status.ConditionIs("DriftDetected", "True")).To(BeTrue(),
			"DriftDetected should be true: %+v", status.Condition("DriftDetected"))
	})

	It("emits a Kubernetes Event an operator would see first", func() {
		// §10.3 lists drift detection among the eight events, and
		// `kubectl describe` is where an operator looks before a dashboard.
		Eventually(func() string {
			out, err := s.Events(suiteCtx, check)
			if err != nil {
				return ""
			}
			return out
		}).WithTimeout(quick).WithPolling(poll).Should(
			ContainSubstring("DriftDetected"),
			"no DriftDetected event was recorded on the object")
	})
})
