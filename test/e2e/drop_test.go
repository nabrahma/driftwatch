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

	// The range the materializer never writes, and the keyspace that makes it
	// observable.
	//
	// The original comment had the right idea and the wrong numbers: at 3,000
	// keys and 400/s a key comes round every 7.5 seconds, so the ~400 keys the
	// skipped range touches were all rewritten — correctly, by a materializer
	// that had resumed — within 7.5 seconds. Detection needs a settlement
	// window plus a confirmation cycle, which is longer than that, so the drift
	// healed before it could be confirmed and the scenario timed out reporting
	// zero divergent keys.
	//
	// The drift was real and driftwatch was right to stop reporting it. The
	// scenario simply never gave it long enough to be seen.
	//
	// 20,000 keys at 400/s was a 50-second cycle and still failed on CI, at
	// coverage 0.9263 with the keyspace only two thirds populated: detection on
	// two cores took longer than the workload took to heal.
	//
	// 12,000 keys at 150/s is 80 seconds, against a 60-second detection budget.
	// The margin is the point. A skipped key has to stay wrong for longer than
	// a settlement window plus a sweep plus a confirmation, on the slowest
	// machine this runs on, or the scenario measures the runner.
	// The range starts late enough that driftwatch is certainly subscribed
	// before it goes past, which 500 was not.
	//
	// The ordering below is the scenario, and it was being hoped for rather
	// than established. At 150/sec, sequence 500 arrives 3.3 seconds after the
	// publisher's first event — and the publisher starts emitting as soon as
	// its container does, well before the fixture reports every deployment
	// Available and the check is even applied. The manager then has to notice
	// the new DriftCheck, start a runner, dial ZMQ and subscribe, all inside
	// those 3.3 seconds.
	//
	// When it lost that race driftwatch never saw the skipped events either,
	// so its oracle never learned those keys, so there was nothing to disagree
	// about and the scenario failed reporting drift 0 — which reads as the
	// detection being broken rather than as the fault never having been
	// injected. That is what run 4 of 5 hit: tracked 9,580 against 9,198 in the
	// store, the 382 keys sitting right there in the difference, and driftwatch
	// correctly silent about keys it had never been told existed.
	//
	// 3,000 is twenty seconds in. The precondition is asserted below rather
	// than assumed, so a scenario that still loses the race says so.
	const (
		skipFrom = 3000
		skipTo   = 3400
	)

	BeforeAll(func() {
		s = newScenario("e2-dropped-event", &FixtureOptions{
			Rate:     150,
			Keys:     12_000,
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

		// Subscribed, established rather than assumed. Applied events are the
		// only proof from outside that the socket is connected and the pipeline
		// is running; the phase reaching Watching says the sweeper started,
		// which can happen off an adopted keyspace with no subscription at all.
		s.waitForCheck(check, converge,
			"the check never began ingesting, so it was never watching the "+
				"stream the materializer is about to drop events from",
			func(st *CheckStatus) bool { return st.EventsApplied > 0 })

		// And subscribed *in time*. If the publisher is already past the
		// skipped range then driftwatch missed those events too, its oracle
		// never learns those keys, and the scenario would go on to assert on a
		// fault that was never injected — failing as though detection were
		// broken. Better to fail here, saying what actually went wrong.
		emitted, err := s.PublisherEmitted(suiteCtx)
		Expect(err).NotTo(HaveOccurred())
		Expect(emitted).To(BeNumerically("<", skipFrom),
			"the publisher reached sequence %d before driftwatch subscribed, so "+
				"the events the materializer is told to skip (%d-%d) were never "+
				"seen by driftwatch either. Nothing would diverge and the "+
				"scenario would prove nothing",
			emitted, skipFrom, skipTo)

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
