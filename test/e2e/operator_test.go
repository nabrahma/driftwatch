//go:build e2e

package e2e

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// E6 — OperatorLifecycle (§14.4).
//
// Create, update, pause, resume, delete — all while traffic flows. The
// controller's unit and envtest suites cover each of these against a stub
// runnable; what only a real cluster can show is that the whole cycle leaves
// the manager exactly as it found it.
//
// The goroutine assertion is the point. Every runner owns a goroutine, a Redis
// connection pool and a ZMQ subscription, and every one of those is released by
// a code path that only runs on a real shutdown. A manager that leaked one per
// lifecycle would look completely healthy for weeks and then run out of file
// descriptors during an incident — which is precisely when nobody has time to
// work out why.
var _ = Describe("E6 OperatorLifecycle", Ordered, func() {
	var s *scenario
	var check string
	var baseline int

	BeforeAll(func() {
		s = newScenario("e6-operator-lifecycle", &FixtureOptions{Rate: 600, Keys: 1000})
		s.waitForPublisher(1000)
	})

	It("records the manager's goroutine count before anything is created", func() {
		// The manager has been up since the suite started and other scenarios
		// have created and deleted checks against it. The count is therefore
		// settled rather than pristine, which is what makes it a fair baseline:
		// the claim is that a lifecycle changes nothing, not that the manager
		// starts from a particular number.
		Eventually(managerGoroutines).
			WithTimeout(quick).WithPolling(poll).
			Should(BeNumerically(">", 0), "the goroutine profile was never readable")

		baseline = managerGoroutines()
		AddReportEntry("goroutines before", baseline)
		fmt.Printf("e2e: manager goroutines before the lifecycle: %d\n", baseline)
	})

	It("creates a check and reaches a steady state", func() {
		var err error
		check, err = s.CreateCheck(suiteCtx, &CheckOptions{})
		Expect(err).NotTo(HaveOccurred())

		s.waitForCheck(check, converge,
			"the created check never reached Watching",
			func(st *CheckStatus) bool {
				return st.Phase == "Watching" && st.TrackedKeys > 200
			})
	})

	It("applies a spec change without leaving two runners", func() {
		before, err := s.Status(suiteCtx, check)
		Expect(err).NotTo(HaveOccurred())

		By("changing sweepInterval and settlementWindow mid-traffic")
		Expect(s.PatchCheck(suiteCtx, check, `{"spec":{"policy":{`+
			`"sweepInterval":"8s",`+
			`"settlementWindow":{"mode":"static","static":"5s","min":"1s","max":"30s"}`+
			`}}}`)).To(Succeed())

		// observedGeneration catching up is the controller saying it has acted
		// on the new spec rather than merely received it.
		status := s.waitForCheck(check, converge,
			"the controller never observed the updated generation",
			func(st *CheckStatus) bool {
				return st.ObservedGeneration > before.ObservedGeneration &&
					st.Phase == "Watching"
			})

		By("still auditing correctly afterwards")
		Expect(status.DivergentKeys).To(BeZero(),
			"a spec change should restart the runner cleanly, not manufacture "+
				"drift: %s", status.Summary())

		// Two runners for one check would both sweep the same store and write
		// metrics under the same check label, so the count would flip between
		// two values. Holding zero for a spell is what would catch that.
		s.holdsForCheck(check, 20*time.Second,
			"divergence appeared after the spec change — the likeliest cause is "+
				"two runners for one check, each with its own oracle",
			func(st *CheckStatus) bool { return st.DivergentKeys == 0 })
	})

	It("pauses without discarding the oracle", func() {
		before, err := s.Status(suiteCtx, check)
		Expect(err).NotTo(HaveOccurred())
		Expect(before.TrackedKeys).To(BeNumerically(">", 200))

		Expect(s.PatchCheck(suiteCtx, check,
			`{"spec":{"policy":{"paused":true}}}`)).To(Succeed())

		status := s.waitForCheck(check, converge,
			"the check never reported Paused",
			func(st *CheckStatus) bool { return st.Phase == "Paused" })

		// The whole reason pause exists as a separate thing from delete.
		// Stopping ingestion instead would make every key suspect for as long
		// as it took to refill, so silencing a check for a deploy would cost an
		// hour of coverage afterwards.
		Expect(status.TrackedKeys).To(BeNumerically(">", 200),
			"pausing discarded the oracle; it should stop sweeping and keep "+
				"ingesting: %s", status.Summary())

		// Paused is a working state, not a broken one. Reporting otherwise
		// would light up every dashboard that gates on readiness the moment
		// somebody silenced one check.
		Expect(status.ConditionIs("Ready", "True")).To(BeTrue(),
			"a paused check is doing what it was told: %+v", status.Condition("Ready"))
	})

	It("resumes and keeps auditing", func() {
		Expect(s.PatchCheck(suiteCtx, check,
			`{"spec":{"policy":{"paused":false}}}`)).To(Succeed())

		status := s.waitForCheck(check, converge,
			"the check never resumed sweeping",
			func(st *CheckStatus) bool {
				return st.Phase == "Watching" && st.SettledKeys > 0
			})

		Expect(status.DivergentKeys).To(BeZero(),
			"resuming manufactured drift: %s", status.Summary())
	})

	It("deletes cleanly, and leaves the manager as it found it", func() {
		By("deleting the check and waiting for the finalizer")
		Expect(s.DeleteCheck(suiteCtx, check)).To(Succeed())

		// The assertion §14.4 names. The runner's goroutine, its connection
		// pool and its subscription are all released on the shutdown path, and
		// this is the only thing in the repository that proves that path
		// actually runs in a real deployment.
		//
		// Eventually rather than a single read: a goroutine told to stop takes
		// a scheduling quantum to exit, and failing on that would be a flaky
		// test rather than a leak.
		By("comparing the goroutine count with the baseline")
		var last int
		Eventually(func() int {
			last = managerGoroutines()
			return last
		}).WithTimeout(quick).WithPolling(poll).
			Should(BeNumerically("<=", baseline+2), func() string {
				return fmt.Sprintf(
					"the manager held %d goroutines before the lifecycle and %d "+
						"after, having created, updated, paused, resumed and "+
						"deleted one check.\n\n"+
						"Every runner owns a goroutine, a Redis connection pool "+
						"and a ZMQ subscription. A manager that leaks one per "+
						"lifecycle looks healthy for weeks and then runs out of "+
						"file descriptors during an incident.",
					baseline, last)
			})

		AddReportEntry("goroutines after", last)
		fmt.Printf("e2e: manager goroutines after the lifecycle: %d (baseline %d)\n",
			last, baseline)

		// The tolerance is +2 rather than exact. The API server's watch
		// connections and client-go's own workers move by one or two
		// independently of anything driftwatch does, and an exact assertion
		// would fail on that rather than on a leak. A leaked runner is worth
		// far more than two.
	})
})
