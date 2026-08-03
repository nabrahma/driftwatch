//go:build e2e

package e2e

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// E7 — PublisherRestart (§14.4).
//
// The publisher pod is deleted and rescheduled. The replacement comes back with
// a sequence number that starts again at 1 and a higher epoch.
//
// The trap is arithmetic. A publisher that was at sequence 900,000 restarts at
// 1, and a sequence tracker that only ever compares "expected next" against
// "received" concludes that 899,999 events are missing — and marks essentially
// the entire keyspace suspect on the strength of it. The check goes silent for
// as long as it takes to rebuild, at exactly the moment a restarting publisher
// makes an audit most valuable.
//
// §15 row 21 covers the same fault in-process with a fake clock. This is the
// version where the pod really is deleted, really is rescheduled by Kubernetes,
// and really does come back on a new IP that the subscription has to re-resolve.
var _ = Describe("E7 PublisherRestart", Ordered, func() {
	var s *scenario
	var check string
	var epochBefore int64

	BeforeAll(func() {
		// 4,000 keys at 200/sec: a 20-second cycle, clear of the 3-second
		// settlement window, and 3,000 events reached in fifteen seconds so the
		// sequence position before the restart is still high.
		s = newScenario("e7-publisher-restart", &FixtureOptions{Rate: 200, Keys: 4000})
		s.waitForPublisher(3000)

		var err error
		check, err = s.CreateCheck(suiteCtx, &CheckOptions{})
		Expect(err).NotTo(HaveOccurred())
	})

	It("builds a high sequence position before the restart", func() {
		status := s.waitForCheck(check, converge,
			"the check never reached a steady state before the restart",
			func(st *CheckStatus) bool {
				return st.Phase == "Watching" && st.TrackedKeys > 300 &&
					len(st.Publishers) > 0 && st.Publishers[0].HighWaterMark > 1000
			})

		Expect(status.DivergentKeys).To(BeZero(),
			"divergence before the restart: %s", status.Summary())

		var err error
		epochBefore, err = s.PublisherEpoch(suiteCtx)
		Expect(err).NotTo(HaveOccurred())

		AddReportEntry("high-water mark before", status.Publishers[0].HighWaterMark)
	})

	It("survives the pod being deleted and rescheduled", func() {
		By("deleting the publisher pod")
		Expect(s.RestartPublisher(suiteCtx)).To(Succeed())

		// The replacement derives its epoch from its own start time, so it is
		// necessarily higher. Waiting on that rather than on the clock is what
		// tells the scenario the new pod is actually the one publishing.
		Eventually(func() int64 {
			epoch, err := s.PublisherEpoch(suiteCtx)
			if err != nil {
				return 0
			}
			return epoch
		}).WithTimeout(converge).WithPolling(poll).
			Should(BeNumerically(">", epochBefore),
				"the rescheduled publisher never came back with a new epoch")

		s.waitForPublisher(500)
	})

	It("does not invent a gap of nine hundred thousand events", func() {
		// The assertion §14.4 singles out, and the one that catches the naive
		// sequence tracker.
		status := s.waitForCheck(check, converge,
			"the check never settled after the publisher restarted",
			func(st *CheckStatus) bool {
				return st.Phase == "Watching" || st.Phase == "Degraded"
			})

		Expect(status.TotalMissingEvents()).To(BeNumerically("<", 1000),
			"driftwatch believes %d events are missing after a publisher "+
				"restarted at sequence 1. The publisher did not lose anything — "+
				"it restarted, and a tracker that reads a sequence reset as a "+
				"gap marks the whole keyspace suspect for no reason: %+v",
			status.TotalMissingEvents(), status.Publishers)
	})

	It("records the restart as what it is", func() {
		// The restart should be counted, so an operator can see why the
		// sequence position moved backwards. Silence here would be worse than
		// a false gap: the position would look impossible with no explanation.
		Eventually(func() float64 {
			return managerMetric("driftwatch_publisher_restarts_total")
		}).WithTimeout(converge).WithPolling(poll).Should(BeNumerically(">", 0),
			"the publisher restarted and nothing recorded it")
	})

	It("converges back to zero divergence", func() {
		// The store was written correctly throughout — the materializer never
		// stopped — so once driftwatch has caught up there is nothing to report.
		status := s.waitForCheck(check, resolve,
			"the check never returned to zero divergence after the restart",
			func(st *CheckStatus) bool { return st.DivergentKeys == 0 })

		s.holdsForCheck(check, 20*time.Second,
			"divergence reappeared after the restart converged",
			func(st *CheckStatus) bool { return st.DivergentKeys == 0 })

		Expect(status.Phase).NotTo(Equal("Failed"),
			"a publisher restart is not a failure of driftwatch: %s", status.Summary())
	})
})

// E8 — MultiCheck (§14.4).
//
// Two DriftChecks over the same Redis, with different key patterns. This is
// legal and reasonable: one team's index and another's counters can live in one
// store, and each wants its own audit with its own policy.
//
// Two things have to hold. The metrics must separate cleanly by the `check`
// label, or an operator cannot tell which audit is reporting what. And a fault
// injected into one must not touch the other — which is really a statement
// about the runner registry: two checks in one manager process, each with its
// own oracle, its own sweeper and its own connection pool.
var _ = Describe("E8 MultiCheck", Ordered, func() {
	var s *scenario
	var owned, foreign string

	BeforeAll(func() {
		// 3,000 keys at 150/sec, a 20-second cycle. The owned check has to have
		// settled keys to notice its keyspace being flushed, and at 1,500 keys
		// and 800/sec the cycle was shorter than the settlement window, so it
		// had none.
		s = newScenario("e8-multi-check", &FixtureOptions{Rate: 150, Keys: 3000})
		s.waitForPublisher(2000)

		var err error

		// The keyspace the publisher actually writes.
		owned, err = s.CreateCheck(suiteCtx, &CheckOptions{
			Name:       "owned",
			KeyPattern: "block:*",
		})
		Expect(err).NotTo(HaveOccurred())

		// A pattern nothing produces, with a scalar projection rather than a
		// set. Two differences at once on purpose: the checks must be separable
		// by pattern *and* must not share projection state.
		foreign, err = s.CreateCheck(suiteCtx, &CheckOptions{
			Name:       "foreign",
			KeyPattern: "ledger:*",
			Projection: "scalar",
		})
		Expect(err).NotTo(HaveOccurred())
	})

	It("runs both checks concurrently in one manager", func() {
		s.waitForCheck(owned, converge,
			"the owned check never reached a steady state",
			func(st *CheckStatus) bool {
				return st.Phase == "Watching" && st.TrackedKeys > 300
			})

		s.waitForCheck(foreign, converge,
			"the foreign check never reached a steady state",
			func(st *CheckStatus) bool { return st.Phase == "Watching" })
	})

	It("keeps each check's verdict to itself", func() {
		ownedStatus, err := s.Status(suiteCtx, owned)
		Expect(err).NotTo(HaveOccurred())
		foreignStatus, err := s.Status(suiteCtx, foreign)
		Expect(err).NotTo(HaveOccurred())

		Expect(ownedStatus.DivergentKeys).To(BeZero(),
			"the owned check reported drift on an unbroken stream: %s",
			ownedStatus.Summary())
		Expect(foreignStatus.DivergentKeys).To(BeZero(),
			"the foreign check reported drift over a keyspace nothing writes: %s",
			foreignStatus.Summary())

		// The keyspaces really are different. Without this the separation
		// assertions below would pass trivially — two checks watching the same
		// keys would agree for the wrong reason.
		Expect(ownedStatus.TrackedKeys).To(BeNumerically(">", 300),
			"the owned check should be tracking the published keyspace")
		Expect(foreignStatus.TrackedKeys).To(BeZero(),
			"nothing writes ledger:*, so the foreign check should track nothing: %s",
			foreignStatus.Summary())
	})

	It("separates the metrics by check label", func() {
		raw, err := ScrapeMetrics(suiteCtx)
		Expect(err).NotTo(HaveOccurred())

		// §12's cardinality discipline puts the check name on every series.
		// Without it, two checks in one process would sum into one number and
		// an operator would have no way to tell whose drift they were reading.
		Expect(raw).To(ContainSubstring(`check="`+s.Namespace+`/`+owned+`"`),
			"no series carried the owned check's label")
		Expect(raw).To(ContainSubstring(`check="`+s.Namespace+`/`+foreign+`"`),
			"no series carried the foreign check's label")
	})

	It("confines a fault to the check that owns the keyspace", func() {
		By("flushing the store, which only the owned check watches")
		Expect(s.ScaleMaterializer(suiteCtx, 0)).To(Succeed())

		_, err := s.RedisCommand(suiteCtx, "FLUSHDB")
		Expect(err).NotTo(HaveOccurred())

		s.waitForCheck(owned, detect,
			"the owned check never noticed its keyspace had been flushed",
			func(st *CheckStatus) bool { return st.DivergentKeys > 0 })

		By("leaving the other check completely untouched")
		foreignStatus, err := s.Status(suiteCtx, foreign)
		Expect(err).NotTo(HaveOccurred())

		Expect(foreignStatus.DivergentKeys).To(BeZero(),
			"a fault in one check produced findings in another. The two share a "+
				"manager process and a Redis, and nothing else — not an oracle, "+
				"not a sweeper, not a connection pool: %s", foreignStatus.Summary())
		Expect(foreignStatus.Phase).NotTo(Equal("Failed"),
			"the untouched check should be unaffected: %s", foreignStatus.Summary())

		// Restored so the fixture teardown is not racing a stopped
		// materializer.
		Expect(s.ScaleMaterializer(suiteCtx, 1)).To(Succeed())
	})
})
