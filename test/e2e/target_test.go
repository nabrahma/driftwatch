//go:build e2e

package e2e

import (
	"strconv"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// E4 — TargetFlushAndRecover (§14.4).
//
// FLUSHDB mid-run: the store loses everything while driftwatch's view of the
// stream stays complete. That is unambiguous, reportable drift — driftwatch
// knows exactly what should be there and it is not.
//
// The half that matters more is the second one. Detection is the easy claim;
// what makes a tool usable day to day is that the number comes *back down* on
// its own once the store is repaired, without anybody restarting anything. A
// detector whose count only ever rises is one an operator learns to ignore
// after the first incident.
var _ = Describe("E4 TargetFlushAndRecover", Ordered, func() {
	var s *scenario
	var check string

	BeforeAll(func() {
		s = newScenario("e4-flush-recover", &FixtureOptions{Rate: 800, Keys: 1500})
		s.waitForPublisher(2000)

		var err error
		check, err = s.CreateCheck(suiteCtx, &CheckOptions{})
		Expect(err).NotTo(HaveOccurred())
	})

	It("starts from agreement", func() {
		s.waitForCheck(check, converge,
			"the check never reached a steady state before the flush",
			func(st *CheckStatus) bool {
				return st.Phase == "Watching" && st.TrackedKeys > 500 &&
					st.SettledKeys > 0 && st.DivergentKeys == 0
			})
	})

	It("confirms mass divergence after a flush", func() {
		By("stopping the materializer so the store stays flushed")
		// Without this the keyspace refills within a couple of sweeps and the
		// drift resolves before it can be confirmed — which is correct
		// behavior and makes the detection unobservable. E4 is about seeing
		// both halves, so the store is held still for the first one.
		Expect(s.ScaleMaterializer(suiteCtx, 0)).To(Succeed())

		before, err := s.RedisDBSize(suiteCtx)
		Expect(err).NotTo(HaveOccurred())
		Expect(before).To(BeNumerically(">", 500))

		By("flushing the store out from under driftwatch")
		_, err = s.RedisCommand(suiteCtx, "FLUSHDB")
		Expect(err).NotTo(HaveOccurred())

		after, err := s.RedisDBSize(suiteCtx)
		Expect(err).NotTo(HaveOccurred())
		Expect(after).To(BeZero(), "FLUSHDB left %d keys", after)

		status := s.waitForCheck(check, detect,
			"the entire store was flushed and driftwatch confirmed nothing",
			func(st *CheckStatus) bool { return st.DivergentKeys > 0 })

		Expect(status.DivergenceByCategory).To(HaveKey("missing_in_target"),
			"a flushed store is missing keys, not holding wrong ones: %v",
			status.DivergenceByCategory)

		// driftwatch's own subscription was never touched, so this is
		// reportable rather than suspect — the same distinction E2 and E3 turn
		// on.
		Expect(status.SuspectDivergentKeys).To(BeZero(),
			"the loss was the store's, not driftwatch's: %s", status.Summary())
	})

	It("resolves back to zero once the materializer catches up", func() {
		By("letting the materializer run again")
		Expect(s.ScaleMaterializer(suiteCtx, 1)).To(Succeed())

		// The half that makes the tool usable. Nothing tells driftwatch the
		// store has been repaired; each key resolves on the first sweep after
		// an event touches it again.
		status := s.waitForCheck(check, resolve,
			"the store was refilled and the divergence count never came back "+
				"down — a detector whose number only rises is one an operator "+
				"stops reading",
			func(st *CheckStatus) bool { return st.DivergentKeys == 0 })

		Expect(status.ConditionIs("DriftDetected", "False")).To(BeTrue(),
			"DriftDetected should have cleared: %+v", status.Condition("DriftDetected"))

		By("recording the episode rather than silently forgetting it")
		events, err := s.Events(suiteCtx, check)
		Expect(err).NotTo(HaveOccurred())
		Expect(events).To(ContainSubstring("DriftResolved"),
			"no DriftResolved event; the recovery left no trace an operator "+
				"could find afterwards:\n%s", events)
	})
})

// E5 — RedisEviction (§14.4).
//
// Redis is given a small `maxmemory` and an LRU policy, so it starts evicting
// under its own memory pressure. Keys vanish from the store that driftwatch
// expects to be there — which looks exactly like the materializer having failed
// to write them.
//
// The distinction matters because the remedy is completely different. Drift
// means something is broken and someone should investigate; eviction means the
// store is doing what it was configured to do and the *check* is misconfigured
// — probably `expiryPolicy: Strict` against a keyspace that has a memory bound.
//
// driftwatch reads Redis's own eviction counter during the sweep, so it can say
// which it is looking at rather than leaving an operator to guess.
var _ = Describe("E5 RedisEviction", Ordered, func() {
	var s *scenario
	var check string

	BeforeAll(func() {
		s = newScenario("e5-eviction", &FixtureOptions{
			Rate: 1500,
			// The keyspace has to outgrow the memory bound decisively, and
			// the first two attempts at this number did not.
			//
			// Measured rather than estimated, because the estimate was wrong
			// twice. This workload's 50,000 two-member sets plateau at a
			// reported used_memory of 5.21M, which includes Redis's own
			// overhead:
			//
			//   12mb -> 5.21M used, 0 evicted. Never evicts.
			//    6mb -> 5.21M used, 0 evicted. Still never evicts; the plateau
			//           sits just under the cap, which is the worst place for
			//           it because the scenario looks nearly right.
			//    4mb -> evicts continuously.
			//
			// A cap that is merely close to the steady-state size proves
			// nothing and takes ninety seconds to say so.
			Keys:                50_000,
			RedisMaxMemory:      "4mb",
			RedisEvictionPolicy: "allkeys-lru",
		})
		s.waitForPublisher(3000)

		var err error
		check, err = s.CreateCheck(suiteCtx, &CheckOptions{})
		Expect(err).NotTo(HaveOccurred())
	})

	It("makes Redis actually evict", func() {
		// Asserted from Redis rather than from driftwatch. If the store is not
		// really evicting then everything below is testing nothing, and that
		// would be invisible without this.
		Eventually(func() int {
			out, err := s.RedisCommand(suiteCtx, "INFO", "stats")
			if err != nil {
				return 0
			}
			return infoInt(out, "evicted_keys")
		}).WithTimeout(converge).WithPolling(poll).Should(BeNumerically(">", 0),
			"Redis never evicted anything, so this scenario would have proven "+
				"nothing — raise the key count or lower maxmemory")
	})

	It("observes the eviction rather than only the absence", func() {
		s.waitForCheck(check, converge,
			"the check never reached a steady state against an evicting store",
			func(st *CheckStatus) bool {
				return st.Phase == "Watching" || st.Phase == "Degraded"
			})

		// The number that separates "the store is broken" from "the store is
		// full". A sweep that finds mass absence while this is rising has an
		// explanation that is not drift, and driftwatch reads it from the store
		// itself rather than inferring it.
		Eventually(func() float64 {
			return managerMetric("driftwatch_target_evictions_observed_total")
		}).WithTimeout(converge).WithPolling(poll).Should(BeNumerically(">", 0),
			"Redis is evicting and driftwatch never noticed. Without this the "+
				"missing keys are indistinguishable from a broken materializer, "+
				"and the operator is sent to investigate the wrong thing")
	})

	It("keeps reporting honestly under memory pressure", func() {
		status, err := s.Status(suiteCtx, check)
		Expect(err).NotTo(HaveOccurred())

		// The store is genuinely smaller than the oracle expects, so findings
		// here are legitimate. What must not happen is driftwatch falling over
		// or going silent: an evicting store is a normal operating condition,
		// not a reason to stop auditing.
		Expect(status.Phase).NotTo(Equal("Failed"),
			"an evicting store is not a failure of driftwatch: %s", status.Summary())
		Expect(status.TargetReachable).To(BeTrue(),
			"the store is up and answering: %s", status.Summary())

		Expect(status.TargetKeyspaceSize).To(BeNumerically("<", int64(status.TrackedKeys)),
			"eviction should leave the store holding fewer keys than the oracle "+
				"expects, which is what makes this scenario meaningful: %s",
			status.Summary())
	})
})

// infoInt reads one integer field out of a Redis INFO response.
func infoInt(info, field string) int {
	for _, line := range strings.Split(info, "\n") {
		name, value, ok := strings.Cut(strings.TrimSpace(line), ":")
		if !ok || name != field {
			continue
		}
		n, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return 0
		}
		return n
	}
	return 0
}
