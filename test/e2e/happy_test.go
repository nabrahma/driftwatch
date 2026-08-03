//go:build e2e

package e2e

import (
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// E1 — HappyPath (§14.4).
//
// The whole real path with nothing broken: a ZMQ publisher over TCP, a
// materializer that is a different process subscribing to it, a real Redis, and
// driftwatch as a third independent subscriber auditing the result through a
// real CRD reconciled by a real operator.
//
// The assertion that matters is not that divergence reaches zero. It is that
// divergence *stays* zero while a couple of thousand events a second flow, at a
// coverage ratio high enough for that to be a statement about the store rather
// than about a corner of it. A tool that reports drift when nothing is wrong
// gets switched off in a week, and this is the scenario that says it does not.
var _ = Describe("E1 HappyPath", Ordered, func() {
	var s *scenario
	var check string

	// 12,000 keys at 200/sec is a 60-second cycle, against a 3-second
	// settlement window: 95% of the keyspace is settled at any moment, and the
	// oracle finishes filling 60 seconds in.
	const (
		keys = 12_000
		rate = 200
	)

	BeforeAll(func() {
		// The keyspace has to be large enough that keys can settle, and the
		// original 3,000 was not. The arithmetic is the whole scenario:
		//
		//   a key is rewritten every  keys / rate  seconds
		//   a key is settled once it has been quiet for W
		//   so the settled fraction is  1 - W / (keys / rate)
		//
		// At 3,000 keys and 2,000 events/sec that cycle is 1.5 seconds against
		// a 3-second window, so a key is rewritten twice over before it could
		// ever settle. Coverage cannot exceed a few tenths no matter how well
		// driftwatch works, and the assertion below asks for 0.90 — which made
		// this scenario unsatisfiable by construction rather than by defect.
		//
		// There is a second constraint, and 60,000 keys at 1,000/sec missed it:
		// the oracle also has to *fill* before the assertion runs, and coverage
		// is measured against tracked keys, which is still growing until every
		// key has been touched once. That run reached 0.8982 with tracked at
		// 39,964 of 60,000 — the ratio was right and the keyspace was not yet
		// populated.
		//
		// So the cycle has to be long enough to settle and short enough to fill
		// inside the scenario's budget.
		//
		// The third constraint is the runner. 40,000 keys at 800/sec satisfied
		// both of the above and then failed on CI with
		// "TargetAvailable Status:False Reason:Unreachable" — Redis timing out
		// under a sweep reading 40,000 keys in 500-key batches, every five
		// seconds, on two shared cores alongside a publisher, a materializer and
		// the manager. Correct behavior from driftwatch, reporting a store it
		// genuinely could not reach.
		//
		// Lowering the rate fixes both at once, which raising the keyspace does
		// not: the cycle is keys/rate, so 12,000 at 200/sec is the same 60
		// seconds while doing a quarter of the work and sweeping a third of the
		// keys. 95% settled, populated at 60 seconds, and light enough that the
		// store stays reachable.
		//
		// All of which was arithmetic about a cycle the publisher did not have.
		// It drew keys uniformly at random, so the keyspace filled as
		// 1-e^(-rate*t/keys) and never finished filling: the run that failed
		// here was at 8,556 tracked keys and still gaining about 57 a second.
		// Coverage is last-sweep-compared over currently-tracked, so a growing
		// denominator drags it down on its own — 7,216 over 8,556 is 0.8433, and
		// the sweep had not skipped anything like 1,340 keys. The publisher now
		// walks the keyspace in order, which is what every one of these
		// paragraphs assumed.
		s = newScenario("e1-happy-path", &FixtureOptions{Rate: rate, Keys: keys})
		s.waitForPublisher(2000)

		var err error
		check, err = s.CreateCheck(suiteCtx, &CheckOptions{})
		Expect(err).NotTo(HaveOccurred())
	})

	It("reaches a steady state with no divergence", func() {
		// A full cycle of the stream, measured in events applied rather than in
		// keys tracked. The coverage assertion below is a ratio against tracked
		// keys, and running it while the oracle is still filling measures the
		// filling rather than the sweep.
		//
		// Keys tracked is the wrong way to wait for that. Bootstrap is Adopt,
		// so a check created against a store the publisher has already filled
		// reports the whole key count within seconds without having seen a
		// single event — and an adopted key is skipped by the sweep until an
		// event refreshes it, so coverage stays low for a full cycle after the
		// gate has already opened. Events applied waits for the thing the
		// assertion actually depends on.
		status := s.waitForCheck(check, converge,
			"the check never saw a full cycle of the stream",
			func(st *CheckStatus) bool {
				return st.Phase == "Watching" &&
					st.EventsApplied >= keys && st.SettledKeys > 0
			})

		Expect(status.DivergentKeys).To(BeZero(),
			"nothing is broken, so nothing may be reported: %s", status.Summary())
	})

	It("holds zero divergence while traffic continues", func() {
		// §14.4 asks for 60 seconds. Held rather than sampled: a steady state a
		// single sweep happens to satisfy is not a steady state, and this is
		// the assertion behind the project's headline claim.
		s.holdsForCheck(check, 60*time.Second,
			"divergence appeared with nothing broken — that is a false positive, "+
				"which is the failure mode driftwatch exists not to have",
			func(st *CheckStatus) bool {
				return st.DivergentKeys == 0 && st.SuspectDivergentKeys == 0
			})
	})

	It("asserts on essentially the whole keyspace", func() {
		status, err := s.Status(suiteCtx, check)
		Expect(err).NotTo(HaveOccurred())

		coverage, err := strconv.ParseFloat(status.CoverageRatio, 64)
		Expect(err).NotTo(HaveOccurred(), "coverage ratio %q", status.CoverageRatio)

		// The number that decides what the zero above is worth. Zero divergence
		// at 3% coverage is a statement about 3% of the store; §12.1 makes this
		// the panel that stops the dashboard lying, and the argument is the same
		// here.
		Expect(coverage).To(BeNumerically(">", 0.90),
			"coverage was %.4f: a zero-divergence verdict over that fraction of "+
				"the keyspace is not the reassurance it looks like (%s)",
			coverage, status.Summary())
	})

	It("agrees with what Redis actually holds", func() {
		// Read from the store directly rather than trusting driftwatch's view
		// of it. Everything above came from driftwatch; this is the one
		// assertion that comes from somewhere else.
		dbsize, err := s.RedisDBSize(suiteCtx)
		Expect(err).NotTo(HaveOccurred())

		status, err := s.Status(suiteCtx, check)
		Expect(err).NotTo(HaveOccurred())

		Expect(dbsize).To(BeNumerically(">", 500),
			"the materializer wrote almost nothing; the stream never reached Redis")

		// Within 10%: the oracle and the store are read at slightly different
		// moments while 2,000 events a second flow, so exact equality would be
		// asserting that time stood still.
		Expect(float64(status.TrackedKeys)).To(
			BeNumerically("~", float64(dbsize), float64(dbsize)*0.1),
			"the oracle tracks %d keys and Redis holds %d; they should agree",
			status.TrackedKeys, dbsize)
	})

	It("reports the conditions an operator gates on", func() {
		status, err := s.Status(suiteCtx, check)
		Expect(err).NotTo(HaveOccurred())

		Expect(status.ConditionIs("Ready", "True")).To(BeTrue(),
			"Ready was not true: %+v", status.Condition("Ready"))
		Expect(status.ConditionIs("SourceConnected", "True")).To(BeTrue(),
			"the subscription was not connected: %+v", status.Condition("SourceConnected"))
		Expect(status.ConditionIs("TargetAvailable", "True")).To(BeTrue(),
			"the store was not reachable: %+v", status.Condition("TargetAvailable"))
		Expect(status.ConditionIs("DriftDetected", "False")).To(BeTrue(),
			"DriftDetected was true with nothing broken: %+v",
			status.Condition("DriftDetected"))
		Expect(status.ConditionIs("SequenceIntegrity", "True")).To(BeTrue(),
			"driftwatch believes it missed events on an unbroken stream: %+v",
			status.Condition("SequenceIntegrity"))

		Expect(status.EventsApplied).To(BeNumerically(">", 1000),
			"only %d events reached the oracle", status.EventsApplied)
		Expect(status.EventsDropped).To(BeZero(),
			"driftwatch dropped %d events on an unbroken stream", status.EventsDropped)

		// The deliberate-failure hook. The diagnostics collector is only
		// worth anything if it has been verified by breaking a test on
		// purpose, and doing that by editing an assertion is how a broken
		// assertion gets committed.
		if breakOnPurpose() {
			Expect(status.DivergentKeys).To(Equal(424242),
				"DRIFTWATCH_E2E_BREAK is set: this failure is deliberate and "+
					"exists so the artifact dump can be inspected")
		}
	})
})
