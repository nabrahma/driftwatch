//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// The suite is eight scenarios against one cluster, per §14.4.
//
// §14.1 is explicit that this stays small and reliable: six to eight scenarios
// that take five minutes and never flake, rather than forty that take
// twenty-five and flake weekly. Everything that can be tested faster elsewhere
// is — the fault matrix covers sixty failure modes in four seconds with a fake
// clock. What is left here is what unit tests structurally cannot reach: real
// ZMQ over TCP between pods, a real Redis, a real operator reconciling a real
// CRD, and the packaging that carries all of it.

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "driftwatch e2e")
}

// cluster is the shared cluster, built once for the whole suite.
var cluster *Cluster

// suiteCtx outlives any one scenario. Ginkgo's per-spec context is canceled
// when the spec ends, and teardown has to keep working after that.
var suiteCtx = context.Background()

// The timeouts scenarios poll with. §14.5 asks for per-assertion timeouts that
// are generous but bounded, and these are named rather than inline so that a
// scenario reads as what it asserts rather than as arithmetic.
const (
	// converge is how long a check gets to reach a steady state. Three sweeps
	// plus a settlement window, with room for a slow first bootstrap scan.
	converge = 90 * time.Second
	// detect is how long a fault gets to be confirmed: raised on one sweep,
	// confirmed on the next.
	detect = 60 * time.Second
	// resolve is how long a repaired fault gets to clear.
	resolve = 90 * time.Second
	// quick is for things that should already be true.
	quick = 30 * time.Second

	// poll is how often Eventually re-reads. Every read is a kubectl
	// round-trip, so a tighter interval buys nothing and costs API server load
	// that shows up as flakiness in whatever else is running.
	poll = 2 * time.Second
)

var _ = SynchronizedBeforeSuite(
	// Runs once, on process 1: create the cluster, build and load the images,
	// install the CRD and the manager. Everything here is setup and everything
	// here may retry (§14.5).
	func() []byte {
		started := time.Now()

		var err error
		cluster, err = EnsureCluster(suiteCtx)
		Expect(err).NotTo(HaveOccurred(), "bringing up the e2e cluster")

		fmt.Printf("e2e: cluster ready in %s\n", time.Since(started).Round(time.Second))
		return nil
	},
	// Runs on every process. The suite is serial today — the scenarios share
	// one manager and E6 asserts on its goroutine count — but the hook has to
	// exist for the shared state to be reachable if that ever changes.
	func([]byte) {
		if cluster == nil {
			cluster = &Cluster{Env: ReadEnv()}
		}
	},
)

var _ = SynchronizedAfterSuite(
	func() {},
	func() {
		if cluster == nil {
			return
		}
		Expect(cluster.Teardown(suiteCtx)).To(Succeed())
	},
)

// scenario is one test's fixture, wired into Ginkgo's lifecycle.
//
// Every scenario gets its own namespace with a generated name (§14.5), so no
// two can interfere — which matters here more than usual, because E4 flushes
// Redis, E5 fills it until it evicts and E7 deletes the publisher pod.
type scenario struct {
	*Fixture
}

// newScenario builds the fixture and registers cleanup.
//
// The DeferCleanup runs after the spec whether it passed, failed or panicked,
// and it collects diagnostics before it removes anything — an AfterEach that
// tore the namespace down first would delete the evidence.
func newScenario(name string, opts *FixtureOptions) *scenario {
	GinkgoHelper()

	started := time.Now()

	fixture, err := NewFixture(suiteCtx, name, opts)
	Expect(err).NotTo(HaveOccurred(), "bringing up the fixture for %s", name)

	AddReportEntry("namespace", fixture.Namespace)
	fmt.Printf("e2e: %s fixture ready in %s (%s)\n",
		name, time.Since(started).Round(time.Second), fixture.Namespace)

	s := &scenario{Fixture: fixture}

	DeferCleanup(func() {
		if CurrentSpecReport().Failed() {
			s.dump()
		}
		// Teardown deletes each DriftCheck and waits for the finalizer before
		// dropping the namespace, per §14.5.
		if err := s.Teardown(suiteCtx); err != nil {
			// Reported, not raised: a cleanup failure must not replace the real
			// failure with itself, and a namespace left behind costs nothing on
			// a cluster that is about to be deleted.
			fmt.Printf("e2e: cleanup of %s: %v\n", s.Namespace, err)
		}
	})

	return s
}

// dump collects the §14.3 artifact set for a failed spec.
func (s *scenario) dump() {
	report := CurrentSpecReport()

	collector, err := NewCollector(report.FullText(), s.Namespace, s.Checks()...)
	if err != nil {
		fmt.Printf("e2e: could not open a diagnostics directory: %v\n", err)
		return
	}

	message := fmt.Sprintf("%s\n\nfailed at %s\n\n%s\n",
		report.FullText(), report.FailureLocation(), report.Failure.Message)

	fmt.Print(collector.Collect(suiteCtx, message))
}

// waitForCheck polls a check's status until it satisfies a predicate.
//
// The whole suite goes through this rather than through bare Eventually, for
// one reason: the failure message. Gomega prints what the matcher gave up on,
// and a raw CheckStatus is forty lines of mostly zeroes; CheckStatus.Summary is
// the six numbers that say what the check was doing. A scenario that times out
// should say "phase=Watching drift=0 tracked=12 coverage=0.02", not print a
// struct.
func (s *scenario) waitForCheck(
	name string, timeout time.Duration, description string, want func(*CheckStatus) bool,
) *CheckStatus {
	GinkgoHelper()

	var last *CheckStatus

	Eventually(func() bool {
		status, err := s.Status(suiteCtx, name)
		if err != nil {
			return false
		}
		last = status
		return want(status)
	}).WithTimeout(timeout).WithPolling(poll).Should(BeTrue(),
		func() string {
			if last == nil {
				return fmt.Sprintf("%s: the check's status was never readable", description)
			}
			return fmt.Sprintf("%s\nlast status: %s", description, last.Summary())
		})

	return last
}

// holdsForCheck asserts a predicate stays true.
//
// §14.4 E1 asks for divergence to be zero and *stay* zero, which is a different
// claim from reaching zero once. A steady state that a single sweep happens to
// satisfy is not steady.
func (s *scenario) holdsForCheck(
	name string, duration time.Duration, description string, want func(*CheckStatus) bool,
) {
	GinkgoHelper()

	var last *CheckStatus

	Consistently(func() bool {
		status, err := s.Status(suiteCtx, name)
		if err != nil {
			// A transient read failure is not the property under test. Holding
			// the previous verdict rather than failing keeps a flaky API server
			// from failing a scenario about driftwatch.
			return last == nil || want(last)
		}
		last = status
		return want(status)
	}).WithTimeout(duration).WithPolling(poll).Should(BeTrue(),
		func() string {
			if last == nil {
				return fmt.Sprintf("%s: the check's status was never readable", description)
			}
			return fmt.Sprintf("%s\nlast status: %s", description, last.Summary())
		})
}

// settlesForCheck waits until a predicate has held continuously for `stable`,
// giving up after `timeout`.
//
// The difference from waitForCheck followed by holdsForCheck is the whole
// point. That pair asserts "the first moment the predicate is true is the
// moment it becomes permanently true", which is a claim about when the
// scenario happened to look rather than about the system, and it is wrong
// whenever the property under test is a limit rather than an instant.
//
// E7 is the case that produced this. It restarts the publisher and asserts
// divergence converges back to zero. Both driftwatch and the fixture's
// materializer are independent SUB sockets, and when the publisher is replaced
// they reconnect on their own schedules — so they do not miss the same events,
// and PUB/SUB has no replay to fix that with. The store ends up genuinely
// behind for the handful of keys that landed in the gap, and stays behind for
// one key cycle until the publisher rewrites them.
//
// Reaching zero before those keys have been swept is not convergence, it is
// the sweep not having got there yet. The old pair read that first zero as the
// answer, started its hold, and failed when the real state arrived: 33 passed,
// one failed, on `drift=14` against 3,148 tracked keys that healed seconds
// later.
//
// Requiring the zero to hold for a stretch longer than a key cycle keeps the
// assertion strong — a divergence that never resolves still fails — while
// letting convergence be what the word means.
func (s *scenario) settlesForCheck(
	name string, timeout, stable time.Duration, description string, want func(*CheckStatus) bool,
) *CheckStatus {
	GinkgoHelper()

	var last *CheckStatus
	var since time.Time
	var longest time.Duration

	Eventually(func() bool {
		status, err := s.Status(suiteCtx, name)
		if err != nil {
			return false
		}
		last = status

		if !want(status) {
			// Broken, so the clock restarts. longest is kept only so the
			// failure message can say how close it came, which is the
			// difference between "never converged" and "converged and would
			// not stay".
			since = time.Time{}
			return false
		}

		if since.IsZero() {
			since = time.Now()
		}
		if held := time.Since(since); held > longest {
			longest = held
		}
		return time.Since(since) >= stable
	}).WithTimeout(timeout).WithPolling(poll).Should(BeTrue(),
		func() string {
			if last == nil {
				return fmt.Sprintf("%s: the check's status was never readable", description)
			}
			return fmt.Sprintf(
				"%s\nheld for at most %s of the %s required\nlast status: %s",
				description, longest.Round(time.Second), stable, last.Summary())
		})

	return last
}

// waitForPublisher blocks until the publisher has emitted at least n events.
//
// Every scenario begins with this. A scenario that started asserting against a
// stream with nothing in it would fail on an empty oracle and read exactly like
// a detection bug.
func (s *scenario) waitForPublisher(n int) {
	GinkgoHelper()

	var emitted int

	Eventually(func() int {
		got, err := s.PublisherEmitted(suiteCtx)
		if err != nil {
			return 0
		}
		emitted = got
		return got
	}).WithTimeout(quick).WithPolling(poll).Should(BeNumerically(">=", n),
		func() string {
			return fmt.Sprintf(
				"the publisher emitted %d events, wanted at least %d — the stream "+
					"never started, so nothing below would have meant anything",
				emitted, n)
		})
}

// waitForPublisherEmitted blocks until the publisher has emitted n more events
// than it had when this was called, and reports the count it started from.
//
// E3 needs this because the size of its partition has to be a quantity rather
// than a race. The scenario cuts driftwatch's subscription and restores it as
// soon as driftwatch notices, and on a fast runner that whole exchange takes
// about five seconds — a blind window whose length is set by how quickly a
// socket reports a closed connection, which is not a property of driftwatch and
// varies by more than the assertion can absorb. Waiting for a known number of
// events to go past while the link is down makes the loss the same size on
// every machine.
func (s *scenario) waitForPublisherEmitted(n int) int {
	GinkgoHelper()

	from, err := s.PublisherEmitted(suiteCtx)
	Expect(err).NotTo(HaveOccurred(), "reading the publisher's position")

	s.waitForPublisher(from + n)
	return from
}

// managerGoroutines reads the manager's live goroutine count.
func managerGoroutines() int {
	GinkgoHelper()

	n, err := GoroutineCount(suiteCtx)
	Expect(err).NotTo(HaveOccurred(), "reading the manager's goroutine profile")
	return n
}

// managerMetric sums one metric across every label combination.
//
// Returns zero rather than failing when the metric is absent, so it can be
// polled: a counter that has never been incremented is not exported at all, and
// an Eventually waiting for one to appear must not fail on the first read.
func managerMetric(name string) float64 {
	v, err := MetricValue(suiteCtx, name)
	if err != nil {
		return 0
	}
	return v
}

// breakOnPurpose reports whether the suite was asked to fail a scenario
// deliberately, so the diagnostics dump can be inspected.
//
// §20 Phase 8 makes "verified by deliberately breaking a test and confirming
// the artifact dump is complete and useful" an exit criterion. Doing that by
// editing an assertion and remembering to put it back is how a broken
// assertion gets committed; this makes it a flag.
func breakOnPurpose() bool {
	return truthy(os.Getenv("DRIFTWATCH_E2E_BREAK"))
}
