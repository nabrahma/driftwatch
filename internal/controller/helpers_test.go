package controller

import (
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/testr"

	"github.com/nabrahma/driftwatch/pkg/clock"
)

// testLogger routes the controller's lines into the test's own output, so a
// failure comes with the reconcile log that produced it rather than with
// nothing.
func testLogger(t *testing.T) logr.Logger {
	t.Helper()
	return testr.NewWithOptions(t, testr.Options{Verbosity: 1})
}

// testEpoch is where every fake clock in this package starts.
//
// A fixed, non-zero instant: a clock at the zero time makes every "is this
// timestamp set" check ambiguous, and a clock at time.Now makes assertions on
// durations depend on when the test ran.
var testEpoch = time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC)

func newFakeClock() clock.FakeClock { return clock.Fake(testEpoch) }
