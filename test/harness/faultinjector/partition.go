package faultinjector

import (
	"fmt"
	"time"

	"github.com/nabrahma/driftwatch/pkg/source"
)

// partitionFault drops everything for a window, then resumes.
type partitionFault struct {
	start    time.Duration
	duration time.Duration

	// began anchors the window to the first message rather than to a wall
	// clock, so the fault behaves the same whenever the scenario starts it.
	began time.Time
}

// Partition drops every message for a window beginning `start` after the first
// message, lasting `duration`.
//
// The difference between this and DropBurst is what the stream looks like
// either side. A burst removes a fixed count; a partition removes whatever
// happened to be published during an interval, so a busy publisher loses more
// than a quiet one — which is what a network partition actually does, and what
// makes the resulting gap sizes depend on traffic rather than on the fault.
func Partition(start, duration time.Duration) Fault {
	return &partitionFault{start: start, duration: duration}
}

func (f *partitionFault) Name() string { return fmt.Sprintf("Partition(%s,%s)", f.start, f.duration) }

func (f *partitionFault) Apply(msg source.RawMessage, _ func(source.RawMessage)) bool {
	if f.began.IsZero() {
		f.began = msg.ObservedAt
	}

	elapsed := msg.ObservedAt.Sub(f.began)
	partitioned := elapsed >= f.start && elapsed < f.start+f.duration
	return !partitioned
}

func (f *partitionFault) Reset() { f.began = time.Time{} }
