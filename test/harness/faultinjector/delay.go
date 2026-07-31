package faultinjector

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/nabrahma/driftwatch/pkg/source"
)

// delayFault holds each message a jittered interval.
type delayFault struct {
	min, max time.Duration
	seed     int64

	rnd     *rand.Rand
	pending []heldMessage
	seq     uint64
}

// Delay holds each message for a jittered interval between min and max.
//
// This is the fault the settlement window exists for, and the one that decides
// whether driftwatch is usable. A materializer that is merely slow must never
// be reported as wrong, so a scenario that delays the whole stream and still
// produces confirmed findings has found a real false positive (§5.3).
func Delay(minDelay, maxDelay time.Duration, seed int64) Fault {
	if maxDelay < minDelay {
		minDelay, maxDelay = maxDelay, minDelay
	}
	f := &delayFault{min: minDelay, max: maxDelay, seed: seed}
	f.Reset()
	return f
}

func (f *delayFault) Name() string {
	return fmt.Sprintf("Delay(%s,%s,%d)", f.min, f.max, f.seed)
}

func (f *delayFault) Apply(msg source.RawMessage, _ func(source.RawMessage)) bool {
	f.seq++
	f.pending = append(f.pending, heldMessage{
		msg: msg,
		at:  msg.ObservedAt.Add(f.jitter()),
		seq: f.seq,
	})
	return false
}

// jitter picks a hold time in [min, max].
func (f *delayFault) jitter() time.Duration {
	span := f.max - f.min
	if span <= 0 {
		return f.min
	}
	return f.min + time.Duration(f.rnd.Int63n(int64(span)+1))
}

// Due releases every message whose hold has elapsed.
func (f *delayFault) Due(now time.Time, emit func(source.RawMessage)) {
	f.pending = releaseDue(f.pending, now, emit)
}

// NextDue reports when the next held message is due.
func (f *delayFault) NextDue() (time.Time, bool) { return nextDue(f.pending) }

func (f *delayFault) Reset() {
	f.rnd = rand.New(rand.NewSource(f.seed)) //nolint:gosec // fault injection, not security
	f.pending = nil
	f.seq = 0
}

// delayPublisherFault delays one publisher's stream and leaves the rest alone.
type delayPublisherFault struct {
	pub string
	d   time.Duration

	pending []heldMessage
	seq     uint64
}

// DelayPublisher holds only the named publisher's messages.
//
// One slow publisher among several is more interesting than a uniformly slow
// stream: the keys it owns fall behind while everything else stays current, so
// a settlement window computed from the whole distribution is too small for
// exactly the keys that need it most. It is also what a single struggling
// replica looks like from the outside.
func DelayPublisher(pub string, d time.Duration) Fault {
	return &delayPublisherFault{pub: pub, d: d}
}

func (f *delayPublisherFault) Name() string {
	return fmt.Sprintf("DelayPublisher(%s,%s)", f.pub, f.d)
}

func (f *delayPublisherFault) Apply(msg source.RawMessage, _ func(source.RawMessage)) bool {
	pub, ok := readString(msg.Payload, "publisher")
	if !ok || pub != f.pub {
		return true
	}

	f.seq++
	f.pending = append(f.pending, heldMessage{msg: msg, at: msg.ObservedAt.Add(f.d), seq: f.seq})
	return false
}

// Due releases the named publisher's messages once their hold has elapsed.
func (f *delayPublisherFault) Due(now time.Time, emit func(source.RawMessage)) {
	f.pending = releaseDue(f.pending, now, emit)
}

// NextDue reports when the next held message is due.
func (f *delayPublisherFault) NextDue() (time.Time, bool) { return nextDue(f.pending) }

func (f *delayPublisherFault) Reset() {
	f.pending = nil
	f.seq = 0
}
