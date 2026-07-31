package faultinjector

import (
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/nabrahma/driftwatch/pkg/source"
)

// duplicateFault re-emits a copy of some messages after a delay.
type duplicateFault struct {
	rate  float64
	delay time.Duration
	seed  int64

	rnd     *rand.Rand
	pending []heldMessage
	seq     uint64
}

// heldMessage is a message waiting for its release time.
type heldMessage struct {
	msg source.RawMessage
	at  time.Time
	// seq breaks ties so that messages released at the same instant come out in
	// a fixed order. Without it a clock advance past several at once would emit
	// them in whatever order the slice happened to hold, and the fault would
	// stop being reproducible.
	seq uint64
}

// Duplicate re-emits a copy of each message with probability rate, after delay.
//
// Redelivery is normal on an at-least-once transport, so driftwatch must not
// treat it as anything at all: applying the same event twice has to leave the
// oracle exactly as applying it once did. That is invariant I1, and this is the
// fault that tests it against a real stream rather than a unit case.
func Duplicate(rate float64, delay time.Duration, seed int64) Fault {
	f := &duplicateFault{rate: rate, delay: delay, seed: seed}
	f.Reset()
	return f
}

func (f *duplicateFault) Name() string {
	return fmt.Sprintf("Duplicate(%.4f,%s,%d)", f.rate, f.delay, f.seed)
}

func (f *duplicateFault) Apply(msg source.RawMessage, _ func(source.RawMessage)) bool {
	if f.rnd.Float64() >= f.rate {
		return true
	}

	f.seq++
	// The copy owns its payload. A duplicate sharing the original's bytes would
	// be corrupted along with it by any later fault in the chain.
	copied := source.RawMessage{
		Topic:      msg.Topic,
		Payload:    clone(msg.Payload),
		ObservedAt: msg.ObservedAt,
	}
	f.pending = append(f.pending, heldMessage{
		msg: copied,
		at:  msg.ObservedAt.Add(f.delay),
		seq: f.seq,
	})
	return true
}

// Due releases every copy whose delay has elapsed.
func (f *duplicateFault) Due(now time.Time, emit func(source.RawMessage)) {
	f.pending = releaseDue(f.pending, now, emit)
}

// NextDue reports when the next copy is due.
func (f *duplicateFault) NextDue() (time.Time, bool) { return nextDue(f.pending) }

func (f *duplicateFault) Reset() {
	f.rnd = rand.New(rand.NewSource(f.seed)) //nolint:gosec // fault injection, not security
	f.pending = nil
	f.seq = 0
}

// releaseDue emits every held message due at or before now, and returns what is
// left.
func releaseDue(pending []heldMessage, now time.Time, emit func(source.RawMessage)) []heldMessage {
	if len(pending) == 0 {
		return pending
	}

	sort.SliceStable(pending, func(a, b int) bool {
		if pending[a].at.Equal(pending[b].at) {
			return pending[a].seq < pending[b].seq
		}
		return pending[a].at.Before(pending[b].at)
	})

	cut := 0
	for cut < len(pending) && !pending[cut].at.After(now) {
		emit(pending[cut].msg)
		cut++
	}
	return pending[cut:]
}

// nextDue returns the earliest release time among held messages.
func nextDue(pending []heldMessage) (time.Time, bool) {
	if len(pending) == 0 {
		return time.Time{}, false
	}

	earliest := pending[0].at
	for _, p := range pending[1:] {
		if p.at.Before(earliest) {
			earliest = p.at
		}
	}
	return earliest, true
}
