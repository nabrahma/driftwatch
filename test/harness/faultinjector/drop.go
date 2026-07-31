package faultinjector

import (
	"fmt"
	"math/rand"

	"github.com/nabrahma/driftwatch/pkg/source"
)

// dropFault drops each message with a fixed probability.
type dropFault struct {
	rate float64
	seed int64
	rnd  *rand.Rand
}

// Drop drops each message with probability rate.
//
// Deterministic despite being probabilistic: the generator is seeded explicitly
// and reset with the fault, so the same seed drops the same messages every run.
// A drop that varied between runs would make a failure impossible to reproduce,
// which §13 says is worse than having no test at all.
func Drop(rate float64, seed int64) Fault {
	f := &dropFault{rate: rate, seed: seed}
	f.Reset()
	return f
}

func (f *dropFault) Name() string { return fmt.Sprintf("Drop(%.4f,%d)", f.rate, f.seed) }

func (f *dropFault) Apply(_ source.RawMessage, _ func(source.RawMessage)) bool {
	// The generator advances on every message rather than only when a drop is
	// possible, so the sequence of decisions depends on the message count alone
	// and not on the rate.
	return f.rnd.Float64() >= f.rate
}

func (f *dropFault) Reset() { f.rnd = rand.New(rand.NewSource(f.seed)) } //nolint:gosec // fault injection, not security

// dropBurstFault drops a run of consecutive messages.
type dropBurstFault struct {
	after, count int
	seen         int
}

// DropBurst drops count consecutive messages after the first `after` have
// passed.
//
// This is the shape a real outage has: a consumer that was down for a few
// seconds loses a contiguous run, not a scattering. It exercises the gap
// detector's interval merging in a way a per-message drop rate does not.
func DropBurst(after, count int) Fault { return &dropBurstFault{after: after, count: count} }

func (f *dropBurstFault) Name() string { return fmt.Sprintf("DropBurst(%d,%d)", f.after, f.count) }

func (f *dropBurstFault) Apply(_ source.RawMessage, _ func(source.RawMessage)) bool {
	f.seen++
	return f.seen <= f.after || f.seen > f.after+f.count
}

func (f *dropBurstFault) Reset() { f.seen = 0 }

// dropSeqRangeFault drops messages whose sequence number falls in a range.
type dropSeqRangeFault struct {
	from, to uint64
}

// DropSeqRange drops exactly the messages with seq in [from, to].
//
// §13 calls this the deterministic drop most tests should use, and it is the
// one to reach for first. It names the messages it removes rather than counting
// them, so a test can say which key ends up divergent and assert on that key by
// name — the assertion stays readable, and stays true if the publisher's
// ordering changes.
func DropSeqRange(from, to uint64) Fault { return &dropSeqRangeFault{from: from, to: to} }

func (f *dropSeqRangeFault) Name() string { return fmt.Sprintf("DropSeqRange(%d,%d)", f.from, f.to) }

func (f *dropSeqRangeFault) Apply(msg source.RawMessage, _ func(source.RawMessage)) bool {
	seq, ok := readUint(msg.Payload, "seq")
	if !ok {
		// A message with no sequence number cannot be in the range. Dropping it
		// would silently widen the fault to messages the test did not name.
		return true
	}
	return seq < f.from || seq > f.to
}

func (f *dropSeqRangeFault) Reset() {}

// oversizeFault replaces one message with an absurdly large one.
type oversizeFault struct {
	atMsg int
	bytes int
	seen  int
}

// Oversize replaces the message at position atMsg with one of the given size.
//
// The interesting failure is not that the big message is refused — it is
// whether refusing it costs anything else. A source that allocates before
// checking, or that treats the refusal as a transport error and reconnects,
// turns one bad publisher into an outage.
func Oversize(atMsg, bytes int) Fault { return &oversizeFault{atMsg: atMsg, bytes: bytes} }

func (f *oversizeFault) Name() string { return fmt.Sprintf("Oversize(%d,%d)", f.atMsg, f.bytes) }

func (f *oversizeFault) Apply(msg source.RawMessage, emit func(source.RawMessage)) bool {
	f.seen++
	if f.seen != f.atMsg {
		return true
	}

	huge := make([]byte, f.bytes)
	for i := range huge {
		huge[i] = 'x'
	}
	emit(source.RawMessage{Topic: msg.Topic, Payload: huge, ObservedAt: msg.ObservedAt})
	return false
}

func (f *oversizeFault) Reset() { f.seen = 0 }
