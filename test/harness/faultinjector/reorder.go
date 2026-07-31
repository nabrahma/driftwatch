package faultinjector

import (
	"fmt"
	"math/rand"

	"github.com/nabrahma/driftwatch/pkg/source"
)

// reorderFault buffers a window of messages and emits them shuffled.
type reorderFault struct {
	window int
	seed   int64
	rnd    *rand.Rand
	buf    []source.RawMessage
}

// Reorder buffers `window` messages and emits them in shuffled order.
//
// Out-of-order delivery is the fault most likely to be mistaken for loss. A
// projection that is not commutative produces a different answer from the same
// events in a different order, and a sequence tracker that treats "seq 7 after
// seq 9" as a gap reports a loss that never happened. This produces both
// situations without needing two publishers.
func Reorder(window int, seed int64) Fault {
	if window < 2 {
		// A window of one cannot reorder anything, and silently doing nothing
		// would make a scenario look like it tested reordering when it did not.
		window = 2
	}
	f := &reorderFault{window: window, seed: seed}
	f.Reset()
	return f
}

func (f *reorderFault) Name() string { return fmt.Sprintf("Reorder(%d,%d)", f.window, f.seed) }

func (f *reorderFault) Apply(msg source.RawMessage, emit func(source.RawMessage)) bool {
	f.buf = append(f.buf, msg)
	if len(f.buf) < f.window {
		return false
	}

	f.shuffleAndEmit(emit)
	return false
}

// shuffleAndEmit permutes the buffer and hands it on.
func (f *reorderFault) shuffleAndEmit(emit func(source.RawMessage)) {
	// Fisher-Yates from the seeded generator: the same seed produces the same
	// permutation, which is what makes a reordering failure reproducible.
	f.rnd.Shuffle(len(f.buf), func(a, b int) { f.buf[a], f.buf[b] = f.buf[b], f.buf[a] })

	for _, m := range f.buf {
		emit(m)
	}
	f.buf = f.buf[:0]
}

// Flush releases a part-full window when the stream ends.
//
// Without this, every scenario would silently lose up to window-size messages
// at the end — a drop the test author never asked for, and one that would look
// like a bug in whatever was being tested.
func (f *reorderFault) Flush(emit func(source.RawMessage)) {
	if len(f.buf) > 0 {
		f.shuffleAndEmit(emit)
	}
}

func (f *reorderFault) Reset() {
	f.rnd = rand.New(rand.NewSource(f.seed)) //nolint:gosec // fault injection, not security
	f.buf = nil
}

// reorderSwapFault swaps exactly two sequence numbers.
type reorderSwapFault struct {
	a, b uint64
	held *source.RawMessage
}

// ReorderSwap swaps exactly the messages with seq a and seq b.
//
// The deterministic counterpart to Reorder, and the one to use when a test
// needs to say which two events arrived in the wrong order. It holds the first
// of the pair until the second arrives, then emits them the other way round.
func ReorderSwap(a, b uint64) Fault { return &reorderSwapFault{a: a, b: b} }

func (f *reorderSwapFault) Name() string { return fmt.Sprintf("ReorderSwap(%d,%d)", f.a, f.b) }

func (f *reorderSwapFault) Apply(msg source.RawMessage, emit func(source.RawMessage)) bool {
	seq, ok := readUint(msg.Payload, "seq")
	if !ok {
		return true
	}

	switch {
	case seq == f.a && f.held == nil:
		// Hold the first until its partner arrives.
		held := msg
		f.held = &held
		return false

	case seq == f.b && f.held != nil:
		// The second arrives: emit it, then release the first behind it.
		emit(msg)
		emit(*f.held)
		f.held = nil
		return false
	}
	return true
}

// Flush releases a held message whose partner never arrived.
//
// A swap that names a sequence the stream never contained must not turn into a
// drop. The scenario asked for a reorder; silently losing the message would
// make the test assert on the wrong fault.
func (f *reorderSwapFault) Flush(emit func(source.RawMessage)) {
	if f.held != nil {
		emit(*f.held)
		f.held = nil
	}
}

func (f *reorderSwapFault) Reset() { f.held = nil }

// interleaveFault multiplexes N synthetic publishers.
type interleaveFault struct {
	pubs int
	seen uint64
}

// Interleave rewrites the publisher identity so one stream looks like N.
//
// Multiple publishers are where sequence tracking gets hard: each has its own
// sequence space, so a tracker that keeps one global high-water mark reports
// constant gaps, and one that keys by publisher has to get the key right. This
// produces that situation from a single stream.
func Interleave(pubs int) Fault {
	if pubs < 1 {
		pubs = 1
	}
	return &interleaveFault{pubs: pubs}
}

func (f *interleaveFault) Name() string { return fmt.Sprintf("Interleave(%d)", f.pubs) }

func (f *interleaveFault) Apply(msg source.RawMessage, emit func(source.RawMessage)) bool {
	which := f.seen % uint64(f.pubs)
	f.seen++

	// Each synthetic publisher gets its own sequence space, counting from one.
	// Keeping the original seq would give every publisher a stream with gaps of
	// N-1, which is a different fault from the one being asked for.
	payload := clone(msg.Payload)
	payload = writeString(payload, "publisher", fmt.Sprintf("pub-%d", which))
	payload = writeUint(payload, "seq", f.seen/uint64(f.pubs)+boolToUint(f.seen%uint64(f.pubs) != 0))

	emit(source.RawMessage{Topic: msg.Topic, Payload: payload, ObservedAt: msg.ObservedAt})
	return false
}

func boolToUint(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}

func (f *interleaveFault) Reset() { f.seen = 0 }
