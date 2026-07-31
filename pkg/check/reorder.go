package check

import (
	"sort"
	"sync"
	"time"

	"github.com/nabrahma/driftwatch/pkg/event"
)

// reorderBuffer restores sequence order within a bounded window.
//
// §9 M6 gives every projection a Commutative method and says that when it
// reports false "the oracle must order by seq before applying". Nothing
// consumed that. The consequence is not subtle: a transport that delivers an
// adjacent pair out of order — which PUB/SUB does routinely, and which §15 row
// 7 is written to catch — leaves the oracle folded in the wrong order and
// permanently disagreeing with a store that applied the same events correctly.
// driftwatch then reports drift it manufactured itself.
//
// The fix has to be bounded in both directions. Holding an out-of-order event
// forever would turn one genuinely lost message into a permanently stalled
// publisher; releasing immediately is what produced the bug. So an event that
// arrives ahead of its predecessor waits, and stops waiting on whichever comes
// first: the predecessor arriving, the window expiring, or the buffer filling.
// When the wait times out the hole is a real gap, and seqtrack says so — which
// is the honest outcome, just later and with fewer false alarms.
//
// It is deliberately per publisher. Sequence numbers only mean anything within
// one publisher's stream, so ordering across publishers would be inventing a
// relationship that does not exist.
type reorderBuffer struct {
	window  time.Duration
	maxHeld int

	// mu guards pubs. Everything that releases events runs on the applier
	// goroutine, but heldCount is read by Status, which the controller and the
	// CLI status line call while the applier is running.
	mu   sync.Mutex
	pubs map[string]*publisherOrder
}

type publisherOrder struct {
	// epoch is the incarnation these sequence numbers belong to. A change
	// invalidates everything held, because the new epoch's seq 1 is not the
	// successor of the old epoch's seq 40,000.
	epoch uint64
	// next is the sequence number that would let the held events through.
	next uint64
	// held maps seq to the event waiting for its predecessor.
	held map[uint64]event.Event
	// oldestAt is when the first currently-held event arrived, which is what
	// the window is measured from.
	oldestAt time.Time
	started  bool
}

// defaultMaxHeldPerPublisher bounds the buffer.
//
// A publisher that jumps far ahead must not be able to make driftwatch hold an
// unbounded number of events waiting for a predecessor that is never coming.
//
// The window itself is DefaultReorderWindow, in config.go. It is short — two
// seconds — because it is pure added latency for the events that are held and
// it delays gap detection by the same amount. That comfortably covers the
// millisecond-scale reordering a network produces and sits well inside any
// sensible settlement window, so a held event still settles before the sweep
// that would have compared it.
const defaultMaxHeldPerPublisher = 1024

func newReorderBuffer(window time.Duration, maxHeld int) *reorderBuffer {
	if maxHeld <= 0 {
		maxHeld = defaultMaxHeldPerPublisher
	}
	return &reorderBuffer{
		window:  window,
		maxHeld: maxHeld,
		pubs:    map[string]*publisherOrder{},
	}
}

// enabled reports whether the buffer does anything.
func (r *reorderBuffer) enabled() bool { return r != nil && r.window > 0 }

// offer presents an event and returns everything now ready to apply, in
// sequence order.
//
// The common case — an event that is the successor of the last one — allocates
// nothing and returns immediately. Reordering is rare enough that the slow path
// is allowed to be slow.
func (r *reorderBuffer) offer(e *event.Event, now time.Time) []event.Event {
	if !r.enabled() {
		return []event.Event{*e}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	pb, ok := r.pubs[e.Publisher]
	if !ok {
		pb = &publisherOrder{held: map[uint64]event.Event{}}
		r.pubs[e.Publisher] = pb
	}

	switch {
	case !pb.started:
		// The first event from a publisher establishes the baseline. There is
		// nothing to be out of order relative to.
		pb.started, pb.epoch, pb.next = true, e.Epoch, e.Seq+1
		return []event.Event{*e}

	case e.Epoch != pb.epoch:
		// A restart. Whatever is held belongs to an incarnation that is over,
		// and holding it any longer would be waiting for a predecessor from a
		// stream that no longer exists.
		out := pb.drain()
		pb.epoch, pb.next = e.Epoch, e.Seq+1
		return append(out, *e)

	case e.Seq < pb.next:
		// A late arrival, a duplicate or a stale epoch. Ordering it is not this
		// buffer's job — seqtrack classifies it, and holding it back would only
		// delay that judgement.
		return []event.Event{*e}

	case e.Seq == pb.next:
		out := append(make([]event.Event, 0, 1+len(pb.held)), *e)
		pb.next++
		return pb.releaseConsecutive(out)

	default:
		return r.hold(pb, e, now)
	}
}

// hold parks an event that arrived ahead of its predecessor.
func (r *reorderBuffer) hold(pb *publisherOrder, e *event.Event, now time.Time) []event.Event {
	if _, exists := pb.held[e.Seq]; !exists {
		if len(pb.held) == 0 {
			pb.oldestAt = now
		}
		pb.held[e.Seq] = *e
	}

	if len(pb.held) < r.maxHeld {
		return nil
	}

	// The buffer is full, so the predecessor is not coming. Release everything
	// in order and let seqtrack record the gap that is really there.
	return pb.drain()
}

// releaseConsecutive drains held events that are now in sequence.
func (pb *publisherOrder) releaseConsecutive(out []event.Event) []event.Event {
	for {
		next, ok := pb.held[pb.next]
		if !ok {
			return out
		}
		delete(pb.held, pb.next)
		pb.next++
		out = append(out, next)
	}
}

// drain releases everything held, in sequence order, and resets the expectation
// to just past the highest sequence number seen.
func (pb *publisherOrder) drain() []event.Event {
	if len(pb.held) == 0 {
		return nil
	}

	seqs := make([]uint64, 0, len(pb.held))
	for seq := range pb.held {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })

	out := make([]event.Event, 0, len(seqs))
	for _, seq := range seqs {
		out = append(out, pb.held[seq])
	}

	pb.held = map[uint64]event.Event{}
	if last := seqs[len(seqs)-1]; last >= pb.next {
		pb.next = last + 1
	}
	return out
}

// expire releases everything whose wait has run out, in sequence order.
//
// Called before every sweep as well as on each incoming message, because a
// stream that goes quiet with an event still held would otherwise leave that
// event unapplied — and the sweep that followed would compare an oracle missing
// an update it had actually received.
func (r *reorderBuffer) expire(now time.Time) []event.Event {
	if !r.enabled() {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	var out []event.Event
	for _, pb := range r.pubs {
		if len(pb.held) == 0 || now.Sub(pb.oldestAt) < r.window {
			continue
		}
		out = append(out, pb.drain()...)
	}
	return out
}

// held returns how many events are waiting for a predecessor.
func (r *reorderBuffer) heldCount() int {
	if !r.enabled() {
		return 0
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	n := 0
	for _, pb := range r.pubs {
		n += len(pb.held)
	}
	return n
}
