// Package faultinjector wraps a Source to drop, reorder, duplicate, delay, partition and corrupt events (§13).
//
// It is Source middleware: it wraps any Source and perturbs the stream. That
// design is what lets every fault scenario run in-process against a fake clock,
// with no cluster and no flakiness — §13 is explicit that a test which fails
// once in fifty runs is worse than no test.
//
// Determinism is therefore the governing constraint here, not a nicety. Every
// fault takes an explicit seed, holds its own generator, and produces the same
// output stream given the same input stream. TestFaults_Deterministic runs each
// one twice over ten thousand messages and requires byte-identical output.
//
// # Where the injector sits
//
// The injector can be placed in front of driftwatch's subscription or in front
// of the materializer's, and the two mean opposite things (§13):
//
//   - Faults on the materializer's stream make the target genuinely wrong.
//     driftwatch must report confirmed divergence.
//   - Faults on driftwatch's own stream make driftwatch's oracle wrong.
//     driftwatch must report suspect divergence and never confirm it, because
//     the disagreement is its own fault and it knows it.
//
// The second is the honesty requirement from §5.2 and the one most projects
// would forget to test.
package faultinjector

import (
	"context"
	"sync"
	"time"

	"github.com/nabrahma/driftwatch/pkg/clock"
	"github.com/nabrahma/driftwatch/pkg/source"
)

// Fault transforms a stream of messages. Deterministic given a seed.
type Fault interface {
	Name() string

	// Apply may drop (return false), modify, delay, duplicate or reorder.
	//
	// emit produces an extra message beyond the returned one, which is how
	// duplication and reordering work. A fault that returns true has its
	// (possibly modified) message forwarded; one that returns false has not.
	Apply(msg source.RawMessage, emit func(source.RawMessage)) (keep bool)

	// Reset clears internal state between scenarios.
	Reset()
}

// Timed is implemented by faults that hold messages and release them later.
//
// The base Fault interface is synchronous, which cannot express "emit this in
// fifty milliseconds". Rather than give every fault a clock, the two that need
// one hand their pending messages back to the injector, which drives them from
// its own loop — and on a fake clock that makes the release times exact rather
// than approximate.
type Timed interface {
	// Due releases every message whose hold has expired.
	Due(now time.Time, emit func(source.RawMessage))
	// NextDue reports when the next held message is due, if any.
	NextDue() (time.Time, bool)
}

// Flusher is implemented by faults that buffer messages and must release them
// when the stream ends.
//
// Without it, a Reorder window that is only part-full when the publisher stops
// would swallow its contents, and every scenario would quietly lose up to
// window-size messages at the end — a fault the test author did not ask for.
type Flusher interface {
	Flush(emit func(source.RawMessage))
}

// Injector wraps a Source and applies faults to the stream.
type Injector struct {
	inner  source.Source
	clk    clock.Clock
	faults []Fault

	mu    sync.Mutex
	stats Stats
	// pending mirrors whether a Timed fault is still holding something.
	//
	// The faults keep their held messages unsynchronized, which is correct
	// because only the run loop touches them. HasPending is called from another
	// goroutine, so the loop publishes the answer here rather than letting a
	// caller reach into the faults and race with the loop that owns them.
	pending bool
}

// Stats counts what the injector did, so a scenario can assert on the fault
// rather than only on its consequences.
type Stats struct {
	Received   uint64
	Emitted    uint64
	Dropped    uint64
	Duplicated uint64
	Delayed    uint64
	Modified   uint64
}

// Wrap returns an Injector applying faults to inner, in the order given.
//
// Order matters and is worth stating: Drop then Reorder is not Reorder then
// Drop. The first drops from the original stream and shuffles what is left; the
// second shuffles first, so which messages the drop rate selects depends on the
// shuffle. Both are legitimate; they are different scenarios.
func Wrap(inner source.Source, clk clock.Clock, faults ...Fault) *Injector {
	if clk == nil {
		clk = clock.Real()
	}
	return &Injector{inner: inner, clk: clk, faults: faults}
}

// Name returns the wrapped source's name, so the pipeline cannot tell it is
// being lied to.
func (i *Injector) Name() string { return i.inner.Name() }

// Run reads from the wrapped source, applies the faults, and forwards what
// survives.
func (i *Injector) Run(ctx context.Context, out chan<- source.RawMessage) error {
	raw := make(chan source.RawMessage, 1024)

	inner := make(chan error, 1)
	innerCtx, cancelInner := context.WithCancel(ctx)
	defer cancelInner()
	go func() { inner <- i.inner.Run(innerCtx, raw) }()

	defer i.flush(ctx, out)

	for {
		// Release anything a Timed fault has been holding whose moment has
		// come, before looking for new input. A message held until now must go
		// out now, not after the next arrival.
		if err := i.releaseDue(ctx, out); err != nil {
			return err
		}

		at, hasPending := i.nextDue()
		i.setPending(hasPending)

		var timer clock.Timer
		var timerC <-chan time.Time
		if hasPending {
			wait := at.Sub(i.clk.Now())
			if wait <= 0 {
				// Already due. This happens when the clock moves between the
				// two lines above, which on a fake clock is a plain race with
				// whoever is advancing it. Parking on a timer whose deadline is
				// in the past would wait for the *next* advance to fire it, and
				// if none comes the held message never leaves — so go back and
				// release it instead.
				continue
			}
			timer = i.clk.NewTimer(wait)
			timerC = timer.C()
		}

		select {
		case <-ctx.Done():
			stopTimer(timer)
			return ctx.Err()

		case err := <-inner:
			stopTimer(timer)
			// The source is finished. Drain whatever it already delivered, then
			// let the deferred flush release the buffers.
			i.drain(ctx, raw, out)
			return err

		case msg := <-raw:
			stopTimer(timer)
			i.handle(ctx, msg, out)

		case <-timerC:
			stopTimer(timer)
		}
	}
}

func stopTimer(t clock.Timer) {
	if t != nil {
		t.Stop()
	}
}

// handle runs one message through the fault chain.
func (i *Injector) handle(ctx context.Context, msg source.RawMessage, out chan<- source.RawMessage) {
	// extra collects messages a fault emitted beyond the one it was given.
	// They are fed through the remaining faults too, so a duplicate created by
	// one fault is still subject to the next — which is what "faults chain"
	// has to mean if the order is to be meaningful.
	msgs := []source.RawMessage{msg}

	for _, fault := range i.faults {
		next := make([]source.RawMessage, 0, len(msgs))
		emit := func(extra source.RawMessage) { next = append(next, extra) }

		for _, m := range msgs {
			if fault.Apply(m, emit) {
				next = append(next, m)
			}
		}
		msgs = next
	}

	// Counted here, after the whole chain has seen the message, rather than on
	// arrival.
	//
	// The difference matters to anything waiting on this counter. A Timed fault
	// holds messages and releases them by clock, so a caller that advances the
	// clock as soon as the last message is *received* can trigger a release
	// while that message is still working its way down the chain — and which
	// messages happen to be pending at the first release decides the output
	// order. Counting on completion makes "Received == N" mean every message is
	// through, which is the property a deterministic run needs.
	i.mu.Lock()
	i.stats.Received++
	i.mu.Unlock()

	for _, m := range msgs {
		i.forward(ctx, m, out)
	}
}

// forward sends one message downstream.
func (i *Injector) forward(ctx context.Context, msg source.RawMessage, out chan<- source.RawMessage) {
	select {
	case out <- msg:
		i.mu.Lock()
		i.stats.Emitted++
		i.mu.Unlock()
	case <-ctx.Done():
	}
}

// nextDue reports the earliest pending release across every Timed fault.
func (i *Injector) nextDue() (time.Time, bool) {
	var earliest time.Time
	var found bool

	for _, fault := range i.faults {
		timed, ok := fault.(Timed)
		if !ok {
			continue
		}
		if at, ok := timed.NextDue(); ok && (!found || at.Before(earliest)) {
			earliest, found = at, true
		}
	}
	return earliest, found
}

// releaseDue emits everything the Timed faults are ready to let go of.
func (i *Injector) releaseDue(ctx context.Context, out chan<- source.RawMessage) error {
	now := i.clk.Now()

	var released []source.RawMessage
	emit := func(msg source.RawMessage) { released = append(released, msg) }

	for _, fault := range i.faults {
		if timed, ok := fault.(Timed); ok {
			timed.Due(now, emit)
		}
	}

	for _, msg := range released {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		i.forward(ctx, msg, out)
	}
	return nil
}

// drain forwards whatever the inner source already delivered.
func (i *Injector) drain(ctx context.Context, raw <-chan source.RawMessage, out chan<- source.RawMessage) {
	for {
		select {
		case msg := <-raw:
			i.handle(ctx, msg, out)
		default:
			return
		}
	}
}

// flush releases every buffered and held message when the stream ends.
func (i *Injector) flush(ctx context.Context, out chan<- source.RawMessage) {
	var released []source.RawMessage
	emit := func(msg source.RawMessage) { released = append(released, msg) }

	for _, fault := range i.faults {
		if flusher, ok := fault.(Flusher); ok {
			flusher.Flush(emit)
		}
	}

	// Everything a Timed fault still holds goes out too. The alternative is a
	// scenario silently losing messages to a delay that outlived the stream,
	// which would look like a drop the test never asked for.
	now := i.clk.Now()
	for _, fault := range i.faults {
		if timed, ok := fault.(Timed); ok {
			timed.Due(now.Add(time.Duration(1)<<62), emit)
		}
	}

	for _, msg := range released {
		i.forward(ctx, msg, out)
	}
	i.setPending(false)
}

// setPending publishes the run loop's view of whether anything is held.
func (i *Injector) setPending(v bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.pending = v
}

// HasPending reports whether any Timed fault is still holding a message.
//
// A caller driving a fake clock needs this to know when a release has finished.
// Held messages leave by one of two paths — the run loop's timer, or the flush
// when the stream ends — and both emit in sorted order, but the split between
// them depends on when the source closed relative to the clock moving. Waiting
// for this to go false puts every release on the first path, so the output
// order is a function of the input and the seeds alone.
func (i *Injector) HasPending() bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.pending
}

// Stats returns what the injector did.
func (i *Injector) Stats() Stats {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.stats
}

// SourceStats returns the wrapped source's transport counters.
func (i *Injector) SourceStats() source.Stats { return i.inner.Stats() }

// Close closes the wrapped source. Idempotent.
func (i *Injector) Close() error { return i.inner.Close() }

// Reset clears every fault's state, so one scenario cannot leak into the next.
func (i *Injector) Reset() {
	for _, fault := range i.faults {
		fault.Reset()
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	i.stats = Stats{}
}

// Gaps forwards the wrapped source's gap signals, if it has any.
//
// The injector deliberately does not synthesize gap signals of its own. A fault
// simulates something going wrong out in the world, and the world does not
// announce that it dropped an event — the whole point of driftwatch is to
// notice loss nobody reported. An injector that signaled its own drops would
// make every scenario easier than reality.
func (i *Injector) Gaps() <-chan source.GapSignal {
	if signaller, ok := i.inner.(source.GapSignaller); ok {
		return signaller.Gaps()
	}
	return nil
}
