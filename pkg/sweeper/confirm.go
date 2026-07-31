package sweeper

import (
	"context"
	"errors"
	"time"

	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// candidate is a disagreement waiting to be re-read.
//
// It holds the oracle version it was raised against, so a confirmation can tell
// "the target is still wrong" from "the oracle has since changed and the
// question is stale", and the window in force at the time, so the confirmation
// delay does not shift underneath a waiting candidate when W is adjusted.
type candidate struct {
	finding differ.Finding
	version uint64
	seenAt  time.Time
	window  time.Duration
}

// dueAt is when this candidate may be re-read.
func (c *candidate) dueAt() time.Time { return c.seenAt.Add(c.window) }

// enqueue adds a candidate to the confirm queue.
//
// The queue is bounded because the load it has to survive is mass divergence,
// where every key is a candidate at once. Under that load, individually
// confirming every key is not useful — what the operator needs is the
// magnitude, which the report already carries — so the overflow is dropped and
// counted rather than allowed to grow (§5.4).
func (s *Sweeper) enqueue(f *differ.Finding, version uint64, now time.Time, w time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := f.Key
	if _, already := s.queued[key]; already {
		// The earlier candidate's wait is further along, so keeping it confirms
		// sooner. Replacing it would restart the clock on every sweep, and a
		// key swept more often than W would then never be confirmed at all.
		return
	}
	if len(s.queue) >= s.cfg.MaxConfirmQueue {
		s.c.confirmQueueDropped.Add(1)
		return
	}

	s.queue = append(s.queue, &candidate{
		finding: *f,
		version: version,
		seenAt:  now,
		window:  w,
	})
	s.queued[key] = struct{}{}
	s.c.candidatesEnqueued.Add(1)
}

// PendingConfirmations returns the depth of the confirm queue.
func (s *Sweeper) PendingConfirmations() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue)
}

// ConfirmDue re-reads every candidate whose settlement window has elapsed, and
// returns how many were decided.
//
// This is phase 2 of §5.4. Nothing driftwatch reports as drift has been seen
// disagreeing only once: a candidate is raised by one sweep, left alone for a
// full W, and read again. A materializer that was merely behind has caught up
// by then, and the candidate is discarded.
//
// The wait is batched rather than slept per item. One goroutine per item would
// be ten thousand goroutines under mass divergence, and one sleep per item on a
// single goroutine would make the queue's delay the sum of its waits rather
// than the longest of them.
func (s *Sweeper) ConfirmDue(ctx context.Context, now time.Time) int {
	if s.isClosed() {
		return 0
	}

	due := s.takeDue(now)
	if len(due) == 0 {
		return 0
	}

	// A confirmation is a read, and a read against an unreachable store answers
	// nothing. Confirming on the strength of a failed read would turn an outage
	// into a wall of drift reports — §23 A5 again, in the place it is easiest
	// to forget. Everything goes back on the queue; the wait has already
	// elapsed, so they are re-read the moment the store answers again.
	if health, err := s.cfg.Target.Health(ctx); err != nil || !health.Reachable {
		s.requeueAll(due)
		s.c.targetUnavailable.Add(1)
		return 0
	}

	s.c.confirmCycles.Add(1)

	decided := 0
	for _, c := range due {
		if ctx.Err() != nil {
			// Put back what has not been looked at, so a canceled cycle loses
			// nothing.
			s.requeueAll(due[decided:])
			break
		}
		s.confirmOne(ctx, c, now)
		decided++
	}
	return decided
}

// takeDue removes and returns the candidates whose wait has elapsed, in the
// order they were raised.
func (s *Sweeper) takeDue(now time.Time) []*candidate {
	s.mu.Lock()
	defer s.mu.Unlock()

	due := make([]*candidate, 0, len(s.queue))
	keep := s.queue[:0]
	for _, c := range s.queue {
		if c.dueAt().After(now) {
			keep = append(keep, c)
			continue
		}
		due = append(due, c)
		delete(s.queued, c.finding.Key)
	}
	s.queue = keep
	return due
}

// requeueAll puts candidates back, keeping their original timing so their wait
// is not restarted.
func (s *Sweeper) requeueAll(cs []*candidate) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, c := range cs {
		if _, already := s.queued[c.finding.Key]; already {
			continue
		}
		if len(s.queue) >= s.cfg.MaxConfirmQueue {
			s.c.confirmQueueDropped.Add(1)
			continue
		}
		s.queue = append(s.queue, c)
		s.queued[c.finding.Key] = struct{}{}
	}
}

// confirmOne decides a single candidate.
func (s *Sweeper) confirmOne(ctx context.Context, c *candidate, now time.Time) {
	key := c.finding.Key

	// The oracle forgot the key. driftwatch can no longer say what the target
	// should hold, so it says nothing.
	version, ok := s.cfg.Oracle.Version(key)
	if !ok {
		s.c.transientKeyEvicted.Add(1)
		return
	}

	// The oracle moved on. The queued disagreement is about a value that is no
	// longer expected, so confirming it would report drift against an
	// expectation driftwatch has already replaced (§5.5, invariant I12).
	if version != c.version {
		s.c.transientOracleAdvanced.Add(1)
		s.markRequeued(key)
		return
	}

	value, err := s.cfg.Target.Get(ctx, key, s.cfg.Shape)
	var wrongType *target.WrongTypeError
	switch {
	case err == nil:
	case errors.As(err, &wrongType):
		// Not a read failure: the key genuinely holds the wrong shape.
	default:
		// Could not find out. The disagreement stays open rather than being
		// counted as confirmed or as resolved.
		s.c.confirmReadFailed.Add(1)
		s.markRequeued(key)
		return
	}

	entry, ok := s.cfg.Oracle.Get(key)
	if !ok || entry.Version != c.version {
		s.c.transientOracleAdvanced.Add(1)
		s.markRequeued(key)
		return
	}

	// The key may have become suspect while the candidate waited — a gap
	// detected after the sweep raised it. The claim is no longer one driftwatch
	// can make.
	if entry.Trust == oracle.TrustSuspect {
		s.c.suspectNotConfirmed.Add(1)
		return
	}

	opts := s.cfg.DifferOptions
	opts.Now = now

	var f *differ.Finding
	if wrongType != nil {
		f = differ.CompareUnreadable(key, entry, wrongType.Got, opts)
	} else {
		f = differ.Compare(key, entry, value, opts)
	}

	if f == nil {
		// The materializer caught up while the candidate waited. This is the
		// common case in a healthy system and the entire reason phase 2 exists.
		//
		// A rising count here with no confirmed drift is a real signal on its
		// own: it means the materializer is slow relative to W (§5.4).
		s.c.transientResolved.Add(1)
		return
	}

	f.Confirmed = true
	f.FirstSeenAt = c.seenAt
	s.confirm(key, &Episode{
		Key:         key,
		Finding:     *f,
		FirstSeenAt: c.seenAt,
		ConfirmedAt: now,
		Window:      c.window,
	})
}

// confirm records an episode and publishes it.
func (s *Sweeper) confirm(key string, episode *Episode) {
	s.mu.Lock()
	if existing, ok := s.confirmed[key]; ok {
		// An episode is already open. Keep its original start so the drift
		// duration covers the episode rather than the latest confirmation.
		episode.FirstSeenAt = existing.FirstSeenAt
		episode.Finding.FirstSeenAt = existing.FirstSeenAt
	}
	s.confirmed[key] = *episode
	s.mu.Unlock()

	s.c.confirmations.Add(1)
	s.publish(&episode.Finding)
}
