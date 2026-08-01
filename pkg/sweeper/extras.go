package sweeper

import (
	"context"
	"fmt"
	"time"

	"github.com/nabrahma/driftwatch/pkg/differ"
	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// extrasPass is the first half of a two-pass extras scan: the keys the target
// held and the oracle did not, and when that was observed.
type extrasPass struct {
	keys      map[string]struct{}
	startedAt time.Time
	window    time.Duration
	truncated bool
}

// dueAt is when the second pass may run.
func (p *extrasPass) dueAt() time.Time { return p.startedAt.Add(p.window) }

// ScanExtrasOnce performs one target→oracle pass.
//
// Finding keys the target holds and the oracle does not is the one direction
// that cannot be driven from the oracle, so it needs a keyspace scan — and a
// scan is not atomic. A key written after the scan passed its slot, or a key
// whose event driftwatch had not yet applied when the scan reached it, both
// look exactly like an extra key. Reporting on a single scan would mean
// reporting every key the system wrote while the scan was running.
//
// So §5.5 requires two passes: a key is only an extra if it was absent from the
// oracle when the scan saw it and is still absent from both the oracle and the
// target a full settlement window later. Anything that appeared mid-scan
// self-resolves.
//
// The two passes are separate calls rather than one call with a sleep in the
// middle. Sleeping would hold the sweeper for a whole window, and a settlement
// window can be two minutes; it would also make the function untestable without
// real time passing. Each call therefore finishes any pass that has waited long
// enough and then starts a new one, so extras need two calls, W apart, to be
// reported. That is what the Run loop's ExtraScanInterval does naturally, since
// it is far longer than W.
func (s *Sweeper) ScanExtrasOnce(ctx context.Context) (*differ.Report, error) {
	if s.isClosed() {
		return nil, ErrClosed
	}
	if _, err := s.checkHealth(ctx); err != nil {
		return nil, err
	}

	now := s.cfg.Clock.Now()
	w := s.cfg.SettlementWindow()

	opts := s.cfg.DifferOptions
	opts.Now = now

	rep := differ.NewReport(now, opts)
	rep.Pass = differ.PassTargetToOracle
	rep.SettlementWindow = w

	// Second pass first: a set that has waited its window is decided now, using
	// the keyspace as it stands before this scan changes what is pending.
	if pending := s.takePendingExtras(now); pending != nil {
		if err := s.secondPass(ctx, rep, pending); err != nil {
			return nil, err
		}
	}

	// First pass: collect the current candidates and park them.
	pass, err := s.firstPass(ctx, w, now)
	if err != nil {
		return nil, err
	}
	s.setPendingExtras(pass)

	rep.KeysCompared = len(pass.keys)
	rep.Truncated = pass.truncated
	rep.FinishedAt = s.cfg.Clock.Now()
	s.c.extraScans.Add(1)
	return rep, nil
}

// firstPass scans the keyspace and keeps the keys the oracle does not expect.
func (s *Sweeper) firstPass(ctx context.Context, w time.Duration, now time.Time) (*extrasPass, error) {
	pass := &extrasPass{
		keys:      map[string]struct{}{},
		startedAt: now,
		window:    w,
	}

	it := s.cfg.Target.Scan(ctx, s.cfg.ExtraScanPattern, s.cfg.ReadBatchSize)
	defer func() { _ = it.Close() }() //nolint:errcheck // read-only iterator; nothing to fail

	for it.Next(ctx) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for _, key := range it.Keys() {
			if _, tracked := s.oracleKnows(key); tracked {
				continue
			}
			if len(pass.keys) >= s.cfg.MaxExtrasTracked {
				// Past the bound the magnitude is what matters, not the list.
				pass.truncated = true
				s.c.extrasTruncated.Add(1)
				return pass, nil
			}
			// A map dedups the cursor repeats SCAN is allowed to produce
			// (§9 M8) without a second pass over the keys.
			pass.keys[key] = struct{}{}
		}
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("scanning for extras: %w", err)
	}
	return pass, nil
}

// secondPass re-checks a parked set and reports only what survives both halves.
func (s *Sweeper) secondPass(ctx context.Context, rep *differ.Report, pass *extrasPass) error {
	keys := make([]string, 0, len(pass.keys))
	for key := range pass.keys {
		keys = append(keys, key)
	}

	for start := 0; start < len(keys); start += s.cfg.ReadBatchSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := min(start+s.cfg.ReadBatchSize, len(keys))

		batch := keys[start:end]
		reads, err := s.cfg.Target.ReadMany(ctx, batch, s.cfg.Shape)
		if err != nil {
			return fmt.Errorf("re-reading %d extras: %w", len(batch), err)
		}

		for i, key := range batch {
			if !s.extraSurvives(key, reads[i]) {
				s.c.extrasSelfResolved.Add(1)
				continue
			}
			rep.Add(&differ.Finding{
				Key:         key,
				Category:    differ.CatExtraInTarget,
				Trust:       oracle.TrustComplete,
				TargetValue: reads[i].Value,
				FirstSeenAt: pass.startedAt,
				// Confirmed, and on the same terms as any other finding: two
				// reads a settlement window apart both said so.
				Confirmed: true,
			})
			s.c.extrasReported.Add(1)
		}
	}
	rep.Truncated = rep.Truncated || pass.truncated
	return nil
}

// extraSurvives reports whether a first-pass candidate is still an extra.
//
// Both conditions from §5.5 must hold: the key is still in the target, and the
// oracle still does not expect it. A key that has since gone from the target
// resolved itself, and a key the oracle has since learned about was only ever
// an artifact of driftwatch's own ingest lag.
func (s *Sweeper) extraSurvives(key string, read target.Read) bool {
	if read.Err != nil {
		// The key holds a shape this projection cannot read. It is present, so
		// it survives the presence half, but calling it an extra would put the
		// wrong category on it — that is CatTypeMismatch, and the oracle-driven
		// sweep is where a key with an expectation gets one. Here there is no
		// expectation to mismatch against, so it is left alone.
		return false
	}
	if read.Value.Kind == event.ValueAbsent {
		return false
	}
	_, tracked := s.oracleKnows(key)
	return !tracked
}

// oracleKnows reports whether the oracle holds an expectation for a key.
//
// A tombstone counts as known. The oracle saying "this key should not exist" is
// an expectation like any other, and the oracle→target sweep already compares
// it — reporting it here as well would report the same divergence twice under
// two different categories.
func (s *Sweeper) oracleKnows(key string) (oracle.Entry, bool) {
	entry, ok := s.cfg.Oracle.Get(key)
	return entry, ok
}

// takePendingExtras removes and returns the parked set if its window elapsed.
func (s *Sweeper) takePendingExtras(now time.Time) *extrasPass {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.extras == nil || s.extras.dueAt().After(now) {
		return nil
	}
	pass := s.extras
	s.extras = nil
	return pass
}

func (s *Sweeper) setPendingExtras(pass *extrasPass) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.extras = pass
}

// PendingExtras returns how many first-pass candidates are awaiting their
// second pass.
func (s *Sweeper) PendingExtras() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.extras == nil {
		return 0
	}
	return len(s.extras.keys)
}
