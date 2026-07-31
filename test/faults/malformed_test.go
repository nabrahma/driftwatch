package faults

import (
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nabrahma/driftwatch/pkg/check"
	"github.com/nabrahma/driftwatch/pkg/source"
	"github.com/nabrahma/driftwatch/test/harness/scenario"
)

// §15.1 rows 15 to 19 — payloads driftwatch cannot read.
//
// The common thread is that none of these may take the process down or stall
// the stream. A producer that starts emitting garbage is a bad deploy, and the
// auditor's job during a bad deploy is to keep running and say what it stopped
// being able to see — not to crash alongside it.
//
// The drop reasons are distinct on purpose. "I could not parse this" and "I
// parsed it and do not know that operation" send an operator to different
// places: the first is a codec or a serializer mismatch, the second is a
// producer that added an event type nobody configured.

// codecMismatchThreshold is the fraction of undecodable events above which the
// configuration, rather than the odd corrupt frame, is the likely explanation.
const codecMismatchThreshold = 0.10

func TestFault15_OnePercentCorruptPayloads(t *testing.T) {
	// Row 15: decode_error rises, no panic, and the CodecMismatch condition
	// does not fire at 1% — one frame in a hundred is a lossy transport, not a
	// codec configured for the wrong format.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			const total = 500

			for seq := uint64(1); seq <= total; seq++ {
				msg := s.Msg(scenario.Event{Seq: seq, Op: "set", Key: keyFor(seq), Value: "v1"})
				if seq%100 == 0 {
					msg = s.Message(`{"publisher":"replica-0","seq":` + itoa(seq) + `,,,`)
				} else {
					s.Materialize(msg)
				}
				s.Ingest(msg)
			}

			// An unreadable frame leaves a hole in the sequence that nothing
			// can fill, because its sequence number was in the part that could
			// not be parsed. The events behind it therefore wait out the
			// reorder window before being applied — bounded, and the price of
			// not treating every reordered pair as loss.
			s.Settle()
			s.Sweep()

			dropped := s.MetricWith("driftwatch_events_dropped_total",
				map[string]string{"reason": "decode_error"})
			require.Equal(t, 5.0, dropped, "one frame in a hundred was unreadable")

			ratio := dropped / float64(total)
			assert.Less(t, ratio, codecMismatchThreshold,
				"a 1%% error rate is below the threshold at which the codec itself "+
					"is the likely explanation")

			// The stream kept flowing: every readable event was applied.
			assert.Equal(t, uint64(total-5), s.Status().EventsApplied)
			s.SweepAndConfirm()
		})
}

func TestFault16_FiftyPercentCorruptPayloads(t *testing.T) {
	// Row 16: at 50% the codec is the explanation, the condition fires, and the
	// process still stays up.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			const total = 400

			for seq := uint64(1); seq <= total; seq++ {
				if seq%2 == 0 {
					s.Ingest(s.Message(`{"publisher":"replica-0",` + itoa(seq)))
					continue
				}
				msg := s.Msg(scenario.Event{Seq: seq, Op: "set", Key: keyFor(seq), Value: "v1"})
				s.Ingest(msg)
				s.Materialize(msg)
			}

			s.Settle()
			s.Sweep()

			dropped := s.MetricWith("driftwatch_events_dropped_total",
				map[string]string{"reason": "decode_error"})
			require.Equal(t, float64(total/2), dropped)

			assert.GreaterOrEqual(t, dropped/float64(total), codecMismatchThreshold,
				"half the stream unreadable is a codec mismatch, not bad luck")

			// Still running, still ingesting the half it can read, and — the
			// part that matters — refusing to assert about keys it may have
			// missed events for.
			assert.Equal(t, uint64(total/2), s.Status().EventsApplied)

			report := s.SweepAndConfirm()
			s.RequireNoConfirmedDrift()
			s.RequireAllFindingsSuspect(report)
		})
}

func TestFault17_TruncatedPayload(t *testing.T) {
	// Row 17: a truncated frame is a decode error like any other. It is its own
	// row because a truncation is the failure a length-prefixed transport
	// produces, and a decoder that read past the end of the buffer would be a
	// memory-safety bug rather than a dropped event.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			full := scenario.Event{Seq: 1, Op: "set", Key: "block:a", Value: "v1"}.JSON()

			for cut := 1; cut < len(full); cut++ {
				s.Ingest(s.Message(full[:cut]))
			}

			assert.Equal(t, float64(len(full)-1),
				s.MetricWith("driftwatch_events_dropped_total",
					map[string]string{"reason": "decode_error"}),
				"every truncation of a valid payload is rejected, at every length")
			assert.Zero(t, s.Status().EventsApplied,
				"and none of them was mistaken for a usable event")
		})
}

func TestFault18_OversizedPayloadIsRefusedWithoutAllocating(t *testing.T) {
	// Row 18: events_dropped_total{reason="too_large"} = 1, and the payload is
	// refused rather than buffered. The allocation assertion is the point: a
	// producer that can make the auditor allocate an arbitrary amount of memory
	// per frame is a denial-of-service vector.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		WithCodec(func(c *check.CodecSpec) { c.MaxPayloadBytes = 1 << 20 }).
		Run(func(s *scenario.Session) {
			oversized := s.Message(`{"publisher":"replica-0","epoch":1,"seq":1,"op":"set",` +
				`"key":"block:a","value":"` + strings.Repeat("x", 2<<20) + `"}`)

			runtime.GC()
			var before runtime.MemStats
			runtime.ReadMemStats(&before)

			s.Ingest(oversized)

			var after runtime.MemStats
			runtime.ReadMemStats(&after)

			assert.Equal(t, 1.0,
				s.MetricWith("driftwatch_events_dropped_total",
					map[string]string{"reason": "too_large"}),
				"the frame was refused for its size, distinctly from being unparseable")
			assert.Zero(t, s.Status().EventsApplied)

			// The 2 MiB payload already exists in the test's own hands; what
			// must not happen is the decoder copying it into the oracle.
			grew := after.TotalAlloc - before.TotalAlloc
			assert.Less(t, grew, uint64(2<<20),
				"decoding allocated %d bytes for a frame it rejected", grew)
		})
}

func TestFault19_UnknownOpCode(t *testing.T) {
	// Row 19: unknown_op, distinct from decode_error. The distinction is what
	// tells an operator whether their serializer or their vocabulary is the
	// problem.
	scenario.New(t).
		WithSettlementWindow(time.Second).
		Run(func(s *scenario.Session) {
			s.Ingest(s.Message(
				`{"publisher":"replica-0","epoch":1,"seq":1,"op":"transmogrify",` +
					`"key":"block:a","value":"v1"}`))

			assert.Equal(t, 1.0,
				s.MetricWith("driftwatch_events_dropped_total",
					map[string]string{"reason": "unknown_op"}),
				"the payload parsed; it was the operation that was not recognized")
			assert.Zero(t,
				s.MetricWith("driftwatch_events_dropped_total",
					map[string]string{"reason": "decode_error"}),
				"and it must not be reported as a parse failure, which would send "+
					"the operator to look at the wrong system")

			assert.Zero(t, s.Status().EventsApplied)
			assert.Zero(t, s.Oracle().Len(), "an operation nobody understands changes nothing")
		})
}

var _ = source.RawMessage{}
