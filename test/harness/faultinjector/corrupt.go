package faultinjector

import (
	"fmt"
	"math/rand"

	"github.com/nabrahma/driftwatch/pkg/source"
)

// corruptFault flips random bytes in the payload.
type corruptFault struct {
	rate float64
	seed int64
	rnd  *rand.Rand
}

// Corrupt flips a byte in the payload with probability rate.
//
// What this tests is not the decoder's ability to reject bad input — that is a
// unit test — but what the pipeline does with the rejection. A corrupt payload
// is an event driftwatch will never see, so it has the same consequence as a
// dropped one and must be treated the same way: counted, and reflected in
// trust. Silently skipping it would leave the oracle wrong with no record why.
func Corrupt(rate float64, seed int64) Fault {
	f := &corruptFault{rate: rate, seed: seed}
	f.Reset()
	return f
}

func (f *corruptFault) Name() string { return fmt.Sprintf("Corrupt(%.4f,%d)", f.rate, f.seed) }

func (f *corruptFault) Apply(msg source.RawMessage, emit func(source.RawMessage)) bool {
	if f.rnd.Float64() >= f.rate || len(msg.Payload) == 0 {
		return true
	}

	payload := clone(msg.Payload)
	at := f.rnd.Intn(len(payload))
	// XOR rather than a random replacement: it always changes the byte, so a
	// corruption never silently does nothing.
	payload[at] ^= 0xff

	emit(source.RawMessage{Topic: msg.Topic, Payload: payload, ObservedAt: msg.ObservedAt})
	return false
}

func (f *corruptFault) Reset() { f.rnd = rand.New(rand.NewSource(f.seed)) } //nolint:gosec // fault injection, not security

// truncateFault cuts payloads short.
type truncateFault struct {
	rate float64
	seed int64
	rnd  *rand.Rand
}

// Truncate cuts the payload short with probability rate.
//
// Distinct from Corrupt because it fails differently: a truncated JSON object
// is unterminated rather than malformed in the middle, which is what a
// connection dropped mid-frame produces. A decoder can plausibly get one right
// and the other wrong.
func Truncate(rate float64, seed int64) Fault {
	f := &truncateFault{rate: rate, seed: seed}
	f.Reset()
	return f
}

func (f *truncateFault) Name() string { return fmt.Sprintf("Truncate(%.4f,%d)", f.rate, f.seed) }

func (f *truncateFault) Apply(msg source.RawMessage, emit func(source.RawMessage)) bool {
	if f.rnd.Float64() >= f.rate || len(msg.Payload) < 2 {
		return true
	}

	// Always leave at least one byte and always remove at least one, so a
	// truncation is never a no-op and never an empty frame — the second is a
	// different fault with a different meaning.
	keep := 1 + f.rnd.Intn(len(msg.Payload)-1)

	emit(source.RawMessage{
		Topic:      msg.Topic,
		Payload:    clone(msg.Payload[:keep]),
		ObservedAt: msg.ObservedAt,
	})
	return false
}

func (f *truncateFault) Reset() { f.rnd = rand.New(rand.NewSource(f.seed)) } //nolint:gosec // fault injection, not security
