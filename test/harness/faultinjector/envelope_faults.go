package faultinjector

import (
	"fmt"
	"time"

	"github.com/nabrahma/driftwatch/pkg/source"
)

// The three faults that rewrite the envelope rather than perturbing delivery.
// They exist because the hardest failures for a sequence tracker are not lost
// messages but messages that arrive intact and lie about where they belong.

// clockSkewFault rewrites one publisher's timestamps.
type clockSkewFault struct {
	pub    string
	offset time.Duration
}

// ClockSkew shifts the named publisher's timestamps by offset.
//
// This is F5, and the fault that proves settlement does not depend on publisher
// clocks. §5.3 requires settlement to use driftwatch's local receive time, so a
// publisher whose clock is an hour fast — or an hour slow, which is worse —
// must change nothing about when its keys become eligible for comparison. A
// design that used the publisher's timestamp would either never settle those
// keys or settle them immediately, and both are silent.
func ClockSkew(pub string, offset time.Duration) Fault {
	return &clockSkewFault{pub: pub, offset: offset}
}

func (f *clockSkewFault) Name() string { return fmt.Sprintf("ClockSkew(%s,%s)", f.pub, f.offset) }

func (f *clockSkewFault) Apply(msg source.RawMessage, emit func(source.RawMessage)) bool {
	pub, ok := readString(msg.Payload, "publisher")
	if !ok || pub != f.pub {
		return true
	}

	// Only the payload's own timestamp is rewritten. ObservedAt is driftwatch's
	// local receive time and is not the publisher's to influence — rewriting it
	// here would make the test prove the opposite of what it means to.
	skewed := writeTimestamp(clone(msg.Payload), "ts", msg.ObservedAt.Add(f.offset))

	emit(source.RawMessage{Topic: msg.Topic, Payload: skewed, ObservedAt: msg.ObservedAt})
	return false
}

func (f *clockSkewFault) Reset() {}

// seqResetFault restarts sequence numbers without bumping the epoch.
type seqResetFault struct {
	atMsg int
	seen  int
	after uint64
}

// SeqReset rewrites sequences from position atMsg onward to restart from 1,
// leaving the epoch alone.
//
// An implicit restart: the publisher came back with no memory of what it had
// sent and no way to say so. From the subscriber's side it is indistinguishable
// from a publisher that has gone backwards, and the only safe reading is that
// everything since is suspect. Getting this wrong in the other direction — a
// tracker that treats seq 1 after seq 900 as 899 duplicates and drops them all
// — is the failure worth catching, because it looks like nothing at all.
func SeqReset(atMsg int) Fault { return &seqResetFault{atMsg: atMsg} }

func (f *seqResetFault) Name() string { return fmt.Sprintf("SeqReset(%d)", f.atMsg) }

func (f *seqResetFault) Apply(msg source.RawMessage, emit func(source.RawMessage)) bool {
	f.seen++
	if f.seen < f.atMsg {
		return true
	}

	f.after++
	emit(source.RawMessage{
		Topic:      msg.Topic,
		Payload:    writeUint(clone(msg.Payload), "seq", f.after),
		ObservedAt: msg.ObservedAt,
	})
	return false
}

func (f *seqResetFault) Reset() {
	f.seen = 0
	f.after = 0
}

// epochBumpFault bumps the epoch and restarts sequences.
type epochBumpFault struct {
	atMsg int
	seen  int
	after uint64
	epoch uint64
}

// EpochBump rewrites messages from position atMsg onward with a higher epoch
// and sequences restarting from 1.
//
// The explicit restart, and the one a publisher should perform: the epoch says
// "I restarted" so the subscriber can reset its sequence expectation without
// guessing. The pair with SeqReset is the point — the same observable sequence
// discontinuity means "possible loss" in one case and "expected restart" in the
// other, and only the epoch distinguishes them.
func EpochBump(atMsg int) Fault { return &epochBumpFault{atMsg: atMsg} }

func (f *epochBumpFault) Name() string { return fmt.Sprintf("EpochBump(%d)", f.atMsg) }

func (f *epochBumpFault) Apply(msg source.RawMessage, emit func(source.RawMessage)) bool {
	f.seen++
	if f.seen < f.atMsg {
		return true
	}

	if f.epoch == 0 {
		// Take the epoch from the message that triggered the bump, so the new
		// one is genuinely higher than what the publisher was using rather than
		// a fixed number that might be lower.
		current, ok := readUint(msg.Payload, "epoch")
		if !ok {
			current = 1
		}
		f.epoch = current + 1
	}
	f.after++

	payload := clone(msg.Payload)
	payload = writeUint(payload, "epoch", f.epoch)
	payload = writeUint(payload, "seq", f.after)

	emit(source.RawMessage{Topic: msg.Topic, Payload: payload, ObservedAt: msg.ObservedAt})
	return false
}

func (f *epochBumpFault) Reset() {
	f.seen = 0
	f.after = 0
	f.epoch = 0
}
