package faultinjector

import (
	"bytes"
	"strconv"
	"time"
)

// Several faults are defined in terms of what is inside the payload rather than
// its position in the stream: DropSeqRange names sequence numbers, ClockSkew
// and DelayPublisher name a publisher, SeqReset and EpochBump rewrite the
// envelope outright.
//
// They read and rewrite the JSON directly rather than going through pkg/codec,
// for two reasons. The codec's field names are configurable, so a fault built
// on it would need the same configuration as the check under test and would
// silently stop matching when the two drifted apart. And the codec's job is to
// produce well-formed events, whereas a fault's job is sometimes to produce
// malformed ones — Corrupt and Truncate exist precisely to emit payloads no
// decoder should accept.
//
// The scan is a plain search for a field. It assumes the harness publisher's
// format, which is the only thing these faults are ever pointed at.

// readUint reads a numeric field. Second return reports whether it was found.
func readUint(payload []byte, field string) (uint64, bool) {
	raw, ok := readRaw(payload, field)
	if !ok {
		return 0, false
	}

	n, err := strconv.ParseUint(string(raw), 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// readString reads a quoted string field.
func readString(payload []byte, field string) (string, bool) {
	i := fieldValueStart(payload, field)
	if i < 0 || payload[i] != '"' {
		return "", false
	}

	rest := payload[i+1:]
	end := bytes.IndexByte(rest, '"')
	if end < 0 {
		return "", false
	}
	return string(rest[:end]), true
}

// readRaw returns the raw bytes of a field's value, up to the delimiter.
func readRaw(payload []byte, field string) ([]byte, bool) {
	i := fieldValueStart(payload, field)
	if i < 0 {
		return nil, false
	}

	end := i
	for end < len(payload) && payload[end] != ',' && payload[end] != '}' && payload[end] != ' ' {
		end++
	}
	if end == i {
		return nil, false
	}
	return payload[i:end], true
}

// fieldValueStart returns the index where a field's value begins, or -1.
func fieldValueStart(payload []byte, field string) int {
	needle := []byte(`"` + field + `"`)

	i := bytes.Index(payload, needle)
	if i < 0 {
		return -1
	}
	rest := payload[i+len(needle):]

	colon := bytes.IndexByte(rest, ':')
	if colon < 0 {
		return -1
	}

	j := colon + 1
	for j < len(rest) && (rest[j] == ' ' || rest[j] == '\t') {
		j++
	}
	if j >= len(rest) {
		return -1
	}
	return i + len(needle) + j
}

// writeUint replaces a numeric field's value, returning a new payload.
//
// It returns the payload unchanged if the field is absent, which is the right
// behavior for a fault pointed at a stream it does not fully understand: a
// rewrite that cannot happen is better than one that mangles the message into
// something the codec will reject for the wrong reason.
func writeUint(payload []byte, field string, value uint64) []byte {
	old, ok := readRaw(payload, field)
	if !ok {
		return payload
	}
	start := fieldValueStart(payload, field)

	out := make([]byte, 0, len(payload)+8)
	out = append(out, payload[:start]...)
	out = strconv.AppendUint(out, value, 10)
	out = append(out, payload[start+len(old):]...)
	return out
}

// writeString replaces a quoted string field's value.
func writeString(payload []byte, field, value string) []byte {
	start := fieldValueStart(payload, field)
	if start < 0 || payload[start] != '"' {
		return payload
	}

	rest := payload[start+1:]
	end := bytes.IndexByte(rest, '"')
	if end < 0 {
		return payload
	}

	out := make([]byte, 0, len(payload)+len(value))
	out = append(out, payload[:start+1]...)
	out = append(out, value...)
	out = append(out, rest[end:]...)
	return out
}

// writeTimestamp replaces an RFC3339 timestamp field.
func writeTimestamp(payload []byte, field string, at time.Time) []byte {
	return writeString(payload, field, at.Format(time.RFC3339Nano))
}

// clone copies a payload so a rewrite never edits a buffer somebody else holds.
//
// Faults run in front of two different consumers in the same scenario, and a
// fault that mutated its input in place would perturb the stream the
// materializer sees as well as driftwatch's — silently turning a one-sided
// fault into a two-sided one and making the test prove nothing.
func clone(payload []byte) []byte {
	out := make([]byte, len(payload))
	copy(out, payload)
	return out
}
