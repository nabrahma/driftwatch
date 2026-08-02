package codec

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nabrahma/driftwatch/pkg/event"
)

func init() { Register("template", newTemplate) }

// templateCodec extracts fields from a line-oriented payload using a regular
// expression with named capture groups.
//
// §7 is blunt about what this is for: a compatibility escape hatch, not a fast
// path. It exists because the alternative for a producer emitting
// `2026-01-01T00:00:00Z ADD block:9f3a replica-0` is to change that producer,
// and nobody is going to change a production publisher to suit an auditing
// tool. A regex on every event costs roughly an order of magnitude more than
// the json scanner, which is a price worth paying once and never worth paying
// by default.
//
// The capture-group names are the event field names, so the configuration is
// the pattern and nothing else:
//
//	codec:
//	  type: template
//	  pattern: '^(?P<ts>\S+) (?P<op>\w+) (?P<key>\S+) (?P<member>\S+)$'
//
// A group named for something that is not an event field is a configuration
// error rather than a silently ignored one. Silently ignoring it is how an
// operator ends up with a codec that has quietly never populated `publisher`.
type templateCodec struct {
	re *regexp.Regexp

	// groups[i] is the event field that capture group i fills, or numFields for
	// a group that fills nothing (index 0 is the whole match). Resolved once at
	// construction so the hot path never looks a name up.
	groups []field

	opMap   map[string]event.Op
	opNames map[string]event.Op
	scratch sync.Pool

	maxPayloadBytes int
	maxKeyBytes     int
	retainRaw       bool
}

// templateFieldNames maps a capture-group name onto the event field it fills.
// The names are the default json field names, so one mental model covers both.
var templateFieldNames = func() map[string]field {
	out := make(map[string]field, numFields)
	for f, spec := range configKeys {
		out[spec.defaultName] = field(f) //nolint:gosec // f indexes a fixed table
	}
	return out
}()

func newTemplate(cfg map[string]string) (Codec, error) {
	pattern := strings.TrimSpace(cfg["pattern"])
	if pattern == "" {
		return nil, fmt.Errorf(
			"%w: template requires a pattern, a regular expression with named "+
				"capture groups such as (?P<key>\\S+)", ErrBadConfig)
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("%w: pattern is not a valid regular expression: %w",
			ErrBadConfig, err)
	}

	// A pattern that can match the empty string will match every payload,
	// including a truncated one, and produce an event of empty fields rather
	// than an error. Refusing it at construction turns a silent stream of
	// meaningless events into a startup failure.
	if re.MatchString("") {
		return nil, fmt.Errorf(
			"%w: pattern matches the empty string, so every payload including a "+
				"truncated one would decode successfully into empty fields",
			ErrBadConfig)
	}

	names := re.SubexpNames()
	groups := make([]field, len(names))
	var sawOp bool
	for i, name := range names {
		if i == 0 || name == "" {
			// Index 0 is the whole match; an unnamed group is positional and
			// deliberately allowed, since a pattern needs grouping for
			// alternation whether or not it wants to capture.
			groups[i] = numFields
			continue
		}
		f, known := templateFieldNames[name]
		if !known {
			return nil, fmt.Errorf(
				"%w: capture group (?P<%s>) does not name an event field; valid "+
					"names are %s", ErrBadConfig, name, templateFieldList())
		}
		groups[i] = f
		if f == fieldOp {
			sawOp = true
		}
	}

	if !sawOp {
		// Every event has an operation, and a pattern with no `op` group would
		// decode every line as OpUnknown — which the pipeline counts as a
		// version mismatch and reports as a producer problem. It is neither.
		return nil, fmt.Errorf(
			"%w: pattern has no (?P<op>) group, so every event would decode as "+
				"an unknown operation", ErrBadConfig)
	}

	c := &templateCodec{
		re:              re,
		groups:          groups,
		opNames:         builtinOpNames(),
		maxPayloadBytes: defaultMaxPayloadBytes,
		maxKeyBytes:     defaultMaxKeyBytes,
	}
	c.scratch.New = func() any {
		b := make([]byte, 0, 64)
		return &b
	}
	if c.opMap, err = parseOpMapping(cfg["opMapping"]); err != nil {
		return nil, err
	}
	if c.maxPayloadBytes, err = intConfig(cfg, "maxPayloadBytes", defaultMaxPayloadBytes); err != nil {
		return nil, err
	}
	if c.maxKeyBytes, err = intConfig(cfg, "maxKeyBytes", defaultMaxKeyBytes); err != nil {
		return nil, err
	}
	if c.retainRaw, err = boolConfig(cfg, "retainRaw"); err != nil {
		return nil, err
	}
	return c, nil
}

func templateFieldList() string {
	names := make([]string, 0, numFields)
	for _, spec := range configKeys {
		names = append(names, spec.defaultName)
	}
	return strings.Join(names, ", ")
}

func (c *templateCodec) Name() string { return "template" }

// Decode matches the payload against the pattern and fills the captured fields.
func (c *templateCodec) Decode(payload []byte, topic string, dst *event.Event) error {
	if len(payload) == 0 {
		return fmt.Errorf("%w: empty payload", ErrMalformed)
	}
	if len(payload) > c.maxPayloadBytes {
		return fmt.Errorf("%w: payload is %d bytes, limit is %d",
			ErrTooLarge, len(payload), c.maxPayloadBytes)
	}

	// A trailing newline is not part of the line, and a pattern anchored with
	// $ would otherwise fail on every event read from a file source.
	payload = trimTrailingNewline(payload)

	idx := c.re.FindSubmatchIndex(payload)
	if idx == nil {
		return fmt.Errorf("%w: payload does not match the configured pattern", ErrMalformed)
	}

	observed := dst.ObservedAt
	*dst = event.Event{ObservedAt: observed, Topic: topic}

	scratch := c.borrow()
	defer c.release(scratch)

	for group := 1; group*2+1 < len(idx); group++ {
		f := c.groups[group]
		if f == numFields {
			continue
		}
		start, end := idx[group*2], idx[group*2+1]
		if start < 0 {
			// A group inside an alternation that did not participate. Absent is
			// not the same as empty, and it must not overwrite what another
			// group set.
			continue
		}
		if err := c.assign(dst, f, string(payload[start:end]), *scratch); err != nil {
			return err
		}
	}

	if dst.Op == event.OpUnknown {
		return fmt.Errorf("%w: op", ErrMissingField)
	}
	if len(dst.Key) > c.maxKeyBytes {
		return fmt.Errorf("%w: key is %d bytes, limit is %d",
			ErrTooLarge, len(dst.Key), c.maxKeyBytes)
	}
	if c.retainRaw {
		dst.Raw = append([]byte(nil), payload...)
	}
	return nil
}

// assign parses one captured string into the event field it names.
//
// Everything arrives as text, so every field is a parse. That is the cost of
// the escape hatch, and it is why §7 says not to run this at high throughput.
func (c *templateCodec) assign(dst *event.Event, f field, raw string, scratch []byte) error {
	switch f {
	case fieldPublisher:
		dst.Publisher = raw

	case fieldEpoch:
		n, err := templateUint(raw, "epoch")
		if err != nil {
			return err
		}
		dst.Epoch = n

	case fieldSeq:
		n, err := templateUint(raw, "seq")
		if err != nil {
			return err
		}
		dst.Seq = n

	case fieldTimestamp:
		ts, err := templateTime(raw)
		if err != nil {
			return err
		}
		dst.PublishedAt = ts

	case fieldOp:
		op, err := c.op(raw, scratch)
		if err != nil {
			return err
		}
		dst.Op = op

	case fieldKey:
		dst.Key = raw

	case fieldMember:
		dst.Member = raw

	case fieldValue:
		dst.Value = []byte(raw)

	case fieldTTL:
		ttl, err := templateTTL(raw)
		if err != nil {
			return err
		}
		dst.TTL = ttl

	case fieldDelta:
		n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
		if err != nil {
			return fmt.Errorf("%w: delta %q is not an integer", ErrMalformed, raw)
		}
		dst.Delta = n

	case numFields:
		// Unreachable: filtered by the caller.
	}
	return nil
}

func (c *templateCodec) op(name string, scratch []byte) (event.Op, error) {
	if op, ok := c.opMap[name]; ok {
		return op, nil
	}
	if op, ok := c.opNames[normalizeOpName([]byte(name), scratch)]; ok {
		return op, nil
	}
	return event.OpUnknown, fmt.Errorf("%w: %q", ErrUnknownOp, name)
}

// templateUint parses a sequence number or epoch from its digits.
//
// Straight from the text, never through a float — the same rule D-002 bought
// for the json codec, applied here where it is easy to get right and would be
// invisible if it were wrong.
func templateUint(raw, name string) (uint64, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, nil
	}
	n, err := strconv.ParseUint(trimmed, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %s %q is not an unsigned integer", ErrMalformed, name, raw)
	}
	return n, nil
}

// templateTime accepts RFC 3339 or a Unix integer, matching the json codec.
func templateTime(raw string) (time.Time, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}, nil
	}
	if ts, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return ts.UTC(), nil
	}
	n, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf(
			"%w: timestamp %q is neither RFC3339 nor a Unix integer", ErrMalformed, raw)
	}
	return unixTimestamp(n), nil
}

// templateTTL accepts a Go duration or a bare number of seconds.
func templateTTL(raw string) (*time.Duration, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}
	if d, err := time.ParseDuration(trimmed); err == nil {
		return &d, nil
	}
	secs, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: ttl %q is neither a duration nor a number of seconds",
			ErrMalformed, raw)
	}
	d := time.Duration(secs) * time.Second
	return &d, nil
}

// trimTrailingNewline removes one CRLF or LF from the end.
//
// The CR is only removed as part of a CRLF pair. A lone trailing CR is data —
// no format driftwatch reads terminates a line with one — and stripping it
// would silently alter the last captured field, which for a key means the
// oracle and the store disagree about a name for a reason nothing reports.
func trimTrailingNewline(b []byte) []byte {
	n := len(b)
	if n == 0 || b[n-1] != '\n' {
		return b
	}
	b = b[:n-1]

	if n := len(b); n > 0 && b[n-1] == '\r' {
		b = b[:n-1]
	}
	return b
}

// borrow and release mirror the json codec's pool accessors, including the
// total form: the pool's New only ever produces *[]byte, but returning a fresh
// buffer rather than panicking keeps the ingest hot path free of a way to fail.
func (c *templateCodec) borrow() *[]byte {
	if b, ok := c.scratch.Get().(*[]byte); ok {
		return b
	}
	b := make([]byte, 0, 64)
	return &b
}

func (c *templateCodec) release(b *[]byte) { c.scratch.Put(b) }
