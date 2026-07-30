package codec

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/nabrahma/driftwatch/pkg/event"
)

func init() { Register("json", newJSON) }

// Default limits. Every one exists to bound something an adversarial or merely
// broken producer could otherwise make unbounded.
const (
	defaultMaxPayloadBytes = 1 << 20 // 1 MiB
	defaultMaxKeyBytes     = 4 << 10 // 4 KiB
	defaultMaxDepth        = 32
	defaultMaxPublishers   = 1024
)

// field identifies one of the event fields the decoder extracts.
type field uint8

const (
	fieldPublisher field = iota
	fieldEpoch
	fieldSeq
	fieldTimestamp
	fieldOp
	fieldKey
	fieldMember
	fieldValue
	fieldTTL
	fieldDelta
	numFields
)

// configKeys maps each field to the config key that renames it and to its
// default name in the canonical driftwatch format.
var configKeys = [numFields]struct{ cfgKey, defaultName string }{
	fieldPublisher: {"publisherField", "publisher"},
	fieldEpoch:     {"epochField", "epoch"},
	fieldSeq:       {"seqField", "seq"},
	fieldTimestamp: {"timestampField", "ts"},
	fieldOp:        {"opField", "op"},
	fieldKey:       {"keyField", "key"},
	fieldMember:    {"memberField", "member"},
	fieldValue:     {"valueField", "value"},
	fieldTTL:       {"ttlField", "ttl"},
	fieldDelta:     {"deltaField", "delta"},
}

// jsonCodec decodes the canonical driftwatch JSON format, or any foreign JSON
// object whose fields can be renamed onto it.
//
// It uses a hand-written scanner rather than encoding/json for three reasons,
// each of which is a correctness requirement rather than a performance one:
// a sequence number must be read from its raw digits so that values above 2^53
// survive (see docs/DISCOVERIES.md D-002), nesting depth must be bounded
// because an arbitrarily deep document is a denial-of-service vector, and a
// number written as a float must be rejected rather than silently rounded.
type jsonCodec struct {
	names map[string][]field
	// opMap holds the operator-configured foreign vocabulary, matched exactly.
	opMap map[string]event.Op
	// opNames holds every built-in operation under its normalized spelling, so
	// the hot path resolves an op with one map lookup and no allocation.
	opNames map[string]event.Op
	scratch sync.Pool

	publishers *interner
	topics     *interner

	maxPayloadBytes int
	maxKeyBytes     int
	maxDepth        int
	retainRaw       bool
}

func newJSON(cfg map[string]string) (Codec, error) {
	c := &jsonCodec{
		names:           make(map[string][]field, numFields),
		opNames:         builtinOpNames(),
		publishers:      newInterner(defaultMaxPublishers),
		topics:          newInterner(defaultMaxPublishers),
		maxPayloadBytes: defaultMaxPayloadBytes,
		maxKeyBytes:     defaultMaxKeyBytes,
		maxDepth:        defaultMaxDepth,
	}
	c.scratch.New = func() any {
		b := make([]byte, 0, 256)
		return &b
	}

	for f, spec := range configKeys {
		name := spec.defaultName
		if override, ok := cfg[spec.cfgKey]; ok {
			if override == "" {
				return nil, fmt.Errorf("%w: %s must not be empty", ErrBadConfig, spec.cfgKey)
			}
			name = override
		}
		// Several event fields may deliberately read from the same JSON field.
		// A KV-cache producer that emits {"replica_id":"vllm-2",...} means that
		// replica as both the publisher and the set member, and forbidding the
		// alias would make the most common foreign format inexpressible.
		c.names[name] = append(c.names[name], field(f)) //nolint:gosec // f indexes a fixed table of fields
	}

	var err error
	if c.opMap, err = parseOpMapping(cfg["opMapping"]); err != nil {
		return nil, err
	}
	if c.maxPayloadBytes, err = intConfig(cfg, "maxPayloadBytes", defaultMaxPayloadBytes); err != nil {
		return nil, err
	}
	if c.maxKeyBytes, err = intConfig(cfg, "maxKeyBytes", defaultMaxKeyBytes); err != nil {
		return nil, err
	}
	if c.maxDepth, err = intConfig(cfg, "maxDepth", defaultMaxDepth); err != nil {
		return nil, err
	}
	if c.retainRaw, err = boolConfig(cfg, "retainRaw"); err != nil {
		return nil, err
	}
	return c, nil
}

// builtinOpNames indexes every operation under its normalized spelling.
//
// Resolving an op through this map costs one lookup and no allocation, which
// matters because it happens on every single event. event.ParseOp remains the
// authority; this is a precomputed cache of its answers.
func builtinOpNames() map[string]event.Op {
	ops := []event.Op{
		event.OpSet, event.OpDelete, event.OpAdd, event.OpRemove, event.OpIncr,
		event.OpSnapshotBegin, event.OpSnapshotEnd, event.OpHeartbeat,
	}
	out := make(map[string]event.Op, len(ops))
	for _, op := range ops {
		out[normalizeOpName([]byte(op.String()), nil)] = op
	}
	return out
}

// normalizeOpName lowercases ASCII and strips the separators that distinguish
// spellings of the same operation, writing into scratch so the hot path
// allocates nothing. It mirrors event.ParseOp's normalization.
func normalizeOpName(raw, scratch []byte) string {
	scratch = scratch[:0]
	for _, ch := range raw {
		if ch == '_' || ch == '-' {
			continue
		}
		if ch >= 'A' && ch <= 'Z' {
			ch += 'a' - 'A'
		}
		scratch = append(scratch, ch)
	}
	return string(scratch)
}

// parseOpMapping parses "BLOCK_STORED=add,BLOCK_EVICTED=remove" into a lookup
// from the producer's operation names onto driftwatch's.
func parseOpMapping(spec string) (map[string]event.Op, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, nil
	}
	out := map[string]event.Op{}
	for _, pair := range strings.Split(spec, ",") {
		name, target, found := strings.Cut(pair, "=")
		name, target = strings.TrimSpace(name), strings.TrimSpace(target)
		if !found || name == "" || target == "" {
			return nil, fmt.Errorf("%w: opMapping entry %q is not name=op", ErrBadConfig, pair)
		}
		op, err := event.ParseOp(target)
		if err != nil {
			return nil, fmt.Errorf("%w: opMapping target %q: %w", ErrBadConfig, target, err)
		}
		out[name] = op
	}
	return out, nil
}

func intConfig(cfg map[string]string, key string, def int) (int, error) {
	raw, ok := cfg[key]
	if !ok {
		return def, nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%w: %s must be a positive integer, got %q", ErrBadConfig, key, raw)
	}
	return n, nil
}

func boolConfig(cfg map[string]string, key string) (bool, error) {
	raw, ok := cfg[key]
	if !ok {
		return false, nil
	}
	v, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		return false, fmt.Errorf("%w: %s must be a boolean, got %q", ErrBadConfig, key, raw)
	}
	return v, nil
}

// Name returns the registry name.
func (c *jsonCodec) Name() string { return "json" }

// Decode parses payload into dst.
func (c *jsonCodec) Decode(payload []byte, topic string, dst *event.Event) error {
	if len(payload) > c.maxPayloadBytes {
		return fmt.Errorf("%w: payload is %d bytes, limit is %d",
			ErrTooLarge, len(payload), c.maxPayloadBytes)
	}

	// ObservedAt belongs to the source that received the frame; everything else
	// comes from the wire.
	observed := dst.ObservedAt
	*dst = event.Event{ObservedAt: observed, Topic: c.topics.getString(topic)}

	var caps [numFields]capture
	if err := c.scan(payload, &caps); err != nil {
		return err
	}
	if err := c.assign(&caps, dst); err != nil {
		return err
	}
	if c.retainRaw {
		dst.Raw = append([]byte(nil), payload...)
	}
	return dst.Validate()
}

// capture is one field's raw value, recorded during the scan and converted
// afterwards. Recording rather than converting inline is what makes duplicate
// JSON keys behave like encoding/json, where the last occurrence wins.
type capture struct {
	raw    []byte
	kind   byte // 's' string, 'n' number, 'b' bool, 'z' null, 'c' container
	escape bool // the string contains backslash escapes
	seen   bool
}

// scan walks the top-level object, capturing the fields the codec cares about
// and skipping the rest.
func (c *jsonCodec) scan(b []byte, caps *[numFields]capture) error {
	p := &parser{b: b, maxDepth: c.maxDepth}

	p.ws()
	if !p.has() || p.b[p.i] != '{' {
		return fmt.Errorf("%w: payload is not a JSON object", ErrMalformed)
	}
	p.i++
	p.ws()
	if p.has() && p.b[p.i] == '}' {
		p.i++
		return p.eof()
	}

	for {
		p.ws()
		name, escaped, err := p.scanString()
		if err != nil {
			return err
		}
		p.ws()
		if !p.has() || p.b[p.i] != ':' {
			return fmt.Errorf("%w: expected ':' after object key", ErrMalformed)
		}
		p.i++
		p.ws()

		wanted := c.lookup(name, escaped)
		start := p.i
		kind, err := p.scanValue(0)
		if err != nil {
			return err
		}
		if len(wanted) > 0 {
			cp := capture{
				raw:    p.b[start:p.i],
				kind:   kind,
				escape: kind == 's' && p.lastStringEscaped,
				seen:   true,
			}
			for _, f := range wanted {
				caps[f] = cp
			}
		}

		p.ws()
		if !p.has() {
			return fmt.Errorf("%w: unterminated object", ErrMalformed)
		}
		switch p.b[p.i] {
		case ',':
			p.i++
		case '}':
			p.i++
			return p.eof()
		default:
			return fmt.Errorf("%w: expected ',' or '}' in object", ErrMalformed)
		}
	}
}

// lookup resolves a raw JSON key to the event fields it feeds, unescaping only
// when it has to.
func (c *jsonCodec) lookup(name []byte, escaped bool) []field {
	if !escaped {
		// The compiler elides the allocation for a map lookup keyed by a byte
		// slice conversion, so the common path costs nothing.
		return c.names[string(name)]
	}
	buf := c.borrow()
	defer c.release(buf)

	decoded, err := unescape((*buf)[:0], name)
	if err != nil {
		return nil
	}
	return c.names[string(decoded)]
}

// assign converts the captured raw values onto dst.
func (c *jsonCodec) assign(caps *[numFields]capture, dst *event.Event) error {
	var err error
	if dst.Op, err = c.resolveOp(caps[fieldOp]); err != nil {
		return err
	}

	if dst.Publisher, err = c.internedString(caps[fieldPublisher], c.publishers, "publisher"); err != nil {
		return err
	}
	if dst.Epoch, err = c.uint(caps[fieldEpoch], "epoch"); err != nil {
		return err
	}
	if dst.Seq, err = c.uint(caps[fieldSeq], "seq"); err != nil {
		return err
	}
	if dst.PublishedAt, err = c.timestamp(caps[fieldTimestamp]); err != nil {
		return err
	}
	if dst.Key, err = c.boundedString(caps[fieldKey], "key"); err != nil {
		return err
	}
	if dst.Member, err = c.boundedString(caps[fieldMember], "member"); err != nil {
		return err
	}
	if dst.Value, err = c.bytes(caps[fieldValue]); err != nil {
		return err
	}
	if dst.Delta, err = c.int(caps[fieldDelta], "delta"); err != nil {
		return err
	}
	if dst.TTL, err = c.ttl(caps[fieldTTL]); err != nil {
		return err
	}
	return nil
}

// fieldName reports the JSON field name currently mapped to f, so an error
// message names the field the operator configured rather than the internal one.
func (c *jsonCodec) fieldName(f field) string {
	for name, fields := range c.names {
		for _, got := range fields {
			if got == f {
				return name
			}
		}
	}
	return configKeys[f].defaultName
}

// resolveOp maps a producer's operation name onto an Op, consulting the
// configured mapping first so a foreign vocabulary wins over the built-in one.
//
// Both lookups are keyed by a byte-slice-to-string conversion inside an index
// expression, which the compiler turns into an allocation-free comparison. Only
// an escaped operation name — which no real producer emits — falls through to
// the allocating path.
func (c *jsonCodec) resolveOp(cp capture) (event.Op, error) {
	if !cp.present() {
		return event.OpUnknown, fmt.Errorf("%w: op (field %q)", ErrMissingField, c.fieldName(fieldOp))
	}
	if cp.kind != 's' {
		return event.OpUnknown, fmt.Errorf("%w: op must be a string", ErrMalformed)
	}

	if !cp.escape {
		body := cp.raw[1 : len(cp.raw)-1]
		if op, ok := c.opMap[string(body)]; ok {
			return op, nil
		}

		buf := c.borrow()
		normalized := normalizeOpName(body, *buf)
		op, ok := c.opNames[normalized]
		c.release(buf)
		if ok {
			return op, nil
		}
		return event.OpUnknown, fmt.Errorf("%w: %q", ErrUnknownOp, body)
	}

	name, err := c.string(cp, "op")
	if err != nil {
		return event.OpUnknown, err
	}
	if op, ok := c.opMap[name]; ok {
		return op, nil
	}
	return event.ParseOp(name)
}

// present reports whether the field appeared with a non-null value. An explicit
// null is treated exactly like an absent field.
func (cp capture) present() bool { return cp.seen && cp.kind != 'z' }

func (c *jsonCodec) borrow() *[]byte {
	if b, ok := c.scratch.Get().(*[]byte); ok {
		return b
	}
	// Unreachable: the pool's New only ever produces *[]byte. Returning a fresh
	// buffer keeps borrow total rather than panicking on the ingest hot path.
	b := make([]byte, 0, 256)
	return &b
}
func (c *jsonCodec) release(b *[]byte) { c.scratch.Put(b) }

// string converts a captured JSON string.
func (c *jsonCodec) string(cp capture, what string) (string, error) {
	if !cp.present() {
		return "", nil
	}
	if cp.kind != 's' {
		return "", fmt.Errorf("%w: %s must be a string", ErrMalformed, what)
	}
	body := cp.raw[1 : len(cp.raw)-1]
	if !cp.escape {
		return string(body), nil
	}
	buf := c.borrow()
	defer c.release(buf)

	decoded, err := unescape((*buf)[:0], body)
	if err != nil {
		return "", err
	}
	return string(decoded), nil
}

func (c *jsonCodec) internedString(cp capture, in *interner, what string) (string, error) {
	if cp.present() && cp.kind == 's' && !cp.escape {
		return in.get(cp.raw[1 : len(cp.raw)-1]), nil
	}
	return c.string(cp, what)
}

// boundedString converts a captured string and enforces the key size cap. A key
// larger than the cap is rejected rather than truncated: Redis permits keys up
// to 512 MB, and tracking one would be a memory-exhaustion vector.
func (c *jsonCodec) boundedString(cp capture, what string) (string, error) {
	if cp.present() && cp.kind == 's' && len(cp.raw)-2 > c.maxKeyBytes {
		return "", fmt.Errorf("%w: %s is %d bytes, limit is %d",
			ErrTooLarge, what, len(cp.raw)-2, c.maxKeyBytes)
	}
	s, err := c.string(cp, what)
	if err != nil {
		return "", err
	}
	if len(s) > c.maxKeyBytes {
		return "", fmt.Errorf("%w: %s is %d bytes, limit is %d",
			ErrTooLarge, what, len(s), c.maxKeyBytes)
	}
	return s, nil
}

// bytes converts a captured string into a value payload. The result never
// aliases the input, because the caller is free to reuse its buffer.
func (c *jsonCodec) bytes(cp capture) ([]byte, error) {
	if !cp.present() {
		return nil, nil
	}
	s, err := c.string(cp, "value")
	if err != nil {
		return nil, err
	}
	return []byte(s), nil
}

// uint converts a captured sequence or epoch number.
//
// The number is parsed from its raw digits, never through float64. A float64
// cannot represent an integer above 2^53 exactly, so a sequence number of
// 9007199254740993 would come back as 9007199254740992 — a silent off-by-one in
// exactly the value driftwatch uses to decide whether an event was lost.
func (c *jsonCodec) uint(cp capture, what string) (uint64, error) {
	if !cp.present() {
		return 0, nil
	}
	raw := cp.raw
	switch cp.kind {
	case 'n':
		if isFloatLiteral(raw) {
			return 0, fmt.Errorf(
				"%w: %s %s is written as a float; a float64 cannot hold an integer above 2^53 exactly",
				ErrMalformed, what, raw)
		}
	case 's':
		// Producers that know their sequence numbers exceed what JavaScript can
		// represent send them as strings. That is the correct thing to do.
		raw = raw[1 : len(raw)-1]
	default:
		return 0, fmt.Errorf("%w: %s must be a number or a numeric string", ErrMalformed, what)
	}

	n, err := strconv.ParseUint(string(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %s %q is not a uint64", ErrMalformed, what, raw)
	}
	return n, nil
}

// int converts a captured signed number, such as a counter delta.
func (c *jsonCodec) int(cp capture, what string) (int64, error) {
	if !cp.present() {
		return 0, nil
	}
	raw := cp.raw
	switch cp.kind {
	case 'n':
		if isFloatLiteral(raw) {
			return 0, fmt.Errorf("%w: %s %s must be an integer", ErrMalformed, what, raw)
		}
	case 's':
		raw = raw[1 : len(raw)-1]
	default:
		return 0, fmt.Errorf("%w: %s must be a number or a numeric string", ErrMalformed, what)
	}

	n, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %s %q is not an int64", ErrMalformed, what, raw)
	}
	return n, nil
}

// Boundaries for timestamp magnitude detection. A Unix value below
// unixSecondsCeiling is read as seconds (the year 5138), below
// unixMillisCeiling as milliseconds (also the year 5138), and anything larger
// as nanoseconds.
//
// The rule is ambiguous by construction: 10_000_000_000 is both a plausible
// seconds value (year 2286) and a plausible milliseconds value (April 1970).
// Seconds wins, because a producer emitting 1970 timestamps is already broken
// in a way driftwatch cannot repair, while a 2286 timestamp is merely distant.
const (
	unixSecondsCeiling = int64(1e11)
	unixMillisCeiling  = int64(1e14)
)

// timestamp converts the producer's clock reading, accepting RFC3339,
// RFC3339Nano, and Unix seconds, milliseconds or nanoseconds.
//
// The result is diagnostic only. driftwatch never uses a producer's clock for
// settlement decisions, because a skew larger than the settlement window would
// make settlement unsound.
func (c *jsonCodec) timestamp(cp capture) (time.Time, error) {
	if !cp.present() {
		return time.Time{}, nil
	}

	switch cp.kind {
	case 's':
		// The canonical format sends RFC3339, on every event. Parsing it from
		// the raw bytes avoids materializing a string that is thrown away
		// immediately, which is one allocation per event on the ingest path.
		if !cp.escape {
			if ts, ok := parseRFC3339(cp.raw[1 : len(cp.raw)-1]); ok {
				return ts, nil
			}
		}

		s, err := c.string(cp, "timestamp")
		if err != nil {
			return time.Time{}, err
		}
		if s == "" {
			return time.Time{}, nil
		}
		if ts, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return ts.UTC(), nil
		}
		// A numeric string is still a number.
		if n, convErr := strconv.ParseInt(s, 10, 64); convErr == nil {
			return unixTimestamp(n), nil
		}
		return time.Time{}, fmt.Errorf("%w: timestamp %q is neither RFC3339 nor a Unix integer",
			ErrMalformed, s)
	case 'n':
		if isFloatLiteral(cp.raw) {
			return time.Time{}, fmt.Errorf("%w: timestamp %s must be an integer", ErrMalformed, cp.raw)
		}
		n, err := strconv.ParseInt(string(cp.raw), 10, 64)
		if err != nil {
			return time.Time{}, fmt.Errorf("%w: timestamp %q is not an int64", ErrMalformed, cp.raw)
		}
		return unixTimestamp(n), nil
	default:
		return time.Time{}, fmt.Errorf("%w: timestamp must be a string or a number", ErrMalformed)
	}
}

// unixTimestamp converts a Unix integer, detecting its unit by magnitude. Zero
// means "unset" rather than the Unix epoch: a producer that omits a clock
// reading and one that sends 1970 are the same thing in practice, and neither
// is a real publish time.
func unixTimestamp(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	magnitude := n
	if magnitude < 0 {
		magnitude = -magnitude
	}
	switch {
	case magnitude < unixSecondsCeiling:
		return time.Unix(n, 0).UTC()
	case magnitude < unixMillisCeiling:
		return time.UnixMilli(n).UTC()
	default:
		return time.Unix(0, n).UTC()
	}
}

// ttl converts an expiry. A number is read as seconds and a string as a Go
// duration, so both "30s" and 30 work.
//
// An absent field and a zero are deliberately different: absent means the event
// says nothing about expiry, and zero means expire immediately.
func (c *jsonCodec) ttl(cp capture) (*time.Duration, error) {
	if !cp.present() {
		return nil, nil
	}

	switch cp.kind {
	case 'n':
		if isFloatLiteral(cp.raw) {
			seconds, err := strconv.ParseFloat(string(cp.raw), 64)
			if err != nil {
				return nil, fmt.Errorf("%w: ttl %q is not a number", ErrMalformed, cp.raw)
			}
			d := time.Duration(seconds * float64(time.Second))
			return &d, nil
		}
		seconds, err := strconv.ParseInt(string(cp.raw), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: ttl %q is not an int64", ErrMalformed, cp.raw)
		}
		d := time.Duration(seconds) * time.Second
		return &d, nil
	case 's':
		s, err := c.string(cp, "ttl")
		if err != nil {
			return nil, err
		}
		d, parseErr := time.ParseDuration(s)
		if parseErr != nil {
			return nil, fmt.Errorf("%w: ttl %q is not a duration: %w", ErrMalformed, s, parseErr)
		}
		return &d, nil
	default:
		return nil, fmt.Errorf("%w: ttl must be a number or a duration string", ErrMalformed)
	}
}

// parseRFC3339 parses an RFC3339 or RFC3339Nano timestamp directly from bytes,
// returning ok=false for anything it does not recognize so the caller can fall
// back to time.Parse.
//
// It exists only to avoid one allocation per event. Every rejection path defers
// to time.Parse, so this can only ever be faster, never more permissive — and
// that constraint is load-bearing. If the two paths disagreed, the same
// timestamp would decode differently depending on whether the payload happened
// to contain a backslash somewhere.
//
// The uppercase-only 'T' and 'Z' are part of that constraint, not an oversight.
// RFC 3339 §5.6 permits the lowercase forms, but Go's time.RFC3339 layout does
// not accept them, so accepting them here would make the fast path a superset
// of the fallback. See docs/DISCOVERIES.md D-001.
func parseRFC3339(b []byte) (time.Time, bool) {
	// 2026-07-30T11:02:31Z is the shortest accepted form.
	if len(b) < 20 || b[4] != '-' || b[7] != '-' || b[10] != 'T' ||
		b[13] != ':' || b[16] != ':' {
		return time.Time{}, false
	}

	year, ok := atoiN(b[0:4])
	month, ok2 := atoiN(b[5:7])
	day, ok3 := atoiN(b[8:10])
	hour, ok4 := atoiN(b[11:13])
	minute, ok5 := atoiN(b[14:16])
	second, ok6 := atoiN(b[17:19])
	if !ok || !ok2 || !ok3 || !ok4 || !ok5 || !ok6 {
		return time.Time{}, false
	}

	i := 19
	nanos := 0
	if i < len(b) && b[i] == '.' {
		i++
		digits := 0
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			if digits < 9 {
				nanos = nanos*10 + int(b[i]-'0')
				digits++
			}
			i++
		}
		if digits == 0 {
			return time.Time{}, false
		}
		for ; digits < 9; digits++ {
			nanos *= 10
		}
	}

	if i >= len(b) {
		return time.Time{}, false
	}

	loc := time.UTC
	switch b[i] {
	case 'Z':
		if i != len(b)-1 {
			return time.Time{}, false
		}
	case '+', '-':
		// An offset needs a Location, and time.FixedZone allocates. Offsets are
		// rare enough in machine-generated timestamps that paying for them here
		// is the right trade.
		if len(b)-i != 6 || b[i+3] != ':' {
			return time.Time{}, false
		}
		offHour, okH := atoiN(b[i+1 : i+3])
		offMin, okM := atoiN(b[i+4 : i+6])
		if !okH || !okM {
			return time.Time{}, false
		}
		offset := offHour*3600 + offMin*60
		if b[i] == '-' {
			offset = -offset
		}
		loc = time.FixedZone("", offset)
	default:
		return time.Time{}, false
	}

	t := time.Date(year, time.Month(month), day, hour, minute, second, nanos, loc)

	// time.Date normalizes out-of-range components rather than rejecting them,
	// so "2026-13-45" would silently become a date in 2027. Round-tripping the
	// components catches that.
	if t.Year() != year || int(t.Month()) != month || t.Day() != day ||
		t.Hour() != hour || t.Minute() != minute || t.Second() != second {
		return time.Time{}, false
	}
	return t.UTC(), true
}

// atoiN parses a fixed-width run of ASCII digits.
func atoiN(b []byte) (int, bool) {
	n := 0
	for _, ch := range b {
		if ch < '0' || ch > '9' {
			return 0, false
		}
		n = n*10 + int(ch-'0')
	}
	return n, true
}

// isFloatLiteral reports whether a JSON number was written with a fraction or
// an exponent, and therefore cannot be trusted to be an exact integer.
func isFloatLiteral(raw []byte) bool {
	for _, ch := range raw {
		if ch == '.' || ch == 'e' || ch == 'E' {
			return true
		}
	}
	return false
}

// parser is a minimal JSON scanner. It validates structure but does not build a
// tree, so nothing is allocated for the fields the codec ignores.
type parser struct {
	b                 []byte
	i                 int
	maxDepth          int
	lastStringEscaped bool
}

func (p *parser) has() bool { return p.i < len(p.b) }

func (p *parser) ws() {
	for p.i < len(p.b) {
		switch p.b[p.i] {
		case ' ', '\t', '\n', '\r':
			p.i++
		default:
			return
		}
	}
}

// eof asserts that only whitespace follows. Trailing content usually means two
// events were concatenated into one frame, which is worth reporting rather than
// silently decoding the first.
func (p *parser) eof() error {
	p.ws()
	if p.has() {
		return fmt.Errorf("%w: trailing bytes after the top-level object", ErrMalformed)
	}
	return nil
}

// scanValue advances past one JSON value and reports its kind.
func (p *parser) scanValue(depth int) (byte, error) {
	if depth > p.maxDepth {
		return 0, fmt.Errorf("%w: nesting deeper than %d", ErrTooLarge, p.maxDepth)
	}
	if !p.has() {
		return 0, fmt.Errorf("%w: expected a value", ErrMalformed)
	}

	switch ch := p.b[p.i]; {
	case ch == '"':
		if _, _, err := p.scanString(); err != nil {
			return 0, err
		}
		return 's', nil
	case ch == '{':
		return 'c', p.scanObject(depth)
	case ch == '[':
		return 'c', p.scanArray(depth)
	case ch == 't':
		return 'b', p.scanLiteral("true")
	case ch == 'f':
		return 'b', p.scanLiteral("false")
	case ch == 'n':
		return 'z', p.scanLiteral("null")
	case ch == '-' || (ch >= '0' && ch <= '9'):
		return 'n', p.scanNumber()
	default:
		return 0, fmt.Errorf("%w: unexpected byte %q", ErrMalformed, ch)
	}
}

func (p *parser) scanObject(depth int) error {
	p.i++ // '{'
	p.ws()
	if p.has() && p.b[p.i] == '}' {
		p.i++
		return nil
	}
	for {
		p.ws()
		if _, _, err := p.scanString(); err != nil {
			return err
		}
		p.ws()
		if !p.has() || p.b[p.i] != ':' {
			return fmt.Errorf("%w: expected ':' in nested object", ErrMalformed)
		}
		p.i++
		p.ws()
		if _, err := p.scanValue(depth + 1); err != nil {
			return err
		}
		p.ws()
		if !p.has() {
			return fmt.Errorf("%w: unterminated nested object", ErrMalformed)
		}
		switch p.b[p.i] {
		case ',':
			p.i++
		case '}':
			p.i++
			return nil
		default:
			return fmt.Errorf("%w: expected ',' or '}' in nested object", ErrMalformed)
		}
	}
}

func (p *parser) scanArray(depth int) error {
	p.i++ // '['
	p.ws()
	if p.has() && p.b[p.i] == ']' {
		p.i++
		return nil
	}
	for {
		p.ws()
		if _, err := p.scanValue(depth + 1); err != nil {
			return err
		}
		p.ws()
		if !p.has() {
			return fmt.Errorf("%w: unterminated array", ErrMalformed)
		}
		switch p.b[p.i] {
		case ',':
			p.i++
		case ']':
			p.i++
			return nil
		default:
			return fmt.Errorf("%w: expected ',' or ']' in array", ErrMalformed)
		}
	}
}

func (p *parser) scanLiteral(want string) error {
	if p.i+len(want) > len(p.b) || string(p.b[p.i:p.i+len(want)]) != want {
		return fmt.Errorf("%w: expected %s", ErrMalformed, want)
	}
	p.i += len(want)
	return nil
}

// scanString advances past a JSON string and returns its contents, excluding
// the surrounding quotes and without unescaping.
func (p *parser) scanString() (body []byte, escaped bool, err error) {
	if !p.has() || p.b[p.i] != '"' {
		return nil, false, fmt.Errorf("%w: expected a string", ErrMalformed)
	}
	start := p.i
	p.i++

	for p.i < len(p.b) {
		switch p.b[p.i] {
		case '\\':
			escaped = true
			p.i += 2 // skip the escape and whatever it escapes
			continue
		case '"':
			p.i++
			p.lastStringEscaped = escaped
			return p.b[start+1 : p.i-1], escaped, nil
		}
		if p.b[p.i] < 0x20 {
			return nil, false, fmt.Errorf("%w: control byte %#x inside a string", ErrMalformed, p.b[p.i])
		}
		p.i++
	}
	return nil, false, fmt.Errorf("%w: unterminated string", ErrMalformed)
}

// scanNumber advances past a JSON number, validating the grammar so that
// "1.2.3" is rejected rather than half-consumed.
func (p *parser) scanNumber() error {
	start := p.i
	if p.has() && p.b[p.i] == '-' {
		p.i++
	}

	digits := 0
	for p.has() && p.b[p.i] >= '0' && p.b[p.i] <= '9' {
		p.i++
		digits++
	}
	if digits == 0 {
		return fmt.Errorf("%w: number with no digits", ErrMalformed)
	}

	if p.has() && p.b[p.i] == '.' {
		p.i++
		frac := 0
		for p.has() && p.b[p.i] >= '0' && p.b[p.i] <= '9' {
			p.i++
			frac++
		}
		if frac == 0 {
			return fmt.Errorf("%w: number with an empty fraction", ErrMalformed)
		}
	}

	if p.has() && (p.b[p.i] == 'e' || p.b[p.i] == 'E') {
		p.i++
		if p.has() && (p.b[p.i] == '+' || p.b[p.i] == '-') {
			p.i++
		}
		exp := 0
		for p.has() && p.b[p.i] >= '0' && p.b[p.i] <= '9' {
			p.i++
			exp++
		}
		if exp == 0 {
			return fmt.Errorf("%w: number with an empty exponent", ErrMalformed)
		}
	}

	if p.i == start {
		return fmt.Errorf("%w: empty number", ErrMalformed)
	}
	return nil
}

// unescape decodes JSON string escapes into dst, which is reused across calls.
func unescape(dst, src []byte) ([]byte, error) {
	for i := 0; i < len(src); {
		ch := src[i]
		if ch != '\\' {
			dst = append(dst, ch)
			i++
			continue
		}
		i++
		if i >= len(src) {
			return nil, fmt.Errorf("%w: string ends inside an escape", ErrMalformed)
		}
		switch src[i] {
		case '"':
			dst = append(dst, '"')
		case '\\':
			dst = append(dst, '\\')
		case '/':
			dst = append(dst, '/')
		case 'b':
			dst = append(dst, '\b')
		case 'f':
			dst = append(dst, '\f')
		case 'n':
			dst = append(dst, '\n')
		case 'r':
			dst = append(dst, '\r')
		case 't':
			dst = append(dst, '\t')
		case 'u':
			r, consumed, err := decodeUnicodeEscape(src[i:])
			if err != nil {
				return nil, err
			}
			dst = utf8.AppendRune(dst, r)
			i += consumed
			continue
		default:
			return nil, fmt.Errorf("%w: unknown escape \\%c", ErrMalformed, src[i])
		}
		i++
	}
	return dst, nil
}

// decodeUnicodeEscape decodes \uXXXX, joining a surrogate pair when one
// follows. src starts at the 'u'. It returns the rune and the number of bytes
// consumed from src.
func decodeUnicodeEscape(src []byte) (r rune, consumed int, err error) {
	if len(src) < 5 {
		return 0, 0, fmt.Errorf("%w: truncated \\u escape", ErrMalformed)
	}
	first, hexErr := hex4(src[1:5])
	if hexErr != nil {
		return 0, 0, hexErr
	}
	r = rune(first)
	consumed = 5

	if utf16.IsSurrogate(r) {
		if len(src) >= 11 && src[5] == '\\' && src[6] == 'u' {
			second, secondErr := hex4(src[7:11])
			if secondErr != nil {
				return 0, 0, secondErr
			}
			if combined := utf16.DecodeRune(r, rune(second)); combined != utf8.RuneError {
				return combined, 11, nil
			}
		}
		// A lone surrogate is not a character. Substituting the replacement
		// rune matches encoding/json and keeps the output valid UTF-8.
		return utf8.RuneError, consumed, nil
	}
	return r, consumed, nil
}

func hex4(b []byte) (uint32, error) {
	var v uint32
	for _, ch := range b {
		v <<= 4
		switch {
		case ch >= '0' && ch <= '9':
			v |= uint32(ch - '0')
		case ch >= 'a' && ch <= 'f':
			v |= uint32(ch-'a') + 10
		case ch >= 'A' && ch <= 'F':
			v |= uint32(ch-'A') + 10
		default:
			return 0, fmt.Errorf("%w: %q is not a hex digit in a \\u escape", ErrMalformed, ch)
		}
	}
	return v, nil
}

// interner deduplicates the small set of repeated strings on the hot path.
// Publishers and topics are few and appear on every single event, so interning
// them removes one allocation per event each.
//
// It is bounded: a producer that invents a new publisher name per event would
// otherwise turn this cache into a memory leak.
type interner struct {
	mu      sync.RWMutex
	m       map[string]string
	entries int
}

func newInterner(entries int) *interner {
	return &interner{m: make(map[string]string), entries: entries}
}

func (in *interner) get(b []byte) string {
	in.mu.RLock()
	s, ok := in.m[string(b)]
	in.mu.RUnlock()
	if ok {
		return s
	}

	s = string(b)
	in.store(s)
	return s
}

// getString is get for a value that is already a string, avoiding the
// byte-slice round trip.
func (in *interner) getString(k string) string {
	in.mu.RLock()
	s, ok := in.m[k]
	in.mu.RUnlock()
	if ok {
		return s
	}

	in.store(k)
	return k
}

func (in *interner) store(s string) {
	in.mu.Lock()
	if len(in.m) < in.entries {
		in.m[s] = s
	}
	in.mu.Unlock()
}

// compile-time assertion that the codec satisfies the interface.
var _ Codec = (*jsonCodec)(nil)
