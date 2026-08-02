package codec

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/nabrahma/driftwatch/pkg/event"
)

func init() { Register("msgpack", newMsgpack) }

// msgpackCodec decodes msgpack maps using the same configurable field mapping
// as the json codec.
//
// The field renaming, the op mapping and the limits are all identical, and
// deliberately so: an operator who has configured one should not have to learn
// a second vocabulary to configure the other. `codec: {type: msgpack}` on an
// otherwise unchanged spec is meant to just work.
//
// Where it differs from json is in what it does *not* have to defend against.
// msgpack encodes integers as integers, so D-002 — a 64-bit sequence number
// silently rounded through float64 — cannot happen at the wire level. It can
// still happen one level up if a producer encodes seq as a float, so that is
// rejected explicitly rather than assumed away.
type msgpackCodec struct {
	// names maps a wire field name onto the event fields it fills. A slice
	// because several event fields may legitimately read one wire field: a
	// producer emitting {"replica_id": "vllm-2"} means that replica as both
	// the publisher and the set member.
	names map[string][]field

	opMap   map[string]event.Op
	opNames map[string]event.Op
	scratch sync.Pool

	maxPayloadBytes int
	maxKeyBytes     int
	retainRaw       bool
}

func newMsgpack(cfg map[string]string) (Codec, error) {
	c := &msgpackCodec{
		names:           make(map[string][]field, numFields),
		opNames:         builtinOpNames(),
		maxPayloadBytes: defaultMaxPayloadBytes,
		maxKeyBytes:     defaultMaxKeyBytes,
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
		c.names[name] = append(c.names[name], field(f)) //nolint:gosec // f indexes a fixed table
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
	if c.retainRaw, err = boolConfig(cfg, "retainRaw"); err != nil {
		return nil, err
	}

	// maxDepth is validated and then ignored, which is better than rejecting a
	// spec that sets it. A msgpack document's nesting is bounded by its own
	// length prefixes — there is no equivalent of a JSON bomb, where a few
	// hundred bytes of "[[[[[" force unbounded recursion — and the decoder
	// below never descends into a nested value at all.
	if _, ok := cfg["maxDepth"]; ok {
		if _, err := intConfig(cfg, "maxDepth", defaultMaxDepth); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (c *msgpackCodec) Name() string { return "msgpack" }

// Decode parses a msgpack map into dst.
//
// It decodes into map[string]any rather than into a struct because the field
// names are operator-configurable and a struct's tags are fixed at compile
// time. The cost is an allocation per event, which is why §7 lists msgpack as
// supported rather than as the fast path — json's hand-written scanner exists
// precisely because that allocation was not acceptable there.
func (c *msgpackCodec) Decode(payload []byte, topic string, dst *event.Event) (err error) {
	// vmihailenco/msgpack is a third-party decoder running on adversarial
	// input, and the one property this package guarantees above all others is
	// that Decode never panics. The recover is not a substitute for the library
	// being correct; it is the guarantee holding even when it is not.
	//
	// A panic becomes ErrMalformed, which is what the input was.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: msgpack decoder panicked: %v", ErrMalformed, r)
		}
	}()

	if len(payload) == 0 {
		return fmt.Errorf("%w: empty payload", ErrMalformed)
	}
	if len(payload) > c.maxPayloadBytes {
		return fmt.Errorf("%w: payload is %d bytes, limit is %d",
			ErrTooLarge, len(payload), c.maxPayloadBytes)
	}

	var fields map[string]any
	if err := msgpack.Unmarshal(payload, &fields); err != nil {
		return fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	if fields == nil {
		return fmt.Errorf("%w: payload is not a map", ErrMalformed)
	}

	// Every field of dst except ObservedAt, which belongs to the source that
	// received the frame and is the only clock driftwatch trusts for elapsed
	// time.
	observed := dst.ObservedAt
	*dst = event.Event{ObservedAt: observed, Topic: topic}

	scratch := c.borrow()
	defer c.release(scratch)

	var sawOp bool
	for name, raw := range fields {
		targets, wanted := c.names[name]
		if !wanted {
			continue
		}
		for _, f := range targets {
			if err := c.assign(dst, f, raw, *scratch); err != nil {
				return err
			}
			if f == fieldOp {
				sawOp = true
			}
		}
	}

	if !sawOp {
		return fmt.Errorf("%w: op", ErrMissingField)
	}
	if len(dst.Key) > c.maxKeyBytes {
		return fmt.Errorf("%w: key is %d bytes, limit is %d",
			ErrTooLarge, len(dst.Key), c.maxKeyBytes)
	}
	if c.retainRaw {
		dst.Raw = bytes.Clone(payload)
	}
	return nil
}

// assign writes one decoded value into the event field it was mapped onto.
func (c *msgpackCodec) assign(dst *event.Event, f field, raw any, scratch []byte) error {
	switch f {
	case fieldPublisher:
		s, err := msgpackString(raw, "publisher")
		if err != nil {
			return err
		}
		dst.Publisher = s

	case fieldEpoch:
		n, err := msgpackUint(raw, "epoch")
		if err != nil {
			return err
		}
		dst.Epoch = n

	case fieldSeq:
		n, err := msgpackUint(raw, "seq")
		if err != nil {
			return err
		}
		dst.Seq = n

	case fieldTimestamp:
		ts, err := msgpackTime(raw)
		if err != nil {
			return err
		}
		dst.PublishedAt = ts

	case fieldOp:
		s, err := msgpackString(raw, "op")
		if err != nil {
			return err
		}
		op, err := c.op(s, scratch)
		if err != nil {
			return err
		}
		dst.Op = op

	case fieldKey:
		s, err := msgpackString(raw, "key")
		if err != nil {
			return err
		}
		dst.Key = s

	case fieldMember:
		s, err := msgpackString(raw, "member")
		if err != nil {
			return err
		}
		dst.Member = s

	case fieldValue:
		v, err := msgpackValue(raw)
		if err != nil {
			return err
		}
		dst.Value = v

	case fieldTTL:
		ttl, err := msgpackTTL(raw)
		if err != nil {
			return err
		}
		dst.TTL = ttl

	case fieldDelta:
		n, err := msgpackInt(raw, "delta")
		if err != nil {
			return err
		}
		dst.Delta = n

	case numFields:
		// Unreachable: numFields is the table's length, never an entry in it.
	}
	return nil
}

// op resolves an operation name, preferring the operator's configured mapping.
//
// The configured mapping is matched first and exactly, because a producer that
// says BLOCK_STORED means whatever the operator said it means — even when the
// name happens to normalize onto a built-in.
func (c *msgpackCodec) op(name string, scratch []byte) (event.Op, error) {
	if op, ok := c.opMap[name]; ok {
		return op, nil
	}
	if op, ok := c.opNames[normalizeOpName([]byte(name), scratch)]; ok {
		return op, nil
	}
	return event.OpUnknown, fmt.Errorf("%w: %q", ErrUnknownOp, name)
}

// msgpackString accepts a string or raw bytes, which is how a producer that
// writes bin8 rather than str8 encodes a key.
func msgpackString(raw any, name string) (string, error) {
	switch v := raw.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case nil:
		return "", nil
	default:
		return "", fmt.Errorf("%w: %s must be a string, got %T", ErrMalformed, name, raw)
	}
}

// msgpackUint reads an unsigned 64-bit field.
//
// A float is rejected rather than converted, which is D-002's lesson applied to
// a format where the trap sits one level higher. msgpack can carry a uint64
// exactly, so a producer that encoded seq as a float has already lost precision
// above 2^53 before the bytes were written. Converting it here would launder a
// corrupted number into a plausible one — which is exactly what made D-002
// expensive to find the first time.
func msgpackUint(raw any, name string) (uint64, error) {
	switch v := raw.(type) {
	case uint64:
		return v, nil
	case uint:
		return uint64(v), nil
	case uint32:
		return uint64(v), nil
	case uint16:
		return uint64(v), nil
	case uint8:
		return uint64(v), nil
	case int64:
		if v < 0 {
			return 0, fmt.Errorf("%w: %s must not be negative, got %d", ErrMalformed, name, v)
		}
		return uint64(v), nil
	case int:
		return msgpackUint(int64(v), name)
	case int32:
		return msgpackUint(int64(v), name)
	case int16:
		return msgpackUint(int64(v), name)
	case int8:
		return msgpackUint(int64(v), name)
	case float32, float64:
		return 0, fmt.Errorf(
			"%w: %s was encoded as a float; a 64-bit value above 2^53 does not "+
				"survive that, and driftwatch will not guess which one this was "+
				"(docs/DISCOVERIES.md D-002)", ErrMalformed, name)
	case string:
		// A numeric string is still a number, and some producers quote large
		// integers precisely to avoid the float problem above.
		n, err := strconv.ParseUint(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%w: %s %q is not an unsigned integer", ErrMalformed, name, v)
		}
		return n, nil
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("%w: %s must be an integer, got %T", ErrMalformed, name, raw)
	}
}

// msgpackInt reads a signed 64-bit field. delta is the only one.
func msgpackInt(raw any, name string) (int64, error) {
	switch v := raw.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("%w: %s %d overflows int64", ErrMalformed, name, v)
		}
		return int64(v), nil //nolint:gosec // bounded by the check above
	case uint:
		return msgpackInt(uint64(v), name)
	case uint32:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case float32, float64:
		return 0, fmt.Errorf("%w: %s must be an integer, not a float", ErrMalformed, name)
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%w: %s %q is not an integer", ErrMalformed, name, v)
		}
		return n, nil
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("%w: %s must be an integer, got %T", ErrMalformed, name, raw)
	}
}

// msgpackValue keeps the payload as bytes, whatever shape it arrived in.
//
// The projection decides what a value means; the codec's job is to hand it over
// unchanged. A number is rendered in canonical decimal rather than kept as a
// float, so that a scalar projection comparing against a store holding "42"
// sees "42".
func msgpackValue(raw any) ([]byte, error) {
	switch v := raw.(type) {
	case nil:
		return nil, nil
	case string:
		return []byte(v), nil
	case []byte:
		return bytes.Clone(v), nil
	case bool:
		return []byte(strconv.FormatBool(v)), nil
	case int64:
		return []byte(strconv.FormatInt(v, 10)), nil
	case uint64:
		return []byte(strconv.FormatUint(v, 10)), nil
	case int:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case float64:
		return []byte(strconv.FormatFloat(v, 'g', -1, 64)), nil
	case float32:
		return []byte(strconv.FormatFloat(float64(v), 'g', -1, 32)), nil
	default:
		return nil, fmt.Errorf(
			"%w: value must be a scalar or bytes, got %T; a nested structure has "+
				"no single representation the target could be compared against",
			ErrMalformed, raw)
	}
}

// msgpackTime converts the producer's clock reading.
//
// msgpack has a native timestamp extension type, which the library decodes to
// time.Time — so unlike json there is a form here needing no interpretation at
// all. The Unix-integer and RFC 3339 forms are accepted too, because a producer
// that ported its JSON encoder field-for-field will send those.
func msgpackTime(raw any) (time.Time, error) {
	switch v := raw.(type) {
	case time.Time:
		return v.UTC(), nil
	case nil:
		return time.Time{}, nil
	case string:
		if strings.TrimSpace(v) == "" {
			return time.Time{}, nil
		}
		if ts, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return ts.UTC(), nil
		}
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return time.Time{}, fmt.Errorf(
				"%w: timestamp %q is neither RFC3339 nor a Unix integer", ErrMalformed, v)
		}
		return unixTimestamp(n), nil
	case float32, float64:
		return time.Time{}, fmt.Errorf("%w: timestamp must be an integer", ErrMalformed)
	default:
		n, err := msgpackInt(raw, "timestamp")
		if err != nil {
			return time.Time{}, err
		}
		return unixTimestamp(n), nil
	}
}

// msgpackTTL converts an expiry. A number is seconds and a string is a Go
// duration, matching the json codec exactly.
//
// Absent and zero are deliberately different: absent means the event says
// nothing about expiry, zero means expire immediately.
func msgpackTTL(raw any) (*time.Duration, error) {
	if raw == nil {
		return nil, nil
	}
	if s, ok := raw.(string); ok {
		trimmed := strings.TrimSpace(s)
		if trimmed == "" {
			return nil, nil
		}
		d, err := time.ParseDuration(trimmed)
		if err != nil {
			return nil, fmt.Errorf("%w: ttl %q is not a duration", ErrMalformed, s)
		}
		return &d, nil
	}
	secs, err := msgpackInt(raw, "ttl")
	if err != nil {
		return nil, err
	}
	d := time.Duration(secs) * time.Second
	return &d, nil
}

// borrow and release mirror the json codec's pool accessors, including the
// total form: the pool's New only ever produces *[]byte, but returning a fresh
// buffer rather than panicking keeps the ingest hot path free of a way to fail.
func (c *msgpackCodec) borrow() *[]byte {
	if b, ok := c.scratch.Get().(*[]byte); ok {
		return b
	}
	b := make([]byte, 0, 64)
	return &b
}

func (c *msgpackCodec) release(b *[]byte) { c.scratch.Put(b) }
