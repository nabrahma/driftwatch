package source

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nabrahma/driftwatch/pkg/clock"
)

func init() { Register("file", newFile) }

// Replay speeds. As-fast-as-possible is the default because the point of a
// replay is usually to get an answer, not to relive the original timing.
const (
	SpeedAsFastAsPossible = "asFastAsPossible"
	SpeedRealtime         = "realtime"
)

// initialScanBuffer is where the line scanner starts. It grows to
// MaxPayloadBytes, so a large event is read rather than refused, but a file of
// small events never allocates the maximum.
const initialScanBuffer = 64 << 10

// FileSource replays newline-delimited JSON from a file or stdin.
//
// This is how a captured production stream gets replayed against a new
// projection — the backbone of `driftwatch replay`, and the reason a drift
// investigation can be re-run offline as many times as it takes. It is also the
// one source that cannot lose messages: it reads every byte or it fails, which
// is why it does not implement GapSignaller.
type FileSource struct {
	path  string
	speed float64 // 0 means as fast as possible
	loop  bool
	clk   clock.Clock
	c     counters

	maxPayload int

	mu     sync.Mutex
	closed bool
	// stdin is neither reopened per loop nor closed by this source, which does
	// not own it.
	stdin io.ReadCloser
}

// FileOption configures a FileSource.
type FileOption func(*FileSource)

// WithSpeed sets the replay rate: 0 or negative for as-fast-as-possible, 1 for
// the original timing, 2 for twice as fast.
func WithSpeed(multiplier float64) FileOption {
	return func(f *FileSource) { f.speed = multiplier }
}

// WithLoop replays the file endlessly, which is how a soak test gets a long
// stream out of a short capture.
func WithLoop(loop bool) FileOption {
	return func(f *FileSource) { f.loop = loop }
}

// NewFile returns a replay source. A path of "-" reads stdin.
func NewFile(path string, clk clock.Clock, opts ...FileOption) (*FileSource, error) {
	if path == "" {
		return nil, fmt.Errorf("%w: file.path is required", ErrBadConfig)
	}
	if clk == nil {
		clk = clock.Real()
	}

	f := &FileSource{path: path, clk: clk, maxPayload: defaultMaxPayloadBytes}
	for _, opt := range opts {
		opt(f)
	}
	if f.loop && path == "-" {
		return nil, fmt.Errorf(
			"%w: file.loop cannot be used with stdin, which cannot be rewound", ErrBadConfig)
	}
	return f, nil
}

func newFile(cfg Config, clk clock.Clock) (Source, error) {
	loop, err := cfg.SettingBool("loop", false)
	if err != nil {
		return nil, err
	}

	speed, err := parseSpeed(cfg.Setting("speed", SpeedAsFastAsPossible))
	if err != nil {
		return nil, err
	}

	f, err := NewFile(cfg.Setting("path", ""), clk, WithSpeed(speed), WithLoop(loop))
	if err != nil {
		return nil, err
	}
	f.maxPayload = cfg.MaxPayloadBytes
	return f, nil
}

// parseSpeed accepts the two named speeds or a positive multiplier.
func parseSpeed(raw string) (float64, error) {
	switch raw {
	case SpeedAsFastAsPossible, "":
		return 0, nil
	case SpeedRealtime:
		return 1, nil
	}

	n, err := strconv.ParseFloat(raw, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf(
			"%w: file.speed must be %q, %q or a positive multiplier, got %q",
			ErrBadConfig, SpeedAsFastAsPossible, SpeedRealtime, raw)
	}
	return n, nil
}

// Name returns the registry name.
func (f *FileSource) Name() string { return "file" }

// Run replays the file until it ends or ctx is done.
func (f *FileSource) Run(ctx context.Context, out chan<- RawMessage) error {
	for {
		if err := f.replayOnce(ctx, out); err != nil {
			return err
		}
		if !f.loop || f.isClosed() {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// replayOnce reads the file from the top and delivers every line.
func (f *FileSource) replayOnce(ctx context.Context, out chan<- RawMessage) error {
	r, isStdin, err := f.open()
	if err != nil {
		f.c.fail(err)
		return err
	}
	if !isStdin {
		defer func() { _ = r.Close() }() //nolint:errcheck // read-only file; a close error changes nothing
	}

	f.c.connected(true)
	defer f.c.connected(false)

	scanner := bufio.NewScanner(r)
	// The starting buffer must not exceed the maximum, or the limit never
	// fires: bufio only reports a too-long token when it needs to grow past
	// maxTokenSize, so a line that already fits in an oversized starting buffer
	// is accepted however large the limit said it could be.
	scanner.Buffer(make([]byte, min(initialScanBuffer, f.maxPayload)), f.maxPayload)

	// firstAt anchors realtime replay. The original timestamps are relative to
	// it, so a capture from last Tuesday replays with its own spacing rather
	// than being scheduled entirely in the past.
	var firstAt, startedAt time.Time

	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if f.isClosed() {
			return nil
		}

		line := scanner.Bytes()
		if len(bytesTrimSpace(line)) == 0 {
			// Blank lines are formatting, not events. A capture concatenated
			// from several files usually has them.
			continue
		}

		topic, at := envelope(line)
		if f.speed > 0 {
			if firstAt.IsZero() {
				firstAt, startedAt = at, f.clk.Now()
			}
			if err := f.pace(ctx, firstAt, startedAt, at); err != nil {
				return err
			}
		}

		// The scanner reuses its buffer, so the payload has to be copied before
		// it is handed on. Without this, every message downstream would end up
		// holding the same bytes: the last line read.
		payload := make([]byte, len(line))
		copy(payload, line)

		msg := RawMessage{Topic: topic, Payload: payload, ObservedAt: f.clk.Now()}
		if !send(ctx, out, msg) {
			return ctx.Err()
		}
		f.c.frame(len(payload), msg.ObservedAt)
	}

	if err := scanner.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			err = fmt.Errorf("%w: a line exceeded %d bytes", ErrPayloadTooLarge, f.maxPayload)
		}
		f.c.fail(err)
		return fmt.Errorf("replaying %s: %w", f.path, err)
	}
	return nil
}

// pace waits until this message is due under the configured replay speed.
func (f *FileSource) pace(ctx context.Context, firstAt, startedAt, at time.Time) error {
	if at.IsZero() || firstAt.IsZero() {
		return nil
	}

	offset := at.Sub(firstAt)
	if f.speed != 1 {
		offset = time.Duration(float64(offset) / f.speed)
	}

	wait := startedAt.Add(offset).Sub(f.clk.Now())
	if wait <= 0 {
		return nil
	}
	return f.clk.Sleep(ctx, wait)
}

// envelope pulls the topic and original timestamp out of a line, if present.
//
// It deliberately does not decode the event: that is pkg/codec's job, and the
// field names are configurable there. This reads only the two fields replay
// needs and is content to find neither — a capture from a foreign producer
// still replays, just without its original pacing.
func envelope(line []byte) (topic string, at time.Time) {
	if raw := scanJSONString(line, "topic"); raw != "" {
		topic = raw
	}
	if raw := scanJSONString(line, "ts"); raw != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
			at = parsed
		}
	}
	return topic, at
}

// scanJSONString finds a string field without decoding the object.
//
// A full decode here would cost an allocation-heavy pass over every line to
// read at most two fields, on the one source whose whole purpose is getting
// through a large file quickly.
func scanJSONString(line []byte, field string) string {
	s := string(line)
	needle := `"` + field + `"`

	i := strings.Index(s, needle)
	if i < 0 {
		return ""
	}
	rest := s[i+len(needle):]

	colon := strings.IndexByte(rest, ':')
	if colon < 0 {
		return ""
	}
	rest = strings.TrimLeft(rest[colon+1:], " \t")

	if rest == "" || rest[0] != '"' {
		return ""
	}
	rest = rest[1:]

	end := strings.IndexByte(rest, '"')
	if end < 0 {
		return ""
	}
	return rest[:end]
}

func bytesTrimSpace(b []byte) []byte {
	return []byte(strings.TrimSpace(string(b)))
}

// open returns a reader for the file, and whether it is stdin.
func (f *FileSource) open() (io.ReadCloser, bool, error) {
	if f.path == "-" {
		f.mu.Lock()
		defer f.mu.Unlock()

		if f.stdin == nil {
			f.stdin = os.Stdin
		}
		return f.stdin, true, nil
	}

	file, err := os.Open(f.path)
	if err != nil {
		return nil, false, fmt.Errorf("opening %s: %w", f.path, err)
	}
	return file, false, nil
}

func (f *FileSource) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// Stats returns transport-level counters.
func (f *FileSource) Stats() Stats { return f.c.snapshot() }

// Close stops the replay. Idempotent.
func (f *FileSource) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.closed = true
	return nil
}
