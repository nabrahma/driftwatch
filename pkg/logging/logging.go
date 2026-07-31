// Package logging configures structured logging and redaction (§12.3, §18).
//
// logr over zap, because the operator half of driftwatch is controller-runtime
// and controller-runtime speaks logr. Everything else in the tool takes a
// logr.Logger, whose zero value is a working no-op — so a library can log
// without a caller having to supply a logger it does not want.
//
// Two things here exist because an incident made them necessary rather than
// because they are tidy. Redact hashes key names for environments where the
// keyspace is itself sensitive, and Sampler bounds repetitive error logging:
// a decode error on a malformed stream arrives at the full event rate, and a
// log line per event turns one publisher's bad deploy into a disk-full outage
// on every node running driftwatch.
package logging

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/nabrahma/driftwatch/pkg/clock"
)

// The log formats §12.3 allows.
const (
	FormatConsole = "console"
	FormatJSON    = "json"
)

// The log levels §12.3 allows. Trace is not a level: it is `-v 2`, because
// logr has verbosity rather than named levels below info.
const (
	LevelError = "error"
	LevelWarn  = "warn"
	LevelInfo  = "info"
	LevelDebug = "debug"
)

// Options configures the logger.
type Options struct {
	// Level is one of error, warn, info, debug. Default info.
	Level string
	// V raises verbosity: 1 is debug, 2 is trace. It only ever opens the
	// logger up, never closes it down, so `-v 2` works whatever Level says.
	V int
	// Format is console or json. Default console.
	Format string
	// Out is where lines are written. Default os.Stderr.
	//
	// Stderr, not stdout, and this is load-bearing rather than conventional:
	// §11 requires `--output json` to emit one well-formed document, which is
	// only true if no log line can ever land in the middle of it.
	Out io.Writer
	// RedactKeys hashes key names passed through Redact.
	RedactKeys bool
	// TimeFormat overrides the timestamp layout. Tests set it to produce
	// stable golden output.
	TimeFormat string
}

func (o *Options) applyDefaults() {
	if o.Level == "" {
		o.Level = LevelInfo
	}
	if o.Format == "" {
		o.Format = FormatConsole
	}
	if o.Out == nil {
		o.Out = os.Stderr
	}
}

// threshold converts the level and verbosity into a zap level.
//
// zapr maps logr V(n) onto zap level -n, so info is 0, debug is -1 and trace
// is -2. Everything below the threshold is dropped by zap before the message
// is even formatted.
func (o *Options) threshold() (zapcore.Level, error) {
	var lvl zapcore.Level
	switch o.Level {
	case LevelError:
		lvl = zapcore.ErrorLevel
	case LevelWarn:
		lvl = zapcore.WarnLevel
	case LevelInfo:
		lvl = zapcore.InfoLevel
	case LevelDebug:
		lvl = zapcore.DebugLevel
	default:
		return 0, fmt.Errorf("--log-level %q: must be one of %s, %s, %s, %s",
			o.Level, LevelError, LevelWarn, LevelInfo, LevelDebug)
	}

	// Only a positive V may move the threshold. Applying it unconditionally
	// would make V(0) — which every caller uses by default — lower `error` and
	// `warn` all the way back to `info`, quietly undoing the flag.
	if o.V > 0 {
		if v := zapcore.Level(-o.V); v < lvl { //nolint:gosec // V is a small flag value
			lvl = v
		}
	}
	return lvl, nil
}

// New returns a logger and a flush function.
//
// The flush function must be called before the process exits or buffered lines
// are lost, which is exactly the lines describing why it is exiting.
// runs once per process.
//
//nolint:gocritic // hugeParam: an options struct by value is the idiom, and this
func New(opts Options) (logr.Logger, func() error, error) {
	opts.applyDefaults()
	SetRedactKeys(opts.RedactKeys)

	noop := func() error { return nil }

	lvl, err := opts.threshold()
	if err != nil {
		return logr.Discard(), noop, err
	}

	encCfg := zap.NewProductionEncoderConfig()
	encCfg.TimeKey = "ts"
	encCfg.MessageKey = "msg"
	encCfg.LevelKey = "level"
	encCfg.CallerKey = ""
	encCfg.StacktraceKey = ""
	if opts.TimeFormat != "" {
		encCfg.EncodeTime = zapcore.TimeEncoderOfLayout(opts.TimeFormat)
	} else {
		encCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	}

	var enc zapcore.Encoder
	switch opts.Format {
	case FormatJSON:
		enc = zapcore.NewJSONEncoder(encCfg)
	case FormatConsole:
		encCfg.EncodeLevel = zapcore.CapitalLevelEncoder
		enc = zapcore.NewConsoleEncoder(encCfg)
	default:
		return logr.Discard(), noop,
			fmt.Errorf("--log-format %q: must be %s or %s", opts.Format, FormatConsole, FormatJSON)
	}

	zl := zap.New(zapcore.NewCore(enc, zapcore.AddSync(opts.Out), lvl))

	flush := func() error {
		// Sync on a terminal returns an error on several platforms and there is
		// nothing useful to do about it, so it is deliberately swallowed. A
		// real write failure has already surfaced at the write itself.
		_ = zl.Sync() //nolint:errcheck // see above
		return nil
	}
	return zapr.NewLogger(zl), flush, nil
}

// redactKeys is a process-wide toggle because §12.3 specifies Redact as a
// one-argument helper used consistently at every call site. Threading a config
// value through every logging call instead would guarantee that one of them
// eventually forgets, which is the failure this exists to prevent.
var redactKeys atomic.Bool

// SetRedactKeys turns key-name hashing on or off for the whole process.
func SetRedactKeys(on bool) { redactKeys.Store(on) }

// RedactKeysEnabled reports whether key names are being hashed.
func RedactKeysEnabled() bool { return redactKeys.Load() }

// Redact returns a key name safe to log.
//
// With redaction off it is the identity, because a tool that will not tell you
// which key diverged is much harder to debug with. With it on the key is
// hashed: distinct keys stay distinguishable across log lines, which is what
// correlating an incident needs, without the keyspace itself leaving the
// cluster.
func Redact(s string) string {
	if s == "" || !redactKeys.Load() {
		return s
	}
	sum := sha256.Sum256([]byte(s))
	return "sha256:" + hex.EncodeToString(sum[:8])
}

// defaultMaxReasons bounds the sampler's map.
const defaultMaxReasons = 64

// Sampler is a per-reason token bucket for repetitive log lines.
//
// §12.3's rule is first N, then one per interval, per unique reason. The
// per-reason part matters: a flood of decode errors must not silence the first
// target error, because the second is the one that explains the first.
type Sampler struct {
	mu         sync.Mutex
	clk        clock.Clock
	burst      int
	interval   time.Duration
	maxReasons int
	states     map[string]*sampleState
}

type sampleState struct {
	// allowed counts lines let through, up to burst.
	allowed int
	// nextAt is when the next line may pass once the burst is spent.
	nextAt time.Time
	// suppressed counts lines dropped since the last one that passed, so the
	// line that does pass can say how much it stands for.
	suppressed int
}

// NewSampler returns a sampler allowing burst lines per reason immediately and
// one per interval afterwards. maxReasons bounds the map; zero uses the default.
func NewSampler(clk clock.Clock, burst int, interval time.Duration, maxReasons int) *Sampler {
	if clk == nil {
		clk = clock.Real()
	}
	if burst < 0 {
		burst = 0
	}
	if interval <= 0 {
		interval = 10 * time.Second
	}
	if maxReasons <= 0 {
		maxReasons = defaultMaxReasons
	}

	return &Sampler{
		clk:        clk,
		burst:      burst,
		interval:   interval,
		maxReasons: maxReasons,
		states:     make(map[string]*sampleState, maxReasons),
	}
}

// Allow reports whether a line for this reason should be written, and how many
// lines were suppressed since the last one that was.
//
// A nil Sampler allows everything, so a caller with no sampler configured does
// not have to branch.
func (s *Sampler) Allow(reason string) (allow bool, suppressed int) {
	if s == nil {
		return true, 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clk.Now()
	st, ok := s.states[reason]
	if !ok {
		if len(s.states) >= s.maxReasons {
			s.evictQuietestLocked()
		}
		st = &sampleState{}
		s.states[reason] = st
	}

	switch {
	case st.allowed < s.burst:
		st.allowed++
	case !now.Before(st.nextAt):
		// The burst is spent but the interval has elapsed.
	default:
		st.suppressed++
		return false, 0
	}

	st.nextAt = now.Add(s.interval)
	n := st.suppressed
	st.suppressed = 0
	return true, n
}

// evictQuietestLocked drops the reason whose next allowance is furthest in the
// past, which is the one that has gone quietest.
func (s *Sampler) evictQuietestLocked() {
	var quietest string
	var quietestAt time.Time
	first := true

	for reason, st := range s.states {
		if first || st.nextAt.Before(quietestAt) {
			quietest, quietestAt, first = reason, st.nextAt, false
		}
	}
	delete(s.states, quietest)
}

// Tracked returns how many distinct reasons the sampler is holding.
func (s *Sampler) Tracked() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.states)
}

// Reasons returns the tracked reasons in sorted order. Diagnostics only.
func (s *Sampler) Reasons() []string {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]string, 0, len(s.states))
	for reason := range s.states {
		out = append(out, reason)
	}
	sort.Strings(out)
	return out
}
