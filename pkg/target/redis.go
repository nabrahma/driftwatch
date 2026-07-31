package target

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/projection"
)

func init() { Register("redis", newRedis) }

// Defaults for the redis target.
const (
	defaultBatchSize = 500
	// defaultScanCount is the COUNT hint. Redis treats it as a per-call effort
	// budget rather than a promise, so a batch may come back short or empty and
	// still not be the end of the iteration.
	defaultScanCount = 1000
	// maxScanCalls bounds a single iteration. A keyspace of 10M keys at a
	// COUNT of 1000 needs roughly 10,000 calls, so this leaves two orders of
	// magnitude of headroom while still terminating rather than spinning if the
	// cursor contract is broken.
	maxScanCalls = 1_000_000
	// maxDedupKeys bounds the per-scan duplicate filter. See the note on
	// redisIterator.seen: remembering every key of a ten-million-key keyspace
	// would be exactly the unbounded collection §19.2 forbids.
	maxDedupKeys = 100_000
)

// RedisTarget reads an audited Redis, and only reads it.
//
// Read-only is enforced here by a command hook that refuses anything outside
// the allowlist before it reaches the socket. That is belt and braces alongside
// RecordingTarget: the wrapper catches it in tests, the hook catches it in
// production, and neither depends on the other being correct.
type RedisTarget struct {
	client redis.UniversalClient
	batch  int
	count  int64

	mu        sync.RWMutex
	observers []func(string)
	closed    bool
}

// RedisOptions configures a RedisTarget.
type RedisOptions struct {
	// Addrs is one address for standalone, several for cluster, or the
	// sentinel addresses when MasterName is set.
	Addrs []string
	// MasterName selects Sentinel mode.
	MasterName string
	// DB is the database index. Ignored in cluster mode.
	DB       int
	Username string
	Password string
	// BatchSize is how many keys one pipeline carries. Default 500.
	BatchSize int
	// ScanCount is the COUNT hint passed to SCAN. Default 1000.
	ScanCount int
	// DialTimeout, ReadTimeout bound the connection.
	DialTimeout time.Duration
	ReadTimeout time.Duration
}

// NewRedis builds a target from options.
//
//nolint:gocritic // hugeParam: an options struct passed by value is the idiom, and this runs once
func NewRedis(opts RedisOptions) (*RedisTarget, error) {
	if len(opts.Addrs) == 0 {
		return nil, fmt.Errorf("%w: redis needs at least one address", ErrBadConfig)
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = defaultBatchSize
	}
	if opts.ScanCount <= 0 {
		opts.ScanCount = defaultScanCount
	}

	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:       opts.Addrs,
		MasterName:  opts.MasterName,
		DB:          opts.DB,
		Username:    opts.Username,
		Password:    opts.Password,
		DialTimeout: opts.DialTimeout,
		ReadTimeout: opts.ReadTimeout,
	})

	t := &RedisTarget{
		client: client,
		batch:  opts.BatchSize,
		count:  int64(opts.ScanCount),
	}
	client.AddHook(&readOnlyHook{target: t})
	return t, nil
}

// NewRedisFromClient wraps an existing client, installing the read-only hook.
// Used by tests that need to point at miniredis.
func NewRedisFromClient(client redis.UniversalClient, batch, scanCount int) *RedisTarget {
	if batch <= 0 {
		batch = defaultBatchSize
	}
	if scanCount <= 0 {
		scanCount = defaultScanCount
	}

	t := &RedisTarget{client: client, batch: batch, count: int64(scanCount)}
	client.AddHook(&readOnlyHook{target: t})
	return t
}

func newRedis(cfg Config) (Target, error) {
	opts := RedisOptions{
		MasterName: cfg.Setting("masterName", ""),
		Username:   cfg.Setting("username", ""),
		Password:   cfg.Setting("password", ""),
	}
	if addrs := cfg.Setting("addrs", ""); addrs != "" {
		for _, a := range strings.Split(addrs, ",") {
			if trimmed := strings.TrimSpace(a); trimmed != "" {
				opts.Addrs = append(opts.Addrs, trimmed)
			}
		}
	}

	var err error
	if opts.DB, err = intSetting(cfg, "db", 0); err != nil {
		return nil, err
	}
	if opts.BatchSize, err = intSetting(cfg, "batchSize", defaultBatchSize); err != nil {
		return nil, err
	}
	if opts.ScanCount, err = intSetting(cfg, "scanCount", defaultScanCount); err != nil {
		return nil, err
	}
	return NewRedis(opts)
}

func intSetting(cfg Config, key string, def int) (int, error) {
	raw := cfg.Setting(key, "")
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%w: %s must be a non-negative integer, got %q", ErrBadConfig, key, raw)
	}
	return n, nil
}

// Name returns the registry name.
func (t *RedisTarget) Name() string { return "redis" }

// ObserveCommands installs a command observer.
func (t *RedisTarget) ObserveCommands(fn func(string)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.observers = append(t.observers, fn)
}

func (t *RedisTarget) emit(name string) {
	t.mu.RLock()
	observers := make([]func(string), len(t.observers))
	copy(observers, t.observers)
	t.mu.RUnlock()

	for _, fn := range observers {
		fn(name)
	}
}

// readOnlyHook refuses any command outside the allowlist before it is sent.
//
// This is the production half of the read-only guarantee. RecordingTarget
// enforces it in tests; this enforces it against a live store, where there is no
// test to fail and the only useful outcome is that the command does not happen.
type readOnlyHook struct{ target *RedisTarget }

func (h *readOnlyHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *readOnlyHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if err := h.check(cmd); err != nil {
			return err
		}
		return next(ctx, cmd)
	}
}

func (h *readOnlyHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			if err := h.check(cmd); err != nil {
				return err
			}
		}
		return next(ctx, cmds)
	}
}

// check reports the command and refuses it if it is not a read.
func (h *readOnlyHook) check(cmd redis.Cmder) error {
	name := commandName(cmd)
	h.target.emit(name)

	if IsReadOnlyCommand(name) {
		return nil
	}
	return fmt.Errorf("%w: %s (permitted: %v)", ErrReadOnlyViolation, name, ReadOnlyCommands())
}

// commandName renders the command for allowlist matching, including the
// subcommand for container commands such as MEMORY USAGE and CLUSTER SLOTS.
func commandName(cmd redis.Cmder) string {
	args := cmd.Args()
	if len(args) == 0 {
		return strings.ToUpper(cmd.Name())
	}

	name := strings.ToUpper(fmt.Sprint(args[0]))
	switch name {
	case "MEMORY", "CLUSTER", "OBJECT", "CLIENT", "CONFIG", "ACL":
		if len(args) > 1 {
			return name + " " + strings.ToUpper(fmt.Sprint(args[1]))
		}
	}
	return name
}

// Get reads one key.
func (t *RedisTarget) Get(ctx context.Context, key string, shape projection.Shape) (event.Value, error) {
	if err := t.checkOpen(); err != nil {
		return event.Value{}, err
	}

	switch shape {
	case projection.ShapeSet:
		members, err := t.client.SMembers(ctx, key).Result()
		if err != nil {
			return event.Value{}, wrapRedisErr(key, shape, err)
		}
		return setValue(members), nil

	case projection.ShapeScalar:
		s, err := t.client.Get(ctx, key).Result()
		if errors.Is(err, redis.Nil) {
			return event.Value{}, nil
		}
		if err != nil {
			return event.Value{}, wrapRedisErr(key, shape, err)
		}
		return event.Value{Kind: event.ValueScalar, Scalar: []byte(s)}, nil

	case projection.ShapeCounter:
		s, err := t.client.Get(ctx, key).Result()
		if errors.Is(err, redis.Nil) {
			return event.Value{}, nil
		}
		if err != nil {
			return event.Value{}, wrapRedisErr(key, shape, err)
		}
		n, convErr := strconv.ParseInt(s, 10, 64)
		if convErr != nil {
			return event.Value{}, &WrongTypeError{Key: key, Want: shape, Got: "non-integer string"}
		}
		return event.Value{Kind: event.ValueCounter, Counter: n}, nil

	default:
		return event.Value{}, &WrongTypeError{Key: key, Want: shape, Got: "unknown shape"}
	}
}

// setValue converts SMEMBERS output.
//
// An empty reply means the key does not exist: Redis deletes a set key when its
// last member is removed, so there is no such thing as an empty set to read
// back. This is the same decision as event.Value.Equal (M2), and it has to be
// made in both places or the differ compares an absent oracle value against a
// present-but-empty target value and reports drift on every key that emptied.
func setValue(members []string) event.Value {
	if len(members) == 0 {
		return event.Value{}
	}
	set := make(map[string]struct{}, len(members))
	for _, m := range members {
		set[m] = struct{}{}
	}
	return event.Value{Kind: event.ValueSet, Members: set}
}

// wrapRedisErr turns a WRONGTYPE reply into a typed error and passes everything
// else through.
//
// WRONGTYPE is not a failure to read; it is a successful read of something
// unexpected, which means somebody wrote a different shape into the index. That
// is drift, and swallowing it would hide the most informative finding the sweep
// could produce.
func wrapRedisErr(key string, shape projection.Shape, err error) error {
	if err == nil {
		return nil
	}
	if isWrongType(err) {
		return &WrongTypeError{Key: key, Want: shape, Got: "a different type"}
	}
	return err
}

func isWrongType(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "WRONGTYPE")
}

// GetMany reads a batch, mapping unreadable keys to absent.
func (t *RedisTarget) GetMany(ctx context.Context, keys []string, shape projection.Shape) ([]event.Value, error) {
	reads, err := t.ReadMany(ctx, keys, shape)
	if err != nil {
		return nil, err
	}
	return valuesFromReads(reads), nil
}

// ReadMany reads a batch through a pipeline, preserving per-key outcomes.
//
// A transport failure discards the whole batch rather than returning what
// arrived. Partial results read as fact downstream, and half a batch of
// "absent" would be indistinguishable from half a keyspace having vanished.
func (t *RedisTarget) ReadMany(ctx context.Context, keys []string, shape projection.Shape) ([]Read, error) {
	if err := t.checkOpen(); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}

	out := make([]Read, 0, len(keys))
	for start := 0; start < len(keys); start += t.batch {
		end := min(start+t.batch, len(keys))

		chunk, err := t.readChunk(ctx, keys[start:end], shape)
		if err != nil {
			return nil, err
		}
		out = append(out, chunk...)
	}
	return out, nil
}

func (t *RedisTarget) readChunk(ctx context.Context, keys []string, shape projection.Shape) ([]Read, error) {
	pipe := t.client.Pipeline()

	setCmds := make([]*redis.StringSliceCmd, 0, len(keys))
	strCmds := make([]*redis.StringCmd, 0, len(keys))
	for _, key := range keys {
		if shape == projection.ShapeSet {
			setCmds = append(setCmds, pipe.SMembers(ctx, key))
			continue
		}
		strCmds = append(strCmds, pipe.Get(ctx, key))
	}

	// Exec reports the first command error, which for a batch containing a
	// missing key is redis.Nil and for one containing a wrong-typed key is
	// WRONGTYPE. Neither is a transport failure, so both are resolved per
	// command below and only a genuine failure aborts the batch.
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) && !isWrongType(err) {
		return nil, err
	}

	out := make([]Read, len(keys))
	for i, key := range keys {
		if shape == projection.ShapeSet {
			members, err := setCmds[i].Result()
			if err != nil && !errors.Is(err, redis.Nil) {
				out[i] = Read{Err: wrapRedisErr(key, shape, err)}
				continue
			}
			out[i] = Read{Value: setValue(members)}
			continue
		}

		s, err := strCmds[i].Result()
		switch {
		case errors.Is(err, redis.Nil):
			out[i] = Read{}
		case err != nil:
			out[i] = Read{Err: wrapRedisErr(key, shape, err)}
		case shape == projection.ShapeCounter:
			n, convErr := strconv.ParseInt(s, 10, 64)
			if convErr != nil {
				out[i] = Read{Err: &WrongTypeError{Key: key, Want: shape, Got: "non-integer string"}}
				continue
			}
			out[i] = Read{Value: event.Value{Kind: event.ValueCounter, Counter: n}}
		default:
			out[i] = Read{Value: event.Value{Kind: event.ValueScalar, Scalar: []byte(s)}}
		}
	}
	return out, nil
}

// TTL returns the remaining lifetime of a key.
func (t *RedisTarget) TTL(ctx context.Context, key string) (*time.Duration, error) {
	if err := t.checkOpen(); err != nil {
		return nil, err
	}

	d, err := t.client.PTTL(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	// Redis encodes both "no such key" and "no expiry" as negative durations,
	// and they mean very different things to the differ: one is a key that is
	// gone, the other is a key that will never go.
	switch {
	case d == -2*time.Millisecond || d == -2:
		return nil, ErrNotFound
	case d < 0:
		return nil, nil
	default:
		return &d, nil
	}
}

// Scan iterates the keyspace matching pattern.
func (t *RedisTarget) Scan(ctx context.Context, pattern string, batch int) Iterator {
	if err := t.checkOpen(); err != nil {
		return &sliceIterator{err: err}
	}
	if batch <= 0 {
		batch = int(t.count)
	}
	if pattern == "" {
		pattern = "*"
	}

	// Cluster scanning has to visit every master, because a cursor is
	// per-node and one node's iteration says nothing about another's.
	if cluster, ok := t.client.(*redis.ClusterClient); ok {
		return newClusterIterator(ctx, cluster, pattern, int64(batch))
	}
	return &redisIterator{
		client:  t.client,
		pattern: pattern,
		count:   int64(batch),
		seen:    map[string]struct{}{},
		cursors: map[uint64]int{},
	}
}

// redisIterator walks one node's keyspace with SCAN.
//
// SCAN guarantees only that keys present for the whole iteration are returned
// at least once. Keys may be returned more than once, and keys added or removed
// mid-scan may or may not appear at all. That is precisely why §5.5 treats
// extras conservatively: a key seen in a scan is not evidence that it exists
// now, only that it existed at some point during the scan.
//
// It is worth being clear about what this iterator can and cannot detect. A
// keyspace destroyed mid-iteration does not hang the scan — that was measured
// against Redis 6.2 and 7.2 and does not reproduce (docs/DISCOVERIES.md D-006).
// It makes the scan finish early and report success, having seen a fraction of
// the keys, with a zero cursor and no error. Nothing here can tell that apart
// from a small keyspace, and nothing here tries to; the sweeper's conservative
// extras rules are what keep the consequence to under-reporting rather than a
// false mass-drift finding.
type redisIterator struct {
	client  redis.UniversalClient
	pattern string
	count   int64

	cursor  uint64
	started bool
	current []string
	err     error

	// seen deduplicates within one iteration, because SCAN may return the same
	// key from more than one call.
	//
	// It is a bounded window rather than a complete record, and that is a
	// deliberate trade. Remembering every key of a ten-million-key keyspace
	// costs gigabytes — measured at roughly 370 MB per million keys — which is
	// the unbounded collection §19.2 forbids, and §9 M8 says in the same breath
	// that a ten-million-key scan "must not accumulate all keys in memory".
	// Both cannot be satisfied exactly.
	//
	// The window is the right side to give: SCAN returns a key twice because of
	// a rehash, and the repeats appear close together, so a window catches
	// essentially all of them. What it cannot catch is harmless anyway —
	// consumers must be idempotent per key regardless, because a duplicate is
	// compared against the same oracle entry and produces the same finding.
	seen map[string]struct{}
	// dedupWindows counts how many times the window has been reset, so a caller
	// can tell whether deduplication was complete.
	dedupWindows int
	// cursors counts how often each cursor has been handed back, so an
	// iteration that is going round rather than forward is detected instead of
	// followed forever.
	cursors map[uint64]int
	calls   int
}

// maxCursorRepeats is how many times the same cursor may recur before the
// iteration is treated as looping.
//
// It is three rather than one deliberately. A repeat was never observed on
// Redis 6.2 or 7.2 across ten thousand keys with the keyspace being destroyed
// and rebuilt underneath the scan (docs/DISCOVERIES.md D-006), so this guards
// against a Redis-compatible server that implements the cursor differently —
// Valkey, KeyDB, Dragonfly, a managed emulation — rather than against Redis
// itself. Aborting on a single repeat would risk failing a legitimate scan to
// defend against something that has not been seen to happen.
const maxCursorRepeats = 3

func (it *redisIterator) Next(ctx context.Context) bool {
	it.current = nil

	for {
		if it.err != nil {
			return false
		}
		if err := ctx.Err(); err != nil {
			it.err = err
			return false
		}
		if it.started && it.cursor == 0 {
			return false
		}

		it.calls++
		if it.calls > maxScanCalls {
			it.err = fmt.Errorf("%w: gave up after %d calls", ErrScanRestarted, maxScanCalls)
			return false
		}

		keys, next, err := it.client.Scan(ctx, it.cursor, it.pattern, it.count).Result()
		if err != nil {
			it.err = err
			return false
		}

		// A cursor handed back repeatedly means the iteration is going round
		// rather than forward. Continuing would either spin or silently rescan,
		// and rescanning would double-count extras.
		if next != 0 {
			it.cursors[next]++
			if it.cursors[next] > maxCursorRepeats {
				it.err = fmt.Errorf("%w: cursor %d returned %d times in %d calls",
					ErrScanRestarted, next, it.cursors[next], it.calls)
				return false
			}
		}

		it.cursor = next
		it.started = true

		for _, key := range keys {
			if _, dup := it.seen[key]; dup {
				continue
			}
			if len(it.seen) >= maxDedupKeys {
				// Start a fresh window rather than growing without bound. Any
				// duplicate separated by more than a window's worth of keys
				// gets through, which the consumer absorbs idempotently.
				it.seen = make(map[string]struct{}, maxDedupKeys)
				it.dedupWindows++
			}
			it.seen[key] = struct{}{}
			it.current = append(it.current, key)
		}

		// An empty batch is not the end. Redis treats COUNT as an effort
		// budget, so a call can legitimately return nothing while the cursor is
		// still non-zero; stopping here would silently truncate the scan.
		if len(it.current) > 0 {
			return true
		}
		if it.cursor == 0 {
			return false
		}
	}
}

func (it *redisIterator) Keys() []string { return it.current }
func (it *redisIterator) Err() error     { return it.err }

func (it *redisIterator) Close() error {
	// Release the dedup set: it is the largest thing the scan holds, and a
	// caller that abandons an iterator should not keep paying for it.
	it.seen = nil
	it.cursors = nil
	return nil
}

// DedupWindows reports how many times the duplicate filter was reset. A
// non-zero value means deduplication was windowed rather than complete, so the
// scan may have yielded a key more than once.
func (it *redisIterator) DedupWindows() int { return it.dedupWindows }

// clusterIterator concatenates a per-master scan across the cluster.
type clusterIterator struct {
	iters []Iterator
	idx   int
	err   error
}

func newClusterIterator(ctx context.Context, cluster *redis.ClusterClient, pattern string, count int64) Iterator {
	out := &clusterIterator{}

	err := cluster.ForEachMaster(ctx, func(_ context.Context, node *redis.Client) error {
		out.iters = append(out.iters, &redisIterator{
			client:  node,
			pattern: pattern,
			count:   count,
			seen:    map[string]struct{}{},
			cursors: map[uint64]int{},
		})
		return nil
	})
	if err != nil {
		out.err = err
	}
	return out
}

func (it *clusterIterator) Next(ctx context.Context) bool {
	for it.err == nil && it.idx < len(it.iters) {
		if it.iters[it.idx].Next(ctx) {
			return true
		}
		if err := it.iters[it.idx].Err(); err != nil {
			it.err = err
			return false
		}
		it.idx++
	}
	return false
}

func (it *clusterIterator) Keys() []string {
	if it.idx < len(it.iters) {
		return it.iters[it.idx].Keys()
	}
	return nil
}

func (it *clusterIterator) Err() error { return it.err }

func (it *clusterIterator) Close() error {
	var first error
	for _, inner := range it.iters {
		if err := inner.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Health parses INFO and DBSIZE.
func (t *RedisTarget) Health(ctx context.Context) (Health, error) {
	if err := t.checkOpen(); err != nil {
		return Health{}, err
	}

	h := Health{}

	// INFO is called with no section argument on purpose. Redis 7 accepts
	// several sections in one call and Redis 6 accepts at most one, so
	// "INFO stats memory replication server" works against 7 and fails against
	// 6 with a wrong-number-of-arguments error. The bare form returns every
	// default section on both, which is a superset of what Health needs.
	// See docs/DISCOVERIES.md D-005.
	raw, err := t.client.Info(ctx).Result()
	if err != nil {
		return Health{Reachable: false}, err
	}
	h.Reachable = true
	parseInfo(raw, &h)

	if size, err := t.client.DBSize(ctx).Result(); err == nil {
		h.KeyspaceSize = size
	}
	return h, nil
}

// parseInfo reads the fields Health needs out of an INFO reply.
//
// It is field-by-field rather than section-by-section on purpose. Redis moves
// fields between sections and adds new ones between versions, and a parser that
// depends on section boundaries or field order breaks on an upgrade — quietly,
// because a missing field reads as a zero and a zero eviction count looks
// exactly like a healthy store.
func parseInfo(raw string, h *Health) {
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		field, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		field, value = strings.TrimSpace(field), strings.TrimSpace(value)

		switch field {
		case "evicted_keys":
			h.EvictedKeys = parseUint(value)
		case "expired_keys":
			h.ExpiredKeys = parseUint(value)
		case "used_memory":
			h.UsedMemoryBytes = parseUint(value)
		case "maxmemory":
			h.MaxMemoryBytes = parseUint(value)
		case "role":
			h.Role = value
		case "redis_version":
			h.Version = value
		case "master_repl_offset", "slave_repl_offset":
			// Not a lag in itself; kept only so a replica reports something
			// rather than nothing until real lag tracking lands.
		case "master_last_io_seconds_ago":
			if n, err := strconv.ParseInt(value, 10, 64); err == nil && n >= 0 {
				h.ReplicationLagMs = n * 1000
			}
		}
	}
}

func parseUint(s string) uint64 {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// Close releases the client. Idempotent.
func (t *RedisTarget) Close() error {
	t.mu.Lock()
	already := t.closed
	t.closed = true
	t.mu.Unlock()

	if already {
		return nil
	}
	err := t.client.Close()
	if err != nil && errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func (t *RedisTarget) checkOpen() error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.closed {
		return ErrClosed
	}
	return nil
}

var (
	_ Target    = (*RedisTarget)(nil)
	_ Commander = (*RedisTarget)(nil)
)
