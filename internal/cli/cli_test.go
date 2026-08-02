package cli_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/nabrahma/driftwatch/internal/cli"
	"github.com/nabrahma/driftwatch/pkg/clock"
)

func TestMain(m *testing.M) {
	// Every entry is a third-party goroutine started at package init and never
	// stopped. No ignore here is ever for one of driftwatch's own goroutines —
	// one of those is a bug to fix, not an entry to add.
	goleak.VerifyTestMain(m,
		goleak.IgnoreAnyFunction("github.com/redis/go-redis/v9/internal/pool.startGlobalTimeCache.func1"),
	)
}

var update = flag.Bool("update", false, "rewrite the golden files in testdata/")

// inProcessSpec runs the whole pipeline with nothing outside the process, so
// every CLI test is hermetic and every golden file is reproducible.
const inProcessSpec = `
name: kvcache-index
namespace: inference
source:
  type: memory
codec:
  type: json
projection:
  type: scalar
  keyTemplate: "block:{{.Key}}"
target:
  type: memory
policy:
  settlementWindow: {mode: static, static: 10ms, min: 10ms, max: 60s}
  sweepInterval: 10s
  bootstrap: Wait
`

// result is one CLI invocation's outcome, with the two streams kept apart.
type result struct {
	code   int
	stdout string
	stderr string
}

// run executes the CLI with the real clock, which is what commands that sleep
// on their own schedule need.
func run(t *testing.T, args ...string) result {
	t.Helper()
	return runWithClock(t, clock.Real(), args...)
}

func runWithClock(t *testing.T, clk clock.Clock, args ...string) result {
	t.Helper()

	var stdout, stderr bytes.Buffer
	code := cli.Execute(&cli.Env{
		Stdout:  &stdout,
		Stderr:  &stderr,
		Args:    args,
		Clock:   clk,
		Version: "v0.5.0",
		Commit:  "abc1234",
		Date:    "2026-01-01T12:00:00Z",
	})

	return result{code: code, stdout: stdout.String(), stderr: stderr.String()}
}

// writeSpec writes a spec to a temporary file and returns its path.
func writeSpec(t *testing.T, body string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "check.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

// ---------------------------------------------------------------------------
// Exit codes (§11).
// ---------------------------------------------------------------------------

func TestExitCodes(t *testing.T) {
	// The codes are a contract: CI pipelines branch on them, so one test per
	// code rather than one test that happens to cover several.
	spec := writeSpec(t, inProcessSpec)

	tests := []struct {
		name     string
		args     []string
		wantCode int
		wantErr  string
	}{
		{
			name:     "a clean run exits 0",
			args:     []string{"version"},
			wantCode: cli.ExitOK,
		},
		{
			name:     "a missing spec exits 2",
			args:     []string{"diff"},
			wantCode: cli.ExitConfigInvalid,
			wantErr:  "--file is required",
		},
		{
			name:     "an unreadable spec exits 2",
			args:     []string{"diff", "-f", "no-such-file.yaml"},
			wantCode: cli.ExitConfigInvalid,
		},
		{
			name:     "an invalid spec exits 2",
			args:     []string{"diff", "-f", writeSpec(t, "source: {type: kafka}\n")},
			wantCode: cli.ExitConfigInvalid,
			wantErr:  "source.type",
		},
		{
			name:     "an unknown output format exits 2",
			args:     []string{"diff", "-f", spec, "-o", "yaml"},
			wantCode: cli.ExitConfigInvalid,
			wantErr:  "--output",
		},
		{
			name:     "an unknown log level exits 2",
			args:     []string{"version", "--log-level", "chatty"},
			wantCode: cli.ExitConfigInvalid,
		},
		{
			name:     "an unknown subcommand exits 1",
			args:     []string{"frobnicate"},
			wantCode: cli.ExitFatal,
		},
		{
			name:     "explain without a key exits 2",
			args:     []string{"explain", "-f", spec},
			wantCode: cli.ExitConfigInvalid,
			wantErr:  "--key",
		},
		{
			name:     "replay without events exits 2",
			args:     []string{"replay", "-f", spec},
			wantCode: cli.ExitConfigInvalid,
			wantErr:  "--events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := run(t, tc.args...)

			assert.Equal(t, tc.wantCode, got.code, "stderr: %s", got.stderr)
			if tc.wantErr != "" {
				assert.Contains(t, got.stderr, tc.wantErr)
			}
			if tc.wantCode != cli.ExitOK {
				assert.NotEmpty(t, got.stderr, "a failure must say why, on stderr")
			}
		})
	}
}

func TestDiff_ExitsThreeWhenDriftIsConfirmed(t *testing.T) {
	// Exit 3 is what makes `driftwatch diff` usable as a CI gate: it means the
	// cache index is wrong, which a pipeline has to tell apart from driftwatch
	// itself having failed.
	spec := specWithCapture(t, inProcessSpec)

	got := run(t, "diff", "-f", spec, "--warmup", "50ms")

	assert.Equal(t, cli.ExitDriftFound, got.code, "stdout:\n%s\nstderr:\n%s", got.stdout, got.stderr)
	assert.Contains(t, got.stderr, "divergent keys confirmed")
	assert.Contains(t, got.stdout, "missing_in_target",
		"the report itself still goes to stdout, so the pipeline can keep it")
	assert.Equal(t, 1, strings.Count(got.stdout, "driftwatch sweep report"))
}

// specWithCapture writes a spec whose source is a capture the store never
// received, which is the simplest real divergence: the oracle expects two keys
// and the store holds neither.
func specWithCapture(t *testing.T, base string) string {
	t.Helper()

	dir := t.TempDir()
	events := writeCapture(t, dir)

	path := filepath.Join(dir, "check.yaml")
	require.NoError(t, os.WriteFile(path, []byte(withFileSource(base, events)), 0o600))
	return path
}

// withFileSource replaces a spec's memory source with a capture.
//
// The path is quoted because a Windows path contains a colon, and an unquoted
// colon inside a YAML flow mapping parses as a nested key rather than as part
// of the path.
func withFileSource(base, events string) string {
	return strings.Replace(base,
		"source:\n  type: memory",
		`source:
  type: file
  file: {path: "`+filepath.ToSlash(events)+`"}`, 1)
}

// ---------------------------------------------------------------------------
// Stream separation (§11).
// ---------------------------------------------------------------------------

func TestOutputJSON_IsOneDocumentEvenAtDebugLogLevel(t *testing.T) {
	// The requirement §11 states outright, and the one most easily broken by a
	// stray Println. Logs go to stderr and data to stdout, always, so
	// `driftwatch diff -o json | jq` works whatever the log level is.
	spec := writeSpec(t, inProcessSpec)

	got := run(t, "diff", "-f", spec, "-o", "json",
		"--log-level", "debug", "--log-format", "json", "-v", "2", "--warmup", "50ms")

	require.Equal(t, cli.ExitOK, got.code, "stderr: %s", got.stderr)
	require.NotEmpty(t, got.stderr, "debug logging should have produced lines to separate")

	var doc map[string]any
	require.NoError(t, json.Unmarshal([]byte(got.stdout), &doc),
		"stdout must be one well-formed JSON document:\n%s", got.stdout)

	assert.Contains(t, doc, "keysCompared")
	assert.Contains(t, doc, "findings")
	assert.NotContains(t, got.stdout, `"level"`, "no log line leaked into the document")
}

func TestOutputJSON_StaysOneDocumentForEveryCommand(t *testing.T) {
	spec := writeSpec(t, inProcessSpec)

	tests := []struct {
		name string
		args []string
	}{
		{"version", []string{"version", "-o", "json"}},
		{"diff", []string{"diff", "-f", spec, "-o", "json", "--warmup", "50ms"}},
		{"explain", []string{
			"explain", "-f", spec, "-o", "json",
			"--key", "block:9f3a", "--wait", "10ms",
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args := make([]string, 0, len(tc.args)+2)
			args = append(args, tc.args...)
			args = append(args, "--log-level", "debug")

			got := run(t, args...)

			require.Equal(t, cli.ExitOK, got.code, "stderr: %s", got.stderr)

			var doc map[string]any
			require.NoError(t, json.Unmarshal([]byte(got.stdout), &doc),
				"stdout was not a single JSON document:\n%s", got.stdout)
		})
	}
}

func TestErrors_NeverGoToStdout(t *testing.T) {
	// A pipeline that redirects stdout to a file must get data or an empty
	// file, never an error message that a later step tries to parse.
	got := run(t, "diff", "-f", "no-such-file.yaml", "-o", "json")

	assert.Equal(t, cli.ExitConfigInvalid, got.code)
	assert.Empty(t, got.stdout)
	assert.Contains(t, got.stderr, "driftwatch:")
}

// ---------------------------------------------------------------------------
// Golden files.
// ---------------------------------------------------------------------------

func TestGoldenOutput(t *testing.T) {
	spec := writeSpec(t, inProcessSpec)

	tests := []struct {
		name string
		args []string
	}{
		{"version-text", []string{"version"}},
		{"version-json", []string{"version", "-o", "json"}},
		{"diff-clean", []string{"diff", "-f", spec, "--warmup", "50ms"}},
		{"help-root", []string{"--help"}},
		{"help-explain", []string{"explain", "--help"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := run(t, tc.args...)
			require.Equal(t, cli.ExitOK, got.code, "stderr: %s", got.stderr)

			body := normalize(got.stdout)
			path := filepath.Join("testdata", tc.name+".golden")

			if *update {
				require.NoError(t, os.MkdirAll("testdata", 0o750))
				require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
				return
			}

			want, err := os.ReadFile(path)
			require.NoError(t, err, "run: go test ./internal/cli/ -update")
			assert.Equal(t, strings.ReplaceAll(string(want), "\r\n", "\n"), body)
		})
	}
}

// normalize removes the parts of the output that legitimately differ between
// machines, so a golden file compares the shape rather than the environment.
// timestampRE matches an RFC 3339 instant anywhere in a line.
//
// Needed for the per-finding "last event <ts>" line. The capture files in these
// tests carry no `ts` field, so each event's timestamp is driftwatch's own
// receive time — correct behavior, and a clock reading rather than a property
// of the replay, exactly like the report's `started` line.
//
// Without this, two replays that reach identical conclusions differ whenever
// the second one happens to start in the next wall-clock second, and a test
// named IsDeterministic fails for being run at the wrong moment.
var timestampRE = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z`)

func normalize(s string) string {
	lines := strings.Split(strings.ReplaceAll(s, "\r\n", "\n"), "\n")

	for i, line := range lines {
		switch {
		case strings.HasPrefix(line, "started "):
			lines[i] = "started    <timestamp>"
		case strings.Contains(line, "last event "):
			lines[i] = timestampRE.ReplaceAllString(line, "<timestamp>")
		case strings.HasPrefix(line, "duration "):
			lines[i] = "duration   <duration>"
		case strings.HasPrefix(line, "  go  "):
			lines[i] = "  go         <goversion>"
		case strings.HasPrefix(line, "  platform "):
			lines[i] = "  platform   <platform>"
		case strings.Contains(line, `"goVersion"`):
			lines[i] = `  "goVersion": "<goversion>",`
		case strings.Contains(line, `"platform"`):
			lines[i] = `  "platform": "<platform>"`
		}
	}
	return strings.Join(lines, "\n")
}

func TestHelp_EveryCommandCarriesARunnableExample(t *testing.T) {
	// §11's requirement, and the difference between a tool someone tries and
	// one they close. A flag list does not tell a reader what to type.
	for _, name := range []string{"watch", "diff", "explain", "replay", "version"} {
		t.Run(name, func(t *testing.T) {
			got := run(t, name, "--help")

			require.Equal(t, cli.ExitOK, got.code)
			assert.Contains(t, got.stdout, "Examples:")
			assert.Contains(t, got.stdout, "driftwatch "+name,
				"the example should be a line the reader can paste")
		})
	}
}

func TestVersion_ReportsTheInjectedBuild(t *testing.T) {
	got := run(t, "version", "-o", "json")

	var info struct {
		Version   string `json:"version"`
		Commit    string `json:"commit"`
		GoVersion string `json:"goVersion"`
	}
	require.NoError(t, json.Unmarshal([]byte(got.stdout), &info))

	assert.Equal(t, "v0.5.0", info.Version)
	assert.Equal(t, "abc1234", info.Commit)
	assert.NotEmpty(t, info.GoVersion)
}

// ---------------------------------------------------------------------------
// Replay (§11): hermetic and deterministic.
// ---------------------------------------------------------------------------

func TestReplay_IsDeterministic(t *testing.T) {
	// The backbone of turning a production incident into a regression test:
	// the same capture has to converge to the same oracle every run, whatever
	// the machine was doing at the time.
	dir := t.TempDir()
	events := writeCapture(t, dir)
	spec := writeSpec(t, inProcessSpec)

	first := filepath.Join(dir, "first.json")
	second := filepath.Join(dir, "second.json")

	// Exit 3, not 0: the capture's keys were never written to the empty store,
	// so the replay correctly finds them missing. What this test is about is
	// that it finds the same thing twice.
	got := run(t, "replay", "-f", spec, "--events", events, "--dump-oracle", first,
		"--settle-for", "50ms")
	require.Equal(t, cli.ExitDriftFound, got.code, "stderr: %s", got.stderr)

	again := run(t, "replay", "-f", spec, "--events", events, "--dump-oracle", second,
		"--settle-for", "50ms")
	require.Equal(t, cli.ExitDriftFound, again.code, "stderr: %s", again.stderr)

	// Normalized, because the report states when the sweep ran and that is a
	// clock reading rather than a property of the replay. Everything else — the
	// keys compared, the findings, their categories and their versions — has to
	// be identical, and that is what determinism means here.
	assert.Equal(t, normalize(got.stdout), normalize(again.stdout),
		"two replays of one capture must reach the same conclusion")
	assert.Equal(t, oracleKeys(t, first), oracleKeys(t, second),
		"the oracle state must be identical, key for key and version for version")
}

func TestReplay_OverridesTheSpecsSourceSoItCannotReachTheNetwork(t *testing.T) {
	// A spec pointing at a production endpoint must not connect during what the
	// operator believes is an offline reproduction.
	dir := t.TempDir()
	events := writeCapture(t, dir)

	spec := writeSpec(t, strings.Replace(inProcessSpec,
		"source:\n  type: memory",
		"source:\n  type: zmq\n  zmq: {endpoints: [\"tcp://198.51.100.1:5557\"], recvHWM: 1000}", 1))

	got := run(t, "replay", "-f", spec, "--events", events, "--settle-for", "50ms")

	// It compared something, which it could only do by reading the capture: the
	// endpoint in the spec is in a reserved documentation range and would have
	// hung rather than connected.
	assert.Equal(t, cli.ExitDriftFound, got.code, "stderr: %s", got.stderr)
	assert.Contains(t, got.stdout, "compared   2 keys")
}

func TestReplay_SeedsTheStoreFromASnapshot(t *testing.T) {
	dir := t.TempDir()
	events := writeCapture(t, dir)

	snapshot := filepath.Join(dir, "store.json")
	require.NoError(t, os.WriteFile(snapshot,
		[]byte(`{"scalars":{"block:9f3a":"v2","block:orphan":"x"}}`), 0o600))

	spec := writeSpec(t, inProcessSpec)
	got := run(t, "replay", "-f", spec, "--events", events,
		"--target-snapshot", snapshot, "--settle-for", "50ms")

	// The capture sets block:9f3a to v2, which the snapshot already holds, so
	// that key agrees and block:8e2b — which the snapshot does not have — is the
	// only finding. Without the snapshot both would be missing, so the absence
	// of block:9f3a from the report is what proves the snapshot was loaded.
	assert.Contains(t, got.stdout, "block:8e2b")
	assert.NotContains(t, got.stdout, "block:9f3a",
		"the snapshot supplied this key, so it should agree")
}

func writeCapture(t *testing.T, dir string) string {
	t.Helper()

	path := filepath.Join(dir, "events.jsonl")
	lines := []string{
		`{"publisher":"replica-0","epoch":1,"seq":1,"op":"set","key":"9f3a","value":"v1"}`,
		`{"publisher":"replica-0","epoch":1,"seq":2,"op":"set","key":"9f3a","value":"v2"}`,
		`{"publisher":"replica-1","epoch":1,"seq":1,"op":"set","key":"8e2b","value":"v1"}`,
	}
	require.NoError(t, os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600))
	return path
}

func oracleKeys(t *testing.T, path string) string {
	t.Helper()

	raw, err := os.ReadFile(path) //nolint:gosec // a path this test just wrote
	require.NoError(t, err)

	var dump struct {
		Keys []struct {
			Key     string `json:"key"`
			Value   string `json:"value"`
			Version uint64 `json:"version"`
		} `json:"keys"`
	}
	require.NoError(t, json.Unmarshal(raw, &dump))

	var b strings.Builder
	for _, k := range dump.Keys {
		b.WriteString(k.Key + "=" + k.Value + "@" + itoa(int(k.Version)) + "\n")
	}
	return b.String()
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [12]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

// ---------------------------------------------------------------------------
// Explain.
// ---------------------------------------------------------------------------

func TestExplain_AcceptsABinaryKeyAsHex(t *testing.T) {
	spec := writeSpec(t, inProcessSpec)

	got := run(t, "explain", "-f", spec, "--key-hex", "626c6f636b3a00ff", "--wait", "10ms")

	require.Equal(t, cli.ExitOK, got.code, "stderr: %s", got.stderr)
	assert.Contains(t, got.stdout, "hex:626c6f636b3a00ff",
		"a binary key is rendered hex-escaped rather than painted into the terminal")
}

func TestExplain_RejectsBothSpellingsOfTheKeyAtOnce(t *testing.T) {
	spec := writeSpec(t, inProcessSpec)

	got := run(t, "explain", "-f", spec, "--key", "a", "--key-hex", "61")

	assert.Equal(t, cli.ExitConfigInvalid, got.code)
	assert.Contains(t, got.stderr, "only one of")
}

func TestExplain_SaysSoForAKeyItHasNeverSeen(t *testing.T) {
	// Not an error. A key driftwatch has never observed is a fact worth
	// reporting, and usually means the keyTemplate does not produce the shape
	// the user typed — which is exactly what the output helps them work out.
	spec := writeSpec(t, inProcessSpec)

	got := run(t, "explain", "-f", spec, "--key", "block:never-seen", "--wait", "10ms")

	require.Equal(t, cli.ExitOK, got.code, "stderr: %s", got.stderr)
	assert.Contains(t, got.stdout, "UNKNOWN KEY")
	assert.Contains(t, got.stdout, "never observed an event for this key")
}

func TestExplain_RefusesFromRunningRatherThanIgnoringIt(t *testing.T) {
	// Silently ignoring a flag is worse than not having it: the user believes
	// they queried the live process and reads numbers from a second one.
	spec := writeSpec(t, inProcessSpec)

	got := run(t, "explain", "-f", spec, "--key", "a",
		"--from-running", "http://localhost:9090")

	assert.Equal(t, cli.ExitConfigInvalid, got.code)
	assert.Contains(t, got.stderr, "not in this build")
}

// ---------------------------------------------------------------------------
// Watch.
// ---------------------------------------------------------------------------

func TestWatch_OnceRunsASingleSweepAndExits(t *testing.T) {
	spec := writeSpec(t, inProcessSpec)

	got := run(t, "watch", "-f", spec, "--once", "--metrics-addr", "")

	require.Equal(t, cli.ExitOK, got.code, "stderr: %s", got.stderr)
	assert.Contains(t, got.stdout, "no divergence found")
}

func TestWatch_StopsAtTheTimeoutAndPrintsAStatusLine(t *testing.T) {
	spec := writeSpec(t, inProcessSpec)

	got := run(t, "watch", "-f", spec, "--timeout", "50ms",
		"--status-interval", "10ms", "--metrics-addr", "")

	require.Equal(t, cli.ExitOK, got.code, "stderr: %s", got.stderr)
	assert.Contains(t, got.stdout, "keys 0")
}

func TestWatch_WarningsGoToStderrEvenWhenLogsAreSilenced(t *testing.T) {
	// A warning about a non-commutative counter has to be seen once, not found
	// in a log search after the report was already believed.
	spec := writeSpec(t, strings.Replace(inProcessSpec,
		"projection:\n  type: scalar", "projection:\n  type: counter", 1))

	got := run(t, "watch", "-f", spec, "--once",
		"--log-level", "error", "--metrics-addr", "")

	require.Equal(t, cli.ExitOK, got.code, "stderr: %s", got.stderr)
	assert.Contains(t, got.stderr, "ProjectionNotCommutative")
}
