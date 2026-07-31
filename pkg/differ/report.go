package differ

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nabrahma/driftwatch/pkg/oracle"
	"github.com/nabrahma/driftwatch/pkg/target"
)

// Report aggregates findings for one sweep.
type Report struct {
	StartedAt  time.Time `json:"startedAt"`
	FinishedAt time.Time `json:"finishedAt"`

	KeysCompared        int `json:"keysCompared"`
	KeysSkippedInFlight int `json:"keysSkippedInFlight"`
	KeysSkippedSuspect  int `json:"keysSkippedSuspect"`
	// KeysSkippedAdopted counts keys read out of the target at startup that no
	// event has touched since. Comparing one against the target would prove
	// only that the target agrees with itself (§5.6).
	KeysSkippedAdopted int `json:"keysSkippedAdopted"`

	// SettlementWindow is the W this sweep ran with, captured once at sweep
	// start. It is stated because a finding means little without it: the same
	// disagreement is a false positive under a 1s window and real drift under a
	// 60s one.
	SettlementWindow time.Duration `json:"settlementWindow"`

	Findings   []Finding                 `json:"findings"`
	ByCategory map[Category]int          `json:"-"`
	ByTrust    map[oracle.TrustState]int `json:"-"`

	// EvictionSuspected is set when the target's eviction counter moved during
	// the sweep. A sweep that finds mass absence at the same moment the store
	// was evicting has an obvious explanation, and saying so saves the operator
	// an hour of looking in the wrong place (§5.7).
	EvictionSuspected bool          `json:"evictionSuspected"`
	TargetHealth      target.Health `json:"targetHealth"`

	// Truncated reports that the finding list hit its cap. Under mass
	// divergence the magnitude matters more than the per-key detail, and the
	// reporter must not run out of memory trying to record all of it.
	Truncated bool `json:"truncated"`

	maxFindings int
}

// NewReport starts a report that caps its finding list at opts.MaxFindings.
func NewReport(startedAt time.Time, opts Options) *Report {
	opts.applyDefaults()
	return &Report{
		StartedAt:   startedAt,
		ByCategory:  map[Category]int{},
		ByTrust:     map[oracle.TrustState]int{},
		maxFindings: opts.MaxFindings,
	}
}

// Add records a finding, or notes the truncation once the cap is reached.
//
// The counts keep rising after the cap. Losing the magnitude would be worse
// than losing the detail: an operator seeing ten thousand findings needs to
// know whether the real number is ten thousand or a million.
func (r *Report) Add(f *Finding) {
	if f == nil {
		return
	}

	r.ByCategory[f.Category]++
	r.ByTrust[f.Trust]++

	if len(r.Findings) >= r.maxFindings {
		r.Truncated = true
		return
	}
	r.Findings = append(r.Findings, *f)
}

// Total returns how many findings the sweep produced, including any beyond the
// cap.
func (r *Report) Total() int {
	n := 0
	for _, count := range r.ByCategory {
		n += count
	}
	return n
}

// Alertable returns the number of findings on keys driftwatch considers itself
// a reliable witness for.
//
// Suspect keys are excluded. This is the distinction that keeps driftwatch
// honest: it never claims the target is broken while it knows its own
// subscription dropped events, which is exactly when it matters most (§23 A7).
func (r *Report) Alertable() int {
	return r.Total() - r.ByTrust[oracle.TrustSuspect]
}

// Duration returns how long the sweep took.
func (r *Report) Duration() time.Duration {
	if r.StartedAt.IsZero() || r.FinishedAt.IsZero() {
		return 0
	}
	return r.FinishedAt.Sub(r.StartedAt)
}

// Summary returns a one-line rendering for logs.
func (r *Report) Summary() string {
	var b strings.Builder

	fmt.Fprintf(&b, "compared %d keys in %s: %d findings",
		r.KeysCompared, r.Duration().Round(time.Millisecond), r.Total())

	if suspect := r.ByTrust[oracle.TrustSuspect]; suspect > 0 {
		fmt.Fprintf(&b, " (%d alertable, %d suspect)", r.Alertable(), suspect)
	}
	if r.KeysSkippedInFlight > 0 {
		fmt.Fprintf(&b, ", %d in flight", r.KeysSkippedInFlight)
	}
	if r.Truncated {
		fmt.Fprintf(&b, ", list truncated at %d", r.maxFindings)
	}
	if r.EvictionSuspected {
		b.WriteString(", eviction suspected")
	}
	return b.String()
}

// Text returns a human-readable multi-line rendering.
func (r *Report) Text() string {
	var b strings.Builder

	b.WriteString("driftwatch sweep report\n")
	b.WriteString("=======================\n\n")

	fmt.Fprintf(&b, "started    %s\n", r.StartedAt.UTC().Format(time.RFC3339))
	fmt.Fprintf(&b, "duration   %s\n", r.Duration().Round(time.Millisecond))
	fmt.Fprintf(&b, "compared   %d keys\n", r.KeysCompared)
	if r.KeysSkippedInFlight > 0 {
		fmt.Fprintf(&b, "in flight  %d keys, not yet settled\n", r.KeysSkippedInFlight)
	}
	if r.KeysSkippedSuspect > 0 {
		fmt.Fprintf(&b, "skipped    %d keys with an incomplete view\n", r.KeysSkippedSuspect)
	}
	b.WriteString("\n")

	if r.Total() == 0 {
		b.WriteString("no divergence found\n")
		r.writeHealth(&b)
		return b.String()
	}

	fmt.Fprintf(&b, "findings   %d total, %d alertable\n", r.Total(), r.Alertable())
	if suspect := r.ByTrust[oracle.TrustSuspect]; suspect > 0 {
		fmt.Fprintf(&b,
			"           %d on keys driftwatch cannot vouch for, reported separately\n", suspect)
	}
	if r.Truncated {
		fmt.Fprintf(&b, "           list truncated at %d; the counts above are complete\n",
			r.maxFindings)
	}
	b.WriteString("\n")

	b.WriteString("by category\n")
	for _, cat := range sortedCategories(r.ByCategory) {
		fmt.Fprintf(&b, "  %-18s %d\n", cat, r.ByCategory[cat])
	}
	b.WriteString("\n")

	b.WriteString("findings\n")
	for i := range r.Findings {
		r.writeFinding(&b, &r.Findings[i])
	}

	r.writeHealth(&b)
	return b.String()
}

func (r *Report) writeFinding(b *strings.Builder, f *Finding) {
	fmt.Fprintf(b, "  %s  %s\n", f.Category, f.Key)
	fmt.Fprintf(b, "    oracle   %s\n", f.OracleValue)
	fmt.Fprintf(b, "    target   %s\n", f.TargetValue)

	if len(f.MissingMembers) > 0 {
		fmt.Fprintf(b, "    missing  %s\n", renderMembers(f.MissingMembers, f.MissingMemberCount))
	}
	if len(f.ExtraMembers) > 0 {
		fmt.Fprintf(b, "    extra    %s\n", renderMembers(f.ExtraMembers, f.ExtraMemberCount))
	}
	if f.TargetType != "" {
		fmt.Fprintf(b, "    holds    %s\n", f.TargetType)
	}
	if f.OracleTTL != nil || f.TargetTTL != nil {
		fmt.Fprintf(b, "    ttl      oracle %s, target %s\n",
			renderTTL(f.OracleTTL), renderTTL(f.TargetTTL))
	}

	fmt.Fprintf(b, "    version  %d, last event %s from %s seq %d\n",
		f.OracleVersion, f.LastEventAt.UTC().Format(time.RFC3339), f.LastPublisher, f.LastSeq)

	if f.Trust != oracle.TrustComplete {
		fmt.Fprintf(b, "    trust    %s\n", f.Trust)
	}
	b.WriteString("\n")
}

func (r *Report) writeHealth(b *strings.Builder) {
	if !r.TargetHealth.Reachable && r.TargetHealth.Version == "" {
		return
	}

	b.WriteString("target\n")
	fmt.Fprintf(b, "  reachable        %t\n", r.TargetHealth.Reachable)
	if r.TargetHealth.Version != "" {
		fmt.Fprintf(b, "  version          %s\n", r.TargetHealth.Version)
	}
	if r.TargetHealth.Role != "" {
		fmt.Fprintf(b, "  role             %s\n", r.TargetHealth.Role)
	}
	fmt.Fprintf(b, "  keyspace         %d\n", r.TargetHealth.KeyspaceSize)
	fmt.Fprintf(b, "  evicted keys     %d\n", r.TargetHealth.EvictedKeys)

	if r.EvictionSuspected {
		b.WriteString("\n  the eviction counter moved during this sweep, which explains\n")
		b.WriteString("  missing keys without any drift having occurred\n")
	}
	if r.TargetHealth.Role == "replica" {
		b.WriteString("\n  reads came from a replica, which can serve stale data and\n")
		b.WriteString("  produce findings that resolve themselves\n")
	}
}

func renderMembers(members []string, total int) string {
	shown := strings.Join(members, " ")
	if total > len(members) {
		return fmt.Sprintf("%s ... and %d more (%d total)", shown, total-len(members), total)
	}
	return shown
}

func renderTTL(d *time.Duration) string {
	if d == nil {
		return "none"
	}
	return d.String()
}

func sortedCategories(counts map[Category]int) []Category {
	out := make([]Category, 0, len(counts))
	for cat := range counts {
		out = append(out, cat)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// reportJSON is the wire shape, with the maps keyed by name rather than by the
// numeric constants, so the output survives a category being renumbered.
type reportJSON struct {
	StartedAt  time.Time `json:"startedAt"`
	FinishedAt time.Time `json:"finishedAt"`
	DurationMs int64     `json:"durationMs"`

	KeysCompared        int `json:"keysCompared"`
	KeysSkippedInFlight int `json:"keysSkippedInFlight"`
	KeysSkippedSuspect  int `json:"keysSkippedSuspect"`
	KeysSkippedAdopted  int `json:"keysSkippedAdopted"`

	SettlementWindow string `json:"settlementWindow"`

	Total     int `json:"total"`
	Alertable int `json:"alertable"`

	ByCategory map[string]int `json:"byCategory"`
	ByTrust    map[string]int `json:"byTrust"`

	Findings []findingJSON `json:"findings"`

	EvictionSuspected bool          `json:"evictionSuspected"`
	Truncated         bool          `json:"truncated"`
	TargetHealth      target.Health `json:"targetHealth"`
}

type findingJSON struct {
	Key      string `json:"key"`
	Category string `json:"category"`
	Trust    string `json:"trust"`

	OracleValue string `json:"oracleValue"`
	TargetValue string `json:"targetValue"`

	MissingMembers     []string `json:"missingMembers,omitempty"`
	MissingMemberCount int      `json:"missingMemberCount,omitempty"`
	ExtraMembers       []string `json:"extraMembers,omitempty"`
	ExtraMemberCount   int      `json:"extraMemberCount,omitempty"`

	TargetType string `json:"targetType,omitempty"`
	OracleTTL  string `json:"oracleTTL,omitempty"`
	TargetTTL  string `json:"targetTTL,omitempty"`

	OracleVersion uint64    `json:"oracleVersion"`
	LastEventAt   time.Time `json:"lastEventAt"`
	LastSeq       uint64    `json:"lastSeq"`
	LastPublisher string    `json:"lastPublisher"`

	FirstSeenAt time.Time `json:"firstSeenAt,omitempty"`
	Confirmed   bool      `json:"confirmed"`
}

// JSON renders the report for machine consumption.
//
// Values are rendered through event.Value.String, which truncates and hex
// encodes. A report is a diagnostic, not a backup: emitting whole values would
// put the contents of the audited store into logs, and §18 forbids that for
// good reason.
func (r *Report) JSON() ([]byte, error) {
	out := reportJSON{
		StartedAt:           r.StartedAt.UTC(),
		FinishedAt:          r.FinishedAt.UTC(),
		DurationMs:          r.Duration().Milliseconds(),
		KeysCompared:        r.KeysCompared,
		KeysSkippedInFlight: r.KeysSkippedInFlight,
		KeysSkippedSuspect:  r.KeysSkippedSuspect,
		KeysSkippedAdopted:  r.KeysSkippedAdopted,
		SettlementWindow:    r.SettlementWindow.String(),
		Total:               r.Total(),
		Alertable:           r.Alertable(),
		ByCategory:          map[string]int{},
		ByTrust:             map[string]int{},
		Findings:            make([]findingJSON, 0, len(r.Findings)),
		EvictionSuspected:   r.EvictionSuspected,
		Truncated:           r.Truncated,
		TargetHealth:        r.TargetHealth,
	}

	for cat, n := range r.ByCategory {
		out.ByCategory[cat.String()] = n
	}
	for trust, n := range r.ByTrust {
		out.ByTrust[trust.String()] = n
	}

	for i := range r.Findings {
		f := &r.Findings[i]
		fj := findingJSON{
			Key:                f.Key,
			Category:           f.Category.String(),
			Trust:              f.Trust.String(),
			OracleValue:        f.OracleValue.String(),
			TargetValue:        f.TargetValue.String(),
			MissingMembers:     f.MissingMembers,
			MissingMemberCount: f.MissingMemberCount,
			ExtraMembers:       f.ExtraMembers,
			ExtraMemberCount:   f.ExtraMemberCount,
			TargetType:         f.TargetType,
			OracleVersion:      f.OracleVersion,
			LastEventAt:        f.LastEventAt.UTC(),
			LastSeq:            f.LastSeq,
			LastPublisher:      f.LastPublisher,
			Confirmed:          f.Confirmed,
		}
		if f.OracleTTL != nil {
			fj.OracleTTL = f.OracleTTL.String()
		}
		if f.TargetTTL != nil {
			fj.TargetTTL = f.TargetTTL.String()
		}
		if !f.FirstSeenAt.IsZero() {
			fj.FirstSeenAt = f.FirstSeenAt.UTC()
		}
		out.Findings = append(out.Findings, fj)
	}

	return json.MarshalIndent(out, "", "  ")
}
