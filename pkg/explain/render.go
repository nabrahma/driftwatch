package explain

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nabrahma/driftwatch/pkg/event"
	"github.com/nabrahma/driftwatch/pkg/seqtrack"
)

// MaxWidth is the column budget for Text.
//
// 100 rather than 80 because the history table genuinely needs the room, and
// well under the 120 §9 M13 sets as the hard limit so that a long key name or
// an unusually wide member does not push a line over it. The limit exists
// because this output is read in a terminal beside a dashboard, not in a
// full-screen editor, and a wrapped line in a table destroys the alignment that
// makes the table worth having.
const MaxWidth = 100

// Text renders the explanation for a terminal.
//
// This is the screenshot that goes in the README, so it is written to be read
// top to bottom by somebody who has just been paged: the verdict first, then
// the two values that produced it, then what driftwatch thinks happened, and
// only then the evidence trail.
func (e *Explanation) Text() string {
	var b strings.Builder

	e.writeHeader(&b)
	e.writeValues(&b)
	e.writeDiagnosis(&b)
	e.writeHistory(&b)
	e.writePublishers(&b)

	return b.String()
}

func (e *Explanation) writeHeader(b *strings.Builder) {
	fmt.Fprintf(b, "KEY  %s\n", clipMiddle(DisplayKey(e.Key), MaxWidth-5))
	b.WriteString(strings.Repeat("─", MaxWidth-30) + "\n")

	right := e.settlementNote()
	fmt.Fprintf(b, "%-*s%s\n", MaxWidth-30-utf8.RuneCountInString(right),
		"VERDICT   "+e.Verdict.String(), right)
	b.WriteString("\n")
}

// settlementNote is the right-hand annotation on the verdict line: the single
// fact that most often decides whether a disagreement means anything.
func (e *Explanation) settlementNote() string {
	switch {
	case e.LastEventAt.IsZero():
		return "no events observed"
	case e.Settled:
		return "settled " + compact(e.Age()) + " ago"
	default:
		return "in flight, settles in " + compact(e.SettlementWindow-e.Age())
	}
}

func (e *Explanation) writeValues(b *strings.Builder) {
	oracleNote := fmt.Sprintf("version %d   trust %s", e.OracleVersion, e.OracleTrust)
	if !e.KnownToOracle {
		oracleNote = "never observed"
	}
	fmt.Fprintf(b, "ORACLE    %-34s %s\n", clip(e.OracleValue.String(), 34), oracleNote)

	switch {
	case !e.TargetReachable:
		fmt.Fprintf(b, "TARGET    %-34s %s\n", "unreadable", "the store could not be reached")
	case e.TargetType != "":
		fmt.Fprintf(b, "TARGET    %-34s %s\n",
			"holds a "+e.TargetType, "which this projection cannot read")
	default:
		fmt.Fprintf(b, "TARGET    %-34s %s\n",
			clip(e.TargetValue.String(), 34), readNote(e.readAge()))
	}

	e.writeDiff(b)
	e.writeTTL(b)
	b.WriteString("\n")
}

func (e *Explanation) writeDiff(b *strings.Builder) {
	if e.MissingMemberCount > 0 {
		writeWrapped(b, "DIFF      missing in target: ",
			renderMembers(e.MissingMembers, e.MissingMemberCount))
	}
	if e.ExtraMemberCount > 0 {
		writeWrapped(b, "DIFF      extra in target:   ",
			renderMembers(e.ExtraMembers, e.ExtraMemberCount))
	}
}

// writeWrapped emits a labeled field, continuing onto further lines indented
// to the label's width.
//
// Member names are operator-chosen and can be any length; a Kubernetes pod name
// alone eats half the budget. Wrapping rather than truncating keeps the whole
// list readable, which matters because the list is the answer.
func writeWrapped(b *strings.Builder, label, body string) {
	indent := strings.Repeat(" ", utf8.RuneCountInString(label))

	for i, line := range wrap(body, MaxWidth-utf8.RuneCountInString(label)) {
		if i == 0 {
			b.WriteString(label + line + "\n")
			continue
		}
		b.WriteString(indent + line + "\n")
	}
}

func (e *Explanation) writeTTL(b *strings.Builder) {
	if e.OracleTTL == nil && e.TargetTTL == nil {
		return
	}
	fmt.Fprintf(b, "TTL       oracle %-27s target %s\n",
		renderTTL(e.OracleTTL), renderTTL(e.TargetTTL))
}

func (e *Explanation) writeDiagnosis(b *strings.Builder) {
	if len(e.Diagnosis) == 0 {
		return
	}

	b.WriteString("DIAGNOSIS\n")
	for i := range e.Diagnosis {
		d := &e.Diagnosis[i]

		// The confidence tag is the first thing on the line because it governs
		// how the sentence after it should be read.
		tag := fmt.Sprintf("  [%s]", d.Confidence)
		for j, line := range wrap(d.Statement, MaxWidth-12) {
			if j == 0 {
				fmt.Fprintf(b, "%-10s %s\n", tag, line)
			} else {
				fmt.Fprintf(b, "%-10s %s\n", "", line)
			}
		}
		for _, ev := range d.Evidence {
			for j, line := range wrap(ev, MaxWidth-16) {
				prefix := "           - "
				if j > 0 {
					prefix = "             "
				}
				b.WriteString(prefix + line + "\n")
			}
		}
	}
	b.WriteString("\n")
}

func (e *Explanation) writeHistory(b *strings.Builder) {
	if len(e.History) == 0 {
		b.WriteString("HISTORY   none\n\n")
		return
	}

	shown := e.History
	note := fmt.Sprintf("last %d", len(shown))
	if e.HistoryTruncated {
		note += " retained, earlier events discarded"
	}

	fmt.Fprintf(b, "HISTORY (%s)\n", note)
	for i := range shown {
		s := &shown[i]
		fmt.Fprintf(b, "  #%-3d %-8d %-11s %-8s %-34s v%-4d %s\n",
			s.Index,
			s.Event.Seq,
			clip(s.Event.Publisher, 11),
			clip(s.Event.Op.String(), 8),
			clip(valueOrMember(s), 34),
			s.Version,
			"-"+compact(e.GeneratedAt.Sub(s.AppliedAt)))

		if s.Note != "" {
			for _, line := range wrap("⚠ "+s.Note, MaxWidth-10) {
				b.WriteString("       " + line + "\n")
			}
		}
	}
	b.WriteString("\n")
}

// valueOrMember renders the part of a step a reader actually scans for: which
// member moved, or what the value became.
func valueOrMember(s *Step) string {
	if s.Event.Member != "" {
		return s.Event.Member + " → " + shortValue(s.ValueAfter)
	}
	return shortValue(s.ValueAfter)
}

func (e *Explanation) writePublishers(b *strings.Builder) {
	if len(e.PublisherStates) == 0 {
		return
	}

	b.WriteString("PUBLISHERS\n")
	for _, ps := range e.PublisherStates {
		gaps := uint64(0)
		if ps.Gaps != nil {
			gaps = ps.Gaps.Count()
		}

		fmt.Fprintf(b, "  %-13s epoch %-4d hwm %-9d gaps %-8d last seen %s ago\n",
			clip(ps.ID, 13), ps.Epoch, ps.HWM, gaps,
			compact(e.GeneratedAt.Sub(ps.LastSeen)))
	}
}

// ---------------------------------------------------------------------------
// Rendering helpers.
// ---------------------------------------------------------------------------

// DisplayKey renders a key for a terminal.
//
// A Redis key is arbitrary bytes, and a binary one printed raw will reset a
// terminal's encoding or paint control characters into the output. §9 M13
// requires binary keys to be hex-escaped; this is where that happens, and
// `--key-hex` on the CLI is the inverse.
func DisplayKey(key string) string {
	if key == "" {
		return `"" (the empty key, which Redis allows)`
	}

	if !utf8.ValidString(key) || strings.ContainsFunc(key, isControl) {
		return "hex:" + hexString(key)
	}
	return key
}

func isControl(r rune) bool { return r < 0x20 || r == 0x7f }

func hexString(s string) string {
	const digits = "0123456789abcdef"

	var b strings.Builder
	b.Grow(len(s) * 2)
	for i := 0; i < len(s); i++ {
		b.WriteByte(digits[s[i]>>4])
		b.WriteByte(digits[s[i]&0x0f])
	}
	return b.String()
}

func shortValue(v event.Value) string {
	switch v.Kind {
	case event.ValueAbsent:
		return "absent"
	case event.ValueScalar:
		return clip(strconv.Quote(string(v.Scalar)), 20)
	case event.ValueCounter:
		return strconv.FormatInt(v.Counter, 10)
	case event.ValueSet:
		members := make([]string, 0, len(v.Members))
		for m := range v.Members {
			members = append(members, m)
		}
		sort.Strings(members)

		const shown = 2
		if len(members) > shown {
			return fmt.Sprintf("{%s,+%d}", strings.Join(members[:shown], ","), len(members)-shown)
		}
		return "{" + strings.Join(members, ",") + "}"
	default:
		return v.String()
	}
}

func renderMembers(shown []string, total int) string {
	joined := strings.Join(shown, ", ")
	if total > len(shown) {
		return fmt.Sprintf("%s ... and %d more (%d total)", joined, total-len(shown), total)
	}
	return joined
}

// readNote says how fresh the target read is.
//
// The freshness matters — a value read a minute ago proves nothing about now —
// but "read 0µs ago" is noise, so a read this instant says so in words.
func readNote(age time.Duration) string {
	if age < time.Millisecond {
		return "read just now"
	}
	return "read " + compact(age) + " ago"
}

func renderTTL(d *time.Duration) string {
	if d == nil {
		return "none"
	}
	return compact(*d)
}

// clip shortens a string to n columns, marking that it was shortened.
func clip(s string, n int) string {
	if utf8.RuneCountInString(s) <= n || n < 1 {
		return s
	}
	runes := []rune(s)
	return string(runes[:n-1]) + "…"
}

// clipMiddle shortens a string from the middle, keeping both ends.
//
// For a key this is the difference between a useful line and a useless one.
// Keys are usually a prefix plus a hash — "block:9f3a2c1e…" — and truncating
// from the right throws away the only part that identifies which key it is,
// while keeping both ends leaves it recognizable and greppable.
func clipMiddle(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n || n < 16 {
		return s
	}

	suffix := fmt.Sprintf("… (%d bytes)", len(s))
	room := n - utf8.RuneCountInString(suffix)
	head := room * 2 / 3

	return string(runes[:head]) + "…" + string(runes[len(runes)-(room-head):]) + suffix
}

// wrap breaks text at word boundaries so no line exceeds n columns.
//
// A single word longer than n is hard-broken rather than allowed to overflow.
// That case is not hypothetical: a member name is whatever the operator's
// naming scheme produces, and one of those is eventually longer than a
// terminal is wide.
func wrap(text string, n int) []string {
	if n < 1 {
		n = 1
	}

	words := strings.Fields(text)
	if len(words) == 0 {
		return []string{""}
	}

	lines := []string{}
	current := ""

	for _, word := range words {
		for utf8.RuneCountInString(word) > n {
			if current != "" {
				lines = append(lines, current)
				current = ""
			}
			runes := []rune(word)
			lines = append(lines, string(runes[:n]))
			word = string(runes[n:])
		}

		switch {
		case current == "":
			current = word
		case utf8.RuneCountInString(current)+1+utf8.RuneCountInString(word) > n:
			lines = append(lines, current)
			current = word
		default:
			current += " " + word
		}
	}
	return append(lines, current)
}

// ---------------------------------------------------------------------------
// JSON.
// ---------------------------------------------------------------------------

// explanationJSON is the wire shape.
//
// Written out by hand rather than tagged onto Explanation because the two have
// different jobs: the struct is what the rules read, and this is a stable
// contract for whoever pipes `--output json` into jq. Values go through
// String, which truncates and hex-encodes — §18 forbids putting the contents of
// the audited store into output that gets shipped somewhere.
type explanationJSON struct {
	Key         string    `json:"key"`
	KeyHex      string    `json:"keyHex,omitempty"`
	GeneratedAt time.Time `json:"generatedAt"`
	Verdict     string    `json:"verdict"`

	Oracle oracleSideJSON `json:"oracle"`
	Target targetSideJSON `json:"target"`

	SettlementWindow string `json:"settlementWindow"`
	Settled          bool   `json:"settled"`

	MissingMembers     []string `json:"missingMembers,omitempty"`
	MissingMemberCount int      `json:"missingMemberCount,omitempty"`
	ExtraMembers       []string `json:"extraMembers,omitempty"`
	ExtraMemberCount   int      `json:"extraMemberCount,omitempty"`

	Diagnosis  []diagnosisJSON `json:"diagnosis"`
	History    []stepJSON      `json:"history"`
	Truncated  bool            `json:"historyTruncated"`
	Publishers []publisherJSON `json:"publishers"`
}

type oracleSideJSON struct {
	Known       bool      `json:"known"`
	Value       string    `json:"value"`
	Version     uint64    `json:"version"`
	Trust       string    `json:"trust"`
	TTL         string    `json:"ttl,omitempty"`
	LastEventAt time.Time `json:"lastEventAt,omitempty"`
}

type targetSideJSON struct {
	Reachable bool      `json:"reachable"`
	Value     string    `json:"value"`
	Type      string    `json:"type,omitempty"`
	TTL       string    `json:"ttl,omitempty"`
	ReadAt    time.Time `json:"readAt,omitempty"`
	Role      string    `json:"role,omitempty"`
	Evictions uint64    `json:"evictionsObserved,omitempty"`
}

type diagnosisJSON struct {
	Code       string   `json:"code"`
	Confidence string   `json:"confidence"`
	Statement  string   `json:"statement"`
	Evidence   []string `json:"evidence,omitempty"`
}

type stepJSON struct {
	Index         int       `json:"index"`
	Publisher     string    `json:"publisher"`
	Epoch         uint64    `json:"epoch"`
	Seq           uint64    `json:"seq"`
	Op            string    `json:"op"`
	Member        string    `json:"member,omitempty"`
	Verdict       string    `json:"verdict"`
	ValueAfter    string    `json:"valueAfter"`
	Version       uint64    `json:"version"`
	AppliedAt     time.Time `json:"appliedAt"`
	DeltaFromPrev string    `json:"deltaFromPrev,omitempty"`
	Note          string    `json:"note,omitempty"`
}

type publisherJSON struct {
	ID            string    `json:"id"`
	Epoch         uint64    `json:"epoch"`
	HWM           uint64    `json:"highWaterMark"`
	MissingCount  uint64    `json:"missingEvents"`
	Gaps          []string  `json:"gaps,omitempty"`
	GapsTruncated bool      `json:"gapsTruncated,omitempty"`
	Restarts      uint64    `json:"restarts"`
	LastSeen      time.Time `json:"lastSeen"`
}

// JSON renders the explanation for machine consumption.
func (e *Explanation) JSON() ([]byte, error) {
	out := explanationJSON{
		Key:              e.Key,
		GeneratedAt:      e.GeneratedAt.UTC(),
		Verdict:          e.Verdict.String(),
		SettlementWindow: e.SettlementWindow.String(),
		Settled:          e.Settled,
		Oracle: oracleSideJSON{
			Known:   e.KnownToOracle,
			Value:   e.OracleValue.String(),
			Version: e.OracleVersion,
			Trust:   e.OracleTrust.String(),
		},
		Target: targetSideJSON{
			Reachable: e.TargetReachable,
			Value:     e.TargetValue.String(),
			Type:      e.TargetType,
			Role:      e.TargetHealth.Role,
			Evictions: e.TargetHealth.EvictedKeys,
		},
		MissingMembers:     e.MissingMembers,
		MissingMemberCount: e.MissingMemberCount,
		ExtraMembers:       e.ExtraMembers,
		ExtraMemberCount:   e.ExtraMemberCount,
		Truncated:          e.HistoryTruncated,
		Diagnosis:          make([]diagnosisJSON, 0, len(e.Diagnosis)),
		History:            make([]stepJSON, 0, len(e.History)),
		Publishers:         make([]publisherJSON, 0, len(e.PublisherStates)),
	}

	if !utf8.ValidString(e.Key) {
		// An invalid UTF-8 key would be mangled by the JSON encoder into
		// replacement characters, which is silent corruption of the one field
		// the whole document is about.
		out.Key, out.KeyHex = "", hexString(e.Key)
	}
	if !e.LastEventAt.IsZero() {
		out.Oracle.LastEventAt = e.LastEventAt.UTC()
	}
	if e.OracleTTL != nil {
		out.Oracle.TTL = e.OracleTTL.String()
	}
	if !e.TargetReadAt.IsZero() {
		out.Target.ReadAt = e.TargetReadAt.UTC()
	}
	if e.TargetTTL != nil {
		out.Target.TTL = e.TargetTTL.String()
	}

	for _, d := range e.Diagnosis {
		out.Diagnosis = append(out.Diagnosis, diagnosisJSON{
			Code:       d.Code,
			Confidence: d.Confidence.String(),
			Statement:  d.Statement,
			Evidence:   d.Evidence,
		})
	}
	for i := range e.History {
		out.History = append(out.History, stepWire(&e.History[i]))
	}
	for _, ps := range e.PublisherStates {
		out.Publishers = append(out.Publishers, publisherWire(ps))
	}

	return json.MarshalIndent(out, "", "  ")
}

func stepWire(s *Step) stepJSON {
	out := stepJSON{
		Index:      s.Index,
		Publisher:  s.Event.Publisher,
		Epoch:      s.Event.Epoch,
		Seq:        s.Event.Seq,
		Op:         s.Event.Op.String(),
		Member:     s.Event.Member,
		Verdict:    s.Verdict.String(),
		ValueAfter: s.ValueAfter.String(),
		Version:    s.Version,
		AppliedAt:  s.AppliedAt.UTC(),
		Note:       s.Note,
	}
	if s.DeltaFromPrev != 0 {
		out.DeltaFromPrev = s.DeltaFromPrev.String()
	}
	return out
}

//nolint:gocritic // hugeParam: see gapTruncationNote.
func publisherWire(ps seqtrack.PublisherState) publisherJSON {
	out := publisherJSON{
		ID:       ps.ID,
		Epoch:    ps.Epoch,
		HWM:      ps.HWM,
		Restarts: ps.RestartCount,
		LastSeen: ps.LastSeen.UTC(),
	}
	if ps.Gaps != nil {
		out.MissingCount = ps.Gaps.Count()
		out.GapsTruncated = ps.Gaps.Truncated()
		for _, iv := range ps.Gaps.Intervals() {
			out.Gaps = append(out.Gaps, iv.String())
		}
	}
	return out
}
