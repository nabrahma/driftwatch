# Discoveries

Things that turned out not to work the way they were expected to.

This file is a primary deliverable (PRD §1.1.7, §21.3), not a changelog. An
entry earns its place by being something a competent engineer would have got
wrong, with the evidence that proves it.

One entry per finding, **newest first**, in this form:

```markdown
## D-000 — One-line statement of the surprising behaviour

**Found:** Phase N, while doing X.

**What happened:** What was expected, what actually happened.

**Why it matters:** The consequence for driftwatch if it had gone unnoticed.

**Fix:** What changed as a result.

**Evidence:** `docs/evidence/D-000-slug.txt`

**Regression test:** `pkg/foo: TestSomething`
```

Rules, from §23 A11: every entry describes something that actually happened, and
every entry links to a real captured file in `docs/evidence/`. Nothing is
written here in anticipation.

---

## D-002 — A JSON sequence number above 2^53 is silently corrupted by any decoder that goes through float64

**Found:** Phase 1, while implementing the json codec.

**What happened:** `encoding/json` is safe here *only* if the destination is a
typed `uint64` field — then it parses the digits directly and
`{"seq":9007199254740993}` survives. Decode the same payload into
`map[string]any` and the number arrives as `float64`, which has 53 bits of
mantissa, so it comes back as `9007199254740992`. Off by one, no error.

The trap is that M3 requires the JSON field names to be **configurable**, so a
foreign producer can be read without a code change. A fixed Go struct with
`json:"seq"` tags cannot express that. The natural implementation is therefore
`map[string]any` or `map[string]json.RawMessage` — and the first of those is
exactly the unsafe one. The requirement that makes the codec useful is the
requirement that steers you into the bug.

**Why it matters:** The sequence number is the single value driftwatch uses to
decide whether an event was lost. An off-by-one in `seq` manufactures a gap that
never happened, which marks keys `Suspect` and suppresses real findings, or
hides a gap that did happen. Either way driftwatch reports confidently on state
it has silently misread — the exact failure mode it exists to detect in other
systems.

2^53 is 9.007e15. A publisher emitting 100k events/sec reaches it in about
2,850 years, so this is not reachable by counting. It is reachable immediately
by any producer that derives `seq` from a timestamp in nanoseconds (1.78e18
today), from a Snowflake-style ID, or from a hash — all of which are ordinary
choices.

**Fix:** `pkg/codec` uses a hand-written scanner that parses `seq` and `epoch`
from their raw digits with `strconv.ParseUint`, never through `float64`. A
number written with a fraction or an exponent is rejected outright with
`ErrMalformed` rather than rounded, because a producer sending `1e300` as a
sequence number is misconfigured and should be told so. Numeric strings are
accepted, since sending large integers as strings is what a careful producer
does.

**Evidence:** `docs/evidence/D-002-json-float64-seq.txt`

**Regression test:** `pkg/codec: TestJSON_Decode/a_seq_above_2^53_survives...`,
`.../a_seq_written_as_a_float_is_rejected...`

---

## D-001 — Go's `time.RFC3339` layout rejects the lowercase `t` and `z` that RFC 3339 permits

**Found:** Phase 1, while writing an allocation-free RFC3339 parser for the json
codec.

**What happened:** The codec has two timestamp paths: a byte-level fast path for
the common case, and a fallback through `time.Parse` for escaped strings. A
differential test asserting the two agree failed on `2026-07-30t11:02:31z`. The
hand-written parser accepted it — RFC 3339 §5.6 says the separators "MAY be
lower case" — but `time.Parse(time.RFC3339, ...)` rejects it, because Go's
layout string matches `T` and `Z` literally.

**Why it matters:** Not because of the lowercase form itself, which is rare.
Because the fast path was a *superset* of the fallback. The two paths are
selected by whether the payload happens to contain a backslash anywhere, so the
same timestamp would have decoded successfully in one event and failed in
another for a reason entirely unrelated to the timestamp. That class of bug —
two code paths that agree on everything you thought to test — is why the
differential test was written before the optimization was trusted.

**Fix:** The fast path matches Go's behavior exactly, rejecting the lowercase
forms so that it can only ever be faster than the fallback, never more
permissive. The constraint is stated on `parseRFC3339` so the next person to
optimize it knows it is load-bearing rather than an oversight.

**Evidence:** `docs/evidence/D-001-rfc3339-lowercase.txt`

**Regression test:** `pkg/codec: TestJSON_RFC3339FastPathAgreesWithTimeParse`,
`TestJSON_RFC3339RejectionsFallThroughAndAreReportedHonestly`
