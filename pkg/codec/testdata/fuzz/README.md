# Fuzz corpus

Committed inputs for `FuzzDecodeJSON`. Go runs every file here as a seed on a
plain `go test ./pkg/codec/`, so these are exercised on every CI run, not only
when someone remembers to pass `-fuzz`.

## Provenance

These are not hand-written. They were selected from the fuzzer's own coverage
cache after a 5-minute run:

```sh
make fuzz FUZZTIME=300s
```

That run executed **43,601,239 inputs at ~140,000/sec and found no crash.** It
left 384 entries in the coverage cache — inputs the fuzzer considered
interesting because each reached a new edge.

384 near-identical blobs is not a corpus worth committing; it is noise that
makes a diff unreadable and tells a reviewer nothing. So the 384 were reduced by
asking what actually distinguishes them: for a decoder, each distinct rejection
reason is a distinct code path. One representative was kept per distinct
`(plain codec outcome, mapped codec outcome)` pair, shortest input first,
because a minimal input that reaches a path is a better regression case than a
long one reaching the same path.

**384 entries collapsed to 6.** That number is worth stating plainly: the
decoder has six distinguishable behaviours on malformed input, and five minutes
of mutation across 43 million executions found no seventh.

## What each file is for

| File | Input | Path it holds open |
|---|---|---|
| `whitespace-padded-empty-object` | `" {}  "` | Leading and trailing whitespace around a valid-but-empty document |
| `truncated-number-empty-key` | `{"":0.A` | A number that starts valid and stops being one, under an empty key |
| `control-char-in-op` | `{"op":"\t"}` | A raw control character where an op name is expected |
| `invalid-utf8-escape-in-op` | `{"op":"\<0x83>"}` | An escape introducing a byte that is not valid UTF-8 |
| `truncated-deep-nesting` | `{"":[[[[[[[0.` | Nesting that hits the depth guard *and* ends mid-token |
| `deep-nesting-empty-key` | `{"":[[[[[[[0]]]]]]]}` | Nesting that hits the depth guard on a well-formed document |

The last two are the pair worth keeping together. They differ only in whether
the document terminates, and they take different paths — which is exactly the
kind of distinction a depth guard gets wrong.

## Note on what is *not* here

No crash reproducers, because there are no crashes. If the fuzzer ever finds
one, the Go toolchain writes it into this directory automatically and it becomes
a permanent regression case (PRD §16.2). A file appearing here that is not in
the table above means the fuzzer found something — read it before deleting it.

The adversarial seeds from PRD §25.1 live in `pkg/codec/fuzz_test.go` as `f.Add`
calls rather than as files here, so that a reader can see what they are and why
without decoding the corpus format.
