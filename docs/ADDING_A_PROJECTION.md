# Adding a projection

A projection is the fold that turns an event stream into an expectation. It is
the piece that varies most between deployments, and the piece with the least
machinery around it: a pure function from a previous value and an event to a new
value.

Three ship: `keysetOwnership`, `scalar` and `counter`. This walks through adding
a fourth.

## The contract

```go
type Projection interface {
    Name() string
    TargetKey(e *event.Event) (string, error)
    Apply(prev event.Value, e *event.Event) (Mutation, error)
    Commutative() bool
    KeyOwnership() OwnershipModel
    TargetShape() Shape
}
```

**`Apply` must be pure.** No I/O, no clock, no randomness, no logging, no
package-level state. It is called on the ingest hot path, once per event, and it
must return the same mutation for the same inputs forever, because `replay`
depends on that to turn a production incident into a regression test, and the
property tests depend on it to compare an optimized implementation against a
naive one.

`TargetKey` exists as a separate method for a reason worth knowing. The store key
is the key template's *output*, not the event's raw `Key` field, and `Apply`
needs the previous value stored under that key. Without the split, the caller
looks up the raw key, misses on every projection with a `keyTemplate` configured,
folds every event onto an empty previous value, and produces a plausible-looking
oracle that is quietly wrong. That was a real defect: [D-013](DISCOVERIES.md).

## Declaring commutativity honestly

`Commutative()` answers: *does the order of events change the final state?*

If it returns `false`, the oracle orders by sequence before applying. If it
returns `true`, driftwatch may apply out of order, which is cheaper and, when
the answer is wrong, produces an oracle that disagrees with the store for reasons
nobody will find.

The interesting case is that **commutativity is often a property of the
configured instance rather than of the type.** `counter` demonstrates it:
addition commutes, so a stream of nothing but increments reaches the same total
in any order. Mix in a single absolute `OpSet` and it does not: set-then-increment
and increment-then-set differ. So `counter.Commutative()` returns `false` unless
the operator declares `incrOnly`, which is a promise about the producer that
driftwatch cannot verify on its own.

`keysetOwnership` returns `false` too, and for a subtler reason: adds and removes
of *different* members commute, but an add and a remove of the *same* member do
not. Reporting `true` because the common case commutes would be wrong in exactly
the case that matters.

When in doubt, return `false`. The cost is ordering work the oracle would have
skipped. The cost of the other mistake is a wrong answer.

## `KeyOwnership`, and why it is worth filling in

```go
type OwnershipModel struct {
    Partitioned bool
    KeyPattern  string  // e.g. "replica:{{.Publisher}}:*"
}
```

If publishers own disjoint keyspaces, say so. When a sequence gap is detected,
driftwatch cannot know which keys the missing events touched, but if publishers
are partitioned, it knows which ones they *could not* have touched, and can leave
those trustworthy instead of suspecting the whole keyspace.

The difference is coverage during a gap. Without partitioning, one publisher
missing forty events makes every key suspect and `coverage_ratio` collapses.
With it, only that publisher's partition is affected.

Leave `Partitioned` false if you are not sure. Claiming a partition that does not
hold means driftwatch keeps asserting on keys it should have stopped asserting
on, which is the direction of error that produces false accusations.

## A worked example

`pkg/projection/scalar.go` is the shortest. The skeleton:

```go
package projection

func init() { Register("myfold", newMyFold) }

type myFold struct {
    keyTmpl   *expander
    ownership OwnershipModel
}

func newMyFold(cfg map[string]string) (Projection, error) {
    keyTmpl, err := newExpander("keyTemplate",
        stringConfig(cfg, "keyTemplate", "{{.Key}}"), newBuilderPool())
    if err != nil {
        return nil, err
    }
    return &myFold{keyTmpl: keyTmpl, ownership: ownershipFrom(cfg)}, nil
}

func (m *myFold) Name() string                  { return "myfold" }
func (m *myFold) Commutative() bool             { return false }
func (m *myFold) KeyOwnership() OwnershipModel  { return m.ownership }
func (m *myFold) TargetShape() Shape            { return ShapeScalar }

func (m *myFold) TargetKey(e *event.Event) (string, error) {
    return m.keyTmpl.expand(e)
}

func (m *myFold) Apply(prev event.Value, e *event.Event) (Mutation, error) {
    key, err := m.keyTmpl.expand(e)
    if err != nil {
        return Mutation{}, err
    }

    switch e.Op {
    case event.OpSet:
        return Mutation{
            Key:    key,
            Action: ActionUpsert,
            Value:  event.Value{Kind: event.ValueScalar, Scalar: e.Value},
            TTL:    e.TTL,
        }, nil

    case event.OpDelete:
        return Mutation{Key: key, Action: ActionDelete}, nil

    case event.OpHeartbeat:
        // Carries no state. ActionNone means "this event does not affect the
        // target", which is different from an error.
        return Mutation{Key: key, Action: ActionNone}, nil

    default:
        // A typed sentinel, so the pipeline counts it as a projection error
        // with a closed-enum reason rather than folding the error string into
        // a metric label.
        return Mutation{}, fmt.Errorf("%w: myfold cannot apply %s",
            ErrUnsupportedOp, e.Op)
    }
}
```

## Two flags that are easy to skip and should not be

`Mutation` carries `Truncated` and `Saturated`. Both mean the same thing: *apply
this mutation, and know the value is approximate*.

- **`Truncated`**: a bound was hit while computing the value, such as a member
  set reaching `maxMembersPerKey`.
- **`Saturated`**: a counter clamped at the limits of `int64` rather than
  wrapping.

Setting them lets the oracle mark the key as holding an incomplete view, so the
differ does not report a disagreement that driftwatch caused itself by refusing
to grow. Not setting them turns a bound into a false positive.

## Testing it

Three levels, and the second is the one that finds things.

**Unit tests** for each op, including the ones you refuse. Assert the sentinel:
`ErrUnsupportedOp` and `ErrShapeMismatch` map onto different metric reasons and
send an operator to different places.

**A property test against a naive reference implementation.** This is the
pattern the existing projections use and the reason to bother:

```go
func TestProp_MyFoldMatchesTheNaiveImplementation(t *testing.T) {
    rapid.Check(t, func(t *rapid.T) {
        events := genEvents(t)

        // The obvious, slow, obviously-correct version.
        want := naiveFold(events)

        // The real one.
        got := applyAll(t, newMyFold(nil), events)

        require.Equal(t, want, got)
    })
}
```

Write the naive version first, in the test file, as slowly and stupidly as
possible. Then optimize the real one freely: the property test is what makes that
safe, and it is a far better use of effort than adding cases to a table.

**A commutativity property**, matching whatever `Commutative()` claims:

```go
func TestProp_MyFoldOrderDoesNotMatter(t *testing.T) {
    // Only if Commutative() returns true. If it returns false, assert the
    // opposite instead — find an ordering that produces a different result,
    // because a projection claiming non-commutativity it does not have is
    // paying for ordering it does not need.
}
```

Coverage floor for `pkg/projection` is 95%, enforced by
`hack/verify-coverage.sh`.

## Registering it

1. `init()` calls `Register("myfold", newMyFold)`.
2. Add the enum value in `api/v1alpha1/driftcheck_types.go`:
   `+kubebuilder:validation:Enum=keysetOwnership;scalar;counter;myfold`
3. `make manifests` to regenerate the CRD and the Helm chart's copy of it.
4. Document each config key in the field's doc comment, that text is what
   `kubectl explain driftcheck.spec.projection` renders, and CI fails if a field
   has none.

The projection type is **immutable on an existing DriftCheck**. Changing it
under a running check would fold new events onto values stored in the old
projection's shape, producing drift reports for an entire keyspace that are all
wrong. The webhook refuses the update and says to delete and recreate.

## What not to do

- **Do not read the target.** A projection that consults the store can agree with
  it for reasons unrelated to the event stream, which destroys the independence
  the whole tool rests on.
- **Do not use the wall clock.** `replay` must produce identical output for
  identical input, and a projection reading `time.Now()` breaks that silently.
- **Do not return an untyped error.** `projectionReason` maps sentinels onto a
  closed metric enum; an error string reaching a label is how a metric acquires
  unbounded cardinality.
- **Do not mutate `prev`.** It is the oracle's value, not a copy.
