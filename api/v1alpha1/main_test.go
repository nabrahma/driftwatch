package v1alpha1

import (
	"testing"

	"go.uber.org/goleak"
)

// §16.5 requires goleak in every package, and this was the one without it.
//
// The webhook code here starts no goroutines of its own, which is exactly why
// the check belongs: the assertion is that it stays that way. Defaulting and
// validation run inside the API server's request path, where a goroutine that
// outlives the request leaks once per admission call — a rate proportional to
// how often the cluster is used, which is the worst shape a leak can have.
//
// The two ignores are third-party goroutines started at package init and never
// stopped. Neither is driftwatch's, and one of driftwatch's own appearing here
// would be a bug to fix rather than an entry to add.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// Reached transitively through pkg/check, which links the Redis client
		// in order to construct a target from a spec.
		goleak.IgnoreAnyFunction("github.com/redis/go-redis/v9/internal/pool.startGlobalTimeCache.func1"),
		goleak.IgnoreAnyFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).reaper"),
	)
}
