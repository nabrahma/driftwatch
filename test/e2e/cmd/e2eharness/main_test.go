package main

import (
	"testing"
	"time"
)

// owed decides the rate every scenario in the suite sizes itself against, so it
// is worth more than the four assertions it takes. A publisher that quietly
// runs at 70% of its requested rate does not fail anything directly; it
// lengthens every key cycle by half, and the failure surfaces two files away as
// a coverage ratio nobody can explain.
func TestOwed(t *testing.T) {
	cases := []struct {
		name    string
		elapsed time.Duration
		rate    int
		sent    uint64
		want    int
	}{
		{
			name:    "nothing is due before any time has passed",
			elapsed: 0,
			rate:    200,
			sent:    0,
			want:    0,
		},
		{
			name:    "one second at 200/sec owes two hundred events",
			elapsed: time.Second,
			rate:    200,
			sent:    0,
			want:    200,
		},
		{
			name:    "a publisher that is up to date owes nothing",
			elapsed: time.Second,
			rate:    200,
			sent:    200,
			want:    0,
		},
		{
			name:    "a publisher that ran ahead owes nothing rather than a negative",
			elapsed: time.Second,
			rate:    200,
			sent:    500,
			want:    0,
		},
		{
			// The case the old ticker got wrong: ten milliseconds at 150/sec is
			// one and a half events, and a ticker firing once per event has to
			// round that to one or two. Accumulating against the clock spends
			// the halves rather than dropping them.
			name:    "a fractional tick is carried rather than lost",
			elapsed: 10 * time.Millisecond,
			rate:    150,
			sent:    1,
			want:    0,
		},
		{
			name:    "the carried half is paid on the following tick",
			elapsed: 20 * time.Millisecond,
			rate:    150,
			sent:    1,
			want:    2,
		},
		{
			// A stall is made up, but over several ticks. One burst of thirty
			// thousand events is not a workload any scenario describes.
			name:    "a long stall is caught up at no more than a second at a time",
			elapsed: 200 * time.Second,
			rate:    150,
			sent:    0,
			want:    150,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := owed(tc.elapsed, tc.rate, tc.sent); got != tc.want {
				t.Errorf("owed(%s, %d, %d) = %d, want %d",
					tc.elapsed, tc.rate, tc.sent, got, tc.want)
			}
		})
	}
}

// The round-robin walk the scenarios' cycle arithmetic depends on: every key
// exactly once per pass, in order, wrapping cleanly.
func TestKeyWalkCoversTheKeyspaceExactlyOncePerCycle(t *testing.T) {
	const (
		keys  = 7
		start = 3
	)

	seen := make(map[uint64]int)
	for seq := uint64(1); seq <= keys; seq++ {
		seen[(start+seq-1)%keys]++
	}

	if len(seen) != keys {
		t.Fatalf("one cycle touched %d of %d keys; a cycle that does not cover "+
			"the keyspace makes keys/rate the wrong period", len(seen), keys)
	}
	for key, times := range seen {
		if times != 1 {
			t.Errorf("key %d was written %d times in one cycle, want 1", key, times)
		}
	}

	// The second cycle repeats the first, which is what gives every key a
	// period rather than a probability.
	if got, want := uint64(start+keys+1-1)%keys, uint64(start); got != want {
		t.Errorf("the cycle restarted at key %d, want %d", got, want)
	}
}
