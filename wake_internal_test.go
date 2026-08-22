package sse

import (
	"strings"
	"testing"
)

func TestConcretePrefix(t *testing.T) {
	tests := []struct{ filter, want string }{
		{"user.4821.>", "user.4821"},
		{"user.4821.inbox", "user.4821.inbox"},
		{"user.*.inbox", "user"},
		{"user.*", "user"},
		{">", ""},
		{"*", ""},
		{"*.tickets", ""},
		{"tenant.acme.project.7.builds", "tenant.acme.project.7.builds"},
		{"tenant.acme.>", "tenant.acme"},
		{"a", "a"},
	}
	for _, tt := range tests {
		if got := concretePrefix(MustFilter(tt.filter)); got != tt.want {
			t.Errorf("concretePrefix(%q) = %q, want %q", tt.filter, got, tt.want)
		}
	}
}

func TestCommonTokenPrefix(t *testing.T) {
	tests := []struct{ a, b, want string }{
		{"tenant.acme.tickets", "tenant.acme.builds", "tenant.acme"},
		{"tenant.acme", "tenant.acme", "tenant.acme"},
		{"tenant.acme", "tenant.globex", "tenant"},
		{"tenant.acme", "other.thing", ""},
		{"a.b.c", "a.b", "a.b"},
		{"a.b", "a.b.c", "a.b"},
		{"", "a.b", ""},
		{"a.b", "", ""},
		// Must stop on a token boundary, never mid-token.
		{"user.48", "user.4821", "user"},
		{"userx.a", "user.a", ""},
	}
	for _, tt := range tests {
		if got := commonTokenPrefix(tt.a, tt.b); got != tt.want {
			t.Errorf("commonTokenPrefix(%q, %q) = %q, want %q", tt.a, tt.b, got, tt.want)
		}
	}
}

// The one invariant the whole scheme rests on.
//
// Over-waking a subscriber costs a wasted scheduler slot. Under-waking one
// costs a stream that silently stops delivering, which is far worse than the
// problem being solved. So: whenever a filter matches a topic, the bucket that
// filter waits on must be among the buckets that topic notifies.
func TestNeverUnderWakes(t *testing.T) {
	filters := []string{
		">", "*", "*.b", "a", "a.>", "a.*", "a.b", "a.b.>", "a.b.c", "a.*.c",
		"user.4821.>", "user.*.inbox", "tenant.acme.>", "tenant.*.tickets",
		"a.b.c.d.e.f.g.h",
	}
	topics := []string{
		"a", "a.b", "a.b.c", "a.b.c.d", "b", "b.c", "user.4821.inbox",
		"user.1.inbox", "tenant.acme.tickets", "tenant.globex.tickets",
		"a.b.c.d.e.f.g.h", "x.y.z",
	}

	for _, fs := range filters {
		f := MustFilter(fs)
		bucket := wakeShardFor([]Filter{f})

		for _, ts := range topics {
			topic := MustTopic(ts)
			if !f.Matches(topic) {
				continue
			}
			if !notifies(ts, bucket) {
				t.Errorf("filter %q matches topic %q but waits on bucket %d, which "+
					"publishing to %q does not notify: this subscriber would hang",
					fs, ts, bucket, ts)
			}
		}
	}
}

// Several filters must land on a bucket that every one of them would be woken
// on, which is why the common prefix is used and why no common prefix falls
// back to bucket zero.
func TestNeverUnderWakesWithSeveralFilters(t *testing.T) {
	sets := [][]string{
		{"tenant.acme.tickets", "tenant.acme.builds"},
		{"tenant.acme.>", "tenant.globex.>"},
		{"a.b.c", "x.y.z"},
		{"user.1.>", "user.1.inbox", "user.1.mentions"},
		{">", "a.b"},
		{"a.b", "a.b.c.d"},
	}
	topics := []string{
		"tenant.acme.tickets", "tenant.acme.builds", "tenant.globex.tickets",
		"a.b", "a.b.c", "a.b.c.d", "x.y.z", "user.1.inbox", "user.2.inbox",
	}

	for _, set := range sets {
		var fs []Filter
		for _, s := range set {
			fs = append(fs, MustFilter(s))
		}
		bucket := wakeShardFor(fs)

		for _, ts := range topics {
			topic := MustTopic(ts)
			matched := false
			for _, f := range fs {
				if f.Matches(topic) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
			if !notifies(ts, bucket) {
				t.Errorf("filters %v match %q but wait on bucket %d, which publishing "+
					"to %q does not notify: this subscriber would hang", set, ts, bucket, ts)
			}
		}
	}
}

// An unaddressed event reaches everyone, so it has to wake every bucket.
func TestUnaddressedEventsWakeEveryBucket(t *testing.T) {
	w := newWakeSet()
	// Register a waiter on a bucket no topic prefix would reach.
	idx := wakeShardFor([]Filter{MustFilter("deeply.nested.private.>")})
	if idx == 0 {
		t.Fatal("expected a non-zero bucket for this filter")
	}
	w.channel(idx)

	before := w.buckets[idx].ch
	w.wakeAll()
	if w.buckets[idx].ch == before {
		t.Error("wakeAll left a bucket untouched; an unaddressed event would not reach it")
	}
}

// notifies reports whether publishing to topic wakes the given bucket.
func notifies(topic string, bucket int) bool {
	if bucket == 0 {
		return true // every publish notifies bucket zero
	}
	end := 0
	rest := topic
	for rest != "" {
		tok, more, found := strings.Cut(rest, ".")
		end += len(tok)
		if shardFor(topic[:end]) == bucket {
			return true
		}
		if !found {
			break
		}
		end++
		rest = more
	}
	return false
}

// A publish must not allocate for buckets nobody is listening on, which is
// where most of the saving comes from.
func TestQuietBucketsAreNotDisturbed(t *testing.T) {
	w := newWakeSet()
	allocs := testing.AllocsPerRun(200, func() {
		w.notify("tenant.acme.project.7.builds")
	})
	if allocs > 0 {
		t.Errorf("notifying with no waiters allocates %.0f times per publish", allocs)
	}
}

// The invariant the whole wake-up scheme rests on, against generated input
// rather than a table somebody thought of.
//
// Over-waking wastes a scheduler slot. Under-waking means a subscriber that
// never receives anything again — silently, for the life of the connection —
// which is the worst failure this library has. A hand-written table only covers
// the shapes its author imagined; this covers the ones nobody did.
func FuzzNeverUnderWakes(f *testing.F) {
	seeds := [][2]string{
		{"a.b.c", "a.b.c"}, {"a.>", "a.b"}, {"a.*.c", "a.b.c"}, {">", "z"},
		{"*", "a"}, {"a.b.>", "a.b.c.d"}, {"tenant.acme.>", "tenant.acme.x"},
		{"user.4821.>", "user.4821.inbox"},
	}
	for _, s := range seeds {
		f.Add(s[0], s[1])
	}

	f.Fuzz(func(t *testing.T, filterStr, topicStr string) {
		filter, err := NewFilter(filterStr)
		if err != nil {
			t.Skip()
		}
		topic, err := NewTopic(topicStr)
		if err != nil {
			t.Skip()
		}
		if !filter.Matches(topic) {
			t.Skip() // over-waking is allowed; only matches must be woken
		}

		// The bucket the subscriber waits on has to be one this topic notifies.
		bucket := wakeShardFor([]Filter{filter})
		if !notifies(topicStr, bucket) {
			t.Fatalf("filter %q matches topic %q but waits on bucket %d, which "+
				"publishing to %q never notifies: this subscriber would stop "+
				"receiving anything, permanently and silently",
				filterStr, topicStr, bucket, topicStr)
		}

		// And the same with the filter paired with an unrelated one, since a
		// subscriber may hold several and the bucket is their common prefix.
		for _, other := range []string{">", "zzz.qqq", filterStr} {
			if o, err := NewFilter(other); err == nil {
				b2 := wakeShardFor([]Filter{filter, o})
				if !notifies(topicStr, b2) {
					t.Fatalf("filters %q + %q match %q but wait on bucket %d, "+
						"which publishing to %q never notifies",
						filterStr, other, topicStr, b2, topicStr)
				}
			}
		}
	})
}

// concretePrefix must always return a prefix that ends on a token boundary and
// is genuinely a prefix of the filter, since the bucket is derived from it.
func FuzzConcretePrefixIsATokenPrefix(f *testing.F) {
	for _, s := range []string{"a", "a.b", "a.>", "a.*.b", ">", "*", "a.b.c.d"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		filter, err := NewFilter(s)
		if err != nil {
			t.Skip()
		}
		got := concretePrefix(filter)
		if !strings.HasPrefix(s, got) {
			t.Fatalf("concretePrefix(%q) = %q, which is not a prefix of it", s, got)
		}
		if got != "" && got != s && s[len(got)] != '.' {
			t.Fatalf("concretePrefix(%q) = %q, which stops mid-token", s, got)
		}
		if strings.ContainsAny(got, "*>") {
			t.Fatalf("concretePrefix(%q) = %q, which contains a wildcard", s, got)
		}
	})
}
