package sse_test

import (
	"net/url"
	"strings"
	"testing"

	"github.com/imlargo/sse"
)

func TestFilterMatching(t *testing.T) {
	tests := []struct {
		filter string
		topic  string
		want   bool
	}{
		// exact
		{"org.42.tickets", "org.42.tickets", true},
		{"org.42.tickets", "org.42.builds", false},
		{"org.42.tickets", "org.42", false},
		{"org.42.tickets", "org.42.tickets.new", false},

		// single-token wildcard matches exactly one token
		{"org.*.tickets", "org.42.tickets", true},
		{"org.*.tickets", "org.7.tickets", true},
		{"org.*.tickets", "org.tickets", false},
		{"org.*.tickets", "org.a.b.tickets", false},
		{"*", "org", true},
		{"*", "org.42", false},
		{"*.*", "org.42", true},

		// tail wildcard matches one or more
		{"org.42.>", "org.42.tickets", true},
		{"org.42.>", "org.42.a.b.c", true},
		{"org.42.>", "org.42", false}, // one or more, not zero
		{"org.42.>", "org.43.tickets", false},
		{">", "anything", true},
		{">", "a.b.c.d", true},

		// combined
		{"tenant.*.project.>", "tenant.acme.project.7.builds", true},
		{"tenant.*.project.>", "tenant.acme.project", false},
		{"tenant.*.project.>", "tenant.acme.other.7", false},

		// a longer topic than the filter
		{"org", "org.42", false},
		{"org.42", "org", false},
	}

	for _, tt := range tests {
		t.Run(tt.filter+"|"+tt.topic, func(t *testing.T) {
			f := sse.MustFilter(tt.filter)
			topic := sse.MustTopic(tt.topic)
			if got := f.Matches(topic); got != tt.want {
				t.Errorf("MustFilter(%q).Matches(%q) = %v, want %v", tt.filter, tt.topic, got, tt.want)
			}
		})
	}
}

// The matcher runs once per subscriber per event, so it must not allocate.
func TestFilterMatchingDoesNotAllocate(t *testing.T) {
	f := sse.MustFilter("tenant.*.project.>")
	hit := sse.MustTopic("tenant.acme.project.7.builds")
	miss := sse.MustTopic("tenant.acme.other.7.builds")

	allocs := testing.AllocsPerRun(500, func() {
		_ = f.Matches(hit)
		_ = f.Matches(miss)
	})
	if allocs > 0 {
		t.Errorf("Matches allocates %.0f times per call; it is on the delivery path", allocs)
	}
}

// D-01: wildcards are for subscribing. Publishing names one concrete topic, so
// which subscribers an event reaches is a property of who is listening and
// never of how it was addressed.
func TestPublishingRejectsWildcards(t *testing.T) {
	for _, s := range []string{"org.*", "org.>", "*", ">", "org.*.tickets"} {
		if _, err := sse.NewTopic(s); err == nil {
			t.Errorf("NewTopic(%q) was accepted; wildcards belong to filters only", s)
		} else if !strings.Contains(err.Error(), "concrete topic") {
			t.Errorf("NewTopic(%q) error does not explain the rule: %v", s, err)
		}
		if _, err := sse.NewFilter(s); err != nil {
			t.Errorf("NewFilter(%q) was rejected: %v", s, err)
		}
	}
}

// '>' matches a variable number of trailing tokens, so allowing it in the
// middle would require backtracking. Keeping it last is what makes matching
// linear.
func TestTailWildcardMustBeLast(t *testing.T) {
	if _, err := sse.NewFilter("org.>.tickets"); err == nil {
		t.Fatal("a mid-pattern tail wildcard was accepted")
	} else if !strings.Contains(err.Error(), "last token") {
		t.Errorf("error does not explain the rule: %v", err)
	}
	if _, err := sse.NewFilter("org.>"); err != nil {
		t.Errorf("a trailing tail wildcard was rejected: %v", err)
	}
}

// RF-B6: a value that arrived in a request must not be able to degrade the
// matcher.
func TestTopicLimits(t *testing.T) {
	tests := []struct{ name, value string }{
		{"empty", ""},
		{"empty token", "org..tickets"},
		{"leading dot", ".org"},
		{"trailing dot", "org."},
		{"too long", strings.Repeat("a", sse.MaxTopicLength+1)},
		{"too many tokens", strings.TrimSuffix(strings.Repeat("a.", sse.MaxTopicTokens+2), ".")},
		{"space", "org 42"},
		{"slash", "org/42"},
		{"fragment marker", "org#42"},
		{"percent", "org%42"},
		{"colon", "org:42"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := sse.NewTopic(tt.value); err == nil {
				t.Errorf("NewTopic(%q) was accepted", tt.value)
			}
			if _, err := sse.NewFilter(tt.value); err == nil {
				t.Errorf("NewFilter(%q) was accepted", tt.value)
			}
		})
	}
}

// RF-B7: EventSource cannot send headers, so topics travel in the query string.
// They must survive that unescaped, and must never contain '#', which a browser
// treats as the start of a fragment and truncates.
func TestTopicsSurviveAQueryString(t *testing.T) {
	topics := []string{
		"org.42.tickets",
		"tenant.acme-corp.project_7.builds",
		"a-b_c.D-E_F.123",
	}
	for _, s := range topics {
		topic := sse.MustTopic(s)
		if got := url.QueryEscape(topic.String()); got != topic.String() {
			t.Errorf("topic %q needs escaping in a query (%q)", s, got)
		}
		u, err := url.Parse("http://x/stream?topic=" + topic.String())
		if err != nil {
			t.Fatalf("topic %q does not survive a URL: %v", s, err)
		}
		if got := u.Query().Get("topic"); got != s {
			t.Errorf("topic came back as %q, want %q", got, s)
		}
	}

	// Filters travel there too. '>' percent-encodes, which every client does
	// automatically, and it round-trips.
	f := sse.MustFilter("org.42.>")
	u, err := url.Parse("http://x/stream?topic=" + url.QueryEscape(f.String()))
	if err != nil {
		t.Fatal(err)
	}
	if got := u.Query().Get("topic"); got != f.String() {
		t.Errorf("filter came back as %q, want %q", got, f.String())
	}
}

// The multi-tenant shape of RF-B3: a subscription that covers a set of topics,
// not just one.
func TestMultiTenantSegmentation(t *testing.T) {
	acme := sse.MustFilter("tenant.acme.>")
	other := sse.MustTopic("tenant.globex.project.1.builds")
	mine := sse.MustTopic("tenant.acme.project.1.builds")

	if !acme.Matches(mine) {
		t.Error("a tenant filter did not match its own topic")
	}
	if acme.Matches(other) {
		t.Error("a tenant filter matched another tenant's topic")
	}
}

func BenchmarkFilterMatches(b *testing.B) {
	cases := []struct{ name, filter, topic string }{
		{"exact-hit", "org.42.tickets", "org.42.tickets"},
		{"exact-miss", "org.42.tickets", "org.43.tickets"},
		{"single-wildcard", "org.*.tickets", "org.42.tickets"},
		{"tail-wildcard", "tenant.acme.>", "tenant.acme.project.7.builds"},
		{"deep-miss", "tenant.acme.>", "tenant.globex.project.7.builds"},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			f := sse.MustFilter(c.filter)
			t := sse.MustTopic(c.topic)
			b.ReportAllocs()
			for b.Loop() {
				_ = f.Matches(t)
			}
		})
	}
}

// Validation must never accept something the matcher then has to cope with, and
// never panic on input that arrived in a query string.
func FuzzTopicValidation(f *testing.F) {
	for _, s := range []string{
		"", "a", "a.b", "org.42.tickets", "a..b", ".a", "a.", "*", ">", "a.*.b",
		"a.>.b", "a#b", "a b", "a/b", "a%b", "\x00", "ñ", strings.Repeat("a.", 40),
	} {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, s string) {
		topic, topicErr := sse.NewTopic(s)
		filter, filterErr := sse.NewFilter(s)

		// Anything a topic accepts a filter must accept: a filter is a topic
		// plus wildcards, so the grammar can only widen.
		if topicErr == nil && filterErr != nil {
			t.Fatalf("%q is a valid topic but not a valid filter: %v", s, filterErr)
		}

		if topicErr == nil {
			if topic.String() != s {
				t.Fatalf("topic round-trip changed %q into %q", s, topic.String())
			}
			if len(s) > sse.MaxTopicLength {
				t.Fatalf("%q is %d bytes and was accepted", s, len(s))
			}
			// A valid topic must match itself, and the universal filter.
			exact, err := sse.NewFilter(s)
			if err != nil {
				t.Fatalf("a valid topic is not a valid exact filter: %v", err)
			}
			if !exact.Matches(topic) {
				t.Fatalf("topic %q does not match itself", s)
			}
			if !sse.MustFilter(">").Matches(topic) {
				t.Fatalf("the universal filter does not match %q", s)
			}
		}

		if filterErr == nil {
			if filter.String() != s {
				t.Fatalf("filter round-trip changed %q into %q", s, filter.String())
			}
			// Matching must not panic against anything, valid or not.
			for _, other := range []string{"a", "a.b", "a.b.c", "x.y.z", s} {
				if ot, err := sse.NewTopic(other); err == nil {
					_ = filter.Matches(ot)
				}
			}
		}
	})
}
