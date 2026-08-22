package sse

import (
	"fmt"
	"strings"
)

// Limits on a topic, so a value taken from a request cannot degrade the
// matcher (RF-B6). The numbers follow NATS's own guidance, which is where the
// vocabulary comes from.
const (
	MaxTopicTokens = 16
	MaxTopicLength = 256
)

// A Topic is the address an event is published to.
//
// Topics are dot-separated tokens of letters, digits, hyphens and underscores:
//
//	org.42.tickets
//	tenant.acme.project.7.builds
//
// The grammar is deliberately the one used by NATS subjects and, with a
// different separator, by MQTT topics. Two reasons. It is vocabulary most
// people already have, and — the decisive one — it maps directly onto the
// subscription mechanisms of the brokers this library is meant to sit on top
// of, so a node can be given only the traffic it needs instead of receiving
// everything and discarding most of it.
//
// The character set is also safe unescaped in a URL query, which matters
// because EventSource cannot send headers, so topics travel in the query
// string. In particular it excludes '#', which a browser treats as the start of
// a fragment and silently truncates (RF-B7).
//
// The zero Topic is invalid; build one with [NewTopic] or [MustTopic].
type Topic struct{ s string }

// NewTopic validates s and returns it as a Topic.
func NewTopic(s string) (Topic, error) {
	if err := validateTopicish(s, false); err != nil {
		return Topic{}, err
	}
	return Topic{s: s}, nil
}

// MustTopic is [NewTopic] for values known at compile time. It panics on an
// invalid value, so it must never be used with input from a request.
func MustTopic(s string) Topic {
	t, err := NewTopic(s)
	if err != nil {
		panic(err)
	}
	return t
}

// String returns the topic.
func (t Topic) String() string { return t.s }

// IsZero reports whether the topic was never built.
func (t Topic) IsZero() bool { return t.s == "" }

// A Filter is what a subscriber declares it wants.
//
// It is a topic pattern with two wildcards:
//
//	org.42.tickets     exactly that topic
//	org.42.*           any one token there: org.42.tickets, not org.42.a.b
//	org.42.>           one or more tokens: org.42.tickets and org.42.a.b
//
// '>' may only appear as the final token, which is what keeps matching linear
// in the number of tokens with no backtracking.
//
// Wildcards are for subscribing only. Publishing always names one concrete
// topic, so the set of subscribers an event reaches is a property of who is
// listening and never of how it was addressed. It is also the rule every
// message broker follows, which is what keeps the mapping onto them exact.
type Filter struct {
	s      string
	tokens []string
}

// NewFilter validates s and returns it as a Filter.
func NewFilter(s string) (Filter, error) {
	if err := validateTopicish(s, true); err != nil {
		return Filter{}, err
	}
	return Filter{s: s, tokens: strings.Split(s, ".")}, nil
}

// MustFilter is [NewFilter] for values known at compile time.
func MustFilter(s string) Filter {
	f, err := NewFilter(s)
	if err != nil {
		panic(err)
	}
	return f
}

// String returns the filter.
func (f Filter) String() string { return f.s }

// IsZero reports whether the filter was never built.
func (f Filter) IsZero() bool { return f.s == "" }

// Matches reports whether the filter selects the topic.
//
// It walks both in step, one token at a time, and allocates nothing: the filter
// keeps its tokens from construction and the topic is cut in place. There is no
// regular expression and no backtracking, because '>' can only be last.
func (f Filter) Matches(t Topic) bool {
	rest := t.s
	for i, want := range f.tokens {
		if want == ">" {
			// Matches one or more remaining tokens, so there must be something
			// left to match.
			return rest != ""
		}
		if rest == "" {
			return false
		}
		got, more, _ := strings.Cut(rest, ".")
		if want != "*" && want != got {
			return false
		}
		rest = more
		if i == len(f.tokens)-1 {
			// The filter is exhausted; the topic must be too.
			return rest == ""
		}
	}
	return rest == ""
}

// validateTopicish checks the shared grammar. Errors say what was wrong and
// what the rule is, because a topic often comes from a request and the person
// debugging it is looking at a rejection, not at the source (RF-G4).
func validateTopicish(s string, allowWildcards bool) error {
	kind := "topic"
	if allowWildcards {
		kind = "filter"
	}
	if s == "" {
		return fmt.Errorf("sse: %s must not be empty", kind)
	}
	if len(s) > MaxTopicLength {
		return fmt.Errorf("sse: %s is %d bytes, over the %d byte limit; "+
			"a bound is what keeps a value taken from a request from degrading the matcher",
			kind, len(s), MaxTopicLength)
	}

	tokens := strings.Split(s, ".")
	if len(tokens) > MaxTopicTokens {
		return fmt.Errorf("sse: %s has %d tokens, over the %d token limit",
			kind, len(tokens), MaxTopicTokens)
	}

	for i, tok := range tokens {
		if tok == "" {
			return fmt.Errorf("sse: %s %q has an empty token; "+
				"tokens are separated by single dots and none may be blank", kind, s)
		}
		if tok == ">" {
			if !allowWildcards {
				return fmt.Errorf("sse: topic %q contains the wildcard %q; "+
					"publishing always names one concrete topic, so the set of subscribers "+
					"an event reaches never depends on how it was addressed", s, tok)
			}
			if i != len(tokens)-1 {
				return fmt.Errorf("sse: filter %q has %q before the end; "+
					"it matches one or more trailing tokens and so may only be the last token",
					s, tok)
			}
			continue
		}
		if tok == "*" {
			if !allowWildcards {
				return fmt.Errorf("sse: topic %q contains the wildcard %q; "+
					"publishing always names one concrete topic", s, tok)
			}
			continue
		}
		if err := validateToken(kind, s, tok); err != nil {
			return err
		}
	}
	return nil
}

func validateToken(kind, whole, tok string) error {
	for _, r := range tok {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '-', r == '_':
		default:
			return fmt.Errorf("sse: %s %q contains %q, which is not allowed; "+
				"tokens use letters, digits, '-' and '_' only, so a topic is safe unescaped "+
				"in a URL query — EventSource cannot send headers, so topics travel there",
				kind, whole, r)
		}
	}
	return nil
}
