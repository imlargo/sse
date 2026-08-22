package sse

import (
	"strings"
	"sync/atomic"
)

// wakeShards is how many buckets subscriber wake-ups are spread across.
//
// The number only has to be large enough that unrelated topics rarely share a
// bucket. A collision costs a spurious wake-up, never a missed one, so being
// wrong here is a performance question and not a correctness one.
const wakeShards = 256

// A wakeSet spreads "something was appended" notifications across buckets keyed
// by topic prefix, so a subscriber is only made runnable for events that could
// plausibly be for it.
//
// The problem it solves: with a single notification channel, appending one
// event makes every subscriber on the node runnable, each of which then wakes,
// takes a read lock, searches, applies its filter and goes back to sleep. Topic
// filtering saves socket writes but no scheduler work, so a notification for one
// user costs exactly as much as a broadcast to everybody. At tens of thousands
// of connections that is the whole capacity budget of the node.
//
// What it deliberately does not do is introduce a subscriber registry on the
// publish path. These are a fixed number of channels; the publisher still never
// touches a subscriber, and a slow subscriber still cannot affect anyone.
//
// Index 0 is the bucket for the empty prefix. Every publish notifies it, and
// every subscriber whose filter begins with a wildcard waits on it, so those
// subscribers keep the old behaviour: correct, and no worse than before.
type wakeSet struct {
	buckets [wakeShards + 1]wakeBucket
}

type wakeBucket struct {
	ch chan struct{}
	// waiting lets a publish skip a bucket nobody is listening on, which is the
	// common case and where most of the saving comes from: allocating a
	// replacement channel for an empty bucket is pure waste.
	waiting atomic.Int32
	// Pad to keep neighbouring buckets off the same cache line, since a publish
	// touches several in a row while readers hold their own.
	_ [40]byte
}

func newWakeSet() *wakeSet {
	w := &wakeSet{}
	for i := range w.buckets {
		w.buckets[i].ch = make(chan struct{})
	}
	return w
}

// channel returns the bucket a reader waits on, and registers its interest. The
// caller must hold at least a read lock on the log, which is what orders the
// registration against a concurrent publish.
func (w *wakeSet) channel(idx int) chan struct{} {
	b := &w.buckets[idx]
	b.waiting.Add(1)
	return b.ch
}

// done deregisters a reader that has stopped waiting.
func (w *wakeSet) done(idx int) { w.buckets[idx].waiting.Add(-1) }

// notify wakes the buckets for every prefix of topic. Callers hold the write
// lock.
//
// A subscriber's bucket is keyed on a prefix of its filter, so if the event
// matches that filter, that prefix is also a prefix of this topic and its
// bucket is among the ones woken here. Over-waking is possible; under-waking is
// not.
func (w *wakeSet) notify(topic string) {
	w.wake(0) // the empty prefix: wildcard subscribers, and the no-topic case

	rest := topic
	end := 0
	for rest != "" {
		tok, more, found := strings.Cut(rest, ".")
		end += len(tok)
		w.wake(shardFor(topic[:end]))
		if !found {
			break
		}
		end++ // the dot
		rest = more
	}
}

// wakeAll notifies every bucket, for events that are not addressed and for
// shutdown.
func (w *wakeSet) wakeAll() {
	for i := range w.buckets {
		w.wake(i)
	}
}

func (w *wakeSet) wake(idx int) {
	b := &w.buckets[idx]
	if b.waiting.Load() == 0 {
		// Nobody is listening. Skipping the close means skipping the
		// replacement allocation too, which is what keeps a publish cheap when
		// most buckets are idle.
		return
	}
	close(b.ch)
	b.ch = make(chan struct{})
}

// shardFor maps a topic prefix to a bucket. The empty prefix is always bucket 0.
func shardFor(prefix string) int {
	if prefix == "" {
		return 0
	}
	// FNV-1a inline: this runs once per publish per prefix, and allocating a
	// hash object for it would show up.
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(prefix); i++ {
		h ^= uint32(prefix[i])
		h *= prime32
	}
	return 1 + int(h%wakeShards)
}

// concretePrefix returns the leading tokens of a filter before its first
// wildcard, which is the most specific thing that can be said about every topic
// the filter matches.
//
//	user.4821.>    -> user.4821
//	user.*.inbox   -> user
//	tenant.acme.x  -> tenant.acme.x
//	>              -> ""
func concretePrefix(f Filter) string {
	end := 0
	for i, tok := range f.tokens {
		if tok == "*" || tok == ">" {
			break
		}
		if i > 0 {
			end++ // the separating dot, counted only once the token is known concrete
		}
		end += len(tok)
	}
	return f.s[:end]
}

// wakeShardFor picks the bucket a subscriber with these filters should wait on.
//
// With several filters it uses their longest common token prefix, so a
// subscriber to two topics under one tenant still lands on that tenant's
// bucket. When they have nothing in common it falls back to bucket 0, which is
// the old behaviour: woken by everything, which is correct and merely
// unoptimised.
func wakeShardFor(filters []Filter) int {
	if len(filters) == 0 {
		return 0
	}
	common := concretePrefix(filters[0])
	for _, f := range filters[1:] {
		common = commonTokenPrefix(common, concretePrefix(f))
		if common == "" {
			return 0
		}
	}
	return shardFor(common)
}

// commonTokenPrefix returns the longest prefix of a and b that ends on a token
// boundary. It never returns a partial token, so "user.48" and "user.4821"
// share "user" and not "user.48".
func commonTokenPrefix(a, b string) string {
	end, i := 0, 0
	for i <= len(a) && i <= len(b) {
		ta, _, _ := strings.Cut(a[i:], ".")
		tb, _, _ := strings.Cut(b[i:], ".")
		if ta == "" || ta != tb {
			break
		}
		next := i + len(ta)
		end = next
		if next == len(a) || next == len(b) {
			break
		}
		i = next + 1
	}
	return a[:end]
}
