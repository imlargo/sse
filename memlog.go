package sse

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"slices"
	"sync"
	"time"
)

// defaultWindow is the in-flight window a log keeps when no retention was
// configured. It exists to serve live readers that are momentarily behind, not
// to offer resumption, and a log using it reports Resumable false.
var defaultWindow = Retention{Events: 256, Bytes: 1 << 20, For: 30 * time.Second}

// A MemoryLog is an in-process [Log].
//
// It is the default, and it is what makes the single-node case work with no
// external dependency. Appending takes a write lock only long enough to add a
// slice element and bump a counter; readers never take it, so a slow subscriber
// cannot slow down a publisher or another subscriber.
//
// A MemoryLog is safe for concurrent use.
type MemoryLog struct {
	mu        sync.RWMutex
	epoch     Epoch
	retention Retention
	resumable bool

	// entries holds the retained window. The live entries are entries[head:];
	// evicting advances head rather than moving anything, so an append stays
	// constant-time however much history is being kept.
	entries []Entry
	head    int
	next    Offset
	evicted Offset
	bytes   int
	closed  bool

	// wake carries "something was appended", split into buckets by topic
	// prefix so a subscriber is only made runnable for events that could
	// plausibly be for it. A reader takes its bucket's channel under the read
	// lock and then waits on it, so a wake-up needs no per-reader bookkeeping
	// and composes with context cancellation.
	wake *wakeSet
}

// NewMemoryLog returns a log retaining history according to r.
//
// The zero Retention means no resumption is offered: the log keeps only the
// small window its live readers need, [LogInfo.Resumable] is false, and a
// client presenting a cursor is told plainly that history is not available. Not
// promising is better than half-promising (RF-C1).
func NewMemoryLog(r Retention) *MemoryLog {
	resumable := !r.IsZero()
	if !resumable {
		r = defaultWindow
	}
	// Give the window a head start so a busy log does not spend its first
	// thousand appends growing and copying. Capped, because a retention of a
	// million events should not cost a hundred megabytes before the first
	// publish.
	const presizeCap = 1024
	presize := r.Events
	if presize <= 0 || presize > presizeCap {
		presize = presizeCap
	}

	return &MemoryLog{
		epoch:     newEpoch(),
		retention: r,
		resumable: resumable,
		wake:      newWakeSet(),
		entries:   make([]Entry, 0, presize),
	}
}

// newEpoch generates a fresh generation identifier.
//
// It is random rather than sequential because the point is to be *different*
// after any loss of contents. A counter that reset with the process would
// collide with itself, which is the silent failure the epoch exists to stop.
func newEpoch() Epoch {
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		return Epoch(time.Now().UnixNano())
	}
	e := Epoch(binary.BigEndian.Uint64(b[:]))
	if e == 0 {
		e = 1 // zero reads as "no epoch"
	}
	return e
}

func (l *MemoryLog) Append(ctx context.Context, f Frame) (Offset, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if f.Time.IsZero() {
		f.Time = time.Now()
	}

	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return 0, ErrLogClosed
	}

	l.next++
	off := l.next

	l.entries = append(l.entries, Entry{Offset: off, Frame: f})
	l.bytes += f.Size()
	l.trim(f.Time)

	// Wake only the buckets that could hold a subscriber for this topic. An
	// unaddressed event has no prefix to narrow by and wakes everyone, which is
	// the single-log case and already optimal.
	if f.Topic == "" {
		l.wake.wakeAll()
	} else {
		l.wake.notify(f.Topic)
	}
	l.mu.Unlock()

	return off, nil
}

// trim evicts entries that fall outside the retention window. It must be called
// with the write lock held.
func (l *MemoryLog) trim(now time.Time) {
	r := l.retention
	live := l.live()
	drop := 0
	for drop < len(live) {
		e := live[drop]
		// An ephemeral entry ages out on the short in-flight window rather than
		// the configured one, so a high-frequency ticker cannot push out the
		// history the log is keeping for everything else. It is still evicted
		// from the head like anything else, which keeps the eviction watermark
		// — and therefore gap detection — exact.
		maxAge := r.For
		if e.Frame.Ephemeral && (maxAge == 0 || defaultWindow.For < maxAge) {
			maxAge = defaultWindow.For
		}
		over := (r.Events > 0 && len(live)-drop > r.Events) ||
			(r.Bytes > 0 && l.bytes > r.Bytes) ||
			(maxAge > 0 && now.Sub(e.Frame.Time) > maxAge)
		if !over {
			break
		}
		l.bytes -= e.Frame.Size()
		l.evicted = e.Offset
		drop++
	}
	if drop == 0 {
		return
	}

	// Release the frames so they can be collected. Advancing head alone would
	// leave the backing array holding them for as long as the log lives, which
	// for a log retaining tens of thousands of events is most of the memory.
	for i := l.head; i < l.head+drop; i++ {
		l.entries[i] = Entry{}
	}
	l.head += drop

	// Reclaim the dead prefix once it is worth the copy. Copying on every
	// eviction instead — which is what rebuilding the slice amounts to — makes
	// an append cost O(retained), so a log holding ten thousand events spends a
	// megabyte and most of a millisecond on every single publish.
	if l.head >= 64 && l.head*2 >= len(l.entries) {
		live := copy(l.entries, l.entries[l.head:])
		for i := live; i < len(l.entries); i++ {
			l.entries[i] = Entry{}
		}
		l.entries = l.entries[:live]
		l.head = 0
	}
}

// live returns the entries still retained. Callers hold at least a read lock.
func (l *MemoryLog) live() []Entry { return l.entries[l.head:] }

func (l *MemoryLog) Read(ctx context.Context, after Offset, opts ReadOptions) (Reader, error) {
	l.mu.RLock()
	closed, evicted := l.closed, l.evicted
	l.mu.RUnlock()
	if closed {
		return nil, ErrLogClosed
	}

	r := &memReader{
		log:    l,
		pos:    after,
		shard:  wakeShardFor(opts.Filters),
		closed: make(chan struct{}),
	}

	// A gap is decided here, before a single event is replayed, so it can be
	// declared to the client ahead of everything else (RF-C4).
	if after > 0 && after < evicted {
		r.gap = &Gap{Reason: GapRetention, From: after, Through: evicted}
	}
	return r, nil
}

func (l *MemoryLog) Info(ctx context.Context) (LogInfo, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	info := LogInfo{
		Epoch:          l.epoch,
		Newest:         l.next,
		EvictedThrough: l.evicted,
		Resumable:      l.resumable,
		Retention:      l.retention,
	}
	if live := l.live(); len(live) > 0 {
		info.Oldest = live[0].Offset
	}
	return info, nil
}

// Close releases the log and unblocks every reader.
func (l *MemoryLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	l.wake.wakeAll()
	return nil
}

// memReader walks the log. It holds only a position, never a copy of any event:
// an extra subscriber costs one integer, not a queue (RF-D4).
type memReader struct {
	log *MemoryLog
	pos Offset
	gap *Gap
	// shard is the wake bucket this reader listens on, chosen once from its
	// filters. Filters do not change during a subscription.
	shard  int
	closed chan struct{}
	once   sync.Once
}

func (r *memReader) Gap() *Gap { return r.gap }

func (r *memReader) Close() error {
	r.once.Do(func() { close(r.closed) })
	return nil
}

func (r *memReader) Next(ctx context.Context) (Entry, error) {
	for {
		r.log.mu.RLock()
		// Entries are sorted by offset, so the next one to deliver is found by
		// binary search: a reader sitting at the tail of a long history costs
		// a lookup, not a scan (RNF-2).
		//
		// There is no separate "replay" and "live" mode here. The position
		// simply advances, which is why the handover between them cannot race
		// (RF-C6) — there is no handover.
		live := r.log.live()
		i, _ := slices.BinarySearchFunc(live, r.pos, func(e Entry, pos Offset) int {
			switch {
			case e.Offset <= pos:
				return -1
			default:
				return 1
			}
		})
		if i < len(live) {
			e := live[i]
			r.pos = e.Offset
			r.log.mu.RUnlock()
			return e, nil
		}
		closed := r.log.closed
		// Registering interest while still holding the read lock is what orders
		// it against a concurrent append: a publisher taking the write lock
		// either sees this reader waiting, or has already added the entry that
		// the search above will find on the next pass.
		wait := r.log.wake.channel(r.shard)
		r.log.mu.RUnlock()

		if closed {
			r.log.wake.done(r.shard)
			return Entry{}, ErrLogClosed
		}
		select {
		case <-wait:
			r.log.wake.done(r.shard)
		case <-ctx.Done():
			r.log.wake.done(r.shard)
			return Entry{}, ctx.Err()
		case <-r.closed:
			r.log.wake.done(r.shard)
			return Entry{}, ErrLogClosed
		}
	}
}
