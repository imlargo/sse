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

	entries []Entry
	next    Offset
	evicted Offset
	bytes   int
	closed  bool

	// waiters is closed and replaced on every append. A reader takes the
	// current channel under the read lock and then waits on it, so a wake-up
	// costs no per-reader bookkeeping and composes with context cancellation.
	waiters chan struct{}
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
	return &MemoryLog{
		epoch:     newEpoch(),
		retention: r,
		resumable: resumable,
		waiters:   make(chan struct{}),
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

	// Wake every reader at once by closing the channel they are all waiting on.
	close(l.waiters)
	l.waiters = make(chan struct{})
	l.mu.Unlock()

	return off, nil
}

// trim evicts entries that fall outside the retention window. It must be called
// with the write lock held.
func (l *MemoryLog) trim(now time.Time) {
	r := l.retention
	drop := 0
	for drop < len(l.entries) {
		e := l.entries[drop]
		// An ephemeral entry ages out on the short in-flight window rather than
		// the configured one, so a high-frequency ticker cannot push out the
		// history the log is keeping for everything else. It is still evicted
		// from the head like anything else, which keeps the eviction watermark
		// — and therefore gap detection — exact.
		maxAge := r.For
		if e.Frame.Ephemeral && (maxAge == 0 || defaultWindow.For < maxAge) {
			maxAge = defaultWindow.For
		}
		over := (r.Events > 0 && len(l.entries)-drop > r.Events) ||
			(r.Bytes > 0 && l.bytes > r.Bytes) ||
			(maxAge > 0 && now.Sub(e.Frame.Time) > maxAge)
		if !over {
			break
		}
		l.bytes -= e.Frame.Size()
		l.evicted = e.Offset
		drop++
	}
	if drop > 0 {
		// Copy rather than reslice so the dropped frames become collectable
		// instead of being pinned by the backing array.
		l.entries = append(l.entries[:0:0], l.entries[drop:]...)
	}
}

func (l *MemoryLog) Read(ctx context.Context, after Offset) (Reader, error) {
	l.mu.RLock()
	closed, evicted := l.closed, l.evicted
	l.mu.RUnlock()
	if closed {
		return nil, ErrLogClosed
	}

	r := &memReader{log: l, pos: after, closed: make(chan struct{})}

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
	if len(l.entries) > 0 {
		info.Oldest = l.entries[0].Offset
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
	close(l.waiters)
	l.waiters = make(chan struct{})
	return nil
}

// memReader walks the log. It holds only a position, never a copy of any event:
// an extra subscriber costs one integer, not a queue (RF-D4).
type memReader struct {
	log    *MemoryLog
	pos    Offset
	gap    *Gap
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
		i, _ := slices.BinarySearchFunc(r.log.entries, r.pos, func(e Entry, pos Offset) int {
			switch {
			case e.Offset <= pos:
				return -1
			default:
				return 1
			}
		})
		if i < len(r.log.entries) {
			e := r.log.entries[i]
			r.pos = e.Offset
			r.log.mu.RUnlock()
			return e, nil
		}
		closed, wait := r.log.closed, r.log.waiters
		r.log.mu.RUnlock()

		if closed {
			return Entry{}, ErrLogClosed
		}
		select {
		case <-wait:
		case <-ctx.Done():
			return Entry{}, ctx.Err()
		case <-r.closed:
			return Entry{}, ErrLogClosed
		}
	}
}
