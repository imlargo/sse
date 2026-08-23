package sse

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// A Policy says what happens to a subscriber that reads more slowly than events
// are published.
//
// The set is closed. That is the point: this is the delivery path, and it is
// where the library's guarantees live. An open interface here would invite
// every user to reimplement the hard part, differently and worse — which is
// exactly the failure of the coarse-grained provider interfaces in the existing
// Go libraries. The flexibility is in configuring these, not in replacing them.
//
// Whatever the policy, a subscriber can never slow down the publisher or any
// other subscriber. Publishing appends to a log and returns; it never touches a
// subscriber at all. The only thing a policy decides is what happens to the one
// connection that has fallen behind.
type Policy int

const (
	// DropOldest discards the events a subscriber has not read yet, oldest
	// first, and tells it exactly which ones went. It is the default: it
	// degrades predictably, it cannot stall anything, and the client learns
	// what it missed instead of quietly receiving an incomplete stream.
	DropOldest Policy = iota

	// DropNewest keeps what is already queued and discards arriving events.
	// Useful when the first events of a burst matter more than the last.
	DropNewest

	// Coalesce keeps only the most recent event for each key, so a subscriber
	// that falls behind on state updates catches up to the current state
	// rather than replaying every intermediate value.
	//
	// This is what state synchronisation and dashboards actually need, and it
	// is the policy almost no library offers. Set the key with [Key]; events
	// with no key are queued normally. With the queue full of distinct keys
	// there is nothing to merge, so it falls back to dropping the oldest.
	Coalesce

	// Block makes the producer wait for room, bounded by
	// [Backpressure.BlockTimeout]. It bounds only this session: the publisher
	// is already finished and gone. On timeout the subscriber is disconnected.
	Block

	// Disconnect ends the session as soon as it falls behind, leaving the
	// client to reconnect and resume. It is only a sensible choice when
	// history is retained; otherwise the client silently loses whatever it
	// missed, and the library says so at startup.
	Disconnect
)

func (p Policy) String() string {
	switch p {
	case DropOldest:
		return "drop-oldest"
	case DropNewest:
		return "drop-newest"
	case Coalesce:
		return "coalesce"
	case Block:
		return "block"
	case Disconnect:
		return "disconnect"
	default:
		return fmt.Sprintf("policy(%d)", int(p))
	}
}

// Backpressure configures what a slow subscriber costs and what happens when it
// exceeds it.
//
// The limits are per subscription, so the memory a connection can hold is
// known and bounded (RF-D4). MaxBytes is the one that matters: tens of
// thousands of connections each allowed a few hundred events is gigabytes, and
// events are not all the same size.
type Backpressure struct {
	// Policy is what to do when a limit is reached. The zero value is
	// [DropOldest].
	Policy Policy

	// MaxEvents caps queued events. Zero means [DefaultQueueEvents].
	MaxEvents int

	// MaxBytes caps the queued bytes. Zero means [DefaultQueueBytes].
	MaxBytes int

	// BlockTimeout bounds how long [Block] waits for room. Zero means
	// [DefaultBlockTimeout].
	BlockTimeout time.Duration
}

// Defaults for a subscription's queue.
const (
	DefaultQueueEvents  = 64
	DefaultQueueBytes   = 1 << 20 // 1 MiB per subscription
	DefaultBlockTimeout = 5 * time.Second
)

func (b Backpressure) withDefaults() Backpressure {
	if b.MaxEvents <= 0 {
		b.MaxEvents = DefaultQueueEvents
	}
	if b.MaxBytes <= 0 {
		b.MaxBytes = DefaultQueueBytes
	}
	if b.BlockTimeout <= 0 {
		b.BlockTimeout = DefaultBlockTimeout
	}
	return b
}

// A queued frame waiting to be written.
type queuedFrame struct {
	buf    *[]byte
	key    string
	offset Offset // position in the log, or 0 for an event not from one
	topic  string
	// published is when the event entered the log, so delivery latency is
	// measured from publication rather than from when the writer got to it.
	published time.Time
}

// dropRecord accumulates what a subscriber lost, so it can be declared as one
// gap rather than a stream of individual notices.
type dropRecord struct {
	count int
	from  Offset
	to    Offset
}

// A sendQueue is the buffer between whatever produces events and the single
// goroutine that writes them to the connection.
//
// It holds encoded frames, never application values, so nothing is serialized
// per subscriber. Its size is what a slow subscriber costs.
type sendQueue struct {
	mu    sync.Mutex
	items []queuedFrame
	bytes int

	bp           Backpressure
	drops        dropRecord
	droppedTopic string
	closed       bool

	// signal wakes the writer; space wakes a producer blocked for room. Both
	// have capacity one and carry no data: they exist so the queue composes
	// with select, which a sync.Cond cannot do.
	signal chan struct{}
	space  chan struct{}
}

func newSendQueue(bp Backpressure) *sendQueue {
	return &sendQueue{
		bp:     bp.withDefaults(),
		signal: make(chan struct{}, 1),
		space:  make(chan struct{}, 1),
	}
}

func notify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// push adds a frame, applying the policy if the queue is already at its limit.
func (q *sendQueue) push(ctx context.Context, qf queuedFrame, done <-chan struct{}) error {
	size := len(*qf.buf)

	q.mu.Lock()
	for {
		if q.closed {
			q.mu.Unlock()
			putBuf(qf.buf)
			return ErrSessionClosed
		}

		// Coalescing first: superseding an unread event makes room without
		// anything being lost, which is the whole point of the policy.
		//
		// Found by scanning. The queue is bounded by MaxEvents, so this is a
		// short walk over contiguous memory — measured at roughly twice the
		// speed of the key-to-index map it replaced, which had to be rebuilt on
		// every pop and cost an allocation of its own.
		if q.bp.Policy == Coalesce && qf.key != "" {
			for i := range q.items {
				if q.items[i].key != qf.key {
					continue
				}
				q.bytes -= len(*q.items[i].buf)
				putBuf(q.items[i].buf)
				q.items[i] = qf
				q.bytes += size

				// A replacement can be larger than what it superseded, so the
				// budget has to be rechecked. Superseding is not a way around
				// the limit: a subscriber's memory has to stay bounded whatever
				// the policy (RF-D4).
				for q.bytes > q.bp.MaxBytes && len(q.items) > 1 {
					q.dropFrontLocked()
				}
				q.mu.Unlock()
				notify(q.signal)
				return nil
			}
		}

		if !q.overLimit(size) {
			q.items = append(q.items, qf)
			q.bytes += size
			q.mu.Unlock()
			notify(q.signal)
			return nil
		}

		switch q.bp.Policy {
		case DropNewest:
			// Keep what is queued; the arriving event is the casualty.
			q.recordDrop(qf.offset, qf.offset)
			q.droppedTopic = qf.topic
			q.mu.Unlock()
			putBuf(qf.buf)
			notify(q.signal)
			return nil

		case Disconnect:
			q.mu.Unlock()
			putBuf(qf.buf)
			return fmt.Errorf("%w: queue full at %d events / %d bytes",
				ErrSlowConsumer, q.bp.MaxEvents, q.bp.MaxBytes)

		case Block:
			q.mu.Unlock()
			timer := time.NewTimer(q.bp.BlockTimeout)
			select {
			case <-q.space:
				timer.Stop()
			case <-timer.C:
				putBuf(qf.buf)
				return fmt.Errorf("%w: no room after %v", ErrSlowConsumer, q.bp.BlockTimeout)
			case <-ctx.Done():
				timer.Stop()
				putBuf(qf.buf)
				return ctx.Err()
			case <-done:
				timer.Stop()
				putBuf(qf.buf)
				return ErrSessionClosed
			}
			q.mu.Lock()
			continue

		default: // DropOldest, and Coalesce once there is nothing left to merge
			if len(q.items) == 0 {
				// A single event larger than the whole budget. Refusing it is
				// the only option that keeps the limit meaningful.
				q.mu.Unlock()
				putBuf(qf.buf)
				return fmt.Errorf("%w: %d bytes exceeds the %d byte queue budget",
					ErrEventTooLarge, size, q.bp.MaxBytes)
			}
			q.dropFrontLocked()
		}
	}
}

func (q *sendQueue) overLimit(incoming int) bool {
	return len(q.items)+1 > q.bp.MaxEvents || q.bytes+incoming > q.bp.MaxBytes
}

// takeFrontLocked removes the oldest queued frame.
func (q *sendQueue) takeFrontLocked() queuedFrame {
	qf := q.items[0]
	q.bytes -= len(*qf.buf)
	// Clear the slot before dropping it: the backing array outlives the
	// reslice, and a stale pointer there keeps a buffer that has gone back to
	// the pool reachable.
	q.items[0] = queuedFrame{}
	q.items = q.items[1:]
	return qf
}

// dropFrontLocked discards the oldest queued event and remembers it.
func (q *sendQueue) dropFrontLocked() {
	old := q.takeFrontLocked()
	putBuf(old.buf)
	q.recordDrop(old.offset, old.offset)
}

// recordDrop widens the range of what this subscriber lost. Callers hold the
// lock.
func (q *sendQueue) recordDrop(from, to Offset) {
	q.drops.count++
	if q.drops.from == 0 || (from != 0 && from < q.drops.from) {
		q.drops.from = from
	}
	if to > q.drops.to {
		q.drops.to = to
	}
}

// pop removes the oldest queued frame.
func (q *sendQueue) pop() (queuedFrame, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return queuedFrame{}, false
	}
	qf := q.takeFrontLocked()
	notify(q.space)
	return qf, true
}

// takeDrops returns and clears what has been lost since the last call, so it
// can be declared to the client as a single gap.
func (q *sendQueue) takeDrops() (dropRecord, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.drops.count == 0 {
		return dropRecord{}, false
	}
	d := q.drops
	q.drops = dropRecord{}
	return d, true
}

// Depth reports the queued events and bytes, for metrics.
func (q *sendQueue) depth() (events, bytes int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items), q.bytes
}

func (q *sendQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	for _, it := range q.items {
		putBuf(it.buf)
	}
	q.items = nil
	q.bytes = 0
	notify(q.signal)
	notify(q.space)
}
