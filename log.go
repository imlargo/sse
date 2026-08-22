package sse

import (
	"context"
	"errors"
	"time"
)

// A Log is an ordered, append-only sequence of encoded events addressable by
// offset. It is the only piece that has to be replaced to go from one node to
// many.
//
// The interface is four operations on purpose. Redis Streams, NATS JetStream
// and Kafka already provide all of them natively, so an integration is a small
// adapter — and it inherits the subscriber registry, topic matching, per-
// subscriber queues, backpressure, replay, gap detection and metrics, because
// all of that lives above this seam and the implementer never touches it.
//
// That is the inversion that matters. A coarser seam forces every integration
// to reimplement exactly the parts where a library's quality lives, and each
// one does it differently and worse.
//
// Implementations must be safe for concurrent use.
type Log interface {
	// Append stores a frame and returns the offset assigned to it. Offsets
	// must increase strictly and must never be reused. They need not be dense:
	// holes are allowed, so an implementation may use its backend's own
	// numbering.
	Append(ctx context.Context, f Frame) (Offset, error)

	// Read returns a reader positioned strictly after the given offset, which
	// follows the tail as new entries arrive. Offset 0 means "everything still
	// retained".
	//
	// If entries after that offset have already been evicted, the reader
	// reports it through [Reader.Gap] rather than quietly starting later.
	Read(ctx context.Context, after Offset) (Reader, error)

	// Info reports the log's generation and the range it currently holds.
	Info(ctx context.Context) (LogInfo, error)
}

// A Reader walks a log from a position, following the tail.
//
// A reader is used by exactly one goroutine.
type Reader interface {
	// Gap describes history that was lost before this reader's position, or
	// nil if nothing was. It is valid immediately, before the first Next, so a
	// gap can be declared to the client ahead of any replayed event.
	Gap() *Gap

	// Next returns the next entry, blocking until one exists or ctx is done.
	Next(ctx context.Context) (Entry, error)

	// Close releases the reader. It is safe to call more than once.
	Close() error
}

// An Entry is one stored event and its position.
type Entry struct {
	Offset Offset
	Frame  Frame
}

// A Frame is an event encoded once, ready to be written to any number of
// subscribers.
//
// Body is a complete event block *without* its id field, because the id encodes
// the offset and the offset is only assigned by the append. Prepending an id
// line yields exactly what encoding the event with that id would have produced,
// which is what keeps fan-out to a single encoding (RNF-1).
type Frame struct {
	// Body is the encoded event block, terminated by a blank line, carrying no
	// id field. It must be treated as immutable once appended.
	Body []byte

	// Name is the event type, kept for observability and for retention
	// policies that distinguish event kinds.
	Name string

	// Key groups events that supersede one another, for the coalescing
	// backpressure policy. Empty means the event stands alone.
	Key string

	// Ephemeral marks an event that should be delivered live but never
	// retained. It is what makes a high-frequency ticker usable inside a log
	// that otherwise keeps a long history.
	Ephemeral bool

	// Time is when the event was published, for age-based retention. Zero
	// means the log stamps it on append.
	Time time.Time
}

// Size reports the frame's contribution to a log's byte budget.
func (f Frame) Size() int { return len(f.Body) + len(f.Name) + len(f.Key) }

// LogInfo describes a log's current state.
type LogInfo struct {
	// Epoch identifies this generation of the contents. A cursor carrying a
	// different epoch refers to events that no longer exist.
	Epoch Epoch

	// Oldest is the offset of the oldest entry still retained, or 0 when the
	// log is empty.
	Oldest Offset

	// Newest is the highest offset assigned so far, or 0 when the log is empty.
	Newest Offset

	// EvictedThrough is the highest offset that has been dropped. A cursor at
	// or below it cannot be fully resolved.
	EvictedThrough Offset

	// Resumable reports whether this log is configured to retain history for
	// resumption. When false the library promises nothing and says so, rather
	// than half-promising.
	Resumable bool

	// Retention is the window the log is configured to keep, so a client can
	// be told what it was actually offered.
	Retention Retention
}

// A Gap is history a client asked for and cannot be given.
//
// It is always declared, never absorbed. A declared failure is acceptable; a
// silent one corrupts the client's state, and that is the difference this
// library is built around (RF-C4).
type Gap struct {
	// Reason says why the history could not be provided.
	Reason GapReason

	// From is the position the client presented.
	From Offset

	// Through is the highest offset known to be missing. Everything in
	// (From, Through] was lost.
	Through Offset
}

// GapReason distinguishes the ways resumption can fail, so an application can
// tell "you were away too long" from "this server is not the one you were
// talking to".
type GapReason string

const (
	// GapRetention means the events fell outside the retention window.
	GapRetention GapReason = "retention"

	// GapEpoch means the cursor belongs to an earlier generation of the log,
	// so its offsets refer to events that no longer exist. Resolving it
	// against current offsets would hand the client unrelated events.
	GapEpoch GapReason = "epoch"

	// GapUnresolvable means the cursor could not be decoded at all.
	GapUnresolvable GapReason = "unresolvable"

	// GapSlowConsumer means the subscriber could not keep up and its
	// backpressure policy discarded events. They are gone for this connection;
	// the client is told the range so it can reload state rather than assume
	// the stream was continuous.
	GapSlowConsumer GapReason = "slow-consumer"

	// GapUnsupported means this stream does not retain history, so nothing
	// could be replayed.
	GapUnsupported GapReason = "unsupported"
)

// Retention is how much history a log keeps.
//
// The limits apply together: an entry is evicted as soon as it violates any one
// of them. The zero Retention is not "keep everything" — it is the small
// in-flight window a log needs to serve its live readers, and a log configured
// that way reports Resumable false and offers no resumption at all.
//
// Retention is a property of a log, so different retention means a different
// log. That is deliberate: differing retention inside one shared log is not
// implementable without sub-logs, which is what more logs already are.
type Retention struct {
	// Events caps the number of entries kept. Zero means unlimited.
	Events int

	// Bytes caps the total size kept. Zero means unlimited.
	Bytes int

	// For caps how old an entry may be. Zero means unlimited.
	For time.Duration
}

// IsZero reports whether no retention was configured, in which case the log
// offers no resumption.
func (r Retention) IsZero() bool { return r == Retention{} }

// ErrLogClosed is returned once a log has been closed.
var ErrLogClosed = errors.New("sse: log is closed")
