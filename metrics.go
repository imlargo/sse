package sse

import (
	"errors"
	"time"
)

// Metrics is a set of optional observation hooks.
//
// It is a struct of functions rather than an interface, modelled on
// httptrace.ClientTrace, for three reasons. The core stays free of any metrics
// dependency, which RNF-9 requires. A partial implementation is trivial —
// set the two hooks you care about and leave the rest nil. And an unset hook
// costs a nil check, so a server that observes nothing pays nothing (RNF-4).
//
// Concrete integrations live in their own modules, outside the core.
//
// # Everything here is local to this process
//
// Not one of these values describes a cluster. Behind a load balancer, active
// sessions, delivered events and queue depth are all per-replica, and a
// dashboard that sums them without saying so is describing something that does
// not exist. The hook names carry "Node" where the distinction is easiest to
// get wrong, and an exporter should carry it into the metric name too (RNF-11).
//
// Hooks are called from the goroutine that did the work, including the writer
// goroutine of a session. They must not block: anything slow belongs behind a
// buffered channel in the implementation.
type Metrics struct {
	// SessionOpened is called once a stream has been accepted and its
	// capability event queued.
	SessionOpened func(SessionStats)

	// SessionClosed is called once a stream has finished, with why it ended.
	// The error is nil for an ordinary close.
	SessionClosed func(SessionStats, error)

	// NodeSessionsActive reports how many sessions are live *in this process*
	// after a change.
	NodeSessionsActive func(n int)

	// EventPublished is called when an event enters a log. It counts what was
	// produced, not what anyone received.
	EventPublished func(topic string, bytes int)

	// EventDelivered is called when an event has been written to a connection
	// and flushed. Latency is the time from publication to that write, which is
	// the number that says whether subscribers are keeping up.
	EventDelivered func(topic string, bytes int, latency time.Duration)

	// EventDropped is called when events are discarded for a subscriber, with
	// the backpressure reason.
	EventDropped func(topic string, reason string, count int)

	// GapDeclared is called when a client is told history could not be
	// provided. A rise here means retention is too short for how long clients
	// are away, or subscribers are falling behind.
	GapDeclared func(reason GapReason)

	// QueueDepth reports a subscriber's queue occupancy after a change. It is
	// the leading indicator: queues fill before anything is dropped.
	QueueDepth func(sessionID string, events, bytes int)
}

// SessionStats describes one session for the observation hooks.
type SessionStats struct {
	SessionID string
	Identity  string
	Resumable bool
	Filters   int
	// Reason is why the session ended, set only on close: "client-gone",
	// "write-timeout", "slow-consumer", "shutdown", "deadline", "completed"
	// or "panic".
	Reason string
}

// The reasons a session ends. They are strings rather than a typed enum
// because they are label values in whatever metrics system is behind the hook.
const (
	ReasonCompleted    = "completed"
	ReasonClientGone   = "client-gone"
	ReasonWriteTimeout = "write-timeout"
	ReasonSlowConsumer = "slow-consumer"
	ReasonShutdown     = "shutdown"
	ReasonDeadline     = "deadline"
	ReasonPanic        = "panic"
	ReasonError        = "error"
)

// call invokes a hook if it is set. Every call site goes through one of these,
// so an unobserved server pays exactly one nil check per event.
func (m *Metrics) sessionOpened(s SessionStats) {
	if m != nil && m.SessionOpened != nil {
		m.SessionOpened(s)
	}
}

func (m *Metrics) sessionClosed(s SessionStats, err error) {
	if m != nil && m.SessionClosed != nil {
		m.SessionClosed(s, err)
	}
}

func (m *Metrics) nodeSessionsActive(n int) {
	if m != nil && m.NodeSessionsActive != nil {
		m.NodeSessionsActive(n)
	}
}

func (m *Metrics) eventPublished(topic string, bytes int) {
	if m != nil && m.EventPublished != nil {
		m.EventPublished(topic, bytes)
	}
}

func (m *Metrics) eventDelivered(topic string, bytes int, latency time.Duration) {
	if m != nil && m.EventDelivered != nil {
		m.EventDelivered(topic, bytes, latency)
	}
}

func (m *Metrics) eventDropped(topic, reason string, count int) {
	if m != nil && m.EventDropped != nil {
		m.EventDropped(topic, reason, count)
	}
}

func (m *Metrics) gapDeclared(reason GapReason) {
	if m != nil && m.GapDeclared != nil {
		m.GapDeclared(reason)
	}
}

func (m *Metrics) queueDepth(id string, events, bytes int) {
	if m != nil && m.QueueDepth != nil {
		m.QueueDepth(id, events, bytes)
	}
}

// reasonFor maps a session's ending error to a stable label value.
func reasonFor(err error) string {
	switch {
	case err == nil:
		return ReasonCompleted
	case errors.Is(err, ErrClientGone):
		return ReasonClientGone
	case errors.Is(err, ErrWriteTimeout):
		return ReasonWriteTimeout
	case errors.Is(err, ErrSlowConsumer):
		return ReasonSlowConsumer
	case errors.Is(err, ErrShuttingDown):
		return ReasonShutdown
	case errors.Is(err, errDeadlineReached):
		return ReasonDeadline
	case isPanicError(err):
		return ReasonPanic
	default:
		return ReasonError
	}
}

func isPanicError(err error) bool {
	var pe *PanicError
	return errors.As(err, &pe)
}
