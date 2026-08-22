package sse

import (
	"context"
	"sync"
)

// A Lifecycle tracks live sessions so they can be drained together.
//
// It is opt-in, via [WithLifecycle]. A handler without one keeps no registry at
// all: the single-client case pays nothing for machinery it does not use
// (RNF-4).
//
// The zero Lifecycle is ready to use.
type Lifecycle struct {
	mu       sync.Mutex
	sessions map[*Session]struct{}
	byID     map[string]*Session
	closed   bool
}

// NewLifecycle returns an empty Lifecycle.
func NewLifecycle() *Lifecycle { return &Lifecycle{} }

// NodeSessionCount returns how many sessions are live *on this process*.
//
// The name says "node" on purpose. Once several replicas are behind a load
// balancer, no number a single process can produce describes the cluster, and a
// value named as if it did would be a lie in every dashboard that used it
// (RNF-11).
func (l *Lifecycle) NodeSessionCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.sessions)
}

// add registers s. It reports false if the Lifecycle is already shutting down,
// in which case the caller must not open the stream.
func (l *Lifecycle) add(s *Session) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return false
	}
	if l.sessions == nil {
		l.sessions = make(map[*Session]struct{})
		l.byID = make(map[string]*Session)
	}
	l.sessions[s] = struct{}{}
	l.byID[s.id] = s
	return true
}

func (l *Lifecycle) remove(s *Session) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.sessions, s)
	delete(l.byID, s.id)
}

// Session returns a live session by its id.
//
// This is what makes a session addressable from outside the request that
// created it, and it is the missing piece behind three separate things.
// EventSource cannot send anything to the server, so changing a subscription
// needs a side channel ([Session.Resubscribe]). A revoked credential needs a
// way to end one session ([Session.Stop]). And a stream that has to be told
// something out of band needs somewhere to be told it.
//
// The id is the one in the connection event, so a client already has it:
//
//	POST /subscriptions?session=<id>&topic=tenant.acme.builds
//
// It is local to this process. Behind a load balancer a session lives on
// exactly one replica, so a side channel either has to reach that replica or go
// through something both share.
func (l *Lifecycle) Session(id string) (*Session, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.byID[id]
	return s, ok
}

// NodeSessions returns every session live in this process.
func (l *Lifecycle) NodeSessions() []*Session {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]*Session, 0, len(l.sessions))
	for s := range l.sessions {
		out = append(out, s)
	}
	return out
}

// Shutdown drains every live session and waits for them to finish.
//
// Each session is told to stop, writes whatever it still has queued, and then
// receives a closing event carrying a jittered retry delay so that clients
// dropped together do not reconnect together (RF-E1, RF-E2).
//
// Shutdown returns the context's error if the deadline passes first; sessions
// that have not finished by then are abandoned to their write deadlines rather
// than held onto.
func (l *Lifecycle) Shutdown(ctx context.Context) error {
	l.mu.Lock()
	l.closed = true
	pending := make([]*Session, 0, len(l.sessions))
	for s := range l.sessions {
		pending = append(pending, s)
	}
	l.mu.Unlock()

	for _, s := range pending {
		s.requestStop()
	}

	for _, s := range pending {
		select {
		case <-s.Done():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
