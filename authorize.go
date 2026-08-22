package sse

import (
	"fmt"
	"net/http"
	"time"
)

// An Authorizer decides whether to accept a connection, and on what terms.
//
// It is the one place where the application sees the whole request before
// anything is committed, and it is deliberately just a function:
//
//	func authorize(r *http.Request) (sse.Grant, error) {
//	    user, ok := session(r)              // cookie, query token, whatever
//	    if !ok {
//	        return sse.Grant{}, sse.Unauthorized("sign in to subscribe")
//	    }
//	    return sse.Grant{
//	        Identity: user.ID,
//	        Filters:  []sse.Filter{sse.MustFilter("tenant." + user.Tenant + ".>")},
//	        Deadline: user.TokenExpiry,
//	    }, nil
//	}
//
// A plain function composes by ordinary wrapping and is substituted in tests by
// passing a different one. There is no container, no registration and no
// reflection — the dependency-injection shape this replaces is powered by
// runtime introspection, which is the wrong trade in Go and worse still on a
// fan-out path.
//
// Returning an error rejects the request with an ordinary HTTP response,
// *before* the stream opens. That ordering is not a detail: once a
// 200 text/event-stream response is committed there is no status left to send,
// and a client that receives a non-200 stops reconnecting for good.
type Authorizer func(r *http.Request) (Grant, error)

// A Grant is what an [Authorizer] decided.
//
// The zero Grant accepts the connection with no filters, which subscribes to
// everything the stream carries.
type Grant struct {
	// Identity is who the application decided this is. It is opaque to the
	// library and is only carried into logs, metrics and the capability event.
	Identity string

	// Filters is what the subscriber may receive. Empty means everything the
	// stream carries.
	Filters []Filter

	// Denied is what was asked for and refused. It is reported to the client in
	// the capability event, so a subscriber that asked for five topics and got
	// three is told which two are missing.
	//
	// RF-F2 forbids the alternative. A denied topic that produces a stream
	// which simply never carries those events looks identical to a topic that
	// is merely quiet, and the client waits forever for something that will
	// never arrive.
	Denied []Denial

	// Deadline ends the session at a set time, which is how a credential that
	// expires mid-stream is handled.
	//
	// The client reconnects by itself through the protocol's own mechanism,
	// presents fresh credentials, and resumes from its cursor. That turns token
	// expiry from a problem into a non-event (RF-F3). Zero means no deadline.
	Deadline time.Time

	// Backpressure overrides the handler's policy for this subscriber, so the
	// behaviour under load can be decided per subscription rather than per
	// server (RF-D1). Nil inherits.
	Backpressure *Backpressure

	// Attributes are added to the capability event, for anything the
	// application wants the client to know at connection time.
	Attributes map[string]string
}

// A Denial is a topic the application refused, and why.
type Denial struct {
	// Topic is what was asked for, as the client wrote it.
	Topic string `json:"topic"`
	// Reason is a short, stable string the client can branch on.
	Reason string `json:"reason"`
}

// Unauthorized rejects a connection with 401.
func Unauthorized(message string) error {
	return &StatusError{Code: http.StatusUnauthorized, Message: message}
}

// Forbidden rejects a connection with 403.
func Forbidden(message string) error {
	return &StatusError{Code: http.StatusForbidden, Message: message}
}

// BadRequest rejects a connection with 400.
func BadRequest(message string) error {
	return &StatusError{Code: http.StatusBadRequest, Message: message}
}

// Grant returns what the [Authorizer] decided for this session.
func (s *Session) Grant() Grant { return s.grant }

// Identity returns who the application said this is, or the empty string when
// no authorizer was configured.
func (s *Session) Identity() string { return s.grant.Identity }

// statusOf maps an authorizer's error to a response status.
//
// Anything that is not a [StatusError] is treated as a server-side failure
// rather than guessed at. Guessing would be worse than useless here: answering
// 403 where 500 was meant tells a client to stop trying forever.
func statusOf(err error) (code int, message string) {
	var se *StatusError
	if asStatusError(err, &se) {
		msg := se.Message
		if msg == "" {
			msg = http.StatusText(se.Code)
		}
		return se.Code, msg
	}
	return http.StatusInternalServerError, "the stream could not be opened"
}

func asStatusError(err error, target **StatusError) bool {
	for err != nil {
		if se, ok := err.(*StatusError); ok {
			*target = se
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// validate checks a grant before it is acted on, so a mistake in an authorizer
// surfaces as an error rather than as a subtly wrong subscription.
func (g Grant) validate() error {
	for i, f := range g.Filters {
		if f.IsZero() {
			return fmt.Errorf("sse: Grant.Filters[%d] is the zero Filter; build it with sse.NewFilter", i)
		}
	}
	if g.Backpressure != nil {
		switch g.Backpressure.Policy {
		case DropOldest, DropNewest, Coalesce, Block, Disconnect:
		default:
			return fmt.Errorf("sse: Grant.Backpressure: unknown policy %d", int(g.Backpressure.Policy))
		}
	}
	return nil
}
