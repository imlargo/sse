package sse

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// Errors are values, distinguishable by cause (RNF-8). Nothing panics across
// the public boundary: a panic in application code is contained and converted
// (RF-E5).
var (
	// ErrClientGone means the connection failed while writing. It is the
	// normal end of a stream and usually not worth logging as a failure.
	//
	// A write failure is the library's primary liveness signal, not a fallback:
	// context cancellation is unreliable on POST with an undrained body over
	// HTTP/1.1, and on fasthttp it only fires at server shutdown.
	ErrClientGone = errors.New("sse: client is gone")

	// ErrWriteTimeout means a write exceeded the configured deadline: the peer
	// stopped reading but did not close. Without a deadline this is the case
	// that pins a goroutine and a connection forever.
	ErrWriteTimeout = errors.New("sse: write deadline exceeded")

	// ErrSessionClosed means the session already finished.
	ErrSessionClosed = errors.New("sse: session is closed")

	// ErrEventTooLarge means one event exceeded the configured limit. An
	// oversized event multiplied by every subscriber is a denial of service
	// against yourself (RF-G5).
	ErrEventTooLarge = errors.New("sse: event exceeds the configured size limit")

	// ErrReservedName means an event name collided with the library's reserved
	// namespace (RF-E4).
	ErrReservedName = errors.New("sse: event name is reserved by the library")

	// ErrShuttingDown means the server is draining and stopped accepting new
	// events on this session.
	ErrShuttingDown = errors.New("sse: server is shutting down")
)

// UnsupportedWriterError says the response writer cannot stream, and names the
// wrapper that broke the chain.
//
// http.ResponseController does not reach through arbitrary middleware: it walks
// a chain of Unwrap() http.ResponseWriter methods, and stops at the first
// wrapper that does not implement one. Rather than open a stream that can never
// be flushed, the handler refuses and says exactly which type to fix (RF-H4).
type UnsupportedWriterError struct {
	// Chain is the wrapper chain from the outermost writer inwards.
	Chain []string
	// BrokenAt is the type that does not implement Unwrap, if the chain ended
	// before reaching a flushable writer.
	BrokenAt string
}

func (e *UnsupportedWriterError) Error() string {
	var b strings.Builder
	b.WriteString("sse: the response writer cannot flush, so the stream was not started.\n")
	b.WriteString("  writer chain: " + strings.Join(e.Chain, " -> ") + "\n")
	if e.BrokenAt != "" {
		fmt.Fprintf(&b, "  %s does not implement Unwrap() http.ResponseWriter, so http.ResponseController\n", e.BrokenAt)
		b.WriteString("  cannot reach the writer underneath it.\n")
	}
	b.WriteString("  Middleware that wraps http.ResponseWriter must expose the original writer:\n")
	b.WriteString("      func (w *yourWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }")
	return b.String()
}

// StatusError lets an application reject a request with a specific HTTP status
// before the stream opens.
//
// The status matters more than usual here: a client permanently stops
// reconnecting when a response is not 200 with a text/event-stream body. That
// makes 204 the way to say "stop reconnecting", and makes any non-200 answer to
// a reconnection during a rolling deploy a way to kill clients for good.
type StatusError struct {
	Code    int
	Message string
	Err     error
}

func (e *StatusError) Error() string {
	msg := e.Message
	if msg == "" {
		msg = http.StatusText(e.Code)
	}
	if e.Err != nil {
		return fmt.Sprintf("sse: %d %s: %v", e.Code, msg, e.Err)
	}
	return fmt.Sprintf("sse: %d %s", e.Code, msg)
}

func (e *StatusError) Unwrap() error { return e.Err }

// Status returns an error that rejects the request with the given status.
func Status(code int, message string) error {
	return &StatusError{Code: code, Message: message}
}

// PanicError wraps a panic recovered from application code. The damage is
// contained to the session that caused it (RF-E5).
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("sse: recovered panic in application code: %v", e.Value)
}
