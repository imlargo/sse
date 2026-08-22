package sse

import (
	"fmt"
	"net/http"
	"time"
)

// A Transport is the connection a session writes to.
//
// It exists so the core never touches http.ResponseWriter directly. That buys
// two things: adapters for engines that are not net/http (fasthttp, and
// anything else where client disconnection is not observable through the
// request context) can satisfy it, and tests can drive a session over an
// in-memory transport — which testing/synctest requires, since it forbids real
// network use.
//
// Exactly one goroutine ever calls these methods, so implementations do not
// need to be safe for concurrent use.
type Transport interface {
	// Header returns the response headers, mutable until the first write.
	Header() http.Header

	// WriteHeader commits the response status.
	WriteHeader(statusCode int)

	// Write appends to the response body.
	Write(p []byte) (int, error)

	// Flush pushes buffered bytes to the client. A stream is useless without
	// it, so a transport that cannot flush is refused before the stream opens.
	Flush() error

	// SetWriteDeadline bounds how long a write may block. Returning an error
	// matching http.ErrNotSupported is allowed: the session then falls back to
	// detecting a dead peer through write failure alone, and reports the
	// reduced capability rather than pretending.
	SetWriteDeadline(t time.Time) error

	// Capabilities reports what this transport can actually do, so the session
	// can degrade honestly instead of assuming.
	Capabilities() Capabilities
}

// Capabilities describes what a transport can actually do. The session reports
// them rather than assuming them.
type Capabilities struct {
	// Flush is always true for a session that started: the handler refuses to
	// open a stream that cannot be flushed.
	Flush bool
	// WriteDeadline reports whether writes are time-bounded. Without it, a peer
	// that stops reading is only detected when the write itself fails.
	WriteDeadline bool
}

// netHTTPTransport adapts a standard library response writer.
type netHTTPTransport struct {
	w    http.ResponseWriter
	rc   *http.ResponseController
	caps Capabilities
}

// newNetHTTPTransport probes w and returns a transport, or an error naming the
// middleware that makes streaming impossible.
func newNetHTTPTransport(w http.ResponseWriter) (*netHTTPTransport, error) {
	canFlush, canDeadline, chain, brokenAt := probeWriter(w)
	if !canFlush {
		return nil, &UnsupportedWriterError{Chain: chain, BrokenAt: brokenAt}
	}
	return &netHTTPTransport{
		w:    w,
		rc:   http.NewResponseController(w),
		caps: Capabilities{Flush: true, WriteDeadline: canDeadline},
	}, nil
}

func (t *netHTTPTransport) Header() http.Header                { return t.w.Header() }
func (t *netHTTPTransport) WriteHeader(code int)               { t.w.WriteHeader(code) }
func (t *netHTTPTransport) Write(p []byte) (int, error)        { return t.w.Write(p) }
func (t *netHTTPTransport) Flush() error                       { return t.rc.Flush() }
func (t *netHTTPTransport) SetWriteDeadline(d time.Time) error { return t.rc.SetWriteDeadline(d) }
func (t *netHTTPTransport) Capabilities() Capabilities         { return t.caps }

// maxUnwrapDepth guards against a wrapper chain that loops back on itself.
const maxUnwrapDepth = 32

// probeWriter walks the Unwrap chain the way http.ResponseController does, but
// without side effects, so the result can be reported as a diagnostic instead
// of discovered by a flush that silently does nothing.
//
// Calling ResponseController.Flush to test for support is not an option: it
// would commit a 200 response, after which no useful error status can be sent.
func probeWriter(w http.ResponseWriter) (canFlush, canDeadline bool, chain []string, brokenAt string) {
	cur := w
	for depth := 0; depth < maxUnwrapDepth; depth++ {
		chain = append(chain, fmt.Sprintf("%T", cur))

		if _, ok := cur.(interface{ FlushError() error }); ok {
			canFlush = true
		}
		if _, ok := cur.(http.Flusher); ok {
			canFlush = true
		}
		if _, ok := cur.(interface{ SetWriteDeadline(time.Time) error }); ok {
			canDeadline = true
		}
		if canFlush && canDeadline {
			return canFlush, canDeadline, chain, ""
		}

		u, ok := cur.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			if !canFlush {
				brokenAt = fmt.Sprintf("%T", cur)
			}
			return canFlush, canDeadline, chain, brokenAt
		}
		next := u.Unwrap()
		if next == nil {
			if !canFlush {
				brokenAt = fmt.Sprintf("%T", cur)
			}
			return canFlush, canDeadline, chain, brokenAt
		}
		cur = next
	}
	if !canFlush {
		brokenAt = "unwrap chain exceeded " + fmt.Sprint(maxUnwrapDepth) + " levels"
	}
	return canFlush, canDeadline, chain, brokenAt
}
