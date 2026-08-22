// Package fibersse serves server-sent event streams from Fiber.
//
// Fiber is the case this library's transport abstraction was designed for, and
// the one where the existing Go ecosystem is documented as broken.
//
// # Why Fiber needs its own transport at all
//
// Fiber runs on fasthttp, not net/http. Two things follow. There is no
// http.ResponseWriter, so nothing in the standard library's streaming machinery
// applies. And more seriously, fasthttp's RequestCtx.Done() fires when the
// *server* shuts down, not when a client disconnects — so every SSE library
// built on net/http leaks a subscriber per dropped client when it is adapted to
// Fiber, permanently, for the life of the process.
//
// This adapter does not have that problem, and not because of anything it does:
// nothing in the core depends on context cancellation to notice a client has
// gone. The keep-alive doubles as a liveness probe, and a failed write is what
// ends a session. That holds here exactly as it does on net/http.
//
// # Write deadlines
//
// The underlying connection is captured before streaming begins and deadlines
// are set on it directly, so a client that stops reading without closing is
// still bounded. Without that, such a client pins a goroutine and a connection
// until the process dies — and it is the failure mode a request context cannot
// help with on any framework.
//
// # Usage
//
//	app.Get("/events", fibersse.Handler(func(ctx context.Context, s *sse.Session) error {
//	    for token := range model.Stream(ctx, prompt) {
//	        if err := s.Send(ctx, sse.Text(token)); err != nil {
//	            return err
//	        }
//	    }
//	    return nil
//	}))
//
// Everything else — heartbeats, backpressure, topics, resumption, graceful
// drain — is the core's and behaves identically to every other framework.
package fibersse

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/imlargo/sse"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
)

// Handler returns a Fiber handler that runs fn as an event stream.
//
// It panics on an invalid option, which at startup can only be a programming
// error. Use [NewHandler] when the configuration is computed.
func Handler(fn sse.StreamFunc, opts ...sse.Option) fiber.Handler {
	h, err := NewHandler(fn, opts...)
	if err != nil {
		panic(err)
	}
	return h
}

// NewHandler is [Handler] for configuration that can fail.
func NewHandler(fn sse.StreamFunc, opts ...sse.Option) (fiber.Handler, error) {
	if fn == nil {
		return nil, fmt.Errorf("fibersse: stream function must not be nil")
	}
	// Validate the options once, here, rather than per request.
	if _, err := sse.NewHandler(fn, opts...); err != nil {
		return nil, err
	}

	return func(c fiber.Ctx) error {
		req, err := adaptRequest(c)
		if err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, err.Error())
		}

		// Headers must be set before the body stream starts, because fasthttp
		// serialises them before it calls the stream writer. Taking them from
		// the core rather than writing them out here is what stops this
		// adapter drifting from it.
		for key, values := range sse.StreamHeaders(req) {
			for _, v := range values {
				c.Set(key, v)
			}
		}
		c.Status(fiber.StatusOK)

		// The connection is captured now, not inside the stream writer.
		// fasthttp may recycle a RequestCtx once the handler returns, and the
		// stream writer runs after that.
		conn := c.RequestCtx().Conn()

		// A context that outlives the handler. RequestCtx.Done() is not usable
		// here: on fasthttp it fires at server shutdown, not on client
		// disconnection. It is still worth observing for the shutdown case,
		// but nothing about correctness rests on it.
		ctx, cancel := context.WithCancel(context.WithoutCancel(c.Context()))

		c.RequestCtx().SetBodyStreamWriter(func(w *bufio.Writer) {
			defer cancel()
			t := &transport{w: w, conn: conn, header: make(http.Header)}
			_ = sse.Serve(ctx, t, req, fn, opts...)
		})
		return nil
	}, nil
}

// adaptRequest builds a net/http request from the fasthttp one, so application
// code, authorizers and the resumption cursor all see the same shape they see
// on every other framework.
func adaptRequest(c fiber.Ctx) (*http.Request, error) {
	var req http.Request
	if err := fasthttpadaptor.ConvertRequest(c.RequestCtx(), &req, true); err != nil {
		return nil, fmt.Errorf("fibersse: converting the request: %w", err)
	}
	return &req, nil
}

// transport writes an event stream over a fasthttp body stream.
//
// Exactly one goroutine ever touches it, which the core guarantees.
type transport struct {
	w      *bufio.Writer
	conn   net.Conn
	header http.Header
}

// Header returns the response headers.
//
// They have already been applied to the fasthttp response before streaming
// began, so this exists to satisfy the interface. The core writes the same
// values it produced from StreamHeaders, so nothing is lost.
func (t *transport) Header() http.Header { return t.header }

// WriteHeader is a no-op: fasthttp committed the status before the body stream
// started, which is why the adapter sets it up front.
func (t *transport) WriteHeader(int) {}

func (t *transport) Write(p []byte) (int, error) { return t.w.Write(p) }

// Flush pushes bytes to the client, and is where a disconnected client is
// discovered. On fasthttp this is the only place it can be discovered, since
// the request context never reports it.
func (t *transport) Flush() error { return t.w.Flush() }

// SetWriteDeadline bounds a write on the underlying connection.
//
// fasthttp gives no per-response deadline, so this reaches the net.Conn
// directly. Without it a client that stops reading but never closes holds a
// goroutine and a connection until the process ends.
func (t *transport) SetWriteDeadline(deadline time.Time) error {
	if t.conn == nil {
		return http.ErrNotSupported
	}
	return t.conn.SetWriteDeadline(deadline)
}

func (t *transport) Capabilities() sse.Capabilities {
	return sse.Capabilities{Flush: true, WriteDeadline: t.conn != nil}
}

// compile-time checks
var (
	_ sse.Transport         = (*transport)(nil)
	_ fasthttp.StreamWriter = func(*bufio.Writer) {}
)
