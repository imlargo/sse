package sse

import (
	"context"
	crand "crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/imlargo/sse/wire"
)

// A Session is one client's stream.
//
// The application produces events by calling [Session.Send]; it never writes a
// loop over a channel, never sets a header and never flushes. That division is
// the whole point: because the library owns the writing, it can own heartbeats,
// write deadlines, drain-on-shutdown and, later, replay ordering — and it can
// improve any of them without breaking a single caller.
//
// A Session is safe for concurrent use by several producers. Exactly one
// internal goroutine ever touches the transport, so it is impossible by
// construction for two flows to write to the same connection (RF-A2).
type Session struct {
	id   string
	req  *http.Request
	cfg  *config
	caps Capabilities
	t    Transport

	frames chan *[]byte

	sendClosed    chan struct{}
	closeSendOnce sync.Once

	stop     chan struct{}
	stopOnce sync.Once

	done chan struct{} // closed when the writer goroutine has exited

	mu  sync.Mutex
	err error
}

// A SendOption adjusts a single event.
type SendOption func(*sendOpts)

type sendOpts struct{ name string }

// Name sets the event type, which is what a client dispatches on. Without it
// the client sees the default "message" type.
func Name(n string) SendOption { return func(o *sendOpts) { o.name = n } }

// ID returns the session identifier. It is generated per connection and is what
// makes a session addressable from outside the handler — the thing SSE itself
// does not carry.
func (s *Session) ID() string { return s.id }

// Request returns the request that opened the stream.
func (s *Session) Request() *http.Request { return s.req }

// Capabilities reports what the underlying transport can actually do.
func (s *Session) Capabilities() Capabilities { return s.caps }

// Done is closed when the session has finished writing.
func (s *Session) Done() <-chan struct{} { return s.done }

// Err returns why the session ended, or nil while it is still running.
func (s *Session) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Send queues one event.
//
// A value that is already a [Payload] is used as-is; anything else goes through
// the configured [Codec]. So all of these work:
//
//	s.Send(ctx, ticket)                        // serialized by the codec
//	s.Send(ctx, sse.Text(token))               // raw text, no serialization
//	s.Send(ctx, sse.Raw(htmlFragment))         // pre-rendered bytes
//	s.Send(ctx, update, sse.Name("changed"))   // with an event type
//
// Send returns when the event has been accepted for writing, not when the
// client has received it. Nothing in this API can confirm delivery, so nothing
// in it is named as if it could.
func (s *Session) Send(ctx context.Context, v any, opts ...SendOption) error {
	var o sendOpts
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	if o.name != "" && strings.HasPrefix(o.name, s.cfg.prefix) {
		return fmt.Errorf("%w: %q starts with the reserved prefix %q",
			ErrReservedName, o.name, s.cfg.prefix)
	}

	p, ok := v.(Payload)
	if !ok {
		p = valuePayload{v}
	}
	return s.emit(ctx, wire.Event{Name: o.name}, p)
}

// Comment writes a comment line. Clients ignore it, so it carries no event.
func (s *Session) Comment(ctx context.Context, text string) error {
	buf := getBuf()
	out, err := wire.AppendComment((*buf)[:0], text)
	if err != nil {
		putBuf(buf)
		return err
	}
	*buf = out
	return s.enqueue(ctx, buf)
}

// emit serializes the payload into ev and queues the encoded frame.
func (s *Session) emit(ctx context.Context, ev wire.Event, p Payload) error {
	data := getBuf()
	body, err := p.appendData((*data)[:0], s.cfg.codec)
	if err != nil {
		putBuf(data)
		return fmt.Errorf("sse: encoding payload: %w", err)
	}
	// A nil Data means "no data field", which a client suppresses. An
	// application event always has a body, even an empty one, so make sure the
	// slice is non-nil.
	if body == nil {
		body = []byte{}
	}
	ev.Data = body

	frame := getBuf()
	out, err := wire.AppendEvent((*frame)[:0], ev)
	putBuf(data)
	if err != nil {
		putBuf(frame)
		return err
	}
	if len(out) > s.cfg.maxEventSize {
		putBuf(frame)
		return fmt.Errorf("%w: %d bytes exceeds the %d byte limit",
			ErrEventTooLarge, len(out), s.cfg.maxEventSize)
	}
	*frame = out
	return s.enqueue(ctx, frame)
}

func (s *Session) enqueue(ctx context.Context, buf *[]byte) error {
	select {
	case <-s.done:
		putBuf(buf)
		return s.endErr()
	case <-s.sendClosed:
		putBuf(buf)
		return ErrSessionClosed
	default:
	}

	select {
	case s.frames <- buf:
		return nil
	case <-s.done:
		putBuf(buf)
		return s.endErr()
	case <-s.stop:
		putBuf(buf)
		return ErrShuttingDown
	case <-ctx.Done():
		putBuf(buf)
		return ctx.Err()
	}
}

func (s *Session) endErr() error {
	if err := s.Err(); err != nil {
		return err
	}
	return ErrSessionClosed
}

func (s *Session) fail(err error) {
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.mu.Unlock()
}

// closeSend tells the writer that no more events are coming. Queued events are
// still written.
func (s *Session) closeSend() {
	s.closeSendOnce.Do(func() { close(s.sendClosed) })
}

// requestStop asks the session to drain and finish, which is what a graceful
// shutdown does to every live session.
func (s *Session) requestStop() {
	s.stopOnce.Do(func() { close(s.stop) })
}

// pump owns the transport. It is the only goroutine that writes to it.
func (s *Session) pump() {
	defer close(s.done)

	var timer *time.Timer
	var tick <-chan time.Time
	if s.cfg.keepAlive > 0 {
		timer = time.NewTimer(s.cfg.keepAlive)
		defer timer.Stop()
		tick = timer.C
	}
	resetKeepAlive := func() {
		if timer != nil {
			timer.Stop()
			timer.Reset(s.cfg.keepAlive)
		}
	}

	for {
		select {
		case buf := <-s.frames:
			if err := s.writeFrame(buf); err != nil {
				s.fail(err)
				return
			}
			resetKeepAlive()

		case <-tick:
			// The heartbeat is also the liveness probe. A peer that has gone
			// away is discovered here, in every environment, whether or not the
			// request context ever fires.
			if err := s.writeBytes(keepAliveLine); err != nil {
				s.fail(err)
				return
			}
			resetKeepAlive()

		case <-s.stop:
			s.drain()
			return

		case <-s.sendClosed:
			s.drain()
			return
		}
	}
}

// drain writes whatever is still queued, then tells the client if the server is
// going away.
//
// Whether to send the closing notice is read from the session's state rather
// than from which select branch woke the writer: a session that is both
// finishing and being shut down has both channels ready at once, and the select
// picks between them at random.
func (s *Session) drain() {
	for {
		select {
		case buf := <-s.frames:
			if err := s.writeFrame(buf); err != nil {
				s.fail(err)
				return
			}
		default:
			if s.stopRequested() {
				if err := s.writeClosing(); err != nil {
					s.fail(err)
				}
			}
			return
		}
	}
}

func (s *Session) stopRequested() bool {
	select {
	case <-s.stop:
		return true
	default:
		return false
	}
}

// writeClosing emits the shutdown notice.
//
// The retry field carries a jittered delay so that clients dropped together do
// not come back together. Note what this deliberately does not do: it does not
// answer with a non-200 status. A client that receives one stops reconnecting
// permanently, so a draining node must never reply to a reconnection with an
// error — it would kill the client instead of moving it to another node.
func (s *Session) writeClosing() error {
	delay := jitterDelay(s.cfg.retry, s.cfg.retryJitter)

	buf := getBuf()
	defer putBuf(buf)

	body := fmt.Sprintf(`{"reason":"shutdown","reconnectAfterMs":%d}`, delay.Milliseconds())
	out, err := wire.AppendEvent((*buf)[:0], wire.Event{
		Name:  s.cfg.prefix + "closing",
		Retry: delay,
		Data:  []byte(body),
	})
	if err != nil {
		return err
	}
	*buf = out
	return s.writeBytes(out)
}

func (s *Session) writeFrame(buf *[]byte) error {
	err := s.writeBytes(*buf)
	putBuf(buf)
	return err
}

func (s *Session) writeBytes(b []byte) error {
	if s.caps.WriteDeadline && s.cfg.writeTimeout > 0 {
		// Without this, a client that stops reading but never closes pins the
		// goroutine and the connection forever. The request context does not
		// help: the write simply blocks.
		_ = s.t.SetWriteDeadline(time.Now().Add(s.cfg.writeTimeout))
	}
	if _, err := s.t.Write(b); err != nil {
		return classifyWriteError(err)
	}
	if err := s.t.Flush(); err != nil {
		return classifyWriteError(err)
	}
	return nil
}

func classifyWriteError(err error) error {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return fmt.Errorf("%w: %v", ErrWriteTimeout, err)
	}
	return fmt.Errorf("%w: %v", ErrClientGone, err)
}

// jitterDelay spreads a delay by up to frac in either direction.
func jitterDelay(base time.Duration, frac float64) time.Duration {
	if frac <= 0 || base <= 0 {
		return base
	}
	spread := float64(base) * frac
	d := time.Duration(float64(base) - spread + rand.Float64()*2*spread)
	if d < time.Millisecond {
		d = time.Millisecond
	}
	return d
}

// keepAliveLine is the cheapest thing that can go on the wire: a bare comment.
var keepAliveLine = []byte(":\n")

// Frames move between the producer and the writer through a pool, so a
// long-lived session settles into steady state without allocating per event.
var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 512)
		return &b
	},
}

func getBuf() *[]byte { return bufPool.Get().(*[]byte) }

func putBuf(b *[]byte) {
	// Do not retain buffers that one huge event grew; they would pin memory for
	// the life of the process.
	if cap(*b) > 64<<10 {
		return
	}
	*b = (*b)[:0]
	bufPool.Put(b)
}

func newSessionID() string {
	var b [16]byte
	if _, err := crand.Read(b[:]); err != nil {
		// crypto/rand does not fail on any supported platform; if it somehow
		// does, a timestamp still gives a usable, unique-enough id.
		return fmt.Sprintf("t%d", time.Now().UnixNano())
	}
	return base64.RawURLEncoding.EncodeToString(b[:])
}

// recoverPanic converts a panic in application code into an error, so one bad
// handler takes down one session instead of the process (RF-E5).
func recoverPanic(err *error) {
	if r := recover(); r != nil {
		*err = &PanicError{Value: r, Stack: debug.Stack()}
	}
}

// wireOpenEvent builds the envelope for the capability event. The retry field
// is set here so a client learns its backoff from the very first bytes, before
// anything can go wrong.
func wireOpenEvent(s *Session) wire.Event {
	return wire.Event{
		Name:  s.cfg.prefix + "open",
		Retry: jitterDelay(s.cfg.retry, s.cfg.retryJitter),
	}
}

// isExpectedEnd reports whether an error is the ordinary way a stream finishes
// rather than something an operator should look at.
func isExpectedEnd(err error) bool {
	return errors.Is(err, ErrClientGone) ||
		errors.Is(err, ErrWriteTimeout) ||
		errors.Is(err, ErrSessionClosed) ||
		errors.Is(err, ErrShuttingDown) ||
		errors.Is(err, context.Canceled)
}
