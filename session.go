package sse

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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

	// grant is what the authorizer decided, resolved before the stream opened.
	grant Grant

	// Resumption state, resolved once before anything is written.
	log       Log
	logID     LogID
	epoch     Epoch
	resumeGap *Gap
	resumable bool

	// presented is the position the client arrived with. It never changes.
	presented Cursor

	// carried holds the positions this session does not own — logs the client
	// has a position in that are not the one being followed. Fixed once
	// resolved, so the live cursor is these plus one offset.
	carried []CursorEntry

	// pos is where this session has reached in its own log. It is an integer
	// rather than a Cursor because it moves on every event, including the ones
	// a subscriber filters out, and rebuilding and encoding a cursor for an
	// event nobody receives is pure waste.
	//
	// The writer goroutine reads it to emit a checkpoint, so it is atomic.
	pos atomic.Uint64

	// checkpointDue says the position has moved past what the client has been
	// told. The keep-alive clears it by writing an id with no data, which the
	// specification commits to the client without dispatching an event.
	checkpointDue atomic.Bool

	// cursorScratch belongs to the follow goroutine, encodeScratch to the
	// writer. They are separate so neither needs a lock.
	cursorScratch []byte
	encodeScratch []byte

	// resub carries a new filter set from outside the session. EventSource
	// cannot send anything, so changing a subscription has to arrive through a
	// side channel — which is only possible because a session is addressable
	// by id.
	resub atomic.Pointer[[]Filter]

	// reader is what the follow loop is currently blocked on. Resubscribe
	// closes it to interrupt that block; the loop then reopens at the same
	// position.
	readerMu sync.Mutex
	reader   Reader

	// batch is the writer's scratch space for gathering queued events into one
	// write, and gathered records what to report once that write succeeds.
	// Both belong to the writer goroutine alone.
	batch    []byte
	gathered []delivery

	// following guards against mixing two sources of truth on one session:
	// while a log is being streamed, ids come from the log, and an event sent
	// directly would carry none and be silently unrecoverable on resume.
	following atomic.Bool

	queue *sendQueue

	sendClosed    chan struct{}
	closeSendOnce sync.Once

	stop     chan struct{}
	stopOnce sync.Once

	// cancelStream unblocks the application's stream function when the session
	// is told to stop. Stopping the writer is not enough on its own: a stream
	// function waiting on a log would otherwise sit there until its own
	// context ended, which for a graceful drain or an expiring credential is
	// never.
	cancelStream context.CancelFunc

	done chan struct{} // closed when the writer goroutine has exited

	mu  sync.Mutex
	err error
}

// A SendOption adjusts a single event.
type SendOption func(*sendOpts)

type sendOpts struct {
	name      string
	key       string
	topic     Topic
	ephemeral bool
}

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

	if err := s.cfg.catalog.check(o.name); err != nil {
		return err
	}
	if s.following.Load() {
		return ErrFollowing
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
	return s.enqueueFrame(ctx, queuedFrame{buf: buf})
}

func (s *Session) enqueueFrame(ctx context.Context, qf queuedFrame) error {
	select {
	case <-s.done:
		putBuf(qf.buf)
		return s.endErr()
	case <-s.sendClosed:
		putBuf(qf.buf)
		return ErrSessionClosed
	case <-s.stop:
		putBuf(qf.buf)
		return ErrShuttingDown
	default:
	}
	err := s.queue.push(ctx, qf, s.done)
	if errors.Is(err, ErrSessionClosed) {
		// The queue closes when the writer exits. If the writer exited because
		// the connection failed, that is the useful error, not the generic
		// one: the caller wants to know the client is gone (RF-A9).
		return s.endErr()
	}
	return err
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

// setReader records what the follow loop is blocked on, so it can be
// interrupted from outside.
func (s *Session) setReader(r Reader) {
	s.readerMu.Lock()
	s.reader = r
	s.readerMu.Unlock()
}

// wakeFollower interrupts the follow loop by closing the reader it is waiting
// on, which is the only thing that can unblock it short of an event arriving.
func (s *Session) wakeFollower() {
	s.readerMu.Lock()
	r := s.reader
	s.readerMu.Unlock()
	if r != nil {
		_ = r.Close()
	}
}

// Stop asks the session to finish: it drains whatever is queued, tells the
// client the stream is closing and hands it a jittered reconnection delay.
//
// It is how an application ends a session from outside — a signed-out user, a
// revoked token, a tenant being suspended. The client reconnects by itself
// through the protocol's own mechanism, so this is a way to force fresh
// credentials rather than a way to cut somebody off.
func (s *Session) Stop() { s.requestStop() }

// requestStop asks the session to drain and finish, which is what a graceful
// shutdown does to every live session.
func (s *Session) requestStop() {
	s.stopOnce.Do(func() {
		close(s.stop)
		if s.cancelStream != nil {
			s.cancelStream()
		}
	})
}

// pump owns the transport. It is the only goroutine that writes to it.
//
// It runs on its own goroutine, which is why it recovers. Everything it calls
// out to is somebody else's code — a [Transport] from a framework adapter, a
// [Metrics] hook from an exporter — and a panic on a bare goroutine cannot be
// recovered by the handler that started it. Without this, one adapter bug ends
// the process and every other connection on it, rather than the one session
// that hit it (RF-E5).
func (s *Session) pump() {
	defer close(s.done)
	defer s.queue.close()
	defer func() {
		if r := recover(); r != nil {
			s.fail(&PanicError{Value: r, Stack: debug.Stack()})
			s.cfg.logger.Error("sse: panic in the connection writer; the session was ended",
				"session", s.id, "panic", r)
		}
	}()

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
		case <-s.queue.signal:
			// A signal does not always mean there is something to write: a
			// policy that discards an arriving event signals too, so the writer
			// can report the drop. Resetting the timer on those would let a
			// stream that is only discarding go silent indefinitely, and the
			// keep-alive is also the liveness probe.
			wrote, err := s.flushQueue()
			if err != nil {
				s.fail(err)
				return
			}
			if wrote {
				resetKeepAlive()
			}

		case <-tick:
			// The heartbeat is also the liveness probe. A peer that has gone
			// away is discovered here, in every environment, whether or not the
			// request context ever fires.
			//
			// It doubles as a cursor checkpoint. The specification commits an
			// id to the client before deciding whether the block carries data,
			// so an id with no data advances the client's resumption position
			// without dispatching an event. That is what stops a subscriber
			// whose filter matches nothing from resuming far in the past and
			// rescanning everything it already skipped.
			if err := s.writeKeepAlive(); err != nil {
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
	if _, err := s.flushQueue(); err != nil {
		s.fail(err)
		return
	}
	if s.stopRequested() {
		if err := s.writeClosing(); err != nil {
			s.fail(err)
		}
	}
}

// maxBatchBytes caps how much is gathered into one write. Large enough that a
// burst becomes one syscall, small enough that the scratch buffer stays a
// sensible size to keep per session.
const maxBatchBytes = 64 << 10

// flushQueue writes everything currently queued.
//
// Whatever is already waiting goes out in a single write and a single flush
// rather than one of each per event (RF-A10). The cost of an event on the wire
// is dominated by the syscall, not by the formatting, so a burst of twenty
// events becomes one syscall instead of twenty. A stream that produces one
// event at a time is unaffected: there is nothing to gather.
func (s *Session) flushQueue() (wrote bool, err error) {
	for {
		qf, ok := s.queue.pop()
		if !ok {
			return wrote, nil
		}

		batch := s.batch[:0]
		gathered := s.gathered[:0]

		batch = append(batch, *qf.buf...)
		gathered = append(gathered, delivery{topic: qf.topic, published: qf.published, size: len(*qf.buf)})
		putBuf(qf.buf)

		// Gather whatever else is already waiting. Only what is there now;
		// nothing is ever held back hoping more will arrive, because latency
		// matters more than syscalls on a quiet stream.
		for len(batch) < maxBatchBytes {
			next, ok := s.queue.pop()
			if !ok {
				break
			}
			batch = append(batch, *next.buf...)
			gathered = append(gathered, delivery{topic: next.topic, published: next.published, size: len(*next.buf)})
			putBuf(next.buf)
		}
		s.batch, s.gathered = batch, gathered

		if err := s.writeBytes(batch); err != nil {
			// Nothing in this batch reached the client, so nothing in it is
			// reported as delivered. Counting first and writing afterwards
			// would make the delivery metric a count of attempts, which is
			// precisely the number nobody wants.
			return wrote, err
		}
		wrote = true

		now := time.Now()
		for _, d := range gathered {
			if d.published.IsZero() {
				continue
			}
			// Publication to delivery, which is the number that says whether
			// subscribers are keeping up.
			s.cfg.metrics.eventDelivered(d.topic, d.size, now.Sub(d.published))
		}

		s.releaseBatch()
		events, bytes := s.queue.depth()
		s.cfg.metrics.queueDepth(s.id, events, bytes)
	}
}

// a delivery records what has to be reported once a batch is actually on the
// wire, since the frames themselves go back to the pool before that.
type delivery struct {
	topic     string
	published time.Time
	size      int
}

// releaseBatch stops one large burst from pinning its scratch space for the
// life of the connection.
//
// The buffers are per session, so at tens of thousands of connections a
// megabyte each that is never handed back is the difference between a server
// that holds and one that does not. The threshold matches the pool's.
func (s *Session) releaseBatch() {
	if cap(s.batch) > maxBatchBytes {
		s.batch = nil
	}
	if cap(s.gathered) > 1024 {
		s.gathered = nil
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

func (s *Session) writeBytes(b []byte) error {
	if s.caps.WriteDeadline && s.cfg.writeTimeout > 0 {
		// Without this, a client that stops reading but never closes pins the
		// goroutine and the connection forever. The request context does not
		// help: the write simply blocks.
		if err := s.t.SetWriteDeadline(time.Now().Add(s.cfg.writeTimeout)); err != nil {
			// The transport said it could bound writes and then could not, so
			// this write has no bound. Ending the session is the safe answer:
			// the alternative is a goroutine and a connection held until the
			// process dies, which is the failure the deadline exists for.
			s.caps.WriteDeadline = false
			return fmt.Errorf("%w: the transport stopped accepting write deadlines: %v",
				ErrWriteTimeout, err)
		}
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

// appendCursor writes this session's current position into dst.
//
// It allocates nothing given a buffer with room, which matters because it runs
// on every delivered event.
func (s *Session) appendCursor(dst []byte, offset Offset) []byte {
	mine := CursorEntry{Log: s.logID, Epoch: s.epoch, Offset: offset}
	if len(s.carried) == 0 {
		// The overwhelming common case: one log, so one entry, and no sorting
		// or merging to do.
		dst = append(dst, cursorPrefix...)
		return appendCursorBody(dst, []CursorEntry{mine})
	}
	return NewCursor(append(slices.Clone(s.carried), mine)...).AppendTo(dst)
}

// writeKeepAlive emits a cursor checkpoint if the position has moved past what
// the client has been told, and a bare comment otherwise.
//
// The checkpoint is built here rather than when the position moved. A
// subscriber with a narrow filter skips far more events than it receives, and
// encoding a cursor for each one — to overwrite it microseconds later and read
// it once every fifteen seconds — is work for nobody.
func (s *Session) writeKeepAlive() error {
	if !s.checkpointDue.Swap(false) {
		return s.writeBytes(keepAliveLine)
	}

	out := append(s.encodeScratch[:0], "id: "...)
	out = s.appendCursor(out, Offset(s.pos.Load()))
	// The first newline ends the id line; the second is the blank line that
	// dispatches the block. With no data field the client suppresses the event
	// but still commits the id, which is what advances its position without
	// delivering anything.
	out = append(out, '\n', '\n')
	s.encodeScratch = out

	// The id occupies one line; nothing here can produce a newline, but the
	// wire package owns that invariant so it is asserted rather than assumed.
	if bytes.ContainsAny(out[:len(out)-1], "\r") {
		return s.writeBytes(keepAliveLine)
	}
	return s.writeBytes(out)
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
		errors.Is(err, ErrSlowConsumer) ||
		errors.Is(err, ErrSessionClosed) ||
		errors.Is(err, ErrShuttingDown) ||
		// A grant expiring is the design working, not a fault: the client
		// reconnects with fresh credentials. Logging it as an error would
		// mean one error line per session per token lifetime, which at any
		// scale drowns the lines that matter.
		errors.Is(err, errDeadlineReached) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}
