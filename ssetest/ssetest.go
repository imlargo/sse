// Package ssetest provides an in-memory transport for driving sessions in
// tests.
//
// It exists because testing/synctest forbids real network use, and the
// behaviour worth testing here — heartbeat cadence, write deadlines, drain
// timing, reconnection backoff — is all time-dependent. Running it against a
// real socket means sleeping in tests, which is slow and flaky; running it in a
// synctest bubble against this transport is instant and deterministic.
//
// It is also the reference for what a framework adapter has to implement.
package ssetest

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/imlargo/sse"
	"github.com/imlargo/sse/wire"
)

// A Conn is an in-memory [sse.Transport] whose failure modes can be driven from
// a test.
type Conn struct {
	mu      sync.Mutex
	header  http.Header
	status  int
	out     bytes.Buffer
	writes  int
	flushes int

	deadline time.Time
	caps     sse.Capabilities

	stalled  bool
	throttle time.Duration
	failed   error
	closed   chan struct{}
}

// NewConn returns a transport that can flush and honour write deadlines, which
// is what a healthy net/http connection looks like.
func NewConn() *Conn {
	return &Conn{
		header: make(http.Header),
		caps:   sse.Capabilities{Flush: true, WriteDeadline: true},
		closed: make(chan struct{}),
	}
}

// NewConnNoDeadline returns a transport that can flush but cannot bound a
// write, which is what an adapter over an engine without deadline support looks
// like. A stalled peer is then only detectable when the write itself fails.
func NewConnNoDeadline() *Conn {
	c := NewConn()
	c.caps = sse.Capabilities{Flush: true, WriteDeadline: false}
	return c
}

func (c *Conn) Header() http.Header            { return c.header }
func (c *Conn) Capabilities() sse.Capabilities { return c.caps }

func (c *Conn) WriteHeader(status int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.status == 0 {
		c.status = status
	}
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	if !c.caps.WriteDeadline {
		return http.ErrNotSupported
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deadline = t
	return nil
}

func (c *Conn) Write(p []byte) (int, error) {
	c.mu.Lock()
	stalled, deadline, failed, throttle := c.stalled, c.deadline, c.failed, c.throttle
	c.mu.Unlock()

	if failed != nil {
		return 0, failed
	}

	// A client that reads, but slowly. This is the interesting case for
	// backpressure: a fully dead connection never observes what a policy did,
	// because nothing can be written to tell it.
	if throttle > 0 && !stalled {
		select {
		case <-time.After(throttle):
		case <-c.closed:
			return 0, io.ErrClosedPipe
		}
	}

	if stalled {
		// A peer that stopped reading but never closed. With a deadline the
		// write eventually fails; without one it blocks until the connection
		// is torn down, which is exactly the case that pins a goroutine
		// forever when nothing bounds the write.
		if c.caps.WriteDeadline && !deadline.IsZero() {
			if d := time.Until(deadline); d > 0 {
				select {
				case <-time.After(d):
				case <-c.closed:
					return 0, io.ErrClosedPipe
				}
			}
			return 0, os.ErrDeadlineExceeded
		}
		<-c.closed
		return 0, io.ErrClosedPipe
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes++
	return c.out.Write(p)
}

func (c *Conn) Flush() error {
	c.mu.Lock()
	failed := c.failed
	if failed == nil {
		c.flushes++
	}
	c.mu.Unlock()
	return failed
}

// Throttle makes every write take d, simulating a client that reads but cannot
// keep up.
func (c *Conn) Throttle(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.throttle = d
}

// Stall makes every subsequent write hang, simulating a client that stopped
// reading without closing.
func (c *Conn) Stall() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stalled = true
}

// Fail makes every subsequent write and flush return err, simulating a peer
// that went away.
func (c *Conn) Fail(err error) {
	if err == nil {
		err = errors.New("ssetest: connection failed")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failed = err
}

// Close releases any write blocked on a stall.
func (c *Conn) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
}

// Status returns the committed response status.
func (c *Conn) Status() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.status
}

// Flushes returns how many times the stream has been flushed. A stream that
// writes without flushing is not a stream.
func (c *Conn) Flushes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flushes
}

// String returns everything written so far, as raw wire bytes.
func (c *Conn) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.out.String()
}

// Messages parses what has been written the way a conforming client would, so
// assertions are made against what the client actually sees rather than against
// the bytes on the wire.
func (c *Conn) Messages() []wire.Message {
	var out []wire.Message
	d := wire.NewDecoder(bytes.NewReader([]byte(c.String())))
	for {
		m, err := d.Next()
		if err != nil {
			return out
		}
		out = append(out, m)
	}
}

// Comments returns the comment lines written so far, which is where keep-alives
// show up.
func (c *Conn) Comments() []string {
	var out []string
	for line := range bytes.SplitSeq([]byte(c.String()), []byte("\n")) {
		if len(line) > 0 && line[0] == ':' {
			out = append(out, string(line[1:]))
		}
	}
	return out
}
