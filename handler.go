package sse

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime/debug"
	"time"
)

// A StreamFunc produces events for one client.
//
// It is called on its own goroutine once the stream is open, and the stream
// ends when it returns. Returning an error ends the session with that error;
// returning nil ends it cleanly.
//
// Note what it does not do: no loop over a library channel, no headers, no
// flush, no heartbeat, no deadline. The loop it does contain is over the
// application's own source of data.
type StreamFunc func(ctx context.Context, s *Session) error

// Handler returns an http.Handler that runs fn as a server-sent event stream.
//
//	http.Handle("/chat", sse.Handler(func(ctx context.Context, s *sse.Session) error {
//	    for token := range model.Stream(ctx, prompt) {
//	        if err := s.Send(ctx, sse.Text(token)); err != nil {
//	            return err
//	        }
//	    }
//	    return nil
//	}))
//
// Handler panics if an option is invalid, because options are constants
// evaluated once at startup and a bad one is a programming error, not a runtime
// condition. Use [NewHandler] when the configuration is computed.
//
// The handler does not care which HTTP method was used. Streaming over POST is
// a first-class case: MCP relies on it.
func Handler(fn StreamFunc, opts ...Option) http.Handler {
	h, err := NewHandler(fn, opts...)
	if err != nil {
		panic(err)
	}
	return h
}

// NewHandler is [Handler] for configuration that is computed rather than
// constant, and so can fail.
func NewHandler(fn StreamFunc, opts ...Option) (http.Handler, error) {
	if fn == nil {
		return nil, fmt.Errorf("sse: NewHandler: stream function must not be nil")
	}
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}
	return &handler{fn: fn, cfg: cfg}, nil
}

type handler struct {
	fn  StreamFunc
	cfg *config
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Probe before committing a status. A stream that cannot be flushed is
	// worse than no stream: it looks like it works and delivers nothing.
	t, err := newNetHTTPTransport(w)
	if err != nil {
		h.cfg.logger.ErrorContext(r.Context(), "sse: refusing to open a stream", "error", err)
		http.Error(w, "streaming is not supported by this server configuration",
			http.StatusInternalServerError)
		return
	}

	// Allow the application to read the request body while the response is
	// being written. Without it the standard library may consume the body
	// before the handler runs, which breaks streaming over POST.
	if h.cfg.fullDuplex {
		_ = http.NewResponseController(w).EnableFullDuplex()
	}

	_ = serveHTTP(r.Context(), t, w, r, h.fn, h.cfg)
}

// serveHTTP runs the authorizer, which can still refuse with a status, and then
// opens the stream.
func serveHTTP(ctx context.Context, t Transport, w http.ResponseWriter, r *http.Request,
	fn StreamFunc, cfg *config) error {

	grant, err := authorizeRequest(r, cfg)
	if err != nil {
		code, message := statusOf(err)
		if code >= 500 {
			cfg.logger.ErrorContext(ctx, "sse: authorizer failed", "error", err)
		}
		// Sent while there is still a status to send. After the stream opens
		// there is none, and a non-200 would stop the client reconnecting for
		// good.
		http.Error(w, message, code)
		return err
	}
	return serve(ctx, t, r, fn, cfg, grant)
}

// authorizeRequest applies the configured authorizer, containing a panic in it
// the same way a panic in a stream function is contained (RF-E5).
func authorizeRequest(r *http.Request, cfg *config) (grant Grant, err error) {
	if cfg.authorizer == nil {
		return Grant{}, nil
	}
	defer func() {
		if rec := recover(); rec != nil {
			err = &PanicError{Value: rec, Stack: debug.Stack()}
		}
	}()
	grant, err = cfg.authorizer(r)
	if err != nil {
		return Grant{}, err
	}
	if err := grant.validate(); err != nil {
		return Grant{}, err
	}
	return grant, nil
}

// Serve runs fn as a stream over an arbitrary transport.
//
// It is what [Handler] uses underneath, and it is the entry point for adapters
// to frameworks that are not built on net/http. An adapter's whole job is to
// implement [Transport]; everything else — headers, flushing, heartbeats, write
// deadlines, drain on shutdown, disconnection detection — is handled here, the
// same way for every framework.
//
// This matters most where client disconnection is not observable through the
// request context, as on fasthttp: nothing here depends on ctx being cancelled,
// so an adapter that cannot report disconnection still cannot leak a session.
func Serve(ctx context.Context, t Transport, r *http.Request, fn StreamFunc, opts ...Option) error {
	return serveWithGrant(ctx, t, r, fn, Grant{}, opts...)
}

// ServeGrant is [Serve] for an adapter that has already run its own
// authorization and holds the result.
func ServeGrant(ctx context.Context, t Transport, r *http.Request, fn StreamFunc,
	grant Grant, opts ...Option) error {
	return serveWithGrant(ctx, t, r, fn, grant, opts...)
}

func serveWithGrant(ctx context.Context, t Transport, r *http.Request, fn StreamFunc,
	grant Grant, opts ...Option) error {
	if fn == nil {
		return fmt.Errorf("sse: Serve: stream function must not be nil")
	}
	if !t.Capabilities().Flush {
		return &UnsupportedWriterError{Chain: []string{fmt.Sprintf("%T", t)}}
	}
	cfg, err := newConfig(opts)
	if err != nil {
		return err
	}
	if cfg.authorizer != nil {
		g, err := authorizeRequest(r, cfg)
		if err != nil {
			return err
		}
		grant = g
	}
	return serve(ctx, t, r, fn, cfg, grant)
}

func serve(ctx context.Context, t Transport, r *http.Request, fn StreamFunc, cfg *config,
	grant Grant) error {

	bp := cfg.backpressure
	if grant.Backpressure != nil {
		// RF-D1: the policy is decidable per subscription, not only per server.
		bp = *grant.Backpressure
	}

	s := &Session{
		grant:      grant,
		id:         newSessionID(),
		req:        r,
		cfg:        cfg,
		caps:       t.Capabilities(),
		t:          t,
		queue:      newSendQueue(bp),
		sendClosed: make(chan struct{}),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
		log:        cfg.log,
		logID:      cfg.logID,
	}

	// Work out where this client is before a single byte is written, so the
	// capability event can tell it the truth and any gap can be declared ahead
	// of everything else.
	s.resolveResumption(ctx, r)

	// The stream function runs under a context that ends when the session
	// does, so a graceful drain or an expiring credential unblocks it rather
	// than leaving it waiting on a log that will never produce anything.
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	s.cancelStream = cancelStream

	// A node that is already draining must still answer 200.
	//
	// This looks wasteful and is not. A client that receives any non-200
	// response stops reconnecting permanently, so answering a reconnection with
	// 503 during a rolling deploy does not move that client to another replica:
	// it kills it. Instead the stream opens, says what is happening and hands
	// back a jittered retry delay, and the client comes back somewhere else.
	draining := false
	if cfg.lifecycle != nil {
		if cfg.lifecycle.add(s) {
			// Removed explicitly at the end so the active count is reported
			// after the change rather than after the metrics call.
		} else {
			draining = true
		}
	}

	setStreamHeaders(t.Header(), r)
	t.WriteHeader(http.StatusOK)

	go s.pump()

	// RF-F3: a credential that expires mid-stream ends the session, and the
	// client reconnects by itself with a fresh one and resumes from its
	// cursor. The protocol's own reconnection turns expiry into a non-event.
	if !grant.Deadline.IsZero() {
		timer := time.AfterFunc(time.Until(grant.Deadline), func() {
			s.fail(errDeadlineReached)
			s.requestStop()
		})
		defer timer.Stop()
	}

	stats := SessionStats{
		SessionID: s.id,
		Identity:  grant.Identity,
		Resumable: s.resumable,
		Filters:   len(grant.Filters),
	}
	cfg.metrics.sessionOpened(stats)
	if cfg.lifecycle != nil {
		cfg.metrics.nodeSessionsActive(cfg.lifecycle.NodeSessionCount())
	}

	err := s.writeOpen()
	switch {
	case err != nil:
	case draining:
		// The drain path writes the queued open event, then the closing notice.
		s.requestStop()
		err = ErrShuttingDown
	default:
		err = runGuarded(streamCtx, s, fn)
	}

	// No more events. Whatever is queued is still written before the writer exits.
	s.closeSend()
	<-s.done

	if err == nil {
		err = s.Err()
	}

	stats.Reason = reasonFor(err)
	cfg.metrics.sessionClosed(stats, err)
	if cfg.lifecycle != nil {
		cfg.lifecycle.remove(s)
		cfg.metrics.nodeSessionsActive(cfg.lifecycle.NodeSessionCount())
	}

	logEnd(ctx, cfg, s, err)
	return err
}

// runGuarded calls the application's stream function with its panic contained,
// so one bad handler ends one session instead of the process (RF-E5).
func runGuarded(ctx context.Context, s *Session, fn StreamFunc) (err error) {
	defer recoverPanic(&err)
	return fn(ctx, s)
}

func logEnd(ctx context.Context, cfg *config, s *Session, err error) {
	switch {
	case err == nil:
		return
	case isExpectedEnd(err):
		// A client going away is how streams normally end, not a failure.
		cfg.logger.DebugContext(ctx, "sse: stream ended", "session", s.ID(), "reason", err)
	default:
		cfg.logger.ErrorContext(ctx, "sse: stream failed", "session", s.ID(), "error", err)
	}
}

// StreamHeaders returns the response headers a stream needs.
//
// It is exported for adapters to frameworks that must set headers before the
// body starts, which is the case for anything built on fasthttp. Using this
// rather than writing the same headers by hand is what keeps an adapter from
// drifting from the core: there is one definition and adapters apply it.
func StreamHeaders(r *http.Request) http.Header {
	h := make(http.Header, 4)
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	// Defeat nginx's response buffering, which otherwise holds the stream until
	// its buffer fills and makes the whole thing look broken.
	h.Set("X-Accel-Buffering", "no")
	if r != nil && r.ProtoMajor == 1 {
		// Connection is a hop-by-hop header and is forbidden under HTTP/2.
		h.Set("Connection", "keep-alive")
	}
	return h
}

func setStreamHeaders(h http.Header, r *http.Request) {
	for k, vs := range StreamHeaders(r) {
		h[k] = vs
	}
}

// writeOpen tells the client what it has been given, before any application
// event: the session identity, whether resumption is on offer, and what
// delivery semantics actually apply (RF-E3).
//
// Saying it out loud is the point. A client that is never told it cannot resume
// will assume it can.
func (s *Session) writeOpen() error {
	if n := s.cfg.padding; n > 0 {
		// Padding is off by default. Some old proxies and XHR polyfills deliver
		// nothing until a volume of bytes has accumulated; this is for them.
		pad := getBuf()
		out := append((*pad)[:0], ':')
		for range n {
			out = append(out, ' ')
		}
		out = append(out, '\n')
		*pad = out
		if err := s.enqueue(context.Background(), pad); err != nil {
			return err
		}
	}

	body, err := json.Marshal(s.capabilities())
	if err != nil {
		return err
	}
	return s.emit(context.Background(), wireOpenEvent(s), Raw(body))
}

// openPayload is what the client is told the moment the stream opens.
//
// It exists because a client that is never told it cannot resume will assume it
// can. Stating the delivery semantics out loud is run-time self-documentation,
// and it is the difference between a weaker guarantee that is understood and a
// stronger one that is implied and untrue (RF-E3, RF-C2).
type openPayload struct {
	SessionID string `json:"sessionId"`
	Resumable bool   `json:"resumable"`
	Delivery  string `json:"delivery"`
	Recovery  string `json:"recovery"`

	// RetentionMs and RetentionEvents describe the window the client can
	// actually come back into, present only when there is one.
	RetentionMs     int64 `json:"retentionMs,omitempty"`
	RetentionEvents int   `json:"retentionEvents,omitempty"`

	KeepAliveMs int64 `json:"keepAliveMs"`
	RetryMs     int64 `json:"retryMs"`

	// Granted and Denied say exactly what the subscription covers. RF-F2
	// forbids the alternative: a denied topic that simply never produces
	// events is indistinguishable from one that is merely quiet, and the
	// client waits forever for something that will never come.
	Granted []string `json:"granted,omitempty"`
	Denied  []Denial `json:"denied,omitempty"`

	// Events is what this stream declares it emits, from the same catalog the
	// API document is generated from, so the two cannot drift.
	Events []string `json:"events,omitempty"`

	Identity   string            `json:"identity,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

func (s *Session) capabilities() openPayload {
	p := openPayload{
		SessionID:   s.id,
		Resumable:   s.resumable,
		Delivery:    "at-most-once",
		Recovery:    "none",
		KeepAliveMs: s.cfg.keepAlive.Milliseconds(),
		RetryMs:     s.cfg.retry.Milliseconds(),
		Denied:      s.grant.Denied,
		Identity:    s.grant.Identity,
		Attributes:  s.grant.Attributes,
		Events:      s.cfg.catalog.Names(),
	}
	for _, f := range s.grant.Filters {
		p.Granted = append(p.Granted, f.String())
	}
	if s.resumable {
		// With retention the promise is at-least-once *within the window*, and
		// never more than that. Duplicates are possible on resume; exactly-once
		// is not on offer and is not implied.
		p.Delivery = "at-least-once-within-retention"
		p.Recovery = "replay"
		if info, err := s.log.Info(context.Background()); err == nil {
			p.RetentionMs = info.Retention.For.Milliseconds()
			p.RetentionEvents = info.Retention.Events
		}
	}
	return p
}
