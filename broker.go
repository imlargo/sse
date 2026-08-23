package sse

import (
	"context"
	"fmt"
	"net/http"
)

// A Broker publishes events to topics and lets subscribers select what they
// want.
//
// It is a log plus addressing, and that is the whole of it. Publishing appends
// to the log and returns; subscribers hold positions in it and filter as they
// read. There is no subscriber registry on the publish path, so an extra
// subscriber costs an offset and a write, and a slow one cannot affect anybody
// else.
//
// A Broker is safe for concurrent use.
type Broker struct {
	name string
	log  Log
	pub  *Publisher
	cfg  *config
}

// NewBroker returns a broker over log.
//
// The name identifies the log inside a resumption cursor. It is hashed rather
// than assigned, so it is stable across nodes and restarts with no
// coordination: a client can reconnect to a different replica and be understood.
//
// It panics on an invalid option, which at startup can only be a programming
// error. Use [NewBrokerWith] when the configuration is computed.
func NewBroker(name string, log Log, opts ...Option) *Broker {
	b, err := NewBrokerWith(name, log, opts...)
	if err != nil {
		panic(err)
	}
	return b
}

// NewBrokerWith is [NewBroker] for configuration that can fail.
func NewBrokerWith(name string, log Log, opts ...Option) (*Broker, error) {
	if name == "" {
		return nil, fmt.Errorf("sse: NewBroker: name must not be empty; it identifies the log in a resumption cursor")
	}
	if log == nil {
		return nil, fmt.Errorf("sse: NewBroker: log must not be nil")
	}
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}
	cfg.log = log
	cfg.logID = NewLogID(name)

	pub, err := NewPublisherWith(log, opts...)
	if err != nil {
		return nil, err
	}
	return &Broker{name: name, log: log, pub: pub, cfg: cfg}, nil
}

// Log returns the broker's log, which is the piece to replace to go from one
// node to many.
func (b *Broker) Log() Log { return b.log }

// Publish appends an event addressed to topic.
//
// It returns when the event is in the log, not when anyone received it. No
// subscriber is touched here at all, which is why one that has stopped reading
// cannot slow this down (RF-D3), and why the name does not suggest a
// confirmation it cannot give (RNF-13).
func (b *Broker) Publish(ctx context.Context, topic Topic, v any, opts ...SendOption) (Offset, error) {
	if topic.IsZero() {
		return 0, fmt.Errorf("sse: Publish: topic must not be empty; build one with sse.NewTopic")
	}
	return b.pub.publish(ctx, topic, v, opts)
}

// Subscribe streams the broker's log to a session, delivering only events the
// filters select. With no filters it delivers everything.
//
// The library owns the loop: resolving the cursor, declaring gaps, filtering,
// checkpointing the position past events this subscriber skipped, and the
// transition into live delivery.
func (b *Broker) Subscribe(ctx context.Context, s *Session, filters ...Filter) error {
	if s.log == nil {
		return fmt.Errorf("sse: Subscribe: the session has no log; build the handler with Broker.Handler")
	}
	s.following.Store(true)
	defer s.following.Store(false)
	return s.follow(ctx, filters)
}

// Handler returns an http.Handler that subscribes a client to this broker.
//
// Filters come from repeated "topic" query parameters, because EventSource
// cannot send headers:
//
//	new EventSource('/events?topic=org.42.>&topic=system.notices')
//
// With no parameter the client receives everything the broker publishes, which
// is what a single broadcast channel wants. Constraining that per client is the
// job of authorization.
func (b *Broker) Handler(opts ...Option) http.Handler {
	inner := b.HandlerFunc(func(ctx context.Context, s *Session) error {
		// An authorizer, when there is one, decides the subscription: it has
		// seen the request and can accept, narrow or refuse what was asked
		// for. Query parameters are the fallback for a stream with no
		// authorization at all.
		if g := s.Grant(); len(g.Filters) > 0 || len(g.Denied) > 0 {
			return b.Subscribe(ctx, s, g.Filters...)
		}
		filters, _ := s.Request().Context().Value(filtersKey{}).([]Filter)
		return b.Subscribe(ctx, s, filters...)
	}, opts...)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Parsed before the stream opens, so a bad filter is an ordinary 400
		// with an explanation. Once the response has been committed as a
		// 200 text/event-stream there is no status left to send, and a client
		// that receives a non-200 stops reconnecting permanently — so getting
		// this order wrong turns a typo into a client that never comes back
		// (RF-F1).
		filters, err := FiltersFromQuery(r, TopicQueryParam)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		inner.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), filtersKey{}, filters)))
	})
}

// filtersKey carries filters resolved before the stream opened through to the
// stream function.
type filtersKey struct{}

// HandlerFunc returns a handler bound to this broker's log, running fn as the
// stream. Use it when the subscription is decided by something other than the
// query string.
func (b *Broker) HandlerFunc(fn StreamFunc, opts ...Option) http.Handler {
	all := append([]Option{WithLog(b.name, b.log)}, b.cfg.inherited()...)
	if b.cfg.authorizer != nil {
		all = append(all, WithAuthorizer(b.cfg.authorizer))
	}
	if b.cfg.metrics != nil {
		all = append(all, WithMetrics(b.cfg.metrics))
	}
	return Handler(fn, append(all, opts...)...)
}

// TopicQueryParam is the query parameter subscribers use to select topics.
const TopicQueryParam = "topic"

// FiltersFromQuery reads topic filters from a request's query string.
//
// Filters travel there because EventSource cannot set headers. Each value is
// validated, so a malformed or oversized one is a 400 with an explanation
// rather than something that reaches the matcher (RF-B6).
func FiltersFromQuery(r *http.Request, param string) ([]Filter, error) {
	if r == nil {
		return nil, nil
	}
	values := r.URL.Query()[param]
	if len(values) == 0 {
		return nil, nil
	}
	if len(values) > MaxTopicTokens {
		return nil, fmt.Errorf("sse: %d %q parameters, at most %d are accepted",
			len(values), param, MaxTopicTokens)
	}
	filters := make([]Filter, 0, len(values))
	for _, v := range values {
		f, err := NewFilter(v)
		if err != nil {
			return nil, err
		}
		filters = append(filters, f)
	}
	return filters, nil
}
