package sse

import (
	"context"
	"fmt"
	"strings"

	"github.com/imlargo/sse/wire"
)

// A Publisher turns application values into frames and appends them to a log.
//
// It is the write side of a stream, and it is deliberately separate from the
// read side: whoever produces events and whoever consumes them need not be the
// same request, the same goroutine or even the same node. That decoupling is
// what makes a language-model stream resumable — generation keeps running while
// the client is away, and reconnecting simply resumes reading.
//
// A Publisher is safe for concurrent use.
type Publisher struct {
	log Log
	cfg *config
}

// NewPublisher returns a Publisher writing to log.
//
// It panics on an invalid option, which can only be a programming error at
// startup. Use [NewPublisherWith] when the configuration is computed.
func NewPublisher(log Log, opts ...Option) *Publisher {
	p, err := NewPublisherWith(log, opts...)
	if err != nil {
		panic(err)
	}
	return p
}

// NewPublisherWith is [NewPublisher] for configuration that can fail.
func NewPublisherWith(log Log, opts ...Option) (*Publisher, error) {
	if log == nil {
		return nil, fmt.Errorf("sse: NewPublisher: log must not be nil")
	}
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}
	return &Publisher{log: log, cfg: cfg}, nil
}

// Log returns the log this publisher writes to.
func (p *Publisher) Log() Log { return p.log }

// Publish serializes v, appends it to the log, and returns the offset it was
// stored at.
//
// The name says what it does. It reports that the event is *in the log*, not
// that any client received it: nothing in this API can confirm delivery, so
// nothing in it is named as though it could (RNF-13).
//
// The value is handled exactly as [Session.Send] handles it — a [Payload] is
// used as-is, anything else goes through the codec — and serialization happens
// once here, before any subscriber is involved, which is what keeps fan-out to
// a single encoding.
func (p *Publisher) Publish(ctx context.Context, v any, opts ...SendOption) (Offset, error) {
	var o sendOpts
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	if o.name != "" && strings.HasPrefix(o.name, p.cfg.prefix) {
		return 0, fmt.Errorf("%w: %q starts with the reserved prefix %q",
			ErrReservedName, o.name, p.cfg.prefix)
	}

	if err := p.cfg.catalog.check(o.name); err != nil {
		return 0, err
	}

	payload, ok := v.(Payload)
	if !ok {
		payload = valuePayload{v}
	}

	data := getBuf()
	body, err := payload.appendData((*data)[:0], p.cfg.codec)
	if err != nil {
		putBuf(data)
		return 0, fmt.Errorf("sse: encoding payload: %w", err)
	}
	if body == nil {
		body = []byte{}
	}

	// The frame is encoded without an id: the id encodes the offset, and the
	// offset does not exist until the append below returns. Each subscriber
	// gets its own id line prepended at write time.
	encoded, err := wire.AppendEvent(nil, wire.Event{Name: o.name, Data: body})
	putBuf(data)
	if err != nil {
		return 0, err
	}
	if len(encoded) > p.cfg.maxEventSize {
		return 0, fmt.Errorf("%w: %d bytes exceeds the %d byte limit",
			ErrEventTooLarge, len(encoded), p.cfg.maxEventSize)
	}

	p.cfg.metrics.eventPublished(o.topic.String(), len(encoded))

	return p.log.Append(ctx, Frame{
		Body:      encoded,
		Topic:     o.topic.String(),
		Name:      o.name,
		Key:       o.key,
		Ephemeral: o.ephemeral,
	})
}

// WithTopic addresses an event to a topic, which is what subscribers select on.
//
// Publishing always names one concrete topic; wildcards belong to filters. That
// way which subscribers an event reaches is a property of who is listening and
// never of how it was addressed.
func WithTopic(t Topic) SendOption { return func(o *sendOpts) { o.topic = t } }

// Key groups events that supersede one another, for the coalescing
// backpressure policy: a newer event with the same key replaces an older one
// that a subscriber has not read yet.
//
// The key is given explicitly rather than derived from the payload by
// reflection or guessed from the event name. Nothing on this path inspects an
// application's types.
func Key(k string) SendOption { return func(o *sendOpts) { o.key = k } }

// Ephemeral marks an event as worth delivering but not worth keeping, so a
// high-frequency ticker can share a log with events that are retained for a
// long time.
func Ephemeral() SendOption { return func(o *sendOpts) { o.ephemeral = true } }
