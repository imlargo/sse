package sse

import (
	"context"
	"fmt"
	"slices"
	"strings"
)

// Declare names an event type and binds it to its payload type.
//
//	var (
//	    TicketCreated = sse.Declare[Ticket]("ticket.created")
//	    BuildFinished = sse.Declare[Build]("build.finished")
//	)
//
//	TicketCreated.Publish(ctx, broker, topic, ticket)   // will not compile with a Build
//
// Declaring is worth doing for three separate reasons, and the third is the one
// no Go library offers: publishing the wrong payload for an event name stops
// being possible, the connection event can tell a client what the stream emits,
// and the whole set can be emitted as an OpenAPI document by the openapi
// subpackage.
//
// Nothing here reflects on the hot path. The declaration captures a zero value
// of T at construction; only the document generator ever looks at it, and that
// runs at startup or from a command, never per event.
func Declare[T any](name string) EventType[T] {
	return EventType[T]{name: name}
}

// An EventType is a declared event name together with its payload type.
type EventType[T any] struct {
	name        string
	description string
	topic       string
}

// WithDescription returns a copy carrying prose for the generated document.
func (e EventType[T]) WithDescription(s string) EventType[T] {
	e.description = s
	return e
}

// OnTopic returns a copy recording which topic pattern this event is published
// to, for the generated document. It does not constrain publishing.
func (e EventType[T]) OnTopic(pattern string) EventType[T] {
	e.topic = pattern
	return e
}

// Name returns the event name as it appears on the wire.
func (e EventType[T]) Name() string { return e.name }

// Describe implements [Declaration].
func (e EventType[T]) Describe() EventDescription {
	var zero T
	return EventDescription{
		Name:        e.name,
		Description: e.description,
		Topic:       e.topic,
		Sample:      zero,
	}
}

// Publish appends a value of the declared type to a broker's log.
func (e EventType[T]) Publish(ctx context.Context, b *Broker, topic Topic, v T, opts ...SendOption) (Offset, error) {
	return b.Publish(ctx, topic, v, append(opts, Name(e.name))...)
}

// PublishTo appends a value of the declared type to a publisher's log, for a
// stream with no topics.
func (e EventType[T]) PublishTo(ctx context.Context, p *Publisher, v T, opts ...SendOption) (Offset, error) {
	return p.Publish(ctx, v, append(opts, Name(e.name))...)
}

// Send writes a value of the declared type directly to one session.
func (e EventType[T]) Send(ctx context.Context, s *Session, v T, opts ...SendOption) error {
	return s.Send(ctx, v, append(opts, Name(e.name))...)
}

// A Declaration is one entry in a [Catalog]. [EventType] implements it; the
// interface exists so declarations of different payload types can be collected
// together, which a slice of a generic type cannot do.
type Declaration interface {
	Describe() EventDescription
}

// An EventDescription is a declaration flattened for tooling.
type EventDescription struct {
	// Name is the event name on the wire.
	Name string
	// Description is prose for the generated document.
	Description string
	// Topic is the topic pattern this event is published to, if recorded.
	Topic string
	// Sample is the zero value of the payload type. It carries the type
	// without the core ever importing reflect: the document generator is the
	// only thing that looks at it.
	Sample any
}

// A Catalog is everything a stream emits, declared in one place.
//
// It is the single source for the event names reported when a client connects
// and for the generated API document, so those cannot drift from each other or
// from the code (RF-G6).
type Catalog struct {
	entries []EventDescription
	byName  map[string]int
}

// NewCatalog collects declarations. It panics on a duplicate or malformed name,
// which at startup can only be a programming error.
func NewCatalog(decls ...Declaration) *Catalog {
	c, err := NewCatalogWith(decls...)
	if err != nil {
		panic(err)
	}
	return c
}

// NewCatalogWith is [NewCatalog] for a set built at runtime.
func NewCatalogWith(decls ...Declaration) (*Catalog, error) {
	c := &Catalog{byName: make(map[string]int, len(decls))}
	for _, d := range decls {
		if d == nil {
			return nil, fmt.Errorf("sse: NewCatalog: nil declaration")
		}
		e := d.Describe()
		if e.Name == "" {
			return nil, fmt.Errorf("sse: NewCatalog: an event was declared with no name")
		}
		if strings.ContainsAny(e.Name, "\r\n") {
			return nil, fmt.Errorf("sse: NewCatalog: event name %q must fit on one line", e.Name)
		}
		if _, dup := c.byName[e.Name]; dup {
			return nil, fmt.Errorf("sse: NewCatalog: %q is declared twice; "+
				"each event name must map to exactly one payload type", e.Name)
		}
		c.byName[e.Name] = len(c.entries)
		c.entries = append(c.entries, e)
	}
	return c, nil
}

// Events returns the declarations, ordered as given.
func (c *Catalog) Events() []EventDescription {
	if c == nil {
		return nil
	}
	return slices.Clone(c.entries)
}

// Names returns the declared event names.
func (c *Catalog) Names() []string {
	if c == nil {
		return nil
	}
	out := make([]string, len(c.entries))
	for i, e := range c.entries {
		out[i] = e.Name
	}
	return out
}

// Declares reports whether name is in the catalog.
func (c *Catalog) Declares(name string) bool {
	if c == nil {
		return true // no catalog means nothing is constrained
	}
	_, ok := c.byName[name]
	return ok
}

// check enforces the catalog on a publish.
//
// This is a runtime check, and calling it a compile-time one would be a lie.
// The payload type *is* checked at compile time, by going through
// [EventType.Publish]; what cannot be checked there is a raw publish that names
// an event the catalog never declared, because Go cannot express "this string
// is a member of that set" in a type. So it is a map lookup, and the error says
// which names exist.
func (c *Catalog) check(name string) error {
	if c == nil || name == "" {
		return nil
	}
	if _, ok := c.byName[name]; ok {
		return nil
	}
	return fmt.Errorf("%w: %q is not in the catalog; declared events are %s",
		ErrUndeclaredEvent, name, strings.Join(c.Names(), ", "))
}
