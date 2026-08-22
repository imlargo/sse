// Package wire implements the text/event-stream format defined by the WHATWG
// HTML standard, section 9.2 "Server-sent events".
//
// It is deliberately standalone and depends only on the standard library: the
// wire format has value on its own for anyone who only needs to produce or
// consume the stream, and keeping it separate lets the conformance suite run
// without a server.
//
// The encoder is the primitive the rest of the library is built on.
// [AppendEvent] writes one event into a caller-supplied buffer without
// allocating, which is what lets a single encoded frame be shared by every
// subscriber of a broadcast instead of being re-encoded per subscriber.
//
// # Three details that implementations commonly get wrong
//
// A block with no data field at all is suppressed by the client, but a data
// field with an empty value is not: it dispatches an event whose data is the
// empty string. [Event.Data] models this with nil versus empty.
//
// The last event ID is committed to the client before the empty-data check, so
// an event carrying only an id advances the client's resumption cursor without
// dispatching anything. That is what makes cheap out-of-band checkpointing
// possible; see [ID].
//
// A client strips exactly one space after the colon, so the encoder always
// emits "data: " with the space. Emitting it conditionally would silently eat a
// character from every payload that begins with a space.
package wire

import (
	"bytes"
	"strings"
	"time"
	"unicode/utf8"
)

// Event is a single event ready to be written to a stream.
//
// The zero Event is invalid: encoding it returns [ErrEmptyEvent], because it
// would put a bare blank line on the wire.
type Event struct {
	// Name is the event type. When empty, no event field is written and the
	// client dispatches the default "message" type.
	Name string

	// Data is the payload, encoded as UTF-8. Line feeds split it across
	// repeated data fields, which the client rejoins with line feeds.
	//
	// Nil and empty mean different things, and the difference is observable by
	// the client:
	//
	//	nil          no data field is written; the client suppresses the event
	//	[]byte{}     one empty data field; the client dispatches data == ""
	//
	// A carriage return is rejected. The event stream data model cannot carry
	// one: the client rebuilds the payload by joining data lines with line
	// feeds, so a CR would either be lost or reframe the stream. Use
	// [NormalizeNewlines] to convert, or encode the payload (JSON escapes it).
	Data []byte

	// ID sets the client's resumption cursor. See [ID] for the three states.
	ID ID

	// Retry tells the client how long to wait before reconnecting. Zero or
	// negative writes no retry field. Values below one millisecond are
	// rejected, because the wire unit is integer milliseconds.
	Retry time.Duration

	// Comment is written as a colon-prefixed line before the fields. Clients
	// ignore it, which makes it the standard way to keep an idle connection
	// alive through proxies.
	Comment string
}

// ID is an event id, with the three states the specification distinguishes.
//
//	[NoID]      no id field is written; the client keeps its current cursor
//	[NewID]     the client's cursor becomes this value
//	[ResetID]   an empty id field; the client's cursor is cleared, so its next
//	            reconnection sends no Last-Event-ID header at all
//
// The zero value is [NoID].
type ID struct {
	val string
	set bool
}

// NoID returns an ID that writes no id field.
func NoID() ID { return ID{} }

// ResetID returns an ID that writes an empty id field, clearing the client's
// resumption cursor. It is the specification's own mechanism for telling a
// client that its stored position is no longer resolvable.
func ResetID() ID { return ID{set: true} }

// NewID validates s and returns it as an ID.
func NewID(s string) (ID, error) {
	if err := checkID(s); err != nil {
		return ID{}, err
	}
	return ID{val: s, set: true}, nil
}

// MustID is [NewID] for values known to be valid at compile time. It panics on
// an invalid value, so it must not be used with input from a request.
func MustID(s string) ID {
	id, err := NewID(s)
	if err != nil {
		panic(err)
	}
	return id
}

// IsSet reports whether an id field will be written.
func (id ID) IsSet() bool { return id.set }

// String returns the id value. It is empty both for [NoID] and for [ResetID];
// use [ID.IsSet] to tell them apart.
func (id ID) String() string { return id.val }

func checkID(s string) error {
	if i := strings.IndexByte(s, 0); i >= 0 {
		return fieldErr("id", ErrNUL,
			"A client ignores an id field containing NUL, so resumption would break silently. Strip the NUL or use a different id scheme.")
	}
	if i := strings.IndexByte(s, '\r'); i >= 0 {
		return fieldErr("id", ErrCarriageReturn,
			"An id must fit on one line. Use only characters that are safe in an HTTP header, since the client returns this value in Last-Event-ID.")
	}
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return fieldErr("id", ErrLineFeed,
			"An id must fit on one line. Use only characters that are safe in an HTTP header, since the client returns this value in Last-Event-ID.")
	}
	if !utf8.ValidString(s) {
		return fieldErr("id", ErrInvalidUTF8,
			"Event streams are always UTF-8. Encode the id, for example with base64url, which is also safe in headers and URLs.")
	}
	return nil
}

// Validate reports whether the event can be encoded. [AppendEvent] calls it, so
// callers only need it to check an event ahead of time.
func (ev Event) Validate() error {
	if ev.Name == "" && ev.Data == nil && !ev.ID.set && ev.Retry <= 0 && ev.Comment == "" {
		return ErrEmptyEvent
	}
	if err := checkLine("event", ev.Name); err != nil {
		return err
	}
	if err := checkLine("comment", ev.Comment); err != nil {
		return err
	}
	if ev.ID.set {
		if err := checkID(ev.ID.val); err != nil {
			return err
		}
	}
	if ev.Retry != 0 && ev.Retry < time.Millisecond {
		return fieldErr("retry", ErrRetryPrecision,
			"A client only accepts ASCII digits and reads them as milliseconds, so anything below 1ms would be written as \"retry: 0\" and make the client reconnect with no delay. Use at least time.Millisecond.")
	}
	if i := bytes.IndexByte(ev.Data, '\r'); i >= 0 {
		return fieldErr("data", ErrCarriageReturn,
			"A client rebuilds the payload by joining data lines with line feeds, so a carriage return cannot survive. Call wire.NormalizeNewlines first, or send the payload in an encoding that escapes it, such as JSON.")
	}
	if !utf8.Valid(ev.Data) {
		return fieldErr("data", ErrInvalidUTF8,
			"Event streams are always UTF-8 and there is no way to select another encoding. Encode binary payloads, for example with base64.")
	}
	return nil
}

func checkLine(field, s string) error {
	if strings.IndexByte(s, '\r') >= 0 {
		return fieldErr(field, ErrCarriageReturn,
			"This field occupies a single line, and a carriage return would terminate it early and reframe the stream.")
	}
	if strings.IndexByte(s, '\n') >= 0 {
		return fieldErr(field, ErrLineFeed,
			"This field occupies a single line. Only data may span several lines.")
	}
	if !utf8.ValidString(s) {
		return fieldErr(field, ErrInvalidUTF8,
			"Event streams are always UTF-8 and there is no way to select another encoding.")
	}
	return nil
}

// Size returns the exact number of bytes the event will occupy on the wire.
// Callers that enforce a size limit can use it to reject an event before
// encoding it.
func (ev Event) Size() int {
	n := 1 // the blank line that terminates the block
	if ev.Comment != "" {
		n += len(":") + len(ev.Comment) + 1
	}
	if ev.Name != "" {
		n += len("event: ") + len(ev.Name) + 1
	}
	if ev.Retry >= time.Millisecond {
		n += len("retry: ") + decimalLen(int64(ev.Retry/time.Millisecond)) + 1
	}
	if ev.ID.set {
		n += len("id: ") + len(ev.ID.val) + 1
	}
	if ev.Data != nil {
		lines := bytes.Count(ev.Data, []byte{'\n'}) + 1
		n += lines*(len("data: ")+1) + len(ev.Data) - (lines - 1)
	}
	return n
}

func decimalLen(v int64) int {
	n := 1
	for v >= 10 {
		v /= 10
		n++
	}
	return n
}

// NormalizeNewlines converts CRLF pairs and lone carriage returns to line
// feeds, returning a value that [Event.Data] accepts.
//
// It is deliberately explicit rather than automatic: dropping carriage returns
// changes the payload, and the library does not silently change what an
// application asked to send.
func NormalizeNewlines(b []byte) []byte {
	if bytes.IndexByte(b, '\r') < 0 {
		return b
	}
	out := make([]byte, 0, len(b))
	for i := 0; i < len(b); i++ {
		if b[i] == '\r' {
			if i+1 < len(b) && b[i+1] == '\n' {
				continue // let the \n through on the next iteration
			}
			out = append(out, '\n')
			continue
		}
		out = append(out, b[i])
	}
	return out
}
