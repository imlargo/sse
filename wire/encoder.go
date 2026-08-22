package wire

import (
	"bytes"
	"io"
	"strconv"
	"time"
)

// Field order on the wire. Metadata first, payload last: it keeps the head of
// a block readable when the payload is large, and it means the varying part of
// a frame (the id) sits before the part that can be shared across subscribers.
//
// The order is not observable by a client. Fields accumulate into buffers and
// are only acted on at the blank line that ends the block.

// AppendEvent encodes ev and appends it to dst, returning the extended buffer.
//
// It is the primitive the rest of the library builds on. Given a buffer with
// enough capacity it performs no allocation, which is what allows one encoded
// frame to be written to every subscriber of a broadcast rather than being
// re-encoded per subscriber.
//
// AppendEvent enforces no size limit; that is policy and belongs to the layer
// that holds configuration. Use [Event.Size] to check ahead of time.
func AppendEvent(dst []byte, ev Event) ([]byte, error) {
	if err := ev.Validate(); err != nil {
		return dst, err
	}
	return appendEvent(dst, ev), nil
}

// appendEvent is AppendEvent without validation, for callers that have already
// validated. Splitting them keeps validation out of replay paths that re-emit
// an event which was checked when it was first published.
func appendEvent(dst []byte, ev Event) []byte {
	if ev.Comment != "" {
		dst = append(dst, ':')
		dst = append(dst, ev.Comment...)
		dst = append(dst, '\n')
	}
	if ev.Name != "" {
		dst = append(dst, "event: "...)
		dst = append(dst, ev.Name...)
		dst = append(dst, '\n')
	}
	if ev.Retry >= time.Millisecond {
		dst = append(dst, "retry: "...)
		dst = strconv.AppendInt(dst, int64(ev.Retry/time.Millisecond), 10)
		dst = append(dst, '\n')
	}
	if ev.ID.set {
		dst = append(dst, "id: "...)
		dst = append(dst, ev.ID.val...)
		dst = append(dst, '\n')
	}
	// A nil Data writes no field at all, which the client suppresses. An empty
	// but non-nil Data writes one empty field, which the client dispatches with
	// an empty payload. The loop below preserves that distinction.
	if ev.Data != nil {
		rest := ev.Data
		for {
			i := bytes.IndexByte(rest, '\n')
			if i < 0 {
				dst = append(dst, "data: "...)
				dst = append(dst, rest...)
				dst = append(dst, '\n')
				break
			}
			dst = append(dst, "data: "...)
			dst = append(dst, rest[:i]...)
			dst = append(dst, '\n')
			rest = rest[i+1:]
		}
	}
	return append(dst, '\n')
}

// AppendComment appends a bare comment line. It is the cheapest thing that can
// be put on the wire, which is what makes it the keep-alive: the WHATWG spec's
// own authoring note recommends a comment roughly every 15 seconds to survive
// proxies that drop idle connections.
func AppendComment(dst []byte, text string) ([]byte, error) {
	if err := checkLine("comment", text); err != nil {
		return dst, err
	}
	dst = append(dst, ':')
	dst = append(dst, text...)
	return append(dst, '\n'), nil
}

// An Encoder writes events to a stream.
//
// It holds a reusable buffer, so a long-lived Encoder encodes without
// allocating once the buffer has grown to the size of the largest event.
// Encoder is not safe for concurrent use: exactly one goroutine must own the
// stream it writes to.
type Encoder struct {
	w   io.Writer
	buf []byte
}

// NewEncoder returns an Encoder writing to w.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: w, buf: make([]byte, 0, 512)}
}

// Encode writes one event.
func (e *Encoder) Encode(ev Event) error {
	if err := ev.Validate(); err != nil {
		return err
	}
	e.buf = appendEvent(e.buf[:0], ev)
	_, err := e.w.Write(e.buf)
	return err
}

// EncodeBatch writes several events in a single call to the underlying writer.
//
// It exists because the cost of an event is dominated by the syscall, not by
// the formatting: batching a burst into one write is the difference between one
// syscall and twenty. Either every event is written or none is, since all of
// them are validated and encoded before anything reaches the writer.
func (e *Encoder) EncodeBatch(evs ...Event) error {
	if len(evs) == 0 {
		return nil
	}
	buf := e.buf[:0]
	for _, ev := range evs {
		if err := ev.Validate(); err != nil {
			e.buf = buf
			return err
		}
		buf = appendEvent(buf, ev)
	}
	e.buf = buf
	_, err := e.w.Write(e.buf)
	return err
}

// Comment writes a bare comment line.
func (e *Encoder) Comment(text string) error {
	var err error
	e.buf, err = AppendComment(e.buf[:0], text)
	if err != nil {
		return err
	}
	_, err = e.w.Write(e.buf)
	return err
}
