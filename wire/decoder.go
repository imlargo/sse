package wire

import (
	"bufio"
	"bytes"
	"io"
	"strconv"
	"time"
)

// Default decoder limits. Arbitrary input must never allocate without bound
// (RP-2), so both a single line and an event's accumulated data are capped.
const (
	DefaultMaxLineSize = 64 << 10 // 64 KiB
	DefaultMaxDataSize = 8 << 20  // 8 MiB
)

var utf8BOM = []byte{0xEF, 0xBB, 0xBF}

// A Message is an event as a client would observe it, after parsing.
type Message struct {
	// Type is the event type. It is "message" when the stream sent no event
	// field, which is the type a browser dispatches in that case.
	Type string

	// Data is the payload, with the data fields of the block rejoined by line
	// feeds and the trailing one removed.
	Data string

	// LastEventID is the client's resumption cursor as of this message. It
	// carries over from earlier blocks: an event with no id field inherits
	// whatever id was last seen.
	LastEventID string
}

// A Decoder parses an event stream exactly as the WHATWG HTML specification
// defines it, including the details implementations usually skip: all three
// line terminators, byte order mark removal, the single-space rule after the
// colon, literal field-name comparison, and the dispatch ordering that commits
// an id before suppressing a block that carries no data.
//
// It exists for tests, for the conformance suite and for anyone consuming a
// stream. It is not a client: it has no reconnection, no backoff and no cursor
// management, which is why a Go client is out of scope for v1.
//
// A Decoder is not safe for concurrent use.
type Decoder struct {
	r       *bufio.Reader
	maxLine int
	maxData int

	bomChecked bool
	line       []byte

	// Parse state, named after the specification's buffers.
	data      []byte
	eventType string
	idBuffer  string // persists across dispatches; never reset by a dispatch

	// Client state, committed at dispatch.
	lastEventID string
	retry       time.Duration

	err error
}

// NewDecoder returns a Decoder reading from r with the default limits.
func NewDecoder(r io.Reader) *Decoder {
	return NewDecoderLimits(r, DefaultMaxLineSize, DefaultMaxDataSize)
}

// NewDecoderLimits returns a Decoder with explicit caps on the length of one
// line and on the accumulated data of one event.
func NewDecoderLimits(r io.Reader, maxLine, maxData int) *Decoder {
	return &Decoder{
		r:       bufio.NewReader(r),
		maxLine: maxLine,
		maxData: maxData,
	}
}

// Next returns the next dispatched message.
//
// Blocks that the specification suppresses — one carrying only an id, or only
// a comment — do not produce a message, but their side effects are applied
// before Next moves on, so [Decoder.LastEventID] and [Decoder.Retry] still
// reflect them.
//
// It returns io.EOF at the end of the stream. Anything buffered from an
// incomplete final block is discarded, because the specification dispatches on
// a blank line and end of file is not one.
func (d *Decoder) Next() (Message, error) {
	if d.err != nil {
		return Message{}, d.err
	}
	d.stripBOM()

	for {
		line, err := d.readLine()
		if err != nil {
			d.err = err
			d.data = d.data[:0]
			d.eventType = ""
			return Message{}, err
		}

		if len(line) == 0 {
			if m, ok := d.dispatch(); ok {
				return m, nil
			}
			continue
		}
		if line[0] == ':' {
			continue
		}

		field, value, hasColon := bytes.Cut(line, []byte{':'})
		if hasColon && len(value) > 0 && value[0] == ' ' {
			// Exactly one space is removed, which is why the encoder always
			// writes one.
			value = value[1:]
		}
		if err := d.processField(field, value); err != nil {
			d.err = err
			return Message{}, err
		}
	}
}

// LastEventID returns the client's resumption cursor. It is set at dispatch,
// including for a block that dispatched nothing because it carried no data,
// which is what lets a server checkpoint a cursor without emitting an event.
func (d *Decoder) LastEventID() string { return d.lastEventID }

// Retry returns the reconnection delay the stream last requested, or zero if
// it never did.
func (d *Decoder) Retry() time.Duration { return d.retry }

func (d *Decoder) stripBOM() {
	if d.bomChecked {
		return
	}
	d.bomChecked = true
	if p, err := d.r.Peek(3); err == nil && bytes.Equal(p, utf8BOM) {
		_, _ = d.r.Discard(3)
	}
}

// readLine returns one line, accepting CRLF, a lone LF and a lone CR as
// terminators. A partial line at end of file is discarded along with its error.
func (d *Decoder) readLine() ([]byte, error) {
	line := d.line[:0]
	for {
		b, err := d.r.ReadByte()
		if err != nil {
			d.line = line
			return nil, err
		}
		switch b {
		case '\n':
			d.line = line
			return line, nil
		case '\r':
			// A CR terminates the line whether or not an LF follows it, so
			// look ahead by one and put the byte back if it is not an LF.
			if nb, nerr := d.r.ReadByte(); nerr == nil && nb != '\n' {
				_ = d.r.UnreadByte()
			}
			d.line = line
			return line, nil
		default:
			if len(line) >= d.maxLine {
				d.line = line
				return nil, ErrLineTooLong
			}
			line = append(line, b)
		}
	}
}

// processField applies one field. Field names are compared literally, with no
// case folding, so "Data" is an unknown field and is ignored.
func (d *Decoder) processField(field, value []byte) error {
	switch string(field) {
	case "event":
		d.eventType = string(value)

	case "data":
		if len(d.data)+len(value)+1 > d.maxData {
			return ErrDataTooLarge
		}
		d.data = append(d.data, value...)
		d.data = append(d.data, '\n')

	case "id":
		// A NUL makes a client ignore the field entirely, leaving its cursor
		// on the previous value.
		if bytes.IndexByte(value, 0) < 0 {
			d.idBuffer = string(value)
		}

	case "retry":
		if n, ok := parseRetry(value); ok {
			d.retry = n
		}

	default:
		// Unknown fields are ignored.
	}
	return nil
}

// dispatch applies the end of a block, in the order the specification defines.
// The ordering matters: the id is committed before the empty-data check, so a
// block carrying only an id advances the client's cursor without dispatching.
func (d *Decoder) dispatch() (Message, bool) {
	d.lastEventID = d.idBuffer

	if len(d.data) == 0 {
		d.eventType = ""
		return Message{}, false
	}

	data := d.data
	if data[len(data)-1] == '\n' {
		data = data[:len(data)-1]
	}

	m := Message{
		Type:        d.eventType,
		Data:        string(data),
		LastEventID: d.lastEventID,
	}
	if m.Type == "" {
		m.Type = "message"
	}

	d.data = d.data[:0]
	d.eventType = ""
	return m, true
}

// parseRetry accepts only ASCII digits, as the specification requires. Anything
// else, including a signed or fractional value, leaves the current delay alone.
func parseRetry(value []byte) (time.Duration, bool) {
	if len(value) == 0 {
		return 0, false
	}
	for _, c := range value {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	ms, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		return 0, false // out of range
	}
	if ms > int64(maxRetryMillis) {
		return 0, false
	}
	return time.Duration(ms) * time.Millisecond, true
}

// maxRetryMillis keeps the multiplication to time.Duration from overflowing.
const maxRetryMillis = int64(1<<63-1) / int64(time.Millisecond)
