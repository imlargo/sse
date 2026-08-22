package wire

import (
	"errors"
	"fmt"
)

// Sentinel causes. Use errors.Is to branch on them; the *FieldError wrapper
// carries which field failed and a message that explains why the spec forbids
// it and what the correct form is (RF-G4, RNF-8).
var (
	// ErrCarriageReturn is returned when a value contains U+000D. The event
	// stream format treats a lone CR, a lone LF and a CRLF pair as equivalent
	// line terminators, so a CR inside a value would silently reframe the
	// stream.
	ErrCarriageReturn = errors.New("value contains a carriage return")

	// ErrLineFeed is returned when a value that must occupy a single line
	// contains U+000A.
	ErrLineFeed = errors.New("value contains a line feed")

	// ErrNUL is returned when an id contains U+0000. The spec makes a client
	// ignore the whole id field in that case, which would silently break
	// resumption.
	ErrNUL = errors.New("value contains a NUL character")

	// ErrInvalidUTF8 is returned for values that are not valid UTF-8. Event
	// streams must always be encoded as UTF-8; there is no way to specify
	// another encoding.
	ErrInvalidUTF8 = errors.New("value is not valid UTF-8")

	// ErrRetryPrecision is returned for a retry below one millisecond. The
	// wire unit is integer milliseconds, so anything smaller would be emitted
	// as "retry: 0", telling the client to reconnect with no delay.
	ErrRetryPrecision = errors.New("retry is below the one-millisecond wire resolution")

	// ErrEmptyEvent is returned when an Event carries no fields at all and
	// would encode to a bare blank line.
	ErrEmptyEvent = errors.New("event has no fields")

	// ErrLineTooLong is returned by Decoder when a single line exceeds its
	// configured limit (RP-2: arbitrary input must never allocate without bound).
	ErrLineTooLong = errors.New("line exceeds the decoder limit")

	// ErrDataTooLarge is returned by Decoder when an event's accumulated data
	// exceeds its configured limit.
	ErrDataTooLarge = errors.New("event data exceeds the decoder limit")
)

// FieldError says which field of an event was rejected and why.
type FieldError struct {
	Field  string // "event", "data", "id", "retry" or "comment"
	Cause  error  // one of the sentinels above
	Advice string // what to do instead
}

func (e *FieldError) Error() string {
	return fmt.Sprintf("sse/wire: %s field: %v. %s", e.Field, e.Cause, e.Advice)
}

func (e *FieldError) Unwrap() error { return e.Cause }

func fieldErr(field string, cause error, advice string) *FieldError {
	return &FieldError{Field: field, Cause: cause, Advice: advice}
}
