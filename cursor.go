package sse

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"slices"
	"strings"
)

// An Offset is a position within one log.
//
// Offsets increase strictly within a log and are never reused. They need not be
// dense: an implementation may leave holes, which is what lets an external log
// use its own native numbering.
//
// Zero means "before everything", so reading from offset 0 replays whatever is
// still retained.
type Offset uint64

// An Epoch identifies one generation of a log's contents.
//
// It changes whenever the stored events are no longer the ones a previous
// cursor referred to — a restart with volatile storage, a different backend, a
// wiped stream. Without it, a client reconnecting after a restart would present
// an offset that now points at completely different events, and the server
// would happily resume against them. That failure is silent and corrupts the
// client's state, which is exactly what RF-C5 exists to prevent.
type Epoch uint64

// A LogID identifies a log within a cursor.
//
// It is derived from the log's name by hashing rather than assigned, so it is
// stable across nodes and restarts with no coordination.
type LogID uint32

// NewLogID derives the identifier for a log name.
func NewLogID(name string) LogID {
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	return LogID(h.Sum32())
}

// MaxCursorSize is the byte budget for an encoded cursor.
//
// The value travels back to the server in the Last-Event-ID header, and proxies
// commonly cap total header size between 4 and 8 KB. A session whose cursor
// cannot fit declares that it does not support resumption rather than silently
// truncating it (RF-C12).
const MaxCursorSize = 4096

// cursorPrefix versions the encoding. An unknown prefix is treated as
// unresolvable — a declared gap — so the scheme can change without a client's
// stored cursor ever resolving against the wrong events.
const cursorPrefix = "sse1."

// ErrCursorUnresolvable means a resumption token cannot be mapped to a
// position. It is never a fatal error: it becomes a declared gap.
var ErrCursorUnresolvable = errors.New("sse: resumption cursor cannot be resolved")

// A Cursor is a client's position, as a vector over logs.
//
// The unit of position is the log, not the topic. That is what keeps the common
// case small: with the default single log a cursor is one entry, roughly thirty
// characters, and a subscriber matching five hundred concrete topics still has
// exactly one position. Tracking a position per topic would grow without bound;
// tracking one per log does not, because the number of logs is a constant the
// operator chooses.
//
// The zero Cursor is a client that has never received anything.
type Cursor struct {
	entries []CursorEntry
}

// A CursorEntry is a position in one log.
type CursorEntry struct {
	Log    LogID
	Epoch  Epoch
	Offset Offset
}

// NewCursor builds a cursor from its entries. Entries are sorted by log so that
// the encoding is deterministic.
func NewCursor(entries ...CursorEntry) Cursor {
	e := slices.Clone(entries)
	slices.SortFunc(e, func(a, b CursorEntry) int {
		switch {
		case a.Log < b.Log:
			return -1
		case a.Log > b.Log:
			return 1
		default:
			return 0
		}
	})
	return Cursor{entries: e}
}

// IsZero reports whether the cursor carries no position at all, which is what a
// client that has never received an event presents.
func (c Cursor) IsZero() bool { return len(c.entries) == 0 }

// Entries returns the positions in the cursor, ordered by log.
func (c Cursor) Entries() []CursorEntry { return slices.Clone(c.entries) }

// Lookup returns the position recorded for a log.
func (c Cursor) Lookup(id LogID) (CursorEntry, bool) {
	for _, e := range c.entries {
		if e.Log == id {
			return e, true
		}
	}
	return CursorEntry{}, false
}

// With returns a copy of the cursor with the position for one log replaced.
//
// Replacing an entry that is already present keeps the existing order, so the
// result needs neither a second copy nor a re-sort. That is the case on every
// delivered event, which is why it is worth separating from the general one.
func (c Cursor) With(e CursorEntry) Cursor {
	for i, existing := range c.entries {
		if existing.Log == e.Log {
			out := slices.Clone(c.entries)
			out[i] = e
			return Cursor{entries: out}
		}
	}
	return NewCursor(append(slices.Clone(c.entries), e)...)
}

// String encodes the cursor as an opaque token, safe in an HTTP header and in a
// URL query. It is what travels in the id field and comes back in
// Last-Event-ID.
//
// The zero cursor encodes as the empty string. Writing an empty id field is the
// specification's own way to clear a client's stored position, so a cleared
// cursor round-trips correctly rather than needing a special case.
func (c Cursor) String() string {
	if c.IsZero() {
		return ""
	}
	return string(c.AppendTo(nil))
}

// AppendTo encodes the cursor into dst and returns the extended buffer.
//
// It exists because a cursor is written on every delivered event, so the
// encoding is on the hot path. Given a buffer with enough capacity it performs
// no allocation, which [Cursor.String] cannot promise.
func (c Cursor) AppendTo(dst []byte) []byte {
	if c.IsZero() {
		return dst
	}
	dst = append(dst, cursorPrefix...)
	return appendCursorBody(dst, c.entries)
}

// appendCursorBody writes the base64url payload for a set of entries.
//
// The varint body is built on the stack for the sizes that matter — a handful
// of logs — so the only work left is the base64 encoding, which writes straight
// into dst.
func appendCursorBody(dst []byte, entries []CursorEntry) []byte {
	var stack [16 * 3 * binary.MaxVarintLen64]byte
	buf := stack[:0]
	if len(entries) > 16 {
		buf = make([]byte, 0, (len(entries)*3+1)*binary.MaxVarintLen64)
	}

	buf = binary.AppendUvarint(buf, uint64(len(entries)))
	for _, e := range entries {
		buf = binary.AppendUvarint(buf, uint64(e.Log))
		buf = binary.AppendUvarint(buf, uint64(e.Epoch))
		buf = binary.AppendUvarint(buf, uint64(e.Offset))
	}

	n := base64.RawURLEncoding.EncodedLen(len(buf))
	start := len(dst)
	dst = append(dst, make([]byte, n)...)
	base64.RawURLEncoding.Encode(dst[start:], buf)
	return dst
}

// Size returns the encoded length in bytes, for checking against
// [MaxCursorSize] before promising a client that resumption works.
func (c Cursor) Size() int { return len(c.String()) }

// ParseCursor decodes a token produced by [Cursor.String].
//
// Anything it cannot make sense of — an unknown version, damaged base64, a
// truncated body — returns [ErrCursorUnresolvable] rather than a cursor. The
// caller turns that into a declared gap. There is no path here that produces a
// position the token did not actually encode.
func ParseCursor(s string) (Cursor, error) {
	if s == "" {
		return Cursor{}, nil
	}
	body, ok := strings.CutPrefix(s, cursorPrefix)
	if !ok {
		return Cursor{}, fmt.Errorf("%w: unrecognised format", ErrCursorUnresolvable)
	}
	raw, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil {
		return Cursor{}, fmt.Errorf("%w: malformed encoding", ErrCursorUnresolvable)
	}

	n, read := binary.Uvarint(raw)
	if read <= 0 {
		return Cursor{}, fmt.Errorf("%w: malformed entry count", ErrCursorUnresolvable)
	}
	// Refuse an absurd count before allocating for it (RP-2).
	if n > uint64(MaxCursorSize) {
		return Cursor{}, fmt.Errorf("%w: implausible entry count %d", ErrCursorUnresolvable, n)
	}
	raw = raw[read:]

	next := func() (uint64, bool) {
		v, r := binary.Uvarint(raw)
		if r <= 0 {
			return 0, false
		}
		raw = raw[r:]
		return v, true
	}

	entries := make([]CursorEntry, 0, n)
	for range n {
		log, ok1 := next()
		epoch, ok2 := next()
		offset, ok3 := next()
		if !ok1 || !ok2 || !ok3 {
			return Cursor{}, fmt.Errorf("%w: truncated entry", ErrCursorUnresolvable)
		}
		if log > math.MaxUint32 {
			return Cursor{}, fmt.Errorf("%w: log id out of range", ErrCursorUnresolvable)
		}
		entries = append(entries, CursorEntry{
			Log:    LogID(log),
			Epoch:  Epoch(epoch),
			Offset: Offset(offset),
		})
	}
	if len(raw) != 0 {
		return Cursor{}, fmt.Errorf("%w: trailing bytes", ErrCursorUnresolvable)
	}
	return NewCursor(entries...), nil
}
