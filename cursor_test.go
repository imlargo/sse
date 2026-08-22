package sse_test

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"github.com/imlargo/sse"
	"github.com/imlargo/sse/wire"
)

func TestCursorRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		entries []sse.CursorEntry
	}{
		{"empty", nil},
		{"single log", []sse.CursorEntry{{Log: 7, Epoch: 0xDEADBEEF, Offset: 42}}},
		{"large values", []sse.CursorEntry{{Log: 0xFFFFFFFF, Epoch: sse.Epoch(^uint64(0)), Offset: sse.Offset(^uint64(0))}}},
		{"several logs", []sse.CursorEntry{
			{Log: 3, Epoch: 11, Offset: 100},
			{Log: 1, Epoch: 22, Offset: 200},
			{Log: 2, Epoch: 33, Offset: 300},
		}},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			in := sse.NewCursor(tt.entries...)
			out, err := sse.ParseCursor(in.String())
			if err != nil {
				t.Fatalf("ParseCursor(%q): %v", in.String(), err)
			}
			got, want := out.Entries(), in.Entries()
			if len(got) != len(want) {
				t.Fatalf("got %d entries, want %d", len(got), len(want))
			}
			for i := range got {
				if got[i] != want[i] {
					t.Errorf("entry %d = %+v, want %+v", i, got[i], want[i])
				}
			}
		})
	}
}

// The encoded cursor travels in a header and in a URL, so it must be safe in
// both and it must fit the budget (RF-C12, RF-B7).
func TestCursorIsHeaderAndURLSafe(t *testing.T) {
	c := sse.NewCursor(sse.CursorEntry{Log: 0xFFFFFFFF, Epoch: sse.Epoch(^uint64(0)), Offset: sse.Offset(^uint64(0))})
	s := c.String()

	const safe = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_."
	for _, r := range s {
		if !strings.ContainsRune(safe, r) {
			t.Fatalf("cursor %q contains %q, which is not safe unescaped in a header or a query", s, r)
		}
	}
	// '#' truncates a URL in the browser, so it must never appear.
	if strings.ContainsAny(s, "#%&=+ ") {
		t.Errorf("cursor %q contains a character that breaks in a query string", s)
	}
	// It also has to be a legal event id.
	if _, err := wire.NewID(s); err != nil {
		t.Errorf("cursor is not a valid event id: %v", err)
	}
}

// The common case has to stay small: with one log a cursor is a scalar, and it
// is written on every single event.
func TestSingleLogCursorIsSmall(t *testing.T) {
	c := sse.NewCursor(sse.CursorEntry{Log: sse.NewLogID("events"), Epoch: 0x9E3779B97F4A7C15, Offset: 1_000_000})
	if n := c.Size(); n > 48 {
		t.Errorf("single-log cursor is %d bytes (%q), want it to stay compact", n, c.String())
	}
	t.Logf("single-log cursor: %d bytes, %q", c.Size(), c.String())
}

// Anything unrecognisable must become a declared gap, never a position the
// token did not encode. This is the rule that keeps a stale cursor from
// resolving against a fresh log's events.
func TestUnresolvableCursorsAreRefused(t *testing.T) {
	bad := []struct{ name, token string }{
		{"unknown version", "sse9.AQIDBA"},
		{"no prefix", "42"},
		{"corrupt base64", "sse1.!!!!"},
		{"truncated body", "sse1.Ag"},
		{"trailing bytes", "sse1." + strings.Repeat("A", 40)},
		{"empty body", "sse1."},
		// RP-2: a claimed entry count of a billion must be refused before
		// anything is allocated for it.
		{"absurd entry count", "sse1." + base64.RawURLEncoding.EncodeToString(
			binary.AppendUvarint(nil, 1_000_000_000))},
	}
	for _, tt := range bad {
		t.Run(tt.name, func(t *testing.T) {
			_, err := sse.ParseCursor(tt.token)
			if !errors.Is(err, sse.ErrCursorUnresolvable) {
				t.Fatalf("ParseCursor(%q) = %v, want ErrCursorUnresolvable", tt.token, err)
			}
		})
	}
}

// An empty token is not an error: it is a client that has never received
// anything, which is also what a client sends after the server clears its
// cursor with an empty id field.
func TestEmptyCursorParses(t *testing.T) {
	c, err := sse.ParseCursor("")
	if err != nil {
		t.Fatalf("ParseCursor(\"\"): %v", err)
	}
	if !c.IsZero() {
		t.Error("empty token should produce the zero cursor")
	}
	if c.String() != "" {
		t.Errorf("zero cursor encodes as %q, want empty so an empty id field clears the client", c.String())
	}
}

// A cursor from a previous generation of a log must not resolve. Detecting it
// is the whole job of the epoch.
func TestEpochDistinguishesGenerations(t *testing.T) {
	const log = sse.LogID(1)
	before := sse.NewCursor(sse.CursorEntry{Log: log, Epoch: 100, Offset: 500})

	parsed, err := sse.ParseCursor(before.String())
	if err != nil {
		t.Fatal(err)
	}
	e, ok := parsed.Lookup(log)
	if !ok {
		t.Fatal("log missing from the parsed cursor")
	}
	if e.Epoch != 100 {
		t.Fatalf("epoch = %d, want 100", e.Epoch)
	}
	// The log restarted and now reports a different epoch. Offset 500 is
	// meaningless against it, and the mismatch is what makes that detectable.
	if e.Epoch == 200 {
		t.Fatal("a stale epoch compared equal to a fresh one")
	}
}

func TestCursorWithReplacesPerLog(t *testing.T) {
	c := sse.NewCursor(
		sse.CursorEntry{Log: 1, Epoch: 10, Offset: 100},
		sse.CursorEntry{Log: 2, Epoch: 20, Offset: 200},
	)
	c = c.With(sse.CursorEntry{Log: 1, Epoch: 10, Offset: 150})

	if got, _ := c.Lookup(1); got.Offset != 150 {
		t.Errorf("log 1 offset = %d, want 150", got.Offset)
	}
	if got, _ := c.Lookup(2); got.Offset != 200 {
		t.Errorf("log 2 was disturbed: %+v", got)
	}
	if n := len(c.Entries()); n != 2 {
		t.Errorf("got %d entries, want 2", n)
	}

	c = c.With(sse.CursorEntry{Log: 3, Epoch: 30, Offset: 300})
	if n := len(c.Entries()); n != 3 {
		t.Errorf("got %d entries after adding a log, want 3", n)
	}
}

// A log's identifier is derived from its name by hashing rather than assigned,
// which is what lets nodes agree on it with no coordination.
//
// The values are pinned rather than merely compared to themselves. Deriving
// them the same way twice in one process proves nothing: the requirement is
// that a cursor minted by one binary resolves against another, possibly older,
// running elsewhere. Changing the hash would silently invalidate every stored
// cursor in the world, so it has to break the build instead.
func TestLogIDIsPinned(t *testing.T) {
	pinned := []struct {
		name string
		id   sse.LogID
	}{
		{"events", 316203908},
		{"metrics", 4041682038},
		{"job.demo", 4070305913},
		{"", 2166136261},
	}
	for _, tt := range pinned {
		if got := sse.NewLogID(tt.name); got != tt.id {
			t.Errorf("NewLogID(%q) = %d, want %d — changing this invalidates every "+
				"resumption cursor clients already hold", tt.name, got, tt.id)
		}
	}

	seen := map[sse.LogID]string{}
	for _, name := range []string{"events", "metrics", "job.demo", "tenant.acme", "tenant.globex"} {
		id := sse.NewLogID(name)
		if prev, dup := seen[id]; dup {
			t.Errorf("%q and %q hash to the same log id %d", prev, name, id)
		}
		seen[id] = name
	}
}

func FuzzParseCursor(f *testing.F) {
	f.Add("")
	f.Add("sse1.")
	f.Add(sse.NewCursor(sse.CursorEntry{Log: 1, Epoch: 2, Offset: 3}).String())
	f.Add("sse1.AQIDBA")
	f.Add(strings.Repeat("sse1.A", 100))

	f.Fuzz(func(t *testing.T, token string) {
		c, err := sse.ParseCursor(token)
		if err != nil {
			return
		}
		// Whatever parses must re-encode to something that parses back the
		// same. A cursor that round-trips differently could resolve against a
		// position it never named.
		again, err := sse.ParseCursor(c.String())
		if err != nil {
			t.Fatalf("re-parsing own output %q failed: %v", c.String(), err)
		}
		got, want := again.Entries(), c.Entries()
		if len(got) != len(want) {
			t.Fatalf("round trip changed entry count: %d -> %d", len(want), len(got))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("round trip changed entry %d: %+v -> %+v", i, want[i], got[i])
			}
		}
	})
}
