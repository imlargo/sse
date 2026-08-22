package wire_test

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/imlargo/sse/wire"
)

func TestAppendEvent(t *testing.T) {
	tests := []struct {
		name string
		ev   wire.Event
		want string
	}{
		{
			name: "data only",
			ev:   wire.Event{Data: []byte("hello")},
			want: "data: hello\n\n",
		},
		{
			name: "always writes the space after the colon",
			ev:   wire.Event{Data: []byte(" leading space")},
			want: "data:  leading space\n\n",
		},
		{
			name: "empty but non-nil data writes one empty field",
			ev:   wire.Event{Data: []byte{}},
			want: "data: \n\n",
		},
		{
			name: "nil data with an id writes no data field",
			ev:   wire.Event{ID: wire.MustID("42")},
			want: "id: 42\n\n",
		},
		{
			name: "multiline data repeats the field",
			ev:   wire.Event{Data: []byte("a\nb\nc")},
			want: "data: a\ndata: b\ndata: c\n\n",
		},
		{
			name: "data ending in a newline",
			ev:   wire.Event{Data: []byte("a\n")},
			want: "data: a\ndata: \n\n",
		},
		{
			name: "every field",
			ev: wire.Event{
				Comment: " keep-alive",
				Name:    "token",
				Data:    []byte("x"),
				ID:      wire.MustID("7"),
				Retry:   5 * time.Second,
			},
			want: ": keep-alive\nevent: token\nretry: 5000\nid: 7\ndata: x\n\n",
		},
		{
			name: "reset id writes an empty field",
			ev:   wire.Event{Data: []byte("x"), ID: wire.ResetID()},
			want: "id: \ndata: x\n\n",
		},
		{
			name: "sub-millisecond retry is not written when zero",
			ev:   wire.Event{Data: []byte("x")},
			want: "data: x\n\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := wire.AppendEvent(nil, tt.ev)
			if err != nil {
				t.Fatalf("AppendEvent: %v", err)
			}
			if string(got) != tt.want {
				t.Errorf("got  %q\nwant %q", got, tt.want)
			}
			if n := tt.ev.Size(); n != len(got) {
				t.Errorf("Size() = %d, encoded %d bytes", n, len(got))
			}
		})
	}
}

// TestRoundTrip is the property that matters: what the encoder writes is what a
// conforming client reads back.
func TestRoundTrip(t *testing.T) {
	payloads := []string{
		"", "a", "hello world", " leading", "trailing ", "a\nb", "a\n", "\n",
		"\n\n", "unicode: áéí 日本語 🌊", ":colon", "data: nested", "  ",
	}
	for _, p := range payloads {
		t.Run(strings.ReplaceAll(p, "\n", `\n`), func(t *testing.T) {
			ev := wire.Event{Name: "e", Data: []byte(p), ID: wire.MustID("1")}
			buf, err := wire.AppendEvent(nil, ev)
			if err != nil {
				t.Fatalf("AppendEvent: %v", err)
			}

			d := wire.NewDecoder(bytes.NewReader(buf))
			m, err := d.Next()
			if err != nil {
				t.Fatalf("Next: %v (encoded %q)", err, buf)
			}
			if m.Data != p {
				t.Errorf("data round-trip: got %q, want %q (encoded %q)", m.Data, p, buf)
			}
			if m.Type != "e" {
				t.Errorf("type = %q, want %q", m.Type, "e")
			}
			if m.LastEventID != "1" {
				t.Errorf("id = %q, want %q", m.LastEventID, "1")
			}
			if _, err := d.Next(); !errors.Is(err, io.EOF) {
				t.Errorf("expected exactly one message, got a second")
			}
		})
	}
}

func TestEncodeBatch(t *testing.T) {
	var buf bytes.Buffer
	enc := wire.NewEncoder(&buf)

	err := enc.EncodeBatch(
		wire.Event{Data: []byte("a")},
		wire.Event{Data: []byte("b")},
		wire.Event{Data: []byte("c")},
	)
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	if got, want := buf.String(), "data: a\n\ndata: b\n\ndata: c\n\n"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// A batch is all-or-nothing: an invalid event must not leave a partial batch on
// the wire, because a half-written block would corrupt the stream framing.
func TestEncodeBatchIsAtomic(t *testing.T) {
	var buf bytes.Buffer
	enc := wire.NewEncoder(&buf)

	err := enc.EncodeBatch(
		wire.Event{Data: []byte("ok")},
		wire.Event{Name: "bad\nname", Data: []byte("x")},
	)
	if err == nil {
		t.Fatal("expected an error for the invalid event")
	}
	if buf.Len() != 0 {
		t.Errorf("wrote %q, want nothing", buf.String())
	}
}

func TestValidation(t *testing.T) {
	tests := []struct {
		name string
		ev   wire.Event
		want error
	}{
		{"zero event", wire.Event{}, wire.ErrEmptyEvent},
		{"CR in data", wire.Event{Data: []byte("a\rb")}, wire.ErrCarriageReturn},
		{"CRLF in data", wire.Event{Data: []byte("a\r\nb")}, wire.ErrCarriageReturn},
		{"LF in name", wire.Event{Name: "a\nb", Data: []byte("x")}, wire.ErrLineFeed},
		{"CR in name", wire.Event{Name: "a\rb", Data: []byte("x")}, wire.ErrCarriageReturn},
		{"LF in comment", wire.Event{Comment: "a\nb"}, wire.ErrLineFeed},
		{"invalid UTF-8 in data", wire.Event{Data: []byte{0xff, 0xfe}}, wire.ErrInvalidUTF8},
		{"sub-millisecond retry", wire.Event{Data: []byte("x"), Retry: time.Microsecond}, wire.ErrRetryPrecision},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := wire.AppendEvent(nil, tt.ev)
			if !errors.Is(err, tt.want) {
				t.Fatalf("got %v, want %v", err, tt.want)
			}
			// RF-G4: the message has to teach, not just refuse.
			if tt.want != wire.ErrEmptyEvent {
				var fe *wire.FieldError
				if !errors.As(err, &fe) {
					t.Fatalf("error is not a *FieldError: %v", err)
				}
				if fe.Field == "" || fe.Advice == "" {
					t.Errorf("FieldError must name the field and say what to do: %+v", fe)
				}
			}
		})
	}
}

func TestIDValidation(t *testing.T) {
	if _, err := wire.NewID("has\x00nul"); !errors.Is(err, wire.ErrNUL) {
		t.Errorf("NUL in id: got %v, want ErrNUL", err)
	}
	if _, err := wire.NewID("has\nnewline"); !errors.Is(err, wire.ErrLineFeed) {
		t.Errorf("LF in id: got %v, want ErrLineFeed", err)
	}
	if id := wire.NoID(); id.IsSet() {
		t.Error("NoID must not be set")
	}
	if id := wire.ResetID(); !id.IsSet() || id.String() != "" {
		t.Error("ResetID must be set and empty")
	}
	// The zero value is NoID, so an Event literal that omits ID behaves.
	var zero wire.ID
	if zero.IsSet() {
		t.Error("the zero ID must be NoID")
	}
}

func TestNormalizeNewlines(t *testing.T) {
	tests := []struct{ in, want string }{
		{"a\r\nb", "a\nb"},
		{"a\rb", "a\nb"},
		{"a\nb", "a\nb"},
		{"a\r\n\r\nb", "a\n\nb"},
		{"none", "none"},
	}
	for _, tt := range tests {
		if got := string(wire.NormalizeNewlines([]byte(tt.in))); got != tt.want {
			t.Errorf("NormalizeNewlines(%q) = %q, want %q", tt.in, got, tt.want)
		}
		// The result must always be acceptable to the encoder.
		if _, err := wire.AppendEvent(nil, wire.Event{Data: wire.NormalizeNewlines([]byte(tt.in))}); err != nil {
			t.Errorf("normalized %q still rejected: %v", tt.in, err)
		}
	}
}
