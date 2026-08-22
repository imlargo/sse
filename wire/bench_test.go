package wire_test

import (
	"io"
	"strings"
	"testing"

	"github.com/imlargo/sse/wire"
)

// The declared allocation budget for the hot path (RNF-3). AppendEvent is what
// produces the single shared frame a broadcast writes to every subscriber, so a
// regression here multiplies by the number of subscribers.
const appendEventAllocBudget = 0

func TestAppendEventAllocBudget(t *testing.T) {
	buf := make([]byte, 0, 4096)
	ev := wire.Event{
		Name: "token",
		Data: []byte("the quick brown fox jumps over the lazy dog"),
		ID:   wire.MustID("0000000000000042"),
	}

	got := testing.AllocsPerRun(200, func() {
		var err error
		buf, err = wire.AppendEvent(buf[:0], ev)
		if err != nil {
			panic(err)
		}
	})
	if got > appendEventAllocBudget {
		t.Errorf("AppendEvent allocates %.0f times per call, budget is %d",
			got, appendEventAllocBudget)
	}
}

func TestEncoderAllocBudget(t *testing.T) {
	enc := wire.NewEncoder(io.Discard)
	ev := wire.Event{Name: "token", Data: []byte("hello world"), ID: wire.MustID("42")}

	// Warm the internal buffer so the measurement excludes its one-time growth.
	if err := enc.Encode(ev); err != nil {
		t.Fatal(err)
	}
	got := testing.AllocsPerRun(200, func() {
		if err := enc.Encode(ev); err != nil {
			panic(err)
		}
	})
	if got > 0 {
		t.Errorf("Encoder.Encode allocates %.0f times per call, budget is 0", got)
	}
}

func BenchmarkAppendEvent(b *testing.B) {
	cases := []struct {
		name string
		ev   wire.Event
	}{
		{"small", wire.Event{Data: []byte("ok")}},
		{"token", wire.Event{Name: "token", Data: []byte("the "), ID: wire.MustID("1234")}},
		{"json1k", wire.Event{Name: "update", Data: []byte(strings.Repeat("x", 1024)), ID: wire.MustID("1234")}},
		{"multiline", wire.Event{Data: []byte(strings.Repeat("line\n", 32))}},
		{"checkpoint", wire.Event{ID: wire.MustID("v1.aGVsbG8.4210")}},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			buf := make([]byte, 0, 8192)
			b.ReportAllocs()
			b.SetBytes(int64(c.ev.Size()))
			for b.Loop() {
				var err error
				buf, err = wire.AppendEvent(buf[:0], c.ev)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkDecoder(b *testing.B) {
	stream := strings.Repeat("event: token\nid: 1234\ndata: the quick brown fox\n\n", 100)
	b.ReportAllocs()
	b.SetBytes(int64(len(stream)))

	for b.Loop() {
		d := wire.NewDecoder(strings.NewReader(stream))
		for {
			if _, err := d.Next(); err != nil {
				break
			}
		}
	}
}
