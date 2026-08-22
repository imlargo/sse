package wire_test

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/imlargo/sse/wire"
)

// FuzzDecoder asserts RP-2: arbitrary input never panics and never allocates
// without bound. The decoder is fed with small limits so the fuzzer reaches the
// guards quickly instead of spending its budget growing buffers.
func FuzzDecoder(f *testing.F) {
	for _, v := range vectors {
		f.Add(v.Input)
	}
	f.Add("")
	f.Add("\x00\x00\x00")
	f.Add("data")
	f.Add(":")
	f.Add("id:\x00")
	f.Add("retry:99999999999999999999999999")
	f.Add("\uFEFFdata: a\n\n")
	f.Add(strings.Repeat("data: x\n", 1000))
	f.Add(strings.Repeat("a", 10000))

	f.Fuzz(func(t *testing.T, input string) {
		const maxLine, maxData = 1 << 10, 1 << 12

		d := wire.NewDecoderLimits(strings.NewReader(input), maxLine, maxData)
		for {
			m, err := d.Next()
			if err != nil {
				// The only errors a decoder may produce are end of stream and
				// the two bounded-growth guards. Anything else means the
				// decoder invented a failure mode.
				if errors.Is(err, io.EOF) ||
					errors.Is(err, wire.ErrLineTooLong) ||
					errors.Is(err, wire.ErrDataTooLarge) {
					return
				}
				t.Fatalf("unexpected error kind: %v", err)
			}
			if len(m.Data) > maxData {
				t.Fatalf("data of %d bytes exceeds the %d limit", len(m.Data), maxData)
			}
			if len(m.Type) > maxLine || len(m.LastEventID) > maxLine {
				t.Fatalf("field exceeded the line limit")
			}
		}
	})
}

// FuzzRoundTrip asserts that anything the encoder accepts survives a decode
// unchanged. A payload the encoder blesses but the client would misread is the
// worst kind of bug: silent corruption.
func FuzzRoundTrip(f *testing.F) {
	f.Add("hello", "message", "1")
	f.Add("", "", "")
	f.Add("a\nb", "token", "42")
	f.Add(" leading", "e", "")
	f.Add("🌊", "unicode", "id-with-dash")

	f.Fuzz(func(t *testing.T, data, name, id string) {
		ev := wire.Event{Name: name, Data: []byte(data)}
		if id != "" {
			parsed, err := wire.NewID(id)
			if err != nil {
				t.Skip()
			}
			ev.ID = parsed
		}

		buf, err := wire.AppendEvent(nil, ev)
		if err != nil {
			t.Skip() // the encoder refused it; that is its job
		}

		d := wire.NewDecoder(strings.NewReader(string(buf)))
		m, err := d.Next()
		if err != nil {
			t.Fatalf("encoder produced %q which the decoder rejected: %v", buf, err)
		}
		if m.Data != data {
			t.Fatalf("data corrupted: sent %q, read %q (wire %q)", data, m.Data, buf)
		}
		wantType := name
		if wantType == "" {
			wantType = "message"
		}
		if m.Type != wantType {
			t.Fatalf("type corrupted: sent %q, read %q (wire %q)", wantType, m.Type, buf)
		}
		if ev.ID.IsSet() && m.LastEventID != id {
			t.Fatalf("id corrupted: sent %q, read %q (wire %q)", id, m.LastEventID, buf)
		}
	})
}
