package wire_test

import (
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/imlargo/sse/wire"
)

var update = flag.Bool("update", false, "regenerate testdata/conformance.json")

// A vector is one conformance case: an input stream and everything a
// specification-conforming client would observe from it.
//
// The table is the source of truth and is also exported to
// testdata/conformance.json, so implementations in other languages can run the
// same cases. That is the difference between a serious library and one more SSE
// library.
type vector struct {
	Name  string `json:"name"`
	Why   string `json:"why"`
	Input string `json:"input"`

	Want        []wantMessage `json:"want"`
	WantLastID  string        `json:"wantLastEventId"`
	WantRetryMS int64         `json:"wantRetryMs"`
}

type wantMessage struct {
	Type        string `json:"type"`
	Data        string `json:"data"`
	LastEventID string `json:"lastEventId"`
}

func msg(typ, data, id string) wantMessage {
	return wantMessage{Type: typ, Data: data, LastEventID: id}
}

var vectors = []vector{
	// ---- line terminators -------------------------------------------------
	{
		Name:  "terminator/lf",
		Why:   "A lone line feed terminates a line.",
		Input: "data: a\n\n",
		Want:  []wantMessage{msg("message", "a", "")},
	},
	{
		Name:  "terminator/crlf",
		Why:   "A CRLF pair terminates a line and counts as one terminator.",
		Input: "data: a\r\n\r\n",
		Want:  []wantMessage{msg("message", "a", "")},
	},
	{
		Name:  "terminator/cr",
		Why:   "A lone carriage return terminates a line. Libraries that split only on LF are incorrect here.",
		Input: "data: a\r\r",
		Want:  []wantMessage{msg("message", "a", "")},
	},
	{
		Name:  "terminator/mixed",
		Why:   "Terminators may be mixed freely within one stream.",
		Input: "data: a\r\ndata: b\rdata: c\n\n",
		Want:  []wantMessage{msg("message", "a\nb\nc", "")},
	},

	// ---- byte order mark --------------------------------------------------
	{
		Name:  "bom/stripped",
		Why:   "UTF-8 decoding strips one leading byte order mark.",
		Input: "\uFEFFdata: a\n\n",
		Want:  []wantMessage{msg("message", "a", "")},
	},
	{
		Name:  "bom/only-one",
		Why:   "Only the first byte order mark is stripped; a second one is part of the field name and makes it unknown.",
		Input: "\uFEFF\uFEFFdata: a\n\n",
		Want:  nil,
	},

	// ---- the single space after the colon ---------------------------------
	{
		Name:  "space/absent",
		Why:   "No space after the colon means no space is removed.",
		Input: "data:a\n\n",
		Want:  []wantMessage{msg("message", "a", "")},
	},
	{
		Name:  "space/one",
		Why:   "Exactly one leading space is removed.",
		Input: "data: a\n\n",
		Want:  []wantMessage{msg("message", "a", "")},
	},
	{
		Name:  "space/two",
		Why:   "Only one space is removed, so the second survives. This is why an encoder must always write the space: doing it conditionally eats a character from any payload starting with one.",
		Input: "data:  a\n\n",
		Want:  []wantMessage{msg("message", " a", "")},
	},

	// ---- data presence ----------------------------------------------------
	{
		Name:  "data/empty-value-dispatches",
		Why:   "A data field with an empty value leaves a line feed in the buffer, so the buffer is not empty and the event IS dispatched with empty data. Only a block with no data field at all is suppressed.",
		Input: "data:\n\n",
		Want:  []wantMessage{msg("message", "", "")},
	},
	{
		Name:  "data/absent-suppresses",
		Why:   "A block carrying only an event field has an empty data buffer and is suppressed.",
		Input: "event: ping\n\n",
		Want:  nil,
	},
	{
		Name:  "data/multiline",
		Why:   "Repeated data fields are rejoined with line feeds.",
		Input: "data: a\ndata: b\n\n",
		Want:  []wantMessage{msg("message", "a\nb", "")},
	},
	{
		Name:  "data/trailing-blank-line-preserved",
		Why:   "Only the final line feed is stripped, so a payload can end in one.",
		Input: "data: a\ndata:\n\n",
		Want:  []wantMessage{msg("message", "a\n", "")},
	},
	{
		Name:  "data/no-colon-is-empty-value",
		Why:   "A line with no colon is the field name with an empty value, so a bare 'data' line appends an empty line.",
		Input: "data\n\n",
		Want:  []wantMessage{msg("message", "", "")},
	},

	// ---- the id field -----------------------------------------------------
	{
		Name:       "id/without-data-advances-cursor",
		Why:        "The last event ID is committed before the empty-data check, so an id-only block advances the client's cursor while dispatching nothing. This is the primitive that makes out-of-band checkpointing free.",
		Input:      "id: 42\n\n",
		Want:       nil,
		WantLastID: "42",
	},
	{
		Name:       "id/persists-across-events",
		Why:        "The id buffer is never reset by a dispatch, so a later event with no id inherits the previous one.",
		Input:      "data: a\nid: 1\n\ndata: b\n\n",
		Want:       []wantMessage{msg("message", "a", "1"), msg("message", "b", "1")},
		WantLastID: "1",
	},
	{
		Name:       "id/empty-resets-cursor",
		Why:        "An empty id field sets the cursor to the empty string, so the client sends no Last-Event-ID on its next reconnection. It is the specification's own way to say a stored position is no longer resolvable.",
		Input:      "data: a\nid: 1\n\ndata: b\nid\n\n",
		Want:       []wantMessage{msg("message", "a", "1"), msg("message", "b", "")},
		WantLastID: "",
	},
	{
		Name:       "id/nul-ignored",
		Why:        "An id containing NUL makes the client ignore the whole field, leaving the cursor where it was.",
		Input:      "id: 1\n\ndata: a\nid: x\x00y\n\n",
		Want:       []wantMessage{msg("message", "a", "1")},
		WantLastID: "1",
	},

	// ---- the retry field --------------------------------------------------
	{
		Name:        "retry/digits",
		Why:         "A value of only ASCII digits sets the reconnection time in milliseconds.",
		Input:       "retry: 5000\ndata: a\n\n",
		Want:        []wantMessage{msg("message", "a", "")},
		WantRetryMS: 5000,
	},
	{
		Name:        "retry/non-numeric-ignored",
		Why:         "Anything that is not only ASCII digits is ignored.",
		Input:       "retry: soon\ndata: a\n\n",
		Want:        []wantMessage{msg("message", "a", "")},
		WantRetryMS: 0,
	},
	{
		Name:        "retry/fractional-ignored",
		Why:         "A decimal point is not an ASCII digit, so the field is ignored entirely rather than truncated.",
		Input:       "retry: 1.5\ndata: a\n\n",
		Want:        []wantMessage{msg("message", "a", "")},
		WantRetryMS: 0,
	},
	{
		Name:        "retry/signed-ignored",
		Why:         "A sign is not an ASCII digit.",
		Input:       "retry: -1\ndata: a\n\n",
		Want:        []wantMessage{msg("message", "a", "")},
		WantRetryMS: 0,
	},

	// ---- unknown fields, comments, case -----------------------------------
	{
		Name:  "field/unknown-ignored",
		Why:   "Unknown field names are ignored.",
		Input: "unknown: x\ndata: a\n\n",
		Want:  []wantMessage{msg("message", "a", "")},
	},
	{
		Name:  "field/case-sensitive",
		Why:   "Field names are compared literally, with no case folding, so 'Data' is an unknown field.",
		Input: "Data: ignored\ndata: a\n\n",
		Want:  []wantMessage{msg("message", "a", "")},
	},
	{
		Name:  "comment/ignored",
		Why:   "A line starting with a colon is a comment.",
		Input: ": keep-alive\ndata: a\n\n",
		Want:  []wantMessage{msg("message", "a", "")},
	},
	{
		Name:  "comment/only-dispatches-nothing",
		Why:   "A block of only comments has an empty data buffer and is suppressed. This is what makes a comment a zero-cost keep-alive.",
		Input: ": keep-alive\n\n",
		Want:  nil,
	},
	{
		Name:  "event/named",
		Why:   "An event field sets the dispatched type.",
		Input: "event: token\ndata: a\n\n",
		Want:  []wantMessage{msg("token", "a", "")},
	},
	{
		Name:  "event/resets-between-blocks",
		Why:   "The event type buffer is reset on dispatch, so a later block with no event field is a plain message.",
		Input: "event: token\ndata: a\n\ndata: b\n\n",
		Want:  []wantMessage{msg("token", "a", ""), msg("message", "b", "")},
	},
	{
		Name:  "event/reset-even-when-suppressed",
		Why:   "The event type buffer is also cleared when a block is suppressed for carrying no data.",
		Input: "event: token\n\ndata: b\n\n",
		Want:  []wantMessage{msg("message", "b", "")},
	},

	// ---- end of stream ----------------------------------------------------
	{
		Name:  "eof/incomplete-block-discarded",
		Why:   "Dispatch happens on a blank line, and end of file is not one. A final block without its blank line is dropped, which is why a graceful shutdown must close the last block.",
		Input: "data: a\n",
		Want:  nil,
	},
	{
		Name:  "eof/partial-line-discarded",
		Why:   "A line with no terminator at end of file is discarded.",
		Input: "data: a",
		Want:  nil,
	},

	// ---- the specification's own example ----------------------------------
	{
		Name: "spec/four-block-example",
		Why:  "The four-block example from the WHATWG specification, end to end: a comment that fires nothing, an event that sets the cursor to 1, an event that resets the cursor, and an event whose payload keeps a leading space.",
		Input: ": test stream\n\n" +
			"data: first event\nid: 1\n\n" +
			"data:second event\nid\n\n" +
			"data:  third event\n\n",
		Want: []wantMessage{
			msg("message", "first event", "1"),
			msg("message", "second event", ""),
			msg("message", " third event", ""),
		},
		WantLastID: "",
	},
}

func TestConformance(t *testing.T) {
	for _, v := range vectors {
		t.Run(v.Name, func(t *testing.T) {
			d := wire.NewDecoder(strings.NewReader(v.Input))

			var got []wantMessage
			for {
				m, err := d.Next()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatalf("Next: %v\nwhy: %s", err, v.Why)
				}
				got = append(got, wantMessage{Type: m.Type, Data: m.Data, LastEventID: m.LastEventID})
			}

			if len(got) != len(v.Want) {
				t.Fatalf("got %d message(s), want %d\ngot:  %+v\nwant: %+v\nwhy:  %s",
					len(got), len(v.Want), got, v.Want, v.Why)
			}
			for i := range got {
				if got[i] != v.Want[i] {
					t.Errorf("message %d:\ngot:  %+v\nwant: %+v\nwhy:  %s", i, got[i], v.Want[i], v.Why)
				}
			}
			if id := d.LastEventID(); id != v.WantLastID {
				t.Errorf("LastEventID = %q, want %q\nwhy: %s", id, v.WantLastID, v.Why)
			}
			wantRetry := time.Duration(v.WantRetryMS) * time.Millisecond
			if r := d.Retry(); r != wantRetry {
				t.Errorf("Retry = %v, want %v\nwhy: %s", r, wantRetry, v.Why)
			}
		})
	}
}

// TestConformanceExport keeps testdata/conformance.json in step with the table
// above, so the suite is consumable by implementations in other languages.
// Regenerate with: go test ./wire -run TestConformanceExport -update
func TestConformanceExport(t *testing.T) {
	path := filepath.Join("testdata", "conformance.json")

	want, err := json.MarshalIndent(struct {
		Comment string   `json:"$comment"`
		Source  string   `json:"source"`
		Vectors []vector `json:"vectors"`
	}{
		Comment: "Conformance vectors for text/event-stream. Generated from wire/conformance_test.go; do not edit by hand.",
		Source:  "https://html.spec.whatwg.org/multipage/server-sent-events.html",
		Vectors: vectors,
	}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	want = append(want, '\n')

	if *update {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, want, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v\nrun: go test ./wire -run TestConformanceExport -update", err)
	}
	if string(got) != string(want) {
		t.Errorf("%s is stale\nrun: go test ./wire -run TestConformanceExport -update", path)
	}
}
