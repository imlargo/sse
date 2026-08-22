package openapi_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/imlargo/sse"
	"github.com/imlargo/sse/openapi"
)

type Ticket struct {
	ID       int       `json:"id"`
	Title    string    `json:"title"`
	Assignee *string   `json:"assignee,omitempty"`
	Labels   []string  `json:"labels"`
	Created  time.Time `json:"created"`
	Secret   string    `json:"-"`
	internal string
}

type Build struct {
	Ref     string         `json:"ref"`
	Ok      bool           `json:"ok"`
	Metrics map[string]int `json:"metrics"`
	Ticket  Ticket         `json:"ticket"`
}

var (
	ticketCreated = sse.Declare[Ticket]("ticket.created").
			WithDescription("A ticket was opened.").
			OnTopic("tenant.*.tickets")
	buildFinished = sse.Declare[Build]("build.finished").
			OnTopic("tenant.*.builds")
)

func generate(t *testing.T, opts openapi.Options, streams ...openapi.Stream) map[string]any {
	t.Helper()
	doc, err := openapi.Generate(opts, streams...)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := doc.JSON()
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("generated document is not valid JSON: %v", err)
	}
	return out
}

func TestGeneratesOpenAPI32(t *testing.T) {
	doc := generate(t, openapi.Options{Title: "Events", Version: "2.0.0"},
		openapi.Stream{
			Path:      "/events",
			Summary:   "Subscribe to tenant events",
			Catalog:   sse.NewCatalog(ticketCreated, buildFinished),
			Topics:    true,
			Resumable: true,
		})

	// 3.2 is the version that introduced sequential media types and itemSchema.
	// Emitting 3.1 would describe the whole stream as one value, which is wrong.
	if got := doc["openapi"]; got != "3.2.0" {
		t.Errorf("openapi = %v, want 3.2.0", got)
	}

	op := dig(t, doc, "paths", "/events", "get")
	media := dig(t, op, "responses", "200", "content", "text/event-stream")

	if _, ok := media["itemSchema"]; !ok {
		t.Fatal("no itemSchema: the document describes the whole stream rather than one event")
	}
	if _, ok := media["schema"]; ok {
		t.Error("a plain schema was emitted alongside itemSchema; that describes the stream as a single value")
	}
}

// The specification's pattern for a stream carrying several event types: a
// oneOf discriminated by a const event name.
func TestHeterogeneousStreamUsesOneOfOnConstEvent(t *testing.T) {
	doc := generate(t, openapi.Options{OmitReservedEvents: true},
		openapi.Stream{Path: "/events", Catalog: sse.NewCatalog(ticketCreated, buildFinished)})

	item := dig(t, doc, "paths", "/events", "get",
		"responses", "200", "content", "text/event-stream", "itemSchema")

	variants, ok := item["oneOf"].([]any)
	if !ok {
		t.Fatal("itemSchema has no oneOf, so the event types are not distinguishable")
	}
	if len(variants) != 2 {
		t.Fatalf("got %d variants, want 2", len(variants))
	}

	names := map[string]bool{}
	for _, v := range variants {
		props := dig(t, v.(map[string]any), "properties")
		ev := dig(t, props, "event")
		c, ok := ev["const"].(string)
		if !ok {
			t.Fatal("a variant does not pin the event name with const, so a consumer cannot tell them apart")
		}
		names[c] = true

		// The payload is JSON inside data, which is a string on the wire.
		// contentMediaType plus contentSchema is how JSON Schema says that.
		data := dig(t, props, "data")
		if data["contentMediaType"] != "application/json" {
			t.Errorf("variant %q does not declare its data as JSON", c)
		}
		if _, ok := data["contentSchema"]; !ok {
			t.Errorf("variant %q describes no payload shape", c)
		}
	}
	for _, want := range []string{"ticket.created", "build.finished"} {
		if !names[want] {
			t.Errorf("%q is missing from the document", want)
		}
	}
}

// A client that is never told about the gap event treats it as unknown and
// ignores exactly the message it most needs.
func TestReservedEventsAreDocumented(t *testing.T) {
	doc := generate(t, openapi.Options{},
		openapi.Stream{Path: "/events", Catalog: sse.NewCatalog(ticketCreated)})

	item := dig(t, doc, "paths", "/events", "get",
		"responses", "200", "content", "text/event-stream", "itemSchema")
	variants := item["oneOf"].([]any)

	found := map[string]bool{}
	for _, v := range variants {
		props := dig(t, v.(map[string]any), "properties")
		if c, ok := dig(t, props, "event")["const"].(string); ok {
			found[c] = true
		}
	}
	for _, want := range []string{"sse.open", "sse.gap", "sse.closing"} {
		if !found[want] {
			t.Errorf("%q is not documented; a client would treat it as an unknown event", want)
		}
	}

	// And they can be left out for a server that documents them elsewhere.
	bare := generate(t, openapi.Options{OmitReservedEvents: true},
		openapi.Stream{Path: "/events", Catalog: sse.NewCatalog(ticketCreated)})
	bareItem := dig(t, bare, "paths", "/events", "get",
		"responses", "200", "content", "text/event-stream", "itemSchema")
	if n := len(bareItem["oneOf"].([]any)); n != 1 {
		t.Errorf("got %d variants with reserved events omitted, want 1", n)
	}
}

// Go types have to come out as sensible JSON Schema, including the parts that
// are easy to get wrong.
func TestSchemaDerivation(t *testing.T) {
	doc := generate(t, openapi.Options{OmitReservedEvents: true},
		openapi.Stream{Path: "/events", Catalog: sse.NewCatalog(ticketCreated, buildFinished)})

	schemas := dig(t, doc, "components", "schemas")
	ticket, ok := schemas["Ticket"].(map[string]any)
	if !ok {
		t.Fatalf("named struct types must be referenced once under components; got %v", keys(schemas))
	}
	props := dig(t, ticket, "properties")

	if _, ok := props["Secret"]; ok {
		t.Error(`a field tagged json:"-" leaked into the schema`)
	}
	if _, ok := props["internal"]; ok {
		t.Error("an unexported field leaked into the schema")
	}
	if dig(t, props, "created")["format"] != "date-time" {
		t.Error("time.Time was described by its struct fields instead of as a date-time string")
	}
	if dig(t, props, "labels")["type"] != "array" {
		t.Error("a slice was not described as an array")
	}

	required, _ := ticket["required"].([]any)
	var reqNames []string
	for _, r := range required {
		reqNames = append(reqNames, r.(string))
	}
	if slicesContains(reqNames, "assignee") {
		t.Error("an omitempty pointer field was marked required")
	}
	if !slicesContains(reqNames, "id") {
		t.Error("a plain field was not marked required")
	}

	// A type used by two events appears once and is referenced.
	build := dig(t, schemas, "Build")
	inner := dig(t, dig(t, build, "properties"), "ticket")
	if inner["$ref"] != "#/components/schemas/Ticket" {
		t.Errorf("a nested named type was inlined instead of referenced: %v", inner)
	}
}

// SSE is not limited to GET; MCP streams over POST.
func TestMethodIsNotAssumed(t *testing.T) {
	doc := generate(t, openapi.Options{OmitReservedEvents: true},
		openapi.Stream{Path: "/mcp", Method: "POST", Catalog: sse.NewCatalog(ticketCreated)})

	item := dig(t, doc, "paths", "/mcp")
	if _, ok := item["post"]; !ok {
		t.Errorf("the operation was not recorded under post: %v", keys(item))
	}
	if _, ok := item["get"]; ok {
		t.Error("a GET operation was invented")
	}
}

// The document must state whether Last-Event-ID does anything, because a client
// that assumes it does will silently lose events.
func TestResumabilityIsStated(t *testing.T) {
	for _, resumable := range []bool{true, false} {
		doc := generate(t, openapi.Options{OmitReservedEvents: true},
			openapi.Stream{Path: "/events", Catalog: sse.NewCatalog(ticketCreated), Resumable: resumable})
		desc, _ := dig(t, doc, "paths", "/events", "get")["description"].(string)

		if resumable && !strings.Contains(desc, "resumes from that position") {
			t.Errorf("a resumable stream does not say so: %q", desc)
		}
		if !resumable && !strings.Contains(desc, "retains no history") {
			t.Errorf("a non-resumable stream does not say so: %q", desc)
		}
	}
	// Last-Event-ID is always documented as a parameter.
	doc := generate(t, openapi.Options{OmitReservedEvents: true},
		openapi.Stream{Path: "/events", Catalog: sse.NewCatalog(ticketCreated)})
	params := dig(t, doc, "paths", "/events", "get")["parameters"].([]any)
	var names []string
	for _, p := range params {
		names = append(names, p.(map[string]any)["name"].(string))
	}
	if !slicesContains(names, "Last-Event-ID") {
		t.Errorf("Last-Event-ID is not documented: %v", names)
	}
}

func TestEmptyCatalogIsAnError(t *testing.T) {
	if _, err := openapi.Generate(openapi.Options{OmitReservedEvents: true},
		openapi.Stream{Path: "/events", Catalog: sse.NewCatalog()}); err == nil {
		t.Error("a stream declaring no events was accepted")
	}
	if _, err := openapi.Generate(openapi.Options{}, openapi.Stream{Catalog: sse.NewCatalog(ticketCreated)}); err == nil {
		t.Error("a stream with no path was accepted")
	}
}

// --- helpers ---------------------------------------------------------------

func dig(t *testing.T, m map[string]any, path ...string) map[string]any {
	t.Helper()
	cur := m
	for i, k := range path {
		next, ok := cur[k]
		if !ok {
			t.Fatalf("missing %q at %v; have %v", k, path[:i], keys(cur))
		}
		cur, ok = next.(map[string]any)
		if !ok {
			t.Fatalf("%q at %v is %T, not an object", k, path[:i], next)
		}
	}
	return cur
}

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func slicesContains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
