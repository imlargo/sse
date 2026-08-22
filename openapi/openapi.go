package openapi

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/imlargo/sse"
)

// Stream describes one server-sent event endpoint.
type Stream struct {
	// Path is the URL path, for example "/events".
	Path string
	// Method is the HTTP method. Empty means GET. It is a field rather than an
	// assumption because SSE is not limited to GET and MCP streams over POST.
	Method string
	// Summary and Description are prose for the operation.
	Summary     string
	Description string
	// Catalog is what the stream emits.
	Catalog *sse.Catalog
	// Topics documents the topic parameter, if the endpoint takes one.
	Topics bool
	// Resumable records whether the stream retains history, so the document
	// states whether Last-Event-ID does anything.
	Resumable bool
}

// Document is a generated OpenAPI 3.2 description.
type Document map[string]any

// Options configure the generated document.
type Options struct {
	Title       string
	Version     string
	Description string
	// IncludeReservedEvents adds the library's own events to every stream.
	// It defaults on: a client that is not told about sse.gap will treat it as
	// an unknown event and ignore exactly the message it most needs.
	OmitReservedEvents bool
	// ReservedPrefix must match the server's, when it was changed.
	ReservedPrefix string
}

// Generate builds an OpenAPI 3.2 document for the given streams.
func Generate(opts Options, streams ...Stream) (Document, error) {
	if opts.Title == "" {
		opts.Title = "Event streams"
	}
	if opts.Version == "" {
		opts.Version = "1.0.0"
	}
	if opts.ReservedPrefix == "" {
		opts.ReservedPrefix = sse.DefaultPrefix
	}

	defs := map[string]Schema{}
	seen := map[reflect.Type]bool{}
	paths := map[string]any{}

	for _, st := range streams {
		if st.Path == "" {
			return nil, fmt.Errorf("openapi: a stream was declared with no path")
		}
		method := st.Method
		if method == "" {
			method = "get"
		}
		method = lower(method)

		item, _ := paths[st.Path].(map[string]any)
		if item == nil {
			item = map[string]any{}
			paths[st.Path] = item
		}
		op, err := operation(st, opts, defs, seen)
		if err != nil {
			return nil, err
		}
		item[method] = op
	}

	doc := Document{
		"openapi": "3.2.0",
		"info": map[string]any{
			"title":       opts.Title,
			"version":     opts.Version,
			"description": opts.Description,
		},
		"paths": paths,
	}
	if len(defs) > 0 {
		doc["components"] = map[string]any{"schemas": defs}
	}
	return doc, nil
}

func operation(st Stream, opts Options, defs map[string]Schema, seen map[reflect.Type]bool) (map[string]any, error) {
	variants, err := eventVariants(st, opts, defs, seen)
	if err != nil {
		return nil, err
	}

	desc := st.Description
	if st.Resumable {
		desc += "\n\nThe stream retains history. A client that reconnects with a " +
			"Last-Event-ID header resumes from that position; when the position " +
			"can no longer be resolved the stream sends a " + opts.ReservedPrefix +
			"gap event before anything else, and the client should reload its state " +
			"rather than assume the stream was continuous."
	} else {
		desc += "\n\nThe stream retains no history. Last-Event-ID is accepted but " +
			"nothing can be replayed, and the connection event reports " +
			"resumable false."
	}

	op := map[string]any{
		"summary":     st.Summary,
		"description": desc,
		"responses": map[string]any{
			"200": map[string]any{
				"description": "An event stream.",
				"content": map[string]any{
					"text/event-stream": map[string]any{
						// itemSchema, not schema: it describes one event as it
						// arrives rather than the stream as a whole, which is
						// what lets a consumer process each item as it is read.
						"itemSchema": map[string]any{
							"$ref":     "#/components/schemas/ServerSentEvent",
							"required": []string{"event", "data"},
							"oneOf":    variants,
						},
					},
				},
			},
		},
	}

	var params []any
	if st.Topics {
		params = append(params, map[string]any{
			"name":        sse.TopicQueryParam,
			"in":          "query",
			"required":    false,
			"description": "Topic filter. Repeat for several. Supports '*' for one token and '>' for one or more trailing tokens. Filters travel in the query string because EventSource cannot send headers.",
			"schema":      map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
		})
	}
	params = append(params, map[string]any{
		"name":        "Last-Event-ID",
		"in":          "header",
		"required":    false,
		"description": "Resumption cursor, returned by the client exactly as the server sent it in the id field.",
		"schema":      map[string]any{"type": "string"},
	})
	op["parameters"] = params

	// The generic envelope, exactly as the specification publishes it.
	defs["ServerSentEvent"] = Schema{
		"type":        "object",
		"description": "One event, after parsing per the text/event-stream algorithm.",
		"required":    []string{"data"},
		"properties": map[string]any{
			"data":  map[string]any{"type": "string"},
			"event": map[string]any{"type": "string"},
			"id":    map[string]any{"type": "string"},
			"retry": map[string]any{"type": "integer", "minimum": 0},
		},
	}
	return op, nil
}

func eventVariants(st Stream, opts Options, defs map[string]Schema, seen map[reflect.Type]bool) ([]any, error) {
	var out []any
	add := func(name, description string, sample any) {
		props := map[string]any{
			"event": map[string]any{"const": name},
		}
		if sample != nil {
			t := typeOf(sample)
			// The payload is JSON inside the data field, which is a string on
			// the wire. contentMediaType and contentSchema are how JSON Schema
			// describes exactly that, and are what the specification points to
			// for this case.
			props["data"] = map[string]any{
				"contentMediaType": "application/json",
				"contentSchema":    schemaFor(t, defs, seen),
			}
		}
		v := map[string]any{"properties": props}
		if description != "" {
			v["description"] = description
		}
		out = append(out, v)
	}

	for _, e := range st.Catalog.Events() {
		d := e.Description
		if e.Topic != "" {
			if d != "" {
				d += " "
			}
			d += "Published to " + e.Topic + "."
		}
		add(e.Name, d, e.Sample)
	}

	if !opts.OmitReservedEvents {
		p := opts.ReservedPrefix
		add(p+"open", "Sent once when the stream opens. Reports the session id, whether resumption is available, the delivery semantics actually in force, the retention window and which filters were granted or denied.", OpenEvent{})
		add(p+"gap", "Sent when history the client asked for cannot be provided, before any replayed event. Reload state rather than assuming the stream was continuous.", GapEvent{})
		add(p+"closing", "Sent when the server is draining. The retry field carries a jittered delay so clients dropped together do not reconnect together.", ClosingEvent{})
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("openapi: stream %q declares no events; pass a catalog", st.Path)
	}
	return out, nil
}

// The payloads of the library's own events.
//
// They are declared here rather than in the core because the delivery path has
// no use for them: their shape is a documentation concern. Exported so a
// consumer can unmarshal into them, and so the generated document names them
// properly instead of leaking Go's unexported naming.
type OpenEvent struct {
	SessionID       string            `json:"sessionId"`
	Resumable       bool              `json:"resumable"`
	Delivery        string            `json:"delivery"`
	Recovery        string            `json:"recovery"`
	RetentionMs     int64             `json:"retentionMs,omitempty"`
	RetentionEvents int               `json:"retentionEvents,omitempty"`
	KeepAliveMs     int64             `json:"keepAliveMs"`
	RetryMs         int64             `json:"retryMs"`
	Granted         []string          `json:"granted,omitempty"`
	Denied          []sse.Denial      `json:"denied,omitempty"`
	Identity        string            `json:"identity,omitempty"`
	Events          []string          `json:"events,omitempty"`
	Attributes      map[string]string `json:"attributes,omitempty"`
}

type GapEvent struct {
	Reason  string `json:"reason"`
	From    uint64 `json:"from"`
	Through uint64 `json:"through"`
	Detail  string `json:"detail"`
}

type ClosingEvent struct {
	Reason           string `json:"reason"`
	ReconnectAfterMs int64  `json:"reconnectAfterMs"`
}

// JSON renders the document.
func (d Document) JSON() ([]byte, error) {
	return json.MarshalIndent(d, "", "  ")
}

func lower(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}
